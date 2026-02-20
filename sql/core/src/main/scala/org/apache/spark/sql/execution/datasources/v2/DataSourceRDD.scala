/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.v2

import scala.language.existentials

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.metric.{CustomMetrics, SQLMetric}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ArrayImplicits._

class DataSourceRDDPartition(val index: Int, val inputPartitions: Seq[InputPartition])
  extends Partition with Serializable

/**
 * Holds the state for a reader in a thread/task, used by the completion listener to access the most
 * recently created reader and iterator for final metrics updates and cleanup.
 *
 * When `compute()` is called multiple times on the same thread (e.g., different input partitions
 * of a scan are coalesced), this state is updated to track the most recent reader. The task
 * completion listener then uses this most recent reader for final cleanup and metrics reporting.
 *
 * @param reader The partition reader
 * @param iterator The metrics iterator wrapping the reader
 */
private case class ReaderState(reader: PartitionReader[_], iterator: MetricsIterator[_])

// TODO: we should have 2 RDDs: an RDD[InternalRow] for row-based scan, an `RDD[ColumnarBatch]` for
// columnar scan.
class DataSourceRDD(
    sc: SparkContext,
    @transient private val inputPartitions: Seq[Seq[InputPartition]],
    partitionReaderFactory: PartitionReaderFactory,
    columnarReads: Boolean,
    customMetrics: Map[String, SQLMetric])
  extends RDD[InternalRow](sc, Nil) {

  // ThreadLocal to store the last reader state for this thread/task.
  // A null value indicates that no completion listener has been added yet.
  @transient lazy private val readerStateThreadLocal = new ThreadLocal[ReaderState]()

  override protected def getPartitions: Array[Partition] = {
    inputPartitions.zipWithIndex.map {
      case (inputPartitions, index) => new DataSourceRDDPartition(index, inputPartitions)
    }.toArray
  }

  private def castPartition(split: Partition): DataSourceRDDPartition = split match {
    case p: DataSourceRDDPartition => p
    case _ => throw QueryExecutionErrors.notADatasourceRDDPartitionError(split)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {

    // In case of early stopping before consuming the entire iterator, we need to do one more metric
    // update at the end of the task.
    // Add completion listener only once per thread (null means no listener added yet)
    val readerState = readerStateThreadLocal.get()
    if (readerState == null) {
      context.addTaskCompletionListener[Unit] { _ =>
        // Use the reader and iterator from ThreadLocal (the last ones created in this thread/task)
        val readerState = readerStateThreadLocal.get()
        if (readerState != null) {
          CustomMetrics.updateMetrics(
            readerState.reader.currentMetricsValues.toImmutableArraySeq, customMetrics)
          readerState.iterator.forceUpdateMetrics()
          readerState.reader.close()
        }
        readerStateThreadLocal.remove()
      }
    }

    val iterator = new Iterator[Object] {
      private val inputPartitions = castPartition(split).inputPartitions
      private var currentIter: Option[Iterator[Object]] = None
      private var currentIndex: Int = 0

      override def hasNext: Boolean = currentIter.exists(_.hasNext) || advanceToNextIter()

      override def next(): Object = {
        if (!hasNext) throw new NoSuchElementException("No more elements")
        currentIter.get.next()
      }

      private def advanceToNextIter(): Boolean = {
        if (currentIndex >= inputPartitions.length) {
          false
        } else {
          val inputPartition = inputPartitions(currentIndex)
          currentIndex += 1

          // TODO: SPARK-25083 remove the type erasure hack in data source scan
          val (iter, reader) = if (columnarReads) {
            val batchReader = partitionReaderFactory.createColumnarReader(inputPartition)
            val iter = new MetricsBatchIterator(
              new PartitionIterator[ColumnarBatch](batchReader, customMetrics))
            (iter, batchReader)
          } else {
            val rowReader = partitionReaderFactory.createReader(inputPartition)
            val iter = new MetricsRowIterator(
              new PartitionIterator[InternalRow](rowReader, customMetrics))
            (iter, rowReader)
          }

          val readerState = readerStateThreadLocal.get()
          if (readerState != null) {
            val metrics = readerState.reader.currentMetricsValues
            CustomMetrics.updateMetrics(metrics.toImmutableArraySeq, customMetrics)
            reader.initMetricsValues(metrics)
            readerState.reader.close()
          }

          // Store the current reader and iterator in ThreadLocal so the completion listener can
          // access the most recently created instances. On subsequent compute() calls in the same
          // thread/task, the metrics from the previous reader are preserved and can be used to
          // initialize the new reader.
          readerStateThreadLocal.set(ReaderState(reader, iter))

          currentIter = Some(iter)
          hasNext
        }
      }
    }

    new InterruptibleIterator(context, iterator).asInstanceOf[Iterator[InternalRow]]
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    castPartition(split).inputPartitions.flatMap(_.preferredLocations())
  }
}

private class PartitionIterator[T](
    reader: PartitionReader[T],
    customMetrics: Map[String, SQLMetric]) extends Iterator[T] {
  private[this] var valuePrepared = false
  private[this] var hasMoreInput = true

  private var numRow = 0L

  override def hasNext: Boolean = {
    if (!valuePrepared && hasMoreInput) {
      hasMoreInput = reader.next()
      valuePrepared = hasMoreInput
    }
    valuePrepared
  }

  override def next(): T = {
    if (!hasNext) {
      throw QueryExecutionErrors.endOfStreamError()
    }
    if (numRow % CustomMetrics.NUM_ROWS_PER_UPDATE == 0) {
      CustomMetrics.updateMetrics(reader.currentMetricsValues.toImmutableArraySeq, customMetrics)
    }
    numRow += 1
    valuePrepared = false
    reader.get()
  }
}

private class MetricsHandler extends Logging with Serializable {
  private val inputMetrics = TaskContext.get().taskMetrics().inputMetrics
  private val startingBytesRead = inputMetrics.bytesRead
  private val getBytesRead = SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()

  def updateMetrics(numRows: Int, force: Boolean = false): Unit = {
    inputMetrics.incRecordsRead(numRows)
    val shouldUpdateBytesRead =
      inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0
    if (shouldUpdateBytesRead || force) {
      inputMetrics.setBytesRead(startingBytesRead + getBytesRead())
    }
  }
}

private abstract class MetricsIterator[I](iter: Iterator[I]) extends Iterator[I] {
  protected val metricsHandler = new MetricsHandler

  override def hasNext: Boolean = {
    if (iter.hasNext) {
      true
    } else {
      forceUpdateMetrics()
      false
    }
  }

  def forceUpdateMetrics(): Unit = metricsHandler.updateMetrics(0, force = true)
}

private class MetricsRowIterator(
    iter: Iterator[InternalRow]) extends MetricsIterator[InternalRow](iter) {
  override def next(): InternalRow = {
    val item = iter.next()
    metricsHandler.updateMetrics(1)
    item
  }
}

private class MetricsBatchIterator(
    iter: Iterator[ColumnarBatch]) extends MetricsIterator[ColumnarBatch](iter) {
  override def next(): ColumnarBatch = {
    val batch: ColumnarBatch = iter.next()
    metricsHandler.updateMetrics(batch.numRows)
    batch
  }
}
