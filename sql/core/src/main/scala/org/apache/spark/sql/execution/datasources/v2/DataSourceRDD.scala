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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.metric.{CustomMetrics, SQLMetric}
import org.apache.spark.sql.vectorized.ColumnarBatch

class DataSourceRDDPartition(val index: Int, val inputPartition: Option[InputPartition])
  extends Partition with Serializable

/**
 * Holds all mutable state for a single Spark task reading from a {@link DataSourceRDD}:
 * <ul>
 *   <li>{@code partitionIterators}: all {@link PartitionIterator}s opened so far for this task.
 *       One iterator is appended per {@code compute()} call (one per input partition).</li>
 *   <li>Spark input metrics ({@code recordsRead}, {@code bytesRead}): owned exclusively by this
 *       object so that {@code setBytesRead} -- a set, not an increment -- is called from a single
 *       owner even when multiple iterators are live concurrently.</li>
 *   <li>{@code closedMetrics}: a pre-merged map of custom metrics from readers closed by natural
 *       exhaustion, kept so iterator references can be released as readers finish.</li>
 * </ul>
 *
 * <p><b>When metrics are reported:</b>
 * <ul>
 *   <li><i>Periodically</i>: {@link #updateMetrics} is called on every {@code next()} and
 *       throttles updates to once per {@code UPDATE_INPUT_METRICS_INTERVAL_RECORDS} rows (input
 *       metrics) and {@code NUM_ROWS_PER_UPDATE} rows (custom metrics).</li>
 *   <li><i>On natural exhaustion</i>: when a reader's iterator is fully consumed, {@code hasNext}
 *       calls {@code updateMetrics(0, force=true)} to flush both input and custom metrics
 *       immediately, then closes the reader and drops the reference.</li>
 *   <li><i>At task completion</i>: the task completion listener calls
 *       {@code updateMetrics(0, force=true)} for a final flush, then closes any iterators not
 *       yet exhausted.</li>
 * </ul>
 *
 * <p><b>Why this works across all execution modes:</b>
 * <ul>
 *   <li><i>One partition per task</i>: a single iterator is opened and closed; periodic + final
 *       updates cover the full read.</li>
 *   <li><i>Sequential coalescing ({@link CoalescedRDD})</i>: partitions are read one at a time.
 *       Each reader is naturally exhausted before the next opens, so its final metrics are folded
 *       into {@code closedMetrics} before its reference is released. The merged view in
 *       {@code mergeAndUpdateCustomMetrics} therefore always includes all partitions read so
 *       far.</li>
 *   <li><i>Concurrent k-way merge ({@link SortedMergeCoalescedRDD})</i>: all N iterators are
 *       opened upfront and interleaved on a single thread. All live readers' current metrics are
 *       merged together on each update, so none are lost. When individual readers are exhausted,
 *       their metrics are folded into {@code closedMetrics} for continued accounting.</li>
 * </ul>
 */
private class TaskState(customMetrics: Map[String, SQLMetric]) {
  val partitionIterators = new ArrayBuffer[PartitionIterator[_]]()

  // Input metrics (recordsRead, bytesRead) tracked for this task.
  private val inputMetrics = TaskContext.get().taskMetrics().inputMetrics
  private val startingBytesRead = inputMetrics.bytesRead
  private val getBytesRead = SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()
  private var recordsReadAtLastBytesUpdate = 0L
  private var recordsReadAtLastCustomMetricsUpdate = 0L

  // Pre-merged custom metrics snapshot of all readers closed by natural exhaustion.
  // Maintained as a map (one entry per metric name).
  private val closedMetrics = new HashMap[String, CustomTaskMetric]()

  def updateMetrics(numRows: Int, force: Boolean = false): Unit = {
    inputMetrics.incRecordsRead(numRows)
    val shouldUpdateBytesRead = force ||
      inputMetrics.recordsRead - recordsReadAtLastBytesUpdate >=
        SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS
    if (shouldUpdateBytesRead) {
      recordsReadAtLastBytesUpdate = inputMetrics.recordsRead
      inputMetrics.setBytesRead(startingBytesRead + getBytesRead())
    }
    val shouldUpdateCustomMetrics = force ||
      inputMetrics.recordsRead - recordsReadAtLastCustomMetricsUpdate >=
        CustomMetrics.NUM_ROWS_PER_UPDATE
    if (shouldUpdateCustomMetrics) {
      recordsReadAtLastCustomMetricsUpdate = inputMetrics.recordsRead
      mergeAndUpdateCustomMetrics()
    }
  }

  private def mergeAndUpdateCustomMetrics(): Unit = {
    partitionIterators.filterInPlace { iter =>
      if (iter.isClosed) {
        iter.finalMetrics.foreach { m =>
          closedMetrics.update(m.name(), closedMetrics.get(m.name()).fold(m)(_.mergeWith(m)))
        }
        false
      } else true
    }
    val mergedMetrics = (partitionIterators.flatMap(_.currentMetricsValues) ++ closedMetrics.values)
      .groupMapReduce(_.name())(identity)(_.mergeWith(_))
      .values
      .toSeq
    if (mergedMetrics.nonEmpty) {
      CustomMetrics.updateMetrics(mergedMetrics, customMetrics)
    }
  }
}

// TODO: we should have 2 RDDs: an RDD[InternalRow] for row-based scan, an `RDD[ColumnarBatch]` for
// columnar scan.
/**
 * An RDD that reads data from a V2 data source.
 *
 * This RDD handles both row-based and columnar reads, tracks custom metrics from the data source,
 * and ensures that task completion listeners are added only once per thread to avoid duplicate
 * metric updates and resource cleanup.
 *
 * @param sc The Spark context
 * @param inputPartitions The input partitions to read from
 * @param partitionReaderFactory Factory for creating partition readers
 * @param columnarReads Whether to use columnar reads
 * @param customMetrics Custom metrics defined by the data source
 */
class DataSourceRDD(
    sc: SparkContext,
    @transient private val inputPartitions: Seq[Option[InputPartition]],
    partitionReaderFactory: PartitionReaderFactory,
    columnarReads: Boolean,
    customMetrics: Map[String, SQLMetric])
  extends RDD[InternalRow](sc, Nil) {

  // One TaskState per task attempt.
  @transient private lazy val taskStates = new ConcurrentHashMap[Long, TaskState]()

  override protected def getPartitions: Array[Partition] = {
    inputPartitions.zipWithIndex.map {
      case (inputPartition, index) => new DataSourceRDDPartition(index, inputPartition)
    }.toArray
  }

  private def castPartition(split: Partition): DataSourceRDDPartition = split match {
    case p: DataSourceRDDPartition => p
    case _ => throw QueryExecutionErrors.notADatasourceRDDPartitionError(split)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val taskAttemptId = context.taskAttemptId()

    // Ensure a TaskState exists for this task and register the completion listener on the
    // first compute() call. computeIfAbsent is atomic; same-task calls are always on one
    // thread, so partitionIterators.isEmpty reliably identifies the first call.
    val taskState = taskStates.computeIfAbsent(taskAttemptId, _ => new TaskState(customMetrics))

    if (taskState.partitionIterators.isEmpty) {
      context.addTaskCompletionListener[Unit] { ctx =>
        // In case of early stopping, do a final metrics update and close all readers.
        try {
          val taskState = taskStates.get(ctx.taskAttemptId())
          if (taskState != null) {
            taskState.updateMetrics(0, force = true)
            taskState.partitionIterators.foreach(_.close())
          }
        } finally {
          taskStates.remove(ctx.taskAttemptId())
        }
      }
    }

    castPartition(split).inputPartition.iterator.flatMap { inputPartition =>
      val iter = if (columnarReads) {
        val batchReader = partitionReaderFactory.createColumnarReader(inputPartition)
        new PartitionIterator[ColumnarBatch](batchReader, _.numRows, taskState)
      } else {
        val rowReader = partitionReaderFactory.createReader(inputPartition)
        new PartitionIterator[InternalRow](rowReader, _ => 1, taskState)
      }

      // Track this iterator; early-stop close and final metrics-flush for iterators not yet
      // naturally exhausted are handled by the task completion listener. This avoids closing
      // live iterators prematurely in the concurrent k-way merge (SortedMergeCoalescedRDD).
      taskState.partitionIterators += iter

      // TODO: SPARK-25083 remove the type erasure hack in data source scan
      new InterruptibleIterator(context, iter.asInstanceOf[Iterator[InternalRow]])
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    castPartition(split).inputPartition.toSeq.flatMap(_.preferredLocations())
  }
}

private class PartitionIterator[T](
    private var reader: PartitionReader[T],
    rowCount: T => Int,
    taskState: TaskState) extends Iterator[T] {
  private var valuePrepared = false
  private var hasMoreInput = true

  // Cached final metrics snapshot, captured just before the reader is closed on natural
  // exhaustion. Allows mergeAndUpdateCustomMetrics() to include this reader's contribution
  // after close() has been called and currentMetricsValues() is no longer valid.
  private var cachedFinalMetrics: Array[CustomTaskMetric] = Array.empty

  def isClosed: Boolean = reader == null

  def finalMetrics: Array[CustomTaskMetric] = cachedFinalMetrics

  def close(): Unit = if (reader != null) {
    reader.close()
    reader = null
  }

  def currentMetricsValues: Array[CustomTaskMetric] = {
    require(!isClosed, "currentMetricsValues called on a closed PartitionIterator")
    reader.currentMetricsValues
  }

  override def hasNext: Boolean = {
    if (!valuePrepared && hasMoreInput) {
      hasMoreInput = reader.next()
      if (!hasMoreInput) {
        cachedFinalMetrics = reader.currentMetricsValues
        taskState.updateMetrics(0, force = true)
        close()
      }
      valuePrepared = hasMoreInput
    }
    valuePrepared
  }

  override def next(): T = {
    if (!hasNext) {
      throw QueryExecutionErrors.endOfStreamError()
    }
    valuePrepared = false
    val result = reader.get()
    taskState.updateMetrics(rowCount(result))
    result
  }
}
