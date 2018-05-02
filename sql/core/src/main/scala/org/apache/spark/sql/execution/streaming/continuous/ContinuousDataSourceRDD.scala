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

package org.apache.spark.sql.execution.streaming.continuous

import java.util.concurrent.TimeUnit
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDDPartition, RowToUnsafeDataReader}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousDataReader, PartitionOffset}
import org.apache.spark.util.ThreadUtils

/**
 * The bottom-most RDD of a continuous processing read task. Wraps a [[ContinuousQueuedDataReader]]
 * to read from the remote source, and polls that queue for incoming rows.
 *
 * Note that continuous processing calls compute() multiple times, and the same
 * [[ContinuousQueuedDataReader]] instance will/must be shared between each call for the same split.
 */
class ContinuousDataSourceRDD(
    sc: SparkContext,
    sqlContext: SQLContext,
    @transient private val readerFactories: Seq[DataReaderFactory[UnsafeRow]])
  extends RDD[UnsafeRow](sc, Nil) {

  private val dataQueueSize = sqlContext.conf.continuousStreamingExecutorQueueSize
  private val epochPollIntervalMs = sqlContext.conf.continuousStreamingExecutorPollIntervalMs

  // When computing the same partition multiple times, we need to use the same data reader to
  // do so for continuity in offsets.
  @GuardedBy("dataReaders")
  private val dataReaders: mutable.Map[Partition, ContinuousQueuedDataReader] =
    mutable.Map[Partition, ContinuousQueuedDataReader]()

  override protected def getPartitions: Array[Partition] = {
    readerFactories.zipWithIndex.map {
      case (readerFactory, index) => new DataSourceRDDPartition(index, readerFactory)
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[UnsafeRow] = {
    // If attempt number isn't 0, this is a task retry, which we don't support.
    if (context.attemptNumber() != 0) {
      throw new ContinuousTaskRetryException()
    }

    val readerForPartition = dataReaders.synchronized {
      if (!dataReaders.contains(split)) {
        dataReaders.put(
          split,
          new ContinuousQueuedDataReader(split, context, dataQueueSize, epochPollIntervalMs))
      }

      dataReaders(split)
    }

    val coordinatorId = context.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY)
    val epochEndpoint = EpochCoordinatorRef.get(coordinatorId, SparkEnv.get)
    new Iterator[UnsafeRow] {
      private val POLL_TIMEOUT_MS = 1000

      private var currentEntry: (UnsafeRow, PartitionOffset) = _

      override def hasNext(): Boolean = {
        while (currentEntry == null) {
          if (context.isInterrupted() || context.isCompleted()) {
            // Force the epoch to end here. The writer will notice the context is interrupted
            // or completed and not start a new one. This makes it possible to achieve clean
            // shutdown of the streaming query.
            // TODO: The obvious generalization of this logic to multiple stages won't work. It's
            // invalid to send an epoch marker from the bottom of a task if all its child tasks
            // haven't sent one.
            currentEntry = (null, null)
          } else {
            if (readerForPartition.dataReaderFailed.get()) {
              throw new SparkException(
                "data read failed", readerForPartition.dataReaderThread.failureReason)
            }
            if (readerForPartition.epochPollFailed.get()) {
              throw new SparkException(
                "epoch poll failed", readerForPartition.epochPollRunnable.failureReason)
            }
            currentEntry = readerForPartition.queue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS)
          }
        }

        currentEntry match {
          // epoch boundary marker
          case (null, null) =>
            epochEndpoint.send(ReportPartitionOffset(
              context.partitionId(),
              readerForPartition.currentEpoch,
              readerForPartition.currentOffset))
            readerForPartition.currentEpoch += 1
            currentEntry = null
            false
          // real row
          case (_, offset) =>
            readerForPartition.currentOffset = offset
            true
        }
      }

      override def next(): UnsafeRow = {
        if (currentEntry == null) throw new NoSuchElementException("No current row was set")
        val r = currentEntry._1
        currentEntry = null
        r
      }
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[DataSourceRDDPartition[UnsafeRow]].readerFactory.preferredLocations()
  }
}

object ContinuousDataSourceRDD {
  private[continuous] def getBaseReader(reader: DataReader[UnsafeRow]): ContinuousDataReader[_] = {
    reader match {
      case r: ContinuousDataReader[UnsafeRow] => r
      case wrapped: RowToUnsafeDataReader =>
        wrapped.rowReader.asInstanceOf[ContinuousDataReader[Row]]
      case _ =>
        throw new IllegalStateException(s"Unknown continuous reader type ${reader.getClass}")
    }
  }
}
