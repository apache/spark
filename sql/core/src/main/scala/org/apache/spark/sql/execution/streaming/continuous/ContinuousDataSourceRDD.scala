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
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDDPartition, RowToUnsafeDataReader}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousDataReader, PartitionOffset}
import org.apache.spark.util.ThreadUtils

class ContinuousDataSourceRDDPartition(
    val index: Int,
    val readerFactory: DataReaderFactory[UnsafeRow])
  extends Partition with Serializable {

  // This is semantically a lazy val - it's initialized once the first time a call to
  // ContinuousDataSourceRDD.compute() needs to access it, so it can be shared across
  // all compute() calls for a partition. This ensures that one compute() picks up where the
  // previous one ended.
  // We don't make it actually a lazy val because it needs input which isn't available here.
  // This will only be initialized on the executors.
  private[continuous] var queueReader: ContinuousQueuedDataReader = _
}

/**
 * The bottom-most RDD of a continuous processing read task. Wraps a [[ContinuousQueuedDataReader]]
 * to read from the remote source, and polls that queue for incoming rows.
 *
 * Note that continuous processing calls compute() multiple times, and the same
 * [[ContinuousQueuedDataReader]] instance will/must be shared between each call for the same split.
 */
class ContinuousDataSourceRDD(
    sc: SparkContext,
    dataQueueSize: Int,
    epochPollIntervalMs: Long,
    @transient private val readerFactories: Seq[DataReaderFactory[UnsafeRow]])
  extends RDD[UnsafeRow](sc, Nil) {

  override protected def getPartitions: Array[Partition] = {
    readerFactories.zipWithIndex.map {
      case (readerFactory, index) => new ContinuousDataSourceRDDPartition(index, readerFactory)
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[UnsafeRow] = {
    // If attempt number isn't 0, this is a task retry, which we don't support.
    if (context.attemptNumber() != 0) {
      throw new ContinuousTaskRetryException()
    }

    val readerForPartition = {
      val partition = split.asInstanceOf[ContinuousDataSourceRDDPartition]
      if (partition.queueReader == null) {
        partition.queueReader =
          new ContinuousQueuedDataReader(
            partition.readerFactory, context, dataQueueSize, epochPollIntervalMs)
      }

      partition.queueReader
    }

    val coordinatorId = context.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY)
    val epochEndpoint = EpochCoordinatorRef.get(coordinatorId, SparkEnv.get)
    new Iterator[UnsafeRow] {
      private var currentEntry: (UnsafeRow, PartitionOffset) = _

      override def hasNext(): Boolean = {
        currentEntry = readerForPartition.next()

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
    split.asInstanceOf[ContinuousDataSourceRDDPartition].readerFactory.preferredLocations()
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
