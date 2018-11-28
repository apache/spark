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

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousPartitionReaderFactory
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.NextIterator

class ContinuousDataSourceRDDPartition(
    val index: Int,
    val inputPartition: InputPartition)
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
    private val inputPartitions: Seq[InputPartition],
    schema: StructType,
    partitionReaderFactory: ContinuousPartitionReaderFactory)
  extends RDD[InternalRow](sc, Nil) {

  override protected def getPartitions: Array[Partition] = {
    inputPartitions.zipWithIndex.map {
      case (inputPartition, index) => new ContinuousDataSourceRDDPartition(index, inputPartition)
    }.toArray
  }

  private def castPartition(split: Partition): ContinuousDataSourceRDDPartition = split match {
    case p: ContinuousDataSourceRDDPartition => p
    case _ => throw new SparkException(s"[BUG] Not a ContinuousDataSourceRDDPartition: $split")
  }

  /**
   * Initialize the shared reader for this partition if needed, then read rows from it until
   * it returns null to signal the end of the epoch.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    // If attempt number isn't 0, this is a task retry, which we don't support.
    if (context.attemptNumber() != 0) {
      throw new ContinuousTaskRetryException()
    }

    val readerForPartition = {
      val partition = castPartition(split)
      if (partition.queueReader == null) {
        val partitionReader = partitionReaderFactory.createReader(
          partition.inputPartition)
        partition.queueReader = new ContinuousQueuedDataReader(
          partition.index, partitionReader, schema, context, dataQueueSize, epochPollIntervalMs)
      }

      partition.queueReader
    }

    new NextIterator[InternalRow] {
      override def getNext(): InternalRow = {
        readerForPartition.next() match {
          case null =>
            finished = true
            null
          case row => row
        }
      }

      override def close(): Unit = {}
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    castPartition(split).inputPartition.preferredLocations()
  }
}
