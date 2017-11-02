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

package org.apache.spark.sql.execution.columnar

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.storage.{RDDBlockId, RDDPartitionMetadataBlockId}

class CachedColumnarRDD(
    @transient private var _sc: SparkContext,
    private var dataRDD: RDD[CachedBatch],
    containsPartitionMetadata: Boolean)
  extends RDD[AnyRef](_sc, Seq(new OneToOneDependency(dataRDD))) {

  override def compute(split: Partition, context: TaskContext): Iterator[AnyRef] = {
    if (containsPartitionMetadata) {
      val parentIterator = dataRDD.iterator(split, context)
      if (!parentIterator.hasNext) {
        Iterator()
      } else {
        val cachedBatch = parentIterator.next()
        SparkEnv.get.blockManager.putSingle(RDDPartitionMetadataBlockId(id, split.index),
          cachedBatch.stats, dataRDD.getStorageLevel)
        Iterator(cachedBatch)
      }
    } else {
      firstParent.iterator(split, context)
    }
  }

  override protected def getPartitions: Array[Partition] = dataRDD.partitions

  private def fetchOrComputeCachedBatch(partition: Partition, context: TaskContext):
      Iterator[CachedBatch] = {
    // metadata block can be evicted by BlockManagers but we may still keep CachedBatch in memory
    // so that we still need to try to fetch it from Cache
    val blockId = RDDBlockId(id, partition.index)
    SparkEnv.get.blockManager.getOrElseUpdate(blockId, getStorageLevel, elementClassTag, () => {
      computeOrReadCheckpoint(partition, context)
    }) match {
      case Left(blockResult) =>
        val existingMetrics = context.taskMetrics().inputMetrics
        existingMetrics.incBytesRead(blockResult.bytes)
        new InterruptibleIterator[CachedBatch](context,
          blockResult.data.asInstanceOf[Iterator[CachedBatch]]) {
          override def next(): CachedBatch = {
            existingMetrics.incRecordsRead(1)
            delegate.next()
          }
        }
      case Right(iter) =>
        new InterruptibleIterator(context, iter.asInstanceOf[Iterator[CachedBatch]])
    }
  }

  override private[spark] def getOrCompute(split: Partition, context: TaskContext):
      Iterator[AnyRef] = {
    val metadataBlockId = RDDPartitionMetadataBlockId(id, split.index)
    val metadataBlockOpt = SparkEnv.get.blockManager.get[InternalRow](metadataBlockId)
    if (metadataBlockOpt.isDefined) {
      val metadataBlock = metadataBlockOpt.get
      new InterruptibleIterator[AnyRef](context, new Iterator[AnyRef] {

        private var fetchingFirstElement = true

        private var delegate: Iterator[CachedBatch] = _

        override def hasNext: Boolean = {
          if (fetchingFirstElement) {
            true
          } else {
            delegate = fetchOrComputeCachedBatch(split, context)
            delegate.hasNext
          }
        }

        override def next(): AnyRef = {
          if (fetchingFirstElement) {
            fetchingFirstElement = false
            val mb = metadataBlock.data.next()
            mb.asInstanceOf[InternalRow]
          } else {
            delegate.next()
          }
        }
      })
    } else {
      fetchOrComputeCachedBatch(split, context)
    }

  }
}
