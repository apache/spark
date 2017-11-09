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
import org.apache.spark.storage.{RDDPartitionMetadataBlockId, StorageLevel}

class CachedColumnarRDD(
    @transient private var _sc: SparkContext,
    private var dataRDD: RDD[CachedBatch],
    containsPartitionMetadata: Boolean,
    expectedStorageLevel: StorageLevel)
  extends RDD[CachedBatch](_sc, Seq(new OneToOneDependency(dataRDD))) {

  override def compute(split: Partition, context: TaskContext): Iterator[CachedBatch] = {
    if (containsPartitionMetadata) {
      val parentIterator = dataRDD.iterator(split, context)
      if (!parentIterator.hasNext) {
        Iterator()
      } else {
        val cachedBatch = parentIterator.next()
        SparkEnv.get.blockManager.putSingle(RDDPartitionMetadataBlockId(id, split.index),
          cachedBatch.stats, expectedStorageLevel)
        Iterator(cachedBatch)
      }
    } else {
      firstParent.iterator(split, context)
    }
  }

  override protected def getPartitions: Array[Partition] = dataRDD.partitions

  override private[spark] def getOrCompute(split: Partition, context: TaskContext):
      Iterator[CachedBatch] = {
    val metadataBlockId = RDDPartitionMetadataBlockId(id, split.index)
    val superGetOrCompute: (Partition, TaskContext) => Iterator[CachedBatch] = super.getOrCompute
    SparkEnv.get.blockManager.getSingle[InternalRow](metadataBlockId).map(metadataBlock =>
      new CachedColumnarPartitionIterator(metadataBlock, context, superGetOrCompute(split, context))
    ).getOrElse(superGetOrCompute(split, context))
  }
}

private[columnar] class CachedColumnarPartitionIterator(
    val partitionStats: InternalRow,
    context: TaskContext,
    delegate: Iterator[CachedBatch])
  extends InterruptibleIterator[CachedBatch](context, delegate) {
  override def next(): CachedBatch = {\
    // scalastyle:off
    println("next")
    // scalastyle:on
    super.next()
  }
}
