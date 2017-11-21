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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.storage.{RDDPartitionMetadataBlockId, StorageLevel}

private[columnar] class CachedColumnarRDD(
    @transient private var _sc: SparkContext,
    private var dataRDD: RDD[CachedBatch],
    containsPartitionMetadata: Boolean,
    expectedStorageLevel: StorageLevel)
  extends RDD[CachedBatch](_sc, Seq(new OneToOneDependency(dataRDD))) {

  override def compute(split: Partition, context: TaskContext): Iterator[CachedBatch] = {
    firstParent.iterator(split, context)
  }

  override protected def getPartitions: Array[Partition] = dataRDD.partitions

  override private[spark] def getOrCompute(split: Partition, context: TaskContext):
      Iterator[CachedBatch] = {
    val metadataBlockId = RDDPartitionMetadataBlockId(id, split.index)
    val superGetOrCompute: (Partition, TaskContext) => Iterator[CachedBatch] = super.getOrCompute
    SparkEnv.get.blockManager.getSingle[InternalRow](metadataBlockId).map(metadataBlock =>
      new InterruptibleIterator[CachedBatch](context,
        new CachedColumnarIterator(metadataBlock, split, context, superGetOrCompute))
    ).getOrElse {
      val batchIter = superGetOrCompute(split, context)
      if (containsPartitionMetadata && getStorageLevel != StorageLevel.NONE && batchIter.hasNext) {
        val cachedBatch = batchIter.next()
        SparkEnv.get.blockManager.putSingle(metadataBlockId, cachedBatch.stats,
          expectedStorageLevel)
        new InterruptibleIterator[CachedBatch](context, Iterator(cachedBatch))
      } else {
        batchIter
      }
    }
  }
}

private[columnar] object CachedColumnarRDD {

  private val rddIdToMetadata = new ConcurrentHashMap[Int, Seq[InternalRow]]()

  def collectStats(rdd: RDD[CachedBatch]): Unit = {
    val metadataBlocks = rdd.partitions.indices.map {
      partitionId => SparkEnv.get.blockManager.getSingle[InternalRow](
        RDDPartitionMetadataBlockId(rdd.id, partitionId)).get
    }
    rddIdToMetadata.put(rdd.id, metadataBlocks)
  }

  def fetchMetadataForRDD(rddId: Int): Option[Seq[InternalRow]] = rddIdToMetadata.asScala.get(rddId)
}

private[columnar] class CachedColumnarIterator(
    val partitionStats: InternalRow,
    partition: Partition,
    context: TaskContext,
    fetchRDDPartition: (Partition, TaskContext) => Iterator[CachedBatch])
  extends Iterator[CachedBatch] {

  private var delegate: Iterator[CachedBatch] = _

  override def hasNext: Boolean = {
    if (delegate == null) {
      delegate = fetchRDDPartition(partition, context)
    }
    delegate.hasNext
  }

  override def next(): CachedBatch = {
    delegate.next()
  }
}
