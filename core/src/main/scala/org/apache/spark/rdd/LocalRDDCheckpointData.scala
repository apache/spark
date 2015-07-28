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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Logging, SparkEnv, SparkException, TaskContext}
import org.apache.spark.storage.{RDDBlockId, StorageLevel}

/**
 * An implementation of checkpointing implemented on top of Spark's caching layer.
 *
 * Local checkpointing trades off fault tolerance for performance by skipping the expensive
 * step of saving the RDD data to a reliable and fault-tolerant storage. Instead, the data
 * is written to the local, ephemeral block storage that lives in each executor. This is useful
 * for use cases where RDDs build up long lineages that need to be truncated often (e.g. GraphX).
 */
private[spark] class LocalRDDCheckpointData[T: ClassTag](@transient rdd: RDD[T])
  extends RDDCheckpointData[T](rdd) with Logging {

  /**
   * Ensure the RDD is fully cached so the partitions can be recovered later.
   */
  protected override def doCheckpoint(): CheckpointRDD[T] = {
    val cm = SparkEnv.get.cacheManager
    val bmm = SparkEnv.get.blockManager.master
    val level = rdd.getStorageLevel

    // Assume storage level uses disk; otherwise memory eviction may cause data loss
    assume(level.useDisk, s"Storage level $level is not appropriate for local checkpointing")

    // Not all RDD actions compute all partitions of the RDD (e.g. take)
    // We must compute and cache any missing partitions for correctness reasons
    rdd.partitions.foreach { p =>
      val blockId = RDDBlockId(rdd.id, p.index)
      if (!bmm.contains(blockId)) {
        cm.getOrCompute(rdd, p, TaskContext.empty(), level)
      }
    }

    new LocalCheckpointRDD[T](rdd)
  }

}

private[spark] object LocalRDDCheckpointData {

  val DEFAULT_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK

  /**
   * Transform the specified storage level to one that uses disk.
   *
   * This guarantees that the RDD can be recomputed multiple times correctly as long as
   * executors do not fail. Otherwise, if the RDD is cached in memory only, for instance,
   * the checkpoint data will be lost if the relevant block is evicted from memory.
   *
   * This method is idempotent.
   */
  def transformStorageLevel(level: StorageLevel): StorageLevel = {
    // If this RDD is to be cached off-heap, fail fast since we cannot provide any
    // correctness guarantees about subsequent computations after the first one
    if (level.useOffHeap) {
      throw new SparkException("Local checkpointing is not compatible with off-heap caching.")
    }

    StorageLevel(useDisk = true, level.useMemory, level.deserialized, level.replication)
  }
}
