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
 * step of replicating the checkpointed data in a reliable storage. This is useful for use
 * cases where RDDs build up long lineages that need to be truncated often (e.g. GraphX).
 */
private[spark] class LocalRDDCheckpointData[T: ClassTag](@transient rdd: RDD[T])
  extends RDDCheckpointData[T](rdd) with Logging {

  /**
   * Transform the specified storage level to one that uses disk.
   *
   * This guarantees that the RDD can be recomputed multiple times correctly as long as
   * executors do not fail. Otherwise, if the RDD is cached in memory only, for instance,
   * the checkpoint data will be lost if the relevant block is evicted from memory.
   *
   * This should be called immediately before the first job on the RDD is run.
   */
  def transformStorageLevel(): Unit = {
    rdd.getStorageLevel match {
      case StorageLevel.NONE =>
        // If this RDD is not already marked for caching, persist it on disk
        rdd.persist(StorageLevel.DISK_ONLY)
      case level if level.useOffHeap =>
        // If this RDD is to be cached off-heap, fail fast since we cannot provide any
        // correctness guarantees about subsequent computations after the first one
        throw new SparkException("Local checkpointing is not compatible with off heap caching.")
      case level =>
        // Otherwise, adjust the existing storage level to use disk
        // This guards against potential data losses caused by memory evictions
        rdd.setStorageLevel(StorageLevel(
          useDisk = true, level.useMemory, level.deserialized, level.replication))
    }
    assert(rdd.getStorageLevel.isValid, s"Resulting level is invalid: ${rdd.getStorageLevel}")
  }

  /**
   * Ensure the RDD is fully cached and return a CheckpointRDD that reads from these blocks.
   */
  protected override def doCheckpoint(): CheckpointRDD[T] = {
    val cm = SparkEnv.get.cacheManager
    val bmm = SparkEnv.get.blockManager.master

    // Local checkpointing relies on the fact that all partitions of this RDD are cached.
    // This may not be the case, however, if the job does not fully drain the RDD iterator.
    // For this reason, we must force cache any partitions that are not already cached.
    rdd.partitions.foreach { p =>
      val blockId = RDDBlockId(rdd.id, p.index)
      if (!bmm.contains(blockId)) {
        cm.getOrCompute(rdd, p, TaskContext.empty(), rdd.getStorageLevel)
      }
    }

    new LocalCheckpointRDD[T](rdd)
  }

}
