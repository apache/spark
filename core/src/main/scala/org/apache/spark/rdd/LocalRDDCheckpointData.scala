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

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * An implementation of checkpointing implemented on top of Spark's caching layer.
 *
 * Local checkpointing trades off fault tolerance for performance by skipping the expensive
 * step of saving the RDD data to a reliable and fault-tolerant storage. Instead, the data
 * is written to the local, ephemeral block storage that lives in each executor. This is useful
 * for use cases where RDDs build up long lineages that need to be truncated often (e.g. GraphX).
 */
private[spark] class LocalRDDCheckpointData[T: ClassTag](@transient private val rdd: RDD[T])
  extends RDDCheckpointData[T](rdd) with Logging {

  /**
   * Ensure the RDD is fully cached so the partitions can be recovered later.
   */
  protected override def doCheckpoint(): CheckpointRDD[T] = {
    val level = rdd.getStorageLevel

    // Assume storage level uses disk; otherwise memory eviction may cause data loss
    assume(level.useDisk, s"Storage level $level is not appropriate for local checkpointing")

    // Not all actions compute all partitions of the RDD (e.g. take). For correctness, we
    // must cache any missing partitions. TODO: avoid running another job here (SPARK-8582).
    val action = (tc: TaskContext, iterator: Iterator[T]) => Utils.getIteratorSize(iterator)
    val missingPartitionIndices = rdd.partitions.map(_.index).filter { i =>
      !SparkEnv.get.blockManager.master.contains(RDDBlockId(rdd.id, i))
    }
    if (missingPartitionIndices.nonEmpty) {
      rdd.sparkContext.runJob(rdd, action, missingPartitionIndices.toImmutableArraySeq)
    }

    // The checkpoint is not yet exposed to readers (markCheckpointed runs after doCheckpoint
    // returns), so this is the point to finalize one consistent version per partition. If
    // verification was enabled for this RDD, seal it at the master.
    sealCheckpointIfEnabled()

    new LocalCheckpointRDD[T](rdd)
  }

  /**
   * Seal this RDD's checkpoint blocks so every later read sees a single consistent version, even
   * if Spark non-determinism plus speculation or stage retries materialized a partition into more
   * than one divergent copy. The master keeps the plurality checksum per partition, evicts the
   * divergent copies, and rejects future divergent registrations; reads then self-check against the
   * sealed checksum. No-op unless this RDD was marked for verification at `localCheckpoint`.
   *
   * Relies on the per-replica checksums recorded by BlockManager at store time (see
   * `SerializerManager.wrapForChecksum`).
   */
  private def sealCheckpointIfEnabled(): Unit = {
    if (rdd.verifySealedChecksum) {
      val unverified = SparkEnv.get.blockManager.master.sealRddChecksums(rdd.id)
      if (unverified > 0) {
        // Content checksums are computed only for serialized blocks materialized after this RDD was
        // marked. A non-zero count means some materialized partitions could not be checked: either
        // the storage level is deserialized (only on-disk blocks are serialized + checksummed), or
        // they were cached before `localCheckpoint` marked the RDD.
        if (rdd.getStorageLevel.deserialized) {
          logWarning(log"Local checkpoint content verification is enabled for RDD " +
            log"${MDC(RDD_ID, rdd.id)} but its storage level is deserialized, so " +
            log"${MDC(NUM_PARTITIONS, unverified)} in-memory partition(s) were left unverified " +
            log"(only on-disk, serialized blocks are checksummed).")
        } else {
          logWarning(log"Local checkpoint content verification is enabled for RDD " +
            log"${MDC(RDD_ID, rdd.id)} but ${MDC(NUM_PARTITIONS, unverified)} partition(s) were " +
            log"materialized before localCheckpoint() and were left unverified.")
        }
      }
    }
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
    StorageLevel(useDisk = true, level.useMemory, level.deserialized, level.replication)
  }
}
