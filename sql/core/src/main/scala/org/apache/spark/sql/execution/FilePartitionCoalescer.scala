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

package org.apache.spark.sql.execution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}

object FilePartitionCoalescer extends org.apache.spark.internal.Logging {

  /**
   * Coalesce file partitions into `targetPartitions`.
   *
   * - If initialPartitions.size <= targetPartitions: returns initialPartitions (no-op).
   * - If initialPartitions.size > targetPartitions: we recompute partitions using
   *   FilePartition.getFilePartitions(...) with a desired split size that targets targetPartitions.
   * - If after recompute there's skew (heavy partitions), we apply a greedy balancer
   *   to pack files into exactly targetPartitions bins.
   *
   * @param spark SparkSession for configs.
   * @param initialPartitions Partitions produced by FilePartition.getFilePartitions(...)
   * @param allFiles Flattened list of PartitionedFile used to re-compute partitions if needed.
   * @param targetPartitions Desired number of partitions after coalescing.
   */
  def coalesce(spark: SparkSession,
               initialPartitions: Seq[FilePartition],
               allFiles: Seq[PartitionedFile],
               targetPartitions: Int): Seq[FilePartition] = {

    logInfo(s"Coalesce called: initialPartitions=" +
      s"${initialPartitions.size}, target=$targetPartitions")

    // 1) If no reduction requested, return early.
    if (targetPartitions >= initialPartitions.size) {
      logInfo("Target partitions >= current; returning initial partitions unchanged.")
      return initialPartitions
    }

    // 2) Recompute partitions by asking Spark to pack into approx `targetPartitions`:
    val openCost = spark.sessionState.conf.filesOpenCostInBytes
    val totalBytes = allFiles.map(f => f.length + openCost).map(BigDecimal(_)).sum
    val desiredBytes = (totalBytes / BigDecimal(targetPartitions))
      .setScale(0, BigDecimal.RoundingMode.UP)
      .longValue

    logInfo(s"Recomputing file partitions with desiredBytes=" +
      s"$desiredBytes to target ~ $targetPartitions partitions.")
    val recomputed = FilePartition.getFilePartitions(spark, allFiles, desiredBytes)
    logInfo(s"Recomputed partitions count = ${recomputed.length}")

    // 3) If recomputed already produced <= target (rare), use it.
    if (recomputed.length <= targetPartitions) {
      // If it produced fewer than requested, that's fine; return result.
      return recomputed
    }

    // 4) Check skew: if skewed beyond threshold, run greedy balancer into exactly targetPartitions.
    val skewFactor = 2.0
    val sizes = recomputed.map(p => p.files.map(_.length).sum)
    val avg = if (sizes.nonEmpty) sizes.sum / sizes.size else 0L
    val max = if (sizes.nonEmpty) sizes.max else 0L

    logInfo(s"Partition size summary -- count=${sizes.size}, avg=$avg, max=$max")

    val needsBalancing = sizes.nonEmpty && max > avg * skewFactor
    if (!needsBalancing) {
      logInfo("No heavy skew detected; returning recomputed partitions.")
      return recomputed
    }

    logWarning(s"Skew detected (max $max > avg $avg * $skewFactor)." +
      s" Applying greedy balancing into $targetPartitions bins.")

    // Call greedy balancer with explicit target
    GreedyCoalesce.bySize(recomputed.map(_.files.toSeq), targetPartitions)
  }
}

/** Greedy load balancer: LPT / largest-file-first to least-loaded bins */
object GreedyCoalesce {

  def bySize(fileGroups: Seq[Seq[PartitionedFile]], targetPartitions: Int): Seq[FilePartition] = {
    // Flatten and sort descending by file size
    val filesSorted: Seq[(PartitionedFile, Long)] =
      fileGroups.flatten.map(f => (f, f.length)).sortBy(-_._2)

    import scala.collection.mutable
    // Min-heap keyed by current load: dequeue gives least-loaded
    val heap = mutable.PriorityQueue.empty[(Long, Int)](Ordering.by(-_._1))
    (0 until targetPartitions).foreach(i => heap.enqueue((0L, i)))

    val buckets = Array.fill(targetPartitions)(mutable.ArrayBuffer.empty[PartitionedFile])

    filesSorted.foreach { case (file, sz) =>
      val (load, idx) = heap.dequeue()
      buckets(idx) += file
      heap.enqueue((load + sz, idx))
    }

    // convert to FilePartition
    buckets.zipWithIndex.map { case (arrBuf, idx) =>
      FilePartition(idx, arrBuf.toArray)
    }
  }
}
