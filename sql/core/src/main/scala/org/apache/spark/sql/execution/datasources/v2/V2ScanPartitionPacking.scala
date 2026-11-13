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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{CONFIG, DESIRED_NUM_PARTITIONS, MAX_NUM_PARTITIONS, NUM_PARTITIONS}
import org.apache.spark.sql.connector.read.{HasPartitionStatistics, InputPartition}
import org.apache.spark.sql.internal.SQLConf

/**
 * Immutable sizing policy that `PackV2ScanPartitions` reads from the session configuration when it
 * plans packed shuffles, before runtime filtering. It holds the configured values only. A default
 * that depends on the cluster, such as the minimum partition count, is resolved when
 * `PackScanPartitionsExec` executes, so equal nodes planned at different times stay equal for
 * exchange and stage reuse.
 */
case class V2ScanPartitionPacking(
    maxPartitionBytes: Long,
    openCostInBytes: Long,
    minPartitionNum: Option[Int],
    maxPartitionNum: Option[Int]) extends Logging {

  /**
   * Group adjacent inputs by estimated size as `FilePartition` groups files, but in the scan's
   * order and never splitting an input. As with
   * `spark.sql.files.maxPartitionNum`, when that gives more groups than `maxPartitionNum`, the
   * groups are sized by the cap instead. If any input lacks a valid size, the whole scan is grouped
   * by count into at most `maxPartitionNum` balanced ranges when that cap is set. Returns None when
   * every input stays a partition of its own, without building a group for each.
   */
  def pack(partitions: Seq[InputPartition], defaultMinPartitionNum: => Int): Option[Seq[Range]] =
    pack(partitions.length, partitions.iterator, defaultMinPartitionNum)

  private def pack(
      numInputs: Int,
      inputs: Iterator[InputPartition],
      defaultMinPartitionNum: => Int): Option[Seq[Range]] = {
    // Unboxed, allocated at the first valid size and read only while every input reports one.
    var sizes: Array[Long] = null
    var total = 0L
    var sized = true
    var index = 0
    while (sized && inputs.hasNext) {
      inputs.next() match {
        case p: HasPartitionStatistics =>
          val bytes = p.sizeInBytes()
          sized = bytes.isPresent && bytes.getAsLong >= 0
          if (sized) {
            if (sizes == null) sizes = new Array[Long](numInputs)
            sizes(index) = bytes.getAsLong
            total = add(total, add(sizes(index), openCostInBytes))
          }
        case _ => sized = false
      }
      index += 1
    }
    if (!sized) {
      // A cap of at least the number of inputs leaves each of them on its own.
      maxPartitionNum.filter(_ < numInputs).map(byCount(numInputs, _))
    } else if (numInputs == 0) {
      None
    } else {
      val minGroups = minPartitionNum.getOrElse(defaultMinPartitionNum)
      // As FilePartition.maxSplitBytes.
      val groups = bySize(sizes,
        math.min(maxPartitionBytes, math.max(openCostInBytes, total / minGroups)))
      // As spark.sql.files.maxPartitionNum: above the cap, size the groups by the cap instead.
      val numGroups = groups.fold(numInputs)(_.size)
      maxPartitionNum.filter(numGroups > _) match {
        case Some(cap) =>
          val capped = bySize(sizes, ceilDiv(total, cap))
          logWarning(log"The number of partitions is ${MDC(NUM_PARTITIONS, numGroups)}, " +
            log"which exceeds the maximum number configured: ${MDC(MAX_NUM_PARTITIONS, cap)}. " +
            log"Spark rescales it to " +
            log"${MDC(DESIRED_NUM_PARTITIONS, capped.fold(numInputs)(_.size))} by ignoring " +
            log"the configuration of " +
            log"${MDC(CONFIG, SQLConf.V2_SCAN_PARTITION_PACKING_MAX_PARTITION_BYTES.key)}.")
          capped
        case None => groups
      }
    }
  }

  /**
   * Packs a scan's RDD partitions, given the input each one reads after runtime filtering (None
   * where runtime filtering emptied it), into consecutive ranges of partitions. The inputs are
   * packed by `pack`, and an empty partition joins the range of the input before it, or the first
   * range, even when `pack` leaves the inputs apart. So every partition is still computed once, as
   * without packing. Returns None when every partition stays on its own.
   */
  def packSlots(
      slots: IndexedSeq[Option[InputPartition]],
      defaultMinPartitionNum: => Int): Option[Seq[Range]] = {
    val numInputs = slots.count(_.isDefined)
    if (numInputs == slots.length) {
      // No partition is empty, so the partitions are the inputs, packed without collecting them.
      pack(numInputs, slots.iterator.map(_.get), defaultMinPartitionNum)
    } else {
      val inputStarts = pack(numInputs, slots.iterator.collect { case Some(p) => p },
        defaultMinPartitionNum).map(_.map(_.start)).getOrElse(0 until numInputs)
      // A range starts at the partition of each group's first input, except that the first range
      // starts at 0, so with no input left one range holds every partition.
      val starts = new mutable.ArrayBuilder.ofInt
      starts.addOne(0)
      var input = 0
      slots.indices.foreach { slot =>
        if (slots(slot).isDefined) {
          if (starts.length < inputStarts.length && input == inputStarts(starts.length)) {
            starts.addOne(slot)
          }
          input += 1
        }
      }
      val groupStarts = starts.result()
      Option.when(groupStarts.length < slots.length) {
        groupStarts.indices.map { group =>
          val end = if (group + 1 < groupStarts.length) groupStarts(group + 1) else slots.length
          groupStarts(group) until end
        }
      }
    }
  }

  // Balanced consecutive ranges, without integer overflow or empty groups.
  private def byCount(numInputs: Int, maxGroups: Int): Seq[Range] = {
    (0 until maxGroups).map { index =>
      val start = (index.toLong * numInputs / maxGroups).toInt
      val end = ((index.toLong + 1) * numInputs / maxGroups).toInt
      start until end
    }
  }

  private def ceilDiv(total: Long, n: Int): Long = total / n + (if (total % n == 0) 0 else 1)

  // Next fit, as FilePartition.getFilePartitions: an input starts a new group when its size would
  // take the current group past the target, and then adds its size and opening cost to the group.
  // Records where each group starts, and builds the groups only if some of them merge.
  private def bySize(sizes: Array[Long], target: Long): Option[Seq[Range]] = {
    val starts = new mutable.ArrayBuilder.ofInt
    var start = 0
    var currentBytes = 0L
    sizes.indices.foreach { index =>
      if (index > start && currentBytes > target - sizes(index)) {
        starts.addOne(start)
        start = index
        currentBytes = 0L
      }
      currentBytes = add(currentBytes, add(sizes(index), openCostInBytes))
    }
    if (start < sizes.length) starts.addOne(start)
    val groupStarts = starts.result()
    Option.when(groupStarts.length < sizes.length) {
      groupStarts.indices.map { group =>
        val end = if (group + 1 < groupStarts.length) groupStarts(group + 1) else sizes.length
        groupStarts(group) until end
      }
    }
  }

  private def add(left: Long, right: Long): Long =
    if (left > Long.MaxValue - right) Long.MaxValue else left + right
}
