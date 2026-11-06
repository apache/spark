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

import org.apache.spark.sql.connector.read.{HasPartitionStatistics, InputPartition}

/**
 * Immutable sizing policy captured when a scan is prepared, before runtime filtering. It holds the
 * configured values only. A default that depends on the cluster, such as the minimum partition
 * count, is resolved when the scan executes, so equal scans planned at different times stay equal.
 */
case class V2ScanPartitionPacking(
    maxPartitionBytes: Long,
    openCostInBytes: Long,
    minPartitionNum: Option[Int],
    maxPartitionNum: Option[Int]) {

  /**
   * Group adjacent inputs by estimated size, keeping an oversized input on its own. If any input
   * lacks a valid size, the whole scan is grouped by count into at most `maxPartitionNum`
   * balanced ranges when that cap is set. Returns None when every input stays a partition of its
   * own, without building a group for each.
   */
  def pack(partitions: Seq[InputPartition], defaultMinPartitionNum: => Int): Option[Seq[Range]] = {
    val numInputs = partitions.length
    // Unboxed, allocated at the first valid size and read only while every input reports one.
    var costs: Array[Long] = null
    var total = 0L
    var sized = true
    val inputs = partitions.iterator
    var index = 0
    while (sized && inputs.hasNext) {
      inputs.next() match {
        case p: HasPartitionStatistics =>
          val bytes = p.sizeInBytes()
          sized = bytes.isPresent && bytes.getAsLong >= 0
          if (sized) {
            if (costs == null) costs = new Array[Long](numInputs)
            costs(index) = add(bytes.getAsLong, openCostInBytes)
            total = add(total, costs(index))
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
      bySize(costs, total, minPartitionNum.getOrElse(defaultMinPartitionNum))
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

  // Records where each group starts, and builds the groups only if some of them merge.
  private def bySize(costs: Array[Long], total: Long, minGroups: Int): Option[Seq[Range]] = {
    val bytesPerTask = total / minGroups + (if (total % minGroups == 0) 0 else 1)
    val target = math.min(maxPartitionBytes, math.max(openCostInBytes, bytesPerTask))
    val starts = new mutable.ArrayBuilder.ofInt
    var start = 0
    var currentBytes = 0L
    costs.indices.foreach { index =>
      val size = costs(index)
      if (index > start && (size > target || currentBytes > target - size)) {
        starts.addOne(start)
        start = index
        currentBytes = 0L
      }
      currentBytes = add(currentBytes, size)
      if (size >= target && size > 0) {
        starts.addOne(start)
        start = index + 1
        currentBytes = 0L
      }
    }
    if (start < costs.length) starts.addOne(start)
    val groupStarts = starts.result()
    Option.when(groupStarts.length < costs.length) {
      groupStarts.indices.map { group =>
        val end = if (group + 1 < groupStarts.length) groupStarts(group + 1) else costs.length
        groupStarts(group) until end
      }
    }
  }

  private def add(left: Long, right: Long): Long =
    if (left > Long.MaxValue - right) Long.MaxValue else left + right
}
