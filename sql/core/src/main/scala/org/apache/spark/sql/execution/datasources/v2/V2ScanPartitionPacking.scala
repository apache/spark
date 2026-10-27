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

import org.apache.spark.rdd.{PartitionCoalescer, PartitionGroup, RDD}
import org.apache.spark.sql.catalyst.plans.physical.KeyedPartitioning
import org.apache.spark.sql.connector.read.{Batch, HasPartitionStatistics, InputPartition, PartitionReaderFactory, Scan}

/** Immutable sizing policy captured when a scan is prepared, before runtime filtering. */
case class V2ScanPartitionPacking(
    maxPartitionBytes: Long,
    openCostInBytes: Long,
    minPartitionNum: Int,
    maxPartitionNum: Option[Int]) {

  /**
   * Group adjacent inputs by estimated size, keeping an oversized input on its own. Missing,
   * negative or mixed statistics use the explicit count cap, or leave inputs unchanged.
   * The cap is only a fallback: it never overrides the size limit for inputs reporting bytes.
   * Empty runtime-pruned slots are retained and contribute no bytes or opening cost.
   */
  def pack(partitions: Seq[Option[InputPartition]]): Seq[Seq[Int]] = {
    if (partitions.isEmpty) return Seq.empty
    val costs = partitions.map {
      case None => Some(0L)
      case Some(p: HasPartitionStatistics) =>
        val bytes = p.sizeInBytes()
        Option.when(bytes.isPresent && bytes.getAsLong >= 0)(
          add(bytes.getAsLong, openCostInBytes))
      case _ => None
    }
    if (costs.exists(_.isEmpty)) {
      val count = maxPartitionNum.getOrElse(partitions.size).min(partitions.size)
      // Balanced consecutive ranges, without integer overflow or empty groups.
      (0 until count).map { index =>
        val start = (index.toLong * partitions.size / count).toInt
        val end = ((index.toLong + 1) * partitions.size / count).toInt
        start until end
      }
    } else {
      val sizes = costs.map(_.get)
      val total = sizes.foldLeft(0L)(add)
      val bytesPerTask = total / minPartitionNum + (if (total % minPartitionNum == 0) 0 else 1)
      val target = math.min(maxPartitionBytes, math.max(openCostInBytes, bytesPerTask))
      val groups = mutable.ArrayBuffer.empty[Seq[Int]]
      val current = mutable.ArrayBuffer.empty[Int]
      var currentBytes = 0L
      def finish(): Unit = {
        if (current.nonEmpty) {
          groups += current.toVector
          current.clear()
          currentBytes = 0L
        }
      }
      sizes.zipWithIndex.foreach { case (size, index) =>
        if (current.nonEmpty && (size > target || currentBytes > target - size)) finish()
        current += index
        currentBytes = add(currentBytes, size)
        if (size >= target && size > 0) finish()
      }
      finish()
      groups.toSeq
    }
  }

  private def add(left: Long, right: Long): Long =
    if (left > Long.MaxValue - right) Long.MaxValue else left + right
}

/**
 * Driver-side snapshot of a scan that has already planned its inputs. Adding task sizing reuses
 * this metadata and factory. Only the sizing policy contributes to scan equality and display.
 */
class V2ScanPartitionPackingInfo(
    val options: V2ScanPartitionPacking,
    @transient val scan: Scan,
    @transient val batch: Batch,
    @transient val inputPartitions: Seq[InputPartition],
    @transient val readerFactory: PartitionReaderFactory,
    @transient val keyedPartitioning: Option[KeyedPartitioning]) extends Serializable {
  override def toString: String = options.toString
}

/** Use the size planner's groups rather than the RDD coalescer's count-based grouping. */
private[v2] class V2ScanPartitionCoalescer(@transient groups: Seq[Seq[Int]])
    extends PartitionCoalescer with Serializable {
  override def coalesce(maxPartitions: Int, parent: RDD[_]): Array[PartitionGroup] = {
    require(groups.size == maxPartitions)
    val assigned = mutable.Map.empty[String, Int].withDefaultValue(0)
    groups.map { indices =>
      // Prefer the host covering the most splits. Balance equal-coverage replicas across
      // groups rather than routing every group to the same lexicographically first host.
      val locations = indices.flatMap(i => parent.preferredLocations(parent.partitions(i)))
      val preferred = locations.groupBy(identity).toSeq.sortBy { case (host, splits) =>
        (-splits.size, assigned(host), host)
      }.headOption.map(_._1)
      preferred.foreach(host => assigned(host) += 1)
      val group = new PartitionGroup(preferred)
      indices.foreach(i => group.partitions += parent.partitions(i))
      group
    }.toArray
  }
}
