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

import java.lang.management.ManagementFactory
import java.util.OptionalLong
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.connector.read.{HasPartitionStatistics, InputPartition}

class V2ScanPartitionPackingSuite extends SparkFunSuite {
  private case class Sized(bytes: Option[Long]) extends HasPartitionStatistics {
    override def sizeInBytes(): OptionalLong = bytes.map(OptionalLong.of)
      .getOrElse(OptionalLong.empty())
    override def numRows(): OptionalLong = OptionalLong.empty()
    override def filesCount(): OptionalLong = OptionalLong.empty()
  }
  private def inputs(sizes: Long*): Seq[InputPartition] =
    sizes.map(size => Sized(Some(size)))

  test("pack actual input sizes, keeping oversized splits separate and covering each input once") {
    val policy = V2ScanPartitionPacking(100, 0, Some(1), None)
    val parts = inputs(30, 40, 200, 20, 30, 90)
    val groups = policy.pack(parts, 4)
    assert(groups === Some(Seq(Seq(0, 1), Seq(2), Seq(3, 4), Seq(5))))
    assert(groups.get.flatten === parts.indices)
    assert(policy.pack(Seq.empty, 4).isEmpty)
    assert(policy.pack(inputs(0, 0, 0), 4) === Some(Seq(Seq(0, 1, 2))))
  }

  test("opening cost and suggested parallelism constrain small inputs") {
    // As for files, an input joins a group while its size fits, and then adds its opening cost.
    assert(V2ScanPartitionPacking(100, 10, Some(1), None).pack(inputs(40, 40, 15), 4) ===
      Some(Seq(Seq(0, 1), Seq(2))))
    assert(V2ScanPartitionPacking(100, 0, Some(1), None).pack(inputs(40, 40, 15), 4) ===
      Some(Seq(Seq(0, 1, 2))))
    // Each input stays a partition of its own.
    assert(V2ScanPartitionPacking(1000, 0, Some(3), None).pack(inputs(100, 100, 100, 100), 1)
      .isEmpty)
    assert(V2ScanPartitionPacking(5, 10, Some(1), None).pack(inputs(0, 0), 4).isEmpty)
    // The target rounds down, as FilePartition.maxSplitBytes does: 39 / 5 = 7.
    assert(V2ScanPartitionPacking(128, 4, Some(5), None).pack(inputs(12, 0, 4, 7), 1).isEmpty)
  }

  test("an unset minimum count uses the default given at packing time") {
    val unset = V2ScanPartitionPacking(1000, 0, None, None)
    val parts = inputs(100, 100, 100, 100)
    assert(unset.pack(parts, 1) === Some(Seq(Seq(0, 1, 2, 3))))
    assert(unset.pack(parts, 3).isEmpty)
    assert(unset.pack(parts, 2) === Some(Seq(Seq(0, 1), Seq(2, 3))))
    assert(unset.copy(minPartitionNum = Some(1)).pack(parts, 3) === Some(Seq(Seq(0, 1, 2, 3))))
  }

  test("above the maximum count, sized inputs are packed by the total size over that count") {
    val parts = inputs(Seq.fill(8)(10L): _*)
    assert(V2ScanPartitionPacking(10, 0, Some(1), None).pack(parts, 1).isEmpty)
    assert(V2ScanPartitionPacking(10, 0, Some(1), Some(2)).pack(parts, 1) ===
      Some(Seq(0 until 4, 4 until 8)))
    // The cap's target rounds up, as for files: 11 / 2 = 6.
    assert(V2ScanPartitionPacking(5, 2, Some(1), Some(2)).pack(inputs(1, 3, 1), 1) ===
      Some(Seq(Seq(0, 1), Seq(2))))
    // The opening cost counts toward the total, as for files: 80 / 2 = 40.
    assert(V2ScanPartitionPacking(10, 10, Some(1), Some(2)).pack(inputs(Seq.fill(8)(0L): _*), 1) ===
      Some(Seq(0 to 4, 5 to 7)))
    // At or below the cap, the groups by size stay.
    assert(V2ScanPartitionPacking(30, 0, Some(1), Some(2)).pack(inputs(10, 10, 10, 10), 1) ===
      Some(Seq(Seq(0, 1, 2), Seq(3))))
    assert(V2ScanPartitionPacking(20, 0, Some(1), Some(4)).pack(inputs(10, 10, 10, 10), 1) ===
      Some(Seq(Seq(0, 1), Seq(2, 3))))
    // As for files, the cap is suggested: inputs that do not fit together stay apart.
    assert(V2ScanPartitionPacking(30, 0, Some(1), Some(2)).pack(inputs(30, 30, 30), 1).isEmpty)
  }

  test("missing, mixed and negative sizes require an explicit count fallback") {
    val unknown = new InputPartition {}
    Seq(unknown, Sized(None), Sized(Some(-1))).foreach { p =>
      val parts = inputs(30, 40) :+ p
      assert(V2ScanPartitionPacking(100, 0, Some(1), None).pack(parts, 1).isEmpty)
      assert(V2ScanPartitionPacking(100, 0, Some(1), Some(2)).pack(parts, 1) ===
        Some(Seq(Seq(0), Seq(1, 2))))
      assert(V2ScanPartitionPacking(100, 0, Some(1), Some(10)).pack(parts, 1).isEmpty)
    }
  }

  test("sizes are read only up to the first input without one") {
    val reads = new AtomicInteger()
    case class Counted(bytes: Option[Long]) extends HasPartitionStatistics {
      override def sizeInBytes(): OptionalLong = {
        reads.incrementAndGet()
        bytes.map(OptionalLong.of).getOrElse(OptionalLong.empty())
      }
      override def numRows(): OptionalLong = OptionalLong.empty()
      override def filesCount(): OptionalLong = OptionalLong.empty()
    }
    val parts = Seq(Counted(Some(10)), Counted(None)) ++ Seq.fill(100000)(Counted(Some(10)))
    assert(V2ScanPartitionPacking(100, 0, Some(1), None).pack(parts, 1).isEmpty)
    assert(reads.get() === 2)
    // With the cap, sizes are still read only up to the first input without one.
    assert(V2ScanPartitionPacking(100, 0, Some(1), Some(2)).pack(parts, 1).map(_.map(_.size)) ===
      Some(Seq(50001, 50001)))
    assert(reads.get() === 4)
  }

  test("an unchanged scan builds no group per input") {
    val bean = ManagementFactory.getThreadMXBean.asInstanceOf[com.sun.management.ThreadMXBean]
    assume(bean.isThreadAllocatedMemorySupported && bean.isThreadAllocatedMemoryEnabled)
    def allocated(groups: => Option[Seq[Range]]): Long = {
      val before = bean.getCurrentThreadAllocatedBytes()
      assert(groups.isEmpty)
      bean.getCurrentThreadAllocatedBytes() - before
    }
    // Inputs that allocate nothing when asked for their size.
    case class Reported(size: OptionalLong) extends HasPartitionStatistics {
      override def sizeInBytes(): OptionalLong = size
      override def numRows(): OptionalLong = OptionalLong.empty()
      override def filesCount(): OptionalLong = OptionalLong.empty()
    }
    val n = 1000000
    val oversized = Vector.fill(n)(Reported(OptionalLong.of(1000L)))
    val unsized = Vector.fill(n)(Reported(OptionalLong.empty()))
    val noCap = V2ScanPartitionPacking(100, 0, Some(1), None)
    // Sizes and group starts take about 20 bytes per input; a group for each input would take
    // about 60 more.
    assert(allocated(noCap.pack(oversized, 1)) < 32L * n)
    // Without valid sizes nothing is kept per input, also under a cap that leaves each input on
    // its own.
    assert(allocated(noCap.pack(unsized, 1)) < n)
    assert(allocated(noCap.copy(maxPartitionNum = Some(n)).pack(unsized, 1)) < n)
    // The same through packSlots, as the packing node calls it, when no partition is empty.
    val oversizedSlots = oversized.map(Some(_))
    val unsizedSlots = unsized.map(Some(_))
    assert(allocated(noCap.packSlots(oversizedSlots, 1)) < 32L * n)
    assert(allocated(noCap.packSlots(unsizedSlots, 1)) < n)
  }

  test("overflow and exact limits") {
    val policy = V2ScanPartitionPacking(Long.MaxValue, 10, Some(1), None)
    assert(policy.pack(inputs(Long.MaxValue, 1), 1).isEmpty)
    assert(policy.pack(inputs(Long.MaxValue - 20, 1, 1), 1) === Some(Seq(Seq(0, 1), Seq(2))))
    assert(V2ScanPartitionPacking(100, 0, Some(1), None).pack(inputs(100, 1), 1).isEmpty)
    assert(V2ScanPartitionPacking(100, 0, Some(1), None).pack(inputs(100, 1, 1), 1) ===
      Some(Seq(Seq(0), Seq(1, 2))))
  }

  test("partitions emptied by runtime filtering join a neighbor's range, each covered once") {
    val policy = V2ScanPartitionPacking(100, 0, Some(1), None)
    def slots(sizes: Option[Long]*): IndexedSeq[Option[InputPartition]] =
      sizes.map(_.map(size => Sized(Some(size)): InputPartition)).toIndexedSeq
    // Leading, intervening and trailing empty partitions join the range before them, or the
    // first range, and the inputs pack as `pack` packs them.
    val packed = policy.packSlots(
      slots(None, Some(30), Some(40), None, Some(200), None, Some(20), None), 4)
    assert(packed === Some(Seq(0 until 4, 4 until 6, 6 until 8)))
    assert(packed.get.flatten === (0 until 8))
    // Inputs that stay apart still take the empty partitions after them.
    assert(policy.packSlots(slots(Some(100), None, Some(100)), 4) ===
      Some(Seq(0 until 2, 2 until 3)))
    // With no input left, one range holds every partition.
    assert(policy.packSlots(slots(None, None, None), 4) === Some(Seq(0 until 3)))
    // Nothing to merge.
    assert(policy.packSlots(slots(None), 4).isEmpty)
    assert(policy.packSlots(slots(Some(100), Some(100)), 4).isEmpty)
    assert(policy.packSlots(IndexedSeq.empty, 4).isEmpty)
    // Inputs without sizes are grouped by count only under the cap, but the empty partitions join
    // the range before them either way.
    val unsized = IndexedSeq(Some(Sized(None)), None, Some(Sized(None)), Some(Sized(None)))
    assert(policy.packSlots(unsized, 4) === Some(Seq(0 until 2, 2 until 3, 3 until 4)))
    assert(policy.copy(maxPartitionNum = Some(2)).packSlots(unsized, 4) ===
      Some(Seq(0 until 2, 2 until 4)))
  }
}
