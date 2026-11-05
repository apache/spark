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
    val policy = V2ScanPartitionPacking(100, 0, Some(1), Some(1))
    val parts = inputs(30, 40, 200, 20, 30, 90)
    val groups = policy.pack(parts, 4)
    assert(groups === Some(Seq(Seq(0, 1), Seq(2), Seq(3, 4), Seq(5))))
    assert(groups.get.flatten === parts.indices)
    assert(policy.pack(Seq.empty, 4).isEmpty)
    assert(policy.pack(inputs(0, 0, 0), 4) === Some(Seq(Seq(0, 1, 2))))
  }

  test("opening cost and suggested parallelism constrain small inputs") {
    assert(V2ScanPartitionPacking(100, 10, Some(1), None).pack(inputs(40, 40, 0), 4) ===
      Some(Seq(Seq(0, 1), Seq(2))))
    // Each input stays a partition of its own.
    assert(V2ScanPartitionPacking(1000, 0, Some(3), None).pack(inputs(100, 100, 100, 100), 1)
      .isEmpty)
    assert(V2ScanPartitionPacking(5, 10, Some(1), None).pack(inputs(0, 0), 4).isEmpty)
  }

  test("an unset minimum count uses the default given at packing time") {
    val unset = V2ScanPartitionPacking(1000, 0, None, None)
    val parts = inputs(100, 100, 100, 100)
    assert(unset.pack(parts, 1) === Some(Seq(Seq(0, 1, 2, 3))))
    assert(unset.pack(parts, 3).isEmpty)
    assert(unset.pack(parts, 2) === Some(Seq(Seq(0, 1), Seq(2, 3))))
    assert(unset.copy(minPartitionNum = Some(1)).pack(parts, 3) === Some(Seq(Seq(0, 1, 2, 3))))
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
  }

  test("overflow and exact limits") {
    val policy = V2ScanPartitionPacking(Long.MaxValue, 10, Some(1), None)
    assert(policy.pack(inputs(Long.MaxValue, 1), 1).isEmpty)
    assert(policy.pack(inputs(Long.MaxValue - 20, 1, 1), 1) === Some(Seq(Seq(0), Seq(1, 2))))
    assert(V2ScanPartitionPacking(100, 0, Some(1), None).pack(inputs(100, 1), 1).isEmpty)
    assert(V2ScanPartitionPacking(100, 0, Some(1), None).pack(inputs(100, 1, 1), 1) ===
      Some(Seq(Seq(0), Seq(1, 2))))
  }
}
