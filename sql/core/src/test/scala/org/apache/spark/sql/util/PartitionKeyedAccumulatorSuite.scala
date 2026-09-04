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

package org.apache.spark.sql.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.streaming.operators.stateful.{
  StateStoreInstanceMetricAccumulator
}
import org.apache.spark.sql.execution.streaming.state.StateStoreSnapshotLastUploadInstanceMetric

class PartitionKeyedAccumulatorSuite extends SparkFunSuite {

  // The cache use case records (rowCount, sizeInBytes) per partition.
  private type Stats = (Long, Long)

  private def sumRows(acc: PartitionKeyedAccumulator[Stats]): Long =
    acc.foldValues(0L)((sum, v) => sum + v._1)

  private def sumBytes(acc: PartitionKeyedAccumulator[Stats]): Long =
    acc.foldValues(0L)((sum, v) => sum + v._2)

  test("isZero, add, value and accumulatedNumPartitions") {
    val acc = new PartitionKeyedAccumulator[Stats]
    assert(acc.isZero)
    assert(acc.accumulatedNumPartitions == 0)
    assert(acc.value.isEmpty)

    acc.add((0, (10L, 100L)))
    assert(!acc.isZero)
    assert(acc.accumulatedNumPartitions == 1)
    assert(acc.value.get(0) == ((10L, 100L)))

    acc.add((1, (5L, 50L)))
    assert(acc.accumulatedNumPartitions == 2)
    assert(sumRows(acc) == 15L)
    assert(sumBytes(acc) == 150L)
  }

  test("add is last-write-wins for the same partition id") {
    val acc = new PartitionKeyedAccumulator[Stats]
    acc.add((0, (1L, 1L)))
    acc.add((0, (2L, 2L))) // re-records partition 0 (e.g. a recompute)
    assert(acc.accumulatedNumPartitions == 1)
    assert(sumRows(acc) == 2L) // the later value wins, not 1 + 2
    assert(sumBytes(acc) == 2L)
  }

  test("merge is last-write-wins per partition id (de-duplicates, does not sum)") {
    // Two references compute the same partitions; partition 0 is computed by both.
    val a = new PartitionKeyedAccumulator[Stats]
    a.add((0, (10L, 100L)))

    val b = new PartitionKeyedAccumulator[Stats]
    b.add((0, (10L, 100L))) // duplicate compute of partition 0
    b.add((1, (5L, 50L)))

    a.merge(b)
    assert(a.accumulatedNumPartitions == 2) // partitions {0, 1}, not 3
    assert(sumRows(a) == 15L) // 10 (partition 0, counted once) + 5, NOT 25
    assert(sumBytes(a) == 150L)
  }

  test("copy is an independent snapshot") {
    val acc = new PartitionKeyedAccumulator[Stats]
    acc.add((0, (10L, 100L)))
    val snapshot = acc.copy()
    acc.add((1, (5L, 50L))) // mutate the original after copying

    assert(snapshot.accumulatedNumPartitions == 1)
    assert(sumRows(snapshot) == 10L)
    assert(acc.accumulatedNumPartitions == 2)
    assert(sumRows(acc) == 15L)
  }

  test("reset and copyAndReset") {
    val acc = new PartitionKeyedAccumulator[Stats]
    acc.add((0, (10L, 100L)))
    assert(!acc.isZero)

    assert(acc.copyAndReset().isZero)
    assert(!acc.isZero) // copyAndReset does not mutate the source

    acc.reset()
    assert(acc.isZero)
    assert(acc.accumulatedNumPartitions == 0)
  }

  test("works for an arbitrary value type") {
    val acc = new PartitionKeyedAccumulator[String]
    acc.add((0, "a"))
    acc.add((1, "b"))
    acc.add((0, "c")) // last-write-wins
    assert(acc.accumulatedNumPartitions == 2)
    assert(acc.foldValues("")((s, v) => s + v).length == 2) // "c" + "b" (each partition once)
  }

  test("SPARK-58272: fold returns an atomic snapshot only after every partition completes") {
    val accumulator = new PartitionKeyedAccumulator[Stats]
    accumulator.add((0, (10L, 100L)))

    assert(accumulator.foldValuesIfComplete(2, (0L, 0L)) {
      case ((rows, bytes), (partitionRows, partitionBytes)) =>
        (rows + partitionRows, bytes + partitionBytes)
    }.isEmpty)

    accumulator.add((1, (5L, 50L)))
    assert(accumulator.foldValuesIfComplete(2, (0L, 0L)) {
      case ((rows, bytes), (partitionRows, partitionBytes)) =>
        (rows + partitionRows, bytes + partitionBytes)
    }.contains((15L, 150L)))

    accumulator.add((1, (7L, 70L)))
    assert(accumulator.foldValuesIfComplete(2, (0L, 0L)) {
      case ((rows, bytes), (partitionRows, partitionBytes)) =>
        (rows + partitionRows, bytes + partitionBytes)
    }.contains((17L, 170L)))
  }

  test("SPARK-59174: StateStoreInstanceMetricAccumulator preserves combine semantics") {
    val metric0 = StateStoreSnapshotLastUploadInstanceMetric(Some(0), "default")
    val metric0Store2 = StateStoreSnapshotLastUploadInstanceMetric(Some(0), "other")
    val metric1 = StateStoreSnapshotLastUploadInstanceMetric(Some(1), "default")

    // 1. Add updates to the same partition: commutative combine (max version wins)
    val acc1 = new StateStoreInstanceMetricAccumulator
    acc1.add((0, Map(metric0 -> 100L)))
    acc1.add((0, Map(metric0 -> 105L)))
    assert(acc1.value.get(0).get(metric0) === Some(105L))

    val acc2 = new StateStoreInstanceMetricAccumulator
    acc2.add((0, Map(metric0 -> 105L)))
    acc2.add((0, Map(metric0 -> 100L)))
    assert(acc2.value.get(0).get(metric0) === Some(105L))

    // Initial value (-1) does not overwrite an existing valid snapshot version
    acc1.add((0, Map(metric0 -> -1L)))
    assert(acc1.value.get(0).get(metric0) === Some(105L))

    // 2. Multiple stores within the same partition merge cleanly
    acc1.add((0, Map(metric0Store2 -> 50L)))
    assert(acc1.value.get(0).size == 2)
    assert(acc1.value.get(0).get(metric0) === Some(105L))
    assert(acc1.value.get(0).get(metric0Store2) === Some(50L))

    // 3. Merge between accumulators: preserves combine semantics across attempts/retries
    val accA = new StateStoreInstanceMetricAccumulator
    accA.add((0, Map(metric0 -> 100L)))
    accA.add((1, Map(metric1 -> 200L)))

    val accB = new StateStoreInstanceMetricAccumulator
    accB.add((0, Map(metric0 -> 105L)))
    accB.add((1, Map(metric1 -> 150L)))

    accA.merge(accB)
    assert(accA.accumulatedNumPartitions == 2)
    assert(accA.value.get(0).get(metric0) === Some(105L))
    assert(accA.value.get(1).get(metric1) === Some(200L))
  }
}
