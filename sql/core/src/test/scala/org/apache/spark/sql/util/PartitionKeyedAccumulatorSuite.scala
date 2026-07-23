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
}
