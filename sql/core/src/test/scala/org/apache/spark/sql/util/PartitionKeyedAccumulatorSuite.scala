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

import java.util.Properties

import org.apache.spark.{SparkFunSuite, TaskContext, TaskContextImpl}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.sql.execution.streaming.operators.stateful.{
  StateStoreInstanceMetricAccumulator
}
import org.apache.spark.sql.execution.streaming.state.StateStoreSnapshotLastUploadInstanceMetric
import org.apache.spark.util.{AccumulatorContext, AccumulatorMetadata, AccumulatorV2, Utils}

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

  test("SPARK-57547: accessors are null-safe while readObject publishes the accumulator") {
    // `AccumulatorV2.readObject` registers `this` with the `TaskContext` before Java
    // deserialization has read this subclass's fields, so the backing map is still unset at that
    // point. `isZero` is the accessor that hit this in production: the executor heartbeater calls
    // it on every registered accumulator, where it threw a NullPointerException and killed the
    // heartbeat thread. `value` is not on the heartbeat path in this window -- it is covered here
    // as additional null-safety, since the driver reaches it later via `toInfoUpdate` should a
    // half-read accumulator ever be shipped. Stand in for those readers by probing from
    // `registerAccumulator`, which `readObject` calls at exactly that moment.
    //
    // Each accessor gets its own fresh deserialization: the first guarded call installs the map,
    // so probing them together would let a regression in any later accessor pass unnoticed.
    val probes = Seq[(String, PartitionKeyedAccumulator[Stats] => Unit)](
      "isZero" -> (acc => assert(acc.isZero)),
      "value" -> (acc => assert(acc.value.isEmpty)))

    probes.foreach { case (accessor, probe) =>
      val acc = new PartitionKeyedAccumulator[Stats]
      acc.metadata =
        AccumulatorMetadata(AccumulatorContext.newId(), None, countFailedValues = false)
      AccumulatorContext.register(acc)

      var probed = false
      val taskContext = new TaskContextImpl(
        stageId = 0,
        stageAttemptNumber = 0,
        partitionId = 0,
        taskAttemptId = 0L,
        attemptNumber = 0,
        numPartitions = 1,
        taskMemoryManager = null,
        localProperties = new Properties,
        metricsSystem = null,
        taskMetrics = TaskMetrics.empty,
        cpuAmount = BigDecimal(1)) {
        private[spark] override def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {
          probe(a.asInstanceOf[PartitionKeyedAccumulator[Stats]])
          probed = true
        }
      }

      TaskContext.setTaskContext(taskContext)
      try {
        Utils.deserialize[PartitionKeyedAccumulator[Stats]](Utils.serialize(acc))
      } finally {
        TaskContext.unset()
        AccumulatorContext.remove(acc.id)
      }
      assert(probed, s"$accessor was never probed, so the race was not exercised")
    }
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
