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

import java.util.Properties

import org.apache.spark.{SparkFunSuite, TaskContext, TaskContextImpl}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{ExpressionEvalHelper, If, SortArray, SparkPartitionID, SpecificInternalRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectSet
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Test suite for [[AggregatingAccumulator]].
 */
class AggregatingAccumulatorSuite
  extends SparkFunSuite
  with SharedSparkSession
  with ExpressionEvalHelper {
  private val a = $"a".long
  private val b = $"b".string
  private val c = $"c".double
  private val inputAttributes = Seq(a, b, c)
  private def str(s: String): UTF8String = UTF8String.fromString(s)

  test("empty aggregation") {
    val acc1 = AggregatingAccumulator(
      Seq(sum(a) + 1L as "sum_a", max(b) as "max_b", approxCountDistinct(c) as "acntd_c"),
      inputAttributes)
    val expectedSchema = new StructType()
      .add("sum_a", "long")
      .add("max_b", "string")
      .add("acntd_c", "long", nullable = false)
    assert(acc1.schema === expectedSchema)

    val accEmpty = acc1.copy()
    val acc2 = acc1.copy()

    // Merge empty
    acc1.merge(accEmpty)
    assert(acc1.isZero)

    // No updates
    assert(acc1.isZero)
    checkResult(acc1.value, InternalRow(null, null, 0), expectedSchema, false)
    assert(acc1.isZero)

    // A few updates
    acc1.add(InternalRow(4L, str("foo"), 4.9d))
    acc1.add(InternalRow(98L, str("bar"), -323.9d))
    acc1.add(InternalRow(-30L, str("baz"), 4129.8d))
    assert(!acc1.isZero)
    checkResult(acc1.value, InternalRow(73L, str("baz"), 3L), expectedSchema, false)

    // Idempotency of result
    checkResult(acc1.value, InternalRow(73L, str("baz"), 3L), expectedSchema, false)

    // A few updates to the copied accumulator using an updater
    val updater = acc2.copyAndReset()
    updater.add(InternalRow(-2L, str("qwerty"), -6773.9d))
    updater.add(InternalRow(-35L, str("zzz-top"), -323.9d))
    assert(acc2.isZero)
    acc2.setState(updater)
    checkResult(acc2.value, InternalRow(-36L, str("zzz-top"), 2L), expectedSchema, false)

    // Merge accumulators
    acc1.merge(acc2)
    acc1.merge(acc2)
    acc1.merge(accEmpty)
    acc1.merge(accEmpty)
    checkResult(acc1.value, InternalRow(1L, str("zzz-top"), 5L), expectedSchema, false)

    // Reset
    acc1.reset()
    assert(acc1.isZero)
  }

  test("non-deterministic expressions") {
    val acc_driver = AggregatingAccumulator(
      Seq(
        min(SparkPartitionID()) as "min_pid",
        max(SparkPartitionID()) as "max_pid",
        SparkPartitionID()),
      Nil)
    checkResult(acc_driver.value, InternalRow(null, null, 0), acc_driver.schema, false)

    def inPartition(id: Int)(f: => Unit): Unit = {
      val ctx = new TaskContextImpl(0, 0, 1, 0, 0, 1, null, new Properties, null)
      TaskContext.setTaskContext(ctx)
      try {
        f
      } finally {
        TaskContext.unset()
      }
    }

    val acc1 = acc_driver.copy()
    inPartition(3) {
      acc1.add(InternalRow.empty)
    }
    val acc2 = acc_driver.copy()
    inPartition(42) {
      acc2.add(InternalRow.empty)
    }
    val acc3 = acc_driver.copy()
    inPartition(96) {
      acc3.add(InternalRow.empty)
    }

    acc_driver.merge(acc1)
    acc_driver.merge(acc2)
    acc_driver.merge(acc3)
    assert(!acc_driver.isZero)
    checkResult(acc_driver.value, InternalRow(3, 96, 0), acc_driver.schema, false)
  }

  test("collect agg metrics on job") {
    val acc = AggregatingAccumulator(
      Seq(
        avg(a) + 1.0d as "avg_a",
        sum(a + 10L) as "sum_a",
        min(b) as "min_b",
        max(b) as "max_b",
        approxCountDistinct(b) as "acntd_b",
        SortArray(CollectSet(If(a < 1000L, a % 3L, a % 6L)).toAggregateExpression(), true)
          as "item_set",
        min(SparkPartitionID()) as "min_pid",
        max(SparkPartitionID()) as "max_pid",
        SparkPartitionID()),
      Seq(a, b))
    sparkContext.register(acc)
    def consume(ids: Iterator[Long]): Unit = {
      val row = new SpecificInternalRow(Seq(LongType, StringType))
      ids.foreach { id =>
        // Create the new row values.
        row.setLong(0, id)
        row.update(1, UTF8String.fromString(f"val_$id%06d"))

        // Update the accumulator
        acc.add(row)
      }
    }

    // Run job 1
    spark.sparkContext
      .range(0, 1000, 1, 8)
      .foreachPartition(consume)
    assert(checkResult(
      acc.value,
      InternalRow(
        500.5d,
        509500L,
        str("val_000000"),
        str("val_000999"),
        1057L,
        new GenericArrayData(Seq(0L, 1L, 2L)),
        0,
        7,
        0),
      acc.schema,
      false))

    // Run job 2
    spark.sparkContext
      .range(1000, 1200, 1, 8)
      .foreachPartition(consume)
    assert(checkResult(
      acc.value,
      InternalRow(
        600.5d,
        731400L,
        str("val_000000"),
        str("val_001199"),
        1280L,
        new GenericArrayData(Seq(0L, 1L, 2L, 3L, 4L, 5L)),
        0,
        7,
        0),
      acc.schema,
      false))
  }
}
