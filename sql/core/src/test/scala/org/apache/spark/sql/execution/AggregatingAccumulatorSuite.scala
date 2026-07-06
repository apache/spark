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

import org.apache.spark.{TaskContext, TaskContextImpl}
import org.apache.spark.scheduler.{TaskInfo, TaskLocality}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, ExpressionEvalHelper, If,
  SortArray, SparkPartitionID, SpecificInternalRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectSet
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Test suite for [[AggregatingAccumulator]].
 */
class AggregatingAccumulatorSuite
  extends SharedSparkSession
  with ExpressionEvalHelper {
  private val a = $"a".long
  private val b = $"b".string
  private val c = $"c".double
  private val inputAttributes = Seq(a, b, c)
  private def str(s: String): UTF8String = UTF8String.fromString(s)

  private def newAccumulator(
      lastAttempt: Boolean,
      functions: Seq[Expression],
      inputAttributes: Seq[Attribute]): AggregatingAccumulator = {
    if (lastAttempt) {
      AggregatingAccumulator.lastAttempt(functions, inputAttributes)
    } else {
      AggregatingAccumulator(functions, inputAttributes)
    }
  }

  /** Creates a last-attempt accumulator, registers it, and initializes its tracking state. */
  private def registeredLastAttempt(
      functions: Seq[Expression],
      inputAttributes: Seq[Attribute]): LastAttemptAggregatingAccumulator = {
    val acc = AggregatingAccumulator.lastAttempt(functions, inputAttributes)
    sparkContext.register(acc)
    acc.initializeLastAttemptAccumulator()
    acc
  }

  /** Builds a non-zero task-side accumulator by copying `main` and adding `rows`. */
  private def taskUpdate(
      main: LastAttemptAggregatingAccumulator,
      rows: InternalRow*): LastAttemptAggregatingAccumulator = {
    val taskAcc = main.copyAndReset()
    rows.foreach(taskAcc.add)
    taskAcc
  }

  private def taskInfo(partitionId: Int, attemptNumber: Int = 0): TaskInfo = {
    new TaskInfo(
      taskId = partitionId.toLong,
      index = partitionId,
      attemptNumber = attemptNumber,
      partitionId = partitionId,
      launchTime = 0L,
      executorId = "executor",
      host = "localhost",
      taskLocality = TaskLocality.ANY,
      speculative = false)
  }

  test("empty aggregation") {
    Seq(false, true).foreach { lastAttempt =>
      val acc1 = newAccumulator(
        lastAttempt,
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
  }

  test("non-deterministic expressions") {
    Seq(false, true).foreach { lastAttempt =>
      val accDriver = newAccumulator(
        lastAttempt,
        Seq(
          min(SparkPartitionID()) as "min_pid",
          max(SparkPartitionID()) as "max_pid",
          SparkPartitionID()),
        Nil)
      checkResult(accDriver.value, InternalRow(null, null, 0), accDriver.schema, false)

      def inPartition(id: Int)(f: => Unit): Unit = {
        val ctx = new TaskContextImpl(0, 0, 1, 0, 0, 1, null, new Properties, null)
        TaskContext.setTaskContext(ctx)
        try {
          f
        } finally {
          TaskContext.unset()
        }
      }

      val acc1 = accDriver.copy()
      inPartition(3) {
        acc1.add(InternalRow.empty)
      }
      val acc2 = accDriver.copy()
      inPartition(42) {
        acc2.add(InternalRow.empty)
      }
      val acc3 = accDriver.copy()
      inPartition(96) {
        acc3.add(InternalRow.empty)
      }

      accDriver.merge(acc1)
      accDriver.merge(acc2)
      accDriver.merge(acc3)
      assert(!accDriver.isZero)
      checkResult(accDriver.value, InternalRow(3, 96, 0), accDriver.schema, false)
    }
  }

  test("collect agg metrics on job") {
    Seq(false, true).foreach { lastAttempt =>
      val acc = newAccumulator(
        lastAttempt,
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
      acc match {
        case lastAttemptAcc: LastAttemptAggregatingAccumulator =>
          lastAttemptAcc.initializeLastAttemptAccumulator()
        case _ =>
      }

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
      val expectedAfterJob1 = InternalRow(
        500.5d,
        509500L,
        str("val_000000"),
        str("val_000999"),
        1057L,
        new GenericArrayData(Seq(0L, 1L, 2L)),
        0,
        7,
        0)
      assert(checkResult(
        acc.value,
        expectedAfterJob1,
        acc.schema,
        false))
      acc match {
        case lastAttemptAcc: LastAttemptAggregatingAccumulator =>
          assert(checkResult(
            lastAttemptAcc.lastAttemptValueForAllRDDs().get,
            expectedAfterJob1,
            acc.schema,
            false))
        case _ =>
      }

      // Run job 2
      spark.sparkContext
        .range(1000, 1200, 1, 8)
        .foreachPartition(consume)
      val expectedAfterJob2 = InternalRow(
        600.5d,
        731400L,
        str("val_000000"),
        str("val_001199"),
        1280L,
        new GenericArrayData(Seq(0L, 1L, 2L, 3L, 4L, 5L)),
        0,
        7,
        0)
      assert(checkResult(
        acc.value,
        expectedAfterJob2,
        acc.schema,
        false))
      acc match {
        case lastAttemptAcc: LastAttemptAggregatingAccumulator =>
          assert(checkResult(
            lastAttemptAcc.lastAttemptValueForAllRDDs().get,
            expectedAfterJob2,
            acc.schema,
            false))
        case _ =>
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Tests exclusive to the last-attempt accumulator.
  // ---------------------------------------------------------------------------

  test("last attempt direct driver value is copied") {
    val acc = registeredLastAttempt(Seq(sum(a) as "sum_a"), Seq(a))

    val row = new SpecificInternalRow(Seq(LongType))
    row.setLong(0, 1L)
    acc.add(row)
    val firstLastAttemptValue = acc.lastAttemptValueForHighestRDDId().get
    checkResult(firstLastAttemptValue, InternalRow(1L), acc.schema, false)

    row.setLong(0, 2L)
    acc.add(row)
    checkResult(acc.lastAttemptValueForHighestRDDId().get, InternalRow(3L), acc.schema, false)
    checkResult(firstLastAttemptValue, InternalRow(1L), acc.schema, false)
  }

  test("last attempt partial merge combines aggregate buffers") {
    val acc = registeredLastAttempt(Seq(sum(a) as "sum_a", max(b) as "max_b"), Seq(a, b))

    val rdd = sparkContext.parallelize(Seq.empty[Int], 2)
    val properties = new Properties

    acc.mergeLastAttempt(
      taskUpdate(acc, InternalRow(1L, str("a")), InternalRow(2L, str("b"))),
      rdd,
      taskInfo(partitionId = 0),
      stageId = 0,
      stageAttemptId = 0,
      properties)
    acc.mergeLastAttempt(
      taskUpdate(acc, InternalRow(10L, str("z")), InternalRow(-5L, str("c"))),
      rdd,
      taskInfo(partitionId = 1),
      stageId = 0,
      stageAttemptId = 0,
      properties)

    checkResult(
      acc.lastAttemptValueForRDDId(rdd.id).get,
      InternalRow(8L, str("z")),
      acc.schema,
      false)
  }

  test("last attempt invalidates on schema mismatch") {
    val acc = registeredLastAttempt(Seq(sum(a) as "sum_a"), Seq(a))

    // A task accumulator with a different schema is not mergeable, so the merge must bail out.
    val mismatched = AggregatingAccumulator.lastAttempt(
      Seq(sum(a) as "sum_a", max(b) as "max_b"),
      Seq(a, b))
    mismatched.add(InternalRow(1L, str("a")))

    val rdd = sparkContext.parallelize(Seq.empty[Int], 1)
    acc.mergeLastAttempt(
      mismatched,
      rdd,
      taskInfo(partitionId = 0),
      stageId = 0,
      stageAttemptId = 0,
      new Properties)

    assert(!acc.getValid)
    assert(acc.lastAttemptValueForAllRDDs().isEmpty)
    assert(acc.lastAttemptValueForRDDId(rdd.id).isEmpty)
  }

  test("last attempt invalidates when driver and task updates are mixed") {
    val acc = registeredLastAttempt(Seq(sum(a) as "sum_a"), Seq(a))

    val rdd = sparkContext.parallelize(Seq.empty[Int], 1)
    acc.mergeLastAttempt(
      taskUpdate(acc, InternalRow(5L)),
      rdd,
      taskInfo(partitionId = 0),
      stageId = 0,
      stageAttemptId = 0,
      new Properties)
    checkResult(acc.lastAttemptValueForAllRDDs().get, InternalRow(5L), acc.schema, false)

    // A direct driver update on top of task updates cannot be reasoned about, so it invalidates.
    val row = new SpecificInternalRow(Seq(LongType))
    row.setLong(0, 1L)
    acc.add(row)

    assert(!acc.getValid)
    assert(acc.lastAttemptValueForAllRDDs().isEmpty)
  }

  test("last attempt value for highest RDD id selects the latest RDD") {
    val acc = registeredLastAttempt(Seq(sum(a) as "sum_a"), Seq(a))

    val rdd1 = sparkContext.parallelize(Seq.empty[Int], 1)
    val rdd2 = sparkContext.parallelize(Seq.empty[Int], 1)
    assert(rdd2.id > rdd1.id)

    acc.mergeLastAttempt(
      taskUpdate(acc, InternalRow(3L)),
      rdd1,
      taskInfo(partitionId = 0),
      stageId = 0,
      stageAttemptId = 0,
      new Properties)
    acc.mergeLastAttempt(
      taskUpdate(acc, InternalRow(10L)),
      rdd2,
      taskInfo(partitionId = 0),
      stageId = 1,
      stageAttemptId = 0,
      new Properties)

    // Highest RDD id aggregates only the latest RDD.
    checkResult(acc.lastAttemptValueForHighestRDDId().get, InternalRow(10L), acc.schema, false)
    // All RDDs aggregates both.
    checkResult(acc.lastAttemptValueForAllRDDs().get, InternalRow(13L), acc.schema, false)
    // Narrowing to the earlier RDD returns only its value.
    checkResult(acc.lastAttemptValueForRDDId(rdd1.id).get, InternalRow(3L), acc.schema, false)
  }

  test("last attempt skips uncomputed partitions") {
    val acc = registeredLastAttempt(Seq(sum(a) as "sum_a"), Seq(a))

    // The RDD has 3 partitions but only 0 and 2 report values (e.g. early stop in take/limit).
    val rdd = sparkContext.parallelize(Seq.empty[Int], 3)
    acc.mergeLastAttempt(
      taskUpdate(acc, InternalRow(4L)),
      rdd,
      taskInfo(partitionId = 0),
      stageId = 0,
      stageAttemptId = 0,
      new Properties)
    acc.mergeLastAttempt(
      taskUpdate(acc, InternalRow(6L)),
      rdd,
      taskInfo(partitionId = 2),
      stageId = 0,
      stageAttemptId = 0,
      new Properties)

    // Partition 1 never reported, so only partitions 0 and 2 contribute.
    checkResult(acc.lastAttemptValueForRDDId(rdd.id).get, InternalRow(10L), acc.schema, false)
  }

  test("last attempt uses the latest stage attempt for a partition") {
    val acc = registeredLastAttempt(Seq(sum(a) as "sum_a"), Seq(a))

    val rdd = sparkContext.parallelize(Seq.empty[Int], 1)
    acc.mergeLastAttempt(
      taskUpdate(acc, InternalRow(3L)),
      rdd,
      taskInfo(partitionId = 0),
      stageId = 0,
      stageAttemptId = 0,
      new Properties)
    checkResult(acc.lastAttemptValueForRDDId(rdd.id).get, InternalRow(3L), acc.schema, false)

    // A later stage attempt recomputes the same partition and replaces its value.
    acc.mergeLastAttempt(
      taskUpdate(acc, InternalRow(100L)),
      rdd,
      taskInfo(partitionId = 0),
      stageId = 0,
      stageAttemptId = 1,
      new Properties)
    checkResult(acc.lastAttemptValueForRDDId(rdd.id).get, InternalRow(100L), acc.schema, false)

    // An older attempt arriving late is discarded.
    acc.mergeLastAttempt(
      taskUpdate(acc, InternalRow(7L)),
      rdd,
      taskInfo(partitionId = 0),
      stageId = 0,
      stageAttemptId = 0,
      new Properties)
    checkResult(acc.lastAttemptValueForRDDId(rdd.id).get, InternalRow(100L), acc.schema, false)
  }
}
