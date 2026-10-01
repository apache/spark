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

package org.apache.spark.sql.execution.window

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Lag, Literal, MutableProjection, NamedExpression, OffsetWindowFunction, SortOrder, SpecificInternalRow}
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.IntegerType

/**
 * Tests for the 32-bit overflow guard that fails window queries with a clear error when a
 * partition has more than `Int.MaxValue` rows and uses a frame that cannot handle it. Only the
 * LEAD/LAG and unbounded-offset frames (which track their cursor as a `Long` and ignore the
 * driver's 32-bit row index) are exempt. See `WindowFunctionFrame.supportsLargePartition` and
 * `WindowEvaluatorFactoryBase.checkPartitionSizeLimit`.
 */
class WindowFunctionFrameSuite extends SparkFunSuite {

  /** Minimal `WindowFunctionFrame` that only exercises the `supportsLargePartition` flag. */
  private def stubFrame(largePartitionSupport: Boolean): WindowFunctionFrame =
    new WindowFunctionFrame {
      override def prepare(rows: ExternalAppendOnlyUnsafeRowArray): Unit = {}
      override def write(index: Int, current: InternalRow): Unit = {}
      override def currentLowerBound(): Int = 0
      override def currentUpperBound(): Int = 0
      override def supportsLargePartition: Boolean = largePartitionSupport
    }

  /** Minimal factory exposing the protected partition-size guard for testing. */
  private class TestFactory extends WindowEvaluatorFactoryBase {
    override def windowExpression: Seq[NamedExpression] = Nil
    override def partitionSpec: Seq[Expression] = Nil
    override def orderSpec: Seq[SortOrder] = Nil
    override def childOutput: Seq[Attribute] = Nil
    override def spillSize: SQLMetric = null

    def check(numRows: Long, frames: Array[WindowFunctionFrame]): Unit =
      checkPartitionSizeLimit(numRows, frames)
  }

  private val maxRows = Int.MaxValue.toLong

  test("partitions up to Int.MaxValue rows are always allowed") {
    val factory = new TestFactory
    // At or below the 32-bit limit nothing throws, even for frames that do not support large
    // partitions.
    factory.check(0L, Array(stubFrame(largePartitionSupport = false)))
    factory.check(maxRows, Array(stubFrame(largePartitionSupport = false)))
  }

  test("oversized partitions are rejected for frames without large-partition support") {
    val factory = new TestFactory
    val e = intercept[SparkException] {
      factory.check(maxRows + 1, Array(stubFrame(largePartitionSupport = false)))
    }
    assert(e.getCondition == "WINDOW_FUNCTION_PARTITION_SIZE_EXCEEDS_LIMIT")
    assert(e.getMessageParameters.get("numRows") == (maxRows + 1).toString)

    // A single unsafe frame in the mix is enough to reject the partition.
    intercept[SparkException] {
      factory.check(
        maxRows + 1,
        Array(stubFrame(largePartitionSupport = true), stubFrame(largePartitionSupport = false)))
    }
  }

  test("oversized partitions are allowed when every frame supports them") {
    val factory = new TestFactory
    // Must not throw: LEAD/LAG-style offset frames handle partitions larger than Int.MaxValue.
    factory.check(maxRows + 1, Array(stubFrame(largePartitionSupport = true)))
    factory.check(
      Long.MaxValue,
      Array(stubFrame(largePartitionSupport = true), stubFrame(largePartitionSupport = true)))
  }

  test("the frameless offset frame (LEAD/LAG) declares large-partition support") {
    val attr = AttributeReference("v", IntegerType, nullable = true)()
    val lag = Lag(attr, Literal(1), Literal(null, IntegerType), false)
    val target = new SpecificInternalRow(Seq(IntegerType))
    val frame = new FrameLessOffsetWindowFunctionFrame(
      target,
      ordinal = 0,
      Array[OffsetWindowFunction](lag),
      Seq[Attribute](attr),
      (expressions, schema) => MutableProjection.create(expressions, schema),
      offset = -1,
      ignoreNulls = false)
    assert(frame.supportsLargePartition)
  }

  test("frames are treated as unsafe for large partitions by default") {
    // The base `WindowFunctionFrame` must default to false so new frames are safe-by-default.
    assert(!stubFrame(largePartitionSupport = false).supportsLargePartition)
  }
}
