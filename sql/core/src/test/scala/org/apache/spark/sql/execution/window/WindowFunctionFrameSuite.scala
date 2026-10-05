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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Lag, Literal, MutableProjection, NamedExpression, NthValue, OffsetWindowFunction, SortOrder, SpecificInternalRow}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.IntegerType

/**
 * Tests for the window partition-size guard. Most frames track the per-row index and window
 * bounds in 32-bit `Int`s, so a partition larger than `Int.MaxValue` rows is rejected with a clear
 * error instead of silently producing wrong results, unless every frame declares
 * `supportsLargePartition` (LEAD/LAG and unbounded NTH_VALUE). See
 * `WindowEvaluatorFactoryBase.checkPartitionSizeLimit`.
 *
 * All frames here are real frames, so the tests read each frame's actual
 * `supportsLargePartition` rather than a value a stub was told to return.
 */
class WindowFunctionFrameSuite extends SparkFunSuite {

  /** Minimal factory exposing the protected partition-size guard for testing. */
  private class TestFactory extends WindowEvaluatorFactoryBase {
    override def windowExpression: Seq[NamedExpression] = Nil
    override def partitionSpec: Seq[Expression] = Nil
    override def orderSpec: Seq[SortOrder] = Nil
    override def childOutput: Seq[Attribute] = Nil
    override def spillSize: SQLMetric = null

    def check(numRows: Long, frames: WindowFunctionFrame*): Unit =
      checkPartitionSizeLimit(numRows, frames.toArray)
  }

  private val maxRows = Int.MaxValue.toLong
  private val attr = AttributeReference("v", IntegerType, nullable = true)()
  private val newProjection =
    (expressions: Seq[Expression], schema: Seq[Attribute]) =>
      MutableProjection.create(expressions, schema)

  private def target = new SpecificInternalRow(Seq(IntegerType))

  /** LAG(v, 1): a frameless offset frame. */
  private def lagFrame: WindowFunctionFrame =
    new FrameLessOffsetWindowFunctionFrame(
      target,
      ordinal = 0,
      Array[OffsetWindowFunction](Lag(attr, Literal(1), Literal(null, IntegerType), false)),
      Seq[Attribute](attr),
      newProjection,
      offset = -1)

  /** NTH_VALUE(v, 1) over the whole partition. */
  private def unboundedNthValueFrame: WindowFunctionFrame =
    new UnboundedOffsetWindowFunctionFrame(
      target,
      ordinal = 0,
      Array[OffsetWindowFunction](NthValue(attr, Literal(1), ignoreNulls = false)),
      Seq[Attribute](attr),
      newProjection,
      offset = 1)

  /** NTH_VALUE(v, 1) over UNBOUNDED PRECEDING AND CURRENT ROW; its `write` reads the row index. */
  private def unboundedPrecedingNthValueFrame: WindowFunctionFrame =
    new UnboundedPrecedingOffsetWindowFunctionFrame(
      target,
      ordinal = 0,
      Array[OffsetWindowFunction](NthValue(attr, Literal(1), ignoreNulls = false)),
      Seq[Attribute](attr),
      newProjection,
      offset = 1)

  /** An aggregate over the whole partition; inherits the default `supportsLargePartition`. */
  private def unboundedAggregateFrame: WindowFunctionFrame =
    new UnboundedWindowFunctionFrame(target, processor = null)

  test("frames declare whether they support partitions larger than Int.MaxValue rows") {
    assert(lagFrame.supportsLargePartition)
    assert(unboundedNthValueFrame.supportsLargePartition)
    assert(!unboundedPrecedingNthValueFrame.supportsLargePartition)
    // Fails if the `WindowFunctionFrame` default ever becomes `true`.
    assert(!unboundedAggregateFrame.supportsLargePartition)
  }

  test("partitions up to Int.MaxValue rows are allowed for every frame") {
    val factory = new TestFactory
    factory.check(0L, unboundedAggregateFrame)
    factory.check(maxRows, unboundedAggregateFrame, unboundedPrecedingNthValueFrame)
  }

  test("larger partitions are allowed when every frame supports them") {
    val factory = new TestFactory
    factory.check(maxRows + 1, lagFrame)
    factory.check(Long.MaxValue, lagFrame, unboundedNthValueFrame)
  }

  test("larger partitions are rejected with a clear error if any frame does not support them") {
    val factory = new TestFactory
    val e = intercept[SparkException] {
      factory.check(maxRows + 1, unboundedAggregateFrame)
    }
    assert(e.getCondition == "WINDOW_FUNCTION_PARTITION_SIZE_EXCEEDS_LIMIT")
    assert(e.getMessageParameters.get("numRows") == (maxRows + 1).toString)

    // A single unsupported frame alongside supported ones is enough to reject.
    intercept[SparkException] {
      factory.check(maxRows + 1, lagFrame, unboundedPrecedingNthValueFrame)
    }
  }
}
