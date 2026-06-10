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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.util.TimestampNanosTestUtils.nanosVal
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.TimestampNanosVal

/**
 * Comparison-expression coverage for the nanosecond timestamp types
 * ([[TimestampNTZNanosType]] / [[TimestampLTZNanosType]], precision in [7, 9]).
 *
 * [[OrderingSuite]] covers `GenerateOrdering` / `PhysicalDataType.ordering` directly; this suite
 * covers the predicate expressions built on top of that ordering: the six [[BinaryComparison]]
 * operators, [[In]] / [[InSet]], and [[Least]] / [[Greatest]] -- in interpreted, codegen and
 * unsafe-projection modes (via `checkEvaluation`). Each test iterates both flavors at all
 * precisions; the physical values are precision-blind, so the coverage is identical.
 */
class TimestampNanosComparisonSuite extends SparkFunSuite with ExpressionEvalHelper {

  private val nanosTypes = Seq(
    TimestampNTZNanosType(7), TimestampNTZNanosType(8), TimestampNTZNanosType(9),
    TimestampLTZNanosType(7), TimestampLTZNanosType(8), TimestampLTZNanosType(9))

  // Three ascending values: `small` and `mid` share the same micro and differ only in
  // nanosWithinMicro (the sub-microsecond tie-breaker); `big` differs in epochMicros.
  private val small = nanosVal(1000L, 100)
  private val mid = nanosVal(1000L, 101)
  private val big = nanosVal(1001L, 0)

  private def lit(v: TimestampNanosVal, dt: DataType): Literal = Literal.create(v, dt)

  test("equality operators") {
    for (dt <- nanosTypes) {
      // Distinct instances with equal fields, to prove value (not reference) equality.
      val smallCopy = nanosVal(1000L, 100)
      checkEvaluation(EqualTo(lit(small, dt), lit(smallCopy, dt)), true)
      checkEvaluation(EqualNullSafe(lit(small, dt), lit(smallCopy, dt)), true)
      // Same micro, different nano: sub-microsecond digits must break equality.
      checkEvaluation(EqualTo(lit(small, dt), lit(mid, dt)), false)
      checkEvaluation(EqualNullSafe(lit(small, dt), lit(mid, dt)), false)
    }
  }

  test("null semantics") {
    for (dt <- nanosTypes) {
      val nullLit = Literal.create(null, dt)
      checkEvaluation(EqualTo(nullLit, lit(small, dt)), null)
      checkEvaluation(LessThan(nullLit, lit(small, dt)), null)
      checkEvaluation(EqualNullSafe(nullLit, lit(small, dt)), false)
      checkEvaluation(EqualNullSafe(lit(small, dt), nullLit), false)
      checkEvaluation(EqualNullSafe(nullLit, Literal.create(null, dt)), true)
    }
  }

  test("ordering operators") {
    for (dt <- nanosTypes) {
      // Sub-microsecond tie-breaker: same epochMicros, nanosWithinMicro decides.
      checkEvaluation(LessThan(lit(small, dt), lit(mid, dt)), true)
      checkEvaluation(LessThanOrEqual(lit(small, dt), lit(mid, dt)), true)
      checkEvaluation(GreaterThan(lit(mid, dt), lit(small, dt)), true)
      checkEvaluation(GreaterThanOrEqual(lit(mid, dt), lit(small, dt)), true)
      checkEvaluation(LessThan(lit(mid, dt), lit(small, dt)), false)
      checkEvaluation(GreaterThan(lit(small, dt), lit(mid, dt)), false)
      // epochMicros dominates the nano field.
      checkEvaluation(LessThan(lit(mid, dt), lit(big, dt)), true)
      checkEvaluation(GreaterThan(lit(big, dt), lit(small, dt)), true)
      // Reflexive bounds.
      checkEvaluation(LessThanOrEqual(lit(small, dt), lit(small, dt)), true)
      checkEvaluation(GreaterThanOrEqual(lit(small, dt), lit(small, dt)), true)
    }
  }

  test("extreme values") {
    for (dt <- nanosTypes) {
      val min = nanosVal(Long.MinValue, 0)
      val max = nanosVal(Long.MaxValue, 999)
      val preEpoch = nanosVal(-1L, 999)
      val epoch = nanosVal(0L, 0)
      checkEvaluation(LessThan(lit(min, dt), lit(max, dt)), true)
      // Pre-epoch sorts before epoch regardless of the nano field.
      checkEvaluation(LessThan(lit(preEpoch, dt), lit(epoch, dt)), true)
      checkEvaluation(GreaterThan(lit(epoch, dt), lit(preEpoch, dt)), true)
    }
  }

  test("In and InSet") {
    for (dt <- nanosTypes) {
      checkEvaluation(In(lit(small, dt), Seq(lit(mid, dt), lit(big, dt))), false)
      checkEvaluation(
        In(lit(small, dt), Seq(lit(mid, dt), lit(nanosVal(1000L, 100), dt))), true)
      checkEvaluation(In(lit(small, dt), Seq(Literal.create(null, dt), lit(mid, dt))), null)
      checkEvaluation(InSet(lit(small, dt), Set[Any](small, big)), true)
      checkEvaluation(InSet(lit(mid, dt), Set[Any](small, big)), false)
    }
  }

  test("Least and Greatest") {
    for (dt <- nanosTypes) {
      checkEvaluation(Least(Seq(lit(mid, dt), lit(big, dt), lit(small, dt))), small)
      checkEvaluation(Greatest(Seq(lit(mid, dt), lit(big, dt), lit(small, dt))), big)
      checkEvaluation(
        Least(Seq(Literal.create(null, dt), lit(mid, dt), lit(small, dt))), small)
    }
  }

  test("comparison across different nanos types is a type mismatch at the expression level") {
    // Cross-precision, cross-flavor (NTZ vs LTZ) and nanos-vs-micros pairs share the same
    // physical value class but are distinct logical types: BinaryComparison must reject them
    // (coercion to a common type is the analyzer's job and is not wired up yet).
    val mismatchedPairs = Seq(
      (lit(small, TimestampNTZNanosType(9)), lit(mid, TimestampNTZNanosType(7))),
      (lit(small, TimestampNTZNanosType(9)), lit(mid, TimestampLTZNanosType(9))),
      (lit(small, TimestampNTZNanosType(9)), Literal.create(123456789L, TimestampNTZType)),
      (lit(small, TimestampLTZNanosType(9)), Literal.create(123456789L, TimestampType)))
    for ((l, r) <- mismatchedPairs) {
      val checkResult = EqualTo(l, r).checkInputDataTypes()
      assert(checkResult.isInstanceOf[DataTypeMismatch],
        s"${l.dataType} vs ${r.dataType} returned $checkResult")
      val lessResult = LessThan(l, r).checkInputDataTypes()
      assert(lessResult.isInstanceOf[DataTypeMismatch],
        s"${l.dataType} vs ${r.dataType} returned $lessResult")
    }
  }
}
