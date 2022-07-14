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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Add, AttributeSet, Literal}

class AggregateExpressionSuite extends SparkFunSuite {

  test("test references from unresolved aggregate functions") {
    val x = UnresolvedAttribute("x")
    val y = UnresolvedAttribute("y")
    val actual = AggregateExpression(Sum(Add(x, y)), mode = Complete, isDistinct = false).references
    val expected = AttributeSet(x :: y :: Nil)
    assert(expected == actual, s"Expected: $expected. Actual: $actual")
  }

  test("test regr_r2 input types") {
    val checkResult1 = RegrR2(Literal("a"), Literal(1d)).checkInputDataTypes()
    assert(checkResult1.isInstanceOf[TypeCheckResult.TypeCheckFailure])
    assert(checkResult1.asInstanceOf[TypeCheckResult.TypeCheckFailure].message
      .contains("argument 1 requires double type, however, ''a'' is of string type"))
    val checkResult2 = RegrR2(Literal(3.0D), Literal('b')).checkInputDataTypes()
    assert(checkResult2.isInstanceOf[TypeCheckResult.TypeCheckFailure])
    assert(checkResult2.asInstanceOf[TypeCheckResult.TypeCheckFailure].message
      .contains("argument 2 requires double type, however, ''b'' is of string type"))
    val checkResult3 = RegrR2(Literal(3.0D), Literal(Array(0))).checkInputDataTypes()
    assert(checkResult3.isInstanceOf[TypeCheckResult.TypeCheckFailure])
    assert(checkResult3.asInstanceOf[TypeCheckResult.TypeCheckFailure].message
      .contains("argument 2 requires double type, however, 'ARRAY(0)' is of array<int> type"))
    assert(RegrR2(Literal(3.0D), Literal(1d)).checkInputDataTypes() ===
      TypeCheckResult.TypeCheckSuccess)
  }

  test("test regr_slope input types") {
    val checkResult1 = RegrSlope(Literal("a"), Literal(1)).checkInputDataTypes()
    assert(checkResult1.isInstanceOf[TypeCheckResult.TypeCheckFailure])
    assert(checkResult1.asInstanceOf[TypeCheckResult.TypeCheckFailure].message
      .contains("argument 1 requires double type, however, ''a'' is of string type"))
    val checkResult2 = RegrSlope(Literal(3.0D), Literal('b')).checkInputDataTypes()
    assert(checkResult2.isInstanceOf[TypeCheckResult.TypeCheckFailure])
    assert(checkResult2.asInstanceOf[TypeCheckResult.TypeCheckFailure].message
      .contains("argument 2 requires double type, however, ''b'' is of string type"))
    val checkResult3 = RegrSlope(Literal(3.0D), Literal(Array(0))).checkInputDataTypes()
    assert(checkResult3.isInstanceOf[TypeCheckResult.TypeCheckFailure])
    assert(checkResult3.asInstanceOf[TypeCheckResult.TypeCheckFailure].message
      .contains("argument 2 requires double type, however, 'ARRAY(0)' is of array<int> type"))
    assert(RegrSlope(Literal(3.0D), Literal(1D)).checkInputDataTypes() ===
      TypeCheckResult.TypeCheckSuccess)
  }

  test("test regr_intercept input types") {
    val checkResult1 = RegrIntercept(Literal("a"), Literal(1)).checkInputDataTypes()
    assert(checkResult1.isInstanceOf[TypeCheckResult.TypeCheckFailure])
    assert(checkResult1.asInstanceOf[TypeCheckResult.TypeCheckFailure].message
      .contains("argument 1 requires double type, however, ''a'' is of string type"))
    val checkResult2 = RegrIntercept(Literal(3.0D), Literal('b')).checkInputDataTypes()
    assert(checkResult2.isInstanceOf[TypeCheckResult.TypeCheckFailure])
    assert(checkResult2.asInstanceOf[TypeCheckResult.TypeCheckFailure].message
      .contains("argument 2 requires double type, however, ''b'' is of string type"))
    val checkResult3 = RegrIntercept(Literal(3.0D), Literal(Array(0))).checkInputDataTypes()
    assert(checkResult3.isInstanceOf[TypeCheckResult.TypeCheckFailure])
    assert(checkResult3.asInstanceOf[TypeCheckResult.TypeCheckFailure].message
      .contains("argument 2 requires double type, however, 'ARRAY(0)' is of array<int> type"))
    assert(RegrIntercept(Literal(3.0D), Literal(1D)).checkInputDataTypes() ===
      TypeCheckResult.TypeCheckSuccess)
  }
}
