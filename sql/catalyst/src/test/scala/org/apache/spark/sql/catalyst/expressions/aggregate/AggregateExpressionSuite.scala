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
    val actual = Sum(Add(x, y)).toAggregateExpression().references
    val expected = AttributeSet(x :: y :: Nil)
    assert(expected == actual, s"Expected: $expected. Actual: $actual")
  }

  test("test regr_r2 input types") {
    Seq(
      RegrR2(Literal("a"), Literal(1d)),
      RegrR2(Literal(3.0D), Literal('b')),
      RegrR2(Literal(3.0D), Literal(Array(0)))
    ).foreach { expr =>
      assert(expr.checkInputDataTypes().isFailure)
    }
    assert(RegrR2(Literal(3.0D), Literal(1d)).checkInputDataTypes() ===
      TypeCheckResult.TypeCheckSuccess)
  }

  test("test regr_slope input types") {
    Seq(
      RegrSlope(Literal("a"), Literal(1)),
      RegrSlope(Literal(3.0D), Literal('b')),
      RegrSlope(Literal(3.0D), Literal(Array(0)))
    ).foreach { expr =>
      assert(expr.checkInputDataTypes().isFailure)
    }
    assert(RegrSlope(Literal(3.0D), Literal(1D)).checkInputDataTypes() ===
      TypeCheckResult.TypeCheckSuccess)
  }

  test("test regr_intercept input types") {
    Seq(
      RegrIntercept(Literal("a"), Literal(1)),
      RegrIntercept(Literal(3.0D), Literal('b')),
      RegrIntercept(Literal(3.0D), Literal(Array(0)))
    ).foreach { expr =>
      assert(expr.checkInputDataTypes().isFailure)
    }
    assert(RegrIntercept(Literal(3.0D), Literal(1D)).checkInputDataTypes() ===
      TypeCheckResult.TypeCheckSuccess)
  }
}
