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
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedAttribute, UnresolvedWithinGroup}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.{Add, Ascending, AttributeReference, AttributeSet, Descending, Literal, SortOrder}
import org.apache.spark.sql.catalyst.util.TypeUtils.ordinalNumber
import org.apache.spark.sql.types.IntegerType

class AggregateExpressionSuite extends SparkFunSuite {

  test("test references from unresolved aggregate functions") {
    val x = UnresolvedAttribute("x")
    val y = UnresolvedAttribute("y")
    val actual = Sum(Add(x, y)).toAggregateExpression().references
    val expected = AttributeSet(x :: y :: Nil)
    assert(expected == actual, s"Expected: $expected. Actual: $actual")
  }

  test("test regr_r2 input types") {
    assert(RegrR2(Literal("a"), Literal(1d)).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" -> "\"DOUBLE\"",
          "inputSql" -> "\"a\"",
          "inputType" -> "\"STRING\""
        )
      )
    )
    assert(RegrR2(Literal(3.0D), Literal('b')).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(1),
          "requiredType" -> "\"DOUBLE\"",
          "inputSql" -> "\"b\"",
          "inputType" -> "\"STRING\""
        )
      )
    )
    assert(RegrR2(Literal(3.0D), Literal(Array(0))).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(1),
          "requiredType" -> "\"DOUBLE\"",
          "inputSql" -> "\"ARRAY(0)\"",
          "inputType" -> "\"ARRAY<INT>\""
        )
      )
    )
    assert(RegrR2(Literal(3.0D), Literal(1d)).checkInputDataTypes() ===
      TypeCheckResult.TypeCheckSuccess)
  }

  test("test regr_slope input types") {
    assert(RegrSlope(Literal("a"), Literal(1)).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" -> "\"DOUBLE\"",
          "inputSql" -> "\"a\"",
          "inputType" -> "\"STRING\""
        )
      )
    )
    assert(RegrSlope(Literal(3.0D), Literal('b')).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(1),
          "requiredType" -> "\"DOUBLE\"",
          "inputSql" -> "\"b\"",
          "inputType" -> "\"STRING\""
        )
      )
    )
    assert(RegrSlope(Literal(3.0D), Literal(Array(0))).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(1),
          "requiredType" -> "\"DOUBLE\"",
          "inputSql" -> "\"ARRAY(0)\"",
          "inputType" -> "\"ARRAY<INT>\""
        )
      )
    )
    assert(RegrSlope(Literal(3.0D), Literal(1D)).checkInputDataTypes() ===
      TypeCheckResult.TypeCheckSuccess)
  }

  test("SPARK-54833: sql() should display correct sort direction for WITHIN GROUP") {
    val col = AttributeReference("col", IntegerType)()

    val modeAsc = Mode(UnresolvedWithinGroup).withOrderingWithinGroup(
      Seq(SortOrder(col, Ascending))
    ).asInstanceOf[Mode]
    assert(modeAsc.reverseOpt === Some(true))
    val ascSql = modeAsc.sql(isDistinct = false)
    assert(!ascSql.contains("DESC"),
      s"MODE() WITHIN GROUP (ORDER BY col ASC) should not contain DESC, but got: $ascSql")

    val modeDesc = Mode(UnresolvedWithinGroup).withOrderingWithinGroup(
      Seq(SortOrder(col, Descending))
    ).asInstanceOf[Mode]
    assert(modeDesc.reverseOpt === Some(false))
    val descSql = modeDesc.sql(isDistinct = false)
    assert(descSql.contains("DESC"),
      s"MODE() WITHIN GROUP (ORDER BY col DESC) should contain DESC, but got: $descSql")
  }

  test("test regr_intercept input types") {
    assert(RegrIntercept(Literal("a"), Literal(1)).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" -> "\"DOUBLE\"",
          "inputSql" -> "\"a\"",
          "inputType" -> "\"STRING\""
        )
      )
    )
    assert(RegrIntercept(Literal(3.0D), Literal('b')).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(1),
          "requiredType" -> "\"DOUBLE\"",
          "inputSql" -> "\"b\"",
          "inputType" -> "\"STRING\""
        )
      )
    )
    assert(RegrIntercept(Literal(3.0D), Literal(Array(0))).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(1),
          "requiredType" -> "\"DOUBLE\"",
          "inputSql" -> "\"ARRAY(0)\"",
          "inputType" -> "\"ARRAY<INT>\""
        )
      )
    )
    assert(RegrIntercept(Literal(3.0D), Literal(1D)).checkInputDataTypes() ===
      TypeCheckResult.TypeCheckSuccess)
  }
}
