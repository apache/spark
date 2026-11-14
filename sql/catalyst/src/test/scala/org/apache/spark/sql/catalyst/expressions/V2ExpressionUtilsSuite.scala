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

import java.time.LocalDate

import org.apache.spark.{SparkFunSuite, SparkIllegalArgumentException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, Literal => _, _}
import org.apache.spark.sql.connector.expressions.filter.{Predicate => V2Predicate}
import org.apache.spark.sql.connector.util.V2ExpressionSQLBuilder
import org.apache.spark.sql.types.{BooleanType, DateType, Decimal, DecimalType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

class V2ExpressionUtilsSuite extends SparkFunSuite {

  test("SPARK-61495: modeled literals accept external and internal representations") {
    val string = Literal(UTF8String.fromString("Y"), StringType)
    val decimal = Literal(Decimal(new java.math.BigDecimal("1.25")), DecimalType(3, 2))
    val date = Literal(19724, DateType)
    val cases: Seq[(V2Expression, Literal)] = Seq(
      Expressions.literal("Y") -> string,
      Expressions.literal(new java.math.BigDecimal("1.25")) -> decimal,
      Expressions.literal(LocalDate.of(2024, 1, 2)) -> date,
      LiteralValue(string.value, string.dataType) -> string,
      LiteralValue(decimal.value, decimal.dataType) -> decimal,
      LiteralValue(date.value, date.dataType) -> date)
    cases.foreach { case (expression, expected) =>
      assert(V2ExpressionUtils.toCatalyst(expression).contains(expected))
    }
  }

  test("SPARK-61495: invalid modeled literal values cannot be converted") {
    val literals: Seq[V2Expression] = Seq(
      LiteralValue("not an integer", IntegerType),
      LiteralValue(UTF8String.fromString("not an integer"), IntegerType),
      LiteralValue(1, StringType))
    literals.foreach { literal =>
      assert(V2ExpressionUtils.toCatalyst(literal).isEmpty)
    }
  }

  test("SPARK-61495: modeled expressions with invalid arity cannot be converted") {
    val arities = Seq(
      "WIDTH_BUCKET" -> Seq(0, 1, 2, 3, 5),
      "IN" -> Seq(0),
      "BOOLEAN_EXPRESSION" -> Seq(0, 2),
      "CASE_WHEN" -> Seq(0, 1),
      "COALESCE" -> Seq(0),
      "GREATEST" -> Seq(0, 1),
      "LEAST" -> Seq(0, 1))
    arities.foreach { case (name, counts) =>
      counts.foreach { count =>
        val children = Array.fill[V2Expression](count)(LiteralValue(1, IntegerType))
        assert(V2ExpressionUtils.toCatalyst(new GeneralScalarExpression(name, children)).isEmpty,
          s"$name with $count children")
      }
    }
  }

  test("SPARK-61495: valid modeled expressions retain their values after arity checks") {
    val one = LiteralValue(1, IntegerType)
    val two = LiteralValue(2, IntegerType)
    val yes = LiteralValue(true, BooleanType)
    val buckets = LiteralValue(2L, LongType)
    val cases = Seq(
      ("WIDTH_BUCKET", Array[V2Expression](one, LiteralValue(0, IntegerType), two, buckets), 2L),
      ("IN", Array[V2Expression](one), false),
      ("IN", Array[V2Expression](one, one), true),
      ("BOOLEAN_EXPRESSION", Array[V2Expression](yes), true),
      ("CASE_WHEN", Array[V2Expression](yes, one), 1),
      ("CASE_WHEN", Array[V2Expression](yes, one, two), 1),
      ("COALESCE", Array[V2Expression](one), 1),
      ("GREATEST", Array[V2Expression](one, two), 2),
      ("LEAST", Array[V2Expression](one, two), 1))
    cases.foreach { case (name, children, expected) =>
      val converted = V2ExpressionUtils.toCatalyst(new GeneralScalarExpression(name, children))
      assert(converted.get.eval() == expected, name)
    }
  }

  test("SPARK-39313: toCatalystOrdering should fail if V2Expression can not be translated") {
    val supportedV2Sort = SortValue(
      FieldReference("a"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    val unsupportedV2Sort = supportedV2Sort.copy(
      expression = ApplyTransform("v2Fun", FieldReference("a") :: Nil))
    val exc = intercept[AnalysisException] {
      V2ExpressionUtils.toCatalystOrdering(
        Array(supportedV2Sort, unsupportedV2Sort),
        LocalRelation.apply(AttributeReference("a", StringType)()))
    }
    assert(exc.message.contains("v2Fun(a) ASC NULLS FIRST is not currently supported"))
  }

  test("SPARK-59983: V2 expression with an unknown name should be rendered as a function call") {
    val dateTrunc = new GeneralScalarExpression("DATE_TRUNC",
      Array(LiteralValue(UTF8String.fromString("MONTH"), StringType), FieldReference("a")))
    assert(dateTrunc.toString === "DATE_TRUNC('MONTH', a)")
    assert(dateTrunc.describe() === "DATE_TRUNC('MONTH', a)")
    assert(new GeneralScalarExpression("ABS", Array(dateTrunc)).toString ===
      "ABS(DATE_TRUNC('MONTH', a))")
    val like = new V2Predicate("LIKE",
      Array(FieldReference("a"), LiteralValue(UTF8String.fromString("x%"), StringType)))
    assert(like.toString === "LIKE(a, 'x%')")
    // The SQL builder for pushdown still rejects an unknown name.
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        new V2ExpressionSQLBuilder().build(dateTrunc)
      },
      condition = "UNEXPECTED_V2_EXPRESSION",
      parameters = Map("expr" -> "DATE_TRUNC('MONTH', a)"))
    checkError(
      exception = intercept[AnalysisException] {
        V2ExpressionUtils.toCatalystOrdering(
          Array(SortValue(dateTrunc, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)),
          LocalRelation.apply(AttributeReference("a", StringType)()))
      },
      condition = "_LEGACY_ERROR_TEMP_3054",
      parameters = Map("expr" -> "DATE_TRUNC('MONTH', a)"))
  }

  test("SPARK-59721: resolveRefOpt returns None for an unresolvable reference") {
    val plan = LocalRelation(AttributeReference("a", StringType)())
    assert(V2ExpressionUtils.resolveRefOpt(FieldReference("a"), plan).isDefined)
    assert(V2ExpressionUtils.resolveRefOpt(FieldReference("missing"), plan).isEmpty)
  }

  test("SPARK-59721: resolveRefOpt throws for a missing nested field or an ambiguous reference") {
    val structPlan =
      LocalRelation(AttributeReference("s", new StructType().add("x", IntegerType))())
    checkError(
      exception = intercept[AnalysisException] {
        V2ExpressionUtils.resolveRefOpt(FieldReference("s.missing"), structPlan)
      },
      condition = "FIELD_NOT_FOUND",
      parameters = Map("fieldName" -> "`missing`", "fields" -> "`x`"))

    val ambiguousPlan = LocalRelation(
      AttributeReference("a", StringType)(), AttributeReference("a", StringType)())
    checkError(
      exception = intercept[AnalysisException] {
        V2ExpressionUtils.resolveRefOpt(FieldReference("a"), ambiguousPlan)
      },
      condition = "AMBIGUOUS_REFERENCE",
      parameters = Map("name" -> "`a`", "referenceNames" -> "[`a`, `a`]"))
  }
}
