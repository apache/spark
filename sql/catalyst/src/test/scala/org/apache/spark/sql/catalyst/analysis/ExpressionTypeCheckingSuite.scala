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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types._

class ExpressionTypeCheckingSuite extends SparkFunSuite {

  val testRelation = LocalRelation(
    Symbol("intField").int,
    Symbol("stringField").string,
    Symbol("booleanField").boolean,
    Symbol("decimalField").decimal(8, 0),
    Symbol("arrayField").array(StringType),
    Symbol("mapField").map(StringType, LongType))

  def assertError(expr: Expression, errorMessage: String): Unit = {
    val e = intercept[AnalysisException] {
      assertSuccess(expr)
    }
    assert(e.getMessage.contains(
      s"cannot resolve '${expr.sql}' due to data type mismatch:"))
    assert(e.getMessage.contains(errorMessage))
  }

  def assertSuccess(expr: Expression): Unit = {
    val analyzed = testRelation.select(expr.as("c")).analyze
    SimpleAnalyzer.checkAnalysis(analyzed)
  }

  def assertErrorForDifferingTypes(expr: Expression): Unit = {
    assertError(expr,
      s"differing types in '${expr.sql}'")
  }

  test("check types for unary arithmetic") {
    assertError(BitwiseNot(Symbol("stringField")), "requires integral type")
  }

  test("check types for binary arithmetic") {
    // We will cast String to Double for binary arithmetic
    assertSuccess(Add(Symbol("intField"), Symbol("stringField")))
    assertSuccess(Subtract(Symbol("intField"), Symbol("stringField")))
    assertSuccess(Multiply(Symbol("intField"), Symbol("stringField")))
    assertSuccess(Divide(Symbol("intField"), Symbol("stringField")))
    assertSuccess(Remainder(Symbol("intField"), Symbol("stringField")))
    // checkAnalysis(BitwiseAnd(Symbol("intField"), Symbol("stringField")))

    assertErrorForDifferingTypes(Add(Symbol("intField"), Symbol("booleanField")))
    assertErrorForDifferingTypes(Subtract(Symbol("intField"), Symbol("booleanField")))
    assertErrorForDifferingTypes(Multiply(Symbol("intField"), Symbol("booleanField")))
    assertErrorForDifferingTypes(Divide(Symbol("intField"), Symbol("booleanField")))
    assertErrorForDifferingTypes(Remainder(Symbol("intField"), Symbol("booleanField")))
    assertErrorForDifferingTypes(BitwiseAnd(Symbol("intField"), Symbol("booleanField")))
    assertErrorForDifferingTypes(BitwiseOr(Symbol("intField"), Symbol("booleanField")))
    assertErrorForDifferingTypes(BitwiseXor(Symbol("intField"), Symbol("booleanField")))

    assertError(Add(Symbol("booleanField"), Symbol("booleanField")),
      "requires (numeric or interval) type")
    assertError(Subtract(Symbol("booleanField"), Symbol("booleanField")),
      "requires (numeric or interval) type")
    assertError(Multiply(Symbol("booleanField"), Symbol("booleanField")), "requires numeric type")
    assertError(Divide(Symbol("booleanField"), Symbol("booleanField")),
      "requires (double or decimal) type")
    assertError(Remainder(Symbol("booleanField"), Symbol("booleanField")), "requires numeric type")

    assertError(BitwiseAnd(Symbol("booleanField"), Symbol("booleanField")),
      "requires integral type")
    assertError(BitwiseOr(Symbol("booleanField"), Symbol("booleanField")), "requires integral type")
    assertError(BitwiseXor(Symbol("booleanField"), Symbol("booleanField")),
      "requires integral type")
  }

  test("check types for predicates") {
    // We will cast String to Double for binary comparison
    assertSuccess(EqualTo(Symbol("intField"), Symbol("stringField")))
    assertSuccess(EqualNullSafe(Symbol("intField"), Symbol("stringField")))
    assertSuccess(LessThan(Symbol("intField"), Symbol("stringField")))
    assertSuccess(LessThanOrEqual(Symbol("intField"), Symbol("stringField")))
    assertSuccess(GreaterThan(Symbol("intField"), Symbol("stringField")))
    assertSuccess(GreaterThanOrEqual(Symbol("intField"), Symbol("stringField")))

    // We will transform EqualTo with numeric and boolean types to CaseKeyWhen
    assertSuccess(EqualTo(Symbol("intField"), Symbol("booleanField")))
    assertSuccess(EqualNullSafe(Symbol("intField"), Symbol("booleanField")))

    assertErrorForDifferingTypes(EqualTo(Symbol("intField"), Symbol("mapField")))
    assertErrorForDifferingTypes(EqualNullSafe(Symbol("intField"), Symbol("mapField")))
    assertErrorForDifferingTypes(LessThan(Symbol("intField"), Symbol("booleanField")))
    assertErrorForDifferingTypes(LessThanOrEqual(Symbol("intField"), Symbol("booleanField")))
    assertErrorForDifferingTypes(GreaterThan(Symbol("intField"), Symbol("booleanField")))
    assertErrorForDifferingTypes(GreaterThanOrEqual(Symbol("intField"), Symbol("booleanField")))

    assertError(EqualTo(Symbol("mapField"), Symbol("mapField")),
      "EqualTo does not support ordering on type map")
    assertError(EqualNullSafe(Symbol("mapField"), Symbol("mapField")),
      "EqualNullSafe does not support ordering on type map")
    assertError(LessThan(Symbol("mapField"), Symbol("mapField")),
      "LessThan does not support ordering on type map")
    assertError(LessThanOrEqual(Symbol("mapField"), Symbol("mapField")),
      "LessThanOrEqual does not support ordering on type map")
    assertError(GreaterThan(Symbol("mapField"), Symbol("mapField")),
      "GreaterThan does not support ordering on type map")
    assertError(GreaterThanOrEqual(Symbol("mapField"), Symbol("mapField")),
      "GreaterThanOrEqual does not support ordering on type map")

    assertError(If(Symbol("intField"), Symbol("stringField"), Symbol("stringField")),
      "type of predicate expression in If should be boolean")
    assertErrorForDifferingTypes(
      If(Symbol("booleanField"), Symbol("intField"), Symbol("booleanField")))

    assertError(
      CaseWhen(Seq((Symbol("booleanField").attr, Symbol("intField").attr),
        (Symbol("booleanField").attr, Symbol("mapField").attr))),
      "THEN and ELSE expressions should all be same type or coercible to a common type")
    assertError(
      CaseKeyWhen(Symbol("intField"), Seq(Symbol("intField"), Symbol("stringField"),
        Symbol("intField"), Symbol("mapField"))),
      "THEN and ELSE expressions should all be same type or coercible to a common type")
    assertError(
      CaseWhen(Seq((Symbol("booleanField").attr, Symbol("intField").attr),
        (Symbol("intField").attr, Symbol("intField").attr))),
      "WHEN expressions in CaseWhen should all be boolean type")
  }

  test("check types for aggregates") {
    // We use AggregateFunction directly at here because the error will be thrown from it
    // instead of from AggregateExpression, which is the wrapper of an AggregateFunction.

    // We will cast String to Double for sum and average
    assertSuccess(Sum(Symbol("stringField")))
    assertSuccess(Average(Symbol("stringField")))
    assertSuccess(Min(Symbol("arrayField")))
    assertSuccess(new BoolAnd(Symbol("booleanField")))
    assertSuccess(new BoolOr(Symbol("booleanField")))

    assertError(Min(Symbol("mapField")), "min does not support ordering on type")
    assertError(Max(Symbol("mapField")), "max does not support ordering on type")
    assertError(Sum(Symbol("booleanField")), "function sum requires numeric type")
    assertError(Average(Symbol("booleanField")), "function average requires numeric type")
  }

  test("check types for others") {
    assertError(CreateArray(Seq(Symbol("intField"), Symbol("booleanField"))),
      "input to function array should all be the same type")
    assertError(Coalesce(Seq(Symbol("intField"), Symbol("booleanField"))),
      "input to function coalesce should all be the same type")
    assertError(Coalesce(Nil), "function coalesce requires at least one argument")
    assertError(new Murmur3Hash(Nil), "function hash requires at least one argument")
    assertError(new XxHash64(Nil), "function xxhash64 requires at least one argument")
    assertError(Explode(Symbol("intField")),
      "input to function explode should be array or map type")
    assertError(PosExplode(Symbol("intField")),
      "input to function explode should be array or map type")
  }

  test("check types for CreateNamedStruct") {
    assertError(
      CreateNamedStruct(Seq("a", "b", 2.0)), "even number of arguments")
    assertError(
      CreateNamedStruct(Seq(1, "a", "b", 2.0)),
      "Only foldable string expressions are allowed to appear at odd position")
    assertError(
      CreateNamedStruct(Seq(Symbol("a").string.at(0), "a", "b", 2.0)),
      "Only foldable string expressions are allowed to appear at odd position")
    assertError(
      CreateNamedStruct(Seq(Literal.create(null, StringType), "a")),
      "Field name should not be null")
  }

  test("check types for CreateMap") {
    assertError(CreateMap(Seq("a", "b", 2.0)), "even number of arguments")
    assertError(
      CreateMap(Seq(Symbol("intField"), Symbol("stringField"),
        Symbol("booleanField"), Symbol("stringField"))),
      "keys of function map should all be the same type")
    assertError(
      CreateMap(Seq(Symbol("stringField"), Symbol("intField"),
        Symbol("stringField"), Symbol("booleanField"))),
      "values of function map should all be the same type")
  }

  test("check types for ROUND/BROUND") {
    assertSuccess(Round(Literal(null), Literal(null)))
    assertSuccess(Round(Symbol("intField"), Literal(1)))

    assertError(Round(Symbol("intField"), Symbol("intField")),
      "Only foldable Expression is allowed")
    assertError(Round(Symbol("intField"), Symbol("booleanField")), "requires int type")
    assertError(Round(Symbol("intField"), Symbol("mapField")), "requires int type")
    assertError(Round(Symbol("booleanField"), Symbol("intField")), "requires numeric type")

    assertSuccess(BRound(Literal(null), Literal(null)))
    assertSuccess(BRound(Symbol("intField"), Literal(1)))

    assertError(BRound(Symbol("intField"), Symbol("intField")),
      "Only foldable Expression is allowed")
    assertError(BRound(Symbol("intField"), Symbol("booleanField")), "requires int type")
    assertError(BRound(Symbol("intField"), Symbol("mapField")), "requires int type")
    assertError(BRound(Symbol("booleanField"), Symbol("intField")), "requires numeric type")
  }

  test("check types for Greatest/Least") {
    for (operator <- Seq[(Seq[Expression] => Expression)](Greatest, Least)) {
      assertError(operator(Seq(Symbol("booleanField"))), "requires at least two arguments")
      assertError(operator(Seq(Symbol("intField"), Symbol("stringField"))),
        "should all have the same type")
      assertError(operator(Seq(Symbol("mapField"), Symbol("mapField"))),
        "does not support ordering")
    }
  }
}
