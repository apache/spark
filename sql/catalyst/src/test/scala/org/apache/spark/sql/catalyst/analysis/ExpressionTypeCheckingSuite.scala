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
    "intField".attr.int,
    "stringField".attr.string,
    "booleanField".attr.boolean,
    "decimalField".attr.decimal(8, 0),
    "arrayField".attr.array(StringType),
    "mapField".attr.mapAttr(StringType, LongType))

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
    assertError(BitwiseNot("stringField".attr), "requires integral type")
  }

  test("check types for binary arithmetic") {
    // We will cast String to Double for binary arithmetic
    assertSuccess(Add("intField".attr, "stringField".attr))
    assertSuccess(Subtract("intField".attr, "stringField".attr))
    assertSuccess(Multiply("intField".attr, "stringField".attr))
    assertSuccess(Divide("intField".attr, "stringField".attr))
    assertSuccess(Remainder("intField".attr, "stringField".attr))
    // checkAnalysis(BitwiseAnd("intField".attr, "stringField".attr)

    assertErrorForDifferingTypes(Add("intField".attr, "booleanField".attr))
    assertErrorForDifferingTypes(Subtract("intField".attr, "booleanField".attr))
    assertErrorForDifferingTypes(Multiply("intField".attr, "booleanField".attr))
    assertErrorForDifferingTypes(Divide("intField".attr, "booleanField".attr))
    assertErrorForDifferingTypes(Remainder("intField".attr, "booleanField".attr))
    assertErrorForDifferingTypes(BitwiseAnd("intField".attr, "booleanField".attr))
    assertErrorForDifferingTypes(BitwiseOr("intField".attr, "booleanField".attr))
    assertErrorForDifferingTypes(BitwiseXor("intField".attr, "booleanField".attr))

    assertError(Add("booleanField".attr, "booleanField".attr),
      "requires (numeric or interval) type")
    assertError(Subtract("booleanField".attr, "booleanField".attr),
      "requires (numeric or interval) type")
    assertError(Multiply("booleanField".attr, "booleanField".attr), "requires numeric type")
    assertError(Divide("booleanField".attr, "booleanField".attr),
      "requires (double or decimal) type")
    assertError(Remainder("booleanField".attr, "booleanField".attr), "requires numeric type")

    assertError(BitwiseAnd("booleanField".attr, "booleanField".attr),
      "requires integral type")
    assertError(BitwiseOr("booleanField".attr, "booleanField".attr), "requires integral type")
    assertError(BitwiseXor("booleanField".attr, "booleanField".attr),
      "requires integral type")
  }

  test("check types for predicates") {
    // We will cast String to Double for binary comparison
    assertSuccess(EqualTo("intField".attr, "stringField".attr))
    assertSuccess(EqualNullSafe("intField".attr, "stringField".attr))
    assertSuccess(LessThan("intField".attr, "stringField".attr))
    assertSuccess(LessThanOrEqual("intField".attr, "stringField".attr))
    assertSuccess(GreaterThan("intField".attr, "stringField".attr))
    assertSuccess(GreaterThanOrEqual("intField".attr, "stringField".attr))

    // We will transform EqualTo with numeric and boolean types to CaseKeyWhen
    assertSuccess(EqualTo("intField".attr, "booleanField".attr))
    assertSuccess(EqualNullSafe("intField".attr, "booleanField".attr))

    assertErrorForDifferingTypes(EqualTo("intField".attr, "mapField".attr))
    assertErrorForDifferingTypes(EqualNullSafe("intField".attr, "mapField".attr))
    assertErrorForDifferingTypes(LessThan("intField".attr, "booleanField".attr))
    assertErrorForDifferingTypes(LessThanOrEqual("intField".attr, "booleanField".attr))
    assertErrorForDifferingTypes(GreaterThan("intField".attr, "booleanField".attr))
    assertErrorForDifferingTypes(GreaterThanOrEqual("intField".attr, "booleanField".attr))

    assertError(EqualTo("mapField".attr, "mapField".attr),
      "EqualTo does not support ordering on type map")
    assertError(EqualNullSafe("mapField".attr, "mapField".attr),
      "EqualNullSafe does not support ordering on type map")
    assertError(LessThan("mapField".attr, "mapField".attr),
      "LessThan does not support ordering on type map")
    assertError(LessThanOrEqual("mapField".attr, "mapField".attr),
      "LessThanOrEqual does not support ordering on type map")
    assertError(GreaterThan("mapField".attr, "mapField".attr),
      "GreaterThan does not support ordering on type map")
    assertError(GreaterThanOrEqual("mapField".attr, "mapField".attr),
      "GreaterThanOrEqual does not support ordering on type map")

    assertError(If("intField".attr, "stringField".attr, "stringField".attr),
      "type of predicate expression in If should be boolean")
    assertErrorForDifferingTypes(
      If("booleanField".attr, "intField".attr, "booleanField".attr))

    assertError(
      CaseWhen(Seq(("booleanField".attr, "intField".attr.attr),
        ("booleanField".attr, "mapField".attr))),
      "THEN and ELSE expressions should all be same type or coercible to a common type")
    assertError(
      CaseKeyWhen("intField".attr, Seq("intField".attr, "stringField".attr,
        "intField".attr, "mapField".attr)),
      "THEN and ELSE expressions should all be same type or coercible to a common type")
    assertError(
      CaseWhen(Seq(("booleanField".attr, "intField".attr.attr),
        ("intField".attr.attr, "intField".attr.attr))),
      "WHEN expressions in CaseWhen should all be boolean type")
  }

  test("check types for aggregates") {
    // We use AggregateFunction directly at here because the error will be thrown from it
    // instead of from AggregateExpression, which is the wrapper of an AggregateFunction.

    // We will cast String to Double for sum and average
    assertSuccess(Sum("stringField".attr))
    assertSuccess(Average("stringField".attr))
    assertSuccess(Min("arrayField".attr))
    assertSuccess(new BoolAnd("booleanField".attr))
    assertSuccess(new BoolOr("booleanField".attr))

    assertError(Min("mapField".attr), "min does not support ordering on type")
    assertError(Max("mapField".attr), "max does not support ordering on type")
    assertError(Sum("booleanField".attr), "function sum requires numeric type")
    assertError(Average("booleanField".attr), "function average requires numeric type")
  }

  test("check types for others") {
    assertError(CreateArray(Seq("intField".attr, "booleanField".attr)),
      "input to function array should all be the same type")
    assertError(Coalesce(Seq("intField".attr, "booleanField".attr)),
      "input to function coalesce should all be the same type")
    assertError(Coalesce(Nil), "function coalesce requires at least one argument")
    assertError(new Murmur3Hash(Nil), "function hash requires at least one argument")
    assertError(new XxHash64(Nil), "function xxhash64 requires at least one argument")
    assertError(Explode("intField".attr),
      "input to function explode should be array or map type")
    assertError(PosExplode("intField".attr),
      "input to function explode should be array or map type")
  }

  test("check types for CreateNamedStruct") {
    assertError(
      CreateNamedStruct(Seq("a", "b", 2.0)), "even number of arguments")
    assertError(
      CreateNamedStruct(Seq(1, "a", "b", 2.0)),
      "Only foldable string expressions are allowed to appear at odd position")
    assertError(
      CreateNamedStruct(Seq("a".attr.string.at(0), "a", "b", 2.0)),
      "Only foldable string expressions are allowed to appear at odd position")
    assertError(
      CreateNamedStruct(Seq(Literal.create(null, StringType), "a")),
      "Field name should not be null")
  }

  test("check types for CreateMap") {
    assertError(CreateMap(Seq("a", "b", 2.0)), "even number of arguments")
    assertError(
      CreateMap(Seq("intField".attr, "stringField".attr,
        "booleanField".attr, "stringField".attr)),
      "keys of function map should all be the same type")
    assertError(
      CreateMap(Seq("stringField".attr, "intField".attr,
        "stringField".attr, "booleanField".attr)),
      "values of function map should all be the same type")
  }

  test("check types for ROUND/BROUND") {
    assertSuccess(Round(Literal(null), Literal(null)))
    assertSuccess(Round("intField".attr, Literal(1)))

    assertError(Round("intField".attr, "intField".attr),
      "Only foldable Expression is allowed")
    assertError(Round("intField".attr, "booleanField".attr), "requires int type")
    assertError(Round("intField".attr, "mapField".attr), "requires int type")
    assertError(Round("booleanField".attr, "intField".attr), "requires numeric type")

    assertSuccess(BRound(Literal(null), Literal(null)))
    assertSuccess(BRound("intField".attr, Literal(1)))

    assertError(BRound("intField".attr, "intField".attr),
      "Only foldable Expression is allowed")
    assertError(BRound("intField".attr, "booleanField".attr), "requires int type")
    assertError(BRound("intField".attr, "mapField".attr), "requires int type")
    assertError(BRound("booleanField".attr, "intField".attr), "requires numeric type")
  }

  test("check types for Greatest/Least") {
    for (operator <- Seq[(Seq[Expression] => Expression)](Greatest, Least)) {
      assertError(operator(Seq("booleanField".attr)), "requires at least two arguments")
      assertError(operator(Seq("intField".attr, "stringField".attr)),
        "should all have the same type")
      assertError(operator(Seq("mapField".attr, "mapField".attr)),
        "does not support ordering")
    }
  }
}
