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
import org.apache.spark.sql.types.{LongType, StringType, TypeCollection}

class ExpressionTypeCheckingSuite extends SparkFunSuite {

  val testRelation = LocalRelation(
    'intField.int,
    'stringField.string,
    'booleanField.boolean,
    'decimalField.decimal(8, 0),
    'arrayField.array(StringType),
    'mapField.map(StringType, LongType))

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
    assertError(UnaryMinus('stringField), "(numeric or calendarinterval) type")
    assertError(Abs('stringField), "requires numeric type")
    assertError(BitwiseNot('stringField), "requires integral type")
  }

  test("check types for binary arithmetic") {
    // We will cast String to Double for binary arithmetic
    assertSuccess(Add('intField, 'stringField))
    assertSuccess(Subtract('intField, 'stringField))
    assertSuccess(Multiply('intField, 'stringField))
    assertSuccess(Divide('intField, 'stringField))
    assertSuccess(Remainder('intField, 'stringField))
    // checkAnalysis(BitwiseAnd('intField, 'stringField))

    assertErrorForDifferingTypes(Add('intField, 'booleanField))
    assertErrorForDifferingTypes(Subtract('intField, 'booleanField))
    assertErrorForDifferingTypes(Multiply('intField, 'booleanField))
    assertErrorForDifferingTypes(Divide('intField, 'booleanField))
    assertErrorForDifferingTypes(Remainder('intField, 'booleanField))
    assertErrorForDifferingTypes(BitwiseAnd('intField, 'booleanField))
    assertErrorForDifferingTypes(BitwiseOr('intField, 'booleanField))
    assertErrorForDifferingTypes(BitwiseXor('intField, 'booleanField))

    assertError(Add('booleanField, 'booleanField), "requires (numeric or calendarinterval) type")
    assertError(Subtract('booleanField, 'booleanField),
      "requires (numeric or calendarinterval) type")
    assertError(Multiply('booleanField, 'booleanField), "requires numeric type")
    assertError(Divide('booleanField, 'booleanField), "requires (double or decimal) type")
    assertError(Remainder('booleanField, 'booleanField), "requires numeric type")

    assertError(BitwiseAnd('booleanField, 'booleanField), "requires integral type")
    assertError(BitwiseOr('booleanField, 'booleanField), "requires integral type")
    assertError(BitwiseXor('booleanField, 'booleanField), "requires integral type")
  }

  test("check types for predicates") {
    // We will cast String to Double for binary comparison
    assertSuccess(EqualTo('intField, 'stringField))
    assertSuccess(EqualNullSafe('intField, 'stringField))
    assertSuccess(LessThan('intField, 'stringField))
    assertSuccess(LessThanOrEqual('intField, 'stringField))
    assertSuccess(GreaterThan('intField, 'stringField))
    assertSuccess(GreaterThanOrEqual('intField, 'stringField))

    // We will transform EqualTo with numeric and boolean types to CaseKeyWhen
    assertSuccess(EqualTo('intField, 'booleanField))
    assertSuccess(EqualNullSafe('intField, 'booleanField))

    assertErrorForDifferingTypes(EqualTo('intField, 'mapField))
    assertErrorForDifferingTypes(EqualNullSafe('intField, 'mapField))
    assertErrorForDifferingTypes(LessThan('intField, 'booleanField))
    assertErrorForDifferingTypes(LessThanOrEqual('intField, 'booleanField))
    assertErrorForDifferingTypes(GreaterThan('intField, 'booleanField))
    assertErrorForDifferingTypes(GreaterThanOrEqual('intField, 'booleanField))

    assertError(EqualTo('mapField, 'mapField), "Cannot use map type in EqualTo")
    assertError(EqualNullSafe('mapField, 'mapField), "Cannot use map type in EqualNullSafe")
    assertError(LessThan('mapField, 'mapField),
      s"requires ${TypeCollection.Ordered.simpleString} type")
    assertError(LessThanOrEqual('mapField, 'mapField),
      s"requires ${TypeCollection.Ordered.simpleString} type")
    assertError(GreaterThan('mapField, 'mapField),
      s"requires ${TypeCollection.Ordered.simpleString} type")
    assertError(GreaterThanOrEqual('mapField, 'mapField),
      s"requires ${TypeCollection.Ordered.simpleString} type")

    assertError(If('intField, 'stringField, 'stringField),
      "type of predicate expression in If should be boolean")
    assertErrorForDifferingTypes(If('booleanField, 'intField, 'booleanField))

    assertError(
      CaseWhen(Seq(('booleanField.attr, 'intField.attr), ('booleanField.attr, 'mapField.attr))),
      "THEN and ELSE expressions should all be same type or coercible to a common type")
    assertError(
      CaseKeyWhen('intField, Seq('intField, 'stringField, 'intField, 'mapField)),
      "THEN and ELSE expressions should all be same type or coercible to a common type")
    assertError(
      CaseWhen(Seq(('booleanField.attr, 'intField.attr), ('intField.attr, 'intField.attr))),
      "WHEN expressions in CaseWhen should all be boolean type")
  }

  test("check types for aggregates") {
    // We use AggregateFunction directly at here because the error will be thrown from it
    // instead of from AggregateExpression, which is the wrapper of an AggregateFunction.

    // We will cast String to Double for sum and average
    assertSuccess(Sum('stringField))
    assertSuccess(Average('stringField))
    assertSuccess(Min('arrayField))

    assertError(Min('mapField), "min does not support ordering on type")
    assertError(Max('mapField), "max does not support ordering on type")
    assertError(Sum('booleanField), "function sum requires numeric type")
    assertError(Average('booleanField), "function average requires numeric type")
  }

  test("check types for others") {
    assertError(CreateArray(Seq('intField, 'booleanField)),
      "input to function array should all be the same type")
    assertError(Coalesce(Seq('intField, 'booleanField)),
      "input to function coalesce should all be the same type")
    assertError(Coalesce(Nil), "input to function coalesce cannot be empty")
    assertError(new Murmur3Hash(Nil), "function hash requires at least one argument")
    assertError(Explode('intField),
      "input to function explode should be array or map type")
    assertError(PosExplode('intField),
      "input to function explode should be array or map type")
  }

  test("check types for CreateNamedStruct") {
    assertError(
      CreateNamedStruct(Seq("a", "b", 2.0)), "even number of arguments")
    assertError(
      CreateNamedStruct(Seq(1, "a", "b", 2.0)),
      "Only foldable StringType expressions are allowed to appear at odd position")
    assertError(
      CreateNamedStruct(Seq('a.string.at(0), "a", "b", 2.0)),
      "Only foldable StringType expressions are allowed to appear at odd position")
    assertError(
      CreateNamedStruct(Seq(Literal.create(null, StringType), "a")),
      "Field name should not be null")
  }

  test("check types for CreateMap") {
    assertError(CreateMap(Seq("a", "b", 2.0)), "even number of arguments")
    assertError(
      CreateMap(Seq('intField, 'stringField, 'booleanField, 'stringField)),
      "keys of function map should all be the same type")
    assertError(
      CreateMap(Seq('stringField, 'intField, 'stringField, 'booleanField)),
      "values of function map should all be the same type")
  }

  test("check types for ROUND/BROUND") {
    assertSuccess(Round(Literal(null), Literal(null)))
    assertSuccess(Round('intField, Literal(1)))

    assertError(Round('intField, 'intField), "Only foldable Expression is allowed")
    assertError(Round('intField, 'booleanField), "requires int type")
    assertError(Round('intField, 'mapField), "requires int type")
    assertError(Round('booleanField, 'intField), "requires numeric type")

    assertSuccess(BRound(Literal(null), Literal(null)))
    assertSuccess(BRound('intField, Literal(1)))

    assertError(BRound('intField, 'intField), "Only foldable Expression is allowed")
    assertError(BRound('intField, 'booleanField), "requires int type")
    assertError(BRound('intField, 'mapField), "requires int type")
    assertError(BRound('booleanField, 'intField), "requires numeric type")
  }

  test("check types for Greatest/Least") {
    for (operator <- Seq[(Seq[Expression] => Expression)](Greatest, Least)) {
      assertError(operator(Seq('booleanField)), "requires at least 2 arguments")
      assertError(operator(Seq('intField, 'stringField)), "should all have the same type")
      assertError(operator(Seq('mapField, 'mapField)), "does not support ordering")
    }
  }
}
