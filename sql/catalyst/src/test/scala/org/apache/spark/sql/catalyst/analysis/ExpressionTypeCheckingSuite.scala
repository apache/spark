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
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types.StringType

class ExpressionTypeCheckingSuite extends SparkFunSuite {

  val testRelation = LocalRelation(
    'intField.int,
    'stringField.string,
    'booleanField.boolean,
    'complexField.array(StringType))

  def assertError(expr: Expression, errorMessage: String): Unit = {
    val e = intercept[AnalysisException] {
      assertSuccess(expr)
    }
    assert(e.getMessage.contains(
      s"cannot resolve '${expr.prettyString}' due to data type mismatch:"))
    assert(e.getMessage.contains(errorMessage))
  }

  def assertSuccess(expr: Expression): Unit = {
    val analyzed = testRelation.select(expr.as("c")).analyze
    SimpleAnalyzer.checkAnalysis(analyzed)
  }

  def assertErrorForDifferingTypes(expr: Expression): Unit = {
    assertError(expr,
      s"differing types in '${expr.prettyString}' (int and boolean)")
  }

  def assertErrorWithImplicitCast(expr: Expression, errorMessage: String): Unit = {
    val e = intercept[AnalysisException] {
      assertSuccess(expr)
    }
    assert(e.getMessage.contains(errorMessage))
  }

  test("check types for unary arithmetic") {
    assertError(UnaryMinus('stringField), "expected to be of type numeric")
    assertError(Abs('stringField), "expected to be of type numeric")
    assertError(BitwiseNot('stringField), "type (boolean or tinyint or smallint or int or bigint)")
  }

  ignore("check types for binary arithmetic") {
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
    assertErrorForDifferingTypes(MaxOf('intField, 'booleanField))
    assertErrorForDifferingTypes(MinOf('intField, 'booleanField))

    assertError(Add('booleanField, 'booleanField), "operator + accepts numeric type")
    assertError(Subtract('booleanField, 'booleanField), "operator - accepts numeric type")
    assertError(Multiply('booleanField, 'booleanField), "operator * accepts numeric type")
    assertError(Divide('booleanField, 'booleanField), "operator / accepts numeric type")
    assertError(Remainder('booleanField, 'booleanField), "operator % accepts numeric type")

    assertError(BitwiseAnd('booleanField, 'booleanField), "operator & accepts integral type")
    assertError(BitwiseOr('booleanField, 'booleanField), "operator | accepts integral type")
    assertError(BitwiseXor('booleanField, 'booleanField), "operator ^ accepts integral type")

    assertError(MaxOf('complexField, 'complexField), "function maxOf accepts non-complex type")
    assertError(MinOf('complexField, 'complexField), "function minOf accepts non-complex type")
  }

  ignore("check types for predicates") {
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

    assertError(EqualTo('intField, 'complexField), "differing types")
    assertError(EqualNullSafe('intField, 'complexField), "differing types")

    assertErrorForDifferingTypes(LessThan('intField, 'booleanField))
    assertErrorForDifferingTypes(LessThanOrEqual('intField, 'booleanField))
    assertErrorForDifferingTypes(GreaterThan('intField, 'booleanField))
    assertErrorForDifferingTypes(GreaterThanOrEqual('intField, 'booleanField))

    assertError(
      LessThan('complexField, 'complexField), "operator < accepts non-complex type")
    assertError(
      LessThanOrEqual('complexField, 'complexField), "operator <= accepts non-complex type")
    assertError(
      GreaterThan('complexField, 'complexField), "operator > accepts non-complex type")
    assertError(
      GreaterThanOrEqual('complexField, 'complexField), "operator >= accepts non-complex type")

    assertError(
      If('intField, 'stringField, 'stringField),
      "type of predicate expression in If should be boolean")
    assertErrorForDifferingTypes(If('booleanField, 'intField, 'booleanField))

    assertError(
      CaseWhen(Seq('booleanField, 'intField, 'booleanField, 'complexField)),
      "THEN and ELSE expressions should all be same type or coercible to a common type")
    assertError(
      CaseKeyWhen('intField, Seq('intField, 'stringField, 'intField, 'complexField)),
      "THEN and ELSE expressions should all be same type or coercible to a common type")
    assertError(
      CaseWhen(Seq('booleanField, 'intField, 'intField, 'intField)),
      "WHEN expressions in CaseWhen should all be boolean type")
  }

  test("check types for aggregates") {
    // We will cast String to Double for sum and average
    assertSuccess(Sum('stringField))
    assertSuccess(SumDistinct('stringField))
    assertSuccess(Average('stringField))

    assertError(Min('complexField), "function min accepts non-complex type")
    assertError(Max('complexField), "function max accepts non-complex type")
    assertError(Sum('booleanField), "function sum accepts numeric type")
    assertError(SumDistinct('booleanField), "function sumDistinct accepts numeric type")
    assertError(Average('booleanField), "function average accepts numeric type")
  }

  test("check types for others") {
    assertError(CreateArray(Seq('intField, 'booleanField)),
      "input to function array should all be the same type")
    assertError(Coalesce(Seq('intField, 'booleanField)),
      "input to function coalesce should all be the same type")
    assertError(Coalesce(Nil), "input to function coalesce cannot be empty")
    assertError(Explode('intField),
      "input to function explode should be array or map type")
  }

  test("check types for CreateNamedStruct") {
    assertError(
      CreateNamedStruct(Seq("a", "b", 2.0)), "even number of arguments")
    assertError(
      CreateNamedStruct(Seq(1, "a", "b", 2.0)),
        "Odd position only allow foldable and not-null StringType expressions")
    assertError(
      CreateNamedStruct(Seq('a.string.at(0), "a", "b", 2.0)),
        "Odd position only allow foldable and not-null StringType expressions")
  }

  test("check types for ROUND") {
    assertErrorWithImplicitCast(Round(Literal(null), 'booleanField),
      "data type mismatch: argument 2 is expected to be of type int")
    assertErrorWithImplicitCast(Round(Literal(null), 'complexField),
      "data type mismatch: argument 2 is expected to be of type int")
    assertSuccess(Round(Literal(null), Literal(null)))
    assertError(Round('booleanField, 'intField),
      "data type mismatch: argument 1 is expected to be of type numeric")
  }
}
