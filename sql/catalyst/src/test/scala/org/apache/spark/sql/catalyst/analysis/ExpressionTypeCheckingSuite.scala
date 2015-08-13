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
import org.apache.spark.sql.types.{TypeCollection, StringType}

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
      s"differing types in '${expr.prettyString}'")
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
    assertErrorForDifferingTypes(MaxOf('intField, 'booleanField))
    assertErrorForDifferingTypes(MinOf('intField, 'booleanField))

    assertError(Add('booleanField, 'booleanField), "requires (numeric or calendarinterval) type")
    assertError(Subtract('booleanField, 'booleanField),
      "requires (numeric or calendarinterval) type")
    assertError(Multiply('booleanField, 'booleanField), "requires numeric type")
    assertError(Divide('booleanField, 'booleanField), "requires numeric type")
    assertError(Remainder('booleanField, 'booleanField), "requires numeric type")

    assertError(BitwiseAnd('booleanField, 'booleanField), "requires integral type")
    assertError(BitwiseOr('booleanField, 'booleanField), "requires integral type")
    assertError(BitwiseXor('booleanField, 'booleanField), "requires integral type")

    assertError(MaxOf('complexField, 'complexField),
      s"requires ${TypeCollection.Ordered.simpleString} type")
    assertError(MinOf('complexField, 'complexField),
      s"requires ${TypeCollection.Ordered.simpleString} type")
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

    assertErrorForDifferingTypes(EqualTo('intField, 'complexField))
    assertErrorForDifferingTypes(EqualNullSafe('intField, 'complexField))
    assertErrorForDifferingTypes(LessThan('intField, 'booleanField))
    assertErrorForDifferingTypes(LessThanOrEqual('intField, 'booleanField))
    assertErrorForDifferingTypes(GreaterThan('intField, 'booleanField))
    assertErrorForDifferingTypes(GreaterThanOrEqual('intField, 'booleanField))

    assertError(LessThan('complexField, 'complexField),
      s"requires ${TypeCollection.Ordered.simpleString} type")
    assertError(LessThanOrEqual('complexField, 'complexField),
      s"requires ${TypeCollection.Ordered.simpleString} type")
    assertError(GreaterThan('complexField, 'complexField),
      s"requires ${TypeCollection.Ordered.simpleString} type")
    assertError(GreaterThanOrEqual('complexField, 'complexField),
      s"requires ${TypeCollection.Ordered.simpleString} type")

    assertError(If('intField, 'stringField, 'stringField),
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

    assertError(Min('complexField), "min does not support ordering on type")
    assertError(Max('complexField), "max does not support ordering on type")
    assertError(Sum('booleanField), "function sum requires numeric type")
    assertError(SumDistinct('booleanField), "function sumDistinct requires numeric type")
    assertError(Average('booleanField), "function average requires numeric type")
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
        "Only foldable StringType expressions are allowed to appear at odd position")
    assertError(
      CreateNamedStruct(Seq('a.string.at(0), "a", "b", 2.0)),
        "Only foldable StringType expressions are allowed to appear at odd position")
    assertError(
      CreateNamedStruct(Seq(Literal.create(null, StringType), "a")),
        "Field name should not be null")
  }

  test("check types for ROUND") {
    assertSuccess(Round(Literal(null), Literal(null)))
    assertSuccess(Round('intField, Literal(1)))

    assertError(Round('intField, 'intField), "Only foldable Expression is allowed")
    assertError(Round('intField, 'booleanField), "requires int type")
    assertError(Round('intField, 'complexField), "requires int type")
    assertError(Round('booleanField, 'intField), "requires numeric type")
  }
}
