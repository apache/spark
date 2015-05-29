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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types.StringType

import org.scalatest.FunSuite


class ExpressionTypeCheckingSuite extends FunSuite {

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
      s"differing types in ${expr.getClass.getSimpleName} (IntegerType and BooleanType).")
  }

  test("check types for unary arithmetic") {
    assertError(UnaryMinus('stringField), "operator - accepts numeric type")
    assertSuccess(Sqrt('stringField)) // We will cast String to Double for sqrt
    assertError(Sqrt('booleanField), "function sqrt accepts numeric type")
    assertError(Abs('stringField), "function abs accepts numeric type")
    assertError(BitwiseNot('stringField), "operator ~ accepts integral type")
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

  test("check types for predicates") {
    // We will cast String to Double for binary comparison
    assertSuccess(EqualTo('intField, 'stringField))
    assertSuccess(EqualNullSafe('intField, 'stringField))
    assertSuccess(LessThan('intField, 'stringField))
    assertSuccess(LessThanOrEqual('intField, 'stringField))
    assertSuccess(GreaterThan('intField, 'stringField))
    assertSuccess(GreaterThanOrEqual('intField, 'stringField))

    assertErrorForDifferingTypes(EqualTo('intField, 'booleanField))
    assertErrorForDifferingTypes(EqualNullSafe('intField, 'booleanField))
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

    // Will write tests for CaseWhen later,
    // as the error reporting of it is not handle by the new interface for now
  }
}
