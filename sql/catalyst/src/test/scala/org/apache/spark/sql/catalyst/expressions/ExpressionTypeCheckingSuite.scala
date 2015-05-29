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
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types.{BooleanType, StringType}
import org.scalatest.FunSuite


class ExpressionTypeCheckingSuite extends FunSuite {

  val testRelation = LocalRelation('a.int, 'b.string, 'c.boolean, 'd.array(StringType))

  def checkError(expr: Expression, errorMessage: String): Unit = {
    val e = intercept[AnalysisException] {
      checkAnalysis(expr)
    }
    assert(e.getMessage.contains(
      s"cannot resolve '${expr.prettyString}' due to data type mismatch:"))
    assert(e.getMessage.contains(errorMessage))
  }

  def checkAnalysis(expr: Expression): Unit = {
    val analyzed = testRelation.select(expr.as("_c")).analyze
    SimpleAnalyzer.checkAnalysis(analyzed)
  }

  test("check types for unary arithmetic") {
    checkError(UnaryMinus('b), "operator - accepts numeric type")
    checkAnalysis(Sqrt('b)) // We will cast String to Double for sqrt
    checkError(Sqrt('c), "function sqrt accepts numeric type")
    checkError(Abs('b), "function abs accepts numeric type")
    checkError(BitwiseNot('b), "operator ~ accepts integral type")
  }

  test("check types for binary arithmetic") {
    // We will cast String to Double for binary arithmetic
    checkAnalysis(Add('a, 'b))
    checkAnalysis(Subtract('a, 'b))
    checkAnalysis(Multiply('a, 'b))
    checkAnalysis(Divide('a, 'b))
    checkAnalysis(Remainder('a, 'b))
    // checkAnalysis(BitwiseAnd('a, 'b))

    val msg = "differing types in BinaryArithmetic, IntegerType != BooleanType"
    checkError(Add('a, 'c), msg)
    checkError(Subtract('a, 'c), msg)
    checkError(Multiply('a, 'c), msg)
    checkError(Divide('a, 'c), msg)
    checkError(Remainder('a, 'c), msg)
    checkError(BitwiseAnd('a, 'c), msg)
    checkError(BitwiseOr('a, 'c), msg)
    checkError(BitwiseXor('a, 'c), msg)
    checkError(MaxOf('a, 'c), msg)
    checkError(MinOf('a, 'c), msg)

    checkError(Add('c, 'c), "operator + accepts numeric type")
    checkError(Subtract('c, 'c), "operator - accepts numeric type")
    checkError(Multiply('c, 'c), "operator * accepts numeric type")
    checkError(Divide('c, 'c), "operator / accepts numeric type")
    checkError(Remainder('c, 'c), "operator % accepts numeric type")

    checkError(BitwiseAnd('c, 'c), "operator & accepts integral type")
    checkError(BitwiseOr('c, 'c), "operator | accepts integral type")
    checkError(BitwiseXor('c, 'c), "operator ^ accepts integral type")

    checkError(MaxOf('d, 'd), "function maxOf accepts non-complex type")
    checkError(MinOf('d, 'd), "function minOf accepts non-complex type")
  }

  test("check types for predicates") {
    // EqualTo don't have type constraint
    checkAnalysis(EqualTo('a, 'c))
    checkAnalysis(EqualNullSafe('a, 'c))

    // We will cast String to Double for binary comparison
    checkAnalysis(LessThan('a, 'b))
    checkAnalysis(LessThanOrEqual('a, 'b))
    checkAnalysis(GreaterThan('a, 'b))
    checkAnalysis(GreaterThanOrEqual('a, 'b))

    val msg = "differing types in BinaryComparison, IntegerType != BooleanType"
    checkError(LessThan('a, 'c), msg)
    checkError(LessThanOrEqual('a, 'c), msg)
    checkError(GreaterThan('a, 'c), msg)
    checkError(GreaterThanOrEqual('a, 'c), msg)

    checkError(LessThan('d, 'd), "operator < accepts non-complex type")
    checkError(LessThanOrEqual('d, 'd), "operator <= accepts non-complex type")
    checkError(GreaterThan('d, 'd), "operator > accepts non-complex type")
    checkError(GreaterThanOrEqual('d, 'd), "operator >= accepts non-complex type")

    checkError(If('a, 'a, 'a), "type of predicate expression in If should be boolean")
    checkError(If('c, 'a, 'b), "differing types in If, IntegerType != StringType")

    // Will write tests for CaseWhen later,
    // as the error reporting of it is not handle by the new interface for now
  }
}
