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

import org.scalactic.TripleEqualsSupport.Spread
import org.scalatest.Matchers._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateProjection, GenerateMutableProjection}
import org.apache.spark.sql.catalyst.optimizer.DefaultOptimizer
import org.apache.spark.sql.catalyst.plans.logical.{OneRowRelation, Project}

/**
 * A few helper functions for expression evaluation testing. Mixin this trait to use them.
 */
trait ExpressionEvalHelper {
  self: SparkFunSuite =>

  protected def create_row(values: Any*): InternalRow = {
    new GenericRow(values.map(CatalystTypeConverters.convertToCatalyst).toArray)
  }

  protected def checkEvaluation(
      expression: Expression, expected: Any, inputRow: InternalRow = EmptyRow): Unit = {
    checkEvaluationWithoutCodegen(expression, expected, inputRow)
    checkEvaluationWithGeneratedMutableProjection(expression, expected, inputRow)
    checkEvaluationWithGeneratedProjection(expression, expected, inputRow)
    checkEvaluationWithOptimization(expression, expected, inputRow)
  }

  protected def evaluate(expression: Expression, inputRow: InternalRow = EmptyRow): Any = {
    expression.eval(inputRow)
  }

  protected def checkEvaluationWithoutCodegen(
      expression: Expression,
      expected: Any,
      inputRow: InternalRow = EmptyRow): Unit = {
    val actual = try evaluate(expression, inputRow) catch {
      case e: Exception => fail(s"Exception evaluating $expression", e)
    }
    if (actual != expected) {
      val input = if (inputRow == EmptyRow) "" else s", input: $inputRow"
      fail(s"Incorrect evaluation (codegen off): $expression, " +
        s"actual: $actual, " +
        s"expected: $expected$input")
    }
  }

  protected def checkEvaluationWithGeneratedMutableProjection(
      expression: Expression,
      expected: Any,
      inputRow: InternalRow = EmptyRow): Unit = {

    val plan = try {
      GenerateMutableProjection.generate(Alias(expression, s"Optimized($expression)")() :: Nil)()
    } catch {
      case e: Throwable =>
        val ctx = GenerateProjection.newCodeGenContext()
        val evaluated = expression.gen(ctx)
        fail(
          s"""
            |Code generation of $expression failed:
            |${evaluated.code}
            |$e
          """.stripMargin)
    }

    val actual = plan(inputRow).apply(0)
    if (actual != expected) {
      val input = if (inputRow == EmptyRow) "" else s", input: $inputRow"
      fail(s"Incorrect Evaluation: $expression, actual: $actual, expected: $expected$input")
    }
  }

  protected def checkEvaluationWithGeneratedProjection(
      expression: Expression,
      expected: Any,
      inputRow: InternalRow = EmptyRow): Unit = {
    val ctx = GenerateProjection.newCodeGenContext()
    lazy val evaluated = expression.gen(ctx)

    val plan = try {
      GenerateProjection.generate(Alias(expression, s"Optimized($expression)")() :: Nil)
    } catch {
      case e: Throwable =>
        fail(
          s"""
            |Code generation of $expression failed:
            |${evaluated.code}
            |$e
          """.stripMargin)
    }

    val actual = plan(inputRow)
    val expectedRow = new GenericRow(Array[Any](CatalystTypeConverters.convertToCatalyst(expected)))
    if (actual.hashCode() != expectedRow.hashCode()) {
      fail(
        s"""
          |Mismatched hashCodes for values: $actual, $expectedRow
          |Hash Codes: ${actual.hashCode()} != ${expectedRow.hashCode()}
          |Expressions: $expression
          |Code: $evaluated
        """.stripMargin)
    }
    if (actual != expectedRow) {
      val input = if (inputRow == EmptyRow) "" else s", input: $inputRow"
      fail(s"Incorrect Evaluation: $expression, actual: $actual, expected: $expected$input")
    }
  }

  protected def checkEvaluationWithOptimization(
      expression: Expression,
      expected: Any,
      inputRow: InternalRow = EmptyRow): Unit = {
    val plan = Project(Alias(expression, s"Optimized($expression)")() :: Nil, OneRowRelation)
    val optimizedPlan = DefaultOptimizer.execute(plan)
    checkEvaluationWithoutCodegen(optimizedPlan.expressions.head, expected, inputRow)
  }

  protected def checkDoubleEvaluation(
      expression: Expression,
      expected: Spread[Double],
      inputRow: InternalRow = EmptyRow): Unit = {
    val actual = try evaluate(expression, inputRow) catch {
      case e: Exception => fail(s"Exception evaluating $expression", e)
    }
    actual.asInstanceOf[Double] shouldBe expected
  }
}
