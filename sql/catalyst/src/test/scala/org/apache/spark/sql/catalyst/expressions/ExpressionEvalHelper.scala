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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.optimizer.DefaultOptimizer
import org.apache.spark.sql.catalyst.plans.logical.{OneRowRelation, Project}

/**
 * A few helper functions for expression evaluation testing. Mixin this trait to use them.
 */
trait ExpressionEvalHelper {
  self: SparkFunSuite =>

  protected def create_row(values: Any*): InternalRow = {
    InternalRow.fromSeq(values.map(CatalystTypeConverters.convertToCatalyst))
  }

  protected def checkEvaluation(
      expression: => Expression, expected: Any, inputRow: InternalRow = EmptyRow): Unit = {
    val catalystValue = CatalystTypeConverters.convertToCatalyst(expected)
    checkEvaluationWithoutCodegen(expression, catalystValue, inputRow)
    checkEvaluationWithGeneratedMutableProjection(expression, catalystValue, inputRow)
    checkEvaluationWithGeneratedProjection(expression, catalystValue, inputRow)
    if (GenerateUnsafeProjection.canSupport(expression.dataType)) {
      checkEvalutionWithUnsafeProjection(expression, catalystValue, inputRow)
    }
    checkEvaluationWithOptimization(expression, catalystValue, inputRow)
  }

  /**
   * Check the equality between result of expression and expected value, it will handle
   * Array[Byte] and Spread[Double].
   */
  protected def checkResult(result: Any, expected: Any): Boolean = {
    (result, expected) match {
      case (result: Array[Byte], expected: Array[Byte]) =>
        java.util.Arrays.equals(result, expected)
      case (result: Double, expected: Spread[Double]) =>
        expected.isWithin(result)
      case _ => result == expected
    }
  }

  protected def evaluate(expression: Expression, inputRow: InternalRow = EmptyRow): Any = {
    expression.foreach {
      case n: Nondeterministic => n.setInitialValues()
      case _ =>
    }
    expression.eval(inputRow)
  }

  protected def generateProject(
      generator: => Projection,
      expression: Expression): Projection = {
    try {
      generator
    } catch {
      case e: Throwable =>
        fail(
          s"""
            |Code generation of $expression failed:
            |$e
            |${e.getStackTraceString}
          """.stripMargin)
    }
  }

  protected def checkEvaluationWithoutCodegen(
      expression: Expression,
      expected: Any,
      inputRow: InternalRow = EmptyRow): Unit = {

    val actual = try evaluate(expression, inputRow) catch {
      case e: Exception => fail(s"Exception evaluating $expression", e)
    }
    if (!checkResult(actual, expected)) {
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

    val plan = generateProject(
      GenerateMutableProjection.generate(Alias(expression, s"Optimized($expression)")() :: Nil)(),
      expression)

    val actual = plan(inputRow).get(0, expression.dataType)
    if (!checkResult(actual, expected)) {
      val input = if (inputRow == EmptyRow) "" else s", input: $inputRow"
      fail(s"Incorrect evaluation: $expression, actual: $actual, expected: $expected$input")
    }
  }

  protected def checkEvaluationWithGeneratedProjection(
      expression: Expression,
      expected: Any,
      inputRow: InternalRow = EmptyRow): Unit = {

    val plan = generateProject(
      GenerateProjection.generate(Alias(expression, s"Optimized($expression)")() :: Nil),
      expression)

    val actual = plan(inputRow)
    val expectedRow = InternalRow(expected)

    // We reimplement hashCode in generated `SpecificRow`, make sure it's consistent with our
    // interpreted version.
    if (actual.hashCode() != expectedRow.hashCode()) {
      val ctx = new CodeGenContext
      val evaluated = expression.gen(ctx)
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
      fail("Incorrect Evaluation in codegen mode: " +
        s"$expression, actual: $actual, expected: $expectedRow$input")
    }
    if (actual.copy() != expectedRow) {
      fail(s"Copy of generated Row is wrong: actual: ${actual.copy()}, expected: $expectedRow")
    }
  }

  protected def checkEvalutionWithUnsafeProjection(
      expression: Expression,
      expected: Any,
      inputRow: InternalRow = EmptyRow): Unit = {

    val plan = generateProject(
      GenerateUnsafeProjection.generate(Alias(expression, s"Optimized($expression)")() :: Nil),
      expression)

    val unsafeRow = plan(inputRow)
    val input = if (inputRow == EmptyRow) "" else s", input: $inputRow"

    if (expected == null) {
      if (!unsafeRow.isNullAt(0)) {
        val expectedRow = InternalRow(expected)
        fail("Incorrect evaluation in unsafe mode: " +
          s"$expression, actual: $unsafeRow, expected: $expectedRow$input")
      }
    } else {
      val lit = InternalRow(expected)
      val expectedRow = UnsafeProjection.create(Array(expression.dataType)).apply(lit)
      if (unsafeRow != expectedRow) {
        fail("Incorrect evaluation in unsafe mode: " +
          s"$expression, actual: $unsafeRow, expected: $expectedRow$input")
      }
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
      expression: => Expression,
      expected: Spread[Double],
      inputRow: InternalRow = EmptyRow): Unit = {
    checkEvaluationWithoutCodegen(expression, expected)
    checkEvaluationWithGeneratedMutableProjection(expression, expected)
    checkEvaluationWithOptimization(expression, expected)

    var plan = generateProject(
      GenerateProjection.generate(Alias(expression, s"Optimized($expression)")() :: Nil),
      expression)
    var actual = plan(inputRow).get(0, expression.dataType)
    assert(checkResult(actual, expected))

    plan = generateProject(
      GenerateUnsafeProjection.generate(Alias(expression, s"Optimized($expression)")() :: Nil),
      expression)
    actual = FromUnsafeProjection(expression.dataType :: Nil)(
      plan(inputRow)).get(0, expression.dataType)
    assert(checkResult(actual, expected))
  }
}
