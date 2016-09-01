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

import org.scalacheck.Gen
import org.scalactic.TripleEqualsSupport.Spread
import org.scalatest.prop.GeneratorDrivenPropertyChecks

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.optimizer.SimpleTestOptimizer
import org.apache.spark.sql.catalyst.plans.logical.{OneRowRelation, Project}
import org.apache.spark.sql.types.DataType

/**
 * A few helper functions for expression evaluation testing. Mixin this trait to use them.
 */
trait ExpressionEvalHelper extends GeneratorDrivenPropertyChecks {
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
    // SPARK-16489 Explicitly doing code generation twice so code gen will fail if
    // some expression is reusing variable names across different instances.
    // This behavior is tested in ExpressionEvalHelperSuite.
    val plan = generateProject(
      GenerateUnsafeProjection.generate(
        Alias(expression, s"Optimized($expression)1")() ::
          Alias(expression, s"Optimized($expression)2")() :: Nil),
      expression)

    val unsafeRow = plan(inputRow)
    val input = if (inputRow == EmptyRow) "" else s", input: $inputRow"

    if (expected == null) {
      if (!unsafeRow.isNullAt(0)) {
        val expectedRow = InternalRow(expected, expected)
        fail("Incorrect evaluation in unsafe mode: " +
          s"$expression, actual: $unsafeRow, expected: $expectedRow$input")
      }
    } else {
      val lit = InternalRow(expected, expected)
      val expectedRow =
        UnsafeProjection.create(Array(expression.dataType, expression.dataType)).apply(lit)
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
    val optimizedPlan = SimpleTestOptimizer.execute(plan)
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

  /**
   * Test evaluation results between Interpreted mode and Codegen mode, making sure we have
   * consistent result regardless of the evaluation method we use.
   *
   * This method test against unary expressions by feeding them arbitrary literals of `dataType`.
   */
  def checkConsistencyBetweenInterpretedAndCodegen(
      c: Expression => Expression,
      dataType: DataType): Unit = {
    forAll (LiteralGenerator.randomGen(dataType)) { (l: Literal) =>
      cmpInterpretWithCodegen(EmptyRow, c(l))
    }
  }

  /**
   * Test evaluation results between Interpreted mode and Codegen mode, making sure we have
   * consistent result regardless of the evaluation method we use.
   *
   * This method test against binary expressions by feeding them arbitrary literals of `dataType1`
   * and `dataType2`.
   */
  def checkConsistencyBetweenInterpretedAndCodegen(
      c: (Expression, Expression) => Expression,
      dataType1: DataType,
      dataType2: DataType): Unit = {
    forAll (
      LiteralGenerator.randomGen(dataType1),
      LiteralGenerator.randomGen(dataType2)
    ) { (l1: Literal, l2: Literal) =>
      cmpInterpretWithCodegen(EmptyRow, c(l1, l2))
    }
  }

  /**
   * Test evaluation results between Interpreted mode and Codegen mode, making sure we have
   * consistent result regardless of the evaluation method we use.
   *
   * This method test against ternary expressions by feeding them arbitrary literals of `dataType1`,
   * `dataType2` and `dataType3`.
   */
  def checkConsistencyBetweenInterpretedAndCodegen(
      c: (Expression, Expression, Expression) => Expression,
      dataType1: DataType,
      dataType2: DataType,
      dataType3: DataType): Unit = {
    forAll (
      LiteralGenerator.randomGen(dataType1),
      LiteralGenerator.randomGen(dataType2),
      LiteralGenerator.randomGen(dataType3)
    ) { (l1: Literal, l2: Literal, l3: Literal) =>
      cmpInterpretWithCodegen(EmptyRow, c(l1, l2, l3))
    }
  }

  /**
   * Test evaluation results between Interpreted mode and Codegen mode, making sure we have
   * consistent result regardless of the evaluation method we use.
   *
   * This method test against expressions take Seq[Expression] as input by feeding them
   * arbitrary length Seq of arbitrary literal of `dataType`.
   */
  def checkConsistencyBetweenInterpretedAndCodegen(
      c: Seq[Expression] => Expression,
      dataType: DataType,
      minNumElements: Int = 0): Unit = {
    forAll (Gen.listOf(LiteralGenerator.randomGen(dataType))) { (literals: Seq[Literal]) =>
      whenever(literals.size >= minNumElements) {
        cmpInterpretWithCodegen(EmptyRow, c(literals))
      }
    }
  }

  private def cmpInterpretWithCodegen(inputRow: InternalRow, expr: Expression): Unit = {
    val interpret = try {
      evaluate(expr, inputRow)
    } catch {
      case e: Exception => fail(s"Exception evaluating $expr", e)
    }

    val plan = generateProject(
      GenerateMutableProjection.generate(Alias(expr, s"Optimized($expr)")() :: Nil)(),
      expr)
    val codegen = plan(inputRow).get(0, expr.dataType)

    if (!compareResults(interpret, codegen)) {
      fail(s"Incorrect evaluation: $expr, interpret: $interpret, codegen: $codegen")
    }
  }

  /**
   * Check the equality between result of expression and expected value, it will handle
   * Array[Byte] and Spread[Double].
   */
  private[this] def compareResults(result: Any, expected: Any): Boolean = {
    (result, expected) match {
      case (result: Array[Byte], expected: Array[Byte]) =>
        java.util.Arrays.equals(result, expected)
      case (result: Double, expected: Spread[Double]) =>
        expected.isWithin(result)
      case (result: Double, expected: Double) if result.isNaN && expected.isNaN =>
        true
      case (result: Float, expected: Float) if result.isNaN && expected.isNaN =>
        true
      case _ => result == expected
    }
  }
}
