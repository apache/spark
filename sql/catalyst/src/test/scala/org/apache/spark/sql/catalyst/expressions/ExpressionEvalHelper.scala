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

import scala.reflect.ClassTag

import org.scalacheck.Gen
import org.scalactic.TripleEqualsSupport.Spread
import org.scalatest.exceptions.TestFailedException
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.ResolveTimeZone
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.optimizer.SimpleTestOptimizer
import org.apache.spark.sql.catalyst.plans.PlanTestBase
import org.apache.spark.sql.catalyst.plans.logical.{OneRowRelation, Project}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, MapData}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * A few helper functions for expression evaluation testing. Mixin this trait to use them.
 *
 * Note: when you write unit test for an expression and call `checkEvaluation` to check the result,
 *       please make sure that you explore all the cases that can lead to null result (including
 *       null in struct fields, array elements and map values). The framework will test the
 *       nullability flag of the expression automatically.
 */
trait ExpressionEvalHelper extends ScalaCheckDrivenPropertyChecks with PlanTestBase {
  self: SparkFunSuite =>

  protected def create_row(values: Any*): InternalRow = {
    InternalRow.fromSeq(values.map(CatalystTypeConverters.convertToCatalyst))
  }

  // Currently MapData just stores the key and value arrays. Its equality is not well implemented,
  // as the order of the map entries should not matter for equality. This method creates MapData
  // with the entries ordering preserved, so that we can deterministically test expressions with
  // map input/output.
  protected def create_map(entries: (_, _)*): ArrayBasedMapData = {
    create_map(entries.map(_._1), entries.map(_._2))
  }

  protected def create_map(keys: Seq[_], values: Seq[_]): ArrayBasedMapData = {
    assert(keys.length == values.length)
    val keyArray = CatalystTypeConverters
      .convertToCatalyst(keys)
      .asInstanceOf[ArrayData]
    val valueArray = CatalystTypeConverters
      .convertToCatalyst(values)
      .asInstanceOf[ArrayData]
    new ArrayBasedMapData(keyArray, valueArray)
  }

  private def prepareEvaluation(expression: Expression): Expression = {
    val serializer = new JavaSerializer(new SparkConf()).newInstance
    val resolver = ResolveTimeZone
    val expr = resolver.resolveTimeZones(expression)
    assert(expr.resolved)
    serializer.deserialize(serializer.serialize(expr))
  }

  protected def checkEvaluation(
      expression: => Expression, expected: Any, inputRow: InternalRow = EmptyRow): Unit = {
    // Make it as method to obtain fresh expression everytime.
    def expr = prepareEvaluation(expression)
    val catalystValue = CatalystTypeConverters.convertToCatalyst(expected)
    checkEvaluationWithoutCodegen(expr, catalystValue, inputRow)
    checkEvaluationWithMutableProjection(expr, catalystValue, inputRow)
    if (GenerateUnsafeProjection.canSupport(expr.dataType)) {
      checkEvaluationWithUnsafeProjection(expr, catalystValue, inputRow)
    }
    checkEvaluationWithOptimization(expr, catalystValue, inputRow)
  }

  /**
   * Check the equality between result of expression and expected value, it will handle
   * Array[Byte], Spread[Double], MapData and Row. Also check whether nullable in expression is
   * true if result is null
   */
  protected def checkResult(result: Any, expected: Any, expression: Expression): Boolean = {
    checkResult(result, expected, expression.dataType, expression.nullable)
  }

  protected def checkResult(
      result: Any,
      expected: Any,
      exprDataType: DataType,
      exprNullable: Boolean): Boolean = {
    val dataType = UserDefinedType.sqlType(exprDataType)

    // The result is null for a non-nullable expression
    assert(result != null || exprNullable, "exprNullable should be true if result is null")
    (result, expected) match {
      case (result: Array[Byte], expected: Array[Byte]) =>
        java.util.Arrays.equals(result, expected)
      case (result: Double, expected: Spread[Double @unchecked]) =>
        expected.isWithin(result)
      case (result: InternalRow, expected: InternalRow) =>
        val st = dataType.asInstanceOf[StructType]
        assert(result.numFields == st.length && expected.numFields == st.length)
        st.zipWithIndex.forall { case (f, i) =>
          checkResult(
            result.get(i, f.dataType), expected.get(i, f.dataType), f.dataType, f.nullable)
        }
      case (result: ArrayData, expected: ArrayData) =>
        result.numElements == expected.numElements && {
          val ArrayType(et, cn) = dataType.asInstanceOf[ArrayType]
          var isSame = true
          var i = 0
          while (isSame && i < result.numElements) {
            isSame = checkResult(result.get(i, et), expected.get(i, et), et, cn)
            i += 1
          }
          isSame
        }
      case (result: MapData, expected: MapData) =>
        val MapType(kt, vt, vcn) = dataType.asInstanceOf[MapType]
        checkResult(result.keyArray, expected.keyArray, ArrayType(kt, false), false) &&
          checkResult(result.valueArray, expected.valueArray, ArrayType(vt, vcn), false)
      case (result: Double, expected: Double) =>
        if (expected.isNaN) result.isNaN else expected == result
      case (result: Float, expected: Float) =>
        if (expected.isNaN) result.isNaN else expected == result
      case (result: Row, expected: InternalRow) => result.toSeq == expected.toSeq(result.schema)
      case _ =>
        result == expected
    }
  }

  protected def checkExceptionInExpression[T <: Throwable : ClassTag](
      expression: => Expression,
      expectedErrMsg: String): Unit = {
    checkExceptionInExpression[T](expression, InternalRow.empty, expectedErrMsg)
  }

  protected def checkExceptionInExpression[T <: Throwable : ClassTag](
      expression: => Expression,
      inputRow: InternalRow,
      expectedErrMsg: String): Unit = {

    def checkException(eval: => Unit, testMode: String): Unit = {
      val modes = Seq(CodegenObjectFactoryMode.CODEGEN_ONLY, CodegenObjectFactoryMode.NO_CODEGEN)
      withClue(s"($testMode)") {
        val errMsg = intercept[T] {
          for (fallbackMode <- modes) {
            withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> fallbackMode.toString) {
              eval
            }
          }
        }.getMessage
        if (errMsg == null) {
          if (expectedErrMsg != null) {
            fail(s"Expected null error message, but `$errMsg` found")
          }
        } else if (!errMsg.contains(expectedErrMsg)) {
          fail(s"Expected error message is `$expectedErrMsg`, but `$errMsg` found")
        }
      }
    }

    // Make it as method to obtain fresh expression everytime.
    def expr = prepareEvaluation(expression)
    checkException(evaluateWithoutCodegen(expr, inputRow), "non-codegen mode")
    checkException(evaluateWithMutableProjection(expr, inputRow), "codegen mode")
    if (GenerateUnsafeProjection.canSupport(expr.dataType)) {
      checkException(evaluateWithUnsafeProjection(expr, inputRow), "unsafe mode")
    }
  }

  protected def evaluateWithoutCodegen(
      expression: Expression, inputRow: InternalRow = EmptyRow): Any = {
    expression.foreach {
      case n: Nondeterministic => n.initialize(0)
      case _ =>
    }
    expression.eval(inputRow)
  }

  protected def checkEvaluationWithoutCodegen(
      expression: Expression,
      expected: Any,
      inputRow: InternalRow = EmptyRow): Unit = {

    val actual = try evaluateWithoutCodegen(expression, inputRow) catch {
      case e: Exception => fail(s"Exception evaluating $expression", e)
    }
    if (!checkResult(actual, expected, expression)) {
      val input = if (inputRow == EmptyRow) "" else s", input: $inputRow"
      fail(s"Incorrect evaluation (codegen off): $expression, " +
        s"actual: $actual, " +
        s"expected: $expected$input")
    }
  }

  protected def checkEvaluationWithMutableProjection(
      expression: => Expression,
      expected: Any,
      inputRow: InternalRow = EmptyRow): Unit = {
    val modes = Seq(CodegenObjectFactoryMode.CODEGEN_ONLY, CodegenObjectFactoryMode.NO_CODEGEN)
    for (fallbackMode <- modes) {
      withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> fallbackMode.toString) {
        val actual = evaluateWithMutableProjection(expression, inputRow)
        if (!checkResult(actual, expected, expression)) {
          val input = if (inputRow == EmptyRow) "" else s", input: $inputRow"
          fail(s"Incorrect evaluation (fallback mode = $fallbackMode): $expression, " +
            s"actual: $actual, expected: $expected$input")
        }
      }
    }
  }

  protected def evaluateWithMutableProjection(
      expression: => Expression,
      inputRow: InternalRow = EmptyRow): Any = {
    val plan = MutableProjection.create(Alias(expression, s"Optimized($expression)")() :: Nil)
    plan.initialize(0)

    plan(inputRow).get(0, expression.dataType)
  }

  protected def checkEvaluationWithUnsafeProjection(
      expression: Expression,
      expected: Any,
      inputRow: InternalRow = EmptyRow): Unit = {
    val modes = Seq(CodegenObjectFactoryMode.CODEGEN_ONLY, CodegenObjectFactoryMode.NO_CODEGEN)
    for (fallbackMode <- modes) {
      withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> fallbackMode.toString) {
        val unsafeRow = evaluateWithUnsafeProjection(expression, inputRow)
        val input = if (inputRow == EmptyRow) "" else s", input: $inputRow"

        val dataType = expression.dataType
        if (!checkResult(unsafeRow.get(0, dataType), expected, dataType, expression.nullable)) {
          fail("Incorrect evaluation in unsafe mode (fallback mode = $fallbackMode): " +
            s"$expression, actual: $unsafeRow, expected: $expected, " +
            s"dataType: $dataType, nullable: ${expression.nullable}")
        }
        if (expected == null) {
          if (!unsafeRow.isNullAt(0)) {
            val expectedRow = InternalRow(expected, expected)
            fail(s"Incorrect evaluation in unsafe mode (fallback mode = $fallbackMode): " +
              s"$expression, actual: $unsafeRow, expected: $expectedRow$input")
          }
        } else {
          val lit = InternalRow(expected, expected)
          val expectedRow = UnsafeProjection.create(Array(dataType, dataType)).apply(lit)
          if (unsafeRow != expectedRow) {
            fail(s"Incorrect evaluation in unsafe mode (fallback mode = $fallbackMode): " +
              s"$expression, actual: $unsafeRow, expected: $expectedRow$input")
          }
        }
      }
    }
  }

  protected def evaluateWithUnsafeProjection(
      expression: Expression,
      inputRow: InternalRow = EmptyRow): InternalRow = {
    // SPARK-16489 Explicitly doing code generation twice so code gen will fail if
    // some expression is reusing variable names across different instances.
    // This behavior is tested in ExpressionEvalHelperSuite.
    val plan = UnsafeProjection.create(
      Alias(expression, s"Optimized($expression)1")() ::
        Alias(expression, s"Optimized($expression)2")() :: Nil)

    plan.initialize(0)
    plan(inputRow)
  }

  protected def checkEvaluationWithOptimization(
      expression: Expression,
      expected: Any,
      inputRow: InternalRow = EmptyRow): Unit = {
    val plan = Project(Alias(expression, s"Optimized($expression)")() :: Nil, OneRowRelation())
    val optimizedPlan = SimpleTestOptimizer.execute(plan)
    checkEvaluationWithoutCodegen(optimizedPlan.expressions.head, expected, inputRow)
  }

  protected def checkDoubleEvaluation(
      expression: => Expression,
      expected: Spread[Double],
      inputRow: InternalRow = EmptyRow): Unit = {
    checkEvaluationWithoutCodegen(expression, expected)
    checkEvaluationWithMutableProjection(expression, expected)
    checkEvaluationWithOptimization(expression, expected)

    var plan: Projection =
      GenerateMutableProjection.generate(Alias(expression, s"Optimized($expression)")() :: Nil)
    plan.initialize(0)
    var actual = plan(inputRow).get(0, expression.dataType)
    assert(checkResult(actual, expected, expression))

    plan = GenerateUnsafeProjection.generate(Alias(expression, s"Optimized($expression)")() :: Nil)
    plan.initialize(0)
    val ref = new BoundReference(0, expression.dataType, nullable = true)
    actual = GenerateSafeProjection.generate(ref :: Nil)(plan(inputRow)).get(0, expression.dataType)
    assert(checkResult(actual, expected, expression))
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
   * consistent result regardless of the evaluation method we use. If an exception is thrown,
   * it checks that both modes throw the same exception.
   *
   * This method test against unary expressions by feeding them arbitrary literals of `dataType`.
   */
  def checkConsistencyBetweenInterpretedAndCodegenAllowingException(
      c: Expression => Expression,
      dataType: DataType): Unit = {
    forAll (LiteralGenerator.randomGen(dataType)) { (l: Literal) =>
      cmpInterpretWithCodegen(EmptyRow, c(l), true)
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
   * consistent result regardless of the evaluation method we use. If an exception is thrown,
   * it checks that both modes throw the same exception.
   *
   * This method test against binary expressions by feeding them arbitrary literals of `dataType1`
   * and `dataType2`.
   */
  def checkConsistencyBetweenInterpretedAndCodegenAllowingException(
      c: (Expression, Expression) => Expression,
      dataType1: DataType,
      dataType2: DataType): Unit = {
    forAll (
      LiteralGenerator.randomGen(dataType1),
      LiteralGenerator.randomGen(dataType2)
    ) { (l1: Literal, l2: Literal) =>
      cmpInterpretWithCodegen(EmptyRow, c(l1, l2), true)
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

  def cmpInterpretWithCodegen(
      inputRow: InternalRow,
      expr: Expression,
      exceptionAllowed: Boolean = false): Unit = {
    val (interpret, interpretExc) = try {
      (Some(evaluateWithoutCodegen(expr, inputRow)), None)
    } catch {
      case e: Exception => if (exceptionAllowed) {
        (None, Some(e))
      } else {
        fail(s"Exception evaluating $expr", e)
      }
    }

    val plan = GenerateMutableProjection.generate(Alias(expr, s"Optimized($expr)")() :: Nil)
    val (codegen, codegenExc) = try {
      (Some(plan(inputRow).get(0, expr.dataType)), None)
    } catch {
      case e: Exception => if (exceptionAllowed) {
        (None, Some(e))
      } else {
        fail(s"Exception evaluating $expr", e)
      }
    }

    if (interpret.isDefined && codegen.isDefined && !compareResults(interpret.get, codegen.get)) {
      fail(s"Incorrect evaluation: $expr, interpret: ${interpret.get}, codegen: ${codegen.get}")
    } else if (interpretExc.isDefined && codegenExc.isEmpty) {
      fail(s"Incorrect evaluation: $expr, interpret threw exception ${interpretExc.get}")
    } else if (interpretExc.isEmpty && codegenExc.isDefined) {
      fail(s"Incorrect evaluation: $expr, codegen threw exception ${codegenExc.get}")
    } else if (interpretExc.isDefined && codegenExc.isDefined
        && !compareExceptions(interpretExc.get, codegenExc.get)) {
      fail(s"Different exception evaluating: $expr, " +
        s"interpret: ${interpretExc.get}, codegen: ${codegenExc.get}")
    }
  }

  /**
   * Checks the equality between two exceptions. Returns true iff the two exceptions are instances
   * of the same class and they have the same message.
   */
  private[this] def compareExceptions(e1: Exception, e2: Exception): Boolean = {
    e1.getClass == e2.getClass && e1.getMessage == e2.getMessage
  }

  /**
   * Check the equality between result of expression and expected value, it will handle
   * Array[Byte] and Spread[Double].
   */
  private[this] def compareResults(result: Any, expected: Any): Boolean = {
    (result, expected) match {
      case (result: Array[Byte], expected: Array[Byte]) =>
        java.util.Arrays.equals(result, expected)
      case (result: Double, expected: Double) if result.isNaN && expected.isNaN =>
        true
      case (result: Double, expected: Double) =>
        relativeErrorComparison(result, expected)
      case (result: Float, expected: Float) if result.isNaN && expected.isNaN =>
        true
      case _ => result == expected
    }
  }

  /**
   * Private helper function for comparing two values using relative tolerance.
   * Note that if x or y is extremely close to zero, i.e., smaller than Double.MinPositiveValue,
   * the relative tolerance is meaningless, so the exception will be raised to warn users.
   *
   * TODO: this duplicates functions in spark.ml.util.TestingUtils.relTol and
   * spark.mllib.util.TestingUtils.relTol, they could be moved to common utils sub module for the
   * whole spark project which does not depend on other modules. See more detail in discussion:
   * https://github.com/apache/spark/pull/15059#issuecomment-246940444
   */
  private def relativeErrorComparison(x: Double, y: Double, eps: Double = 1E-8): Boolean = {
    val absX = math.abs(x)
    val absY = math.abs(y)
    val diff = math.abs(x - y)
    if (x == y) {
      true
    } else if (absX < Double.MinPositiveValue || absY < Double.MinPositiveValue) {
      throw new TestFailedException(
        s"$x or $y is extremely close to zero, so the relative tolerance is meaningless.", 0)
    } else {
      diff < eps * math.min(absX, absY)
    }
  }

  def testBothCodegenAndInterpreted(name: String)(f: => Unit): Unit = {
    val modes = Seq(CodegenObjectFactoryMode.CODEGEN_ONLY, CodegenObjectFactoryMode.NO_CODEGEN)
    for (fallbackMode <- modes) {
      test(s"$name with $fallbackMode") {
        withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> fallbackMode.toString) {
          f
        }
      }
    }
  }
}
