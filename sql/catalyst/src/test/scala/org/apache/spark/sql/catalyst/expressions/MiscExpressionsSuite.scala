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

import java.io.PrintStream

import scala.util.Random

import org.apache.datasketches.kll.{KllDoublesSketch, KllFloatsSketch, KllLongsSketch}

import org.apache.spark.{SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.catalyst.util.TypeUtils.ordinalNumber
import org.apache.spark.sql.types._

class MiscExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("RaiseError") {
    checkExceptionInExpression[RuntimeException](
      RaiseError(Literal("error message")),
      EmptyRow,
      "error message"
    )

    checkExceptionInExpression[RuntimeException](
      RaiseError(Literal.create(null, StringType)),
      EmptyRow,
      "[USER_RAISED_EXCEPTION] null"
    )

    // Expects a string
    assert(RaiseError(Literal(5)).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(1),
          "requiredType" -> "\"MAP<STRING, STRING>\"",
          "inputSql" -> "\"map(errorMessage, 5)\"",
          "inputType" -> "\"MAP<STRING, INT>\""
        )
      )
    )
  }

  test("SPARK-58627: RaiseError is throwable") {
    assert(RaiseError(Literal("error!")).throwable)
  }

  test("SPARK-55109: RaiseError.sql uses single-argument form only for known error classes") {
    assert(RaiseError(Literal("error!")).sql === "raise_error('error!')")

    // A custom errorClass should NOT produce the single-argument form
    val customError = RaiseError(
      Literal("CUSTOM_ERROR"),
      CreateMap(Seq(Literal("errorMessage"), Literal("error!"))))
    assert(customError.sql === "raise_error('CUSTOM_ERROR', map('errorMessage', 'error!'))")
  }

  test("uuid") {
    checkEvaluation(Length(Uuid(Some(0))), 36)
    val r = new Random()
    val seed1 = Some(r.nextLong())
    assert(evaluateWithoutCodegen(Uuid(seed1)) === evaluateWithoutCodegen(Uuid(seed1)))
    assert(evaluateWithMutableProjection(Uuid(seed1)) ===
      evaluateWithMutableProjection(Uuid(seed1)))
    assert(evaluateWithUnsafeProjection(Uuid(seed1)) ===
      evaluateWithUnsafeProjection(Uuid(seed1)))

    val seed2 = Some(r.nextLong())
    assert(evaluateWithoutCodegen(Uuid(seed1)) !== evaluateWithoutCodegen(Uuid(seed2)))
    assert(evaluateWithMutableProjection(Uuid(seed1)) !==
      evaluateWithMutableProjection(Uuid(seed2)))
    assert(evaluateWithUnsafeProjection(Uuid(seed1)) !==
      evaluateWithUnsafeProjection(Uuid(seed2)))

    val seed3 = Literal.create(r.nextInt())
    assert(evaluateWithoutCodegen(new Uuid(seed3)) === evaluateWithoutCodegen(new Uuid(seed3)))
    assert(evaluateWithMutableProjection(new Uuid(seed3)) ===
      evaluateWithMutableProjection(new Uuid(seed3)))
    assert(evaluateWithUnsafeProjection(new Uuid(seed3)) ===
      evaluateWithUnsafeProjection(new Uuid(seed3)))
  }

  Seq(0L, 42L, Long.MinValue).foreach { seed =>
    test(s"uuid fresh copies own independent generators for seed $seed") {
      val original = Uuid(Some(seed))
      val first = original.freshCopyIfContainsStatefulExpression().asInstanceOf[Uuid]
      val second = original.freshCopyIfContainsStatefulExpression().asInstanceOf[Uuid]
      assert(first ne original)
      assert(second ne original)
      assert(first ne second)
      assert(first.randomSeed == original.randomSeed)
      original.initialize(0)
      first.initialize(0)
      second.initialize(0)
      for (_ <- 0 until 20) {
        val expected = original.eval()
        assert(first.eval() == expected)
        assert(second.eval() == expected)
      }
      first.initialize(1)
      assert(second.eval() == original.eval())
      val otherPartition = Uuid(Some(seed))
      otherPartition.initialize(1)
      assert(first.eval() == otherPartition.eval())
    }
  }

  test("PrintToStderr") {
    val inputExpr = Literal(1)
    val systemErr = System.err

    val (outputEval, outputCodegen) = try {
      val errorStream = new java.io.ByteArrayOutputStream()
      System.setErr(new PrintStream(errorStream))
      // check without codegen
      checkEvaluationWithoutCodegen(PrintToStderr(inputExpr), 1)
      val outputEval = errorStream.toString
      errorStream.reset()
      // check with codegen
      checkEvaluationWithMutableProjection(PrintToStderr(inputExpr), 1)
      val outputCodegen = errorStream.toString
      (outputEval, outputCodegen)
    } finally {
      System.setErr(systemErr)
    }

    assert(outputCodegen.contains(s"Result of $inputExpr is 1"))
    assert(outputEval.contains(s"Result of $inputExpr is 1"))
  }

  test("Hmac") {
    def bytes(hex: String): Array[Byte] =
      hex.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray

    val key = Literal("key".getBytes("UTF-8"))
    val message = Literal("message".getBytes("UTF-8"))

    // Default algorithm is SHA-256.
    checkEvaluation(
      new Hmac(key, message),
      bytes("6e9ef29b75fffc5b7abae527d58fdadb2fe42e7219011976917343065f58ed4a"))

    // Explicit algorithm.
    checkEvaluation(
      Hmac(key, message, Literal("SHA-1")),
      bytes("2088df74d5f2146b48146caf4965377e9d0be3a4"))

    // Null propagation.
    checkEvaluation(Hmac(Literal.create(null, BinaryType), message, Literal("SHA-256")), null)
    checkEvaluation(Hmac(key, Literal.create(null, BinaryType), Literal("SHA-256")), null)
    checkEvaluation(Hmac(key, message, Literal.create(null, StringType)), null)
  }

  test("Hmac unsupported algorithm") {
    checkErrorInExpression[SparkRuntimeException](
      Hmac(Literal("key".getBytes("UTF-8")), Literal("message".getBytes("UTF-8")),
        Literal("SHA-3")),
      "INVALID_PARAMETER_VALUE.HMAC_ALGORITHM",
      Map(
        "parameter" -> "`algorithm`",
        "functionName" -> "`hmac`",
        "algorithm" -> "'SHA-3'"))
  }

  Seq(LongType, FloatType, DoubleType).foreach { valueType =>
    val sketch = valueType match {
      case LongType =>
        val buffer = KllLongsSketch.newHeapInstance()
        Seq(10L, 20L, 30L).foreach(buffer.update)
        buffer.toByteArray
      case FloatType =>
        val buffer = KllFloatsSketch.newHeapInstance()
        Seq(10.0f, 20.0f, 30.0f).foreach(buffer.update)
        buffer.toByteArray
      case DoubleType =>
        val buffer = KllDoublesSketch.newHeapInstance()
        Seq(10.0d, 20.0d, 30.0d).foreach(buffer.update)
        buffer.toByteArray
    }

    val quantile: Expression => Expression = valueType match {
      case LongType => KllSketchGetQuantileBigint(Literal(sketch), _)
      case FloatType => KllSketchGetQuantileFloat(Literal(sketch), _)
      case DoubleType => KllSketchGetQuantileDouble(Literal(sketch), _)
    }
    val rank: Expression => Expression = valueType match {
      case LongType => KllSketchGetRankBigint(Literal(sketch), _)
      case FloatType => KllSketchGetRankFloat(Literal(sketch), _)
      case DoubleType => KllSketchGetRankDouble(Literal(sketch), _)
    }

    Seq(("rank", DoubleType, quantile), ("quantile", valueType, rank)).foreach {
      case (argumentName, argumentType, getter) =>
        test(s"KLL $valueType $argumentName rejects NULL array elements") {
          val value = Cast(Literal(0), argumentType).eval()
          Seq(
            Array[Any](null, value, value),
            Array[Any](value, null, value),
            Array[Any](value, value, null),
            Array[Any](null, null)).foreach { values =>
            val array = Literal(new GenericArrayData(values),
              ArrayType(argumentType, containsNull = true))
            assert(getter(array).checkInputDataTypes() == DataTypeMismatch(
              errorSubClass = "UNEXPECTED_NULL",
              messageParameters = Map("exprName" -> s"$argumentName array elements")))
          }
          val array = CreateArray(Seq(Cast(Literal(0), argumentType),
            Cast(Literal.create(null, NullType), argumentType)))
          assert(getter(array).checkInputDataTypes().isFailure)
        }

        test(s"KLL $valueType $argumentName preserves valid arrays and scalar NULL") {
          val value = Cast(Literal(0), argumentType).eval()
          val nullableArray = Literal(new GenericArrayData(Array[Any](value)),
            ArrayType(argumentType, containsNull = true))
          val expression = getter(nullableArray)
          assert(expression.checkInputDataTypes().isSuccess)
          val expected = if (argumentName == "rank") {
            Cast(Literal(10), valueType).eval()
          } else {
            0.0d
          }
          checkEvaluation(expression, Seq(expected))
          val emptyArray = Literal(new GenericArrayData(Array.empty[Any]),
            ArrayType(argumentType, containsNull = true))
          assert(getter(emptyArray).checkInputDataTypes().isSuccess)
          checkEvaluation(getter(emptyArray), Seq.empty)
          val nullArray = Literal.create(null, ArrayType(argumentType, containsNull = true))
          assert(getter(nullArray).checkInputDataTypes().isSuccess)
          checkEvaluation(getter(nullArray), null)
          val scalarNull = Literal.create(null, argumentType)
          assert(getter(scalarNull).checkInputDataTypes().isSuccess)
          checkEvaluation(getter(scalarNull), null)
          checkEvaluation(getter(Literal(value, argumentType)), expected)
          val nonFoldable = AttributeReference("input", ArrayType(argumentType))()
          assert(getter(nonFoldable).checkInputDataTypes().asInstanceOf[DataTypeMismatch]
            .errorSubClass == "NON_FOLDABLE_INPUT")
          assert(getter(Literal("invalid")).checkInputDataTypes().isFailure)
        }
    }
  }
}
