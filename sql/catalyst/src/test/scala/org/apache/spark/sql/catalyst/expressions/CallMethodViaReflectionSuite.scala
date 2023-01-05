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

import java.sql.Timestamp

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.Cast.toSQLType
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.types._

/** A static class for testing purpose. */
object ReflectStaticClass {
  def method1(): String = "m1"
  def method2(v1: Int): String = "m" + v1
  def method3(v1: java.lang.Integer): String = "m" + v1
  def method4(v1: Int, v2: String): String = "m" + v1 + v2
}

/** A non-static class for testing purpose. */
class ReflectDynamicClass {
  def method1(): String = "m1"
}

/**
 * Test suite for [[CallMethodViaReflection]] and its companion object.
 */
class CallMethodViaReflectionSuite extends SparkFunSuite with ExpressionEvalHelper {

  import CallMethodViaReflection._

  // Get rid of the $ so we are getting the companion object's name.
  private val staticClassName = ReflectStaticClass.getClass.getName.stripSuffix("$")
  private val dynamicClassName = classOf[ReflectDynamicClass].getName

  test("findMethod via reflection for static methods") {
    assert(findMethod(staticClassName, "method1", Seq.empty).exists(_.getName == "method1"))
    assert(findMethod(staticClassName, "method2", Seq(IntegerType)).isDefined)
    assert(findMethod(staticClassName, "method3", Seq(IntegerType)).isDefined)
    assert(findMethod(staticClassName, "method4", Seq(IntegerType, StringType)).isDefined)
  }

  test("findMethod for a JDK library") {
    assert(findMethod(classOf[java.util.UUID].getName, "randomUUID", Seq.empty).isDefined)
  }

  test("class not found") {
    val wrongClassName = "some-random-class"
    val ret = createExpr(wrongClassName, "method").checkInputDataTypes()
    assert(ret.isFailure)
    assert(ret ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_CLASS_TYPE",
        messageParameters = Map("className" -> wrongClassName)
      )
    )
  }

  test("method not found because name does not match") {
    val wrongMethodName = "notfoundmethod"
    val ret = createExpr(staticClassName, wrongMethodName).checkInputDataTypes()
    assert(ret.isFailure)
    assert(ret ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_STATIC_METHOD",
        messageParameters = Map("methodName" -> wrongMethodName, "className" -> staticClassName)
      )
    )
  }

  test("method not found because there is no static method") {
    val wrongMethodName = "method1"
    val ret = createExpr(dynamicClassName, wrongMethodName).checkInputDataTypes()
    assert(ret.isFailure)
    assert(ret ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_STATIC_METHOD",
        messageParameters = Map("methodName" -> wrongMethodName, "className" -> dynamicClassName)
      )
    )
  }

  test("input type checking") {
    assert(CallMethodViaReflection(Seq.empty).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "WRONG_NUM_ARGS",
        messageParameters = Map(
          "functionName" -> "`reflect`",
          "expectedNum" -> "> 1",
          "actualNum" -> "0")
      )
    )
    assert(CallMethodViaReflection(Seq(Literal(staticClassName))).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "WRONG_NUM_ARGS",
        messageParameters = Map(
          "functionName" -> "`reflect`",
          "expectedNum" -> "> 1",
          "actualNum" -> "1")
      )
    )
    assert(CallMethodViaReflection(
      Seq(Literal(staticClassName), Literal(1))).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> "method",
          "inputType" -> "\"STRING\"",
          "inputExpr" -> "\"1\"")
      )
    )
    assert(createExpr(staticClassName, "method1").checkInputDataTypes().isSuccess)
  }

  test("unsupported type checking") {
    val ret = createExpr(staticClassName, "method1", new Timestamp(1)).checkInputDataTypes()
    assert(ret.isFailure)
    assert(ret ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> "3",
          "requiredType" -> toSQLType(
            TypeCollection(BooleanType, ByteType, ShortType,
              IntegerType, LongType, FloatType, DoubleType, StringType)),
          "inputSql" -> "\"TIMESTAMP '1969-12-31 16:00:00.001'\"",
          "inputType" -> "\"TIMESTAMP\""
        )
      )
    )
  }

  test("invoking methods using acceptable types") {
    checkEvaluation(createExpr(staticClassName, "method1"), "m1")
    checkEvaluation(createExpr(staticClassName, "method2", 2), "m2")
    checkEvaluation(createExpr(staticClassName, "method3", 3), "m3")
    checkEvaluation(createExpr(staticClassName, "method4", 4, "four"), "m4four")
  }

  test("escaping of class and method names") {
    GenerateUnsafeProjection.generate(
      CallMethodViaReflection(Seq(Literal("\"quote"), Literal("\"quote"), Literal(null))) :: Nil)
  }

  private def createExpr(className: String, methodName: String, args: Any*) = {
    CallMethodViaReflection(
      Literal.create(className, StringType) +:
      Literal.create(methodName, StringType) +:
      args.map(Literal.apply)
    )
  }
}
