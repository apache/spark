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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckFailure
import org.apache.spark.sql.types.{IntegerType, StringType}

/**
 * Test suite for [[Reflect]] and its companion object.
 */
class ReflectSuite extends SparkFunSuite with ExpressionEvalHelper {

  import Reflect._

  private val staticClassName = ReflectStaticClass.getClass.getName
  private val dynamicClassName = classOf[ReflectClass].getName

  test("findMethod via reflection for static methods") {
    for (className <- Seq(staticClassName, dynamicClassName)) {
      assert(findMethod(className, "method1", Seq.empty).exists(_.getName == "method1"))
      assert(findMethod(className, "method2", Seq(IntegerType)).isDefined)
      assert(findMethod(className, "method3", Seq(IntegerType)).isDefined)
      assert(findMethod(className, "method4", Seq(IntegerType, StringType)).isDefined)
    }
  }

  test("instantiate class via reflection") {
    // Should succeed since the following two should have no-arg ctor.
    assert(instantiate(dynamicClassName).isDefined)
    assert(instantiate(staticClassName).isDefined)

    // Should fail since there is no no-arg ctor.
    assert(instantiate(classOf[ReflectClass1].getName).isEmpty)
  }

  test("findMethod for a JDK library") {
    assert(findMethod(classOf[java.util.UUID].getName, "randomUUID", Seq.empty).isDefined)
  }

  test("class not found") {
    val ret = reflectExpr("some-random-class", "method").checkInputDataTypes()
    assert(ret.isFailure)
    val errorMsg = ret.asInstanceOf[TypeCheckFailure].message
    assert(errorMsg.contains("not found") && errorMsg.contains("class"))
  }

  test("method not found due to input type mismatch") {
    val ret = reflectExpr(staticClassName, "notfoundmethod").checkInputDataTypes()
    assert(ret.isFailure)
    val errorMsg = ret.asInstanceOf[TypeCheckFailure].message
    assert(errorMsg.contains("cannot find a method"))
  }

  test("type checking for static classes") {
    assert(Reflect(Seq.empty).checkInputDataTypes().isFailure)
    assert(Reflect(Seq(Literal(staticClassName))).checkInputDataTypes().isFailure)
    assert(Reflect(Seq(Literal(staticClassName), Literal(1))).checkInputDataTypes().isFailure)
    assert(reflectExpr(staticClassName, "method1").checkInputDataTypes().isSuccess)
  }

  test("type checking for dynamic classes") {
    assert(Reflect(Seq.empty).checkInputDataTypes().isFailure)
    assert(Reflect(Seq(Literal(dynamicClassName))).checkInputDataTypes().isFailure)
    assert(Reflect(Seq(Literal(dynamicClassName), Literal(1))).checkInputDataTypes().isFailure)
    assert(reflectExpr(dynamicClassName, "method1").checkInputDataTypes().isSuccess)
  }

  test("invoking methods using acceptable types") {
    for (className <- Seq(staticClassName, dynamicClassName)) {
      checkEvaluation(reflectExpr(className, "method1"), "m1")
      checkEvaluation(reflectExpr(className, "method2", 2), "m2")
      checkEvaluation(reflectExpr(className, "method3", 3), "m3")
      checkEvaluation(reflectExpr(className, "method4", 4, "four"), "m4four")
    }
  }

  private def reflectExpr(className: String, methodName: String, args: Any*): Reflect = {
    Reflect(
      Literal.create(className, StringType) +:
      Literal.create(methodName, StringType) +:
      args.map(Literal.apply)
    )
  }
}

/** A static class for testing purpose. */
object ReflectStaticClass {
  def method1(): String = "m1"
  def method2(v1: Int): String = "m" + v1
  def method3(v1: java.lang.Integer): String = "m" + v1
  def method4(v1: Int, v2: String): String = "m" + v1 + v2
}

/** A non-static class with a no-arg constructor for testing purpose. */
class ReflectClass {
  def method1(): String = "m1"
  def method2(v1: Int): String = "m" + v1
  def method3(v1: java.lang.Integer): String = "m" + v1
  def method4(v1: Int, v2: String): String = "m" + v1 + v2
}

/** A non-static class without a no-arg constructor for testing purpose. */
class ReflectClass1(val value: Int) {
  def method0(): Int = 10
}
