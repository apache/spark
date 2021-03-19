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

import org.scalatestplus.mockito.MockitoSugar
import scala.util.Try

import org.apache.spark.sql.catalyst.{FunctionIdentifier, ScalaReflection}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.PlanTest

class FunctionRegistryTest extends PlanTest with MockitoSugar{

  test("SPARK-34804 should return true for same functions") {
    val functionRegistry = new SimpleFunctionRegistry
    val input = new ExpressionInfo("testClass", null, "testName", null, "", "", "", "", "", "")
    val input2 = new ExpressionInfo("testClass", null, "testName", null, "", "", "", "", "", "")
    assert(functionRegistry.testFunctionEquality(input, input2))
  }

  test("SPARK-34804 should return false for different functions") {
    val functionRegistry = new SimpleFunctionRegistry
    val input = new ExpressionInfo("testClass", null, "testName", null, "", "", "", "", "", "")
    val input2 = new ExpressionInfo("testClass", null, "testName", null, "a,b", "", "", "", "", "")
    assert(!functionRegistry.testFunctionEquality(input, input2))
  }

  test("SPARK-34804 test with udf registration call") {
    val func = (i: Int) => (i*2)
    val ScalaReflection.Schema(dataType, nullable) = ScalaReflection.schemaFor[Int]
    val inputEncoders = Try(ExpressionEncoder[Int]()).toOption :: Nil
    val builder = (e: Seq[Expression]) => ScalaUDF(
      func,
      dataType,
      e,
      inputEncoders,
      udfName = Some("test_func"),
      nullable = nullable)
    var equality = false
    val functionRegistry = new SimpleFunctionRegistry {
      override def testFunctionEquality(previousFunction: ExpressionInfo,
                                        newFunction: ExpressionInfo): Boolean = {
        val r = super.testFunctionEquality(previousFunction, newFunction)
        equality = r
        r
      }
    }
    functionRegistry.registerFunction(FunctionIdentifier("test_func"), builder)
    functionRegistry.registerFunction(FunctionIdentifier("test_func"), builder)
    assert(equality)
  }

}
