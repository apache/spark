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

package org.apache.spark.sql

import java.sql.Timestamp

import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.Decimal

class MiscFunctionsSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("reflect and java_method") {
    val df = Seq((1, "one")).toDF("a", "b")
    val className = ReflectClass.getClass.getName.stripSuffix("$")
    checkAnswer(
      df.selectExpr(
        s"reflect('$className', 'method1', a, b)",
        s"java_method('$className', 'method1', a, b)"),
      Row("m1one", "m1one"))
  }

  test("reflect and java_method throws an analysis exception for unsupported types") {
    val df = Seq((new Timestamp(1), Decimal(10))).toDF("a", "b")
    val className = ReflectClass.getClass.getName.stripSuffix("$")
    val messageOne = intercept[AnalysisException] {
      df.selectExpr(
        s"reflect('$className', 'method1', a, b)").collect()
    }.getMessage

    assert(messageOne.contains(
      "arguments from the third require boolean, byte, short, " +
        "integer, long, float, double or string expressions"))

    val messageTwo = intercept[AnalysisException] {
      df.selectExpr(
        s"java_method('$className', 'method1', a, b)").collect()
    }.getMessage

    assert(messageTwo.contains(
      "arguments from the third require boolean, byte, short, " +
        "integer, long, float, double or string expressions"))
  }

  test("reflect and java_method throws an analysis exception for non-existing method/class") {
    val df = Seq((1, "one")).toDF("a", "b")
    val className = ReflectClass.getClass.getName.stripSuffix("$")
    val messageOne = intercept[AnalysisException] {
      df.selectExpr(
        s"reflect('$className', 'abcde', a, b)").collect()
    }.getMessage

    assert(messageOne.contains(
      "cannot find a static method that matches the argument types in"))

    val messageTwo = intercept[AnalysisException] {
      df.selectExpr(
        s"reflect('abcd', 'method1', a, b)").collect()
    }.getMessage

    assert(messageTwo.contains("class abcd not found"))
  }
}

object ReflectClass {
  def method1(v1: Int, v2: String): String = "m" + v1 + v2
}
