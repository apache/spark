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

import org.apache.spark.{SPARK_REVISION, SPARK_VERSION_SHORT}
import org.apache.spark.sql.test.SharedSparkSession

class MiscFunctionsSuite extends QueryTest with SharedSparkSession {
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

  test("version") {
    val df = sql("SELECT version()")
    checkAnswer(
      df,
      Row(SPARK_VERSION_SHORT + " " + SPARK_REVISION))
    assert(df.schema.fieldNames === Seq("version()"))
  }
}

object ReflectClass {
  def method1(v1: Int, v2: String): String = "m" + v1 + v2
}
