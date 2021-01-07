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

package org.apache.spark.sql.execution

import org.apache.spark.{SparkException, TestUtils}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.test.SharedSparkSession

class SparkScriptTransformationSuite extends BaseScriptTransformationSuite with SharedSparkSession {
  import testImplicits._

  override def createScriptTransformationExec(
      input: Seq[Expression],
      script: String,
      output: Seq[Attribute],
      child: SparkPlan,
      ioschema: ScriptTransformationIOSchema): BaseScriptTransformationExec = {
    SparkScriptTransformationExec(
      input = input,
      script = script,
      output = output,
      child = child,
      ioschema = ioschema
    )
  }

  test("SPARK-32106: TRANSFORM with serde without hive should throw exception") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))
    withTempView("v") {
      val df = Seq("a", "b", "c").map(Tuple1.apply).toDF("a")
      df.createTempView("v")

      val e = intercept[ParseException] {
        sql(
          """
            |SELECT TRANSFORM (a)
            |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
            |USING 'cat' AS (a)
            |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
            |FROM v
          """.stripMargin)
      }.getMessage
      assert(e.contains("TRANSFORM with serde is only supported in hive mode"))
    }
  }

  test("SPARK-32106: TRANSFORM doesn't support ArrayType/MapType/StructType " +
    "as output data type (no serde)") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))
    // check for ArrayType
    val e1 = intercept[SparkException] {
      sql(
        """
          |SELECT TRANSFORM(a)
          |USING 'cat' AS (a array<int>)
          |FROM VALUES (array(1, 1), map('1', 1), struct(1, 'a')) t(a, b, c)
        """.stripMargin).collect()
    }.getMessage
    assert(e1.contains("SparkScriptTransformation without serde does not support" +
      " ArrayType as output data type"))

    // check for MapType
    val e2 = intercept[SparkException] {
      sql(
        """
          |SELECT TRANSFORM(b)
          |USING 'cat' AS (b map<int, string>)
          |FROM VALUES (array(1, 1), map('1', 1), struct(1, 'a')) t(a, b, c)
        """.stripMargin).collect()
    }.getMessage
    assert(e2.contains("SparkScriptTransformation without serde does not support" +
      " MapType as output data type"))

    // check for StructType
    val e3 = intercept[SparkException] {
      sql(
        """
          |SELECT TRANSFORM(c)
          |USING 'cat' AS (c struct<col1:int, col2:string>)
          |FROM VALUES (array(1, 1), map('1', 1), struct(1, 'a')) t(a, b, c)
        """.stripMargin).collect()
    }.getMessage
    assert(e3.contains("SparkScriptTransformation without serde does not support" +
      " StructType as output data type"))
  }
}
