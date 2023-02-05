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

import org.apache.spark.TestUtils
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.test.SharedSparkSession

class SparkScriptTransformationSuite extends BaseScriptTransformationSuite with SharedSparkSession {
  import testImplicits._

  override protected def defaultSerDe(): String = "row-format-delimited"

  override def createScriptTransformationExec(
      script: String,
      output: Seq[Attribute],
      child: SparkPlan,
      ioschema: ScriptTransformationIOSchema): BaseScriptTransformationExec = {
    SparkScriptTransformationExec(
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

      val sqlText =
        """SELECT TRANSFORM (a)
          |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
          |USING 'cat' AS (a)
          |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
          |FROM v""".stripMargin
      checkError(
        exception = intercept[ParseException](sql(sqlText)),
        errorClass = "UNSUPPORTED_FEATURE.TRANSFORM_NON_HIVE",
        parameters = Map.empty,
        context = ExpectedContext(sqlText, 0, 185))
    }
  }
}
