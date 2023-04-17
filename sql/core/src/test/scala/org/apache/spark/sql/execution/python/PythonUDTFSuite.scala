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

package org.apache.spark.sql.execution.python

import org.apache.spark.sql.{IntegratedUDFTestUtils, QueryTest, Row}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.test.SharedSparkSession

class PythonUDTFSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  import IntegratedUDFTestUtils._

  val pythonTestUDTF = TestPythonUDTF(name = "pyUDTF")

  test("Simple PythonUDTF") {
    // scalastyle:off assume
    assume(shouldTestPythonUDFs)
    // scalastyle:on assume
    val df = pythonTestUDTF(spark, lit(1), lit(2))
    checkAnswer(df, Seq(Row(1, 2, -1), Row(1, 2, 1), Row(1, 2, 3)))
  }

  test("PythonUDTF with lateral join") {
    // scalastyle:off assume
    assume(shouldTestPythonUDFs)
    // scalastyle:on assume
    withTempView("t") {
      val func = createUserDefinedPythonTableFunction("testUDTF")
      spark.udtf.registerPython("testUDTF", func)
      Seq((0, 1), (1, 2)).toDF("a", "b").createOrReplaceTempView("t")
      checkAnswer(
        sql("SELECT f.* FROM t, LATERAL testUDTF(a, b) f"),
        sql("SELECT * FROM t, LATERAL explode(array(a + b, a - b, b - a)) t(c)")
      )
    }
  }
}
