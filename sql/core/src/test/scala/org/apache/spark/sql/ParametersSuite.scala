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

import org.apache.spark.sql.test.SharedSparkSession

class ParametersSuite extends QueryTest with SharedSparkSession {

  test("bind parameters") {
    val sqlText =
      """
        |SELECT id, id % :div as c0
        |FROM VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9) AS t(id)
        |WHERE id < :constA
        |""".stripMargin
    val args = Map("div" -> "3", "constA" -> "4L")
    checkAnswer(
      spark.sql(sqlText, args),
      Row(0, 0) :: Row(1, 1) :: Row(2, 2) :: Row(3, 0) :: Nil)

    checkAnswer(
      spark.sql("""SELECT contains('Spark \'SQL\'', :subStr)""", Map("subStr" -> "'SQL'")),
      Row(true))
  }

  test("non-substituted parameters") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.sql("select :abc, :def", Map("abc" -> "1"))
      },
      errorClass = "UNBOUND_SQL_PARAMETER",
      parameters = Map("name" -> "`def`"),
      context = ExpectedContext(
        fragment = ":def",
        start = 13,
        stop = 16))
    checkError(
      exception = intercept[AnalysisException] {
        sql("select :abc").collect()
      },
      errorClass = "UNBOUND_SQL_PARAMETER",
      parameters = Map("name" -> "`abc`"),
      context = ExpectedContext(
        fragment = ":abc",
        start = 7,
        stop = 10))
  }

  test("non-literal argument of `sql()`") {
    Seq("col1 + 1", "CAST('100' AS INT)", "map('a', 1, 'b', 2)", "array(1)").foreach { arg =>
      checkError(
        exception = intercept[AnalysisException] {
          spark.sql("SELECT :param1 FROM VALUES (1) AS t(col1)", Map("param1" -> arg))
        },
        errorClass = "INVALID_SQL_ARG",
        parameters = Map("name" -> "`param1`"),
        context = ExpectedContext(
          fragment = arg,
          start = 0,
          stop = arg.length - 1))
    }
  }
}
