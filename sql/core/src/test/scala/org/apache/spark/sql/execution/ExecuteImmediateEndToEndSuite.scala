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

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession

class ExecuteImmediateEndToEndSuite extends QueryTest with SharedSparkSession {

  test("SPARK-47033: EXECUTE IMMEDIATE USING does not recognize session variable names") {
    try {
      spark.sql("DECLARE parm = 'Hello';")

      val originalQuery = spark.sql(
        "EXECUTE IMMEDIATE 'SELECT :parm' USING system.session.parm AS parm;")
      val newQuery = spark.sql("EXECUTE IMMEDIATE 'SELECT :parm' USING system.session.parm;")

      assert(originalQuery.columns sameElements newQuery.columns)

      checkAnswer(originalQuery, newQuery.collect().toIndexedSeq)
    } finally {
      spark.sql("DROP TEMPORARY VARIABLE IF EXISTS parm;")
    }
  }

  test("SQL Scripting not supported inside EXECUTE IMMEDIATE") {
    val executeImmediateText = "EXECUTE IMMEDIATE 'BEGIN SELECT 1; END'"
    checkError(
      exception = intercept[AnalysisException ] {
        spark.sql(executeImmediateText)
      },
      condition = "SQL_SCRIPT_IN_EXECUTE_IMMEDIATE",
      parameters = Map("sqlString" -> "BEGIN SELECT 1; END"))
  }

  test("EXECUTE IMMEDIATE resolves session variables in body") {
    withSessionVariable("v1", "v2") {
      spark.sql("DECLARE v1 = 42")
      spark.sql("DECLARE v2 = 99")
      checkAnswer(spark.sql("EXECUTE IMMEDIATE 'SELECT system.session.v1, v2'"), Row(42, 99))
    }
  }

  test("EXECUTE IMMEDIATE resolves session variables inside script") {
    withSessionVariable("v1", "v2") {
      spark.sql("DECLARE v1 = 10")
      spark.sql("DECLARE v2 = 20")
      val result = spark.sql(
        """
          |BEGIN
          |  DECLARE v3 = 1;
          |  EXECUTE IMMEDIATE 'SELECT system.session.v1, v2';
          |END
          |""".stripMargin)
      checkAnswer(result, Row(10, 20))
    }
  }

  test("EXECUTE IMMEDIATE does not resolve local variables") {
    val result = intercept[AnalysisException] {
      spark.sql(
        """
          |BEGIN
          |  DECLARE v1 = 5;
          |  EXECUTE IMMEDIATE 'SELECT v1';
          |END
          |""".stripMargin)
    }
    checkError(
      exception = result,
      condition = "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
      sqlState = "42703",
      parameters = Map("objectName" -> "`v1`"),
      context = ExpectedContext(
        objectType = "EXECUTE IMMEDIATE",
        objectName = "",
        startIndex = 7,
        stopIndex = 8,
        fragment = "v1"))
  }

  test("EXECUTE IMMEDIATE resolves local variable in USING clause") {
    val result = spark.sql(
      """
        |BEGIN
        |  DECLARE v1 = 5;
        |  EXECUTE IMMEDIATE 'SELECT ?' USING v1;
        |END
        |""".stripMargin)
    checkAnswer(result, Row(5))
  }

  test("EXECUTE IMMEDIATE resolves session var in body and local var in USING") {
    withSessionVariable("v1") {
      spark.sql("DECLARE v1 = 10")
      val result = spark.sql(
        """
          |BEGIN
          |  DECLARE v2 = 20;
          |  EXECUTE IMMEDIATE 'SELECT system.session.v1, ?' USING v2;
          |END
          |""".stripMargin)
      checkAnswer(result, Row(10, 20))
    }
  }

  test("EXECUTE IMMEDIATE fails when local var referenced in body alongside session var") {
    withSessionVariable("v1") {
      spark.sql("DECLARE v1 = 10")
      val e = intercept[AnalysisException] {
        spark.sql(
          """
            |BEGIN
            |  DECLARE v2 = 20;
            |  EXECUTE IMMEDIATE 'SELECT v1, v2';
            |END
            |""".stripMargin)
      }
      checkError(
        exception = e,
        condition = "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
        sqlState = "42703",
        parameters = Map("objectName" -> "`v2`"),
        context = ExpectedContext(
          objectType = "EXECUTE IMMEDIATE",
          objectName = "",
          startIndex = 11,
          stopIndex = 12,
          fragment = "v2"))
    }
  }
}
