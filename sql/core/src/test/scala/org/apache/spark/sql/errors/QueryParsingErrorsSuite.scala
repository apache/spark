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

package org.apache.spark.sql.errors

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.test.SharedSparkSession

class QueryParsingErrorsSuite extends QueryTest with SharedSparkSession {
  def validateParsingError(
      sqlText: String,
      errorClass: String,
      sqlState: String,
      message: String): Unit = {
    val e = intercept[ParseException] {
      sql(sqlText)
    }
    assert(e.getErrorClass === errorClass)
    assert(e.getSqlState === sqlState)
    assert(e.getMessage === message)
  }

  test("UNSUPPORTED_FEATURE: LATERAL join with NATURAL join not supported") {
    validateParsingError(
      sqlText = "SELECT * FROM t1 NATURAL JOIN LATERAL (SELECT c1 + c2 AS c2)",
      errorClass = "UNSUPPORTED_FEATURE",
      sqlState = "0A000",
      message =
        """
          |The feature is not supported: LATERAL join with NATURAL join.(line 1, pos 14)
          |
          |== SQL ==
          |SELECT * FROM t1 NATURAL JOIN LATERAL (SELECT c1 + c2 AS c2)
          |--------------^^^
          |""".stripMargin)
  }

  test("UNSUPPORTED_FEATURE: LATERAL join with USING join not supported") {
    validateParsingError(
      sqlText = "SELECT * FROM t1 JOIN LATERAL (SELECT c1 + c2 AS c2) USING (c2)",
      errorClass = "UNSUPPORTED_FEATURE",
      sqlState = "0A000",
      message =
        """
          |The feature is not supported: LATERAL join with USING join.(line 1, pos 14)
          |
          |== SQL ==
          |SELECT * FROM t1 JOIN LATERAL (SELECT c1 + c2 AS c2) USING (c2)
          |--------------^^^
          |""".stripMargin)
  }

  test("UNSUPPORTED_FEATURE: Unsupported LATERAL join type") {
    Seq(
      ("RIGHT OUTER", "RightOuter"),
      ("FULL OUTER", "FullOuter"),
      ("LEFT SEMI", "LeftSemi"),
      ("LEFT ANTI", "LeftAnti")).foreach { pair =>
      validateParsingError(
        sqlText = s"SELECT * FROM t1 ${pair._1} JOIN LATERAL (SELECT c1 + c2 AS c3) ON c2 = c3",
        errorClass = "UNSUPPORTED_FEATURE",
        sqlState = "0A000",
        message =
          s"""
            |The feature is not supported: LATERAL join type '${pair._2}'.(line 1, pos 14)
            |
            |== SQL ==
            |SELECT * FROM t1 ${pair._1} JOIN LATERAL (SELECT c1 + c2 AS c3) ON c2 = c3
            |--------------^^^
            |""".stripMargin)
    }
  }

  test("SPARK-35789: INVALID_SQL_SYNTAX - LATERAL can only be used with subquery") {
    Seq(
      "SELECT * FROM t1, LATERAL t2" -> 26,
      "SELECT * FROM t1 JOIN LATERAL t2" -> 30,
      "SELECT * FROM t1, LATERAL (t2 JOIN t3)" -> 26,
      "SELECT * FROM t1, LATERAL (LATERAL t2)" -> 26,
      "SELECT * FROM t1, LATERAL VALUES (0, 1)" -> 26,
      "SELECT * FROM t1, LATERAL RANGE(0, 1)" -> 26
    ).foreach { case (sqlText, pos) =>
      validateParsingError(
        sqlText = sqlText,
        errorClass = "INVALID_SQL_SYNTAX",
        sqlState = "42000",
        message =
          s"""
            |Invalid SQL syntax: LATERAL can only be used with subquery.(line 1, pos $pos)
            |
            |== SQL ==
            |$sqlText
            |${"-" * pos}^^^
            |""".stripMargin)
    }
  }

  test("UNSUPPORTED_FEATURE: NATURAL CROSS JOIN is not supported") {
    validateParsingError(
      sqlText = "SELECT * FROM a NATURAL CROSS JOIN b",
      errorClass = "UNSUPPORTED_FEATURE",
      sqlState = "0A000",
      message =
        """
          |The feature is not supported: NATURAL CROSS JOIN.(line 1, pos 14)
          |
          |== SQL ==
          |SELECT * FROM a NATURAL CROSS JOIN b
          |--------------^^^
          |""".stripMargin)
  }
}
