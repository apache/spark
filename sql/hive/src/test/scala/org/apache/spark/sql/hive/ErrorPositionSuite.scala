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

package org.apache.spark.sql.hive

import scala.util.Try

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.hive.test.TestHiveSingleton

class ErrorPositionSuite extends QueryTest with TestHiveSingleton with BeforeAndAfterEach {
  import spark.implicits._

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    if (spark.catalog.listTables().collect().map(_.name).contains("src")) {
      spark.catalog.dropTempView("src")
    }
    Seq((1, "")).toDF("key", "value").createOrReplaceTempView("src")
    Seq((1, 1, 1)).toDF("a", "a", "b").createOrReplaceTempView("dupAttributes")
  }

  override protected def afterEach(): Unit = {
    try {
      spark.catalog.dropTempView("src")
      spark.catalog.dropTempView("dupAttributes")
    } finally {
      super.afterEach()
    }
  }

  positionTest("ambiguous attribute reference 1",
    "SELECT a from dupAttributes", "a")

  positionTest("ambiguous attribute reference 2",
    "SELECT a, b from dupAttributes", "a")

  positionTest("ambiguous attribute reference 3",
    "SELECT b, a from dupAttributes", "a")

  positionTest("unresolved attribute 1",
    "SELECT x FROM src", "x")

  positionTest("unresolved attribute 2",
    "SELECT        x FROM src", "x")

  positionTest("unresolved attribute 3",
    "SELECT key, x FROM src", "x")

  positionTest("unresolved attribute 4",
    """SELECT key,
      |x FROM src
    """.stripMargin, "x")

  positionTest("unresolved attribute 5",
    """SELECT key,
      |  x FROM src
    """.stripMargin, "x")

  positionTest("unresolved attribute 6",
    """SELECT key,
      |
      |  1 + x FROM src
    """.stripMargin, "x")

  positionTest("unresolved attribute 7",
    """SELECT key,
      |
      |  1 + x + 1 FROM src
    """.stripMargin, "x")

  positionTest("multi-char unresolved attribute",
    """SELECT key,
      |
      |  1 + abcd + 1 FROM src
    """.stripMargin, "abcd")

  positionTest("unresolved attribute group by",
    """SELECT key FROM src GROUP BY
       |x
    """.stripMargin, "x")

  positionTest("unresolved attribute order by",
    """SELECT key FROM src ORDER BY
      |x
    """.stripMargin, "x")

  positionTest("unresolved attribute where",
    """SELECT key FROM src
      |WHERE x = true
    """.stripMargin, "x")

  positionTest("unresolved attribute backticks",
    "SELECT `x` FROM src", "`x`")

  positionTest("parse error",
    "SELECT WHERE", "WHERE")

  positionTest("bad relation",
    "SELECT * FROM badTable", "badTable")

  ignore("other expressions") {
    positionTest("bad addition",
      "SELECT 1 + array(1)", "1 + array")
  }

  /**
   * Creates a test that checks to see if the error thrown when analyzing a given query includes
   * the location of the given token in the query string.
   *
   * @param name the name of the test
   * @param query the query to analyze
   * @param token a unique token in the string that should be indicated by the exception
   */
  def positionTest(name: String, query: String, token: String): Unit = {
    def ast = spark.sessionState.sqlParser.parsePlan(query)
    def parseTree = Try(quietly(ast.treeString)).getOrElse("<failed to parse>")

    test(name) {
      val error = intercept[AnalysisException] {
        quietly(spark.sql(query))
      }

      assert(!error.getMessage.contains("Seq("))
      assert(!error.getMessage.contains("List("))

      val (line, expectedLineNum) = query.split("\n").zipWithIndex.collect {
        case (l, i) if l.contains(token) => (l, i + 1)
      }.headOption.getOrElse(sys.error(s"Invalid test. Token $token not in $query"))
      val actualLine = error.line.getOrElse {
        fail(
          s"line not returned for error '${error.getMessage}' on token $token\n$parseTree"
        )
      }
      assert(actualLine === expectedLineNum, "wrong line")

      val expectedStart = line.indexOf(token)
      val actualStart = error.startPosition.getOrElse {
        fail(s"start not returned for error on token $token\n${ast.treeString}")
      }
      assert(expectedStart === actualStart,
       s"""Incorrect start position.
          |== QUERY ==
          |$query
          |
          |== AST ==
          |$parseTree
          |
          |Actual: $actualStart, Expected: $expectedStart
          |$line
          |${" " * actualStart}^
          |0123456789 123456789 1234567890
          |          2         3
        """.stripMargin)
    }
  }
}
