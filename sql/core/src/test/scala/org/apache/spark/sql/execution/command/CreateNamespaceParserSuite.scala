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

package org.apache.spark.sql.execution.command

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedNamespace}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.CreateNamespace
import org.apache.spark.sql.test.SharedSparkSession

class CreateNamespaceParserSuite extends AnalysisTest with SharedSparkSession {

  private def parseException(sqlText: String): SparkThrowable = {
    intercept[ParseException](sql(sqlText).collect())
  }

  test("create namespace - backward compatibility with DATABASE/DBPROPERTIES") {
    val expected = CreateNamespace(
      UnresolvedNamespace(Seq("a", "b", "c")),
      ifNotExists = true,
      Map(
        "a" -> "a",
        "b" -> "b",
        "c" -> "c",
        "comment" -> "namespace_comment",
        "location" -> "/home/user/db"))

    comparePlans(
      parsePlan(
        """
          |CREATE NAMESPACE IF NOT EXISTS a.b.c
          |WITH PROPERTIES ('a'='a', 'b'='b', 'c'='c')
          |COMMENT 'namespace_comment' LOCATION '/home/user/db'
        """.stripMargin),
      expected)

    comparePlans(
      parsePlan(
        """
          |CREATE DATABASE IF NOT EXISTS a.b.c
          |WITH DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')
          |COMMENT 'namespace_comment' LOCATION '/home/user/db'
        """.stripMargin),
      expected)
  }

  test("create namespace - check duplicates") {
    def createNamespace(duplicateClause: String): String = {
      s"""CREATE NAMESPACE IF NOT EXISTS a.b.c
         |$duplicateClause
         |$duplicateClause""".stripMargin
    }

    val sql1 = createNamespace("COMMENT 'namespace_comment'")
    checkError(
      exception = parseException(sql1),
      errorClass = "_LEGACY_ERROR_TEMP_0041",
      parameters = Map("clauseName" -> "COMMENT"),
      context = ExpectedContext(
        fragment = sql1,
        start = 0,
        stop = 91))

    val sql2 = createNamespace("LOCATION '/home/user/db'")
    checkError(
      exception = parseException(sql2),
      errorClass = "_LEGACY_ERROR_TEMP_0041",
      parameters = Map("clauseName" -> "LOCATION"),
      context = ExpectedContext(
        fragment = sql2,
        start = 0,
        stop = 85))

    val sql3 = createNamespace("WITH PROPERTIES ('a'='a', 'b'='b', 'c'='c')")
    checkError(
      exception = parseException(sql3),
      errorClass = "_LEGACY_ERROR_TEMP_0041",
      parameters = Map("clauseName" -> "WITH PROPERTIES"),
      context = ExpectedContext(
        fragment = sql3,
        start = 0,
        stop = 123))

    val sql4 = createNamespace("WITH DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')")
    checkError(
      exception = parseException(sql4),
      errorClass = "_LEGACY_ERROR_TEMP_0041",
      parameters = Map("clauseName" -> "WITH DBPROPERTIES"),
      context = ExpectedContext(
        fragment = sql4,
        start = 0,
        stop = 127))
  }

  test("create namespace - property values must be set") {
    val sql = "CREATE NAMESPACE a.b.c WITH PROPERTIES('key_without_value', 'key_with_value'='x')"
    checkError(
      exception = parseException(sql),
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> "Values must be specified for key(s): [key_without_value]"),
      context = ExpectedContext(
        fragment = sql,
        start = 0,
        stop = 80))
  }

  test("create namespace - either PROPERTIES or DBPROPERTIES is allowed") {
    val sql =
      s"""CREATE NAMESPACE IF NOT EXISTS a.b.c
         |WITH PROPERTIES ('a'='a', 'b'='b', 'c'='c')
         |WITH DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')""".stripMargin
    checkError(
      exception = parseException(sql),
      errorClass = "UNSUPPORTED_FEATURE.SET_PROPERTIES_AND_DBPROPERTIES",
      parameters = Map.empty,
      context = ExpectedContext(
        fragment = sql,
        start = 0,
        stop = 125))
  }

  test("create namespace - support for other types in PROPERTIES") {
    val sql =
      """
        |CREATE NAMESPACE a.b.c
        |LOCATION '/home/user/db'
        |WITH PROPERTIES ('a'=1, 'b'=0.1, 'c'=TRUE)
      """.stripMargin
    comparePlans(
      parsePlan(sql),
      CreateNamespace(
        UnresolvedNamespace(Seq("a", "b", "c")),
        ifNotExists = false,
        Map(
          "a" -> "1",
          "b" -> "0.1",
          "c" -> "true",
          "location" -> "/home/user/db")))
  }
}
