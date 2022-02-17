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

import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedDBObjectName}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.plans.logical.CreateNamespace

class CreateNamespaceParserSuite extends AnalysisTest {
  test("create namespace -- backward compatibility with DATABASE/DBPROPERTIES") {
    val expected = CreateNamespace(
      UnresolvedDBObjectName(Seq("a", "b", "c"), true),
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

  test("create namespace -- check duplicates") {
    def createNamespace(duplicateClause: String): String = {
      s"""
         |CREATE NAMESPACE IF NOT EXISTS a.b.c
         |$duplicateClause
         |$duplicateClause
      """.stripMargin
    }
    val sql1 = createNamespace("COMMENT 'namespace_comment'")
    val sql2 = createNamespace("LOCATION '/home/user/db'")
    val sql3 = createNamespace("WITH PROPERTIES ('a'='a', 'b'='b', 'c'='c')")
    val sql4 = createNamespace("WITH DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')")

    intercept(sql1, "Found duplicate clauses: COMMENT")
    intercept(sql2, "Found duplicate clauses: LOCATION")
    intercept(sql3, "Found duplicate clauses: WITH PROPERTIES")
    intercept(sql4, "Found duplicate clauses: WITH DBPROPERTIES")
  }

  test("create namespace - property values must be set") {
    intercept(
      "CREATE NAMESPACE a.b.c WITH PROPERTIES('key_without_value', 'key_with_value'='x')",
      "Operation not allowed: Values must be specified for key(s): [key_without_value]")
  }

  test("create namespace -- either PROPERTIES or DBPROPERTIES is allowed") {
    val sql =
      s"""
         |CREATE NAMESPACE IF NOT EXISTS a.b.c
         |WITH PROPERTIES ('a'='a', 'b'='b', 'c'='c')
         |WITH DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')
      """.stripMargin
    intercept(sql, "Either PROPERTIES or DBPROPERTIES is allowed")
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
        UnresolvedDBObjectName(Seq("a", "b", "c"), true),
        ifNotExists = false,
        Map(
          "a" -> "1",
          "b" -> "0.1",
          "c" -> "true",
          "location" -> "/home/user/db")))
  }

  private def intercept(sqlCommand: String, messages: String*): Unit =
    interceptParseException(parsePlan)(sqlCommand, messages: _*)
}
