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

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructType}

/**
 * This base suite contains unified tests for the `SHOW NAMESPACES` and `SHOW DATABASES` commands
 * that check V1 and V2 table catalogs. The tests that cannot run for all supported catalogs are
 * located in more specific test suites:
 *
 *   - V2 table catalog tests: `org.apache.spark.sql.execution.command.v2.ShowNamespacesSuite`
 *   - V1 table catalog tests: `org.apache.spark.sql.execution.command.v1.ShowNamespacesSuiteBase`
 *     - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.ShowNamespacesSuite`
 *     - V1 Hive External catalog: `org.apache.spark.sql.hive.execution.command.ShowNamespacesSuite`
 */
trait ShowNamespacesSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "SHOW NAMESPACES"

  protected def runShowNamespacesSql(sqlText: String, expected: Seq[String]): Unit = {
    val df = spark.sql(sqlText)
    assert(df.schema === new StructType().add("namespace", StringType, false))
    checkAnswer(df, expected.map(Row(_)))
  }

  protected def topNamespaces(ns: Seq[String]): Seq[String]

  test("IN namespace doesn't exist") {
    val errMsg = intercept[AnalysisException] {
      sql("SHOW NAMESPACES in dummy")
    }.getMessage
    assert(errMsg.contains("Namespace 'dummy' not found"))
  }

  test("default namespace") {
    withSQLConf(SQLConf.DEFAULT_CATALOG.key -> catalog) {
      runShowNamespacesSql("SHOW NAMESPACES", topNamespaces(Seq.empty))
    }
    runShowNamespacesSql(s"SHOW NAMESPACES IN $catalog", topNamespaces(Seq.empty))
  }

  test("at the top level") {
    withNamespace(s"$catalog.ns1", s"$catalog.ns2") {
      sql(s"CREATE DATABASE $catalog.ns1")
      sql(s"CREATE NAMESPACE $catalog.ns2")

      runShowNamespacesSql(s"SHOW NAMESPACES IN $catalog", topNamespaces(Seq("ns1", "ns2")))
    }
  }

  test("exact matching") {
    withNamespace(s"$catalog.ns1", s"$catalog.ns2") {
      sql(s"CREATE NAMESPACE $catalog.ns1")
      sql(s"CREATE NAMESPACE $catalog.ns2")
      Seq(
        s"SHOW NAMESPACES IN $catalog LIKE 'ns2'",
        s"SHOW NAMESPACES IN $catalog 'ns2'",
        s"SHOW NAMESPACES FROM $catalog LIKE 'ns2'",
        s"SHOW NAMESPACES FROM $catalog 'ns2'").foreach { sqlCmd =>
        withClue(sqlCmd) {
          runShowNamespacesSql(sqlCmd, Seq("ns2"))
        }
      }
    }
  }

  test("does not match to any namespace") {
    Seq(
      "SHOW DATABASES LIKE 'non-existentdb'",
      "SHOW NAMESPACES 'non-existentdb'").foreach { sqlCmd =>
      runShowNamespacesSql(sqlCmd, Seq.empty)
    }
  }

  test("show root namespaces with the default catalog") {
    withSQLConf(SQLConf.DEFAULT_CATALOG.key -> catalog) {
      runShowNamespacesSql("SHOW NAMESPACES", topNamespaces(Seq.empty))

      withNamespace("ns1", "ns2") {
        sql(s"CREATE NAMESPACE ns1")
        sql(s"CREATE NAMESPACE ns2")

        runShowNamespacesSql("SHOW NAMESPACES", topNamespaces(Seq("ns1", "ns2")))
        runShowNamespacesSql("SHOW NAMESPACES LIKE '*1*'", Seq("ns1"))
      }
    }
  }

  test("complex namespace patterns") {
    withNamespace(s"$catalog.showdb2b", s"$catalog.showdb1a") {
      sql(s"CREATE NAMESPACE $catalog.showdb2b")
      sql(s"CREATE NAMESPACE $catalog.showdb1a")

      Seq(
        "'*db1A'" -> Seq("showdb1a"),
        "'*db1A|*db2B'" -> Seq("showdb1a", "showdb2b")
      ).foreach { case (pattern, expected) =>
        runShowNamespacesSql(s"SHOW NAMESPACES IN $catalog LIKE $pattern", expected)
      }
    }
  }
}
