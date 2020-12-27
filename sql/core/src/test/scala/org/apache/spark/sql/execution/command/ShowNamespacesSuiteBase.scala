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

trait ShowNamespacesSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "SHOW NAMESPACES"

  protected def runShowNamespacesSql(sqlText: String, expected: Seq[String]): Unit = {
    val df = spark.sql(sqlText)
    assert(df.schema === new StructType().add("namespace", StringType, false))
    checkAnswer(df, expected.map(Row(_)))
  }

  protected def topNamespaces(ns: Seq[String]): Seq[String]

  test("a namespace doesn't exist") {
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
}
