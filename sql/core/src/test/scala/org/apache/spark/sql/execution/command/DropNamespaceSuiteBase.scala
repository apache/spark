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
 * This base suite contains unified tests for the `DROP NAMESPACE` command that check V1 and V2
 * table catalogs. The tests that cannot run for all supported catalogs are located in more
 * specific test suites:
 *
 *   - V2 table catalog tests: `org.apache.spark.sql.execution.command.v2.DropNamespaceSuite`
 *   - V1 table catalog tests: `org.apache.spark.sql.execution.command.v1.DropNamespaceSuiteBase`
 *     - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.DropNamespaceSuite`
 *     - V1 Hive External catalog: `org.apache.spark.sql.hive.execution.command.DropNamespaceSuite`
 */
trait DropNamespaceSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "DROP NAMESPACE"

  protected def builtinTopNamespaces: Seq[String] = Seq.empty
  protected def isCasePreserving: Boolean = true
  protected def namespaceAlias: String = "namespace"

  protected def checkNamespace(expected: Seq[String]): Unit = {
    val df = spark.sql(s"SHOW NAMESPACES IN $catalog")
    assert(df.schema === new StructType().add("namespace", StringType, false))
    checkAnswer(df, expected.map(Row(_)))
  }

  test("basic tests") {
    sql(s"CREATE NAMESPACE $catalog.ns")
    checkNamespace(Seq("ns") ++ builtinTopNamespaces)

    sql(s"DROP NAMESPACE $catalog.ns")
    checkNamespace(builtinTopNamespaces)
  }

  test("test handling of 'IF EXISTS'") {
    // It must not throw any exceptions
    sql(s"DROP NAMESPACE IF EXISTS $catalog.unknown")
    checkNamespace(builtinTopNamespaces)
  }

  test("namespace does not exist") {
    // Namespace $catalog.unknown does not exist.
    val e = intercept[AnalysisException] {
      sql(s"DROP NAMESPACE $catalog.unknown")
    }
    checkError(e,
      condition = "SCHEMA_NOT_FOUND",
      parameters = Map("schemaName" -> s"`$catalog`.`unknown`"))
  }

  test("drop non-empty namespace with a non-cascading mode") {
    sql(s"CREATE NAMESPACE $catalog.ns")
    sql(s"CREATE TABLE $catalog.ns.table (id bigint) $defaultUsing")
    checkNamespace(Seq("ns") ++ builtinTopNamespaces)

    // $catalog.ns.table is present, thus $catalog.ns cannot be dropped.
    val e = intercept[AnalysisException] {
      sql(s"DROP NAMESPACE $catalog.ns")
    }
    checkError(e,
      condition = "SCHEMA_NOT_EMPTY",
      parameters = Map("schemaName" -> "`ns`"))
    sql(s"DROP TABLE $catalog.ns.table")

    // Now that $catalog.ns is empty, it can be dropped.
    sql(s"DROP NAMESPACE $catalog.ns")
    checkNamespace(builtinTopNamespaces)
  }

  test("drop non-empty namespace with a cascade mode") {
    sql(s"CREATE NAMESPACE $catalog.ns")
    sql(s"CREATE TABLE $catalog.ns.table (id bigint) $defaultUsing")
    checkNamespace(Seq("ns") ++ builtinTopNamespaces)

    sql(s"DROP NAMESPACE $catalog.ns CASCADE")
    checkNamespace(builtinTopNamespaces)
  }

  test("drop current namespace") {
    sql(s"CREATE NAMESPACE $catalog.ns")
    sql(s"USE $catalog.ns")
    sql(s"DROP NAMESPACE $catalog.ns")
    checkNamespace(builtinTopNamespaces)
  }

  test("drop namespace with case sensitivity") {
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        sql(s"CREATE NAMESPACE $catalog.AAA")
        sql(s"CREATE NAMESPACE $catalog.bbb")
        // TODO: The v1 in-memory catalog should be case preserving as well.
        val casePreserving = isCasePreserving && (catalogVersion == "V2" || caseSensitive)
        val expected = if (casePreserving) "AAA" else "aaa"

        sql(s"DROP NAMESPACE $catalog.$expected")
        sql(s"DROP NAMESPACE $catalog.bbb")
        checkNamespace(builtinTopNamespaces)
      }
    }
  }
}
