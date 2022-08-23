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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
import org.apache.spark.sql.execution.command

/**
 * The class contains tests for the `SHOW TABLES` command to check V2 table catalogs.
 */
class ShowTablesSuite extends command.ShowTablesSuiteBase with CommandSuiteBase {
  override def defaultNamespace: Seq[String] = Nil

  // The test fails for V1 catalog with the error:
  // org.apache.spark.sql.AnalysisException:
  //   The namespace in session catalog must have exactly one name part: spark_catalog.n1.n2.db
  test("show tables in nested namespaces") {
    withTable(s"$catalog.n1.n2.db") {
      spark.sql(s"CREATE TABLE $catalog.n1.n2.db.table_name (id bigint, data string) $defaultUsing")
      runShowTablesSql(
        s"SHOW TABLES FROM $catalog.n1.n2.db",
        Seq(Row("n1.n2.db", "table_name", false)))
    }
  }

  // The test fails for V1 catalog with the error:
  // org.apache.spark.sql.AnalysisException:
  //   The namespace in session catalog must have exactly one name part: spark_catalog.table
  test("using v2 catalog with empty namespace") {
    withTable(s"$catalog.table") {
      spark.sql(s"CREATE TABLE $catalog.table (id bigint, data string) $defaultUsing")
      runShowTablesSql(s"SHOW TABLES FROM $catalog", Seq(Row("", "table", false)))
    }
  }

  // The test fails for V1 catalog with the error:
  // org.apache.spark.sql.AnalysisException:
  //   The namespace in session catalog must have exactly one name part: spark_catalog.ns1.ns2.tbl
  test("SHOW TABLE EXTENDED not valid v1 database") {
    def testV1CommandNamespace(sqlCommand: String, namespace: String): Unit = {
      val e = intercept[AnalysisException] {
        sql(sqlCommand)
      }
      assert(e.message.contains(s"SHOW TABLE EXTENDED is not supported for v2 tables"))
    }

    val namespace = s"$catalog.ns1.ns2"
    val table = "tbl"
    withTable(s"$namespace.$table") {
      sql(s"CREATE TABLE $namespace.$table (id bigint, data string) " +
        s"$defaultUsing PARTITIONED BY (id)")

      testV1CommandNamespace(s"SHOW TABLE EXTENDED FROM $namespace LIKE 'tb*'",
        namespace)
      testV1CommandNamespace(s"SHOW TABLE EXTENDED IN $namespace LIKE 'tb*'",
        namespace)
      testV1CommandNamespace("SHOW TABLE EXTENDED " +
        s"FROM $namespace LIKE 'tb*' PARTITION(id=1)",
        namespace)
      testV1CommandNamespace("SHOW TABLE EXTENDED " +
        s"IN $namespace LIKE 'tb*' PARTITION(id=1)",
        namespace)
    }
  }

  // TODO(SPARK-33393): Support SHOW TABLE EXTENDED in DSv2
  test("SHOW TABLE EXTENDED: an existing table") {
    val table = "people"
    withTable(s"$catalog.$table") {
      sql(s"CREATE TABLE $catalog.$table (name STRING, id INT) $defaultUsing")
      val errMsg = intercept[AnalysisException] {
        sql(s"SHOW TABLE EXTENDED FROM $catalog LIKE '*$table*'").collect()
      }.getMessage
      assert(errMsg.contains("SHOW TABLE EXTENDED is not supported for v2 tables"))
    }
  }

  test("show table in a not existing namespace") {
    val msg = intercept[NoSuchNamespaceException] {
      runShowTablesSql(s"SHOW TABLES IN $catalog.unknown", Seq())
    }.getMessage
    assert(msg.matches("(Database|Namespace) 'unknown' not found"))
  }
}
