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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.connector.InMemoryTableCatalog
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructType}

class ShowTablesSuite extends command.ShowTablesSuiteBase with SharedSparkSession {
  override def version: String = "V2"
  override def catalog: String = "test_catalog"
  override def defaultNamespace: Seq[String] = Nil
  override def defaultUsing: String = "USING _"
  override def showSchema: StructType = {
    new StructType()
      .add("namespace", StringType, nullable = false)
      .add("tableName", StringType, nullable = false)
  }
  override def getRows(showRows: Seq[ShowRow]): Seq[Row] = {
    showRows.map {
      case ShowRow(namespace, table, _) => Row(namespace, table)
    }
  }

  override def sparkConf: SparkConf = super.sparkConf
    .set(s"spark.sql.catalog.$catalog", classOf[InMemoryTableCatalog].getName)

  // The test fails for V1 catalog with the error:
  // org.apache.spark.sql.AnalysisException:
  //   The namespace in session catalog must have exactly one name part: spark_catalog.n1.n2.db
  test("show tables in nested namespaces") {
    withTable(s"$catalog.n1.n2.db") {
      spark.sql(s"CREATE TABLE $catalog.n1.n2.db.table_name (id bigint, data string) $defaultUsing")
      runShowTablesSql(
        s"SHOW TABLES FROM $catalog.n1.n2.db",
        Seq(ShowRow("n1.n2.db", "table_name", false)))
    }
  }

  // The test fails for V1 catalog with the error:
  // org.apache.spark.sql.AnalysisException:
  //   The namespace in session catalog must have exactly one name part: spark_catalog.table
  test("using v2 catalog with empty namespace") {
    withTable(s"$catalog.table") {
      spark.sql(s"CREATE TABLE $catalog.table (id bigint, data string) $defaultUsing")
      runShowTablesSql(s"SHOW TABLES FROM $catalog", Seq(ShowRow("", "table", false)))
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
      assert(e.message.contains(s"The database name is not valid: ${namespace}"))
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
      val errMsg = intercept[NoSuchDatabaseException] {
        sql(s"SHOW TABLE EXTENDED FROM $catalog LIKE '*$table*'").collect()
      }.getMessage
      assert(errMsg.contains(s"Database '$catalog' not found"))
    }
  }
}
