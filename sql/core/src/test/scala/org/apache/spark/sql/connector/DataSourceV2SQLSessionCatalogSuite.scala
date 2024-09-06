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

package org.apache.spark.sql.connector

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryTable, Table, TableCatalog}

class DataSourceV2SQLSessionCatalogSuite
  extends InsertIntoTests(supportsDynamicOverwrite = true, includeSQLOnlyTests = true)
  with AlterTableTests
  with SessionCatalogTest[InMemoryTable, InMemoryTableSessionCatalog] {

  override protected val catalogAndNamespace = ""

  override protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode): Unit = {
    val tmpView = "tmp_view"
    withTempView(tmpView) {
      insert.createOrReplaceTempView(tmpView)
      val overwrite = if (mode == SaveMode.Overwrite) "OVERWRITE" else "INTO"
      sql(s"INSERT $overwrite TABLE $tableName SELECT * FROM $tmpView")
    }
  }

  override protected def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(spark.table(tableName), expected)
    checkAnswer(sql(s"SELECT * FROM $tableName"), expected)
    checkAnswer(sql(s"SELECT * FROM default.$tableName"), expected)
    checkAnswer(sql(s"TABLE $tableName"), expected)
  }

  override def getTableMetadata(tableName: String): Table = {
    val v2Catalog = spark.sessionState.catalogManager.currentCatalog
    val nameParts = spark.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    v2Catalog.asInstanceOf[TableCatalog]
      .loadTable(Identifier.of(nameParts.init.toArray, nameParts.last))
  }

  test("SPARK-30697: catalog.isView doesn't throw an error for specialized identifiers") {
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")

      def idResolver(id: Identifier): Identifier = Identifier.of(Array("default"), id.name())

      InMemoryTableSessionCatalog.withCustomIdentifierResolver(idResolver) {
        // The following should not throw AnalysisException.
        sql(s"DESCRIBE TABLE ignored.$t1")
      }
    }
  }

  test("SPARK-33651: allow CREATE EXTERNAL TABLE without LOCATION") {
    withTable("t") {
      val prop = TestV2SessionCatalogBase.SIMULATE_ALLOW_EXTERNAL_PROPERTY + "=true"
      // The following should not throw AnalysisException.
      sql(s"CREATE EXTERNAL TABLE t (i INT) USING $v2Format TBLPROPERTIES($prop)")
    }
  }

  test("SPARK-49152: partition columns should be put at the end") {
    withTable("t") {
      sql("CREATE TABLE t (c1 INT, c2 INT) USING json PARTITIONED BY (c1)")
      // partition columns should be put at the end.
      assert(getTableMetadata("default.t").columns().map(_.name()) === Seq("c2", "c1"))
    }
  }

  test("SPARK-49246: test saveAsTable") {
    withTable("t") {
      sql("CREATE TABLE t(id INT) USING csv")
      val df = spark.range(10)
      df.write.mode(SaveMode.Overwrite).format("csv").saveAsTable("t")
      verifyTable("t", df.toDF("id"))
    }
  }
}
