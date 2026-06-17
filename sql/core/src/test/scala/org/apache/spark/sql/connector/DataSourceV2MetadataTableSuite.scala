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

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.{Identifier, MetadataTable, Table, TableCatalog, TableChange, TableInfo, TableSummary}
import org.apache.spark.sql.connector.expressions.LogicalExpressions
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Tests for the data-source-table side of [[MetadataTable]]: a v2 catalog returns
 * metadata-only tables and Spark reads / writes them via the V1 data-source path.
 * View-related paths live in [[DataSourceV2MetadataViewSuite]].
 */
class DataSourceV2MetadataTableSuite extends SharedSparkSession {
  import testImplicits._

  override def sparkConf: SparkConf = super.sparkConf
    .set(
      "spark.sql.catalog.table_catalog",
      classOf[TestingDataSourceTableCatalog].getName)

  test("file source table") {
    withTempPath { path =>
      val loc = path.getCanonicalPath
      val tableName = s"table_catalog.`$loc`.test_json"

      spark.range(10).select($"id".cast("string").as("col")).write.json(loc)
      checkAnswer(spark.table(tableName), 0.until(10).map(i => Row(i.toString)))

      sql(s"INSERT INTO $tableName SELECT 'abc'")
      checkAnswer(spark.table(tableName), 0.until(10).map(i => Row(i.toString)) :+ Row("abc"))

      sql(s"INSERT OVERWRITE $tableName SELECT 'xyz'")
      checkAnswer(spark.table(tableName), Row("xyz"))
    }
  }

  test("partitioned file source table") {
    withTempPath { path =>
      val loc = path.getCanonicalPath
      val tableName = s"table_catalog.`$loc`.test_partitioned_json"

      Seq(1 -> 1, 2 -> 1).toDF("c1", "c2").write.partitionBy("c2").json(loc)
      checkAnswer(spark.table(tableName), Seq(Row(1, 1), Row(2, 1)))

      sql(s"INSERT INTO $tableName SELECT 1, 2")
      checkAnswer(spark.table(tableName), Seq(Row(1, 1), Row(2, 1), Row(1, 2)))

      sql(s"INSERT INTO $tableName PARTITION(c2=3) SELECT 1")
      checkAnswer(spark.table(tableName), Seq(Row(1, 1), Row(2, 1), Row(1, 2), Row(1, 3)))

      sql(s"INSERT OVERWRITE $tableName PARTITION(c2=2) SELECT 10")
      checkAnswer(spark.table(tableName), Seq(Row(1, 1), Row(2, 1), Row(10, 2), Row(1, 3)))

      sql(s"INSERT OVERWRITE $tableName SELECT 20, 20")
      checkAnswer(spark.table(tableName), Row(20, 20))
    }
  }

  // TODO: move the v2 data source table handling from V2SessionCatalog to the analyzer
  ignore("v2 data source table") {
    val tableName = "table_catalog.default.test_v2"
    checkAnswer(spark.table(tableName), 0.until(10).map(i => Row(i, -i)))
  }

  test("fully-qualified column reference uses the real catalog name") {
    withTempPath { path =>
      val loc = path.getCanonicalPath
      val tableName = s"table_catalog.`$loc`.test_json"

      spark.range(3).select($"id".cast("string").as("col")).write.json(loc)

      // 1-part and 2-part references resolve via last-part suffix matching.
      checkAnswer(
        sql(s"SELECT test_json.col FROM $tableName"),
        Seq(Row("0"), Row("1"), Row("2")))
      checkAnswer(
        sql(s"SELECT `$loc`.test_json.col FROM $tableName"),
        Seq(Row("0"), Row("1"), Row("2")))

      // 3-part reference uses the real catalog name. `V1Table.toCatalogTable` sets
      // `CatalogTable.multipartIdentifier` to `[table_catalog, <loc>, test_json]`; the
      // SessionCatalog change in this PR makes `getRelation` prefer that over the hardcoded
      // `spark_catalog` qualifier, so the SubqueryAlias carries the real catalog and this
      // 3-part column ref resolves.
      checkAnswer(
        sql(s"SELECT $tableName.col FROM $tableName"),
        Seq(Row("0"), Row("1"), Row("2")))
    }
  }
}

/**
 * A read-only [[TableCatalog]] that returns [[MetadataTable]] for a small set of canned
 * table fixtures. Used to drive the data-source-table read path (file source + v2 provider)
 * through Spark's V1 data-source machinery.
 */
class TestingDataSourceTableCatalog extends TableCatalog {
  override def loadTable(ident: Identifier): Table = ident.name() match {
    case "test_json" =>
      val info = new TableInfo.Builder()
        .withSchema(new StructType().add("col", "string"))
        .withProvider("json")
        .withLocation(ident.namespace().head)
        .withTableType(TableSummary.EXTERNAL_TABLE_TYPE)
        .build()
      new MetadataTable(info, ident.toString)
    case "test_partitioned_json" =>
      val partitioning = LogicalExpressions.identity(LogicalExpressions.reference(Seq("c2")))
      val info = new TableInfo.Builder()
        .withSchema(new StructType().add("c1", "int").add("c2", "int"))
        .withProvider("json")
        .withLocation(ident.namespace().head)
        .withTableType(TableSummary.EXTERNAL_TABLE_TYPE)
        .withPartitions(Array(partitioning))
        .build()
      new MetadataTable(info, ident.toString)
    case "test_v2" =>
      val info = new TableInfo.Builder()
        .withSchema(FakeV2Provider.schema)
        .withProvider(classOf[FakeV2Provider].getName)
        .build()
      new MetadataTable(info, ident.toString)
    case _ => throw new NoSuchTableException(ident)
  }

  override def createTable(ident: Identifier, info: TableInfo): Table =
    throw new RuntimeException("shouldn't be called")
  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    throw new RuntimeException("shouldn't be called")
  override def dropTable(ident: Identifier): Boolean =
    throw new RuntimeException("shouldn't be called")
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    throw new RuntimeException("shouldn't be called")
  override def listTables(namespace: Array[String]): Array[Identifier] =
    throw new RuntimeException("shouldn't be called")

  private var catalogName = ""
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
  }
  override def name(): String = catalogName
}
