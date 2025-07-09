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

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCapability, TableCatalog, TableChange, TableSummary}
import org.apache.spark.sql.connector.expressions.{LogicalExpressions, Transform}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DataSourceV2GeneralTableSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.general_catalog", classOf[TestingGeneralCatalog].getName)

  test("file source table") {
    withTempPath { path =>
      val loc = path.getCanonicalPath
      val tableName = s"general_catalog.`$loc`.test_json"

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
      val tableName = s"general_catalog.`$loc`.test_partitioned_json"

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
    val tableName = "general_catalog.default.test_v2"
    spark.table(tableName).show()
    spark.table(tableName).explain(true)
    checkAnswer(spark.table(tableName), 0.until(10).map(i => Row(i, -i)))
  }

  test("general table as view") {
    // TODO: support creating views.
    withTable("spark_catalog.default.t") {
      Seq("a", "b").toDF("col").write.saveAsTable("spark_catalog.default.t")
      // Make sure the view config applies correctly.
      intercept[Exception](spark.table("general_catalog.ansi.test_view").collect())
      checkAnswer(spark.table("general_catalog.non_ansi.test_view"), Row("b", null))
    }
  }
}

class TestingGeneralCatalog extends TableCatalog {

  override def loadTable(ident: Identifier): Table = {
    ident.name() match {
      case "test_json" => new TestingGeneralJsonTable(ident.namespace().head)
      case "test_partitioned_json" => new TestingGeneralPartitionedJsonTable(ident.namespace().head)
      case "test_v2" => new TestingGeneralV2Table
      case "test_view" => new TestingGeneralViewTable(ident.namespace().head == "ansi")
      case _ => throw new NoSuchTableException(ident)
    }
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new RuntimeException("shouldn't be called")
  }
  override def dropTable(ident: Identifier): Boolean = {
    throw new RuntimeException("shouldn't be called")
  }
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new RuntimeException("shouldn't be called")
  }
  override def listTables(namespace: Array[String]): Array[Identifier] = {
    throw new RuntimeException("shouldn't be called")
  }

  private var catalogName = ""
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
  }
  override def name(): String = catalogName
}

class TestingGeneralJsonTable(path: String) extends Table {
  override def name(): String = "test_json"

  override def schema(): StructType = new StructType().add("col", "string")

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(TableCapability.GENERAL_TABLE)

  override def properties(): util.Map[String, String] = util.Map.ofEntries(
    util.Map.entry(TableCatalog.PROP_PROVIDER, "json"),
    util.Map.entry(TableCatalog.PROP_LOCATION, path)
  );
}

class TestingGeneralPartitionedJsonTable(path: String) extends Table {
  override def name(): String = "test_partitioned_json"

  override def schema(): StructType = new StructType().add("c1", "int").add("c2", "int")

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(TableCapability.GENERAL_TABLE)

  override def partitioning(): Array[Transform] =
    Array(LogicalExpressions.identity(LogicalExpressions.reference(Seq("c2"))))

  override def properties(): util.Map[String, String] = util.Map.ofEntries(
    util.Map.entry(TableCatalog.PROP_PROVIDER, "json"),
    util.Map.entry(TableCatalog.PROP_LOCATION, path)
  );
}

class TestingGeneralV2Table extends Table {
  override def name(): String = "test_v2"

  override def schema(): StructType = FakeV2Provider.schema

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(TableCapability.GENERAL_TABLE)

  override def properties(): util.Map[String, String] = util.Map.ofEntries(
    util.Map.entry(TableCatalog.PROP_PROVIDER, classOf[FakeV2Provider].getName)
  );
}

class TestingGeneralViewTable(ansi: Boolean) extends Table {
  override def name(): String = "test_view"

  override def schema(): StructType = new StructType().add("col", "string").add("i", "int")

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(TableCapability.GENERAL_TABLE)

  override def properties(): util.Map[String, String] = util.Map.ofEntries(
    util.Map.entry(TableCatalog.PROP_TABLE_TYPE, TableSummary.VIEW_TABLE_TYPE),
    util.Map.entry(
      TableCatalog.PROP_VIEW_TEXT,
      "SELECT col, col::int AS i FROM spark_catalog.default.t WHERE col = 'b'"),
    util.Map.entry(
      TableCatalog.VIEW_CONF_PREFIX + SQLConf.ANSI_ENABLED.key,
      ansi.toString)
  );
}
