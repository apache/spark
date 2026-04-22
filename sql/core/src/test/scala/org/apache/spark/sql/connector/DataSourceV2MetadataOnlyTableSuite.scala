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

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog.{Identifier, MetadataOnlyTable, StagedTable, StagingTableCatalog, Table, TableCatalog, TableCatalogCapability, TableChange, TableInfo, TableSummary}
import org.apache.spark.sql.connector.expressions.LogicalExpressions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DataSourceV2MetadataOnlyTableSuite extends QueryTest with SharedSparkSession {
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

  test("general table as view with stored current catalog/namespace") {
    withTable("spark_catalog.default.t") {
      Seq("a", "b").toDF("col").write.saveAsTable("spark_catalog.default.t")
      // View text uses the unqualified name `t`; it resolves via the stored
      // current catalog / namespace properties.
      checkAnswer(spark.table("general_catalog.ns.test_unqualified_view"), Row("b"))
    }
  }

  test("view current catalog/namespace are serialized into a single property") {
    val info = new TableInfo.Builder()
      .withSchema(new StructType().add("col", "string"))
      .withViewText("SELECT * FROM t")
      .withCurrentCatalogAndNamespace("spark_catalog", Array("default"))
      .build()
    val table = new MetadataOnlyTable(info)
    assert(table.properties().get(TableCatalog.PROP_VIEW_CURRENT_CATALOG_AND_NAMESPACE) ==
      "spark_catalog.default")
  }

  test("view current catalog/namespace quotes multi-part names with dots") {
    val info = new TableInfo.Builder()
      .withSchema(new StructType().add("col", "string"))
      .withViewText("SELECT * FROM t")
      .withCurrentCatalogAndNamespace("spark_catalog", Array("weird.db", "normal"))
      .build()
    val table = new MetadataOnlyTable(info)
    assert(table.properties().get(TableCatalog.PROP_VIEW_CURRENT_CATALOG_AND_NAMESPACE) ==
      "spark_catalog.`weird.db`.normal")
  }

  test("view with no current catalog/namespace omits the property") {
    val info = new TableInfo.Builder()
      .withSchema(new StructType().add("col", "string"))
      .withViewText("SELECT * FROM spark_catalog.default.t")
      .build()
    val table = new MetadataOnlyTable(info)
    assert(!table.properties().containsKey(
      TableCatalog.PROP_VIEW_CURRENT_CATALOG_AND_NAMESPACE))
  }

  test("CREATE VIEW on a v2 catalog") {
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
      sql("CREATE VIEW general_catalog.default.my_view AS " +
        "SELECT x FROM spark_catalog.default.t WHERE x > 1")
      checkAnswer(spark.table("general_catalog.default.my_view"), Seq(Row(2), Row(3)))
    }
  }

  test("CREATE VIEW IF NOT EXISTS is a no-op when the view exists") {
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
      sql("CREATE VIEW general_catalog.default.v_ifne AS " +
        "SELECT x FROM spark_catalog.default.t")
      // Re-running with IF NOT EXISTS should not fail and should not change the view.
      sql("CREATE VIEW IF NOT EXISTS general_catalog.default.v_ifne AS " +
        "SELECT x + 100 AS x FROM spark_catalog.default.t")
      checkAnswer(spark.table("general_catalog.default.v_ifne"),
        Seq(Row(1), Row(2), Row(3)))
    }
  }

  test("CREATE VIEW without IF NOT EXISTS fails when the view exists") {
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
      sql("CREATE VIEW general_catalog.default.v_dup AS " +
        "SELECT x FROM spark_catalog.default.t")
      intercept[AnalysisException] {
        sql("CREATE VIEW general_catalog.default.v_dup AS " +
          "SELECT x FROM spark_catalog.default.t")
      }
    }
  }

  test("CREATE OR REPLACE VIEW replaces an existing view") {
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
      sql("CREATE VIEW general_catalog.default.v_replace AS " +
        "SELECT x FROM spark_catalog.default.t WHERE x > 10")
      checkAnswer(spark.table("general_catalog.default.v_replace"), Seq.empty[Row])
      sql("CREATE OR REPLACE VIEW general_catalog.default.v_replace AS " +
        "SELECT x FROM spark_catalog.default.t WHERE x > 1")
      checkAnswer(spark.table("general_catalog.default.v_replace"), Seq(Row(2), Row(3)))
    }
  }

  test("CREATE VIEW on a catalog without SUPPORTS_CREATE_VIEW fails") {
    withSQLConf(
      "spark.sql.catalog.no_view_catalog" -> classOf[TestingTableOnlyCatalog].getName) {
      val ex = intercept[AnalysisException] {
        sql("CREATE VIEW no_view_catalog.default.v AS SELECT 1")
      }
      assert(ex.getCondition == "MISSING_CATALOG_ABILITY.VIEWS")
    }
  }

  test("CREATE VIEW rejects user column list with SCHEMA EVOLUTION") {
    withTable("spark_catalog.default.t") {
      Seq(1 -> 10).toDF("x", "y").write.saveAsTable("spark_catalog.default.t")
      // The parser either rejects `v(a, b) WITH SCHEMA EVOLUTION` outright or lets it through
      // to the exec, where `buildTableInfo` throws an internal error. Either is acceptable.
      val ex = intercept[Exception] {
        sql("CREATE VIEW general_catalog.default.v_evo (a, b) WITH SCHEMA EVOLUTION AS " +
          "SELECT x, y FROM spark_catalog.default.t")
      }
      assert(ex.isInstanceOf[AnalysisException] || ex.isInstanceOf[SparkException])
    }
  }

  test("CREATE VIEW rejects too-few / too-many user-specified columns") {
    withTable("spark_catalog.default.t") {
      Seq(1 -> 10).toDF("x", "y").write.saveAsTable("spark_catalog.default.t")
      intercept[AnalysisException] {
        sql("CREATE VIEW general_catalog.default.v_few (a) AS " +
          "SELECT x, y FROM spark_catalog.default.t")
      }
      intercept[AnalysisException] {
        sql("CREATE VIEW general_catalog.default.v_many (a, b, c) AS " +
          "SELECT x, y FROM spark_catalog.default.t")
      }
    }
  }

  test("CREATE VIEW rejects reference to a temporary function") {
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
      spark.udf.register("temp_udf", (i: Int) => i + 1)
      val ex = intercept[AnalysisException] {
        sql("CREATE VIEW general_catalog.default.v_tempfn AS " +
          "SELECT temp_udf(x) FROM spark_catalog.default.t")
      }
      assert(ex.getMessage.toLowerCase.contains("temporary"))
    }
  }

  test("CREATE VIEW rejects reference to a temporary view") {
    withTempView("tv") {
      spark.range(3).createOrReplaceTempView("tv")
      val ex = intercept[AnalysisException] {
        sql("CREATE VIEW general_catalog.default.v_tempview AS SELECT id FROM tv")
      }
      assert(ex.getMessage.toLowerCase.contains("temporary"))
    }
  }

  test("CREATE VIEW propagates DEFAULT COLLATION to TableInfo") {
    withTable("spark_catalog.default.t") {
      Seq("a", "b").toDF("col").write.saveAsTable("spark_catalog.default.t")
      sql("CREATE VIEW general_catalog.default.v_coll DEFAULT COLLATION UTF8_BINARY AS " +
        "SELECT col FROM spark_catalog.default.t")
      // TestingGeneralCatalog stores the TableInfo verbatim, so the collation property is
      // observable via the catalog-stored builder output.
      val catalog = spark.sessionState.catalogManager.catalog("general_catalog")
        .asInstanceOf[TestingGeneralCatalog]
      val info = catalog.getStoredView(Array("default"), "v_coll")
      assert(info.properties().get(TableCatalog.PROP_COLLATION) == "UTF8_BINARY")
    }
  }

  test("withCurrentCatalogAndNamespace clears the property when catalog is null or empty") {
    val infoNull = new TableInfo.Builder()
      .withSchema(new StructType().add("col", "string"))
      .withViewText("SELECT 1 AS col")
      .withCurrentCatalogAndNamespace("spark_catalog", Array("default"))
      .withCurrentCatalogAndNamespace(null, Array("ignored"))
      .build()
    assert(!infoNull.properties().containsKey(
      TableCatalog.PROP_VIEW_CURRENT_CATALOG_AND_NAMESPACE))

    val infoEmpty = new TableInfo.Builder()
      .withSchema(new StructType().add("col", "string"))
      .withViewText("SELECT 1 AS col")
      .withCurrentCatalogAndNamespace("spark_catalog", Array("default"))
      .withCurrentCatalogAndNamespace("", Array("ignored"))
      .build()
    assert(!infoEmpty.properties().containsKey(
      TableCatalog.PROP_VIEW_CURRENT_CATALOG_AND_NAMESPACE))
  }

  test("CREATE VIEW on a StagingTableCatalog uses the atomic exec") {
    withSQLConf(
      "spark.sql.catalog.staging_catalog" -> classOf[TestingStagingCatalog].getName) {
      withTable("spark_catalog.default.t") {
        Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")

        // Plain CREATE — exercises stageCreate.
        sql("CREATE VIEW staging_catalog.default.v_atomic AS " +
          "SELECT x FROM spark_catalog.default.t WHERE x > 1")
        checkAnswer(
          spark.table("staging_catalog.default.v_atomic"),
          Seq(Row(2), Row(3)))

        // Second CREATE without IF NOT EXISTS — should surface viewAlreadyExistsError
        // (TestingStagingCatalog's stageCreate throws TableAlreadyExistsException, which the
        // exec wraps).
        val ex = intercept[AnalysisException] {
          sql("CREATE VIEW staging_catalog.default.v_atomic AS " +
            "SELECT x FROM spark_catalog.default.t WHERE x > 1")
        }
        assert(ex.getMessage.toLowerCase.contains("already exists"))

        // CREATE OR REPLACE — exercises stageCreateOrReplace.
        sql("CREATE OR REPLACE VIEW staging_catalog.default.v_atomic AS " +
          "SELECT x FROM spark_catalog.default.t WHERE x > 2")
        checkAnswer(spark.table("staging_catalog.default.v_atomic"), Row(3))

        // CREATE IF NOT EXISTS on an existing view — no-op. After the PR reorders atomic to
        // validate first, this should still succeed (the body is valid); the earlier behavior
        // where a broken body was silently skipped no longer applies.
        sql("CREATE VIEW IF NOT EXISTS staging_catalog.default.v_atomic AS " +
          "SELECT x + 100 AS x FROM spark_catalog.default.t")
        // Value unchanged — IF NOT EXISTS was a no-op.
        checkAnswer(spark.table("staging_catalog.default.v_atomic"), Row(3))
      }
    }
  }
}

class TestingGeneralCatalog extends TableCatalog {

  // Holds views created via createTable within the session. Keyed by (namespace, name).
  private val createdViews =
    new java.util.concurrent.ConcurrentHashMap[(Seq[String], String), TableInfo]()

  override def capabilities(): java.util.Set[TableCatalogCapability] =
    java.util.Collections.singleton(TableCatalogCapability.SUPPORTS_CREATE_VIEW)

  override def loadTable(ident: Identifier): Table = {
    val key = (ident.namespace().toSeq, ident.name())
    Option(createdViews.get(key)).map(new MetadataOnlyTable(_)).getOrElse {
      ident.name() match {
        case "test_json" =>
          val info = new TableInfo.Builder()
            .withSchema(new StructType().add("col", "string"))
            .withProvider("json")
            .withLocation(ident.namespace().head)
            .withTableType(TableSummary.EXTERNAL_TABLE_TYPE)
            .build()
          new MetadataOnlyTable(info)
        case "test_partitioned_json" =>
          val partitioning = LogicalExpressions.identity(LogicalExpressions.reference(Seq("c2")))
          val info = new TableInfo.Builder()
            .withSchema(new StructType().add("c1", "int").add("c2", "int"))
            .withProvider("json")
            .withLocation(ident.namespace().head)
            .withTableType(TableSummary.EXTERNAL_TABLE_TYPE)
            .withPartitions(Array(partitioning))
            .build()
          new MetadataOnlyTable(info)
        case "test_v2" =>
          val info = new TableInfo.Builder()
            .withSchema(FakeV2Provider.schema)
            .withProvider(classOf[FakeV2Provider].getName)
            .build()
          new MetadataOnlyTable(info)
        case "test_view" =>
          val viewProps = new java.util.HashMap[String, String]()
          viewProps.put(
            TableCatalog.VIEW_CONF_PREFIX + SQLConf.ANSI_ENABLED.key,
            (ident.namespace().head == "ansi").toString)
          val info = new TableInfo.Builder()
            .withSchema(new StructType().add("col", "string").add("i", "int"))
            .withProperties(viewProps)
            .withViewText(
              "SELECT col, col::int AS i FROM spark_catalog.default.t WHERE col = 'b'")
            .build()
          new MetadataOnlyTable(info)
        case "test_unqualified_view" =>
          val info = new TableInfo.Builder()
            .withSchema(new StructType().add("col", "string"))
            .withViewText("SELECT col FROM t WHERE col = 'b'")
            .withCurrentCatalogAndNamespace("spark_catalog", Array("default"))
            .build()
          new MetadataOnlyTable(info)
        case _ => throw new NoSuchTableException(ident)
      }
    }
  }

  override def tableExists(ident: Identifier): Boolean = {
    val key = (ident.namespace().toSeq, ident.name())
    createdViews.containsKey(key) || super.tableExists(ident)
  }

  override def createTable(ident: Identifier, info: TableInfo): Table = {
    val key = (ident.namespace().toSeq, ident.name())
    if (createdViews.putIfAbsent(key, info) != null) {
      throw new TableAlreadyExistsException(ident)
    }
    new MetadataOnlyTable(info)
  }

  /** Test-only accessor: returns the stored TableInfo for a created view. */
  def getStoredView(namespace: Array[String], name: String): TableInfo = {
    Option(createdViews.get((namespace.toSeq, name))).getOrElse {
      throw new NoSuchTableException(Identifier.of(namespace, name))
    }
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new RuntimeException("shouldn't be called")
  }
  override def dropTable(ident: Identifier): Boolean = {
    val key = (ident.namespace().toSeq, ident.name())
    createdViews.remove(key) != null
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

/**
 * A minimal [[StagingTableCatalog]] used to drive `AtomicCreateV2ViewExec`. Views are stored
 * in a local map; staging commits write through, aborts discard. Supports SUPPORTS_CREATE_VIEW.
 */
class TestingStagingCatalog extends StagingTableCatalog {

  private val views =
    new java.util.concurrent.ConcurrentHashMap[(Seq[String], String), TableInfo]()

  override def capabilities(): java.util.Set[TableCatalogCapability] =
    java.util.Collections.singleton(TableCatalogCapability.SUPPORTS_CREATE_VIEW)

  private def keyOf(ident: Identifier): (Seq[String], String) =
    (ident.namespace().toSeq, ident.name())

  override def loadTable(ident: Identifier): Table = {
    Option(views.get(keyOf(ident))).map(new MetadataOnlyTable(_))
      .getOrElse(throw new NoSuchTableException(ident))
  }

  override def tableExists(ident: Identifier): Boolean = views.containsKey(keyOf(ident))

  override def createTable(ident: Identifier, info: TableInfo): Table = {
    if (views.putIfAbsent(keyOf(ident), info) != null) {
      throw new TableAlreadyExistsException(ident)
    }
    new MetadataOnlyTable(info)
  }

  override def stageCreate(ident: Identifier, info: TableInfo): StagedTable = {
    if (views.containsKey(keyOf(ident))) throw new TableAlreadyExistsException(ident)
    new RecordingStagedTable(info, () => views.put(keyOf(ident), info), () => ())
  }

  override def stageReplace(ident: Identifier, info: TableInfo): StagedTable = {
    if (!views.containsKey(keyOf(ident))) throw new NoSuchTableException(ident)
    new RecordingStagedTable(info, () => views.put(keyOf(ident), info), () => ())
  }

  override def stageCreateOrReplace(ident: Identifier, info: TableInfo): StagedTable = {
    new RecordingStagedTable(info, () => views.put(keyOf(ident), info), () => ())
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    throw new RuntimeException("shouldn't be called")
  override def dropTable(ident: Identifier): Boolean = views.remove(keyOf(ident)) != null
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    throw new RuntimeException("shouldn't be called")
  override def listTables(namespace: Array[String]): Array[Identifier] = Array.empty

  private var catalogName = ""
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
  }
  override def name(): String = catalogName
}

private class RecordingStagedTable(
    info: TableInfo,
    onCommit: () => Unit,
    onAbort: () => Unit) extends MetadataOnlyTable(info) with StagedTable {
  override def commitStagedChanges(): Unit = onCommit()
  override def abortStagedChanges(): Unit = onAbort()
}

/** A v2 catalog that does not declare SUPPORTS_CREATE_VIEW. Used to exercise the capability
  * gate in `DataSourceV2Strategy`. */
class TestingTableOnlyCatalog extends TableCatalog {
  override def loadTable(ident: Identifier): Table = throw new NoSuchTableException(ident)
  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    throw new RuntimeException("shouldn't be called")
  override def dropTable(ident: Identifier): Boolean = false
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    throw new RuntimeException("shouldn't be called")
  override def listTables(namespace: Array[String]): Array[Identifier] = Array.empty
  private var catalogName = ""
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
  }
  override def name(): String = catalogName
}
