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
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog.{Identifier, MetadataOnlyTable, StagedTable, StagingTableCatalog, Table, TableCatalog, TableCatalogCapability, TableChange, TableInfo, TableSummary, V1Table}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Tests for the view side of [[MetadataOnlyTable]]: view-text expansion on read, and
 * CREATE VIEW / ALTER VIEW ... AS going through the v2 write path
 * (`CreateV2ViewExec` / `AlterV2ViewExec` and their atomic staging variants).
 * Data-source-table read paths live in
 * [[org.apache.spark.sql.connector.DataSourceV2MetadataOnlyTableSuite]].
 *
 * TODO: once the remaining v2 view DDL is implemented (SET/UNSET TBLPROPERTIES, SHOW CREATE
 * VIEW, RENAME TO, SCHEMA BINDING, DESCRIBE / SHOW TBLPROPERTIES on v2 views), register a
 * `MetadataOnlyTable`-backed `DelegatingCatalogExtension` as `spark.sql.catalog.spark_catalog`
 * and run the shared [[org.apache.spark.sql.execution.PersistedViewTestSuite]] body against
 * the v2 path for full parity with the v1 persisted-view coverage.
 */
class DataSourceV2MetadataOnlyViewSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.view_catalog", classOf[TestingViewCatalog].getName)

  // --- View read path -----------------------------------------------------

  test("read view expands SQL text and applies captured SQL configs") {
    withTable("spark_catalog.default.t") {
      Seq("a", "b").toDF("col").write.saveAsTable("spark_catalog.default.t")
      // view_catalog.ansi.test_view stores view.sqlConfig.spark.sql.ansi.enabled=true;
      // view_catalog.non_ansi.test_view stores it =false. The view body does
      // `col::int` which errors in ANSI mode and yields NULL in non-ANSI mode.
      intercept[Exception](spark.table("view_catalog.ansi.test_view").collect())
      checkAnswer(spark.table("view_catalog.non_ansi.test_view"), Row("b", null))
    }
  }

  test("read view resolves unqualified refs via captured current catalog/namespace") {
    withTable("spark_catalog.default.t") {
      Seq("a", "b").toDF("col").write.saveAsTable("spark_catalog.default.t")
      // View text uses the unqualified name `t`; it resolves via the stored
      // current catalog / namespace properties.
      checkAnswer(spark.table("view_catalog.ns.test_unqualified_view"), Row("b"))
    }
  }

  // --- TableInfo.Builder unit tests for view-specific properties ----------

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

  test("multi-part captured namespace round-trips through V1Table.toCatalogTable") {
    // End-to-end coverage of the v2 encoder -> parser round-trip for multi-level namespaces:
    // (a) TableInfo.Builder serializes (cat, Array(db1, db2)) into a quoted multi-part
    // identifier, (b) V1Table.toCatalogTable parses it back via parseMultipartIdentifier, and
    // (c) the resulting CatalogTable exposes the full (cat, db1, db2) via
    // viewCatalogAndNamespace -- which is what the v1 view-resolution path consumes to expand
    // unqualified references in the view body.
    val info = new TableInfo.Builder()
      .withSchema(new StructType().add("col", "string"))
      .withViewText("SELECT col FROM t")
      .withCurrentCatalogAndNamespace("my_cat", Array("db1", "db2"))
      .build()
    val motTable = new MetadataOnlyTable(info)
    // Any CatalogPlugin works here; toCatalogTable only reads `catalog.name()`.
    val catalog = spark.sessionState.catalogManager.catalog("view_catalog")
    val ct = V1Table.toCatalogTable(
      catalog, Identifier.of(Array("ns"), "v"), motTable)
    assert(ct.viewCatalogAndNamespace == Seq("my_cat", "db1", "db2"))

    // And for a namespace part that needs backtick-quoting.
    val infoWeird = new TableInfo.Builder()
      .withSchema(new StructType().add("col", "string"))
      .withViewText("SELECT col FROM t")
      .withCurrentCatalogAndNamespace("my_cat", Array("weird.db", "normal"))
      .build()
    val ctWeird = V1Table.toCatalogTable(
      catalog, Identifier.of(Array("ns"), "v"), new MetadataOnlyTable(infoWeird))
    assert(ctWeird.viewCatalogAndNamespace == Seq("my_cat", "weird.db", "normal"))
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

  // --- CREATE VIEW on a plain TableCatalog --------------------------------

  test("CREATE VIEW on a v2 catalog") {
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
      sql("CREATE VIEW view_catalog.default.my_view AS " +
        "SELECT x FROM spark_catalog.default.t WHERE x > 1")
      checkAnswer(spark.table("view_catalog.default.my_view"), Seq(Row(2), Row(3)))
    }
  }

  test("CREATE VIEW IF NOT EXISTS is a no-op when the view exists") {
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
      sql("CREATE VIEW view_catalog.default.v_ifne AS " +
        "SELECT x FROM spark_catalog.default.t")
      // Re-running with IF NOT EXISTS should not fail and should not change the view.
      sql("CREATE VIEW IF NOT EXISTS view_catalog.default.v_ifne AS " +
        "SELECT x + 100 AS x FROM spark_catalog.default.t")
      checkAnswer(spark.table("view_catalog.default.v_ifne"),
        Seq(Row(1), Row(2), Row(3)))
    }
  }

  test("CREATE VIEW without IF NOT EXISTS fails when the view exists") {
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
      sql("CREATE VIEW view_catalog.default.v_dup AS " +
        "SELECT x FROM spark_catalog.default.t")
      intercept[AnalysisException] {
        sql("CREATE VIEW view_catalog.default.v_dup AS " +
          "SELECT x FROM spark_catalog.default.t")
      }
    }
  }

  test("CREATE OR REPLACE VIEW replaces an existing view") {
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
      sql("CREATE VIEW view_catalog.default.v_replace AS " +
        "SELECT x FROM spark_catalog.default.t WHERE x > 10")
      checkAnswer(spark.table("view_catalog.default.v_replace"), Seq.empty[Row])
      sql("CREATE OR REPLACE VIEW view_catalog.default.v_replace AS " +
        "SELECT x FROM spark_catalog.default.t WHERE x > 1")
      checkAnswer(spark.table("view_catalog.default.v_replace"), Seq(Row(2), Row(3)))
    }
  }

  test("CREATE VIEW on a catalog without SUPPORTS_VIEW fails") {
    withSQLConf(
      "spark.sql.catalog.no_view_catalog" -> classOf[TestingTableOnlyCatalog].getName) {
      val ex = intercept[AnalysisException] {
        sql("CREATE VIEW no_view_catalog.default.v AS SELECT 1")
      }
      assert(ex.getCondition == "MISSING_CATALOG_ABILITY.VIEWS")
    }
  }

  test("CREATE VIEW rejects too-few / too-many user-specified columns") {
    withTable("spark_catalog.default.t") {
      Seq(1 -> 10).toDF("x", "y").write.saveAsTable("spark_catalog.default.t")
      intercept[AnalysisException] {
        sql("CREATE VIEW view_catalog.default.v_few (a) AS " +
          "SELECT x, y FROM spark_catalog.default.t")
      }
      intercept[AnalysisException] {
        sql("CREATE VIEW view_catalog.default.v_many (a, b, c) AS " +
          "SELECT x, y FROM spark_catalog.default.t")
      }
    }
  }

  test("CREATE VIEW rejects reference to a temporary function") {
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
      spark.udf.register("temp_udf", (i: Int) => i + 1)
      val ex = intercept[AnalysisException] {
        sql("CREATE VIEW view_catalog.default.v_tempfn AS " +
          "SELECT temp_udf(x) FROM spark_catalog.default.t")
      }
      assert(ex.getMessage.toLowerCase(java.util.Locale.ROOT).contains("temporary"))
    }
  }

  test("CREATE VIEW rejects reference to a temporary view") {
    withTempView("tv") {
      spark.range(3).createOrReplaceTempView("tv")
      val ex = intercept[AnalysisException] {
        sql("CREATE VIEW view_catalog.default.v_tempview AS SELECT id FROM tv")
      }
      assert(ex.getMessage.toLowerCase(java.util.Locale.ROOT).contains("temporary"))
    }
  }

  test("CREATE VIEW rejects reference to a temporary variable") {
    withSessionVariable("temp_var") {
      sql("DECLARE VARIABLE temp_var INT DEFAULT 1")
      val ex = intercept[AnalysisException] {
        sql("CREATE VIEW view_catalog.default.v_tempvar AS SELECT temp_var AS x")
      }
      assert(ex.getMessage.toLowerCase(java.util.Locale.ROOT).contains("temporary"))
    }
  }

  test("CREATE VIEW propagates DEFAULT COLLATION to TableInfo") {
    withTable("spark_catalog.default.t") {
      Seq("a", "b").toDF("col").write.saveAsTable("spark_catalog.default.t")
      sql("CREATE VIEW view_catalog.default.v_coll DEFAULT COLLATION UTF8_BINARY AS " +
        "SELECT col FROM spark_catalog.default.t")
      // TestingViewCatalog stores the TableInfo verbatim, so the collation property is
      // observable via the catalog-stored builder output.
      val catalog = spark.sessionState.catalogManager.catalog("view_catalog")
        .asInstanceOf[TestingViewCatalog]
      val info = catalog.getStoredView(Array("default"), "v_coll")
      assert(info.properties().get(TableCatalog.PROP_COLLATION) == "UTF8_BINARY")
    }
  }

  test("CREATE OR REPLACE VIEW detects cyclic view references") {
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
      sql("CREATE VIEW view_catalog.default.v_cycle_a AS " +
        "SELECT x FROM spark_catalog.default.t")
      sql("CREATE VIEW view_catalog.default.v_cycle_b AS " +
        "SELECT x FROM view_catalog.default.v_cycle_a")
      val ex = intercept[AnalysisException] {
        sql("CREATE OR REPLACE VIEW view_catalog.default.v_cycle_a AS " +
          "SELECT x FROM view_catalog.default.v_cycle_b")
      }
      assert(ex.getCondition == "RECURSIVE_VIEW")
    }
  }

  test("CREATE VIEW over a non-view table entry is rejected (plain TableCatalog)") {
    val catalog = spark.sessionState.catalogManager.catalog("view_catalog")
      .asInstanceOf[TestingViewCatalog]
    val tableIdent = Identifier.of(Array("default"), "v_existing_table")
    val tableInfo = new TableInfo.Builder()
      .withSchema(new StructType().add("col", "string"))
      .withTableType(TableSummary.EXTERNAL_TABLE_TYPE)
      .build()
    catalog.createTable(tableIdent, tableInfo)
    try {
      withTable("spark_catalog.default.t") {
        Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")

        // CREATE OR REPLACE VIEW must not silently destroy a non-view table -- v1 parity.
        val replaceEx = intercept[AnalysisException] {
          sql("CREATE OR REPLACE VIEW view_catalog.default.v_existing_table AS " +
            "SELECT x FROM spark_catalog.default.t")
        }
        assert(replaceEx.getCondition == "EXPECT_VIEW_NOT_TABLE.NO_ALTERNATIVE")

        // Plain CREATE VIEW over a table surfaces TABLE_OR_VIEW_ALREADY_EXISTS, matching v1.
        val createEx = intercept[AnalysisException] {
          sql("CREATE VIEW view_catalog.default.v_existing_table AS " +
            "SELECT x FROM spark_catalog.default.t")
        }
        assert(createEx.getCondition == "TABLE_OR_VIEW_ALREADY_EXISTS")

        // CREATE VIEW IF NOT EXISTS is a no-op -- the table entry is untouched.
        sql("CREATE VIEW IF NOT EXISTS view_catalog.default.v_existing_table AS " +
          "SELECT x FROM spark_catalog.default.t")
        val stored = catalog.getStoredView(Array("default"), "v_existing_table")
        assert(stored.properties().get(TableCatalog.PROP_TABLE_TYPE) ==
          TableSummary.EXTERNAL_TABLE_TYPE)
      }
    } finally {
      catalog.dropTable(tableIdent)
    }
  }

  // --- CREATE VIEW on a StagingTableCatalog -------------------------------

  test("CREATE VIEW on a StagingTableCatalog uses the atomic exec") {
    withSQLConf(
      "spark.sql.catalog.staging_catalog" -> classOf[TestingStagingCatalog].getName) {
      withTable("spark_catalog.default.t") {
        Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")

        // Plain CREATE -- exercises stageCreate.
        sql("CREATE VIEW staging_catalog.default.v_atomic AS " +
          "SELECT x FROM spark_catalog.default.t WHERE x > 1")
        checkAnswer(
          spark.table("staging_catalog.default.v_atomic"),
          Seq(Row(2), Row(3)))

        // Second CREATE without IF NOT EXISTS -- should surface viewAlreadyExistsError
        // (TestingStagingCatalog's stageCreate throws TableAlreadyExistsException, which the
        // exec wraps).
        val ex = intercept[AnalysisException] {
          sql("CREATE VIEW staging_catalog.default.v_atomic AS " +
            "SELECT x FROM spark_catalog.default.t WHERE x > 1")
        }
        assert(ex.getMessage.toLowerCase(java.util.Locale.ROOT).contains("already exists"))

        // CREATE OR REPLACE -- exercises stageCreateOrReplace.
        sql("CREATE OR REPLACE VIEW staging_catalog.default.v_atomic AS " +
          "SELECT x FROM spark_catalog.default.t WHERE x > 2")
        checkAnswer(spark.table("staging_catalog.default.v_atomic"), Row(3))

        // CREATE IF NOT EXISTS on an existing view -- no-op, but the body is still validated
        // first (the atomic exec builds the TableInfo before the allow-existing short-circuit),
        // so a malformed body is rejected even when creation is skipped.
        sql("CREATE VIEW IF NOT EXISTS staging_catalog.default.v_atomic AS " +
          "SELECT x + 100 AS x FROM spark_catalog.default.t")
        // Value unchanged -- IF NOT EXISTS was a no-op.
        checkAnswer(spark.table("staging_catalog.default.v_atomic"), Row(3))
      }
    }
  }

  test("CREATE VIEW over a non-view table entry is rejected (StagingTableCatalog)") {
    withSQLConf(
      "spark.sql.catalog.staging_catalog" -> classOf[TestingStagingCatalog].getName) {
      val stagingCatalog = spark.sessionState.catalogManager.catalog("staging_catalog")
        .asInstanceOf[TestingStagingCatalog]
      val tableIdent = Identifier.of(Array("default"), "v_existing_table")
      val tableInfo = new TableInfo.Builder()
        .withSchema(new StructType().add("col", "string"))
        .withTableType(TableSummary.EXTERNAL_TABLE_TYPE)
        .build()
      stagingCatalog.createTable(tableIdent, tableInfo)
      try {
        withTable("spark_catalog.default.t") {
          Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")

          // CREATE OR REPLACE VIEW must not silently destroy a non-view table. On a staging
          // catalog this specifically guards against `stageCreateOrReplace` committing over
          // the table.
          val replaceEx = intercept[AnalysisException] {
            sql("CREATE OR REPLACE VIEW staging_catalog.default.v_existing_table AS " +
              "SELECT x FROM spark_catalog.default.t")
          }
          assert(replaceEx.getCondition == "EXPECT_VIEW_NOT_TABLE.NO_ALTERNATIVE")

          val createEx = intercept[AnalysisException] {
            sql("CREATE VIEW staging_catalog.default.v_existing_table AS " +
              "SELECT x FROM spark_catalog.default.t")
          }
          assert(createEx.getCondition == "TABLE_OR_VIEW_ALREADY_EXISTS")

          sql("CREATE VIEW IF NOT EXISTS staging_catalog.default.v_existing_table AS " +
            "SELECT x FROM spark_catalog.default.t")
          val loaded = stagingCatalog.loadTable(tableIdent).asInstanceOf[MetadataOnlyTable]
          assert(loaded.getTableInfo.properties.get(TableCatalog.PROP_TABLE_TYPE) ==
            TableSummary.EXTERNAL_TABLE_TYPE)
        }
      } finally {
        stagingCatalog.dropTable(tableIdent)
      }
    }
  }

  // --- ALTER VIEW ---------------------------------------------------------

  test("ALTER VIEW ... AS updates the view body on a v2 catalog") {
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
      sql("CREATE VIEW view_catalog.default.v_alter AS " +
        "SELECT x FROM spark_catalog.default.t WHERE x > 10")
      checkAnswer(spark.table("view_catalog.default.v_alter"), Seq.empty[Row])

      sql("ALTER VIEW view_catalog.default.v_alter AS " +
        "SELECT x FROM spark_catalog.default.t WHERE x > 1")
      checkAnswer(spark.table("view_catalog.default.v_alter"), Seq(Row(2), Row(3)))
    }
  }

  test("ALTER VIEW on a missing view fails at analysis") {
    // UnresolvedView resolves through lookupTableOrView and the missing view surfaces as an
    // AnalysisException before we ever reach the v2 exec. The exact error condition (e.g.
    // TABLE_OR_VIEW_NOT_FOUND) varies across Spark versions; we just assert we fail cleanly.
    intercept[AnalysisException] {
      sql("ALTER VIEW view_catalog.default.does_not_exist AS SELECT 1 AS x")
    }
  }

  test("ALTER VIEW rejects reference to a temporary function") {
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
      sql("CREATE VIEW view_catalog.default.v_alter_tempfn AS " +
        "SELECT x FROM spark_catalog.default.t")
      spark.udf.register("temp_udf_alter", (i: Int) => i + 1)
      val ex = intercept[AnalysisException] {
        sql("ALTER VIEW view_catalog.default.v_alter_tempfn AS " +
          "SELECT temp_udf_alter(x) FROM spark_catalog.default.t")
      }
      assert(ex.getMessage.toLowerCase(java.util.Locale.ROOT).contains("temporary"))
    }
  }

  test("ALTER VIEW preserves user-set TBLPROPERTIES") {
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
      sql("CREATE VIEW view_catalog.default.v_preserve " +
        "TBLPROPERTIES ('mykey'='myvalue') AS " +
        "SELECT x FROM spark_catalog.default.t")
      sql("ALTER VIEW view_catalog.default.v_preserve AS " +
        "SELECT x + 1 AS x FROM spark_catalog.default.t")

      val catalog = spark.sessionState.catalogManager.catalog("view_catalog")
        .asInstanceOf[TestingViewCatalog]
      val info = catalog.getStoredView(Array("default"), "v_preserve")
      assert(info.properties().get("mykey") == "myvalue")
    }
  }

  test("ALTER VIEW preserves PROP_OWNER (v1-parity)") {
    val catalog = spark.sessionState.catalogManager.catalog("view_catalog")
      .asInstanceOf[TestingViewCatalog]
    val viewIdent = Identifier.of(Array("default"), "v_owner")
    // Pre-seed a view whose stored TableInfo carries an explicit owner.
    val initialInfo = new TableInfo.Builder()
      .withSchema(new StructType().add("x", "int"))
      .withViewText("SELECT 1 AS x")
      .withOwner("alice")
      .withCurrentCatalogAndNamespace("spark_catalog", Array("default"))
      .build()
    catalog.createTable(viewIdent, initialInfo)
    try {
      withTable("spark_catalog.default.t") {
        Seq(2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
        sql("ALTER VIEW view_catalog.default.v_owner AS " +
          "SELECT x FROM spark_catalog.default.t")
        // v1 ALTER VIEW AS carries `owner` forward via `viewMeta.copy(...)`. v2 must match:
        // the stored TableInfo after the ALTER should still have the original owner.
        val info = catalog.getStoredView(Array("default"), "v_owner")
        assert(info.properties().get(TableCatalog.PROP_OWNER) == "alice")
      }
    } finally {
      catalog.dropTable(viewIdent)
    }
  }

  test("ALTER VIEW preserves SCHEMA EVOLUTION binding mode") {
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
      sql("CREATE VIEW view_catalog.default.v_evo WITH SCHEMA EVOLUTION AS " +
        "SELECT x FROM spark_catalog.default.t")
      sql("ALTER VIEW view_catalog.default.v_evo AS " +
        "SELECT x + 1 AS x FROM spark_catalog.default.t")

      val catalog = spark.sessionState.catalogManager.catalog("view_catalog")
        .asInstanceOf[TestingViewCatalog]
      val info = catalog.getStoredView(Array("default"), "v_evo")
      // Use the same stored key v1 uses (CatalogTable.VIEW_SCHEMA_MODE = "view.schemaMode").
      assert(info.properties().get("view.schemaMode") == "EVOLUTION")
    }
  }

  test("ALTER VIEW re-captures the current session's SQL configs") {
    withTable("spark_catalog.default.t") {
      Seq("a", "b").toDF("col").write.saveAsTable("spark_catalog.default.t")
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        sql("CREATE VIEW view_catalog.default.v_configs AS " +
          "SELECT col FROM spark_catalog.default.t")
      }
      val catalog = spark.sessionState.catalogManager.catalog("view_catalog")
        .asInstanceOf[TestingViewCatalog]
      val ansiKey = TableCatalog.VIEW_CONF_PREFIX + SQLConf.ANSI_ENABLED.key
      assert(catalog.getStoredView(Array("default"), "v_configs").properties().get(ansiKey)
        == "true")

      // ALTER under a different ANSI setting should replace the stored config, not merge.
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        sql("ALTER VIEW view_catalog.default.v_configs AS " +
          "SELECT col FROM spark_catalog.default.t WHERE col = 'b'")
      }
      assert(catalog.getStoredView(Array("default"), "v_configs").properties().get(ansiKey)
        == "false")
    }
  }

  test("CREATE OR REPLACE VIEW whose new body references a nonexistent table fails at " +
    "analysis") {
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
      sql("CREATE VIEW view_catalog.default.v_replace_missing AS " +
        "SELECT x FROM spark_catalog.default.t")
      val ex = intercept[AnalysisException] {
        sql("CREATE OR REPLACE VIEW view_catalog.default.v_replace_missing AS " +
          "SELECT * FROM spark_catalog.default.does_not_exist")
      }
      assert(ex.getCondition == "TABLE_OR_VIEW_NOT_FOUND")
    }
  }

  test("ALTER VIEW on a StagingTableCatalog uses the atomic exec (stageReplace)") {
    withSQLConf(
      "spark.sql.catalog.staging_catalog" -> classOf[TestingStagingCatalog].getName) {
      withTable("spark_catalog.default.t") {
        Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
        sql("CREATE VIEW staging_catalog.default.v_atomic_alter AS " +
          "SELECT x FROM spark_catalog.default.t WHERE x > 10")
        checkAnswer(spark.table("staging_catalog.default.v_atomic_alter"), Seq.empty[Row])

        sql("ALTER VIEW staging_catalog.default.v_atomic_alter AS " +
          "SELECT x FROM spark_catalog.default.t WHERE x > 1")
        checkAnswer(
          spark.table("staging_catalog.default.v_atomic_alter"),
          Seq(Row(2), Row(3)))
      }
    }
  }

  test("ALTER VIEW on a catalog without SUPPORTS_VIEW fails with MISSING_CATALOG_ABILITY") {
    // TestingTableOnlyCatalog does NOT declare SUPPORTS_VIEW but DOES round-trip
    // `default.v` as a view-typed MetadataOnlyTable, so view resolution succeeds and we
    // reach the capability gate in `CheckViewReferences`. Verifies the gate fires on the
    // ALTER path (not only on CREATE), which would otherwise silently regress if
    // `SUPPORTS_VIEW` got added to the default capability set.
    withSQLConf(
      "spark.sql.catalog.no_view_catalog" -> classOf[TestingTableOnlyCatalog].getName) {
      val ex = intercept[AnalysisException] {
        sql("ALTER VIEW no_view_catalog.default.v AS SELECT 1 AS x")
      }
      assert(ex.getCondition == "MISSING_CATALOG_ABILITY.VIEWS")
    }
  }

  test("cyclic detection distinguishes views across multi-level namespaces") {
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")

      // Two views whose last namespace segment collides (`inner`) but whose full multi-part
      // identifiers differ. Before the `fullIdent` change both collapsed to
      // `TableIdentifier(v, Some("inner"), Some("view_catalog"))` and cyclic detection would
      // false-positive on a legitimate cross-namespace REPLACE.
      sql("CREATE VIEW view_catalog.ns1.inner.v AS SELECT x FROM spark_catalog.default.t")
      sql("CREATE VIEW view_catalog.ns2.inner.v AS " +
        "SELECT x FROM view_catalog.ns1.inner.v")
      // Legitimate non-cyclic REPLACE -- new body references a different view that happens to
      // share the last namespace segment. Must not false-positive.
      sql("CREATE OR REPLACE VIEW view_catalog.ns1.inner.v AS " +
        "SELECT x FROM spark_catalog.default.t WHERE x > 1")
      checkAnswer(spark.table("view_catalog.ns1.inner.v"), Seq(Row(2), Row(3)))

      // Real cycle across the two namespaces must still be caught.
      val ex = intercept[AnalysisException] {
        sql("CREATE OR REPLACE VIEW view_catalog.ns1.inner.v AS " +
          "SELECT x FROM view_catalog.ns2.inner.v")
      }
      assert(ex.getCondition == "RECURSIVE_VIEW")
    }
  }

  test("ALTER VIEW cyclic detection distinguishes views across multi-level namespaces") {
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")

      sql("CREATE VIEW view_catalog.ns1.inner.v_alter AS " +
        "SELECT x FROM spark_catalog.default.t")
      sql("CREATE VIEW view_catalog.ns2.inner.v_alter AS " +
        "SELECT x FROM view_catalog.ns1.inner.v_alter")

      // Legitimate non-cyclic ALTER -- new body does not reference the altered view. Before
      // `fullIdent` this false-positived because the two views collapsed to the same
      // TableIdentifier(v_alter, Some("inner"), Some("view_catalog")).
      sql("ALTER VIEW view_catalog.ns1.inner.v_alter AS " +
        "SELECT x FROM spark_catalog.default.t WHERE x > 1")
      checkAnswer(
        spark.table("view_catalog.ns1.inner.v_alter"),
        Seq(Row(2), Row(3)))

      // Real cycle across the two namespaces must still be caught.
      val ex = intercept[AnalysisException] {
        sql("ALTER VIEW view_catalog.ns1.inner.v_alter AS " +
          "SELECT x FROM view_catalog.ns2.inner.v_alter")
      }
      assert(ex.getCondition == "RECURSIVE_VIEW")
    }
  }

  // --- Follow-up-blocked view DDL / inspection on a non-session v2 catalog ------------
  // These plans don't have a dedicated v2 strategy yet (tracked for a follow-up PR). We pin
  // the current failure mode -- UNSUPPORTED_FEATURE.TABLE_OPERATION with a statement-specific
  // operation string -- so a future generic "no plan found" regression would surface here
  // rather than silently degrading the UX.

  private def seedV2View(name: String): Unit = {
    sql(s"CREATE VIEW view_catalog.default.$name AS SELECT 1 AS x")
  }

  private def assertUnsupportedViewOp(statement: String): Unit = {
    val ex = intercept[AnalysisException](sql(statement))
    assert(ex.getCondition == "UNSUPPORTED_FEATURE.TABLE_OPERATION", s"got ${ex.getCondition}")
  }

  test("ALTER VIEW ... SET TBLPROPERTIES on a v2 view is rejected") {
    seedV2View("v_set_props")
    assertUnsupportedViewOp(
      "ALTER VIEW view_catalog.default.v_set_props SET TBLPROPERTIES ('k' = 'v')")
  }

  test("ALTER VIEW ... UNSET TBLPROPERTIES on a v2 view is rejected") {
    seedV2View("v_unset_props")
    assertUnsupportedViewOp(
      "ALTER VIEW view_catalog.default.v_unset_props UNSET TBLPROPERTIES ('k')")
  }

  test("ALTER VIEW ... WITH SCHEMA on a v2 view is rejected") {
    seedV2View("v_schema_binding")
    assertUnsupportedViewOp(
      "ALTER VIEW view_catalog.default.v_schema_binding WITH SCHEMA EVOLUTION")
  }

  test("ALTER VIEW ... RENAME TO on a v2 view is rejected") {
    seedV2View("v_rename")
    assertUnsupportedViewOp(
      "ALTER VIEW view_catalog.default.v_rename RENAME TO view_catalog.default.v_renamed")
  }

  test("SHOW CREATE TABLE on a v2 view is rejected") {
    seedV2View("v_show_create")
    assertUnsupportedViewOp("SHOW CREATE TABLE view_catalog.default.v_show_create")
  }

  test("SHOW TBLPROPERTIES on a v2 view is rejected") {
    seedV2View("v_show_props")
    assertUnsupportedViewOp("SHOW TBLPROPERTIES view_catalog.default.v_show_props")
  }

  test("SHOW COLUMNS on a v2 view is rejected") {
    seedV2View("v_show_cols")
    assertUnsupportedViewOp("SHOW COLUMNS IN view_catalog.default.v_show_cols")
  }

  test("DESCRIBE TABLE on a v2 view is rejected") {
    seedV2View("v_describe")
    assertUnsupportedViewOp("DESCRIBE TABLE view_catalog.default.v_describe")
  }

  test("DESCRIBE TABLE ... COLUMN on a v2 view is rejected") {
    seedV2View("v_describe_col")
    // Column resolution against a v2 view's output isn't wired up yet, so the analyzer fails
    // with UNRESOLVED_COLUMN before reaching the planner. That's still a clean
    // AnalysisException (not a generic "no plan found"), which is the pin we care about.
    intercept[AnalysisException](
      sql("DESCRIBE TABLE view_catalog.default.v_describe_col x"))
  }

  // --- SHOW TABLES / SHOW VIEWS on a v2 catalog --------------------------------

  private def seedV2Table(name: String): Unit = {
    val catalog = spark.sessionState.catalogManager.catalog("view_catalog")
      .asInstanceOf[TestingViewCatalog]
    catalog.createTable(
      Identifier.of(Array("default"), name),
      new TableInfo.Builder()
        .withSchema(new StructType().add("x", "int"))
        .withTableType(TableSummary.EXTERNAL_TABLE_TYPE)
        .build())
  }

  test("SHOW TABLES on a v2 catalog includes views (v1 parity)") {
    // v1 SHOW TABLES returns both tables and views; the `isTemporary` column distinguishes
    // temp views from everything else. v2 catalogs have no temp views, so `isTemporary` is
    // always false -- tables and permanent views are indistinguishable at the row level, but
    // both must appear (callers that want only tables should use listTableSummaries and
    // filter).
    seedV2View("v_in_show_tables")
    seedV2Table("t_in_show_tables")
    val rows = sql("SHOW TABLES IN view_catalog.default").collect()
    val names = rows.map(_.getString(1)).toSet
    assert(names.contains("v_in_show_tables"), s"view missing from SHOW TABLES: $names")
    assert(names.contains("t_in_show_tables"), s"table missing from SHOW TABLES: $names")
    rows.foreach(r => assert(!r.getBoolean(2), s"isTemporary must be false: $r"))
  }

  test("SHOW VIEWS on a v2 catalog returns only views") {
    seedV2View("v_in_show_views")
    seedV2Table("t_not_in_show_views")
    val rows = sql("SHOW VIEWS IN view_catalog.default").collect()
    val names = rows.map(_.getString(1)).toSet
    assert(names.contains("v_in_show_views"), s"view missing: $names")
    assert(!names.contains("t_not_in_show_views"),
      s"non-view leaked into SHOW VIEWS: $names")
    rows.foreach(r => assert(!r.getBoolean(2), s"isTemporary must be false for v2: $r"))
  }

  test("SHOW VIEWS with LIKE pattern filters on the view name") {
    seedV2View("v_foo")
    seedV2View("v_bar")
    val rows = sql("SHOW VIEWS IN view_catalog.default LIKE 'v_foo'").collect()
    val names = rows.map(_.getString(1)).toSet
    assert(names == Set("v_foo"), s"expected only v_foo, got $names")
  }

  test("SHOW VIEWS on a catalog without SUPPORTS_VIEW is rejected") {
    withSQLConf(
      "spark.sql.catalog.no_view_catalog" -> classOf[TestingTableOnlyCatalog].getName) {
      val ex = intercept[AnalysisException] {
        sql("SHOW VIEWS IN no_view_catalog.default")
      }
      assert(ex.getCondition == "MISSING_CATALOG_ABILITY.VIEWS")
    }
  }

  test("ALTER VIEW detects cyclic view references") {
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
      sql("CREATE VIEW view_catalog.default.v_alter_cycle_a AS " +
        "SELECT x FROM spark_catalog.default.t")
      sql("CREATE VIEW view_catalog.default.v_alter_cycle_b AS " +
        "SELECT x FROM view_catalog.default.v_alter_cycle_a")
      val ex = intercept[AnalysisException] {
        sql("ALTER VIEW view_catalog.default.v_alter_cycle_a AS " +
          "SELECT x FROM view_catalog.default.v_alter_cycle_b")
      }
      assert(ex.getCondition == "RECURSIVE_VIEW")
    }
  }
}

/**
 * A [[TableCatalog]] that supports SUPPORTS_VIEW: round-trips [[MetadataOnlyTable]] for created
 * views and tables (via `createTable` / `dropTable` / `tableExists` / `listTables`) and exposes
 * two canned read-only fixtures (`test_view`, `test_unqualified_view`) used by the view-read
 * tests. Entries created via `createTable` can be either tables or views -- their
 * [[TableCatalog#PROP_TABLE_TYPE]] property is what distinguishes them.
 */
class TestingViewCatalog extends TableCatalog {

  // Holds entries (views and tables) created via createTable within the session. Keyed by
  // (namespace, name); PROP_TABLE_TYPE in the stored TableInfo distinguishes views from tables.
  private val createdViews =
    new java.util.concurrent.ConcurrentHashMap[(Seq[String], String), TableInfo]()

  override def capabilities(): java.util.Set[TableCatalogCapability] =
    java.util.Collections.singleton(TableCatalogCapability.SUPPORTS_VIEW)

  override def loadTable(ident: Identifier): Table = {
    val key = (ident.namespace().toSeq, ident.name())
    Option(createdViews.get(key)).map(new MetadataOnlyTable(_)).getOrElse {
      ident.name() match {
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
    // Per the TableCatalog contract (v1 parity), this returns identifiers for both tables and
    // views; `listTableSummaries` (default impl: listTables + loadTable + read PROP_TABLE_TYPE)
    // is what distinguishes them.
    val targetNs = namespace.toSeq
    val ids = new java.util.ArrayList[Identifier]()
    createdViews.forEach { (key, _) =>
      if (key._1 == targetNs) ids.add(Identifier.of(key._1.toArray, key._2))
    }
    ids.toArray(new Array[Identifier](0))
  }

  private var catalogName = ""
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
  }
  override def name(): String = catalogName
}

/**
 * A minimal [[StagingTableCatalog]] used to drive `AtomicCreateV2ViewExec`. Views are stored
 * in a local map; staging commits write through, aborts discard. Supports SUPPORTS_VIEW.
 */
class TestingStagingCatalog extends StagingTableCatalog {

  private val views =
    new java.util.concurrent.ConcurrentHashMap[(Seq[String], String), TableInfo]()

  override def capabilities(): java.util.Set[TableCatalogCapability] =
    java.util.Collections.singleton(TableCatalogCapability.SUPPORTS_VIEW)

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

/**
 * A v2 catalog that does not declare SUPPORTS_VIEW. Used to exercise the capability gate:
 * it returns a view-typed [[MetadataOnlyTable]] from `loadTable`, so ALTER VIEW progresses
 * past view resolution and actually hits the gate in [[CheckViewReferences]].
 */
class TestingTableOnlyCatalog extends TableCatalog {
  // Pre-seeded view at default.v, used by the ALTER VIEW capability-gate test. Stored here
  // rather than in createTable so tests don't need to first create the view through Spark
  // (which would itself be blocked by the capability gate they're verifying).
  private val fixtureView: TableInfo = new TableInfo.Builder()
    .withSchema(new StructType().add("x", "int"))
    .withViewText("SELECT 1 AS x")
    .build()

  override def loadTable(ident: Identifier): Table =
    if (ident.namespace().toSeq == Seq("default") && ident.name() == "v") {
      new MetadataOnlyTable(fixtureView)
    } else {
      throw new NoSuchTableException(ident)
    }

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
