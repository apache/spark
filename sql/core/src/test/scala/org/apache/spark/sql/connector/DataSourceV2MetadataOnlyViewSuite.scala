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
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, NoSuchViewException, TableAlreadyExistsException, ViewAlreadyExistsException}
import org.apache.spark.sql.connector.catalog.{Identifier, MetadataOnlyTable, RelationCatalog, Table, TableCatalog, TableChange, TableInfo, TableSummary, V1Table, ViewCatalog, ViewInfo}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Tests for the view side of [[MetadataOnlyTable]]: view-text expansion on read, and
 * CREATE VIEW / ALTER VIEW ... AS going through the v2 write path
 * (`CreateV2ViewExec` / `AlterV2ViewExec`). View writes route through
 * [[ViewCatalog#createView]] / [[ViewCatalog#replaceView]].
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
    .set("spark.sql.catalog.view_catalog", classOf[TestingRelationCatalog].getName)

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

  test("read view resolves unqualified refs via multi-part captured namespace") {
    // End-to-end coverage of the v2 encoder -> parser round-trip: test_unqualified_multi is a
    // view whose captured catalog+namespace is view_catalog.ns1.ns2 (two-part namespace) and
    // whose body references `t` unqualified. At read time the unqualified `t` must expand to
    // view_catalog.ns1.ns2.t via the captured context -- which TestingRelationCatalog resolves to
    // its own `t` fixture at that namespace.
    checkAnswer(
      spark.table("view_catalog.outer_ns.test_unqualified_multi"),
      Row("multi"))
  }

  // --- ViewInfo unit tests -----------------------------------------------

  test("multi-part captured namespace round-trips through V1Table.toCatalogTable") {
    // (a) ViewInfo.Builder stores (cat, Array(db1, db2)) as typed fields.
    // (b) V1Table.toCatalogTable reads them directly and emits v1's numbered
    //     view.catalogAndNamespace.* keys so (c) the resulting CatalogTable's
    //     `viewCatalogAndNamespace` exposes the full (cat, db1, db2), which is what the v1
    //     view-resolution path consumes to expand unqualified references in the view body.
    val info = new ViewInfo.Builder()
      .withSchema(new StructType().add("col", "string"))
      .withQueryText("SELECT col FROM t")
      .withCurrentCatalog("my_cat")
      .withCurrentNamespace(Array("db1", "db2"))
      .build()
    val motTable = new MetadataOnlyTable(info, "v")
    // Any CatalogPlugin works here; toCatalogTable only reads `catalog.name()`.
    val catalog = spark.sessionState.catalogManager.catalog("view_catalog")
    val ct = V1Table.toCatalogTable(
      catalog, Identifier.of(Array("ns"), "v"), motTable)
    assert(ct.viewCatalogAndNamespace == Seq("my_cat", "db1", "db2"))

    // Namespace parts containing dots flow through structurally (no string encoding).
    val infoWeird = new ViewInfo.Builder()
      .withSchema(new StructType().add("col", "string"))
      .withQueryText("SELECT col FROM t")
      .withCurrentCatalog("my_cat")
      .withCurrentNamespace(Array("weird.db", "normal"))
      .build()
    val ctWeird = V1Table.toCatalogTable(
      catalog, Identifier.of(Array("ns"), "v"), new MetadataOnlyTable(infoWeird, "v"))
    assert(ctWeird.viewCatalogAndNamespace == Seq("my_cat", "weird.db", "normal"))
  }

  test("view with no captured catalog omits viewCatalogAndNamespace") {
    val info = new ViewInfo.Builder()
      .withSchema(new StructType().add("col", "string"))
      .withQueryText("SELECT * FROM spark_catalog.default.t")
      .build()
    val motTable = new MetadataOnlyTable(info, "v")
    val catalog = spark.sessionState.catalogManager.catalog("view_catalog")
    val ct = V1Table.toCatalogTable(catalog, Identifier.of(Array("ns"), "v"), motTable)
    assert(ct.viewCatalogAndNamespace.isEmpty)
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

  test("CREATE VIEW on a catalog without ViewCatalog fails") {
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
      // TestingRelationCatalog stores the TableInfo verbatim, so the collation property is
      // observable via the catalog-stored builder output.
      val catalog = spark.sessionState.catalogManager.catalog("view_catalog")
        .asInstanceOf[TestingRelationCatalog]
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
      .asInstanceOf[TestingRelationCatalog]
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
        val stored = catalog.getStoredInfo(Array("default"), "v_existing_table")
        assert(!stored.isInstanceOf[ViewInfo])
        assert(stored.properties().get(TableCatalog.PROP_TABLE_TYPE) ==
          TableSummary.EXTERNAL_TABLE_TYPE)
      }
    } finally {
      catalog.dropTable(tableIdent)
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

  test("ALTER VIEW rejects reference to a temporary view") {
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
      sql("CREATE VIEW view_catalog.default.v_alter_tempview AS " +
        "SELECT x FROM spark_catalog.default.t")
      withTempView("tv_alter") {
        spark.range(3).createOrReplaceTempView("tv_alter")
        val ex = intercept[AnalysisException] {
          sql("ALTER VIEW view_catalog.default.v_alter_tempview AS SELECT id FROM tv_alter")
        }
        assert(ex.getMessage.toLowerCase(java.util.Locale.ROOT).contains("temporary"))
      }
    }
  }

  test("ALTER VIEW rejects reference to a temporary variable") {
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
      sql("CREATE VIEW view_catalog.default.v_alter_tempvar AS " +
        "SELECT x FROM spark_catalog.default.t")
      withSessionVariable("temp_var_alter") {
        sql("DECLARE VARIABLE temp_var_alter INT DEFAULT 1")
        val ex = intercept[AnalysisException] {
          sql("ALTER VIEW view_catalog.default.v_alter_tempvar AS SELECT temp_var_alter AS x")
        }
        assert(ex.getMessage.toLowerCase(java.util.Locale.ROOT).contains("temporary"))
      }
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
        .asInstanceOf[TestingRelationCatalog]
      val info = catalog.getStoredView(Array("default"), "v_preserve")
      assert(info.properties().get("mykey") == "myvalue")
    }
  }

  test("CREATE VIEW stamps PROP_OWNER on the stored TableInfo") {
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
      sql("CREATE VIEW view_catalog.default.v_owner_create AS " +
        "SELECT x FROM spark_catalog.default.t")

      val catalog = spark.sessionState.catalogManager.catalog("view_catalog")
        .asInstanceOf[TestingRelationCatalog]
      val info = catalog.getStoredView(Array("default"), "v_owner_create")
      // v2 CREATE VIEW stamps the current user into PROP_OWNER, matching v2 CREATE TABLE
      // (via CatalogV2Util.withDefaultOwnership) and v1 CREATE VIEW (via CatalogTable.owner's
      // default). Without this, the ALTER VIEW preservation test above would have nothing to
      // carry forward on a v2-created view.
      val owner = info.properties().get(TableCatalog.PROP_OWNER)
      assert(owner != null && owner.nonEmpty, s"expected a non-empty owner, got: $owner")
    }
  }

  test("ALTER VIEW preserves PROP_OWNER (v1-parity)") {
    val catalog = spark.sessionState.catalogManager.catalog("view_catalog")
      .asInstanceOf[TestingRelationCatalog]
    val viewIdent = Identifier.of(Array("default"), "v_owner")
    // Pre-seed a view whose stored ViewInfo carries an explicit owner.
    val initialInfo = new ViewInfo.Builder()
      .withSchema(new StructType().add("x", "int"))
      .withQueryText("SELECT 1 AS x")
      .withOwner("alice")
      .withCurrentCatalog("spark_catalog")
      .withCurrentNamespace(Array("default"))
      .build()
    catalog.createView(viewIdent, initialInfo)
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
        .asInstanceOf[TestingRelationCatalog]
      assert(catalog.getStoredView(Array("default"), "v_evo").schemaMode() == "EVOLUTION")
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
        .asInstanceOf[TestingRelationCatalog]
      assert(catalog.getStoredView(Array("default"), "v_configs")
        .sqlConfigs().get(SQLConf.ANSI_ENABLED.key) == "true")

      // ALTER under a different ANSI setting should replace the stored config, not merge.
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        sql("ALTER VIEW view_catalog.default.v_configs AS " +
          "SELECT col FROM spark_catalog.default.t WHERE col = 'b'")
      }
      assert(catalog.getStoredView(Array("default"), "v_configs")
        .sqlConfigs().get(SQLConf.ANSI_ENABLED.key) == "false")
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

  test("ALTER VIEW on a catalog without ViewCatalog fails with MISSING_CATALOG_ABILITY") {
    // ALTER VIEW's identifier is resolved via `UnresolvedView`, whose `viewOnly=true` path
    // in `Analyzer.lookupTableOrView` rejects non-ViewCatalog catalogs up front with the
    // expected error class -- before `loadTable` is even called. `TestingTableOnlyCatalog`
    // happens to round-trip `default.v` as a view-typed MetadataOnlyTable, but that fixture
    // is not actually consulted on this path. CREATE VIEW's capability check lives in
    // `CheckViewReferences`; ALTER VIEW's lives in the analyzer gate. Both yield
    // `MISSING_CATALOG_ABILITY.VIEWS`.
    withSQLConf(
      "spark.sql.catalog.no_view_catalog" -> classOf[TestingTableOnlyCatalog].getName) {
      val ex = intercept[AnalysisException] {
        sql("ALTER VIEW no_view_catalog.default.v AS SELECT 1 AS x")
      }
      assert(ex.getCondition == "MISSING_CATALOG_ABILITY.VIEWS")
    }
  }

  // --- Pure ViewCatalog (no TableCatalog mixin) ---------------------------

  test("read view from a pure ViewCatalog (no TableCatalog mixin)") {
    // The analyzer's table-side lookup must skip `loadTable` entirely for catalogs that don't
    // implement `TableCatalog`; otherwise `asTableCatalog` would throw
    // MISSING_CATALOG_ABILITY.TABLES and the legitimate `loadView` fallback would never run.
    withSQLConf(
      "spark.sql.catalog.view_only" -> classOf[TestingViewOnlyCatalog].getName) {
      withTable("spark_catalog.default.t") {
        Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
        // The fixture stores a `pure_v` view whose body filters spark_catalog.default.t.
        checkAnswer(spark.table("view_only.default.pure_v"), Seq(Row(2), Row(3)))
      }
    }
  }

  test("ALTER VIEW on a pure ViewCatalog (no TableCatalog mixin)") {
    withSQLConf(
      "spark.sql.catalog.view_only" -> classOf[TestingViewOnlyCatalog].getName) {
      val catalog = spark.sessionState.catalogManager.catalog("view_only")
        .asInstanceOf[TestingViewOnlyCatalog]
      withTable("spark_catalog.default.t") {
        Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
        sql("ALTER VIEW view_only.default.pure_v AS " +
          "SELECT x FROM spark_catalog.default.t WHERE x > 2")
        assert(catalog.loadView(Identifier.of(Array("default"), "pure_v")).queryText() ==
          "SELECT x FROM spark_catalog.default.t WHERE x > 2")
      }
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

  test("view error messages render the full multi-level namespace") {
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
      sql("CREATE VIEW view_catalog.ns1.inner.v_err AS " +
        "SELECT x FROM spark_catalog.default.t")
      // Second CREATE surfaces `viewAlreadyExistsError` (via TableAlreadyExistsException from
      // the catalog). Before the error signatures took `Seq[String]`, `legacyName` collapsed
      // ns1.inner into just `inner` and the error said `view_catalog.inner.v_err` -- missing
      // the outer `ns1` segment.
      val dup = intercept[AnalysisException] {
        sql("CREATE VIEW view_catalog.ns1.inner.v_err AS " +
          "SELECT x FROM spark_catalog.default.t")
      }
      assert(dup.getCondition == "TABLE_OR_VIEW_ALREADY_EXISTS")
      assert(dup.getMessage.contains("`view_catalog`.`ns1`.`inner`.`v_err`"),
        s"expected full multi-part name in error, got: ${dup.getMessage}")

      // CREATE OR REPLACE VIEW over a non-view table entry surfaces
      // `unsupportedCreateOrReplaceViewOnTableError`. Pre-seed a non-view entry at a
      // multi-level-namespace identifier to exercise the rendering.
      val catalog = spark.sessionState.catalogManager.catalog("view_catalog")
        .asInstanceOf[TestingRelationCatalog]
      val tblIdent = Identifier.of(Array("ns1", "inner"), "t_err")
      catalog.createTable(
        tblIdent,
        new TableInfo.Builder()
          .withSchema(new StructType().add("col", "string"))
          .withTableType(TableSummary.EXTERNAL_TABLE_TYPE)
          .build())
      try {
        val notView = intercept[AnalysisException] {
          sql("CREATE OR REPLACE VIEW view_catalog.ns1.inner.t_err AS " +
            "SELECT x FROM spark_catalog.default.t")
        }
        assert(notView.getCondition == "EXPECT_VIEW_NOT_TABLE.NO_ALTERNATIVE")
        assert(notView.getMessage.contains("`view_catalog`.`ns1`.`inner`.`t_err`"),
          s"expected full multi-part name in error, got: ${notView.getMessage}")
      } finally {
        catalog.dropTable(tblIdent)
      }

      // Column-arity mismatch error.
      val arity = intercept[AnalysisException] {
        sql("CREATE VIEW view_catalog.ns1.inner.v_arity (a, b) AS " +
          "SELECT x FROM spark_catalog.default.t")
      }
      assert(arity.getMessage.contains("`view_catalog`.`ns1`.`inner`.`v_arity`"),
        s"expected full multi-part name in error, got: ${arity.getMessage}")
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

  test("temp-object reference errors render the full multi-level namespace") {
    // `verifyTemporaryObjectsNotExists` / `verifyAutoGeneratedAliasesNotExists` used to take a
    // `TableIdentifier` built via `asLegacyTableIdentifier`, which collapses multi-level
    // namespaces to the last segment -- so a temp-function reference on
    // `view_catalog.ns1.inner.v_tempfn` produced an error naming
    // `view_catalog.inner.v_tempfn` and dropped the `ns1` middle segment. Post-migration the
    // errors render the full multi-part name.
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
      spark.udf.register("temp_udf_multi", (i: Int) => i + 1)
      val ex = intercept[AnalysisException] {
        sql("CREATE VIEW view_catalog.ns1.inner.v_tempfn AS " +
          "SELECT temp_udf_multi(x) FROM spark_catalog.default.t")
      }
      assert(ex.getCondition == "INVALID_TEMP_OBJ_REFERENCE")
      assert(ex.getMessage.contains("`view_catalog`.`ns1`.`inner`.`v_tempfn`"),
        s"expected full multi-part name, got: ${ex.getMessage}")
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

  // These plans reach `DataSourceV2Strategy` with a `ResolvedPersistentView` child on a
  // non-session v2 view (because `ResolvedV1TableOrViewIdentifier` now skips non-session views).
  // Without explicit pins they would hit `QueryPlanner`'s `assert(pruned.hasNext, "No plan for
  // ...")` and surface a raw AssertionError. Pin each to UNSUPPORTED_FEATURE.TABLE_OPERATION.

  test("REFRESH TABLE on a v2 view is rejected") {
    seedV2View("v_refresh")
    assertUnsupportedViewOp("REFRESH TABLE view_catalog.default.v_refresh")
  }

  test("ANALYZE TABLE on a v2 view is rejected") {
    seedV2View("v_analyze")
    assertUnsupportedViewOp(
      "ANALYZE TABLE view_catalog.default.v_analyze COMPUTE STATISTICS")
  }

  test("ANALYZE TABLE ... FOR COLUMNS on a v2 view is rejected") {
    seedV2View("v_analyze_cols")
    assertUnsupportedViewOp(
      "ANALYZE TABLE view_catalog.default.v_analyze_cols COMPUTE STATISTICS FOR COLUMNS x")
  }

  // --- DROP VIEW on a v2 catalog --------------------------------

  test("DROP VIEW on a ViewCatalog drops the view") {
    val catalog = spark.sessionState.catalogManager.catalog("view_catalog")
      .asInstanceOf[TestingRelationCatalog]
    withTable("spark_catalog.default.t") {
      Seq(1, 2, 3).toDF("x").write.saveAsTable("spark_catalog.default.t")
      sql("CREATE VIEW view_catalog.default.v_drop AS " +
        "SELECT x FROM spark_catalog.default.t")
      assert(catalog.viewExists(Identifier.of(Array("default"), "v_drop")))
      sql("DROP VIEW view_catalog.default.v_drop")
      assert(!catalog.viewExists(Identifier.of(Array("default"), "v_drop")))
    }
  }

  test("DROP VIEW IF EXISTS on a v2 catalog is a no-op when the view is missing") {
    // Exercises the `ifExists=true` path -- DropViewExec should not throw when the view
    // doesn't exist on a ViewCatalog.
    sql("DROP VIEW IF EXISTS view_catalog.default.v_never_existed")
  }

  test("DROP VIEW on a non-view table entry is rejected (v1-parity)") {
    // v1 `DropTableCommand(isView = true)` rejects a non-view target via
    // `wrongCommandForObjectTypeError`. The v2 path must also refuse -- otherwise
    // `DROP VIEW view_catalog.default.<table>` would silently destroy the table's entry.
    val catalog = spark.sessionState.catalogManager.catalog("view_catalog")
      .asInstanceOf[TestingRelationCatalog]
    val tableIdent = Identifier.of(Array("default"), "t_not_a_view")
    catalog.createTable(
      tableIdent,
      new TableInfo.Builder()
        .withSchema(new StructType().add("col", "string"))
        .withTableType(TableSummary.EXTERNAL_TABLE_TYPE)
        .build())
    try {
      val ex = intercept[AnalysisException] {
        sql("DROP VIEW view_catalog.default.t_not_a_view")
      }
      assert(ex.getCondition == "EXPECT_VIEW_NOT_TABLE.NO_ALTERNATIVE")
      // The table entry must still be there -- DROP VIEW did not destroy it.
      assert(catalog.tableExists(tableIdent))
    } finally {
      catalog.dropTable(tableIdent)
    }
  }

  test("DROP VIEW on a catalog without ViewCatalog is rejected") {
    withSQLConf(
      "spark.sql.catalog.no_view_catalog" -> classOf[TestingTableOnlyCatalog].getName) {
      val ex = intercept[AnalysisException] {
        sql("DROP VIEW no_view_catalog.default.v")
      }
      // Preserves the pre-PR error surface for non-ViewCatalog catalogs.
      assert(ex.getMessage.toLowerCase(java.util.Locale.ROOT).contains("views"))
    }
  }

  // --- SHOW TABLES / SHOW VIEWS on a v2 catalog --------------------------------

  private def seedV2Table(name: String): Unit = {
    val catalog = spark.sessionState.catalogManager.catalog("view_catalog")
      .asInstanceOf[TestingRelationCatalog]
    catalog.createTable(
      Identifier.of(Array("default"), name),
      new TableInfo.Builder()
        .withSchema(new StructType().add("x", "int"))
        .withTableType(TableSummary.EXTERNAL_TABLE_TYPE)
        .build())
  }

  test("SHOW TABLES on a v2 catalog returns only tables") {
    // Per the new `TableCatalog.listTables` contract, SHOW TABLES returns table identifiers
    // only -- views (in mixed catalogs) are listed via SHOW VIEWS / `ViewCatalog.listViews`.
    // This is an intentional divergence from v1 SHOW TABLES (which includes both tables and
    // views in a single listing); v2 catalogs separate the two so callers can target either
    // kind without filtering.
    seedV2View("v_in_show_tables")
    seedV2Table("t_in_show_tables")
    val rows = sql("SHOW TABLES IN view_catalog.default").collect()
    val names = rows.map(_.getString(1)).toSet
    assert(names.contains("t_in_show_tables"), s"table missing from SHOW TABLES: $names")
    assert(!names.contains("v_in_show_tables"), s"view leaked into SHOW TABLES: $names")
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

  test("SHOW VIEWS on a catalog without ViewCatalog is rejected") {
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
 * A [[RelationCatalog]]: round-trips [[MetadataOnlyTable]] for created views and tables and
 * exposes a few canned read-only view fixtures (`test_view`, `test_unqualified_view`,
 * `test_unqualified_multi`, plus an unqualified-target view at `ns1.ns2.t`) used by the
 * view-read tests. Entries created via `createTable` / `createView` are distinguished by the
 * stored value's runtime type (ViewInfo vs TableInfo). The single-RPC perf entry point
 * [[loadRelation]] returns either kind; [[loadTable]] is tables-only per the
 * [[TableCatalog#loadTable]] contract.
 */
class TestingRelationCatalog extends RelationCatalog {

  // Holds entries (views and tables) created via createTable / createView within the session.
  // Keyed by (namespace, name); the stored value's runtime type (ViewInfo vs TableInfo)
  // distinguishes views from tables. Mixed-catalog: shared identifier namespace per the
  // RelationCatalog contract.
  private val createdViews =
    new java.util.concurrent.ConcurrentHashMap[(Seq[String], String), TableInfo]()

  // Canned read-only view fixtures, exposed only via the perf path (loadRelation). loadView
  // does not need to expose them because the resolver routes RelationCatalog reads through
  // loadRelation.
  private def fixtureView(ident: Identifier): Option[ViewInfo] = ident.name() match {
    case "test_view" =>
      Some(new ViewInfo.Builder()
        .withSchema(new StructType().add("col", "string").add("i", "int"))
        .withQueryText(
          "SELECT col, col::int AS i FROM spark_catalog.default.t WHERE col = 'b'")
        .withSqlConfigs(java.util.Collections.singletonMap(
          SQLConf.ANSI_ENABLED.key, (ident.namespace().head == "ansi").toString))
        .build())
    case "test_unqualified_view" =>
      Some(new ViewInfo.Builder()
        .withSchema(new StructType().add("col", "string"))
        .withQueryText("SELECT col FROM t WHERE col = 'b'")
        .withCurrentCatalog("spark_catalog")
        .withCurrentNamespace(Array("default"))
        .build())
    case "test_unqualified_multi" =>
      // View whose captured catalog+namespace is view_catalog.ns1.ns2 (two-part). The
      // unqualified `t` in the body must resolve via that captured context to
      // view_catalog.ns1.ns2.t, which this catalog also serves (see `t` case below).
      Some(new ViewInfo.Builder()
        .withSchema(new StructType().add("col", "string"))
        .withQueryText("SELECT col FROM t")
        .withCurrentCatalog("view_catalog")
        .withCurrentNamespace(Array("ns1", "ns2"))
        .build())
    case "t" if ident.namespace().toSeq == Seq("ns1", "ns2") =>
      // Target of test_unqualified_multi's unqualified reference. Self-contained view so
      // the test doesn't need external data.
      Some(new ViewInfo.Builder()
        .withSchema(new StructType().add("col", "string"))
        .withQueryText("SELECT 'multi' AS col")
        .build())
    case _ => None
  }

  override def loadRelation(ident: Identifier): Table = {
    // Single-RPC perf path: returns tables AND views (as MetadataOnlyTable). Stored entries
    // win over fixture views (the fixture namespace is read-only and disjoint from
    // createdViews in practice). loadTable, loadView, tableExists, viewExists all derive
    // from this via the RelationCatalog default impls.
    val key = (ident.namespace().toSeq, ident.name())
    Option(createdViews.get(key))
      .orElse(fixtureView(ident))
      .map(new MetadataOnlyTable(_, ident.toString))
      .getOrElse(throw new NoSuchTableException(ident))
  }

  override def createTable(ident: Identifier, info: TableInfo): Table = {
    // Mixed-catalog contract: createTable rejects when a view sits at ident with
    // TableAlreadyExistsException. The shared `createdViews` keyspace makes `putIfAbsent`
    // throw uniformly for both table-at-ident and view-at-ident collisions.
    val key = (ident.namespace().toSeq, ident.name())
    if (createdViews.putIfAbsent(key, info) != null) {
      throw new TableAlreadyExistsException(ident)
    }
    new MetadataOnlyTable(info, ident.toString)
  }

  /** Test-only accessor: returns the stored TableInfo (table or view) for the identifier. */
  def getStoredInfo(namespace: Array[String], name: String): TableInfo = {
    Option(createdViews.get((namespace.toSeq, name))).getOrElse {
      throw new NoSuchTableException(Identifier.of(namespace, name))
    }
  }

  /** Test-only accessor: returns the stored ViewInfo; fails if the entry is not a view. */
  def getStoredView(namespace: Array[String], name: String): ViewInfo = getStoredInfo(
    namespace, name) match {
    case v: ViewInfo => v
    case _ => throw new IllegalStateException(
      s"stored entry at ${namespace.mkString(".")}.$name is not a view")
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new RuntimeException("shouldn't be called")
  }
  override def dropTable(ident: Identifier): Boolean = {
    val key = (ident.namespace().toSeq, ident.name())
    val existing = createdViews.get(key)
    if (existing == null || existing.isInstanceOf[ViewInfo]) return false
    createdViews.remove(key) != null
  }
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new RuntimeException("shouldn't be called")
  }
  override def listTables(namespace: Array[String]): Array[Identifier] = {
    // Tables only -- views are listed via ViewCatalog.listViews per the new contract.
    val targetNs = namespace.toSeq
    val ids = new java.util.ArrayList[Identifier]()
    createdViews.forEach { (key, info) =>
      if (key._1 == targetNs && !info.isInstanceOf[ViewInfo]) {
        ids.add(Identifier.of(key._1.toArray, key._2))
      }
    }
    ids.toArray(new Array[Identifier](0))
  }

  // ViewCatalog methods. Storage is shared with TableCatalog (mixed-catalog pattern).

  override def listViews(namespace: Array[String]): Array[Identifier] = {
    val targetNs = namespace.toSeq
    val ids = new java.util.ArrayList[Identifier]()
    createdViews.forEach { (key, info) =>
      if (key._1 == targetNs && info.isInstanceOf[ViewInfo]) {
        ids.add(Identifier.of(key._1.toArray, key._2))
      }
    }
    ids.toArray(new Array[Identifier](0))
  }

  override def createView(ident: Identifier, info: ViewInfo): ViewInfo = {
    val key = (ident.namespace().toSeq, ident.name())
    if (createdViews.putIfAbsent(key, info) != null) {
      throw new ViewAlreadyExistsException(ident)
    }
    info
  }

  override def replaceView(ident: Identifier, info: ViewInfo): ViewInfo = {
    val key = (ident.namespace().toSeq, ident.name())
    val existing = createdViews.get(key)
    if (existing == null || !existing.isInstanceOf[ViewInfo]) {
      throw new NoSuchViewException(ident)
    }
    createdViews.put(key, info)
    info
  }

  override def dropView(ident: Identifier): Boolean = {
    val key = (ident.namespace().toSeq, ident.name())
    val existing = createdViews.get(key)
    if (existing == null || !existing.isInstanceOf[ViewInfo]) return false
    createdViews.remove(key) != null
  }

  private var catalogName = ""
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
  }
  override def name(): String = catalogName
}

/**
 * A v2 catalog that does not implement ViewCatalog. Used by capability-gate tests: the gate
 * fires in `Analyzer.lookupTableOrView(viewOnly=true)` for ALTER VIEW and in
 * [[CheckViewReferences]] for CREATE VIEW -- in both cases before `loadTable` is called --
 * so this catalog's content is intentionally empty.
 */
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

/**
 * A pure [[ViewCatalog]] (no [[TableCatalog]] mixin). Used to exercise that the analyzer's
 * resolution paths skip the `loadTable` step and fall through to `loadView` for catalogs that
 * cannot host tables. Pre-seeds a single mutable view at `default.pure_v` so the read and
 * ALTER VIEW tests can both reach it.
 */
class TestingViewOnlyCatalog extends ViewCatalog {
  private val store =
    new java.util.concurrent.ConcurrentHashMap[(Seq[String], String), ViewInfo]()

  // Seeded on first `initialize`. Filters `spark_catalog.default.t` so the read test can
  // assert deterministic output. ALTER VIEW tests overwrite it via `replaceView`.
  private def seedDefault(): Unit = {
    val key = (Seq("default"), "pure_v")
    if (!store.containsKey(key)) {
      val info = new ViewInfo.Builder()
        .withSchema(new StructType().add("x", "int"))
        .withQueryText("SELECT x FROM spark_catalog.default.t WHERE x > 1")
        .build()
      store.put(key, info)
    }
  }

  override def listViews(namespace: Array[String]): Array[Identifier] = {
    val target = namespace.toSeq
    val ids = new java.util.ArrayList[Identifier]()
    store.forEach { (key, _) =>
      if (key._1 == target) ids.add(Identifier.of(key._1.toArray, key._2))
    }
    ids.toArray(new Array[Identifier](0))
  }

  override def loadView(ident: Identifier): ViewInfo = {
    val key = (ident.namespace().toSeq, ident.name())
    Option(store.get(key)).getOrElse(throw new NoSuchViewException(ident))
  }

  override def createView(ident: Identifier, info: ViewInfo): ViewInfo = {
    val key = (ident.namespace().toSeq, ident.name())
    if (store.putIfAbsent(key, info) != null) {
      throw new ViewAlreadyExistsException(ident)
    }
    info
  }

  override def replaceView(ident: Identifier, info: ViewInfo): ViewInfo = {
    val key = (ident.namespace().toSeq, ident.name())
    if (!store.containsKey(key)) throw new NoSuchViewException(ident)
    store.put(key, info)
    info
  }

  override def dropView(ident: Identifier): Boolean = {
    val key = (ident.namespace().toSeq, ident.name())
    store.remove(key) != null
  }

  private var catalogName = ""
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
    seedDefault()
  }
  override def name(): String = catalogName
}
