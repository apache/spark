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
import org.apache.spark.sql.connector.catalog.{Identifier, MetadataTable, Table, TableCatalog, TableChange, TableInfo, TableSummary, TableViewCatalog, V1Table, ViewCatalog, ViewInfo}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Tests for the view side of [[MetadataTable]]: view-text expansion on read, and
 * CREATE VIEW / ALTER VIEW ... AS going through the v2 write path
 * (`CreateV2ViewExec` / `AlterV2ViewExec`). View writes route through
 * [[ViewCatalog#createView]] / [[ViewCatalog#replaceView]].
 * Data-source-table read paths live in
 * [[org.apache.spark.sql.connector.DataSourceV2MetadataTableSuite]].
 *
 * TODO: register a `MetadataTable`-backed `DelegatingCatalogExtension` as
 * `spark.sql.catalog.spark_catalog` and run the shared
 * [[org.apache.spark.sql.execution.PersistedViewTestSuite]] body against the v2 path for full
 * parity with the v1 persisted-view coverage.
 */
class DataSourceV2MetadataViewSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.view_catalog", classOf[TestingTableViewCatalog].getName)

  // --- View read path -----------------------------------------------------

  test("read view expands SQL text and applies captured SQL configs") {
    withTable("spark_catalog.default.t") {
      Seq("a", "b").toDF("col").write.saveAsTable("spark_catalog.default.t")
      // view_catalog.ansi.test_view stores view.sqlConfig.spark.sql.ansi.enabled=true;
      // view_catalog.non_ansi.test_view stores the same key with value `false`. The view body
      // does `col::int` which errors in ANSI mode and yields NULL in non-ANSI mode.
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
    // view_catalog.ns1.ns2.t via the captured context -- which TestingTableViewCatalog resolves to
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
    val motTable = new MetadataTable(info, "v")
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
      catalog, Identifier.of(Array("ns"), "v"), new MetadataTable(infoWeird, "v"))
    assert(ctWeird.viewCatalogAndNamespace == Seq("my_cat", "weird.db", "normal"))
  }

  test("view with no captured catalog omits viewCatalogAndNamespace") {
    val info = new ViewInfo.Builder()
      .withSchema(new StructType().add("col", "string"))
      .withQueryText("SELECT * FROM spark_catalog.default.t")
      .build()
    val motTable = new MetadataTable(info, "v")
    val catalog = spark.sessionState.catalogManager.catalog("view_catalog")
    val ct = V1Table.toCatalogTable(catalog, Identifier.of(Array("ns"), "v"), motTable)
    assert(ct.viewCatalogAndNamespace.isEmpty)
  }

  // CREATE VIEW behavior tests live in the per-catalog triplet
  // `sql.execution.command.{,v1/,v2/}.CreateViewSuite{,Base}`.

  // ALTER VIEW behavior tests live in the per-catalog triplet
  // `sql.execution.command.{,v1/,v2/}.AlterViewAsSuite{,Base}`.

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
      // identifiers differ -- pin that cyclic detection compares them by `fullIdent`
      // (`view_catalog.ns1.inner.v` vs `view_catalog.ns2.inner.v`) and not by the lossy
      // 3-part `TableIdentifier` form, which would collapse both to
      // `TableIdentifier(v, Some("inner"), Some("view_catalog"))` and false-positive on
      // legitimate cross-namespace REPLACE.
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
      // the catalog). Pin that the rendered name carries every multi-level-namespace segment
      // (`view_catalog.ns1.inner.v_err`) -- routing the name through `Seq[String]` rather
      // than a 3-part `TableIdentifier` is what preserves the outer `ns1` segment in the
      // user-visible message.
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
        .asInstanceOf[TestingTableViewCatalog]
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

      // Legitimate non-cyclic ALTER -- the new body references `spark_catalog.default.t`,
      // not the view being altered. Pin that ALTER's cyclic detection compares views by
      // `fullIdent` so the two `inner.v_alter` views in different namespaces stay distinct;
      // a comparison via the lossy 3-part `TableIdentifier` would collapse both to
      // `TableIdentifier(v_alter, Some("inner"), Some("view_catalog"))` and false-positive
      // here.
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
    // `verifyTemporaryObjectsNotExists` / `verifyAutoGeneratedAliasesNotExists` route the
    // view name through `Seq[String]` rather than a 3-part `TableIdentifier`, so a
    // temp-function reference inside `view_catalog.ns1.inner.v_tempfn` surfaces an error
    // naming the full multi-part identifier. Routing through `asLegacyTableIdentifier`
    // would collapse `ns1.inner` to the last segment and drop the outer `ns1` from the
    // user-visible message.
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

  // --- v2 view DDL / inspection on a non-session v2 catalog ----------------------------
  // ResolveSessionCatalog's `ResolvedViewIdentifier` matcher is gated on isSessionCatalog, so
  // these plans flow through to DataSourceV2Strategy with a `ResolvedPersistentView` child.
  // Each is handled by a dedicated v2 exec defined alongside the v1 commands.

  private def seedV2View(name: String): Unit = {
    sql(s"CREATE VIEW view_catalog.default.$name AS SELECT 1 AS x")
  }

  // Used by the REFRESH / ANALYZE pins below: those plans still don't have a v2 implementation
  // and surface UNSUPPORTED_FEATURE.TABLE_OPERATION via DataSourceV2Strategy.
  private def assertUnsupportedViewOp(statement: String): Unit = {
    val ex = intercept[AnalysisException](sql(statement))
    assert(ex.getCondition == "UNSUPPORTED_FEATURE.TABLE_OPERATION", s"got ${ex.getCondition}")
  }

  // SET / UNSET / SCHEMA / RENAME / SHOW CREATE / SHOW TBLPROPERTIES / SHOW COLUMNS /
  // DESCRIBE TABLE on a v2 view live in the per-catalog test triplets under
  // `sql.execution.command.{,v1/,v2/}`; see e.g. AlterViewSetTblPropertiesSuite{,Base}.

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

  test("DESCRIBE TABLE ... PARTITION on a v2 view is rejected") {
    // The parser builds an `UnresolvedTableOrView` for DESCRIBE, so this reaches the v2
    // strategy with a `ResolvedPersistentView` child. Without an explicit pin the planner
    // falls through to a "No plan for DescribeTablePartition" assertion; pin it with
    // FORBIDDEN_OPERATION/DESC PARTITION on VIEW to mirror the v1 runtime check in
    // `DescribeTableCommand.describeDetailedPartitionInfo`.
    seedV2View("v_desc_part")
    val ex = intercept[AnalysisException] {
      sql("DESCRIBE TABLE view_catalog.default.v_desc_part PARTITION (x = 1)")
    }
    assert(ex.getCondition == "FORBIDDEN_OPERATION", s"got ${ex.getCondition}")
  }

  test("DESCRIBE TABLE EXTENDED ... AS JSON on a v2 view succeeds") {
    // `DescribeRelationJsonCommand` is a v1 runnable command that reads v1-shaped fields off
    // a `CatalogTable`. For non-session v2 views the resolved `ResolvedPersistentView.info`
    // is a plain `ViewInfo`; the command projects it to a `CatalogTable` via
    // `V1Table.toCatalogTable` so DESC ... AS JSON works uniformly across session and
    // non-session view catalogs.
    seedV2View("v_desc_json")
    val rows = sql(
      "DESCRIBE TABLE EXTENDED view_catalog.default.v_desc_json AS JSON").collect()
    assert(rows.length == 1, s"DESC AS JSON should produce one row, got: ${rows.length}")
    val json = rows.head.getString(0)
    assert(json.contains("\"v_desc_json\""), s"JSON output missing view name: $json")
    assert(json.contains("\"VIEW\""), s"JSON output missing VIEW table_type: $json")
  }

  // DROP VIEW behavior tests live in the per-catalog triplet
  // `sql.execution.command.{,v1/,v2/}.DropViewSuite{,Base}`.

  // --- SHOW TABLES / SHOW VIEWS on a v2 catalog --------------------------------

  private def seedV2Table(name: String): Unit = {
    val catalog = spark.sessionState.catalogManager.catalog("view_catalog")
      .asInstanceOf[TestingTableViewCatalog]
    catalog.createTable(
      Identifier.of(Array("default"), name),
      new TableInfo.Builder()
        .withSchema(new StructType().add("x", "int"))
        .withTableType(TableSummary.EXTERNAL_TABLE_TYPE)
        .build())
  }

  test("SHOW TABLES on a TableViewCatalog returns both tables and views (v1-parity)") {
    // For a `TableViewCatalog` (a catalog exposing both tables and views in a shared
    // identifier namespace), SHOW TABLES routes through `listTableAndViewSummaries` so views
    // appear alongside tables -- matching the v1 SHOW TABLES output. Pure `TableCatalog`
    // catalogs (no view mixin) continue to use `listTables` and return tables only.
    seedV2View("v_in_show_tables")
    seedV2Table("t_in_show_tables")
    val rows = sql("SHOW TABLES IN view_catalog.default").collect()
    val names = rows.map(_.getString(1)).toSet
    assert(names.contains("t_in_show_tables"), s"table missing from SHOW TABLES: $names")
    assert(names.contains("v_in_show_tables"), s"view missing from SHOW TABLES: $names")
    rows.foreach(r => assert(!r.getBoolean(2), s"isTemporary must be false: $r"))
  }

  // SHOW VIEWS behavior tests live in the per-catalog triplet
  // `sql.execution.command.{,v1/,v2/}.ShowViewsSuite{,Base}`.
}

/**
 * A [[TableViewCatalog]]: round-trips [[MetadataTable]] for created views and tables and
 * exposes a few canned read-only view fixtures (`test_view`, `test_unqualified_view`,
 * `test_unqualified_multi`, plus an unqualified-target view at `ns1.ns2.t`) used by the
 * view-read tests. Entries created via `createTable` / `createView` are distinguished by the
 * stored value's runtime type (ViewInfo vs TableInfo). The single-RPC perf entry point
 * [[loadTableOrView]] returns either kind; [[loadTable]] is tables-only per the
 * [[TableCatalog#loadTable]] contract.
 */
class TestingTableViewCatalog extends TableViewCatalog {

  // Holds entries (views and tables) created via createTable / createView within the session.
  // Keyed by (namespace, name); the stored value's runtime type (ViewInfo vs TableInfo)
  // distinguishes views from tables. Mixed-catalog: shared identifier namespace per the
  // TableViewCatalog contract.
  private val createdViews =
    new java.util.concurrent.ConcurrentHashMap[(Seq[String], String), TableInfo]()

  // Canned read-only view fixtures, exposed only via the perf path (loadTableOrView). loadView
  // does not need to expose them because the resolver routes TableViewCatalog reads through
  // loadTableOrView.
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

  override def loadTableOrView(ident: Identifier): Table = {
    // Single-RPC perf path: returns tables AND views (as MetadataTable). Stored entries
    // win over fixture views (the fixture namespace is read-only and disjoint from
    // createdViews in practice). loadTable, loadView, tableExists, viewExists all derive
    // from this via the TableViewCatalog default impls.
    val key = (ident.namespace().toSeq, ident.name())
    Option(createdViews.get(key))
      .orElse(fixtureView(ident))
      .map(new MetadataTable(_, ident.toString))
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
    new MetadataTable(info, ident.toString)
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

  override def renameView(oldIdent: Identifier, newIdent: Identifier): Unit = {
    val oldKey = (oldIdent.namespace().toSeq, oldIdent.name())
    val newKey = (newIdent.namespace().toSeq, newIdent.name())
    val existing = createdViews.get(oldKey)
    if (existing == null || !existing.isInstanceOf[ViewInfo]) {
      throw new NoSuchViewException(oldIdent)
    }
    if (createdViews.putIfAbsent(newKey, existing) != null) {
      throw new ViewAlreadyExistsException(newIdent)
    }
    createdViews.remove(oldKey)
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

  override def renameView(oldIdent: Identifier, newIdent: Identifier): Unit = {
    val oldKey = (oldIdent.namespace().toSeq, oldIdent.name())
    val newKey = (newIdent.namespace().toSeq, newIdent.name())
    val existing = store.get(oldKey)
    if (existing == null) throw new NoSuchViewException(oldIdent)
    if (store.putIfAbsent(newKey, existing) != null) {
      throw new ViewAlreadyExistsException(newIdent)
    }
    store.remove(oldKey)
  }

  private var catalogName = ""
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
    seedDefault()
  }
  override def name(): String = catalogName
}
