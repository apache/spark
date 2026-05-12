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

package org.apache.spark.sql.execution

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{NoSuchViewException, ViewAlreadyExistsException}
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryTableCatalog, MetadataTable, Table, TableCatalog, TableDependency, TableSummary, TableViewCatalog, ViewInfo}
import org.apache.spark.sql.metricview.serde.{AssetSource, Column, Constants, DimensionExpression, MeasureExpression, MetricView, MetricViewFactory, SQLSource}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.Metadata

/**
 * Tests that exercise [[org.apache.spark.sql.execution.command.CreateMetricViewCommand]] on a
 * non-session V2 catalog. Metric views are persisted through the same [[ViewCatalog]] interface
 * as plain views; the only marker that distinguishes them is `PROP_TABLE_TYPE = METRIC_VIEW`
 * plus the typed `viewDependencies` field on [[ViewInfo]]. The recording catalog used here is a
 * minimal [[TableViewCatalog]] so the same instance can also host the source table referenced by
 * the metric view's YAML.
 */
class MetricViewV2CatalogSuite extends SharedSparkSession {

  import testImplicits._

  private val testCatalogName = "testcat"
  private val testNamespace = "ns"
  private val sourceTableName = "events"
  private val fullSourceTableName =
    s"$testCatalogName.$testNamespace.$sourceTableName"
  private val metricViewName = "mv"
  private val fullMetricViewName =
    s"$testCatalogName.$testNamespace.$metricViewName"

  private val metricViewColumns = Seq(
    Column("region", DimensionExpression("region"), 0),
    Column("count_sum", MeasureExpression("sum(count)"), 1))

  private val testTableData = Seq(
    ("region_1", 1, 5.0),
    ("region_2", 2, 10.0))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(
      s"spark.sql.catalog.$testCatalogName",
      classOf[MetricViewRecordingCatalog].getName)
    // A catalog that does not implement ViewCatalog - used for the negative gate test.
    spark.conf.set(
      s"spark.sql.catalog.${MetricViewV2CatalogSuite.noViewCatalogName}",
      classOf[InMemoryTableCatalog].getName)
  }

  override protected def afterAll(): Unit = {
    spark.conf.unset(s"spark.sql.catalog.$testCatalogName")
    spark.conf.unset(
      s"spark.sql.catalog.${MetricViewV2CatalogSuite.noViewCatalogName}")
    super.afterAll()
  }

  private def withTestCatalogTables(body: => Unit): Unit = {
    MetricViewRecordingCatalog.reset()
    testTableData.toDF("region", "count", "price")
      .createOrReplaceTempView("metric_view_v2_source")
    try {
      sql(
        s"""CREATE TABLE $fullSourceTableName
           |USING foo AS SELECT * FROM metric_view_v2_source""".stripMargin)
      body
    } finally {
      // The metric-view ident `mv` may have ended up as either a view (most tests) or as a
      // pre-created table (a few negative tests pre-create a table at the same ident to
      // exercise cross-type collisions). Sweep both kinds so subsequent tests in the suite
      // start from a clean catalog state. Wrap each DROP in a Try because:
      //   - DROP VIEW IF EXISTS on a leftover *table* throws WRONG_COMMAND_FOR_OBJECT_TYPE
      //     under master's new DropViewExec active-rejection contract.
      //   - DROP TABLE IF EXISTS on a leftover *view* throws the symmetric error.
      //   - On a totally clean state both are silent no-ops.
      scala.util.Try(sql(s"DROP VIEW IF EXISTS $fullMetricViewName"))
      scala.util.Try(sql(s"DROP TABLE IF EXISTS $fullMetricViewName"))
      scala.util.Try(sql(s"DROP TABLE IF EXISTS $fullSourceTableName"))
      spark.catalog.dropTempView("metric_view_v2_source")
      MetricViewRecordingCatalog.reset()
    }
  }

  private def createMetricView(
      name: String,
      metricView: MetricView,
      comment: Option[String] = None): String = {
    val yaml = MetricViewFactory.toYAML(metricView)
    val commentClause = comment.map(c => s"\nCOMMENT '$c'").getOrElse("")
    sql(
      s"""CREATE VIEW $name
         |WITH METRICS$commentClause
         |LANGUAGE YAML
         |AS
         |$$$$
         |$yaml
         |$$$$""".stripMargin)
    yaml
  }

  private def capturedViewInfo(): ViewInfo = {
    val ident = Identifier.of(Array(testNamespace), metricViewName)
    val info = MetricViewRecordingCatalog.capturedViews.get(ident)
    assert(info != null,
      s"Expected ViewInfo for $ident to be captured by the V2 catalog")
    info
  }

  // ============================================================
  // Section 1: CREATE-related tests
  // ============================================================


  test("V2 catalog receives METRIC_VIEW table type and view text via ViewInfo") {
    withTestCatalogTables {
      val metricView = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = metricViewColumns)
      val yaml = createMetricView(fullMetricViewName, metricView)

      val info = capturedViewInfo()
      // PROP_TABLE_TYPE is overwritten to METRIC_VIEW after `ViewInfo`'s constructor stamps it
      // to VIEW; this is the marker `V1Table.toCatalogTable` reads to map the round-tripped row
      // back to `CatalogTableType.METRIC_VIEW`.
      assert(info.properties().get(TableCatalog.PROP_TABLE_TYPE)
        === TableSummary.METRIC_VIEW_TABLE_TYPE)
      // The captured queryText is the raw text between `$$ ... $$` -- including the leading
      // and trailing newline our SQL fixture inserts -- so trim before comparing to the
      // pre-substitution YAML body.
      assert(info.queryText().trim === yaml.trim)

      val deps = info.viewDependencies()
      assert(deps != null)
      assert(deps.dependencies().length === 1)
      val tableDep = deps.dependencies()(0).asInstanceOf[TableDependency]
      assert(tableDep.nameParts().toSeq ===
        Seq(testCatalogName, testNamespace, sourceTableName))
    }
  }

  test("V2 catalog path populates metric_view.* + view context + sql configs on ViewInfo") {
    withTestCatalogTables {
      val metricView = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = Some("count > 0"),
        select = metricViewColumns)
      createMetricView(fullMetricViewName, metricView)

      val info = capturedViewInfo()
      val props = info.properties()

      // metric_view.* descriptive properties (mirrors the canonical metric-view property
      // layout).
      assert(props.get(MetricView.PROP_FROM_TYPE) === "ASSET")
      assert(props.get(MetricView.PROP_FROM_NAME) === fullSourceTableName)
      assert(props.get(MetricView.PROP_FROM_SQL) === null)
      assert(props.get(MetricView.PROP_WHERE) === "count > 0")

      // SQL configs and current catalog/namespace are first-class typed fields on ViewInfo, no
      // longer encoded into properties for V2 catalogs.
      assert(info.sqlConfigs().size > 0,
        s"Expected at least one captured SQL config; got ${info.sqlConfigs()}")
      assert(info.currentCatalog() ===
        spark.sessionState.catalogManager.currentCatalog.name())
      assert(info.currentNamespace().toSeq ===
        spark.sessionState.catalogManager.currentNamespace.toSeq)
    }
  }

  test("V2 catalog path captures SQL source and comment") {
    withTestCatalogTables {
      val metricView = MetricView(
        "0.1",
        SQLSource(s"SELECT * FROM $fullSourceTableName"),
        where = None,
        select = metricViewColumns)
      createMetricView(fullMetricViewName, metricView, comment = Some("my mv"))

      val info = capturedViewInfo()
      val props = info.properties()
      assert(props.get(TableCatalog.PROP_TABLE_TYPE)
        === TableSummary.METRIC_VIEW_TABLE_TYPE)
      assert(props.get(MetricView.PROP_FROM_TYPE) === "SQL")
      assert(props.get(MetricView.PROP_FROM_NAME) === null)
      assert(props.get(MetricView.PROP_FROM_SQL) ===
        s"SELECT * FROM $fullSourceTableName")
      assert(props.get(TableCatalog.PROP_COMMENT) === "my mv")

      val deps = info.viewDependencies()
      assert(deps != null && deps.dependencies().length === 1)
      val tableDep = deps.dependencies()(0).asInstanceOf[TableDependency]
      assert(tableDep.nameParts().toSeq ===
        Seq(testCatalogName, testNamespace, sourceTableName))
    }
  }

  test("metric view columns carry metric_view.type / metric_view.expr in column metadata") {
    withTestCatalogTables {
      val metricView = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = metricViewColumns)
      createMetricView(fullMetricViewName, metricView)

      val cols = capturedViewInfo().columns()
      assert(cols.length === metricViewColumns.length)

      val byName = cols.map(c => c.name() -> c).toMap
      def metadataOf(name: String): Metadata =
        Metadata.fromJson(Option(byName(name).metadataInJSON()).getOrElse("{}"))

      val regionMeta = metadataOf("region")
      assert(regionMeta.getString(Constants.COLUMN_TYPE_PROPERTY_KEY) === "dimension")
      assert(regionMeta.getString(Constants.COLUMN_EXPR_PROPERTY_KEY) === "region")

      val countMeta = metadataOf("count_sum")
      assert(countMeta.getString(Constants.COLUMN_TYPE_PROPERTY_KEY) === "measure")
      assert(countMeta.getString(Constants.COLUMN_EXPR_PROPERTY_KEY) === "sum(count)")
    }
  }

  test("user-specified column names with comments preserve metric_view.* metadata") {
    withTestCatalogTables {
      val metricView = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = metricViewColumns)
      val yaml = MetricViewFactory.toYAML(metricView)
      // Pins aliasPlan(retainMetadata = true): metric_view.* keys must survive a column
      // rename with comments.
      sql(
        s"""CREATE VIEW $fullMetricViewName (reg COMMENT 'region alias', n COMMENT 'count')
           |WITH METRICS
           |LANGUAGE YAML
           |AS
           |$$$$
           |$yaml
           |$$$$""".stripMargin)

      val cols = capturedViewInfo().columns()
      val byName = cols.map(c => c.name() -> c).toMap
      assert(byName.keySet === Set("reg", "n"))

      def metadataOf(name: String): Metadata =
        Metadata.fromJson(Option(byName(name).metadataInJSON()).getOrElse("{}"))

      val regMeta = metadataOf("reg")
      assert(regMeta.getString(Constants.COLUMN_TYPE_PROPERTY_KEY) === "dimension")
      assert(regMeta.getString(Constants.COLUMN_EXPR_PROPERTY_KEY) === "region")
      // `CatalogV2Util.structTypeToV2Columns` peels "comment" off into `Column.comment()`
      // rather than leaving it inside `metadataInJSON`; assert via the V2 column accessor.
      assert(byName("reg").comment() === "region alias")

      val nMeta = metadataOf("n")
      assert(nMeta.getString(Constants.COLUMN_TYPE_PROPERTY_KEY) === "measure")
      assert(nMeta.getString(Constants.COLUMN_EXPR_PROPERTY_KEY) === "sum(count)")
      assert(byName("n").comment() === "count")
    }
  }

  test("CREATE OR REPLACE VIEW ... WITH METRICS replaces an existing v2 metric view") {
    withTestCatalogTables {
      val first = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = Some("count > 0"),
        select = metricViewColumns)
      createMetricView(fullMetricViewName, first)

      // Replace with a new body (different WHERE clause).
      val replacement = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = Some("count > 100"),
        select = metricViewColumns)
      val replacementYaml = MetricViewFactory.toYAML(replacement)
      sql(
        s"""CREATE OR REPLACE VIEW $fullMetricViewName
           |WITH METRICS
           |LANGUAGE YAML
           |AS
           |$$$$
           |$replacementYaml
           |$$$$""".stripMargin)

      val finalInfo = capturedViewInfo()
      // Assert on the distinguishing fields of the replacement, not on diff vs. the original.
      // queryText keeps the surrounding `\n` from the SQL `$$ ... $$` markers; trim first.
      assert(finalInfo.queryText().trim === replacementYaml.trim)
      assert(finalInfo.properties().get(MetricView.PROP_WHERE) === "count > 100")
      val deps = finalInfo.viewDependencies()
      assert(deps != null && deps.dependencies().length === 1)
      val tableDep = deps.dependencies()(0).asInstanceOf[TableDependency]
      assert(tableDep.nameParts().toSeq ===
        Seq(testCatalogName, testNamespace, sourceTableName))
    }
  }

  test("CREATE VIEW IF NOT EXISTS ... WITH METRICS is a no-op when the view exists") {
    withTestCatalogTables {
      val original = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = metricViewColumns)
      createMetricView(fullMetricViewName, original)
      val originalYaml = capturedViewInfo().queryText()

      // Now CREATE VIEW IF NOT EXISTS with a different YAML body. The catalog should not see
      // the second create at all (V2ViewPreparation's `viewExists` short-circuit fires before
      // `buildViewInfo`), so the captured ViewInfo retains the original body.
      val replacement = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = Some("count > 999"),
        select = metricViewColumns)
      val replacementYaml = MetricViewFactory.toYAML(replacement)
      sql(
        s"""CREATE VIEW IF NOT EXISTS $fullMetricViewName
           |WITH METRICS
           |LANGUAGE YAML
           |AS
           |$$$$
           |$replacementYaml
           |$$$$""".stripMargin)

      assert(capturedViewInfo().queryText().trim === originalYaml.trim,
        "IF NOT EXISTS over an existing metric view should be a no-op.")
    }
  }

  test("CREATE VIEW ... WITH METRICS over a v2 table at the ident throws " +
      "EXPECT_VIEW_NOT_TABLE.NO_ALTERNATIVE") {
    withTestCatalogTables {
      // Pre-create a regular v2 table at the same ident the metric view will target. The
      // catalog's `createView` call below should raise `ViewAlreadyExistsException`, which
      // `CreateV2MetricViewExec` then decodes (via `tableExists`) into the precise cross-type
      // collision error that `CreateV2ViewExec` emits.
      sql(s"CREATE TABLE $fullMetricViewName (x INT) USING foo")

      val mv = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = metricViewColumns)
      val yaml = MetricViewFactory.toYAML(mv)
      val ex = intercept[AnalysisException] {
        sql(
          s"""CREATE VIEW $fullMetricViewName
             |WITH METRICS
             |LANGUAGE YAML
             |AS
             |$$$$
             |$yaml
             |$$$$""".stripMargin)
      }
      // SPARK-56655 added an analyzer-time pre-check for "ident already occupied by a table"
      // before the v2 view-create exec runs, so the more specific
      // `EXPECT_VIEW_NOT_TABLE.NO_ALTERNATIVE` decoded by `CreateV2MetricViewExec.run`'s catch
      // block is no longer reachable when a *plain* table sits at the ident -- the analyzer
      // raises `TABLE_OR_VIEW_ALREADY_EXISTS` first. Both errors carry the same actionable
      // signal ("can't create a view here because something else already lives at this ident").
      assert(ex.getCondition === "TABLE_OR_VIEW_ALREADY_EXISTS",
        s"Expected TABLE_OR_VIEW_ALREADY_EXISTS, got ${ex.getCondition}: ${ex.getMessage}")
    }
  }

  test("CREATE VIEW IF NOT EXISTS ... WITH METRICS is a no-op when a v2 table sits at the " +
      "ident") {
    withTestCatalogTables {
      sql(s"CREATE TABLE $fullMetricViewName (x INT) USING foo")
      val mv = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = metricViewColumns)
      val yaml = MetricViewFactory.toYAML(mv)
      // IF NOT EXISTS over a table is a no-op (v1 parity), not an error.
      sql(
        s"""CREATE VIEW IF NOT EXISTS $fullMetricViewName
           |WITH METRICS
           |LANGUAGE YAML
           |AS
           |$$$$
           |$yaml
           |$$$$""".stripMargin)
      val ident = Identifier.of(Array(testNamespace), metricViewName)
      assert(!MetricViewRecordingCatalog.capturedViews.containsKey(ident),
        "IF NOT EXISTS over a v2 table should not register a view in the catalog.")
    }
  }

  test("CREATE VIEW ... WITH METRICS on a non-ViewCatalog catalog fails with " +
      "MISSING_CATALOG_ABILITY.VIEWS") {
    val ex = intercept[AnalysisException] {
      sql(
        s"""CREATE VIEW ${MetricViewV2CatalogSuite.noViewCatalogName}.default.mv
           |WITH METRICS
           |LANGUAGE YAML
           |AS
           |$$$$
           |${MetricViewFactory.toYAML(MetricView(
              "0.1",
              AssetSource(fullSourceTableName),
              where = None,
              select = metricViewColumns))}
           |$$$$""".stripMargin)
    }
    // SPARK-56655 added the `.VIEWS` subclass; the bare `MISSING_CATALOG_ABILITY` no longer
    // surfaces directly for the missing-view-ability case.
    assert(ex.getCondition === "MISSING_CATALOG_ABILITY.VIEWS")
    assert(ex.getMessage.contains("VIEWS"))
  }

  test("CREATE VIEW ... WITH METRICS at a multi-level-namespace v2 target succeeds") {
    val deepNamespace = Array("ns_a", "ns_b")
    val deepMetricViewName = "mv_deep"
    val fullDeepName =
      s"$testCatalogName.${deepNamespace.mkString(".")}.$deepMetricViewName"
    withTestCatalogTables {
      // Pre-create the multi-level namespace + a source table inside it. The metric view
      // *target* lives in the same multi-level namespace -- that's what exercises the
      // `MetricViewHelper.analyzeMetricViewText` lift to multi-part nameParts. The pre-lift
      // code path failed at `ident.asTableIdentifier` with `requiresSinglePartNamespaceError`.
      sql(s"CREATE NAMESPACE IF NOT EXISTS $testCatalogName.${deepNamespace.head}")
      sql(s"CREATE NAMESPACE IF NOT EXISTS " +
        s"$testCatalogName.${deepNamespace.mkString(".")}")
      try {
        val mv = MetricView(
          "0.1",
          AssetSource(fullSourceTableName),
          where = None,
          select = metricViewColumns)
        val yaml = MetricViewFactory.toYAML(mv)
        sql(
          s"""CREATE VIEW $fullDeepName
             |WITH METRICS
             |LANGUAGE YAML
             |AS
             |$$$$
             |$yaml
             |$$$$""".stripMargin)

        val deepIdent = Identifier.of(deepNamespace, deepMetricViewName)
        val info = MetricViewRecordingCatalog.capturedViews.get(deepIdent)
        assert(info != null, s"Expected ViewInfo for $deepIdent to be captured")
        assert(info.properties().get(TableCatalog.PROP_TABLE_TYPE)
          === TableSummary.METRIC_VIEW_TABLE_TYPE)
      } finally {
        scala.util.Try(sql(s"DROP VIEW IF EXISTS $fullDeepName"))
        sql(s"DROP NAMESPACE IF EXISTS " +
          s"$testCatalogName.${deepNamespace.mkString(".")} CASCADE")
        sql(s"DROP NAMESPACE IF EXISTS $testCatalogName.${deepNamespace.head} CASCADE")
      }
    }
  }

  // ============================================================
  // Section 2: Dependency extraction
  // ============================================================


  test("dependency extraction: SQL source JOIN captures both tables") {
    withTestCatalogTables {
      val secondSource = s"$testCatalogName.$testNamespace.customers"
      sql(
        s"""CREATE TABLE $secondSource (id INT, name STRING)
           |USING foo""".stripMargin)
      try {
        val joinSql =
          s"SELECT c.name, t.count FROM $fullSourceTableName t " +
            s"JOIN $secondSource c ON t.count = c.id"
        val metricView = MetricView(
          "0.1",
          SQLSource(joinSql),
          where = None,
          select = Seq(
            Column("name", DimensionExpression("name"), 0),
            Column("count_sum", MeasureExpression("sum(count)"), 1)))
        createMetricView(fullMetricViewName, metricView)

        val deps = capturedViewInfo().viewDependencies()
        assert(deps != null)
        val depParts = deps.dependencies()
          .map(_.asInstanceOf[TableDependency].nameParts().toSeq).toSet
        assert(depParts === Set(
          Seq(testCatalogName, testNamespace, sourceTableName),
          Seq(testCatalogName, testNamespace, "customers")),
          s"Expected dependencies on both source tables, got $depParts")
      } finally {
        sql(s"DROP TABLE IF EXISTS $secondSource")
      }
    }
  }

  test("dependency extraction: SQL source subquery deduplicates same-table references") {
    withTestCatalogTables {
      val subquerySql =
        s"SELECT * FROM $fullSourceTableName " +
          s"WHERE count > (SELECT avg(count) FROM $fullSourceTableName)"
      val metricView = MetricView(
        "0.1",
        SQLSource(subquerySql),
        where = None,
        select = metricViewColumns)
      createMetricView(fullMetricViewName, metricView)

      val deps = capturedViewInfo().viewDependencies()
      assert(deps != null && deps.dependencies().length === 1,
        s"Expected 1 deduplicated dependency, got " +
          s"${Option(deps).map(_.dependencies().length).getOrElse(0)}")
      val tableDep = deps.dependencies()(0).asInstanceOf[TableDependency]
      assert(tableDep.nameParts().toSeq ===
        Seq(testCatalogName, testNamespace, sourceTableName))
    }
  }

  test("dependency extraction: SQL source self-join deduplicates same-table references") {
    withTestCatalogTables {
      val selfJoinSql =
        s"SELECT a.region AS a_region, a.count AS a_count " +
          s"FROM $fullSourceTableName a JOIN $fullSourceTableName b " +
          s"ON a.region = b.region"
      val metricView = MetricView(
        "0.1",
        SQLSource(selfJoinSql),
        where = None,
        select = Seq(
          Column("region", DimensionExpression("a_region"), 0),
          Column("count_sum", MeasureExpression("sum(a_count)"), 1)))
      createMetricView(fullMetricViewName, metricView)

      val deps = capturedViewInfo().viewDependencies()
      assert(deps != null && deps.dependencies().length === 1,
        s"Expected 1 deduplicated dependency for self-join, got " +
          s"${Option(deps).map(_.dependencies().length).getOrElse(0)}")
      val tableDep = deps.dependencies()(0).asInstanceOf[TableDependency]
      assert(tableDep.nameParts().toSeq ===
        Seq(testCatalogName, testNamespace, sourceTableName))
    }
  }

  test("dependency extraction: V1 session-catalog source emits 3-part nameParts") {
    val v1Source = "metric_view_v2_v1source"
    spark.range(0, 5).toDF("v")
      .write.mode("overwrite").saveAsTable(v1Source)
    try {
      withTestCatalogTables {
        val mv = MetricView(
          "0.1",
          // SQL source resolves through the current (session) catalog; the resolved
          // `LogicalRelation` carries a session-catalog `CatalogTable`.
          SQLSource(s"SELECT v AS region, v AS count FROM $v1Source"),
          where = None,
          select = metricViewColumns)
        createMetricView(fullMetricViewName, mv)

        val deps = capturedViewInfo().viewDependencies()
        assert(deps != null && deps.dependencies().length === 1)
        val parts =
          deps.dependencies()(0).asInstanceOf[TableDependency].nameParts().toSeq
        // `MetricViewHelper.qualifyV1` normalizes any `TableIdentifier.nameParts` shape
        // (1, 2, or 3 parts depending on what the analyzer captured) to the stable
        // `[spark_catalog, db, table]` shape so downstream consumers see deterministic
        // arity per source kind.
        assert(parts.length === 3,
          s"V1 nameParts should normalize to exactly 3 parts, got ${parts.length}: $parts")
        assert(parts.head === "spark_catalog",
          s"V1 nameParts head should be the session-catalog name, got $parts")
        assert(parts.last === v1Source, s"Last part should be the table name, got $parts")
      }
    } finally {
      sql(s"DROP TABLE IF EXISTS $v1Source")
    }
  }

  test("dependency extraction: multi-level V2 namespace source emits N+2 nameParts") {
    val multiNamespace = Array("ns_a", "ns_b")
    val multiTable = "events_deep"
    val multiFull = s"$testCatalogName.${multiNamespace.mkString(".")}.$multiTable"
    withTestCatalogTables {
      // The InMemoryTableCatalog (TableViewCatalog mixin) supports multi-level namespaces.
      sql(s"CREATE NAMESPACE IF NOT EXISTS $testCatalogName.${multiNamespace.head}")
      sql(s"CREATE NAMESPACE IF NOT EXISTS " +
        s"$testCatalogName.${multiNamespace.mkString(".")}")
      sql(s"CREATE TABLE $multiFull (region STRING, count INT) USING foo")
      try {
        val mv = MetricView(
          "0.1",
          SQLSource(s"SELECT region, count FROM $multiFull"),
          where = None,
          select = metricViewColumns)
        createMetricView(fullMetricViewName, mv)

        val deps = capturedViewInfo().viewDependencies()
        assert(deps != null && deps.dependencies().length === 1)
        val parts =
          deps.dependencies()(0).asInstanceOf[TableDependency].nameParts().toSeq
        assert(parts === Seq(testCatalogName, multiNamespace(0), multiNamespace(1), multiTable),
          s"Multi-level nameParts should preserve every namespace component, got $parts")
      } finally {
        sql(s"DROP TABLE IF EXISTS $multiFull")
        sql(s"DROP NAMESPACE IF EXISTS " +
          s"$testCatalogName.${multiNamespace.mkString(".")} CASCADE")
        sql(s"DROP NAMESPACE IF EXISTS $testCatalogName.${multiNamespace.head} CASCADE")
      }
    }
  }

  // ============================================================
  // Section 3: SELECT cases
  // ============================================================


  test("SELECT measure(...) from a v2 metric view returns aggregated rows") {
    withTestCatalogTables {
      val mv = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = metricViewColumns)
      createMetricView(fullMetricViewName, mv)
      // The fixture's `events` source has rows ("region_1", 1, 5.0), ("region_2", 2, 10.0).
      // The metric view aggregates by `region` summing `count`. Resolution flows through
      // loadTableOrView -> MetadataTable(ViewInfo) -> V1Table.toCatalogTable(ViewInfo) ->
      // CatalogTableType.METRIC_VIEW -> ResolveMetricView, which rewrites the view body
      // into Aggregate(Seq(region), Seq(sum(count) AS count_sum)) over `events`. The
      // `measure(...)` wrapper is required for measure columns -- selecting `count_sum`
      // bare would fail (mirrors the v1 `MetricViewSuite` query syntax).
      checkAnswer(
        sql(s"SELECT region, measure(count_sum) FROM $fullMetricViewName " +
          "GROUP BY region ORDER BY region"),
        sql(s"SELECT region, sum(count) FROM $fullSourceTableName " +
          "GROUP BY region ORDER BY region"))
    }
  }

  test("SELECT measure(...) with a WHERE clause on a dimension") {
    withTestCatalogTables {
      val mv = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = metricViewColumns)
      createMetricView(fullMetricViewName, mv)
      // Filter at the query layer (not on the metric view's own `where:`).
      checkAnswer(
        sql(s"SELECT measure(count_sum) FROM $fullMetricViewName " +
          "WHERE region = 'region_2'"),
        sql(s"SELECT sum(count) FROM $fullSourceTableName " +
          "WHERE region = 'region_2'"))
    }
  }

  test("SELECT against a v2 metric view honors the view's pre-defined where clause") {
    withTestCatalogTables {
      // Pre-define a filter on the metric view itself: only rows with count > 1 should be
      // visible to consumers (i.e. region_2 only).
      val mv = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = Some("count > 1"),
        select = metricViewColumns)
      createMetricView(fullMetricViewName, mv)
      checkAnswer(
        sql(s"SELECT region, measure(count_sum) FROM $fullMetricViewName " +
          "GROUP BY region ORDER BY region"),
        sql(s"SELECT region, sum(count) FROM $fullSourceTableName " +
          "WHERE count > 1 GROUP BY region ORDER BY region"))
    }
  }

  test("SELECT from a v2 metric view supports multiple measures with different aggregations") {
    withTestCatalogTables {
      // Add a second measure (sum of price) so we exercise the multi-measure rewrite path.
      val cols = Seq(
        Column("region", DimensionExpression("region"), 0),
        Column("count_sum", MeasureExpression("sum(count)"), 1),
        Column("price_sum", MeasureExpression("sum(price)"), 2),
        Column("price_max", MeasureExpression("max(price)"), 3))
      val mv = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = cols)
      createMetricView(fullMetricViewName, mv)
      checkAnswer(
        sql(s"SELECT measure(count_sum), measure(price_sum), measure(price_max) " +
          s"FROM $fullMetricViewName"),
        sql(s"SELECT sum(count), sum(price), max(price) FROM $fullSourceTableName"))
    }
  }

  test("SELECT from a v2 metric view supports ORDER BY and LIMIT on measures") {
    withTestCatalogTables {
      val mv = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = metricViewColumns)
      createMetricView(fullMetricViewName, mv)
      checkAnswer(
        sql(s"SELECT region, measure(count_sum) FROM $fullMetricViewName " +
          "GROUP BY region ORDER BY 2 DESC LIMIT 1"),
        sql(s"SELECT region, sum(count) FROM $fullSourceTableName " +
          "GROUP BY region ORDER BY 2 DESC LIMIT 1"))
    }
  }

  // ============================================================
  // Section 4: DESCRIBE cases
  // ============================================================


  test("DESCRIBE TABLE EXTENDED on a v2 metric view round-trips through loadTableOrView") {
    withTestCatalogTables {
      val mv = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = metricViewColumns)
      val yaml = createMetricView(fullMetricViewName, mv)

      // DESCRIBE TABLE EXTENDED resolves the ident through `Analyzer.lookupTableOrView`,
      // which calls `TableViewCatalog.loadTableOrView` once and gets back a
      // `MetadataTable(ViewInfo)`. The analyzer wraps it as a `ResolvedPersistentView` and
      // `DataSourceV2Strategy` routes through SPARK-56655's `DescribeV2ViewExec`, which
      // reads the typed `ViewInfo` directly and emits the standard "Type" / "View Text" /
      // "View Current Catalog" / "View Schema Mode" / etc. rows. Pins that `DescribeV2ViewExec`
      // emits a "Type" row for parity with v1 `CatalogTable.toJsonLinkedHashMap`, so users
      // can distinguish a plain VIEW from a sub-kind like METRIC_VIEW.
      val rows = sql(s"DESCRIBE TABLE EXTENDED $fullMetricViewName").collect()
      val rowMap = rows.map(r => r.getString(0) -> r.getString(1)).toMap

      assert(rowMap.contains("View Text"),
        s"Expected 'View Text' row in DESCRIBE EXTENDED output, got keys: ${rowMap.keys}")
      // `DescribeV2ViewExec` writes `viewInfo.queryText` directly, so trim handles the
      // leading/trailing newline the SQL `$$ ... $$` fixture inserts vs. the bare yaml body.
      assert(rowMap("View Text").trim === yaml.trim,
        s"View Text should round-trip the YAML body, got: ${rowMap("View Text")}")
      assert(rowMap.get("Type").contains(TableSummary.METRIC_VIEW_TABLE_TYPE),
        s"Type row should reflect METRIC_VIEW, got: ${rowMap.get("Type")}")
    }
  }

  test("DESCRIBE TABLE on a v2 metric view returns the aliased columns") {
    withTestCatalogTables {
      val mv = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = metricViewColumns)
      createMetricView(fullMetricViewName, mv)
      val rows = sql(s"DESCRIBE TABLE $fullMetricViewName").collect()
      val byName = rows.map(r => r.getString(0) -> r.getString(1)).toMap
      assert(byName.contains("region"), s"Missing 'region' col, got: ${byName.keys}")
      assert(byName.contains("count_sum"), s"Missing 'count_sum' col, got: ${byName.keys}")
    }
  }

  // ============================================================
  // Section 5: DROP / SHOW cases
  // ============================================================


  test("DROP VIEW succeeds on a V2 metric view") {
    withTestCatalogTables {
      val metricView = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = metricViewColumns)
      createMetricView(fullMetricViewName, metricView)
      val ident = Identifier.of(Array(testNamespace), metricViewName)

      assert(MetricViewRecordingCatalog.capturedViews.containsKey(ident))

      sql(s"DROP VIEW $fullMetricViewName")
      assert(!MetricViewRecordingCatalog.capturedViews.containsKey(ident))
    }
  }

  test("DROP TABLE on a v2 metric view throws WRONG_COMMAND_FOR_OBJECT_TYPE") {
    withTestCatalogTables {
      val mv = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = metricViewColumns)
      createMetricView(fullMetricViewName, mv)

      // SPARK-56655's `DropTableExec` actively rejects with `WRONG_COMMAND_FOR_OBJECT_TYPE`
      // ("Use DROP VIEW instead") when a view sits at the ident, replacing the prior
      // `EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE` decoding. Same actionable signal for users.
      val ex = intercept[AnalysisException] {
        sql(s"DROP TABLE $fullMetricViewName")
      }
      assert(ex.getCondition === "WRONG_COMMAND_FOR_OBJECT_TYPE",
        s"Expected WRONG_COMMAND_FOR_OBJECT_TYPE, got ${ex.getCondition}: ${ex.getMessage}")
      assert(ex.getMessage.contains("DROP VIEW"),
        s"Error message should mention 'DROP VIEW', got: ${ex.getMessage}")

      // The metric view is still present after the failed DROP TABLE.
      val ident = Identifier.of(Array(testNamespace), metricViewName)
      assert(MetricViewRecordingCatalog.capturedViews.containsKey(ident),
        "DROP TABLE on a metric view must not delete it.")
    }
  }

  test("DROP TABLE IF EXISTS on a v2 metric view also throws WRONG_COMMAND_FOR_OBJECT_TYPE") {
    withTestCatalogTables {
      val mv = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = metricViewColumns)
      createMetricView(fullMetricViewName, mv)

      // IF EXISTS does not silence the wrong-type error: the entity exists, just not as a
      // table. (Mirrors the v1 `DropTableCommand` behavior; `IF EXISTS` only short-circuits
      // the not-found branch.)
      val ex = intercept[AnalysisException] {
        sql(s"DROP TABLE IF EXISTS $fullMetricViewName")
      }
      assert(ex.getCondition === "WRONG_COMMAND_FOR_OBJECT_TYPE",
        s"Expected WRONG_COMMAND_FOR_OBJECT_TYPE, got ${ex.getCondition}: ${ex.getMessage}")
    }
  }

  test("SHOW CREATE TABLE on a v2 metric view is unsupported") {
    withTestCatalogTables {
      val mv = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = metricViewColumns)
      createMetricView(fullMetricViewName, mv)

      // SHOW CREATE TABLE on a metric view is rejected with the dedicated
      // UNSUPPORTED_SHOW_CREATE_TABLE.ON_METRIC_VIEW error class (same one the v1 path uses
      // in `tables.scala`'s `ShowCreateTableCommand`), so the message is identical no matter
      // which catalog kind owns the view. There's no round-trippable
      // `CREATE VIEW ... WITH METRICS` form yet, so explicit "unsupported" is the right
      // answer rather than emitting a misleading plain `CREATE VIEW ...`.
      val ex = intercept[AnalysisException] {
        sql(s"SHOW CREATE TABLE $fullMetricViewName")
      }
      assert(ex.getCondition === "UNSUPPORTED_SHOW_CREATE_TABLE.ON_METRIC_VIEW",
        s"Expected UNSUPPORTED_SHOW_CREATE_TABLE.ON_METRIC_VIEW, got " +
          s"${ex.getCondition}: ${ex.getMessage}")
      assert(ex.getMessage.contains("metric view"),
        s"Error message should mention 'metric view', got: ${ex.getMessage}")
    }
  }

  test("DROP VIEW IF EXISTS on a non-existent V2 metric view is a no-op") {
    withTestCatalogTables {
      sql(s"DROP VIEW IF EXISTS $testCatalogName.$testNamespace.does_not_exist")
    }
  }

  test("ALTER VIEW <metric_view> RENAME TO ... succeeds and preserves metric view metadata") {
    withTestCatalogTables {
      val mv = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = metricViewColumns)
      createMetricView(fullMetricViewName, mv)
      // Per upstream DataSourceV2SQLSuite convention (see lines 2477 / 2484 there), the
      // RENAME TO clause takes a 2-part `namespace.name` -- the new ident is implicitly
      // within the same catalog as the source view. Including a 3-part `catalog.ns.name`
      // would leak the catalog component into `newName.asIdentifier` and the catalog's
      // `renameView` would store under a key the loader can't find.
      val renamedRelative = s"$testNamespace.mv_renamed"
      val renamedFull = s"$testCatalogName.$renamedRelative"
      try {
        // RenameTable on a `ResolvedPersistentView` is routed by `DataSourceV2Strategy` to
        // `RenameV2ViewExec`, which calls `ViewCatalog.renameView` -- the fixture
        // `MetricViewRecordingCatalog.renameView` relocates both the `views` entry and the
        // `capturedViews` entry under the new ident. Pin the wiring end-to-end so the
        // metric view kind survives the rename.
        sql(s"ALTER VIEW $fullMetricViewName RENAME TO $renamedRelative")

        // Old ident is gone from the v2 catalog -- DESCRIBE should fail to resolve.
        val oldEx = intercept[AnalysisException] {
          sql(s"DESCRIBE TABLE $fullMetricViewName").collect()
        }
        assert(oldEx.getCondition === "TABLE_OR_VIEW_NOT_FOUND",
          s"Expected TABLE_OR_VIEW_NOT_FOUND for the old ident, got " +
            s"${oldEx.getCondition}: ${oldEx.getMessage}")

        // New ident loads through `TableViewCatalog.loadTableOrView` and surfaces the same
        // metric-view kind on `DESCRIBE TABLE EXTENDED`.
        val rows = sql(s"DESCRIBE TABLE EXTENDED $renamedFull").collect()
        val rowMap = rows.map(r => r.getString(0) -> r.getString(1)).toMap
        assert(rowMap.get("Type").contains(TableSummary.METRIC_VIEW_TABLE_TYPE),
          s"Renamed view should still be a METRIC_VIEW, got Type=${rowMap.get("Type")}")
      } finally {
        sql(s"DROP VIEW IF EXISTS $renamedFull")
      }
    }
  }

  test("SHOW TABLES on a v2 TableViewCatalog lists both tables and metric views") {
    withTestCatalogTables {
      val mv = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = metricViewColumns)
      createMetricView(fullMetricViewName, mv)
      val tables = sql(s"SHOW TABLES IN $testCatalogName.$testNamespace")
        .collect().map(_.getString(1)).toSet
      // SPARK-56655 routes SHOW TABLES on a `TableViewCatalog` through `listRelationSummaries`
      // so views appear alongside tables in the output (matching v1 SHOW TABLES on a session
      // catalog). Pure `TableCatalog` catalogs continue to return tables only.
      assert(tables.contains(sourceTableName),
        s"SHOW TABLES should list the source table, got: $tables")
      assert(tables.contains(metricViewName),
        s"SHOW TABLES on a TableViewCatalog should also list metric views, got: $tables")
    }
  }

  test("SHOW VIEWS lists v2 metric views") {
    withTestCatalogTables {
      val mv = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = metricViewColumns)
      createMetricView(fullMetricViewName, mv)
      val views = sql(s"SHOW VIEWS IN $testCatalogName.$testNamespace")
        .collect().map(_.getString(1)).toSet
      assert(views.contains(metricViewName),
        s"SHOW VIEWS should list metric views, got: $views")
    }
  }
}

object MetricViewV2CatalogSuite {
  val noViewCatalogName: String = "testcat_no_view"
}

/**
 * Minimal [[TableViewCatalog]] used by [[MetricViewV2CatalogSuite]]. Layers `ViewCatalog`
 * methods over [[InMemoryTableCatalog]] (which provides table storage + namespace ops) and
 * captures every [[ViewInfo]] passed to `createView` so tests can inspect the typed payload.
 *
 * The metric-view CREATE path goes via `ViewCatalog.createView`, so the captured map keys are
 * the view identifiers; the source table created by the test fixture is stored separately in
 * the inherited table catalog.
 */
class MetricViewRecordingCatalog extends InMemoryTableCatalog with TableViewCatalog {
  private val views =
    new ConcurrentHashMap[(Seq[String], String), ViewInfo]()

  // -- ViewCatalog methods --

  override def listViews(namespace: Array[String]): Array[Identifier] = {
    val target = namespace.toSeq
    val out = new java.util.ArrayList[Identifier]()
    views.forEach { (key, _) =>
      if (key._1 == target) out.add(Identifier.of(key._1.toArray, key._2))
    }
    out.asScala.toArray
  }

  // `loadView`, `tableExists`, and `viewExists` are inherited from `TableViewCatalog`'s
  // defaults, which derive from `loadTableOrView` -- a stored `ViewInfo` is wrapped in
  // `MetadataTable` by `loadTableOrView` and the defaults unwrap it correctly.

  // Bypasses `TableViewCatalog.tableExists` (whose default delegates to `loadTableOrView`,
  // which checks our `views` map first); we want a tables-only check here so the cross-type
  // collision branches in `createView` / `replaceView` see only "is there a *table* at this
  // ident?".
  private def tableExistsTablesOnly(ident: Identifier): Boolean =
    try { super[InMemoryTableCatalog].loadTable(ident); true }
    catch { case _: org.apache.spark.sql.catalyst.analysis.NoSuchTableException => false }

  override def createView(ident: Identifier, info: ViewInfo): ViewInfo = {
    // TableViewCatalog active-rejection contract: createView must throw
    // ViewAlreadyExistsException when *either* a view *or* a table sits at the ident.
    if (tableExistsTablesOnly(ident)) {
      throw new ViewAlreadyExistsException(ident)
    }
    val key = (ident.namespace().toSeq, ident.name())
    if (views.putIfAbsent(key, info) != null) {
      throw new ViewAlreadyExistsException(ident)
    }
    MetricViewRecordingCatalog.capturedViews.put(ident, info)
    info
  }

  override def replaceView(ident: Identifier, info: ViewInfo): ViewInfo = {
    // Per the TableViewCatalog contract, replaceView must surface NoSuchViewException
    // when a *table* sits at the ident (not silently succeed and shadow the table).
    if (tableExistsTablesOnly(ident)) throw new NoSuchViewException(ident)
    val key = (ident.namespace().toSeq, ident.name())
    if (!views.containsKey(key)) throw new NoSuchViewException(ident)
    views.put(key, info)
    MetricViewRecordingCatalog.capturedViews.put(ident, info)
    info
  }

  override def dropView(ident: Identifier): Boolean = {
    val key = (ident.namespace().toSeq, ident.name())
    val removed = views.remove(key) != null
    if (removed) {
      MetricViewRecordingCatalog.capturedViews.remove(ident)
    }
    removed
  }

  override def renameView(oldIdent: Identifier, newIdent: Identifier): Unit = {
    val oldKey = (oldIdent.namespace().toSeq, oldIdent.name())
    val newKey = (newIdent.namespace().toSeq, newIdent.name())
    val existing = views.get(oldKey)
    if (existing == null) throw new NoSuchViewException(oldIdent)
    if (views.putIfAbsent(newKey, existing) != null) {
      throw new ViewAlreadyExistsException(newIdent)
    }
    views.remove(oldKey)
    val captured = MetricViewRecordingCatalog.capturedViews.remove(oldIdent)
    if (captured != null) {
      MetricViewRecordingCatalog.capturedViews.put(newIdent, captured)
    }
  }

  // -- TableViewCatalog single-RPC perf path --

  override def loadTableOrView(ident: Identifier): Table = {
    val key = (ident.namespace().toSeq, ident.name())
    Option(views.get(key)) match {
      case Some(info) => new MetadataTable(info, ident.toString)
      // Bypass `TableViewCatalog.loadTable` (whose default delegates back to `loadTableOrView`)
      // and call `InMemoryTableCatalog.loadTable` directly to avoid infinite recursion.
      case None => super[InMemoryTableCatalog].loadTable(ident)
    }
  }
}

object MetricViewRecordingCatalog {
  // Captures every ViewInfo that flows through createView / replaceView so individual tests
  // can assert on it. Cleared between tests via `reset()`.
  val capturedViews: ConcurrentHashMap[Identifier, ViewInfo] =
    new ConcurrentHashMap[Identifier, ViewInfo]()

  def reset(): Unit = capturedViews.clear()
}
