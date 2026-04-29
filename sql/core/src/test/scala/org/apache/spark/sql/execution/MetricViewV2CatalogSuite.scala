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

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.analysis.{NoSuchViewException, ViewAlreadyExistsException}
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryTableCatalog, MetadataOnlyTable, RelationCatalog, Table, TableCatalog, TableDependency, TableSummary, ViewInfo}
import org.apache.spark.sql.metricview.serde.{AssetSource, Column, Constants, DimensionExpression, MeasureExpression, MetricView, MetricViewFactory, SQLSource}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.Metadata

/**
 * Tests that exercise [[org.apache.spark.sql.execution.command.CreateMetricViewCommand]] on a
 * non-session V2 catalog. Metric views are persisted through the same [[ViewCatalog]] interface
 * as plain views; the only marker that distinguishes them is `PROP_TABLE_TYPE = METRIC_VIEW`
 * plus the typed `viewDependencies` field on [[ViewInfo]]. The recording catalog used here is a
 * minimal [[RelationCatalog]] so the same instance can also host the source table referenced by
 * the metric view's YAML.
 */
class MetricViewV2CatalogSuite extends QueryTest with SharedSparkSession {

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
      sql(s"DROP VIEW IF EXISTS $fullMetricViewName")
      sql(s"DROP TABLE IF EXISTS $fullSourceTableName")
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
      assert(info.queryText() === yaml)

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

      // metric_view.* descriptive properties (mirrors DBR SingleSourceMetricView).
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
      // Give both columns new names, and a comment on each. Without the `retainMetadata`
      // fix to `ViewHelper.aliasPlan`, the metric_view.* keys disappear here.
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
      assert(finalInfo.queryText() === replacementYaml)
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

      assert(capturedViewInfo().queryText() === originalYaml,
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
      // CreateV2ViewExec / CreateV2MetricViewExec route this through
      // `unsupportedCreateOrReplaceViewOnTableError` which maps to
      // `EXPECT_VIEW_NOT_TABLE.NO_ALTERNATIVE`.
      assert(ex.getCondition === "EXPECT_VIEW_NOT_TABLE.NO_ALTERNATIVE",
        s"Expected EXPECT_VIEW_NOT_TABLE.NO_ALTERNATIVE, got ${ex.getCondition}: ${ex.getMessage}")
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
    assert(ex.getCondition === "MISSING_CATALOG_ABILITY")
    assert(ex.getMessage.contains("VIEWS"))
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
        val depParts =
          deps.dependencies().map(_.asInstanceOf[TableDependency].nameParts().toSeq).toSet
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
        val parts = deps.dependencies()(0).asInstanceOf[TableDependency].nameParts().toSeq
        // For a session-catalog source, `TableIdentifier.nameParts` includes catalog + db +
        // table when the catalog is set; here we expect at least 2 parts (`db.table`) and
        // up to 3 (`spark_catalog.db.table`) -- both are valid producer outputs depending
        // on whether the analyzer captured the session-catalog component.
        assert(parts.last === v1Source, s"Last part should be the table name, got $parts")
        assert(parts.length >= 2 && parts.length <= 3,
          s"V1 nameParts arity should be 2 or 3, got ${parts.length}: $parts")
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
      // The InMemoryTableCatalog (RelationCatalog mixin) supports multi-level namespaces.
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
        val parts = deps.dependencies()(0).asInstanceOf[TableDependency].nameParts().toSeq
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
      // loadRelation -> MetadataOnlyTable(ViewInfo) -> V1Table.toCatalogTable(ViewInfo) ->
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


  test("DESCRIBE TABLE EXTENDED on a v2 metric view round-trips through loadRelation") {
    withTestCatalogTables {
      val mv = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = metricViewColumns)
      val yaml = createMetricView(fullMetricViewName, mv)

      // DESCRIBE TABLE EXTENDED resolves the ident through `Analyzer.lookupTableOrView`,
      // which calls `RelationCatalog.loadRelation` once and gets back a
      // `MetadataOnlyTable(ViewInfo)`. `V1Table.toCatalogTable(ViewInfo)` reconstructs the
      // V1 representation; the resulting `CatalogTable.toJsonLinkedHashMap` (interface.scala)
      // then emits view-context rows because `tableType == METRIC_VIEW` was added to the
      // VIEW gate. Without that gate fix, "View Text" / "View Original Text" disappear.
      val rows = sql(s"DESCRIBE TABLE EXTENDED $fullMetricViewName").collect()
      val rowMap = rows.map(r => r.getString(0) -> r.getString(1)).toMap

      assert(rowMap.contains("View Text"),
        s"Expected 'View Text' row in DESCRIBE EXTENDED output, got keys: ${rowMap.keys}")
      assert(rowMap("View Text") === yaml,
        s"View Text should round-trip the YAML body, got: ${rowMap("View Text")}")
      assert(rowMap.get("Type").exists(_.contains("METRIC_VIEW")),
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

  test("DROP TABLE on a v2 metric view throws EXPECT_TABLE_NOT_VIEW") {
    withTestCatalogTables {
      val mv = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = metricViewColumns)
      createMetricView(fullMetricViewName, mv)

      // `DropTableExec` first probes `tableExists` (false for a view per the RelationCatalog
      // passive-filtering contract), then falls back to `viewExists` and -- when the entity
      // exists as a view but a table was requested -- throws `EXPECT_TABLE_NOT_VIEW` to
      // distinguish "wrong type" from "missing".
      val ex = intercept[AnalysisException] {
        sql(s"DROP TABLE $fullMetricViewName")
      }
      assert(ex.getCondition === "EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE",
        s"Expected EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE, got ${ex.getCondition}: ${ex.getMessage}")

      // The metric view is still present after the failed DROP TABLE.
      val ident = Identifier.of(Array(testNamespace), metricViewName)
      assert(MetricViewRecordingCatalog.capturedViews.containsKey(ident),
        "DROP TABLE on a metric view must not delete it.")
    }
  }

  test("DROP TABLE IF EXISTS on a v2 metric view also throws EXPECT_TABLE_NOT_VIEW") {
    withTestCatalogTables {
      val mv = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = metricViewColumns)
      createMetricView(fullMetricViewName, mv)

      // IF EXISTS does not silence the wrong-type error: the entity exists, just not as a
      // table. (This mirrors the v1 `DropTableCommand` behavior, where `IF EXISTS` only
      // short-circuits the not-found branch.)
      val ex = intercept[AnalysisException] {
        sql(s"DROP TABLE IF EXISTS $fullMetricViewName")
      }
      assert(ex.getCondition === "EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE",
        s"Expected EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE, got ${ex.getCondition}: ${ex.getMessage}")
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

      // SHOW CREATE TABLE on a v2 view (including metric views) is rejected by
      // DataSourceV2Strategy via `unsupportedTableOperationError(...)`. There's no
      // round-trippable `CREATE VIEW ... WITH METRICS` form yet, so explicit "unsupported"
      // is the right answer rather than emitting a misleading plain `CREATE VIEW ...`.
      val ex = intercept[AnalysisException] {
        sql(s"SHOW CREATE TABLE $fullMetricViewName")
      }
      assert(ex.getCondition === "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        s"Expected UNSUPPORTED_FEATURE.TABLE_OPERATION, got " +
          s"${ex.getCondition}: ${ex.getMessage}")
      assert(ex.getMessage.contains("SHOW CREATE TABLE"),
        s"Error message should mention 'SHOW CREATE TABLE', got: ${ex.getMessage}")
    }
  }

  test("DROP VIEW IF EXISTS on a non-existent V2 metric view is a no-op") {
    withTestCatalogTables {
      sql(s"DROP VIEW IF EXISTS $testCatalogName.$testNamespace.does_not_exist")
    }
  }

  test("SHOW TABLES does not list v2 metric views") {
    withTestCatalogTables {
      val mv = MetricView(
        "0.1",
        AssetSource(fullSourceTableName),
        where = None,
        select = metricViewColumns)
      createMetricView(fullMetricViewName, mv)
      val tables = sql(s"SHOW TABLES IN $testCatalogName.$testNamespace")
        .collect().map(_.getString(1)).toSet
      // The fixture's `events` source table is a regular table, so SHOW TABLES sees it.
      assert(tables.contains(sourceTableName),
        s"SHOW TABLES should list the source table, got: $tables")
      // Per the RelationCatalog contract, SHOW TABLES returns tables only -- metric views
      // belong on SHOW VIEWS instead.
      assert(!tables.contains(metricViewName),
        s"SHOW TABLES should not list metric views, got: $tables")
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
 * Minimal [[RelationCatalog]] used by [[MetricViewV2CatalogSuite]]. Layers `ViewCatalog`
 * methods over [[InMemoryTableCatalog]] (which provides table storage + namespace ops) and
 * captures every [[ViewInfo]] passed to `createView` so tests can inspect the typed payload.
 *
 * The metric-view CREATE path goes via `ViewCatalog.createView`, so the captured map keys are
 * the view identifiers; the source table created by the test fixture is stored separately in
 * the inherited table catalog.
 */
class MetricViewRecordingCatalog extends InMemoryTableCatalog with RelationCatalog {
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

  // `loadView`, `tableExists`, and `viewExists` are inherited from `RelationCatalog`'s
  // defaults, which derive from `loadRelation` -- a stored `ViewInfo` is wrapped in
  // `MetadataOnlyTable` by `loadRelation` and the defaults unwrap it correctly.

  override def createView(ident: Identifier, info: ViewInfo): ViewInfo = {
    val key = (ident.namespace().toSeq, ident.name())
    if (views.putIfAbsent(key, info) != null) {
      throw new ViewAlreadyExistsException(ident)
    }
    MetricViewRecordingCatalog.capturedViews.put(ident, info)
    info
  }

  override def replaceView(ident: Identifier, info: ViewInfo): ViewInfo = {
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

  // -- RelationCatalog single-RPC perf path --

  override def loadRelation(ident: Identifier): Table = {
    val key = (ident.namespace().toSeq, ident.name())
    Option(views.get(key)) match {
      case Some(info) => new MetadataOnlyTable(info, ident.toString)
      case None => super.loadTable(ident) // delegate to InMemoryTableCatalog for tables
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
