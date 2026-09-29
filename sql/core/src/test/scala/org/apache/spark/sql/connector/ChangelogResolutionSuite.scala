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

import java.util.Collections

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ResolveChangelogTable
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Window}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.ChangelogRange
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference, Transform}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.execution.datasources.v2.{ChangelogTable, DataSourceV2Relation, V2TableRefreshUtil}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, MapType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

/**
 * Tests for the CDC (Change Data Capture) analyzer resolution path:
 * RelationChanges -> resolveChangelog -> DataSourceV2Relation(ChangelogTable).
 */
class ChangelogResolutionSuite extends SharedSparkSession {

  private val cdcCatalogName = "cdc_catalog"
  private val noCdcCatalogName = "no_cdc_catalog"
  private val ident = Identifier.of(Array.empty, "test_table")

  private def cdcCatalog: ChangelogStateOptionsCatalog = {
    spark.sessionState.catalogManager.catalog(cdcCatalogName)
      .asInstanceOf[ChangelogStateOptionsCatalog]
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(s"spark.sql.catalog.$cdcCatalogName",
      classOf[ChangelogStateOptionsCatalog].getName)
    spark.conf.set(s"spark.sql.catalog.$cdcCatalogName.tableStateOptionKeys", "branch")
    spark.conf.set(s"spark.sql.catalog.$noCdcCatalogName",
      classOf[InMemoryTableCatalog].getName)
  }

  override def afterAll(): Unit = {
    spark.conf.unset(s"spark.sql.catalog.$cdcCatalogName")
    spark.conf.unset(s"spark.sql.catalog.$cdcCatalogName.tableStateOptionKeys")
    spark.conf.unset(s"spark.sql.catalog.$noCdcCatalogName")
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    cdcCatalog.changelogKeys = None
    cdcCatalog.rejectCurrentTableLoads = false
    cdcCatalog.clearChangeRows(ident)
    cdcCatalog.setChangelogProperties(ident, ChangelogProperties())
    val catalog = spark.sessionState.catalogManager.catalog(cdcCatalogName).asTableCatalog
    if (catalog.tableExists(ident)) {
      catalog.dropTable(ident)
    }
    catalog.createTable(
      ident,
      Array(
        Column.create("id", LongType),
        Column.create("data", StringType)),
      Array.empty[Transform],
      Collections.emptyMap[String, String]())

    val noCdcCat = spark.sessionState.catalogManager.catalog(noCdcCatalogName).asTableCatalog
    val ident2 = Identifier.of(Array.empty, "test_table")
    if (noCdcCat.tableExists(ident2)) {
      noCdcCat.dropTable(ident2)
    }
    noCdcCat.createTable(
      ident2,
      Array(
        Column.create("id", LongType),
        Column.create("data", StringType)),
      Array.empty[Transform],
      Collections.emptyMap[String, String]())
  }

  test("CHANGES clause resolves to DataSourceV2Relation with ChangelogTable") {
    val df = sql(
      s"SELECT * FROM $cdcCatalogName.test_table CHANGES FROM VERSION 1 TO VERSION 5")
    val analyzed = df.queryExecution.analyzed
    val dsv2Relations = analyzed.collect {
      case r: DataSourceV2Relation => r
    }
    assert(dsv2Relations.length == 1)
    assert(dsv2Relations.head.table.isInstanceOf[ChangelogTable])
    val changelogTable = dsv2Relations.head.table.asInstanceOf[ChangelogTable]
    assert(changelogTable.name().endsWith("test_table_changelog"))
  }

  test("CHANGES clause - catalog without loadChangelog throws") {
    checkError(
      intercept[AnalysisException] {
        sql(s"SELECT * FROM $noCdcCatalogName.test_table CHANGES FROM VERSION 1 TO VERSION 5")
      },
      condition = "UNSUPPORTED_FEATURE.CHANGE_DATA_CAPTURE",
      parameters = Map("catalogName" -> noCdcCatalogName))
  }

  test("CHANGES clause - table not found throws") {
    val e = intercept[AnalysisException] {
      sql(s"SELECT * FROM $cdcCatalogName.nonexistent CHANGES FROM VERSION 1 TO VERSION 5")
    }
    assert(e.getMessage.contains("TABLE_OR_VIEW_NOT_FOUND") ||
      e.getMessage.contains("nonexistent"))
  }

  test("DataFrame API - changes() resolves correctly") {
    val df = spark.read
      .option("startingVersion", "1")
      .option("endingVersion", "5")
      .changes(s"$cdcCatalogName.test_table")
    val analyzed = df.queryExecution.analyzed
    val dsv2Relations = analyzed.collect {
      case r: DataSourceV2Relation => r
    }
    assert(dsv2Relations.length == 1)
    assert(dsv2Relations.head.table.isInstanceOf[ChangelogTable])
  }

  test("DataFrame API - changes() on catalog without CDC throws") {
    checkError(
      intercept[AnalysisException] {
        spark.read
          .option("startingVersion", "1")
          .changes(s"$noCdcCatalogName.test_table")
      },
      condition = "UNSUPPORTED_FEATURE.CHANGE_DATA_CAPTURE",
      parameters = Map("catalogName" -> noCdcCatalogName))
  }

  test("CHANGES clause - schema includes CDC metadata columns") {
    val df = sql(
      s"SELECT * FROM $cdcCatalogName.test_table CHANGES FROM VERSION 1 TO VERSION 5")
    val colNames = df.schema.fieldNames
    assert(colNames.contains("id"))
    assert(colNames.contains("data"))
    assert(colNames.contains("_change_type"))
    assert(colNames.contains("_commit_version"))
    assert(colNames.contains("_commit_timestamp"))
  }

  test("DataStreamReader - changes() rejects user-specified schema") {
    val e = intercept[AnalysisException] {
      import org.apache.spark.sql.types.StructType
      spark.readStream
        .schema(new StructType().add("id", LongType))
        .changes(s"$cdcCatalogName.test_table")
    }
    assert(e.getMessage.contains("changes"))
  }

  test("DataStreamReader - changes() resolves to StreamingRelationV2 with ChangelogTable") {
    val df = spark.readStream
      .option("startingVersion", "1")
      .changes(s"$cdcCatalogName.test_table")
    val analyzed = df.queryExecution.analyzed
    val streamRelations = analyzed.collect {
      case r: StreamingRelationV2 => r
    }
    assert(streamRelations.length == 1)
    assert(streamRelations.head.table.isInstanceOf[ChangelogTable])
    val colNames = df.schema.fieldNames
    assert(colNames.contains("_change_type"))
    assert(colNames.contains("_commit_version"))
    assert(colNames.contains("_commit_timestamp"))
  }

  test("DataStreamReader - changes() on catalog without CDC throws") {
    checkError(
      intercept[AnalysisException] {
        spark.readStream
          .option("startingVersion", "1")
          .changes(s"$noCdcCatalogName.test_table")
      },
      condition = "UNSUPPORTED_FEATURE.CHANGE_DATA_CAPTURE",
      parameters = Map("catalogName" -> noCdcCatalogName))
  }

  test("CHANGES clause on CTE relation throws") {
    checkError(
      intercept[AnalysisException] {
        sql("WITH x AS (SELECT 1) SELECT * FROM x CHANGES FROM VERSION 1 TO VERSION 5")
      },
      condition = "UNSUPPORTED_FEATURE.CHANGE_DATA_CAPTURE_ON_RELATION",
      sqlState = None,
      parameters = Map("relationId" -> "`x`"))
  }

  test("CHANGES clause passes changelogContext to catalog") {
    sql(s"SELECT * FROM $cdcCatalogName.test_table CHANGES FROM VERSION 1 TO VERSION 5")
    val cat = spark.sessionState.catalogManager
      .catalog(cdcCatalogName)
      .asInstanceOf[InMemoryChangelogCatalog]
    val info = cat.lastChangelogContext
    assert(info.isDefined)
    val range = info.get.range().asInstanceOf[ChangelogRange.VersionRange]
    assert(range.startingVersion() == "1")
    assert(range.endingVersion().get() == "5")
  }

  gridTest("CDC resolution filters state options and retains scan options")(
      for (streaming <- Seq(false, true); useSql <- Seq(false, true)) yield (streaming, useSql)) {
    case (streaming, useSql) =>
      val cat = cdcCatalog
      cat.resetLoadChangelogCalls()
      val tableName = s"$cdcCatalogName.test_table"
      val df = if (useSql) {
        val prefix = if (streaming) "STREAM " else ""
        sql(s"SELECT * FROM $prefix$tableName CHANGES FROM VERSION 1 " +
          "WITH ('BrAnCh' = 'Main', 'customOption' = 'customValue')")
      } else if (streaming) {
        spark.readStream.option("startingVersion", "1").option("BrAnCh", "Main")
          .option("customOption", "customValue").changes(tableName)
      } else {
        spark.read.option("startingVersion", "1").option("BrAnCh", "Main")
          .option("customOption", "customValue").changes(tableName)
      }
      val options = df.queryExecution.analyzed.collectFirst {
        case r: DataSourceV2Relation => r.options
        case r: StreamingRelationV2 => r.extraOptions
      }.get
      assert(cat.loadChangelogCalls.size == 1)
      assert(cat.lastOptions.get.size() == 1)
      assert(cat.lastOptions.get.get("branch") == "Main")
      assert(options.get("branch") == "Main")
      assert(options.get("customOption") == "customValue")
      if (!useSql) assert(options.get("startingVersion") == "1")
      if (!streaming) {
        df.queryExecution.optimizedPlan
        assert(cat.lastScanOptions.contains(options))
      }
  }

  gridTest("CDC state option keys can inherit, override, or disable table state keys")(
      Seq("default", "override", "empty")) { keyMode =>
    val cat = cdcCatalog
    cat.changelogKeys = keyMode match {
      case "override" => Some(java.util.Set.of("ReF"))
      case "empty" => Some(java.util.Set.of[String]())
      case _ => None
    }
    spark.read.option("startingVersion", "1").option("BRANCH", "Main")
      .option("ref", "Dev").option("scanOption", "10")
      .changes(s"$cdcCatalogName.test_table").queryExecution.analyzed

    val expected = keyMode match {
      case "default" => new CaseInsensitiveStringMap(Collections.singletonMap("branch", "Main"))
      case "override" => new CaseInsensitiveStringMap(Collections.singletonMap("ref", "Dev"))
      case _ => CaseInsensitiveStringMap.empty()
    }
    assert(cat.lastOptions.contains(expected))
  }

  private def changeRow(id: Long, version: Long): InternalRow = InternalRow(
    id, UTF8String.fromString(s"data-$id"), UTF8String.fromString(Changelog.CHANGE_TYPE_INSERT),
    version, version * 1000000L)

  test("CDC metadata loads are shared across scan options but isolate contexts and state values") {
    val cat = cdcCatalog
    cat.addChangeRows(ident, Seq(changeRow(1L, 1L), changeRow(2L, 2L)))
    cat.resetLoadChangelogCalls()
    val tableName = s"$cdcCatalogName.test_table"
    val df = sql(
      s"SELECT id FROM $tableName CHANGES FROM VERSION 1 TO VERSION 1 " +
        "WITH ('BrAnCh' = 'Main', 'split-size' = '5') UNION ALL " +
        s"SELECT id FROM $tableName CHANGES FROM VERSION 1 TO VERSION 1 " +
        "WITH ('branch' = 'Main', 'split-size' = '9') UNION ALL " +
        s"SELECT id FROM $tableName CHANGES FROM VERSION 2 TO VERSION 2 " +
        "WITH ('branch' = 'Main') UNION ALL " +
        s"SELECT id FROM $tableName CHANGES FROM VERSION 1 TO VERSION 1 " +
        "WITH ('branch' = 'main') UNION ALL " +
        s"SELECT id FROM $tableName CHANGES FROM VERSION 1 TO VERSION 1 " +
        "WITH ('branch' = 'Main', 'deduplicationMode' = 'none')")
    val relations = df.queryExecution.analyzed.collect { case r: DataSourceV2Relation => r }
    val changelogs = relations.map(_.table.asInstanceOf[ChangelogTable])
    assert(changelogs.head.changelog eq changelogs(1).changelog)
    assert(changelogs.head.changelog ne changelogs(2).changelog)
    assert(changelogs.head.changelog ne changelogs(3).changelog)
    assert(changelogs.head.changelog ne changelogs(4).changelog)
    assert(relations.take(2).map(_.options.get("split-size")) == Seq("5", "9"))
    assert(cat.loadChangelogCalls.size == 4)
    QueryTest.checkAnswer(
      df, Seq(Row(1L), Row(1L), Row(1L), Row(1L), Row(2L)), checkToRDD = false)

    cat.resetLoadChangelogCalls()
    cat.resetLoadTableCalls()
    val refreshed = V2TableRefreshUtil.refresh(spark, df.queryExecution.analyzed)
    assert(cat.loadChangelogCalls.size == 4)
    assert(cat.loadTableCalls.isEmpty)
    val refreshedChanges = refreshed.collect {
      case r: DataSourceV2Relation => r.table.asInstanceOf[ChangelogTable]
    }
    assert(refreshedChanges.head.changelog eq refreshedChanges(1).changelog)
    assert(refreshedChanges.map(_.changelogContext) == changelogs.map(_.changelogContext))
    assert(refreshedChanges.forall(_.resolved))
  }

  test("ordinary and CDC metadata caches remain separate") {
    val tableName = s"$cdcCatalogName.test_table"
    sql(s"INSERT INTO $tableName VALUES (100, 'current')")
    val cat = cdcCatalog
    cat.addChangeRows(ident, Seq(changeRow(1L, 1L)))
    cat.resetLoadTableCalls()
    cat.resetLoadChangelogCalls()
    val df = sql(s"SELECT id FROM $tableName UNION ALL " +
      s"SELECT id FROM $tableName CHANGES FROM VERSION 1 TO VERSION 10")
    val relations = df.queryExecution.analyzed.collect { case r: DataSourceV2Relation => r }
    assert(relations.count(_.table.isInstanceOf[ChangelogTable]) == 1)
    assert(cat.loadTableCalls.size == 1)
    assert(cat.loadChangelogCalls.size == 1)
    QueryTest.checkAnswer(df, Seq(Row(100L), Row(1L)), checkToRDD = false)
  }

  test("refreshing cached CDC data preserves CDC reads and ordinary table cache isolation") {
    val tableName = s"$cdcCatalogName.test_table"
    val cdcQuery = s"SELECT * FROM $tableName CHANGES FROM VERSION 1 TO VERSION 10"
    sql(s"INSERT INTO $tableName VALUES (100, 'current')")
    val cat = cdcCatalog
    cat.addChangeRows(ident, Seq(changeRow(1L, 1L)))
    val cached = sql(cdcQuery).cache()
    val cacheManager = spark.sharedState.cacheManager
    try {
      checkAnswer(cached.select("id"), Seq(Row(1L)))
      assert(cacheManager.lookupCachedData(spark.table(tableName)).isEmpty)
      checkAnswer(spark.table(tableName).select("id"), Seq(Row(100L)))

      cat.addChangeRows(ident, Seq(changeRow(2L, 2L)))
      spark.catalog.refreshTable(tableName)

      assert(cacheManager.numCachedEntries == 1)
      assert(cacheManager.lookupCachedData(spark.table(tableName)).isEmpty)
      checkAnswer(spark.table(tableName).select("id"), Seq(Row(100L)))

      assert(cacheManager.lookupCachedData(cached).isDefined)
      checkAnswer(cached.select("id"), Seq(Row(1L), Row(2L)))

      val fresh = sql(cdcQuery)
      assert(cacheManager.lookupCachedData(fresh).isDefined)
      checkAnswer(fresh.select("id"), Seq(Row(1L), Row(2L)))

      val differentRange = sql(
        s"SELECT * FROM $tableName CHANGES FROM VERSION 2 TO VERSION 10")
      assert(cacheManager.lookupCachedData(differentRange).isEmpty)
    } finally {
      spark.catalog.clearCache()
    }
  }

  test("bounded CDC reads do not load the latest ordinary table") {
    val cat = cdcCatalog
    cat.addChangeRows(ident, Seq(changeRow(1L, 1L), changeRow(10L, 10L), changeRow(100L, 100L)))
    cat.rejectCurrentTableLoads = true
    cat.resetLoadTableCalls()
    cat.resetLoadChangelogCalls()
    val df = sql(
      s"SELECT id FROM $cdcCatalogName.test_table CHANGES FROM VERSION 1 TO VERSION 10")
    QueryTest.checkAnswer(df, Seq(Row(1L), Row(10L)), checkToRDD = false)
    assert(cat.loadTableCalls.isEmpty)
    assert(cat.loadChangelogCalls.size == 1)
    V2TableRefreshUtil.refresh(spark, df.queryExecution.analyzed)
    assert(cat.loadTableCalls.isEmpty)
    assert(cat.loadChangelogCalls.size == 2)
  }

  test("versioned-only refresh leaves an unversioned changelog captured") {
    val cat = cdcCatalog
    val analyzed = spark.read.option("startingVersion", "1")
      .changes(s"$cdcCatalogName.test_table").queryExecution.analyzed
    cat.resetLoadChangelogCalls()
    cat.resetLoadTableCalls()
    val refreshed = V2TableRefreshUtil.refresh(spark, analyzed, versionedOnly = true)
    assert(refreshed.fastEquals(analyzed))
    assert(cat.loadChangelogCalls.isEmpty)
    assert(cat.loadTableCalls.isEmpty)
  }

  test("CDC refresh uses CDC state keys and validates the captured schema") {
    val cat = cdcCatalog
    cat.changelogKeys = Some(java.util.Set.of("ReF"))
    val analyzed = spark.read.option("startingVersion", "1").option("endingVersion", "10")
      .option("branch", "Main").option("ref", "History").option("split-size", "5")
      .changes(s"$cdcCatalogName.test_table").queryExecution.analyzed
    val context = cat.lastChangelogContext.get
    cat.resetLoadChangelogCalls()
    cat.resetLoadTableCalls()
    val refreshed = V2TableRefreshUtil.refresh(spark, analyzed)
    assert(cat.loadTableCalls.isEmpty)
    assert(cat.loadChangelogCalls.size == 1)
    assert(cat.lastChangelogContext.contains(context))
    assert(cat.lastOptions.get.size() == 1)
    assert(cat.lastOptions.get.get("ref") == "History")
    val relation = refreshed.collectFirst { case r: DataSourceV2Relation => r }.get
    assert(relation.options.get("split-size") == "5")
    assert(relation.table.asInstanceOf[ChangelogTable].resolved)

    cat.alterTable(ident, TableChange.deleteColumn(Array("data"), false))
    checkError(
      intercept[AnalysisException] { V2TableRefreshUtil.refresh(spark, analyzed) },
      condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH",
      parameters = Map(
        "tableName" -> s"`$cdcCatalogName`.`test_table`",
        "errors" -> "- `data` STRING has been removed"))
  }

  test("CDC temp views preserve distinct contexts when metadata is reloaded") {
    withTempView("cdc_first", "cdc_second") {
      val cat = cdcCatalog
      cat.addChangeRows(ident, Seq(changeRow(1L, 1L), changeRow(2L, 2L)))
      val tableName = s"$cdcCatalogName.test_table"
      sql(s"SELECT * FROM $tableName CHANGES FROM VERSION 1 TO VERSION 1")
        .createOrReplaceTempView("cdc_first")
      sql(s"SELECT * FROM $tableName CHANGES FROM VERSION 2 TO VERSION 2")
        .createOrReplaceTempView("cdc_second")
      cat.resetLoadChangelogCalls()
      cat.resetLoadTableCalls()
      val df = sql("SELECT id FROM cdc_first UNION ALL SELECT id FROM cdc_second " +
        "UNION ALL SELECT id FROM cdc_first")
      val changelogs = df.queryExecution.analyzed.collect {
        case r: DataSourceV2Relation => r.table.asInstanceOf[ChangelogTable]
      }
      assert(changelogs.map(_.changelogContext).distinct.size == 2)
      assert(changelogs.head.changelog eq changelogs.last.changelog)
      assert(changelogs.forall(_.resolved))
      assert(cat.loadChangelogCalls.size == 2)
      assert(cat.loadTableCalls.isEmpty)
      QueryTest.checkAnswer(df, Seq(Row(1L), Row(1L), Row(2L)), checkToRDD = false)
    }
  }

  test("CDC refresh preserves post-processing and rejects changed row-version metadata") {
    val tableIdent = recreatePostProcessingTable()
    val cat = cdcCatalog
    val properties = ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version"))
    cat.setChangelogProperties(tableIdent, properties)
    cat.addChangeRows(tableIdent, Seq(
      InternalRow(1L, 1L, UTF8String.fromString(Changelog.CHANGE_TYPE_DELETE), 1L, 1000000L),
      InternalRow(1L, 1L, UTF8String.fromString(Changelog.CHANGE_TYPE_INSERT), 1L, 1000000L)))
    val analyzed = spark.read.option("startingVersion", "1")
      .changes(s"$cdcCatalogName.test_table").select("id").queryExecution.analyzed
    assert(analyzed.exists(_.isInstanceOf[Window]))
    cat.addChangeRows(tableIdent, Seq(
      InternalRow(2L, 2L, UTF8String.fromString(Changelog.CHANGE_TYPE_INSERT), 2L, 2000000L)))
    val refreshed = V2TableRefreshUtil.refresh(spark, analyzed)
    assert(ResolveChangelogTable(refreshed).fastEquals(refreshed))
    QueryTest.checkAnswer(Dataset.ofRows(spark, refreshed), Seq(Row(2L)), checkToRDD = false)

    cat.setChangelogProperties(tableIdent,
      properties.copy(rowVersionName = Some("_commit_version")))
    checkError(
      intercept[AnalysisException] { V2TableRefreshUtil.refresh(spark, analyzed) },
      condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.CHANGELOG_METADATA_MISMATCH",
      parameters = Map(
        "tableName" -> s"`$cdcCatalogName`.`test_table_changelog`",
        "changedProperties" -> "rowVersion"))
  }

  // ===========================================================================
  // Streaming post-processing
  // ===========================================================================
  //
  // Row-level passes (carry-over removal and update detection) rewrite the streaming plan
  // into Aggregate -> [Filter] -> Generate(Inline) -> [Project] under an
  // EventTimeWatermark on `_commit_timestamp`. Net-change computation is still rejected
  // since it requires reasoning over the entire requested range.

  /** Re-creates the test table with non-nullable columns suitable as rowId / rowVersion. */
  private def recreatePostProcessingTable(): Identifier = {
    val cat = spark.sessionState.catalogManager.catalog(cdcCatalogName).asTableCatalog
    val ident = Identifier.of(Array.empty, "test_table")
    if (cat.tableExists(ident)) cat.dropTable(ident)
    cat.createTable(
      ident,
      Array(
        Column.create("id", LongType, false),
        Column.create("row_commit_version", LongType, false)),
      Array.empty[Transform],
      Collections.emptyMap[String, String]())
    ident
  }

  private def assertStreamingRowLevelRewrite(plan: LogicalPlan): Unit = {
    import org.apache.spark.sql.catalyst.plans.logical.{
      Aggregate, EventTimeWatermark, Generate}
    val watermarks = plan.collect { case w: EventTimeWatermark => w }
    assert(watermarks.nonEmpty,
      s"Expected EventTimeWatermark in streaming row-level rewrite. Plan:\n$plan")
    assert(watermarks.head.eventTime.name == "_commit_timestamp",
      s"Watermark must be on `_commit_timestamp`. Plan:\n$plan")
    val aggs = plan.collect { case a: Aggregate => a }
    assert(aggs.nonEmpty,
      s"Expected Aggregate in streaming row-level rewrite. Plan:\n$plan")
    val gens = plan.collect { case g: Generate => g }
    assert(gens.nonEmpty,
      s"Expected Generate(Inline) in streaming row-level rewrite. Plan:\n$plan")
  }

  test("DataStreamReader - changes() with carry-over capability rewrites plan") {
    val ident = recreatePostProcessingTable()
    val cat = spark.sessionState.catalogManager
      .catalog(cdcCatalogName)
      .asInstanceOf[InMemoryChangelogCatalog]
    cat.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    val analyzed = spark.readStream
      .changes(s"$cdcCatalogName.test_table")
      .queryExecution.analyzed
    assertStreamingRowLevelRewrite(analyzed)
  }

  test("DataStreamReader - changes() with computeUpdates rewrites plan") {
    val ident = recreatePostProcessingTable()
    val cat = spark.sessionState.catalogManager
      .catalog(cdcCatalogName)
      .asInstanceOf[InMemoryChangelogCatalog]
    cat.setChangelogProperties(ident, ChangelogProperties(
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    val analyzed = spark.readStream
      .option("computeUpdates", "true")
      .option("deduplicationMode", "none")
      .changes(s"$cdcCatalogName.test_table")
      .queryExecution.analyzed
    assertStreamingRowLevelRewrite(analyzed)
  }

  test("DataStreamReader - changes() with deduplicationMode=netChanges rewrites plan") {
    import org.apache.spark.sql.catalyst.plans.logical.TransformWithState
    val ident = recreatePostProcessingTable()
    val cat = spark.sessionState.catalogManager
      .catalog(cdcCatalogName)
      .asInstanceOf[InMemoryChangelogCatalog]
    cat.setChangelogProperties(ident, ChangelogProperties(
      containsIntermediateChanges = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    val analyzed = spark.readStream
      .option("deduplicationMode", "netChanges")
      .changes(s"$cdcCatalogName.test_table")
      .queryExecution.analyzed
    val tws = analyzed.collect { case t: TransformWithState => t }
    assert(tws.size == 1,
      s"Expected exactly one TransformWithState; found ${tws.size}. Plan:\n$analyzed")
  }

  // ===========================================================================
  // Generic changelog schema validation
  // ===========================================================================

  private def stubInfo(): ChangelogContext = new ChangelogContext(
    new ChangelogRange.VersionRange("1", java.util.Optional.of("2"), true, true),
    ChangelogContext.DeduplicationMode.DROP_CARRYOVERS,
    false)

  private def cl(name: String, cols: (String, org.apache.spark.sql.types.DataType)*)
      : TestChangelog = {
    new TestChangelog(name, cols.map { case (n, t) => Column.create(n, t) }.toArray)
  }

  private def missing(columnName: String): Map[String, String] =
    Map("changelogName" -> "bad_cl", "columnName" -> columnName)

  private def wrongType(columnName: String, expected: String, actual: String)
      : Map[String, String] = Map(
    "changelogName" -> "bad_cl",
    "columnName" -> columnName,
    "expectedType" -> expected,
    "actualType" -> actual)

  // Valid metadata tuples; tests swap one of these out to create broken schemas.
  private val validChangeType = "_change_type" -> StringType
  private val validVersion = "_commit_version" -> LongType
  private val validTimestamp = "_commit_timestamp" -> TimestampType

  test("ChangelogTable - missing _change_type column throws") {
    checkError(
      intercept[AnalysisException] {
        ChangelogTable(cl("bad_cl", validVersion, validTimestamp), stubInfo())
      },
      condition = "INVALID_CHANGELOG_SCHEMA.MISSING_COLUMN",
      parameters = missing("_change_type"))
  }

  test("ChangelogTable - missing _commit_version column throws") {
    checkError(
      intercept[AnalysisException] {
        ChangelogTable(cl("bad_cl", validChangeType, validTimestamp), stubInfo())
      },
      condition = "INVALID_CHANGELOG_SCHEMA.MISSING_COLUMN",
      parameters = missing("_commit_version"))
  }

  test("ChangelogTable - missing _commit_timestamp column throws") {
    checkError(
      intercept[AnalysisException] {
        ChangelogTable(cl("bad_cl", validChangeType, validVersion), stubInfo())
      },
      condition = "INVALID_CHANGELOG_SCHEMA.MISSING_COLUMN",
      parameters = missing("_commit_timestamp"))
  }

  test("ChangelogTable - wrong _change_type data type throws") {
    checkError(
      intercept[AnalysisException] {
        ChangelogTable(
          cl("bad_cl", "_change_type" -> IntegerType, validVersion, validTimestamp),
          stubInfo())
      },
      condition = "INVALID_CHANGELOG_SCHEMA.INVALID_COLUMN_TYPE",
      parameters = wrongType("_change_type", "STRING", "INT"))
  }

  test("ChangelogTable - wrong _commit_timestamp data type throws") {
    checkError(
      intercept[AnalysisException] {
        ChangelogTable(
          cl("bad_cl", validChangeType, validVersion, "_commit_timestamp" -> LongType),
          stubInfo())
      },
      condition = "INVALID_CHANGELOG_SCHEMA.INVALID_COLUMN_TYPE",
      parameters = wrongType("_commit_timestamp", "TIMESTAMP", "BIGINT"))
  }

  test("ChangelogTable - _commit_version accepts LongType and StringType") {
    Seq(LongType, StringType).foreach { versionType =>
      ChangelogTable(
        cl("any_cl", validChangeType, "_commit_version" -> versionType, validTimestamp),
        stubInfo())
    }
  }

  test("ChangelogTable - _commit_version rejects all other data types") {
    val structVersion = StructType(Seq(StructField("v", LongType)))
    Seq[(org.apache.spark.sql.types.DataType, String)](
      // Other atomic types previously allowed under the AtomicType contract.
      IntegerType -> "INT",
      TimestampType -> "TIMESTAMP",
      // Complex types (always rejected).
      ArrayType(LongType) -> "ARRAY<BIGINT>",
      MapType(StringType, LongType) -> "MAP<STRING, BIGINT>",
      structVersion -> structVersion.sql).foreach { case (versionType, sql) =>
      checkError(
        intercept[AnalysisException] {
          ChangelogTable(
            cl("bad_cl", validChangeType, "_commit_version" -> versionType, validTimestamp),
            stubInfo())
        },
        condition = "INVALID_CHANGELOG_SCHEMA.INVALID_COLUMN_TYPE",
        parameters = wrongType("_commit_version", "BIGINT or STRING", sql))
    }
  }

  test("ChangelogTable - valid schema with data columns passes") {
    ChangelogTable(
      cl("good_cl", "id" -> LongType, "name" -> StringType,
        validChangeType, validVersion, validTimestamp),
      stubInfo())
  }

  test("ChangelogTable - nested rowId and rowVersion references pass (Delta-style _metadata)") {
    val metadataRowId = FieldReference(Seq("_metadata", "row_id"))
    val metadataRowVersion = FieldReference(Seq("_metadata", "row_commit_version"))
    val cl = new TestChangelog(
      "delta_cl",
      Array(
        Column.create("id", LongType, false),
        Column.create("_change_type", StringType),
        Column.create("_commit_version", LongType),
        Column.create("_commit_timestamp", TimestampType)),
      carryoverRows = true,
      rowIdRefs = Array(metadataRowId),
      rowVersionRef = Some(metadataRowVersion))
    ChangelogTable(cl, stubInfo())
  }

  test("ChangelogTable - representsUpdateAsDeleteAndInsert=true requires non-empty rowId") {
    val cl = new TestChangelog(
      "bad_cl",
      Array(
        Column.create("_change_type", StringType),
        Column.create("_commit_version", LongType),
        Column.create("_commit_timestamp", TimestampType)),
      updateAsDeleteInsert = true,
      rowIdRefs = Array.empty,
      rowVersionRef = Some(FieldReference.column("_commit_version")))
    checkError(
      intercept[AnalysisException] { ChangelogTable(cl, stubInfo()) },
      condition = "INVALID_CHANGELOG_SCHEMA.MISSING_ROW_ID",
      parameters = Map("changelogName" -> "bad_cl"))
  }

  test("ChangelogTable - containsIntermediateChanges=true requires non-empty rowId") {
    val cl = new TestChangelog(
      "bad_cl",
      Array(
        Column.create("_change_type", StringType),
        Column.create("_commit_version", LongType),
        Column.create("_commit_timestamp", TimestampType)),
      intermediateChanges = true,
      rowIdRefs = Array.empty)
    checkError(
      intercept[AnalysisException] { ChangelogTable(cl, stubInfo()) },
      condition = "INVALID_CHANGELOG_SCHEMA.MISSING_ROW_ID",
      parameters = Map("changelogName" -> "bad_cl"))
  }

  test("ChangelogTable - UnsupportedOperationException surfaces when rowId() not implemented") {
    val cl = new TestChangelog(
      "bad_cl",
      Array(
        Column.create("_change_type", StringType),
        Column.create("_commit_version", LongType),
        Column.create("_commit_timestamp", TimestampType)),
      carryoverRows = true,
      rowIdSupported = false,
      rowVersionRef = Some(FieldReference.column("_commit_version")))
    intercept[UnsupportedOperationException] { ChangelogTable(cl, stubInfo()) }
  }

  test("ChangelogTable - UnsupportedOperationException surfaces when rowVersion() missing") {
    val cl = new TestChangelog(
      "bad_cl",
      Array(
        Column.create("_change_type", StringType),
        Column.create("_commit_version", LongType),
        Column.create("_commit_timestamp", TimestampType)),
      carryoverRows = true,
      rowIdRefs = Array(FieldReference.column("id")),
      rowVersionRef = None)
    intercept[UnsupportedOperationException] { ChangelogTable(cl, stubInfo()) }
  }

}

/** Supports configurable CDC keys and detects accidental ordinary-table loads in CDC paths. */
class ChangelogStateOptionsCatalog extends InMemoryChangelogCatalog {
  var changelogKeys: Option[java.util.Set[String]] = None
  var rejectCurrentTableLoads: Boolean = false

  override def changelogStateOptionKeys(): java.util.Set[String] = {
    changelogKeys.getOrElse(tableStateOptionKeys())
  }

  override def loadTable(ident: Identifier): Table = {
    assert(!rejectCurrentTableLoads, "CDC must not load the current ordinary table")
    super.loadTable(ident)
  }
}

/**
 * Test-only [[Changelog]] implementation that returns a hand-crafted schema. Used to
 * exercise [[ChangelogTable]]'s schema validation without going through a real catalog.
 *
 * Defaults match a minimal connector with no post-processing capabilities. Tests opt
 * into capability flags or `rowVersion()` overrides via constructor params.
 */
private class TestChangelog(
    nameArg: String,
    cols: Array[Column],
    carryoverRows: Boolean = false,
    updateAsDeleteInsert: Boolean = false,
    intermediateChanges: Boolean = false,
    rowIdRefs: Array[NamedReference] = Array.empty,
    rowIdSupported: Boolean = true,
    rowVersionRef: Option[NamedReference] = None) extends Changelog {
  override def name(): String = nameArg
  override def columns(): Array[Column] = cols
  override def containsCarryoverRows(): Boolean = carryoverRows
  override def containsIntermediateChanges(): Boolean = intermediateChanges
  override def representsUpdateAsDeleteAndInsert(): Boolean = updateAsDeleteInsert
  override def rowId(): Array[NamedReference] =
    if (rowIdSupported) rowIdRefs else super.rowId()
  override def rowVersion(): NamedReference =
    rowVersionRef.getOrElse(super.rowVersion())
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    throw new UnsupportedOperationException("not needed for schema validation tests")
  }
}
