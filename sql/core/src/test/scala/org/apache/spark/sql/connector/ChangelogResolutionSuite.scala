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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.ChangelogRange
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference, Transform}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.execution.datasources.v2.{ChangelogTable, DataSourceV2Relation}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Tests for the CDC (Change Data Capture) analyzer resolution path:
 * RelationChanges -> resolveChangelog -> DataSourceV2Relation(ChangelogTable).
 */
class ChangelogResolutionSuite extends SharedSparkSession {

  private val cdcCatalogName = "cdc_catalog"
  private val noCdcCatalogName = "no_cdc_catalog"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(s"spark.sql.catalog.$cdcCatalogName",
      classOf[InMemoryChangelogCatalog].getName)
    spark.conf.set(s"spark.sql.catalog.$noCdcCatalogName",
      classOf[InMemoryTableCatalog].getName)
  }

  override def afterAll(): Unit = {
    spark.conf.unset(s"spark.sql.catalog.$cdcCatalogName")
    spark.conf.unset(s"spark.sql.catalog.$noCdcCatalogName")
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    val catalog = spark.sessionState.catalogManager.catalog(cdcCatalogName).asTableCatalog
    val ident = Identifier.of(Array.empty, "test_table")
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

  test("CHANGES clause passes changelogInfo to catalog") {
    sql(s"SELECT * FROM $cdcCatalogName.test_table CHANGES FROM VERSION 1 TO VERSION 5")
    val cat = spark.sessionState.catalogManager
      .catalog(cdcCatalogName)
      .asInstanceOf[InMemoryChangelogCatalog]
    val info = cat.lastChangelogInfo
    assert(info.isDefined)
    val range = info.get.range().asInstanceOf[ChangelogRange.VersionRange]
    assert(range.startingVersion() == "1")
    assert(range.endingVersion().get() == "5")
  }

  // ===========================================================================
  // Streaming post-processing rejection
  // ===========================================================================
  //
  // Streaming CDC reads bypass the post-processing analyzer rule's transformation
  // path. To prevent silent wrong results when the requested options would require
  // post-processing, the rule throws an explicit AnalysisException for streaming.

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

  test("DataStreamReader - changes() with carry-over capability throws") {
    val ident = recreatePostProcessingTable()
    val cat = spark.sessionState.catalogManager
      .catalog(cdcCatalogName)
      .asInstanceOf[InMemoryChangelogCatalog]
    cat.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    checkError(
      intercept[AnalysisException] {
        spark.readStream
          .changes(s"$cdcCatalogName.test_table")
          .queryExecution.analyzed
      },
      condition = "INVALID_CDC_OPTION.STREAMING_POST_PROCESSING_NOT_SUPPORTED",
      parameters = Map("changelogName" -> s"$cdcCatalogName.test_table_changelog"))
  }

  test("DataStreamReader - changes() with computeUpdates throws") {
    val ident = recreatePostProcessingTable()
    val cat = spark.sessionState.catalogManager
      .catalog(cdcCatalogName)
      .asInstanceOf[InMemoryChangelogCatalog]
    cat.setChangelogProperties(ident, ChangelogProperties(
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    checkError(
      intercept[AnalysisException] {
        spark.readStream
          .option("computeUpdates", "true")
          .option("deduplicationMode", "none")
          .changes(s"$cdcCatalogName.test_table")
          .queryExecution.analyzed
      },
      condition = "INVALID_CDC_OPTION.STREAMING_POST_PROCESSING_NOT_SUPPORTED",
      parameters = Map("changelogName" -> s"$cdcCatalogName.test_table_changelog"))
  }

  // ===========================================================================
  // Generic changelog schema validation
  // ===========================================================================

  private def stubInfo(): ChangelogInfo = new ChangelogInfo(
    new ChangelogRange.VersionRange("1", java.util.Optional.of("2"), true, true),
    ChangelogInfo.DeduplicationMode.DROP_CARRYOVERS,
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

  test("ChangelogTable - _commit_version type is connector-defined (any type accepted)") {
    Seq(IntegerType, LongType, StringType).foreach { versionType =>
      ChangelogTable(
        cl("any_cl", validChangeType, "_commit_version" -> versionType, validTimestamp),
        stubInfo())
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
