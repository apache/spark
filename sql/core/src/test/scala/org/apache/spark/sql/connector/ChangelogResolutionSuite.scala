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

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.ChangelogRange
import org.apache.spark.sql.execution.datasources.v2.{ChangelogTable, DataSourceV2Relation}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType}

/**
 * Tests for the CDC (Change Data Capture) analyzer resolution path:
 * RelationChanges -> resolveChangelog -> DataSourceV2Relation(ChangelogTable).
 */
class ChangelogResolutionSuite extends QueryTest with SharedSparkSession {

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
      Array.empty,
      new util.HashMap[String, String]())

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
      Array.empty,
      new util.HashMap[String, String]())
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
}
