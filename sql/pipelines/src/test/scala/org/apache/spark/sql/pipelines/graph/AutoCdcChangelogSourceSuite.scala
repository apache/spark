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

package org.apache.spark.sql.pipelines.graph

import org.apache.spark.sql.{functions, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.connector.catalog.{
  ChangelogProperties,
  Column => V2Column,
  Identifier,
  InMemoryChangelogCatalog
}
import org.apache.spark.sql.connector.catalog.Changelog.{
  CHANGE_TYPE_DELETE,
  CHANGE_TYPE_INSERT,
  CHANGE_TYPE_UPDATE_POSTIMAGE,
  CHANGE_TYPE_UPDATE_PREIMAGE
}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.pipelines.autocdc.AutoCdcReservedNames
import org.apache.spark.sql.pipelines.utils.ExecutionTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * End-to-end tests for AutoCDC SCD type 1 flows whose source is a native CDC
 * [[org.apache.spark.sql.connector.catalog.Changelog]] read (via
 * `DataStreamReader.changes`). These exercise the [[org.apache.spark.sql.pipelines.autocdc.
 * ChangelogAutoCdcBridge]] auto-detection: the delete condition and metadata-column exclusion are
 * derived from the changelog contract rather than supplied by the user, and `update_preimage`
 * rows are filtered from the feed.
 */
class AutoCdcChangelogSourceSuite
    extends ExecutionTest
    with SharedSparkSession
    with AutoCdcGraphExecutionTestMixin {

  /** A second catalog, backed by [[InMemoryChangelogCatalog]], serving the changelog sources. */
  private val cdcCatalog: String = "cdc"
  private val cdcNamespace: String = "cdcns"

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    spark.conf.set(
      s"spark.sql.catalog.$cdcCatalog",
      classOf[InMemoryChangelogCatalog].getName
    )
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $cdcCatalog.$cdcNamespace")
  }

  override protected def afterEach(): Unit = {
    spark.sessionState.conf.unsetConf(s"spark.sql.catalog.$cdcCatalog")
    super.afterEach()
  }

  /** The [[InMemoryChangelogCatalog]] instance backing [[cdcCatalog]]. */
  private def changelogCatalog: InMemoryChangelogCatalog =
    spark.sessionState.catalogManager
      .catalog(cdcCatalog)
      .asInstanceOf[InMemoryChangelogCatalog]

  /** Identifier of a changelog source table under [[cdcCatalog]].[[cdcNamespace]]. */
  private def sourceIdent(table: String): Identifier =
    Identifier.of(Array(cdcNamespace), table)

  /**
   * Create an `(id BIGINT, name STRING)` changelog source table and add the given change rows.
   * Each row is `(id, name, _change_type, _commit_version, _commit_timestamp)`.
   */
  private def createChangelogSource(
      table: String,
      rows: Seq[(Long, String, String, Long)],
      properties: ChangelogProperties = ChangelogProperties()): Unit = {
    val cat = changelogCatalog
    val ident = sourceIdent(table)
    cat.createTable(
      ident,
      Array(V2Column.create("id", LongType), V2Column.create("name", StringType)),
      Array.empty,
      new java.util.HashMap[String, String]()
    )
    cat.setChangelogProperties(ident, properties)
    val internalRows = rows.map { case (id, name, changeType, version) =>
      InternalRow(
        id,
        UTF8String.fromString(name),
        UTF8String.fromString(changeType),
        version,
        // _commit_timestamp in micros; unused for raw (non-post-processed) streaming reads.
        version * 1000000L
      )
    }
    cat.addChangeRows(ident, internalRows)
  }

  /** A streaming `changes()` DataFrame over a changelog source table. */
  private def changelogStream(table: String, options: Map[String, String] = Map.empty): DataFrame = {
    val reader = (Map("startingVersion" -> "1") ++ options).foldLeft(spark.readStream) {
      case (r, (k, v)) => r.option(k, v)
    }
    reader.changes(s"$cdcCatalog.$cdcNamespace.$table")
  }

  /** Create the standard `(id, name, _cdc_metadata)` SCD1 target table. */
  private def createTarget(table: String): Unit = {
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.$table " +
      s"(id BIGINT NOT NULL, name STRING, $cdcMetadataDdl)"
    )
  }

  test("native CDC changelog source: insert/update/delete propagate to SCD1 target") {
    createTarget("target")
    // id=1 is inserted then updated (pre/post-image pair at v2); id=2 is inserted then deleted.
    createChangelogSource(
      "src",
      Seq(
        (1L, "alice", CHANGE_TYPE_INSERT, 1L),
        (1L, "alice", CHANGE_TYPE_UPDATE_PREIMAGE, 2L),
        (1L, "alice2", CHANGE_TYPE_UPDATE_POSTIMAGE, 2L),
        (2L, "bob", CHANGE_TYPE_INSERT, 1L),
        (2L, "bob", CHANGE_TYPE_DELETE, 3L)
      )
    )

    val ctx = singleAutoCdcFlowPipeline(
      flowName = "auto_cdc_flow",
      target = "target",
      sourceDf = changelogStream("src"),
      keys = Seq("id"),
      sequencing = functions.col("_commit_version")
    )
    runPipeline(ctx)

    // id=1 converges to the update post-image (the pre-image is filtered out); id=2 is deleted.
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1L, "alice2", cdcMeta(None, Some(2L))))
    )
  }

  test("native CDC metadata columns are excluded from the target output") {
    createTarget("target")
    createChangelogSource(
      "src",
      Seq((1L, "alice", CHANGE_TYPE_INSERT, 1L))
    )

    val ctx = singleAutoCdcFlowPipeline(
      flowName = "auto_cdc_flow",
      target = "target",
      sourceDf = changelogStream("src"),
      keys = Seq("id"),
      sequencing = functions.col("_commit_version")
    )
    runPipeline(ctx)

    // The target schema must not have gained the native CDC metadata columns: only the user
    // data columns plus the AutoCDC metadata struct.
    val targetFields = spark.table(s"$catalog.$namespace.target").schema.fieldNames.toSeq
    assert(targetFields == Seq("id", "name", AutoCdcReservedNames.cdcMetadataColName))
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1L, "alice", cdcMeta(None, Some(1L))))
    )
  }

  test("an explicit apply_as_deletes overrides the derived _change_type delete condition") {
    createTarget("target")
    // The row is flagged delete via `_change_type`, but the explicit delete condition never
    // matches, so the event is treated as an upsert and the row survives.
    createChangelogSource(
      "src",
      Seq(
        (1L, "alice", CHANGE_TYPE_INSERT, 1L),
        (1L, "alice", CHANGE_TYPE_DELETE, 2L)
      )
    )

    val ctx = singleAutoCdcFlowPipeline(
      flowName = "auto_cdc_flow",
      target = "target",
      sourceDf = changelogStream("src"),
      keys = Seq("id"),
      sequencing = functions.col("_commit_version"),
      deleteCondition = Some(functions.lit(false))
    )
    runPipeline(ctx)

    // With deletes disabled, the highest-sequenced event (the delete at v2, treated as an
    // upsert) keeps the row alive.
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1L, "alice", cdcMeta(None, Some(2L))))
    )
  }

  test("changelog source representing updates as delete+insert without computeUpdates fails") {
    createTarget("target")
    val cat = changelogCatalog
    val ident = sourceIdent("src")
    cat.createTable(
      ident,
      Array(
        V2Column.create("id", LongType),
        V2Column.create("name", StringType),
        V2Column.create("row_version", LongType)
      ),
      Array.empty,
      new java.util.HashMap[String, String]()
    )
    cat.setChangelogProperties(
      ident,
      ChangelogProperties(
        representsUpdateAsDeleteAndInsert = true,
        rowIdNames = Seq("id"),
        rowVersionName = Some("row_version")
      )
    )

    val ctx = singleAutoCdcFlowPipeline(
      flowName = "auto_cdc_flow",
      target = "target",
      // computeUpdates is left at its default of false.
      sourceDf = changelogStream("src"),
      keys = Seq("id"),
      sequencing = functions.col("_commit_version")
    )

    val ex = intercept[RuntimeException] { runPipeline(ctx) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "AUTOCDC_CHANGELOG_REQUIRES_COMPUTE_UPDATES",
      sqlState = Some("22023"),
      parameters = Map("tableName" -> s"`$catalog`.`$namespace`.`target`")
    )
  }

  test("changelog source surfacing carry-over rows with deduplicationMode=none fails") {
    createTarget("target")
    val cat = changelogCatalog
    val ident = sourceIdent("src")
    cat.createTable(
      ident,
      Array(
        V2Column.create("id", LongType),
        V2Column.create("name", StringType),
        V2Column.create("row_version", LongType)
      ),
      Array.empty,
      new java.util.HashMap[String, String]()
    )
    cat.setChangelogProperties(
      ident,
      ChangelogProperties(
        containsCarryoverRows = true,
        rowIdNames = Seq("id"),
        rowVersionName = Some("row_version")
      )
    )

    val ctx = singleAutoCdcFlowPipeline(
      flowName = "auto_cdc_flow",
      target = "target",
      sourceDf = changelogStream("src", options = Map("deduplicationMode" -> "none")),
      keys = Seq("id"),
      sequencing = functions.col("_commit_version")
    )

    val ex = intercept[RuntimeException] { runPipeline(ctx) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "AUTOCDC_CHANGELOG_REQUIRES_CARRYOVER_REMOVAL",
      sqlState = Some("22023"),
      parameters = Map("tableName" -> s"`$catalog`.`$namespace`.`target`")
    )
  }

  test("a non-changelog source is unaffected by the changelog bridge") {
    val session = spark
    import session.implicits._

    // No metadata-column exclusion is derived for a non-changelog source, so the sequencing
    // column flows through to the target as an ordinary data column.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target_plain " +
      s"(id BIGINT NOT NULL, name STRING, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    // A plain streaming source with no native CDC metadata columns: no delete condition is
    // derived, so every event is an upsert.
    val stream = MemoryStream[(Long, String, Long)]
    stream.addData((1L, "alice", 1L), (1L, "alice2", 2L))

    val ctx = singleAutoCdcFlowPipeline(
      flowName = "auto_cdc_flow",
      target = "target_plain",
      sourceDf = stream.toDF().toDF("id", "name", "version"),
      keys = Seq("id"),
      sequencing = functions.col("version")
    )
    runPipeline(ctx)

    checkAnswer(
      spark.table(s"$catalog.$namespace.target_plain"),
      Seq(Row(1L, "alice2", 2L, cdcMeta(None, Some(2L))))
    )
  }
}
