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

import java.sql.Timestamp
import java.util

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NamedStreamingRelation
import org.apache.spark.sql.catalyst.streaming.UserProvided
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.Changelog.{
  CHANGE_TYPE_DELETE, CHANGE_TYPE_INSERT, CHANGE_TYPE_UPDATE_POSTIMAGE, CHANGE_TYPE_UPDATE_PREIMAGE}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * End-to-end tests for Change Data Capture (CDC) queries using
 * [[InMemoryChangelogCatalog]].
 */
class ChangelogEndToEndSuite extends SharedSparkSession {

  private val catalogName = "cdc_e2e"
  private val testTableName = "test_table"
  private val fullTableName = s"$catalogName.$testTableName"
  private val ident = Identifier.of(Array.empty, testTableName)

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(
      s"spark.sql.catalog.$catalogName",
      classOf[InMemoryChangelogCatalog].getName)
  }

  override def afterAll(): Unit = {
    spark.conf.unset(s"spark.sql.catalog.$catalogName")
    super.afterAll()
  }

  private def catalog: InMemoryChangelogCatalog = {
    spark.sessionState.catalogManager
      .catalog(catalogName)
      .asInstanceOf[InMemoryChangelogCatalog]
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    val cat = catalog
    if (cat.tableExists(ident)) {
      cat.dropTable(ident)
    }
    cat.createTable(
      ident,
      Array(
        Column.create("id", LongType),
        Column.create("data", StringType)),
      Array.empty,
      new util.HashMap[String, String]())
    cat.clearChangeRows(ident)
  }

  private def makeChangeRow(
      id: Long,
      data: String,
      changeType: String,
      commitVersion: Long,
      commitTimestamp: Long): InternalRow = {
    InternalRow(
      id,
      UTF8String.fromString(data),
      UTF8String.fromString(changeType),
      commitVersion,
      commitTimestamp)
  }

  // ---------- Batch: basic data retrieval ----------

  test("changes() returns change data") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", CHANGE_TYPE_INSERT, 1L, 1000000L),
      makeChangeRow(2L, "b", CHANGE_TYPE_DELETE, 2L, 2000000L)))

    val expected = Seq(
      Row(1L, "a", CHANGE_TYPE_INSERT, 1L, new Timestamp(1000L)),
      Row(2L, "b", CHANGE_TYPE_DELETE, 2L, new Timestamp(2000L)))

    // DataFrame API
    checkAnswer(
      spark.read
        .option("startingVersion", "1")
        .option("endingVersion", "2")
        .changes(fullTableName),
      expected)

    // SQL
    checkAnswer(
      sql(s"SELECT * FROM $fullTableName CHANGES FROM VERSION 1 TO VERSION 2"),
      expected)
  }

  test("changes() with open-ended version range") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", CHANGE_TYPE_INSERT, 1L, 1000000L),
      makeChangeRow(2L, "b", CHANGE_TYPE_INSERT, 2L, 2000000L),
      makeChangeRow(3L, "c", CHANGE_TYPE_INSERT, 3L, 3000000L)))

    val expected = Seq(
      Row(2L, "b", CHANGE_TYPE_INSERT, 2L, new Timestamp(2000L)),
      Row(3L, "c", CHANGE_TYPE_INSERT, 3L, new Timestamp(3000L)))

    // DataFrame API
    checkAnswer(
      spark.read.option("startingVersion", "2").changes(fullTableName),
      expected)

    // SQL
    checkAnswer(
      sql(s"SELECT * FROM $fullTableName CHANGES FROM VERSION 2"),
      expected)
  }

  test("changes() returns empty result when no changes exist") {
    // DataFrame API
    val dfApi = spark.read
      .option("startingVersion", "1")
      .option("endingVersion", "5")
      .changes(fullTableName)
    assert(dfApi.collect().isEmpty)
    assert(dfApi.schema.fieldNames === Array(
      "id", "data", "_change_type",
      "_commit_version", "_commit_timestamp"))

    // SQL
    val dfSql = sql(
      s"SELECT * FROM $fullTableName CHANGES FROM VERSION 1 TO VERSION 5")
    assert(dfSql.collect().isEmpty)
    assert(dfSql.schema.fieldNames.contains("_change_type"))
  }

  // ---------- Batch: projection, filter, aggregation ----------

  test("changes() select CDC metadata columns") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", CHANGE_TYPE_INSERT, 1L, 1000000L),
      makeChangeRow(2L, "b", CHANGE_TYPE_DELETE, 2L, 2000000L)))

    val expected = Seq(
      Row(1L, CHANGE_TYPE_INSERT, 1L),
      Row(2L, CHANGE_TYPE_DELETE, 2L))

    // DataFrame API
    checkAnswer(
      spark.read.option("startingVersion", "1").changes(fullTableName)
        .select("id", "_change_type", "_commit_version"),
      expected)

    // SQL
    checkAnswer(
      sql(s"SELECT id, _change_type, _commit_version FROM $fullTableName " +
        "CHANGES FROM VERSION 1"),
      expected)
  }

  test("changes() with projection and filter") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", CHANGE_TYPE_INSERT, 1L, 1000000L),
      makeChangeRow(2L, "b", CHANGE_TYPE_INSERT, 1L, 1000000L),
      makeChangeRow(1L, "a2", CHANGE_TYPE_INSERT, 2L, 2000000L)))

    val expected = Seq(Row(1L, "a2"))

    // DataFrame API
    checkAnswer(
      spark.read.option("startingVersion", "1").changes(fullTableName)
        .filter("_commit_version = 2").select("id", "data"),
      expected)

    // SQL
    checkAnswer(
      sql(s"SELECT id, data FROM $fullTableName CHANGES FROM VERSION 1 " +
        "WHERE _commit_version = 2"),
      expected)
  }

  test("changes() with aggregation on change types") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", CHANGE_TYPE_INSERT, 1L, 1000000L),
      makeChangeRow(2L, "b", CHANGE_TYPE_INSERT, 1L, 1000000L),
      makeChangeRow(1L, "a", CHANGE_TYPE_DELETE, 2L, 2000000L)))

    val expected = Seq(
      Row(CHANGE_TYPE_INSERT, 2L),
      Row(CHANGE_TYPE_DELETE, 1L))

    // DataFrame API
    checkAnswer(
      spark.read.option("startingVersion", "1").changes(fullTableName)
        .groupBy("_change_type").count(),
      expected)

    // SQL
    checkAnswer(
      sql(s"SELECT _change_type, count(*) FROM $fullTableName " +
        "CHANGES FROM VERSION 1 GROUP BY _change_type"),
      expected)
  }

  test("schema includes CDC metadata columns") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", CHANGE_TYPE_INSERT, 1L, 1000000L)))

    // DataFrame API
    val dfApi = spark.read.option("startingVersion", "1").changes(fullTableName)
    assert(dfApi.schema.fieldNames === Array(
      "id", "data", "_change_type",
      "_commit_version", "_commit_timestamp"))

    // SQL
    val dfSql = sql(s"SELECT * FROM $fullTableName CHANGES FROM VERSION 1")
    assert(dfSql.schema.fieldNames === Array(
      "id", "data", "_change_type",
      "_commit_version", "_commit_timestamp"))
  }

  // ---------- Batch: version range filtering ----------

  test("changes() version range filters correctly") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", CHANGE_TYPE_INSERT, 1L, 1000000L),
      makeChangeRow(2L, "b", CHANGE_TYPE_INSERT, 2L, 2000000L),
      makeChangeRow(3L, "c", CHANGE_TYPE_INSERT, 3L, 3000000L),
      makeChangeRow(4L, "d", CHANGE_TYPE_INSERT, 4L, 4000000L)))

    val expected = Seq(
      Row(2L, "b", CHANGE_TYPE_INSERT, 2L, new Timestamp(2000L)),
      Row(3L, "c", CHANGE_TYPE_INSERT, 3L, new Timestamp(3000L)))

    // DataFrame API
    checkAnswer(
      spark.read
        .option("startingVersion", "2")
        .option("endingVersion", "3")
        .changes(fullTableName),
      expected)

    // SQL
    checkAnswer(
      sql(s"SELECT * FROM $fullTableName CHANGES FROM VERSION 2 TO VERSION 3"),
      expected)
  }

  // ---------- Batch: bound inclusivity ----------

  test("changes() default bounds are inclusive") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", CHANGE_TYPE_INSERT, 1L, 1000000L),
      makeChangeRow(2L, "b", CHANGE_TYPE_INSERT, 2L, 2000000L),
      makeChangeRow(3L, "c", CHANGE_TYPE_INSERT, 3L, 3000000L)))

    val expected = Seq(
      Row(1L, "a", CHANGE_TYPE_INSERT, 1L, new Timestamp(1000L)),
      Row(2L, "b", CHANGE_TYPE_INSERT, 2L, new Timestamp(2000L)),
      Row(3L, "c", CHANGE_TYPE_INSERT, 3L, new Timestamp(3000L)))

    // DataFrame API - default (both inclusive)
    checkAnswer(
      spark.read
        .option("startingVersion", "1")
        .option("endingVersion", "3")
        .changes(fullTableName),
      expected)

    // SQL - default (both inclusive)
    checkAnswer(
      sql(s"SELECT * FROM $fullTableName CHANGES FROM VERSION 1 TO VERSION 3"),
      expected)

    // SQL - explicit INCLUSIVE keywords
    checkAnswer(
      sql(s"SELECT * FROM $fullTableName " +
        "CHANGES FROM VERSION 1 INCLUSIVE TO VERSION 3 INCLUSIVE"),
      expected)
  }

  test("changes() with startingBoundInclusive=false") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", CHANGE_TYPE_INSERT, 1L, 1000000L),
      makeChangeRow(2L, "b", CHANGE_TYPE_INSERT, 2L, 2000000L),
      makeChangeRow(3L, "c", CHANGE_TYPE_INSERT, 3L, 3000000L)))

    // Exclusive start: version 1 excluded, versions 2 and 3 included
    val expected = Seq(
      Row(2L, "b", CHANGE_TYPE_INSERT, 2L, new Timestamp(2000L)),
      Row(3L, "c", CHANGE_TYPE_INSERT, 3L, new Timestamp(3000L)))

    // DataFrame API
    checkAnswer(
      spark.read
        .option("startingVersion", "1")
        .option("endingVersion", "3")
        .option("startingBoundInclusive", "false")
        .changes(fullTableName),
      expected)

    // SQL
    checkAnswer(
      sql(s"SELECT * FROM $fullTableName " +
        "CHANGES FROM VERSION 1 EXCLUSIVE TO VERSION 3"),
      expected)
  }

  test("changes() with endingBoundInclusive=false") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", CHANGE_TYPE_INSERT, 1L, 1000000L),
      makeChangeRow(2L, "b", CHANGE_TYPE_INSERT, 2L, 2000000L),
      makeChangeRow(3L, "c", CHANGE_TYPE_INSERT, 3L, 3000000L)))

    // Exclusive end: versions 1 and 2 included, version 3 excluded
    val expected = Seq(
      Row(1L, "a", CHANGE_TYPE_INSERT, 1L, new Timestamp(1000L)),
      Row(2L, "b", CHANGE_TYPE_INSERT, 2L, new Timestamp(2000L)))

    // DataFrame API
    checkAnswer(
      spark.read
        .option("startingVersion", "1")
        .option("endingVersion", "3")
        .option("endingBoundInclusive", "false")
        .changes(fullTableName),
      expected)

    // SQL
    checkAnswer(
      sql(s"SELECT * FROM $fullTableName " +
        "CHANGES FROM VERSION 1 TO VERSION 3 EXCLUSIVE"),
      expected)
  }

  test("changes() with both bounds exclusive") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", CHANGE_TYPE_INSERT, 1L, 1000000L),
      makeChangeRow(2L, "b", CHANGE_TYPE_INSERT, 2L, 2000000L),
      makeChangeRow(3L, "c", CHANGE_TYPE_INSERT, 3L, 3000000L)))

    // Both exclusive: only version 2 included
    val expected = Seq(
      Row(2L, "b", CHANGE_TYPE_INSERT, 2L, new Timestamp(2000L)))

    // DataFrame API
    checkAnswer(
      spark.read
        .option("startingVersion", "1")
        .option("endingVersion", "3")
        .option("startingBoundInclusive", "false")
        .option("endingBoundInclusive", "false")
        .changes(fullTableName),
      expected)

    // SQL
    checkAnswer(
      sql(s"SELECT * FROM $fullTableName " +
        "CHANGES FROM VERSION 1 EXCLUSIVE TO VERSION 3 EXCLUSIVE"),
      expected)
  }

  // ---------- Batch: CDC options ----------

  test("changes() default deduplication mode is dropCarryovers") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", CHANGE_TYPE_INSERT, 1L, 1000000L)))

    // DataFrame API
    spark.read.option("startingVersion", "1").changes(fullTableName).collect()
    val info1 = catalog.lastChangelogInfo.get
    assert(info1.deduplicationMode() === ChangelogInfo.DeduplicationMode.DROP_CARRYOVERS)
    assert(info1.computeUpdates() === false)

    // SQL (no WITH clause = defaults)
    sql(s"SELECT * FROM $fullTableName CHANGES FROM VERSION 1").collect()
    val info2 = catalog.lastChangelogInfo.get
    assert(info2.deduplicationMode() === ChangelogInfo.DeduplicationMode.DROP_CARRYOVERS)
    assert(info2.computeUpdates() === false)
  }

  test("changes() with deduplicationMode none") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", CHANGE_TYPE_INSERT, 1L, 1000000L)))

    // DataFrame API
    spark.read
      .option("startingVersion", "1")
      .option("deduplicationMode", "none")
      .changes(fullTableName)
      .collect()
    assert(catalog.lastChangelogInfo.get.deduplicationMode() ===
      ChangelogInfo.DeduplicationMode.NONE)

    // SQL
    sql(s"SELECT * FROM $fullTableName CHANGES FROM VERSION 1 " +
      "WITH (deduplicationMode = 'none')").collect()
    assert(catalog.lastChangelogInfo.get.deduplicationMode() ===
      ChangelogInfo.DeduplicationMode.NONE)
  }

  test("changes() passes computeUpdates to catalog") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", CHANGE_TYPE_INSERT, 1L, 1000000L)))

    // DataFrame API
    spark.read
      .option("startingVersion", "1")
      .option("computeUpdates", "true")
      .changes(fullTableName)
      .collect()
    assert(catalog.lastChangelogInfo.get.computeUpdates() === true)

    // SQL
    sql(s"SELECT * FROM $fullTableName CHANGES FROM VERSION 1 " +
      "WITH (computeUpdates = 'true')").collect()
    assert(catalog.lastChangelogInfo.get.computeUpdates() === true)
  }

  // ---------- Batch: timestamp range ----------

  test("changes() with timestamp range") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", CHANGE_TYPE_INSERT, 1L, 1000000L)))

    // DataFrame API
    spark.read
      .option("startingTimestamp", "2024-01-01 00:00:00")
      .option("endingTimestamp", "2024-12-31 23:59:59")
      .changes(fullTableName)
      .collect()
    assert(catalog.lastChangelogInfo.get.range()
      .isInstanceOf[ChangelogRange.TimestampRange])

    // SQL
    sql(s"SELECT * FROM $fullTableName " +
      "CHANGES FROM TIMESTAMP '2024-01-01 00:00:00' " +
      "TO TIMESTAMP '2024-12-31 23:59:59'").collect()
    assert(catalog.lastChangelogInfo.get.range()
      .isInstanceOf[ChangelogRange.TimestampRange])
  }

  // ---------- Batch: error cases ----------

  test("changes() rejects user-specified schema") {
    val e = intercept[AnalysisException] {
      spark.read
        .schema("id LONG, data STRING")
        .option("startingVersion", "1")
        .changes(fullTableName)
    }
    assert(e.getMessage.contains("changes"))
  }

  // ---------- Streaming: basic data retrieval ----------

  test("streaming changes() returns change data") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", CHANGE_TYPE_INSERT, 1L, 1000000L),
      makeChangeRow(2L, "b", CHANGE_TYPE_INSERT, 1L, 1000000L),
      makeChangeRow(1L, "a", CHANGE_TYPE_DELETE, 2L, 2000000L)))

    val expected = Seq(
      Row(1L, "a", CHANGE_TYPE_INSERT, 1L, new Timestamp(1000L)),
      Row(2L, "b", CHANGE_TYPE_INSERT, 1L, new Timestamp(1000L)),
      Row(1L, "a", CHANGE_TYPE_DELETE, 2L, new Timestamp(2000L)))

    // DataFrame API
    val dfApiStream = spark.readStream
      .option("startingVersion", "1")
      .changes(fullTableName)
    val q1 = dfApiStream.writeStream
      .format("memory").queryName("cdc_stream_df").start()
    try {
      q1.processAllAvailable()
      checkAnswer(spark.sql("SELECT * FROM cdc_stream_df"), expected)
    } finally {
      q1.stop()
    }

    // SQL
    val sqlStream = sql(
      s"SELECT * FROM STREAM $fullTableName CHANGES FROM VERSION 1")
    val q2 = sqlStream.writeStream
      .format("memory").queryName("cdc_stream_sql").start()
    try {
      q2.processAllAvailable()
      checkAnswer(spark.sql("SELECT * FROM cdc_stream_sql"), expected)
    } finally {
      q2.stop()
    }
  }

  test("streaming changes() with startingVersion filters data") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", CHANGE_TYPE_INSERT, 1L, 1000000L),
      makeChangeRow(2L, "b", CHANGE_TYPE_INSERT, 1L, 1000000L),
      makeChangeRow(1L, "a", CHANGE_TYPE_DELETE, 2L, 2000000L)))

    val expected = Seq(
      Row(1L, "a", CHANGE_TYPE_DELETE, 2L, new Timestamp(2000L)))

    // DataFrame API
    val dfApiStream = spark.readStream
      .option("startingVersion", "2")
      .changes(fullTableName)
    val q1 = dfApiStream.writeStream
      .format("memory").queryName("cdc_stream_v2_df").start()
    try {
      q1.processAllAvailable()
      checkAnswer(spark.sql("SELECT * FROM cdc_stream_v2_df"), expected)
    } finally {
      q1.stop()
    }

    // SQL
    val sqlStream = sql(
      s"SELECT * FROM STREAM $fullTableName CHANGES FROM VERSION 2")
    val q2 = sqlStream.writeStream
      .format("memory").queryName("cdc_stream_v2_sql").start()
    try {
      q2.processAllAvailable()
      checkAnswer(spark.sql("SELECT * FROM cdc_stream_v2_sql"), expected)
    } finally {
      q2.stop()
    }
  }

  test("streaming changes() with projection and filter") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", CHANGE_TYPE_INSERT, 1L, 1000000L),
      makeChangeRow(2L, "b", CHANGE_TYPE_INSERT, 1L, 1000000L),
      makeChangeRow(3L, "c", CHANGE_TYPE_INSERT, 2L, 2000000L)))

    val expected = Seq(Row(1L, "a"), Row(2L, "b"))

    // DataFrame API
    val dfApiStream = spark.readStream
      .option("startingVersion", "1")
      .changes(fullTableName)
      .filter("_commit_version = 1")
      .select("id", "data")
    val q1 = dfApiStream.writeStream
      .format("memory").queryName("cdc_stream_proj_df").start()
    try {
      q1.processAllAvailable()
      checkAnswer(spark.sql("SELECT * FROM cdc_stream_proj_df"), expected)
    } finally {
      q1.stop()
    }

    // SQL
    val sqlStream = sql(
      s"SELECT id, data FROM STREAM $fullTableName " +
        "CHANGES FROM VERSION 1 WHERE _commit_version = 1")
    val q2 = sqlStream.writeStream
      .format("memory").queryName("cdc_stream_proj_sql").start()
    try {
      q2.processAllAvailable()
      checkAnswer(spark.sql("SELECT * FROM cdc_stream_proj_sql"), expected)
    } finally {
      q2.stop()
    }
  }

  // ---------- Streaming: CDC options ----------

  test("streaming changes() passes computeUpdates to catalog") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", CHANGE_TYPE_INSERT, 1L, 1000000L)))

    // DataFrame API
    val dfApiStream = spark.readStream
      .option("startingVersion", "1")
      .option("computeUpdates", "true")
      .changes(fullTableName)
    val q1 = dfApiStream.writeStream
      .format("memory").queryName("cdc_stream_opts_df").start()
    try {
      q1.processAllAvailable()
      assert(catalog.lastChangelogInfo.get.computeUpdates() === true)
    } finally {
      q1.stop()
    }

    // SQL
    val sqlStream = sql(
      s"SELECT * FROM STREAM $fullTableName CHANGES FROM VERSION 1 " +
        "WITH (computeUpdates = 'true')")
    val q2 = sqlStream.writeStream
      .format("memory").queryName("cdc_stream_opts_sql").start()
    try {
      q2.processAllAvailable()
      assert(catalog.lastChangelogInfo.get.computeUpdates() === true)
    } finally {
      q2.stop()
    }
  }

  // ---------- Streaming: .name() API ----------

  test("streaming changes() supports .name() API with source evolution enabled") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", CHANGE_TYPE_INSERT, 1L, 1000000L)))

    val expected = Seq(
      Row(1L, "a", CHANGE_TYPE_INSERT, 1L, new Timestamp(1000L)))

    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
      val stream = spark.readStream
        .name("my_cdc_source")
        .option("startingVersion", "1")
        .changes(fullTableName)

      // Verify the logical plan contains NamedStreamingRelation with the user-provided name
      val plan = stream.queryExecution.logical
      val namedRelations = plan.collect {
        case n: NamedStreamingRelation => n
      }
      assert(namedRelations.size === 1)
      assert(namedRelations.head.sourceIdentifyingName === UserProvided("my_cdc_source"))

      val q = stream.writeStream
        .format("memory").queryName("cdc_stream_named").start()
      try {
        q.processAllAvailable()
        checkAnswer(spark.sql("SELECT * FROM cdc_stream_named"), expected)
      } finally {
        q.stop()
      }
    }
  }

  // ---------- Streaming: error cases ----------

  test("streaming changes() rejects user-specified schema") {
    val e = intercept[AnalysisException] {
      spark.readStream
        .schema("id LONG, data STRING")
        .option("startingVersion", "1")
        .changes(fullTableName)
    }
    assert(e.getMessage.contains("changes"))
  }

  // ---------- Streaming: row-level post-processing ----------
  //
  // Streaming row-level passes (carry-over removal, update detection) rewrite the plan
  // into Aggregate(rowId, _commit_version, _commit_timestamp) -> [Filter] ->
  // Generate(Inline(events)) -> [relabel Project], under an EventTimeWatermark on
  // _commit_timestamp.

  /** Schema variant for post-processing tests: includes `row_commit_version`. */
  private def recreateWithRowVersion(): Identifier = {
    val id = ident
    val cat = catalog
    if (cat.tableExists(id)) cat.dropTable(id)
    cat.createTable(
      id,
      Array(
        Column.create("id", LongType, false),
        Column.create("data", StringType),
        Column.create("row_commit_version", LongType, false)),
      Array.empty,
      new util.HashMap[String, String]())
    cat.clearChangeRows(id)
    id
  }

  /** Row constructor for the row-version-enabled schema. */
  private def ppRow(
      id: Long,
      data: String,
      rcv: Long,
      changeType: String,
      commitVersion: Long,
      commitTimestampMicros: Long): InternalRow = {
    InternalRow(
      id,
      UTF8String.fromString(data),
      rcv,
      UTF8String.fromString(changeType),
      commitVersion,
      commitTimestampMicros)
  }

  test("streaming carry-over removal drops CoW pairs") {
    val id = recreateWithRowVersion()
    catalog.setChangelogProperties(id, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    catalog.addChangeRows(id, Seq(
      // v1: insert Alice (rcv=1), Bob (rcv=1)
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_INSERT, 1L, 1000000L),
      ppRow(2L, "Bob", 1L, CHANGE_TYPE_INSERT, 1L, 1000000L),
      // v2: real delete Alice + carry-over for Bob (rcv unchanged)
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_DELETE, 2L, 2000000L),
      ppRow(2L, "Bob", 1L, CHANGE_TYPE_DELETE, 2L, 2000000L),
      ppRow(2L, "Bob", 1L, CHANGE_TYPE_INSERT, 2L, 2000000L)))

    val q = spark.readStream
      .option("startingVersion", "1")
      .changes(fullTableName)
      .select("id", "data", "_change_type", "_commit_version")
      .writeStream
      .format("memory")
      .queryName("cdc_stream_carryover")
      .outputMode("append")
      .start()
    try {
      q.processAllAvailable()
      // The next micro-batch advances the input watermark to the max _commit_timestamp
      // seen in the previous batch; append-mode aggregate eviction (eventTime <= watermark)
      // then emits all groups including the highest commit. v1 inserts + Alice's real
      // delete survive; Bob's carry-over pair at v2 is dropped.
      checkAnswer(
        spark.sql("SELECT * FROM cdc_stream_carryover"),
        Seq(
          Row(1L, "Alice", CHANGE_TYPE_INSERT, 1L),
          Row(2L, "Bob", CHANGE_TYPE_INSERT, 1L),
          Row(1L, "Alice", CHANGE_TYPE_DELETE, 2L)))
    } finally {
      q.stop()
    }
  }

  test("streaming update detection relabels delete+insert as update") {
    val id = recreateWithRowVersion()
    catalog.setChangelogProperties(id, ChangelogProperties(
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    catalog.addChangeRows(id, Seq(
      // v1: insert Alice (rcv=1)
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_INSERT, 1L, 1000000L),
      // v2: real update Alice -> Robert (delete old, insert new)
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_DELETE, 2L, 2000000L),
      ppRow(1L, "Robert", 2L, CHANGE_TYPE_INSERT, 2L, 2000000L)))

    val q = spark.readStream
      .option("startingVersion", "1")
      .option("computeUpdates", "true")
      .option("deduplicationMode", "none")
      .changes(fullTableName)
      .select("id", "data", "_change_type", "_commit_version")
      .writeStream
      .format("memory")
      .queryName("cdc_stream_updates")
      .outputMode("append")
      .start()
    try {
      q.processAllAvailable()
      checkAnswer(
        spark.sql("SELECT * FROM cdc_stream_updates"),
        Seq(
          Row(1L, "Alice", CHANGE_TYPE_INSERT, 1L),
          Row(1L, "Alice", CHANGE_TYPE_UPDATE_PREIMAGE, 2L),
          Row(1L, "Robert", CHANGE_TYPE_UPDATE_POSTIMAGE, 2L)))
    } finally {
      q.stop()
    }
  }

  // TransformWithState requires the RocksDB state store backend.
  private val rocksDbProviderConf = SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"

  test("streaming netChanges collapses INSERT then DELETE to no output") {
    val id = recreateWithRowVersion()
    catalog.setChangelogProperties(id, ChangelogProperties(
      containsIntermediateChanges = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    catalog.addChangeRows(id, Seq(
      // v1: insert Alice (rcv=1)
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_INSERT, 1L, 1000000L),
      // v2: delete Alice -- net cancels out
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_DELETE, 2L, 2000000L),
      // v3: insert Bob -- emits at end-of-input flush
      ppRow(2L, "Bob", 3L, CHANGE_TYPE_INSERT, 3L, 3000000L)))

    withSQLConf(rocksDbProviderConf) {
      val q = spark.readStream
        .option("startingVersion", "1")
        .option("deduplicationMode", "netChanges")
        .changes(fullTableName)
        .select("id", "data", "_change_type", "_commit_version")
        .writeStream
        .format("memory")
        .queryName("cdc_stream_netchanges_cancel")
        .outputMode("append")
        .start()
      try {
        q.processAllAvailable()
        // End-of-input flushes all timers so Bob's insert emits.
        // Alice's INSERT then DELETE cancels out (no row), and the final "Bob" stays.
        checkAnswer(
          spark.sql("SELECT * FROM cdc_stream_netchanges_cancel"),
          Seq(Row(2L, "Bob", CHANGE_TYPE_INSERT, 3L)))
      } finally {
        q.stop()
      }
    }
  }

  test("streaming netChanges with computeUpdates labels persisting rows as updates") {
    val id = recreateWithRowVersion()
    catalog.setChangelogProperties(id, ChangelogProperties(
      containsIntermediateChanges = true,
      representsUpdateAsDeleteAndInsert = false, // updates already materialized
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    // Row identity 1 already exists before the stream window, so the first event we
    // observe is its update_preimage -> existedBefore = true. The last event is the
    // update_postimage in v2 -> existsAfter = true. With computeUpdates = true the
    // (true, true) cell of the SPIP matrix emits a relabeled
    // update_preimage + update_postimage pair (rather than delete + insert).
    catalog.addChangeRows(id, Seq(
      // v1: pre-existing Alice updated to Bob
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_UPDATE_PREIMAGE, 1L, 1000000L),
      ppRow(1L, "Bob", 1L, CHANGE_TYPE_UPDATE_POSTIMAGE, 1L, 1000000L),
      // v2: Bob updated to Robert -- the v1 preimage and the v2 postimage are the
      // first and last events for row identity 1 across the entire range.
      ppRow(1L, "Bob", 1L, CHANGE_TYPE_UPDATE_PREIMAGE, 2L, 2000000L),
      ppRow(1L, "Robert", 2L, CHANGE_TYPE_UPDATE_POSTIMAGE, 2L, 2000000L)))

    withSQLConf(rocksDbProviderConf) {
      val q = spark.readStream
        .option("startingVersion", "1")
        .option("deduplicationMode", "netChanges")
        .option("computeUpdates", "true")
        .changes(fullTableName)
        .select("id", "data", "_change_type")
        .writeStream
        .format("memory")
        .queryName("cdc_stream_netchanges_update")
        .outputMode("append")
        .start()
      try {
        q.processAllAvailable()
        checkAnswer(
          spark.sql("SELECT * FROM cdc_stream_netchanges_update"),
          Seq(
            Row(1L, "Alice", CHANGE_TYPE_UPDATE_PREIMAGE),
            Row(1L, "Robert", CHANGE_TYPE_UPDATE_POSTIMAGE)))
      } finally {
        q.stop()
      }
    }
  }

  // The streaming row-level rewrite injects a streaming Aggregate, which is only
  // semantically valid with Append output mode (Update / Complete would re-emit
  // per-batch updates or the entire result table per batch, neither of which matches
  // batch CDC semantics). UnsupportedOperationChecker now rejects those modes.

  test("streaming row-level post-processing with update output mode is rejected") {
    val id = recreateWithRowVersion()
    catalog.setChangelogProperties(id, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))
    catalog.addChangeRows(id, Seq(
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_INSERT, 1L, 1000000L)))

    val e = intercept[AnalysisException] {
      spark.readStream
        .option("startingVersion", "1")
        .changes(fullTableName)
        .writeStream
        .format("memory")
        .queryName("cdc_stream_update_rejected")
        .outputMode("update")
        .start()
    }
    assert(e.getCondition == "STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION",
      s"Unexpected error: ${e.getMessage}")
    assert(e.getMessage.contains("Change Data Capture"),
      s"Error should mention CDC: ${e.getMessage}")
  }

  test("streaming row-level post-processing with complete output mode is rejected") {
    val id = recreateWithRowVersion()
    catalog.setChangelogProperties(id, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))
    catalog.addChangeRows(id, Seq(
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_INSERT, 1L, 1000000L)))

    val e = intercept[AnalysisException] {
      spark.readStream
        .option("startingVersion", "1")
        .changes(fullTableName)
        .writeStream
        .format("memory")
        .queryName("cdc_stream_complete_rejected")
        .outputMode("complete")
        .start()
    }
    assert(e.getCondition == "STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION",
      s"Unexpected error: ${e.getMessage}")
    assert(e.getMessage.contains("Change Data Capture"),
      s"Error should mention CDC: ${e.getMessage}")
  }

  // The streaming netChanges-only path injects a TransformWithState whose internal
  // outputMode is Append. Without an explicit per-operator check the analyzer would
  // happily accept the user requesting Update output mode at the writer, even though
  // the rewrite is only valid for Append (Update would re-emit per-batch state changes
  // that don't match batch netChanges semantics). UnsupportedOperationChecker therefore
  // detects the netChanges processor and rejects non-Append modes with a clear error.
  // (Complete is also rejected by the generic "Complete requires aggregations" check
  // since the netChanges-only path has no streaming Aggregate, but we assert it here
  // too for symmetry with the row-level rejection tests above.)

  test("streaming netChanges with update output mode is rejected") {
    val id = recreateWithRowVersion()
    catalog.setChangelogProperties(id, ChangelogProperties(
      containsIntermediateChanges = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))
    catalog.addChangeRows(id, Seq(
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_INSERT, 1L, 1000000L)))

    val e = intercept[AnalysisException] {
      withSQLConf(rocksDbProviderConf) {
        spark.readStream
          .option("startingVersion", "1")
          .option("deduplicationMode", "netChanges")
          .changes(fullTableName)
          .writeStream
          .format("memory")
          .queryName("cdc_stream_netchanges_update_rejected")
          .outputMode("update")
          .start()
      }
    }
    assert(e.getCondition == "STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION",
      s"Unexpected error: ${e.getMessage}")
    assert(e.getMessage.contains("Change Data Capture"),
      s"Error should mention CDC: ${e.getMessage}")
  }

  test("streaming netChanges with complete output mode is rejected") {
    val id = recreateWithRowVersion()
    catalog.setChangelogProperties(id, ChangelogProperties(
      containsIntermediateChanges = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))
    catalog.addChangeRows(id, Seq(
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_INSERT, 1L, 1000000L)))

    val e = intercept[AnalysisException] {
      withSQLConf(rocksDbProviderConf) {
        spark.readStream
          .option("startingVersion", "1")
          .option("deduplicationMode", "netChanges")
          .changes(fullTableName)
          .writeStream
          .format("memory")
          .queryName("cdc_stream_netchanges_complete_rejected")
          .outputMode("complete")
          .start()
      }
    }
    assert(e.getCondition == "STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION",
      s"Unexpected error: ${e.getMessage}")
  }

  test("streaming row-level rewrite raises on NULL _commit_timestamp") {
    val id = recreateWithRowVersion()
    catalog.setChangelogProperties(id, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    // Insert a row with NULL _commit_timestamp (last column).
    val row = InternalRow(
      1L, UTF8String.fromString("Alice"), 1L,
      UTF8String.fromString(CHANGE_TYPE_INSERT), 1L, null)
    catalog.addChangeRows(id, Seq(row))

    val q = spark.readStream
      .option("startingVersion", "1")
      .changes(fullTableName)
      .writeStream
      .format("memory")
      .queryName("cdc_stream_null_ts")
      .outputMode("append")
      .start()
    try {
      val e = intercept[org.apache.spark.sql.streaming.StreamingQueryException] {
        q.processAllAvailable()
      }
      // The CHANGELOG_CONTRACT_VIOLATION runtime error wraps the message; it should
      // mention NULL_COMMIT_TIMESTAMP somewhere in the chain.
      assert(e.getMessage.contains("NULL_COMMIT_TIMESTAMP") ||
        Option(e.getCause).map(_.getMessage).getOrElse("").contains("NULL_COMMIT_TIMESTAMP"),
        s"Expected NULL_COMMIT_TIMESTAMP in the error chain. Got: ${e.getMessage}")
    } finally {
      q.stop()
    }
  }

  test("streaming netChanges emits DELETE for pre-existing row deleted in range") {
    // Exercises the SPIP `(true, false)` cell: existedBefore = true (first event is a
    // delete or update_preimage), existsAfter = false (last event is a delete).
    val id = recreateWithRowVersion()
    catalog.setChangelogProperties(id, ChangelogProperties(
      containsIntermediateChanges = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    catalog.addChangeRows(id, Seq(
      // v1: pre-existing Alice gets updated to Bob.
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_UPDATE_PREIMAGE, 1L, 1000000L),
      ppRow(1L, "Bob", 1L, CHANGE_TYPE_UPDATE_POSTIMAGE, 1L, 1000000L),
      // v2: Bob deleted -- the v1 preimage is the first event and the v2 delete is
      // the last event for row identity 1 across the entire range.
      ppRow(1L, "Bob", 1L, CHANGE_TYPE_DELETE, 2L, 2000000L),
      // v3: insert Carol -- gives the watermark something to advance past, so row
      // identity 1's timer fires before end-of-input.
      ppRow(2L, "Carol", 3L, CHANGE_TYPE_INSERT, 3L, 3000000L)))

    withSQLConf(rocksDbProviderConf) {
      val q = spark.readStream
        .option("startingVersion", "1")
        .option("deduplicationMode", "netChanges")
        .changes(fullTableName)
        .select("id", "data", "_change_type")
        .writeStream
        .format("memory")
        .queryName("cdc_stream_netchanges_delete")
        .outputMode("append")
        .start()
      try {
        q.processAllAvailable()
        checkAnswer(
          spark.sql("SELECT * FROM cdc_stream_netchanges_delete"),
          Seq(
            // (true, false): emit a single DELETE carrying the *first* event's data
            // (the preimage), per the batch contract.
            Row(1L, "Alice", CHANGE_TYPE_DELETE),
            Row(2L, "Carol", CHANGE_TYPE_INSERT)))
      } finally {
        q.stop()
      }
    }
  }

  test("streaming netChanges without computeUpdates keeps persisting rows as DELETE+INSERT") {
    // Exercises the SPIP `(true, true)` cell with computeUpdates = false: the pair is
    // emitted as DELETE + INSERT rather than relabeled as
    // update_preimage + update_postimage.
    val id = recreateWithRowVersion()
    catalog.setChangelogProperties(id, ChangelogProperties(
      containsIntermediateChanges = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    catalog.addChangeRows(id, Seq(
      // v1: pre-existing Alice updated to Bob.
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_UPDATE_PREIMAGE, 1L, 1000000L),
      ppRow(1L, "Bob", 1L, CHANGE_TYPE_UPDATE_POSTIMAGE, 1L, 1000000L),
      // v2: Bob updated to Robert -- row identity 1 spans (preimage Alice) ..
      // (postimage Robert).
      ppRow(1L, "Bob", 1L, CHANGE_TYPE_UPDATE_PREIMAGE, 2L, 2000000L),
      ppRow(1L, "Robert", 2L, CHANGE_TYPE_UPDATE_POSTIMAGE, 2L, 2000000L)))

    withSQLConf(rocksDbProviderConf) {
      val q = spark.readStream
        .option("startingVersion", "1")
        .option("deduplicationMode", "netChanges")
        // computeUpdates defaults to false.
        .changes(fullTableName)
        .select("id", "data", "_change_type")
        .writeStream
        .format("memory")
        .queryName("cdc_stream_netchanges_no_compute_updates")
        .outputMode("append")
        .start()
      try {
        q.processAllAvailable()
        checkAnswer(
          spark.sql("SELECT * FROM cdc_stream_netchanges_no_compute_updates"),
          Seq(
            Row(1L, "Alice", CHANGE_TYPE_DELETE),
            Row(1L, "Robert", CHANGE_TYPE_INSERT)))
      } finally {
        q.stop()
      }
    }
  }

  test("streaming netChanges + carry-over removal: combined post-processing") {
    // Validates the design point that the row-level rewrite and the netChanges rewrite
    // share a single EventTimeWatermark on `_commit_timestamp` and produce the
    // expected combined result. Carry-over CoW pairs are dropped before the netChanges
    // collapse runs, so the final emission only reflects real content changes.
    val id = recreateWithRowVersion()
    catalog.setChangelogProperties(id, ChangelogProperties(
      containsCarryoverRows = true,
      containsIntermediateChanges = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    catalog.addChangeRows(id, Seq(
      // v1: insert Alice, Bob (rcv=1).
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_INSERT, 1L, 1000000L),
      ppRow(2L, "Bob", 1L, CHANGE_TYPE_INSERT, 1L, 1000000L),
      // v2: real delete Alice + carry-over for Bob (rcv unchanged means CoW rewrite,
      // no content change).
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_DELETE, 2L, 2000000L),
      ppRow(2L, "Bob", 1L, CHANGE_TYPE_DELETE, 2L, 2000000L),
      ppRow(2L, "Bob", 1L, CHANGE_TYPE_INSERT, 2L, 2000000L),
      // v3: insert Carol -- advances the watermark past v2 so timers for row
      // identities 1 and 2 fire and the netChanges output is emitted.
      ppRow(3L, "Carol", 3L, CHANGE_TYPE_INSERT, 3L, 3000000L)))

    withSQLConf(rocksDbProviderConf) {
      val q = spark.readStream
        .option("startingVersion", "1")
        .option("deduplicationMode", "netChanges")
        .changes(fullTableName)
        .select("id", "data", "_change_type")
        .writeStream
        .format("memory")
        .queryName("cdc_stream_netchanges_with_carryover")
        .outputMode("append")
        .start()
      try {
        q.processAllAvailable()
        // After carry-over removal: Alice has v1 INSERT + v2 DELETE; Bob has only
        // v1 INSERT (the v2 CoW pair was dropped); Carol has v3 INSERT.
        // After netChanges:
        //   Alice -- (false, false) -> no output
        //   Bob   -- (false, true)  -> emit INSERT
        //   Carol -- (false, true)  -> emit INSERT
        checkAnswer(
          spark.sql("SELECT * FROM cdc_stream_netchanges_with_carryover"),
          Seq(
            Row(2L, "Bob", CHANGE_TYPE_INSERT),
            Row(3L, "Carol", CHANGE_TYPE_INSERT)))
      } finally {
        q.stop()
      }
    }
  }

  // ---------- Streaming: extended row-level post-processing coverage ----------

  test("streaming carry-over removal with composite rowId removes pairs per (id, name)") {
    val id = recreateWithRowVersion()
    catalog.setChangelogProperties(id, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id", "data"),
      rowVersionName = Some("row_commit_version")))

    catalog.addChangeRows(id, Seq(
      // v1: insert two rows that share id=1 but different `data`. The composite rowId
      // (id, data) means each is its own row identity.
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_INSERT, 1L, 1000000L),
      ppRow(1L, "Bob", 1L, CHANGE_TYPE_INSERT, 1L, 1000000L),
      // v2: CoW carry-over for (1, "Alice"); real delete for (1, "Bob").
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_DELETE, 2L, 2000000L),
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_INSERT, 2L, 2000000L),
      ppRow(1L, "Bob", 1L, CHANGE_TYPE_DELETE, 2L, 2000000L)))

    val q = spark.readStream
      .option("startingVersion", "1")
      .changes(fullTableName)
      .select("id", "data", "_change_type", "_commit_version")
      .writeStream
      .format("memory")
      .queryName("cdc_stream_composite_carryover")
      .outputMode("append")
      .start()
    try {
      q.processAllAvailable()
      // (1, "Alice") carry-over is dropped; (1, "Bob") delete survives. With broken
      // single-column rowId partitioning the four v2 rows would collapse into one
      // partition with del_cnt=2 / ins_cnt=1 and no rows would qualify as carry-over.
      checkAnswer(
        spark.sql("SELECT * FROM cdc_stream_composite_carryover"),
        Seq(
          Row(1L, "Alice", CHANGE_TYPE_INSERT, 1L),
          Row(1L, "Bob", CHANGE_TYPE_INSERT, 1L),
          Row(1L, "Bob", CHANGE_TYPE_DELETE, 2L)))
    } finally {
      q.stop()
    }
  }

  test("streaming update detection with composite rowId keeps different tuples raw") {
    val id = recreateWithRowVersion()
    catalog.setChangelogProperties(id, ChangelogProperties(
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id", "data"),
      rowVersionName = Some("row_commit_version")))

    catalog.addChangeRows(id, Seq(
      // v1: insert pre-existing Alice and Bob (so v2 has rows to fall through).
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_INSERT, 1L, 1000000L),
      ppRow(1L, "Bob", 1L, CHANGE_TYPE_INSERT, 1L, 1000000L),
      // v2: delete (1, "Alice") and insert (1, "Bob"). These are DIFFERENT composite
      // rowIds; they MUST NOT be relabeled as update.
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_DELETE, 2L, 2000000L),
      ppRow(1L, "Carol", 2L, CHANGE_TYPE_INSERT, 2L, 2000000L)))

    val q = spark.readStream
      .option("startingVersion", "1")
      .option("computeUpdates", "true")
      .option("deduplicationMode", "none")
      .changes(fullTableName)
      .select("id", "data", "_change_type")
      .writeStream
      .format("memory")
      .queryName("cdc_stream_composite_updates")
      .outputMode("append")
      .start()
    try {
      q.processAllAvailable()
      checkAnswer(
        spark.sql("SELECT * FROM cdc_stream_composite_updates"),
        Seq(
          Row(1L, "Alice", CHANGE_TYPE_INSERT),
          Row(1L, "Bob", CHANGE_TYPE_INSERT),
          Row(1L, "Alice", CHANGE_TYPE_DELETE),
          Row(1L, "Carol", CHANGE_TYPE_INSERT)))
    } finally {
      q.stop()
    }
  }

  test("streaming carry-over removal and update detection across multiple commits") {
    val id = recreateWithRowVersion()
    catalog.setChangelogProperties(id, ChangelogProperties(
      containsCarryoverRows = true,
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    catalog.addChangeRows(id, Seq(
      // v1: insert 3 rows.
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_INSERT, 1L, 1000000L),
      ppRow(2L, "Bob", 1L, CHANGE_TYPE_INSERT, 1L, 1000000L),
      ppRow(3L, "Charlie", 1L, CHANGE_TYPE_INSERT, 1L, 1000000L),
      // v2: real delete Alice; CoW carry-overs for Bob/Charlie.
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_DELETE, 2L, 2000000L),
      ppRow(2L, "Bob", 1L, CHANGE_TYPE_DELETE, 2L, 2000000L),
      ppRow(2L, "Bob", 1L, CHANGE_TYPE_INSERT, 2L, 2000000L),
      ppRow(3L, "Charlie", 1L, CHANGE_TYPE_DELETE, 2L, 2000000L),
      ppRow(3L, "Charlie", 1L, CHANGE_TYPE_INSERT, 2L, 2000000L),
      // v3: real update Bob -> Robert (rcv bumps); CoW for Charlie.
      ppRow(2L, "Bob", 1L, CHANGE_TYPE_DELETE, 3L, 3000000L),
      ppRow(2L, "Robert", 3L, CHANGE_TYPE_INSERT, 3L, 3000000L),
      ppRow(3L, "Charlie", 1L, CHANGE_TYPE_DELETE, 3L, 3000000L),
      ppRow(3L, "Charlie", 1L, CHANGE_TYPE_INSERT, 3L, 3000000L),
      // v4: insert Diana.
      ppRow(4L, "Diana", 4L, CHANGE_TYPE_INSERT, 4L, 4000000L)))

    val q = spark.readStream
      .option("startingVersion", "1")
      .option("computeUpdates", "true")
      .changes(fullTableName)
      .select("id", "data", "_change_type", "_commit_version")
      .writeStream
      .format("memory")
      .queryName("cdc_stream_multi_commit")
      .outputMode("append")
      .start()
    try {
      q.processAllAvailable()
      val result = spark.sql(
        "SELECT * FROM cdc_stream_multi_commit ORDER BY _commit_version, id, _change_type")
      checkAnswer(
        result,
        Seq(
          Row(1L, "Alice", CHANGE_TYPE_INSERT, 1L),
          Row(2L, "Bob", CHANGE_TYPE_INSERT, 1L),
          Row(3L, "Charlie", CHANGE_TYPE_INSERT, 1L),
          // v2: only Alice's real delete survives (Bob and Charlie carry-over dropped).
          Row(1L, "Alice", CHANGE_TYPE_DELETE, 2L),
          // v3: Bob -> Robert relabeled as update_pre/postimage; Charlie carry-over
          // dropped. The ORDER BY breaks ties on `_change_type` ascending where
          // `update_postimage` < `update_preimage` alphabetically.
          Row(2L, "Robert", CHANGE_TYPE_UPDATE_POSTIMAGE, 3L),
          Row(2L, "Bob", CHANGE_TYPE_UPDATE_PREIMAGE, 3L),
          // v4
          Row(4L, "Diana", CHANGE_TYPE_INSERT, 4L)))
    } finally {
      q.stop()
    }
  }

  test("streaming DELETE-all-rows: only deletes survive at the deleting commit") {
    val id = recreateWithRowVersion()
    catalog.setChangelogProperties(id, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    catalog.addChangeRows(id, Seq(
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_INSERT, 1L, 1000000L),
      ppRow(2L, "Bob", 1L, CHANGE_TYPE_INSERT, 1L, 1000000L),
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_DELETE, 2L, 2000000L),
      ppRow(2L, "Bob", 1L, CHANGE_TYPE_DELETE, 2L, 2000000L)))

    val q = spark.readStream
      .option("startingVersion", "1")
      .changes(fullTableName)
      .select("id", "data", "_change_type", "_commit_version")
      .writeStream
      .format("memory")
      .queryName("cdc_stream_delete_all")
      .outputMode("append")
      .start()
    try {
      q.processAllAvailable()
      checkAnswer(
        spark.sql("SELECT * FROM cdc_stream_delete_all"),
        Seq(
          Row(1L, "Alice", CHANGE_TYPE_INSERT, 1L),
          Row(2L, "Bob", CHANGE_TYPE_INSERT, 1L),
          Row(1L, "Alice", CHANGE_TYPE_DELETE, 2L),
          Row(2L, "Bob", CHANGE_TYPE_DELETE, 2L)))
    } finally {
      q.stop()
    }
  }

  test("streaming UPDATE-all-rows: every row gets update_pre/postimage") {
    val id = recreateWithRowVersion()
    catalog.setChangelogProperties(id, ChangelogProperties(
      containsCarryoverRows = true,
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    catalog.addChangeRows(id, Seq(
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_INSERT, 1L, 1000000L),
      ppRow(2L, "Bob", 1L, CHANGE_TYPE_INSERT, 1L, 1000000L),
      // v2: every row is a real update (different rcv on pre vs post).
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_DELETE, 2L, 2000000L),
      ppRow(1L, "AliceUpdated", 2L, CHANGE_TYPE_INSERT, 2L, 2000000L),
      ppRow(2L, "Bob", 1L, CHANGE_TYPE_DELETE, 2L, 2000000L),
      ppRow(2L, "BobUpdated", 2L, CHANGE_TYPE_INSERT, 2L, 2000000L)))

    val q = spark.readStream
      .option("startingVersion", "1")
      .option("computeUpdates", "true")
      .changes(fullTableName)
      .select("id", "data", "_change_type", "_commit_version")
      .writeStream
      .format("memory")
      .queryName("cdc_stream_update_all")
      .outputMode("append")
      .start()
    try {
      q.processAllAvailable()
      checkAnswer(
        spark.sql("SELECT * FROM cdc_stream_update_all"),
        Seq(
          Row(1L, "Alice", CHANGE_TYPE_INSERT, 1L),
          Row(2L, "Bob", CHANGE_TYPE_INSERT, 1L),
          Row(1L, "Alice", CHANGE_TYPE_UPDATE_PREIMAGE, 2L),
          Row(1L, "AliceUpdated", CHANGE_TYPE_UPDATE_POSTIMAGE, 2L),
          Row(2L, "Bob", CHANGE_TYPE_UPDATE_PREIMAGE, 2L),
          Row(2L, "BobUpdated", CHANGE_TYPE_UPDATE_POSTIMAGE, 2L)))
    } finally {
      q.stop()
    }
  }

  test("streaming append-only workload: all inserts pass through") {
    val id = recreateWithRowVersion()
    catalog.setChangelogProperties(id, ChangelogProperties(
      // Append-only connectors typically declare no special CDC capabilities.
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    catalog.addChangeRows(id, Seq(
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_INSERT, 1L, 1000000L),
      ppRow(2L, "Bob", 2L, CHANGE_TYPE_INSERT, 2L, 2000000L),
      ppRow(3L, "Charlie", 3L, CHANGE_TYPE_INSERT, 3L, 3000000L)))

    val q = spark.readStream
      .option("startingVersion", "1")
      .changes(fullTableName)
      .select("id", "data", "_change_type", "_commit_version")
      .writeStream
      .format("memory")
      .queryName("cdc_stream_append_only")
      .outputMode("append")
      .start()
    try {
      q.processAllAvailable()
      checkAnswer(
        spark.sql("SELECT * FROM cdc_stream_append_only"),
        Seq(
          Row(1L, "Alice", CHANGE_TYPE_INSERT, 1L),
          Row(2L, "Bob", CHANGE_TYPE_INSERT, 2L),
          Row(3L, "Charlie", CHANGE_TYPE_INSERT, 3L)))
    } finally {
      q.stop()
    }
  }

  test("streaming no-op UPDATE is labeled as update (rcv differs on pre/post)") {
    val id = recreateWithRowVersion()
    catalog.setChangelogProperties(id, ChangelogProperties(
      containsCarryoverRows = true,
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    catalog.addChangeRows(id, Seq(
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_INSERT, 1L, 1000000L),
      // v2 no-op update: identical data, but rcv differs (Delta bumps it on every UPDATE).
      // Carry-over filter requires _min_rv = _max_rv, which is false here, so this is
      // correctly treated as a real update -- not a carry-over.
      ppRow(1L, "Alice", 1L, CHANGE_TYPE_DELETE, 2L, 2000000L),
      ppRow(1L, "Alice", 2L, CHANGE_TYPE_INSERT, 2L, 2000000L)))

    val q = spark.readStream
      .option("startingVersion", "1")
      .option("computeUpdates", "true")
      .changes(fullTableName)
      .select("id", "data", "_change_type", "_commit_version")
      .writeStream
      .format("memory")
      .queryName("cdc_stream_noop_update")
      .outputMode("append")
      .start()
    try {
      q.processAllAvailable()
      checkAnswer(
        spark.sql("SELECT * FROM cdc_stream_noop_update"),
        Seq(
          Row(1L, "Alice", CHANGE_TYPE_INSERT, 1L),
          Row(1L, "Alice", CHANGE_TYPE_UPDATE_PREIMAGE, 2L),
          Row(1L, "Alice", CHANGE_TYPE_UPDATE_POSTIMAGE, 2L)))
    } finally {
      q.stop()
    }
  }

  test("streaming carry-over removal at scale: many CoW pairs, one real change") {
    val id = recreateWithRowVersion()
    catalog.setChangelogProperties(id, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    val v1Inserts = (1 to 10).map { i =>
      ppRow(i.toLong, ('A' + i - 1).toChar.toString, 1L,
        CHANGE_TYPE_INSERT, 1L, 1000000L)
    }
    // At v2: row 5 is really deleted; the other 9 rows are CoW carry-over pairs
    // (rcv unchanged on both sides means content unchanged).
    val v2CarryOvers = (1 to 10).filter(_ != 5).flatMap { i =>
      val name = ('A' + i - 1).toChar.toString
      Seq(
        ppRow(i.toLong, name, 1L, CHANGE_TYPE_DELETE, 2L, 2000000L),
        ppRow(i.toLong, name, 1L, CHANGE_TYPE_INSERT, 2L, 2000000L))
    }
    val v2RealDelete = Seq(ppRow(5L, "E", 1L, CHANGE_TYPE_DELETE, 2L, 2000000L))
    catalog.addChangeRows(id, v1Inserts ++ v2CarryOvers ++ v2RealDelete)

    val q = spark.readStream
      .option("startingVersion", "2")
      .option("endingVersion", "2")
      .changes(fullTableName)
      .select("id", "data", "_change_type")
      .writeStream
      .format("memory")
      .queryName("cdc_stream_many_carryovers")
      .outputMode("append")
      .start()
    try {
      q.processAllAvailable()
      // 9 carry-over pairs dropped; only the real delete of row 5 survives.
      checkAnswer(
        spark.sql("SELECT * FROM cdc_stream_many_carryovers"),
        Seq(Row(5L, "E", CHANGE_TYPE_DELETE)))
    } finally {
      q.stop()
    }
  }
}
