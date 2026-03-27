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

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NamedStreamingRelation
import org.apache.spark.sql.catalyst.streaming.UserProvided
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * End-to-end tests for Change Data Capture (CDC) queries using
 * [[InMemoryChangelogCatalog]].
 */
class ChangelogEndToEndSuite extends QueryTest with SharedSparkSession {

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
      makeChangeRow(1L, "a", "insert", 1L, 1000000L),
      makeChangeRow(2L, "b", "delete", 2L, 2000000L)))

    val expected = Seq(
      Row(1L, "a", "insert", 1L, new Timestamp(1000L)),
      Row(2L, "b", "delete", 2L, new Timestamp(2000L)))

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
      makeChangeRow(1L, "a", "insert", 1L, 1000000L),
      makeChangeRow(2L, "b", "insert", 2L, 2000000L),
      makeChangeRow(3L, "c", "insert", 3L, 3000000L)))

    val expected = Seq(
      Row(2L, "b", "insert", 2L, new Timestamp(2000L)),
      Row(3L, "c", "insert", 3L, new Timestamp(3000L)))

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
      makeChangeRow(1L, "a", "insert", 1L, 1000000L),
      makeChangeRow(2L, "b", "delete", 2L, 2000000L)))

    val expected = Seq(
      Row(1L, "insert", 1L),
      Row(2L, "delete", 2L))

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
      makeChangeRow(1L, "a", "insert", 1L, 1000000L),
      makeChangeRow(2L, "b", "insert", 1L, 1000000L),
      makeChangeRow(1L, "a2", "insert", 2L, 2000000L)))

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
      makeChangeRow(1L, "a", "insert", 1L, 1000000L),
      makeChangeRow(2L, "b", "insert", 1L, 1000000L),
      makeChangeRow(1L, "a", "delete", 2L, 2000000L)))

    val expected = Seq(
      Row("insert", 2L),
      Row("delete", 1L))

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
      makeChangeRow(1L, "a", "insert", 1L, 1000000L)))

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
      makeChangeRow(1L, "a", "insert", 1L, 1000000L),
      makeChangeRow(2L, "b", "insert", 2L, 2000000L),
      makeChangeRow(3L, "c", "insert", 3L, 3000000L),
      makeChangeRow(4L, "d", "insert", 4L, 4000000L)))

    val expected = Seq(
      Row(2L, "b", "insert", 2L, new Timestamp(2000L)),
      Row(3L, "c", "insert", 3L, new Timestamp(3000L)))

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
      makeChangeRow(1L, "a", "insert", 1L, 1000000L),
      makeChangeRow(2L, "b", "insert", 2L, 2000000L),
      makeChangeRow(3L, "c", "insert", 3L, 3000000L)))

    val expected = Seq(
      Row(1L, "a", "insert", 1L, new Timestamp(1000L)),
      Row(2L, "b", "insert", 2L, new Timestamp(2000L)),
      Row(3L, "c", "insert", 3L, new Timestamp(3000L)))

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
      makeChangeRow(1L, "a", "insert", 1L, 1000000L),
      makeChangeRow(2L, "b", "insert", 2L, 2000000L),
      makeChangeRow(3L, "c", "insert", 3L, 3000000L)))

    // Exclusive start: version 1 excluded, versions 2 and 3 included
    val expected = Seq(
      Row(2L, "b", "insert", 2L, new Timestamp(2000L)),
      Row(3L, "c", "insert", 3L, new Timestamp(3000L)))

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
      makeChangeRow(1L, "a", "insert", 1L, 1000000L),
      makeChangeRow(2L, "b", "insert", 2L, 2000000L),
      makeChangeRow(3L, "c", "insert", 3L, 3000000L)))

    // Exclusive end: versions 1 and 2 included, version 3 excluded
    val expected = Seq(
      Row(1L, "a", "insert", 1L, new Timestamp(1000L)),
      Row(2L, "b", "insert", 2L, new Timestamp(2000L)))

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
      makeChangeRow(1L, "a", "insert", 1L, 1000000L),
      makeChangeRow(2L, "b", "insert", 2L, 2000000L),
      makeChangeRow(3L, "c", "insert", 3L, 3000000L)))

    // Both exclusive: only version 2 included
    val expected = Seq(
      Row(2L, "b", "insert", 2L, new Timestamp(2000L)))

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
      makeChangeRow(1L, "a", "insert", 1L, 1000000L)))

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
      makeChangeRow(1L, "a", "insert", 1L, 1000000L)))

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

  test("changes() passes deduplicationMode and computeUpdates to catalog") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", "insert", 1L, 1000000L)))

    // DataFrame API
    spark.read
      .option("startingVersion", "1")
      .option("deduplicationMode", "netChanges")
      .option("computeUpdates", "true")
      .changes(fullTableName)
      .collect()
    val info1 = catalog.lastChangelogInfo.get
    assert(info1.deduplicationMode() === ChangelogInfo.DeduplicationMode.NET_CHANGES)
    assert(info1.computeUpdates() === true)

    // SQL
    sql(s"SELECT * FROM $fullTableName CHANGES FROM VERSION 1 " +
      "WITH (deduplicationMode = 'netChanges', computeUpdates = 'true')").collect()
    val info2 = catalog.lastChangelogInfo.get
    assert(info2.deduplicationMode() === ChangelogInfo.DeduplicationMode.NET_CHANGES)
    assert(info2.computeUpdates() === true)
  }

  // ---------- Batch: timestamp range ----------

  test("changes() with timestamp range") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", "insert", 1L, 1000000L)))

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
      makeChangeRow(1L, "a", "insert", 1L, 1000000L),
      makeChangeRow(2L, "b", "insert", 1L, 1000000L),
      makeChangeRow(1L, "a", "delete", 2L, 2000000L)))

    val expected = Seq(
      Row(1L, "a", "insert", 1L, new Timestamp(1000L)),
      Row(2L, "b", "insert", 1L, new Timestamp(1000L)),
      Row(1L, "a", "delete", 2L, new Timestamp(2000L)))

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
      makeChangeRow(1L, "a", "insert", 1L, 1000000L),
      makeChangeRow(2L, "b", "insert", 1L, 1000000L),
      makeChangeRow(1L, "a", "delete", 2L, 2000000L)))

    val expected = Seq(
      Row(1L, "a", "delete", 2L, new Timestamp(2000L)))

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
      makeChangeRow(1L, "a", "insert", 1L, 1000000L),
      makeChangeRow(2L, "b", "insert", 1L, 1000000L),
      makeChangeRow(3L, "c", "insert", 2L, 2000000L)))

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

  test("streaming changes() passes deduplicationMode and computeUpdates to catalog") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", "insert", 1L, 1000000L)))

    // DataFrame API
    val dfApiStream = spark.readStream
      .option("startingVersion", "1")
      .option("deduplicationMode", "netChanges")
      .option("computeUpdates", "true")
      .changes(fullTableName)
    val q1 = dfApiStream.writeStream
      .format("memory").queryName("cdc_stream_opts_df").start()
    try {
      q1.processAllAvailable()
      val info1 = catalog.lastChangelogInfo.get
      assert(info1.deduplicationMode() === ChangelogInfo.DeduplicationMode.NET_CHANGES)
      assert(info1.computeUpdates() === true)
    } finally {
      q1.stop()
    }

    // SQL
    val sqlStream = sql(
      s"SELECT * FROM STREAM $fullTableName CHANGES FROM VERSION 1 " +
        "WITH (deduplicationMode = 'netChanges', computeUpdates = 'true')")
    val q2 = sqlStream.writeStream
      .format("memory").queryName("cdc_stream_opts_sql").start()
    try {
      q2.processAllAvailable()
      val info2 = catalog.lastChangelogInfo.get
      assert(info2.deduplicationMode() === ChangelogInfo.DeduplicationMode.NET_CHANGES)
      assert(info2.computeUpdates() === true)
    } finally {
      q2.stop()
    }
  }

  // ---------- Streaming: .name() API ----------

  test("streaming changes() supports .name() API with source evolution enabled") {
    catalog.addChangeRows(ident, Seq(
      makeChangeRow(1L, "a", "insert", 1L, 1000000L)))

    val expected = Seq(
      Row(1L, "a", "insert", 1L, new Timestamp(1000L)))

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
}
