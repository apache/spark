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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.connector.catalog.{
  ChangelogProperties, Column, Identifier, InMemoryChangelogCatalog}
import org.apache.spark.sql.connector.catalog.Changelog.{
  CHANGE_TYPE_DELETE, CHANGE_TYPE_INSERT, CHANGE_TYPE_UPDATE_POSTIMAGE,
  CHANGE_TYPE_UPDATE_PREIMAGE}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.v2.ChangelogTable
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{
  BinaryType, BooleanType, DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Tests for [[org.apache.spark.sql.catalyst.analysis.ResolveChangelogTable]] using the
 * in-memory changelog catalog. These tests don't depend on Delta or any specific connector;
 * they directly control what the connector "returns" by populating the in-memory changelog
 * with hand-crafted change rows.
 *
 * Each test sets up [[ChangelogProperties]] on the catalog to enable specific post-processing
 * paths (carry-over removal, update detection) and then verifies that Spark's analyzer rule
 * correctly transforms the plan and produces the expected output.
 */
class ResolveChangelogTablePostProcessingSuite
    extends QueryTest
    with SharedSparkSession
    with BeforeAndAfterEach {

  private val catalogName = "cdc_test_catalog"
  private val testTableName = "events"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(
      s"spark.sql.catalog.$catalogName",
      classOf[InMemoryChangelogCatalog].getName)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    val cat = catalog
    val ident = Identifier.of(Array.empty, testTableName)
    if (cat.tableExists(ident)) cat.dropTable(ident)
    cat.clearChangeRows(ident)
    cat.setChangelogProperties(ident, ChangelogProperties())
    cat.createTable(
      ident,
      Array(
        Column.create("id", LongType),
        Column.create("name", StringType),
        Column.create("row_commit_version", LongType, false)),
      Array.empty[Transform],
      Collections.emptyMap[String, String]())
  }

  private def catalog: InMemoryChangelogCatalog = {
    spark.sessionState.catalogManager
      .catalog(catalogName)
      .asInstanceOf[InMemoryChangelogCatalog]
  }

  private def ident = Identifier.of(Array.empty, testTableName)

  /**
   * Helper to create a change row matching schema
   * (id, name, row_commit_version, _change_type, _commit_version, _commit_timestamp).
   *
   * `rowCommitVersion` follows Delta row-tracking semantics: carry-over pairs (CoW-rewritten
   * unchanged rows) share the same value on both sides; real updates carry the OLD value on
   * the delete side and the NEW value on the insert side. Defaults to `commitVersion` for
   * tests that don't exercise carry-over removal.
   */
  private def changeRow(
      id: Long,
      name: String,
      changeType: String,
      commitVersion: Long,
      rowCommitVersion: Long = -1L,
      commitTimestamp: Long = 0L): InternalRow = {
    val rcv = if (rowCommitVersion == -1L) commitVersion else rowCommitVersion
    InternalRow(
      id,
      UTF8String.fromString(name),
      rcv,
      UTF8String.fromString(changeType),
      commitVersion,
      commitTimestamp)
  }

  // ===========================================================================
  // Carry-Over Removal
  // ===========================================================================

  test("carry-over removal drops identical delete+insert pairs") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    // v1: insert Alice and Bob (rcv=1 each)
    // v2: real delete Alice (preimage carries old rcv=1);
    //     carry-over for Bob (CoW, rcv unchanged on both sides)
    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 1L, rowCommitVersion = 1L),
      changeRow(2L, "Bob", CHANGE_TYPE_INSERT, 1L, rowCommitVersion = 1L),
      changeRow(1L, "Alice", CHANGE_TYPE_DELETE, 2L, rowCommitVersion = 1L),
      changeRow(2L, "Bob", CHANGE_TYPE_DELETE, 2L, rowCommitVersion = 1L),  // carry-over
      changeRow(2L, "Bob", CHANGE_TYPE_INSERT, 2L, rowCommitVersion = 1L))) // carry-over (same rcv)

    checkAnswer(
      sql(
        s"SELECT id, name, _change_type, _commit_version " +
        s"FROM $catalogName.$testTableName CHANGES FROM VERSION 1 TO VERSION 2"),
      Seq(
        Row(1L, "Alice", CHANGE_TYPE_INSERT, 1L),
        Row(2L, "Bob", CHANGE_TYPE_INSERT, 1L),
        Row(1L, "Alice", CHANGE_TYPE_DELETE, 2L)))
  }

  test("deduplicationMode=none keeps all carry-over rows") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 1L, rowCommitVersion = 1L),
      changeRow(2L, "Bob", CHANGE_TYPE_DELETE, 2L, rowCommitVersion = 1L),
      changeRow(2L, "Bob", CHANGE_TYPE_INSERT, 2L, rowCommitVersion = 1L)))

    checkAnswer(
      sql(
        s"SELECT id FROM $catalogName.$testTableName " +
        s"CHANGES FROM VERSION 1 TO VERSION 2 WITH (deduplicationMode = 'none')"),
      Seq(Row(1L), Row(2L), Row(2L)))
  }

  test("NULL rowVersion on one side is NOT silently dropped as carry-over") {
    // Regression for a NULL-safety hole: min/max skip NULLs, so _min_rv = _max_rv alone
    // would match a pair with one NULL and one non-null rowVersion. The _rv_cnt = 2
    // clause in the carry-over filter prevents that.
    //
    // The fixture table here declares `row_commit_version` as nullable so the optimizer
    // is not allowed to fold IsNull(non-nullable-col) to false; the NULL is a legitimate
    // value the guard must defend against.
    val nullableRcvTable = "events_nullable_rcv"
    val nullableIdent = Identifier.of(Array.empty, nullableRcvTable)
    val cat = catalog
    if (cat.tableExists(nullableIdent)) cat.dropTable(nullableIdent)
    cat.clearChangeRows(nullableIdent)
    cat.createTable(
      nullableIdent,
      Array(
        Column.create("id", LongType),
        Column.create("name", StringType),
        Column.create("row_commit_version", LongType, true)),
      Array.empty[Transform],
      Collections.emptyMap[String, String]())
    cat.setChangelogProperties(nullableIdent, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    cat.addChangeRows(nullableIdent, Seq(
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 1L, rowCommitVersion = 1L),
      // v2: one side has NULL rowVersion (buggy connector), the other has a real value.
      InternalRow(1L, UTF8String.fromString("Alice"), null,
        UTF8String.fromString(CHANGE_TYPE_DELETE), 2L, 0L),
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 2L, rowCommitVersion = 5L)))

    checkAnswer(
      sql(s"SELECT id, name, _change_type, _commit_version " +
          s"FROM $catalogName.$nullableRcvTable CHANGES FROM VERSION 1 TO VERSION 2"),
      Seq(
        Row(1L, "Alice", CHANGE_TYPE_INSERT, 1L),
        Row(1L, "Alice", CHANGE_TYPE_DELETE, 2L),
        Row(1L, "Alice", CHANGE_TYPE_INSERT, 2L)))
  }

  // ===========================================================================
  // Update Detection
  // ===========================================================================

  test("update detection relabels delete+insert with different data as update") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = false,  // no carry-overs in this test
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 1L),
      // v2: Alice -> Robert (delete old, insert new)
      changeRow(1L, "Alice", CHANGE_TYPE_DELETE, 2L),
      changeRow(1L, "Robert", CHANGE_TYPE_INSERT, 2L)))

    val rows = sql(
      s"SELECT id, name, _change_type, _commit_version " +
      s"FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 TO VERSION 2 WITH (computeUpdates = 'true')")
      .orderBy("_commit_version", "_change_type")
      .collect()

    val descs = rows.map(r =>
      s"${r.getLong(0)}:${r.getString(1)}:${r.getString(2)}")

    assert(descs.contains("1:Alice:insert"), s"v1 insert. Got: ${descs.mkString(",")}")
    assert(descs.contains("1:Alice:update_preimage"))
    assert(descs.contains("1:Robert:update_postimage"))
    // No raw delete/insert at v2
    assert(!descs.contains("1:Alice:delete"))
    assert(!descs.contains("1:Robert:insert"))
  }

  test("delete and insert in different versions are NOT labeled as update") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 1L),
      changeRow(1L, "Alice", CHANGE_TYPE_DELETE, 2L),
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 3L)))

    val rows = sql(
      s"SELECT _change_type, _commit_version FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 TO VERSION 3 " +
      s"WITH (computeUpdates = 'true', deduplicationMode = 'none')")
      .collect()

    assert(!rows.exists(_.getString(0).contains("update_")),
      "Delete and insert in different versions should not be labeled as update")
  }

  // ===========================================================================
  // Composite rowId: partitioning uses every rowId column
  // ===========================================================================
  //
  // With a composite rowId such as Seq("id", "name"), the (rowId, _commit_version)
  // window partition must include BOTH columns. A regression that drops one of the
  // rowId columns would either falsely merge two different row identities into one
  // partition (silently mislabeling unrelated delete/insert pairs as updates) or
  // trip the UNEXPECTED_MULTIPLE_CHANGES_PER_ROW_VERSION runtime guard.

  test("update detection with composite rowId keeps different (id, name) tuples raw") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id", "name"),
      rowVersionName = Some("row_commit_version")))

    // delete (1, Alice) and insert (1, Bob) at v2. These are DIFFERENT composite
    // rowIds; they must NOT be relabeled as update.
    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", CHANGE_TYPE_DELETE, 2L),
      changeRow(1L, "Bob", CHANGE_TYPE_INSERT, 2L)))

    val rows = sql(
      s"SELECT id, name, _change_type FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 2 TO VERSION 2 WITH (computeUpdates = 'true')")
      .collect()

    val descs = rows.map(r =>
      s"${r.getLong(0)}:${r.getString(1)}:${r.getString(2)}").toSet

    assert(descs == Set("1:Alice:delete", "1:Bob:insert"),
      s"Composite rowId must keep different (id, name) tuples raw. Got: $descs")
  }

  test("carry-over removal with composite rowId removes pairs per (id, name) tuple") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id", "name"),
      rowVersionName = Some("row_commit_version")))

    // Two independent carry-over pairs at v2, both with id=1 but different names.
    // With correct composite-rowId partitioning, each pair lives in its own
    // (id, name, _commit_version) partition, has _del_cnt=1 / _ins_cnt=1 and equal
    // _min_rv / _max_rv, and gets dropped. With broken (id-only) partitioning, the
    // four rows would collapse into one partition with _del_cnt=2 / _ins_cnt=2 and
    // the carry-over filter (which requires =1) would keep them all.
    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 1L, rowCommitVersion = 1L),
      changeRow(1L, "Bob", CHANGE_TYPE_INSERT, 1L, rowCommitVersion = 1L),
      changeRow(1L, "Alice", CHANGE_TYPE_DELETE, 2L, rowCommitVersion = 1L),
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 2L, rowCommitVersion = 1L),
      changeRow(1L, "Bob", CHANGE_TYPE_DELETE, 2L, rowCommitVersion = 1L),
      changeRow(1L, "Bob", CHANGE_TYPE_INSERT, 2L, rowCommitVersion = 1L)))

    val rows = sql(
      s"SELECT id, name, _change_type, _commit_version " +
      s"FROM $catalogName.$testTableName CHANGES FROM VERSION 2 TO VERSION 2")
      .collect()

    val descs = rows.map(r =>
      s"${r.getLong(0)}:${r.getString(1)}:${r.getString(2)}")
    assert(rows.isEmpty,
      s"Both Alice and Bob carry-over pairs at v2 should be removed. Got: ${descs.mkString(",")}")
  }

  // ===========================================================================
  // No row identity: post-processing skipped
  // ===========================================================================

  test("no capability flags -> post-processing not injected in plan") {
    // Default ChangelogProperties has no capability flags set; the rule sees nothing to do.
    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 1L),
      changeRow(2L, "Bob", CHANGE_TYPE_DELETE, 2L),
      changeRow(2L, "Bob", CHANGE_TYPE_INSERT, 2L)))

    val df = sql(
      s"SELECT * FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 TO VERSION 2 WITH (computeUpdates = 'true')")

    val plan = df.queryExecution.analyzed.treeString
    assert(!plan.contains("__spark_cdc_del_cnt"),
      s"Plan must not contain post-processing window helpers. Plan:\n$plan")
    assert(!plan.contains("__spark_cdc_ins_cnt"),
      s"Plan must not contain post-processing window helpers. Plan:\n$plan")
  }

  test("streaming without post-processing options passes through") {
    // Streaming reads with no capability flags on the connector and no
    // post-processing options must resolve without the rule throwing.
    val df = spark.readStream
      .option("startingVersion", "1")
      .changes(s"$catalogName.$testTableName")
    val analyzed = df.queryExecution.analyzed
    val plan = analyzed.treeString
    assert(!plan.contains("__spark_cdc_del_cnt"),
      s"Streaming plan must not contain post-processing helpers. Plan:\n$plan")

    // Positive assertion: the rule actually fired on the streaming relation. Without this,
    // a regression that deletes the streaming arm of `ResolveChangelogTable.apply` would
    // also pass the absence-of-helpers check above.
    val tableResolved = analyzed.collectFirst {
      case rel: StreamingRelationV2 if rel.table.isInstanceOf[ChangelogTable] =>
        rel.table.asInstanceOf[ChangelogTable].resolved
    }
    assert(tableResolved.contains(true),
      s"Expected ChangelogTable to be marked resolved by the rule. Plan:\n$plan")
  }

  // The streaming netChanges path is covered by
  // ResolveChangelogTableStreamingPostProcessingSuite -- not duplicated here, since
  // this suite focuses on the batch path.

  // ===========================================================================
  // Combined
  // ===========================================================================

  test("carry-over removal and update detection combined") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = true,
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    // v1: insert Alice (rcv=1), Bob (rcv=1)
    // v2: Alice carry-over (CoW, rcv unchanged), Bob real update (old rcv=1, new rcv=2)
    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 1L, rowCommitVersion = 1L),
      changeRow(2L, "Bob", CHANGE_TYPE_INSERT, 1L, rowCommitVersion = 1L),
      changeRow(1L, "Alice", CHANGE_TYPE_DELETE, 2L, rowCommitVersion = 1L),   // carry-over
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 2L, rowCommitVersion = 1L),   // carry-over
      changeRow(2L, "Bob", CHANGE_TYPE_DELETE, 2L, rowCommitVersion = 1L), // update preimage
      changeRow(2L, "Robert", CHANGE_TYPE_INSERT, 2L, rowCommitVersion = 2L))) // update postimage

    val rows = sql(
      s"SELECT id, name, _change_type FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 TO VERSION 2 WITH (computeUpdates = 'true')")
      .orderBy("_commit_version", "id", "_change_type")
      .collect()

    val descs = rows.map(r =>
      s"${r.getLong(0)}:${r.getString(1)}:${r.getString(2)}").toSet

    // v1 inserts
    assert(descs.contains("1:Alice:insert"))
    assert(descs.contains("2:Bob:insert"))
    // Alice carry-over dropped
    assert(!descs.contains("1:Alice:delete"))
    // Bob -> Robert as update
    assert(descs.contains("2:Bob:update_preimage"))
    assert(descs.contains("2:Robert:update_postimage"))
    // Should be exactly 4 rows
    assert(rows.length == 4, s"Expected 4 rows, got ${rows.length}: ${descs.mkString(",")}")
  }

  // ===========================================================================
  // computeUpdates default (false) keeps raw delete+insert
  // ===========================================================================

  test("without computeUpdates, delete+insert with different data stays raw") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = true,
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 1L, rowCommitVersion = 1L),
      changeRow(2L, "Bob", CHANGE_TYPE_INSERT, 1L, rowCommitVersion = 1L),
      // Alice: carry-over (CoW, rcv unchanged on both sides)
      changeRow(1L, "Alice", CHANGE_TYPE_DELETE, 2L, rowCommitVersion = 1L),
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 2L, rowCommitVersion = 1L),
      // Bob -> Robert: real change (old rcv on pre, new rcv on post)
      changeRow(2L, "Bob", CHANGE_TYPE_DELETE, 2L, rowCommitVersion = 1L),
      changeRow(2L, "Robert", CHANGE_TYPE_INSERT, 2L, rowCommitVersion = 2L)))

    // Default computeUpdates=false: do NOT relabel, but DO drop carry-overs
    val rows = sql(
      s"SELECT id, name, _change_type FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 TO VERSION 2")
      .orderBy("_commit_version", "id", "_change_type")
      .collect()

    val descs = rows.map(r =>
      s"${r.getLong(0)}:${r.getString(1)}:${r.getString(2)}")

    assert(descs.contains("2:Bob:delete"), s"Bob delete remains raw. Got: ${descs.mkString(",")}")
    assert(descs.contains("2:Robert:insert"), "Robert insert remains raw")
    assert(!descs.exists(_.contains("update_")), "No update_* without computeUpdates")
    assert(!descs.contains("1:Alice:delete"), "Alice carry-over removed")
  }

  test("update detection on pure inserts leaves them as inserts") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 1L),
      changeRow(2L, "Bob", CHANGE_TYPE_INSERT, 2L)))

    val rows = sql(
      s"SELECT id, _change_type FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 TO VERSION 2 WITH (computeUpdates = 'true')")
      .collect()

    assert(rows.length == 2)
    assert(rows.forall(_.getString(1) == CHANGE_TYPE_INSERT),
      s"Pure inserts must stay 'insert'. Got: ${rows.map(_.getString(1)).mkString(",")}")
  }

  // ===========================================================================
  // Keep Carry-over Rows and deduplication flag tests
  // ===========================================================================

  test("computeUpdates with deduplicationMode=none is rejected on COW connector") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = true,
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    checkError(
      intercept[AnalysisException] {
        sql(s"SELECT * FROM $catalogName.$testTableName " +
          s"CHANGES FROM VERSION 1 TO VERSION 2 " +
          s"WITH (computeUpdates = 'true', deduplicationMode = 'none')")
      },
      condition = "INVALID_CDC_OPTION.UPDATE_DETECTION_REQUIRES_CARRY_OVER_REMOVAL",
      parameters = Map("changelogName" -> s"$catalogName.${testTableName}_changelog"))
  }

  test("computeUpdates with deduplicationMode=none is allowed on non-COW connector") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = false,  // MOR-style: no carry-overs possible
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 1L),
      // v2: Alice -> Robert (delete old, insert new)
      changeRow(1L, "Alice", CHANGE_TYPE_DELETE, 2L),
      changeRow(1L, "Robert", CHANGE_TYPE_INSERT, 2L)))

    val rows = sql(
      s"SELECT id, name, _change_type FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 TO VERSION 2 " +
      s"WITH (computeUpdates = 'true', deduplicationMode = 'none')")
      .collect()

    val descs = rows.map(r =>
      s"${r.getLong(0)}:${r.getString(1)}:${r.getString(2)}")
    assert(descs.contains("1:Alice:update_preimage"),
      s"Expected Alice update_preimage. Got: ${descs.mkString(",")}")
    assert(descs.contains("1:Robert:update_postimage"),
      s"Expected Robert update_postimage. Got: ${descs.mkString(",")}")
  }

  // ===========================================================================
  // Contract enforcement: at most one delete + one insert per (rowId, version)
  // ===========================================================================
  //
  // With `representsUpdateAsDeleteAndInsert = true` and `containsIntermediateChanges = false`,
  // the `Changelog` contract guarantees at most one logical change per (rowId, _commit_version)
  // partition. The update-relabel projection enforces this at runtime: if it sees more than one
  // delete or more than one insert in a partition, it raises
  // INVALID_CDC_OPTION.UNEXPECTED_MULTIPLE_CHANGES_PER_ROW_VERSION instead of silently
  // mislabeling extra rows as updates.

  test("update detection raises on multiple inserts for same (rowId, _commit_version)") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    // Contract violation: 2 inserts for id=1 at v2.
    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", CHANGE_TYPE_DELETE, 2L),
      changeRow(1L, "Alice2", CHANGE_TYPE_INSERT, 2L),
      changeRow(1L, "Alice3", CHANGE_TYPE_INSERT, 2L)))

    checkError(
      intercept[SparkRuntimeException] {
        sql(s"SELECT * FROM $catalogName.$testTableName " +
          s"CHANGES FROM VERSION 2 TO VERSION 2 WITH (computeUpdates = 'true')")
          .collect()
      },
      condition = "CHANGELOG_CONTRACT_VIOLATION.UNEXPECTED_MULTIPLE_CHANGES_PER_ROW_VERSION",
      parameters = Map.empty)
  }

  test("update detection raises on multiple deletes for same (rowId, _commit_version)") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    // Contract violation: 2 deletes for id=1 at v2.
    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", CHANGE_TYPE_DELETE, 2L),
      changeRow(1L, "Alice2", CHANGE_TYPE_DELETE, 2L),
      changeRow(1L, "Alice3", CHANGE_TYPE_INSERT, 2L)))

    checkError(
      intercept[SparkRuntimeException] {
        sql(s"SELECT * FROM $catalogName.$testTableName " +
          s"CHANGES FROM VERSION 2 TO VERSION 2 WITH (computeUpdates = 'true')")
          .collect()
      },
      condition = "CHANGELOG_CONTRACT_VIOLATION.UNEXPECTED_MULTIPLE_CHANGES_PER_ROW_VERSION",
      parameters = Map.empty)
  }

  // ===========================================================================
  // Range edge cases
  // ===========================================================================

  test("multiple operations across versions") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = true,
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    catalog.addChangeRows(ident, Seq(
      // v1: insert 3 rows (rcv=1 each)
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 1L, rowCommitVersion = 1L),
      changeRow(2L, "Bob", CHANGE_TYPE_INSERT, 1L, rowCommitVersion = 1L),
      changeRow(3L, "Charlie", CHANGE_TYPE_INSERT, 1L, rowCommitVersion = 1L),
      // v2: delete Alice (preimage carries old rcv=1); CoW carry-overs for Bob/Charlie
      //     keep rcv=1 on both sides (row unchanged).
      changeRow(1L, "Alice", CHANGE_TYPE_DELETE, 2L, rowCommitVersion = 1L),
      changeRow(2L, "Bob", CHANGE_TYPE_DELETE, 2L, rowCommitVersion = 1L),
      changeRow(2L, "Bob", CHANGE_TYPE_INSERT, 2L, rowCommitVersion = 1L),
      changeRow(3L, "Charlie", CHANGE_TYPE_DELETE, 2L, rowCommitVersion = 1L),
      changeRow(3L, "Charlie", CHANGE_TYPE_INSERT, 2L, rowCommitVersion = 1L),
      // v3: update Bob -> Robert (old rcv=1, new rcv=3); CoW carry-over for Charlie (rcv=1)
      changeRow(2L, "Bob", CHANGE_TYPE_DELETE, 3L, rowCommitVersion = 1L),
      changeRow(2L, "Robert", CHANGE_TYPE_INSERT, 3L, rowCommitVersion = 3L),
      changeRow(3L, "Charlie", CHANGE_TYPE_DELETE, 3L, rowCommitVersion = 1L),
      changeRow(3L, "Charlie", CHANGE_TYPE_INSERT, 3L, rowCommitVersion = 1L),
      // v4: insert Diana (rcv=4)
      changeRow(4L, "Diana", CHANGE_TYPE_INSERT, 4L, rowCommitVersion = 4L)))

    val rows = sql(
      s"SELECT id, name, _change_type, _commit_version FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 TO VERSION 4 WITH (computeUpdates = 'true')")
      .orderBy("_commit_version", "id", "_change_type")
      .collect()

    val descs = rows.map(r =>
      s"${r.getLong(0)}:${r.getString(1)}:${r.getString(2)}:v${r.getLong(3)}").toSet

    // v1
    assert(descs.contains("1:Alice:insert:v1"))
    assert(descs.contains("2:Bob:insert:v1"))
    assert(descs.contains("3:Charlie:insert:v1"))
    // v2
    assert(descs.contains("1:Alice:delete:v2"))
    assert(!descs.contains("2:Bob:delete:v2"), "Bob carry-over dropped")
    assert(!descs.contains("3:Charlie:delete:v2"), "Charlie carry-over dropped")
    // v3
    assert(descs.contains("2:Bob:update_preimage:v3"))
    assert(descs.contains("2:Robert:update_postimage:v3"))
    assert(!descs.contains("3:Charlie:delete:v3"), "Charlie carry-over dropped in v3")
    // v4
    assert(descs.contains("4:Diana:insert:v4"))
  }

  test("larger insert batch returns all rows") {
    catalog.addChangeRows(ident, (1 to 5).map(i =>
      changeRow(i.toLong, ('A' + i - 1).toChar.toString, CHANGE_TYPE_INSERT, 1L)))

    val rows = sql(
      s"SELECT id, _change_type FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 TO VERSION 1 WITH (deduplicationMode = 'none')")
      .collect()

    assert(rows.length == 5)
    assert(rows.forall(_.getString(1) == CHANGE_TYPE_INSERT))
  }

  test("DELETE all rows: no carry-over inserts at v2") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    // v1 inserts carry rcv=1; v2 deletes carry the old rcv=1 (rcv tracks last modification)
    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 1L, rowCommitVersion = 1L),
      changeRow(2L, "Bob", CHANGE_TYPE_INSERT, 1L, rowCommitVersion = 1L),
      changeRow(1L, "Alice", CHANGE_TYPE_DELETE, 2L, rowCommitVersion = 1L),
      changeRow(2L, "Bob", CHANGE_TYPE_DELETE, 2L, rowCommitVersion = 1L)))

    val rows = sql(
      s"SELECT id, name, _change_type, _commit_version FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 TO VERSION 2")
      .orderBy("_commit_version", "id")
      .collect()

    val descs = rows.map(r =>
      s"${r.getLong(0)}:${r.getString(1)}:${r.getString(2)}:v${r.getLong(3)}")

    assert(descs.contains("1:Alice:insert:v1"))
    assert(descs.contains("2:Bob:insert:v1"))
    assert(descs.contains("1:Alice:delete:v2"))
    assert(descs.contains("2:Bob:delete:v2"))
    assert(!descs.exists(_.contains("insert:v2")), "No inserts at v2")
  }

  test("UPDATE all rows: every row gets update_pre/postimage, no carry-overs") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = true,
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    // Every v2 row is a real update: delete side carries old rcv=1, insert side new rcv=2.
    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 1L, rowCommitVersion = 1L),
      changeRow(2L, "Bob", CHANGE_TYPE_INSERT, 1L, rowCommitVersion = 1L),
      changeRow(1L, "Alice", CHANGE_TYPE_DELETE, 2L, rowCommitVersion = 1L),
      changeRow(1L, "Alice_updated", CHANGE_TYPE_INSERT, 2L, rowCommitVersion = 2L),
      changeRow(2L, "Bob", CHANGE_TYPE_DELETE, 2L, rowCommitVersion = 1L),
      changeRow(2L, "Bob_updated", CHANGE_TYPE_INSERT, 2L, rowCommitVersion = 2L)))

    val rows = sql(
      s"SELECT id, name, _change_type, _commit_version FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 TO VERSION 2 WITH (computeUpdates = 'true')")
      .orderBy("_commit_version", "id", "_change_type")
      .collect()

    val descs = rows.map(r =>
      s"${r.getLong(0)}:${r.getString(1)}:${r.getString(2)}:v${r.getLong(3)}").toSet

    assert(descs.contains("1:Alice:update_preimage:v2"))
    assert(descs.contains("1:Alice_updated:update_postimage:v2"))
    assert(descs.contains("2:Bob:update_preimage:v2"))
    assert(descs.contains("2:Bob_updated:update_postimage:v2"))
    assert(rows.length == 6, s"Expected 2 inserts + 2 pre + 2 post. Got ${rows.length}")
  }

  test("append-only workload: all inserts, no carry-over needed") {
    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 1L),
      changeRow(2L, "Bob", CHANGE_TYPE_INSERT, 2L),
      changeRow(3L, "Charlie", CHANGE_TYPE_INSERT, 3L)))

    val rows = sql(
      s"SELECT id, _change_type FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 TO VERSION 3")
      .collect()

    assert(rows.length == 3)
    assert(rows.forall(_.getString(1) == CHANGE_TYPE_INSERT))
  }

  test("carry-over removal with many rows: only real change remains") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    // 10 inserts at v1 (rcv=1 each). At v2: delete row 5; CoW writes 9 carry-over pairs
    // (rcv unchanged since v1, i.e. rcv=1 on both sides) plus 1 real delete (rcv=1, old).
    val v1Inserts = (1 to 10).map(i =>
      changeRow(
        i.toLong, ('A' + i - 1).toChar.toString, CHANGE_TYPE_INSERT, 1L, rowCommitVersion = 1L))
    val v2Carryovers = (1 to 10).filter(_ != 5).flatMap { i =>
      val name = ('A' + i - 1).toChar.toString
      Seq(
        changeRow(i.toLong, name, CHANGE_TYPE_DELETE, 2L, rowCommitVersion = 1L),
        changeRow(i.toLong, name, CHANGE_TYPE_INSERT, 2L, rowCommitVersion = 1L))
    }
    val v2RealDelete = Seq(changeRow(5L, "E", CHANGE_TYPE_DELETE, 2L, rowCommitVersion = 1L))
    catalog.addChangeRows(ident, v1Inserts ++ v2Carryovers ++ v2RealDelete)

    val rows = sql(
      s"SELECT id, name, _change_type FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 2 TO VERSION 2")
      .collect()

    val descs = rows.map(r =>
      s"${r.getLong(0)}:${r.getString(1)}:${r.getString(2)}")
    assert(rows.length == 1,
      s"Only 1 real change should remain (9 carry-overs dropped). Got: ${descs.mkString(",")}")
    assert(descs.contains("5:E:delete"))
  }

  test("carry-over removal with mixed types (DOUBLE, BOOLEAN, BINARY)") {
    val mixedTable = "events_mixed"
    val mixedIdent = Identifier.of(Array.empty, mixedTable)
    val cat = catalog
    if (cat.tableExists(mixedIdent)) cat.dropTable(mixedIdent)
    cat.clearChangeRows(mixedIdent)
    cat.createTable(
      mixedIdent,
      Array(
        Column.create("id", LongType),
        Column.create("name", StringType),
        Column.create("score", DoubleType),
        Column.create("active", BooleanType),
        Column.create("payload", BinaryType),
        Column.create("row_commit_version", LongType, false)),
      Array.empty[Transform],
      Collections.emptyMap[String, String]())
    cat.setChangelogProperties(mixedIdent, ChangelogProperties(
      containsCarryoverRows = true,
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    def mixedRow(
        id: Long, name: String, score: Double, active: Boolean, payload: Array[Byte],
        ct: String, v: Long, rowCommitVersion: Long): InternalRow = {
      InternalRow(
        id, UTF8String.fromString(name), score, active, payload, rowCommitVersion,
        UTF8String.fromString(ct), v, 0L)
    }

    val alicePayload = Array[Byte](1, 2, 3)
    val bobPayload = Array[Byte](4, 5, 6)

    cat.addChangeRows(mixedIdent, Seq(
      mixedRow(
        1L, "Alice", 95.5, true, alicePayload, CHANGE_TYPE_INSERT, 1L, rowCommitVersion = 1L),
      mixedRow(
        2L, "Bob", 87.3, false, bobPayload, CHANGE_TYPE_INSERT, 1L, rowCommitVersion = 1L),
      // v2: update Alice's score (old rcv=1, new rcv=2); Bob is carry-over (rcv unchanged)
      mixedRow(
        1L, "Alice", 95.5, true, alicePayload, CHANGE_TYPE_DELETE, 2L, rowCommitVersion = 1L),
      mixedRow(
        1L, "Alice", 99.0, true, alicePayload, CHANGE_TYPE_INSERT, 2L, rowCommitVersion = 2L),
      mixedRow(
        2L, "Bob", 87.3, false, bobPayload, CHANGE_TYPE_DELETE, 2L, rowCommitVersion = 1L),
      mixedRow(
        2L, "Bob", 87.3, false, bobPayload, CHANGE_TYPE_INSERT, 2L, rowCommitVersion = 1L)))

    val rows = sql(
      s"SELECT id, name, score, active, _change_type FROM $catalogName.$mixedTable " +
      s"CHANGES FROM VERSION 1 TO VERSION 2 WITH (computeUpdates = 'true')")
      .orderBy("_commit_version", "id", "_change_type")
      .collect()

    val descs = rows.map(r => s"${r.getLong(0)}:${r.getString(4)}")
    assert(descs.contains("1:update_preimage"))
    assert(descs.contains("1:update_postimage"))
    assert(!descs.contains("2:delete"),
      s"Bob carry-over must be dropped despite DOUBLE/BOOLEAN/BINARY. Got: " +
        descs.mkString(","))

    val pre = rows.find(r =>
      r.getLong(0) == 1L && r.getString(4) == CHANGE_TYPE_UPDATE_PREIMAGE).get
    val post = rows.find(r =>
      r.getLong(0) == 1L && r.getString(4) == CHANGE_TYPE_UPDATE_POSTIMAGE).get
    assert(pre.getDouble(2) == 95.5)
    assert(post.getDouble(2) == 99.0)
  }

  // ===========================================================================
  // Regression: nested rowId + nested rowVersion end-to-end
  // ===========================================================================

  // End-to-end check that nested rowId paths (e.g. `payload.id`) are resolved on the plan
  // and threaded through carry-over detection. The pair survives the filter because the
  // row_commit_version differs across delete/insert, not because of any sibling-field data.
  test("nested rowId path resolves correctly through carry-over filter") {
    val nestedTable = "events_nested"
    val nestedIdent = Identifier.of(Array.empty, nestedTable)
    val cat = catalog
    if (cat.tableExists(nestedIdent)) cat.dropTable(nestedIdent)
    cat.clearChangeRows(nestedIdent)

    val payloadType = StructType(Seq(
      StructField("id", LongType),
      StructField("value", StringType)))

    cat.createTable(
      nestedIdent,
      Array(
        Column.create("payload", payloadType),
        Column.create("row_commit_version", LongType, false)),
      Array.empty[Transform],
      Collections.emptyMap[String, String]())

    cat.setChangelogProperties(nestedIdent, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdPaths = Seq(Seq("payload", "id")),
      rowVersionName = Some("row_commit_version")))

    def nestedRow(
        id: Long, value: String, ct: String, v: Long, rowCommitVersion: Long): InternalRow = {
      InternalRow(
        InternalRow(id, UTF8String.fromString(value)),
        rowCommitVersion,
        UTF8String.fromString(ct), v, 0L)
    }

    cat.addChangeRows(nestedIdent, Seq(
      nestedRow(1L, "original", CHANGE_TYPE_INSERT, 1L, rowCommitVersion = 1L),
      // v2 update: rowId same, rowVersion differs (old rcv=1 on preimage, new rcv=2 on postimage)
      nestedRow(1L, "original", CHANGE_TYPE_DELETE, 2L, rowCommitVersion = 1L),
      nestedRow(1L, "CHANGED", CHANGE_TYPE_INSERT, 2L, rowCommitVersion = 2L)))

    val rows = sql(
      s"SELECT payload.id AS id, payload.value AS value, _change_type, _commit_version " +
      s"FROM $catalogName.$nestedTable CHANGES FROM VERSION 1 TO VERSION 2")
      .orderBy("_commit_version", "_change_type")
      .collect()

    val descs = rows.map(r =>
      s"${r.getLong(0)}:${r.getString(1)}:${r.getString(2)}:v${r.getLong(3)}")

    assert(descs.contains("1:original:insert:v1"),
      s"v1 insert must survive. Got: ${descs.mkString(",")}")
    assert(descs.contains("1:original:delete:v2"),
      s"v2 delete must survive (payload.value differs from insert). Got: ${descs.mkString(",")}")
    assert(descs.contains("1:CHANGED:insert:v2"),
      s"v2 insert must survive (payload.value differs from delete). Got: ${descs.mkString(",")}")
    assert(rows.length == 3,
      s"Expected 3 rows (v1 insert + v2 delete + v2 insert). Got ${rows.length}: " +
      descs.mkString(","))
  }

  // ===========================================================================
  // No-op UPDATE is correctly preserved as update_preimage/postimage
  // ===========================================================================

  test("no-op UPDATE is labeled as update (row_commit_version differs on pre/post)") {
    // A no-op UPDATE bumps row_commit_version even when data is byte-identical, so the
    // delete side carries the OLD rcv and the insert side the NEW rcv. Window post-processing
    // sees different rowVersions, treats this as a real change, and labels both rows as
    // update_preimage / update_postimage.
    catalog.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = true,
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 1L, rowCommitVersion = 1L),
      // v2 no-op update: identical data, but rcv differs (Delta bumps it on any UPDATE)
      changeRow(1L, "Alice", CHANGE_TYPE_DELETE, 2L, rowCommitVersion = 1L),
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 2L, rowCommitVersion = 2L)))

    val rows = sql(
      s"SELECT id, name, _change_type, _commit_version FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 TO VERSION 2 WITH (computeUpdates = 'true')")
      .orderBy("_commit_version", "_change_type")
      .collect()

    val descs = rows.map(r =>
      s"${r.getLong(0)}:${r.getString(1)}:${r.getString(2)}:v${r.getLong(3)}")

    assert(descs.contains("1:Alice:insert:v1"))
    assert(descs.contains("1:Alice:update_preimage:v2"),
      s"No-op UPDATE preimage must be labeled. Got: ${descs.mkString(",")}")
    assert(descs.contains("1:Alice:update_postimage:v2"),
      s"No-op UPDATE postimage must be labeled. Got: ${descs.mkString(",")}")
    assert(rows.length == 3,
      s"Expected v1 insert + v2 update pre/post = 3 rows. Got ${rows.length}")
  }

  // ===========================================================================
  // Baseline (range syntax / connector range filtering -- rule bypassed via
  // deduplicationMode = 'none'; included as smoke tests for the SQL surface).
  // ===========================================================================

  test("baseline: single-version range FROM VERSION X TO VERSION X") {
    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 1L),
      changeRow(2L, "Bob", CHANGE_TYPE_INSERT, 1L),
      changeRow(3L, "Charlie", CHANGE_TYPE_INSERT, 2L)))

    val rows = sql(
      s"SELECT id, _change_type, _commit_version FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 2 TO VERSION 2 WITH (deduplicationMode = 'none')")
      .collect()

    assert(rows.length == 1, s"Single version: 1 row. Got ${rows.length}")
    assert(rows(0).getLong(0) == 3L)
    assert(rows(0).getString(1) == CHANGE_TYPE_INSERT)
  }

  test("baseline: EXCLUSIVE start bound skips the start version") {
    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 1L),
      changeRow(2L, "Bob", CHANGE_TYPE_INSERT, 2L),
      changeRow(3L, "Charlie", CHANGE_TYPE_INSERT, 3L)))

    val rows = sql(
      s"SELECT id, _commit_version FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 EXCLUSIVE TO VERSION 3 " +
      s"WITH (deduplicationMode = 'none')")
      .orderBy("_commit_version")
      .collect()

    assert(!rows.exists(_.getLong(1) == 1L), "v1 must be excluded")
    assert(rows.exists(_.getLong(0) == 2L), "Bob (v2) included")
    assert(rows.exists(_.getLong(0) == 3L), "Charlie (v3) included")
  }

  test("baseline: open-ended range (no TO clause) reads to latest") {
    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", CHANGE_TYPE_INSERT, 1L),
      changeRow(2L, "Bob", CHANGE_TYPE_INSERT, 2L),
      changeRow(3L, "Charlie", CHANGE_TYPE_INSERT, 3L)))

    val rows = sql(
      s"SELECT id, _commit_version FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 WITH (deduplicationMode = 'none')")
      .orderBy("_commit_version", "id")
      .collect()

    assert(rows.length == 3, s"Open-ended range should see all 3. Got ${rows.length}")
    assert(rows.exists(r => r.getLong(0) == 3L && r.getLong(1) == 3L))
  }
}
