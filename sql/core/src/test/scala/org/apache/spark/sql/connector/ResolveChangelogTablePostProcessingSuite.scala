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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{
  ChangelogProperties, Column, Identifier, InMemoryChangelogCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{
  BinaryType, BooleanType, DoubleType, LongType, StringType}
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
        Column.create("name", StringType)),
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
   * (id, name, _change_type, _commit_version, _commit_timestamp).
   */
  private def changeRow(
      id: Long,
      name: String,
      changeType: String,
      commitVersion: Long,
      commitTimestamp: Long = 0L): InternalRow = {
    InternalRow(
      id,
      UTF8String.fromString(name),
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
      rowVersionName = Some("_commit_version")))

    // v1: insert Alice and Bob
    // v2: real delete Alice, carry-over for Bob (delete + insert with identical data)
    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", "insert", 1L),
      changeRow(2L, "Bob", "insert", 1L),
      changeRow(1L, "Alice", "delete", 2L),
      changeRow(2L, "Bob", "delete", 2L),  // carry-over
      changeRow(2L, "Bob", "insert", 2L))) // carry-over

    val rows = sql(
      s"SELECT id, name, _change_type, _commit_version " +
      s"FROM $catalogName.$testTableName CHANGES FROM VERSION 1 TO VERSION 2")
      .orderBy("_commit_version", "id", "_change_type")
      .collect()

    val descs = rows.map(r =>
      s"${r.getLong(0)}:${r.getString(1)}:${r.getString(2)}:v${r.getLong(3)}")

    // v1 inserts kept
    assert(descs.contains("1:Alice:insert:v1"))
    assert(descs.contains("2:Bob:insert:v1"))
    // Real Alice delete kept
    assert(descs.contains("1:Alice:delete:v2"))
    // Bob carry-over pair removed
    assert(!descs.contains("2:Bob:delete:v2"),
      s"Bob delete should be dropped. Got: ${descs.mkString(",")}")
    assert(!descs.contains("2:Bob:insert:v2"),
      s"Bob insert should be dropped. Got: ${descs.mkString(",")}")
  }

  test("deduplicationMode=none keeps all carry-over rows") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("_commit_version")))

    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", "insert", 1L),
      changeRow(2L, "Bob", "delete", 2L),
      changeRow(2L, "Bob", "insert", 2L)))

    val rows = sql(
      s"SELECT id FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 TO VERSION 2 WITH (deduplicationMode = 'none')")
      .collect()

    assert(rows.length == 3, "Without dedup, all 3 raw rows should be returned")
  }

  // ===========================================================================
  // Update Detection
  // ===========================================================================

  test("update detection relabels delete+insert with different data as update") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = false,  // no carry-overs in this test
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("_commit_version")))

    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", "insert", 1L),
      // v2: Alice -> Robert (delete old, insert new)
      changeRow(1L, "Alice", "delete", 2L),
      changeRow(1L, "Robert", "insert", 2L)))

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
      rowVersionName = Some("_commit_version")))

    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", "insert", 1L),
      changeRow(1L, "Alice", "delete", 2L),
      changeRow(1L, "Alice", "insert", 3L)))

    val rows = sql(
      s"SELECT _change_type, _commit_version FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 TO VERSION 3 " +
      s"WITH (computeUpdates = 'true', deduplicationMode = 'none')")
      .collect()

    assert(!rows.exists(_.getString(0).contains("update_")),
      "Delete and insert in different versions should not be labeled as update")
  }

  // ===========================================================================
  // No row identity: post-processing skipped
  // ===========================================================================

  test("empty rowId skips post-processing in plan") {
    // Default ChangelogProperties has no rowId; post-processing must not be injected
    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", "insert", 1L),
      changeRow(2L, "Bob", "delete", 2L),
      changeRow(2L, "Bob", "insert", 2L)))

    val df = sql(
      s"SELECT * FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 TO VERSION 2 WITH (computeUpdates = 'true')")

    val plan = df.queryExecution.analyzed.treeString
    assert(!plan.contains("CarryOverRemoval"),
      s"Plan should NOT contain CarryOverRemoval without rowId. Plan:\n$plan")
    assert(!plan.contains("_ins_cnt"),
      s"Plan should NOT contain update detection window without rowId. Plan:\n$plan")
  }

  // ===========================================================================
  // Combined
  // ===========================================================================

  test("carry-over removal and update detection combined") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = true,
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("_commit_version")))

    // v1: insert Alice, Bob
    // v2: Alice carry-over (CoW), Bob real update (Bob -> Robert)
    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", "insert", 1L),
      changeRow(2L, "Bob", "insert", 1L),
      changeRow(1L, "Alice", "delete", 2L),  // carry-over
      changeRow(1L, "Alice", "insert", 2L),  // carry-over
      changeRow(2L, "Bob", "delete", 2L),    // update preimage
      changeRow(2L, "Robert", "insert", 2L))) // update postimage

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
      rowVersionName = Some("_commit_version")))

    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", "insert", 1L),
      changeRow(2L, "Bob", "insert", 1L),
      changeRow(1L, "Alice", "delete", 2L),  // carry-over (CoW)
      changeRow(1L, "Alice", "insert", 2L),  // carry-over (CoW)
      changeRow(2L, "Bob", "delete", 2L),    // real change pre
      changeRow(2L, "Robert", "insert", 2L))) // real change post

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
      rowVersionName = Some("_commit_version")))

    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", "insert", 1L),
      changeRow(2L, "Bob", "insert", 2L)))

    val rows = sql(
      s"SELECT id, _change_type FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 TO VERSION 2 WITH (computeUpdates = 'true')")
      .collect()

    assert(rows.length == 2)
    assert(rows.forall(_.getString(1) == "insert"),
      s"Pure inserts must stay 'insert'. Got: ${rows.map(_.getString(1)).mkString(",")}")
  }

  // Pins the CASE-WHEN default branch in injectUpdateDetection: when a (rowId, rowVersion)
  // partition has _del_cnt=1 but _ins_cnt=0, isUpdate is false and _change_type stays 'delete'.
  test("update detection keeps delete as-is when partition has no paired insert") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("_commit_version")))

    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", "insert", 1L),
      changeRow(1L, "Alice", "delete", 2L)))  // unpaired delete at v2

    val rows = sql(
      s"SELECT id, name, _change_type, _commit_version FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 TO VERSION 2 WITH (computeUpdates = 'true')")
      .collect()

    val descs = rows.map(r =>
      s"${r.getLong(0)}:${r.getString(1)}:${r.getString(2)}:v${r.getLong(3)}")

    assert(descs.contains("1:Alice:insert:v1"))
    assert(descs.contains("1:Alice:delete:v2"),
      s"Unpaired delete must stay 'delete', not become update_preimage. " +
        s"Got: ${descs.mkString(",")}")
    assert(!descs.exists(_.contains("update_")),
      "No update_* labels when no insert partner exists in the partition")
  }

  // ===========================================================================
  // Range edge cases
  // ===========================================================================

  test("single-version range FROM VERSION X TO VERSION X") {
    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", "insert", 1L),
      changeRow(2L, "Bob", "insert", 1L),
      changeRow(3L, "Charlie", "insert", 2L)))

    val rows = sql(
      s"SELECT id, _change_type, _commit_version FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 2 TO VERSION 2 WITH (deduplicationMode = 'none')")
      .collect()

    assert(rows.length == 1, s"Single version: 1 row. Got ${rows.length}")
    assert(rows(0).getLong(0) == 3L)
    assert(rows(0).getString(1) == "insert")
  }

  test("multiple operations across versions") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = true,
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("_commit_version")))

    catalog.addChangeRows(ident, Seq(
      // v1: insert 3 rows
      changeRow(1L, "Alice", "insert", 1L),
      changeRow(2L, "Bob", "insert", 1L),
      changeRow(3L, "Charlie", "insert", 1L),
      // v2: delete Alice; CoW carry-overs for Bob/Charlie
      changeRow(1L, "Alice", "delete", 2L),
      changeRow(2L, "Bob", "delete", 2L),
      changeRow(2L, "Bob", "insert", 2L),
      changeRow(3L, "Charlie", "delete", 2L),
      changeRow(3L, "Charlie", "insert", 2L),
      // v3: update Bob -> Robert; CoW carry-over for Charlie
      changeRow(2L, "Bob", "delete", 3L),
      changeRow(2L, "Robert", "insert", 3L),
      changeRow(3L, "Charlie", "delete", 3L),
      changeRow(3L, "Charlie", "insert", 3L),
      // v4: insert Diana
      changeRow(4L, "Diana", "insert", 4L)))

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

  test("EXCLUSIVE start bound skips the start version") {
    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", "insert", 1L),
      changeRow(2L, "Bob", "insert", 2L),
      changeRow(3L, "Charlie", "insert", 3L)))

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

  test("open-ended range (no TO clause) reads to latest") {
    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", "insert", 1L),
      changeRow(2L, "Bob", "insert", 2L),
      changeRow(3L, "Charlie", "insert", 3L)))

    val rows = sql(
      s"SELECT id, _commit_version FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 WITH (deduplicationMode = 'none')")
      .orderBy("_commit_version", "id")
      .collect()

    assert(rows.length == 3, s"Open-ended range should see all 3. Got ${rows.length}")
    assert(rows.exists(r => r.getLong(0) == 3L && r.getLong(1) == 3L))
  }

  test("DELETE all rows: no carry-over inserts at v2") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("_commit_version")))

    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", "insert", 1L),
      changeRow(2L, "Bob", "insert", 1L),
      changeRow(1L, "Alice", "delete", 2L),
      changeRow(2L, "Bob", "delete", 2L)))

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
      rowVersionName = Some("_commit_version")))

    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", "insert", 1L),
      changeRow(2L, "Bob", "insert", 1L),
      changeRow(1L, "Alice", "delete", 2L),
      changeRow(1L, "Alice_updated", "insert", 2L),
      changeRow(2L, "Bob", "delete", 2L),
      changeRow(2L, "Bob_updated", "insert", 2L)))

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

  test("carry-over removal with many rows: only real change remains") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("_commit_version")))

    // 10 inserts at v1, then at v2: delete row 5; CoW writes 9 carry-over pairs + 1 real delete
    val v1Inserts = (1 to 10).map(i =>
      changeRow(i.toLong, ('A' + i - 1).toChar.toString, "insert", 1L))
    val v2Carryovers = (1 to 10).filter(_ != 5).flatMap { i =>
      val name = ('A' + i - 1).toChar.toString
      Seq(
        changeRow(i.toLong, name, "delete", 2L),
        changeRow(i.toLong, name, "insert", 2L))
    }
    val v2RealDelete = Seq(changeRow(5L, "E", "delete", 2L))
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

  test("carry-over removal with mixed types (DOUBLE, BOOLEAN)") {
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
        Column.create("active", BooleanType)),
      Array.empty[Transform],
      Collections.emptyMap[String, String]())
    cat.setChangelogProperties(mixedIdent, ChangelogProperties(
      containsCarryoverRows = true,
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("_commit_version")))

    def mixedRow(
        id: Long, name: String, score: Double, active: Boolean,
        ct: String, v: Long): InternalRow = {
      InternalRow(
        id, UTF8String.fromString(name), score, active,
        UTF8String.fromString(ct), v, 0L)
    }

    cat.addChangeRows(mixedIdent, Seq(
      mixedRow(1L, "Alice", 95.5, true, "insert", 1L),
      mixedRow(2L, "Bob", 87.3, false, "insert", 1L),
      // v2: update Alice's score; Bob is carry-over
      mixedRow(1L, "Alice", 95.5, true, "delete", 2L),
      mixedRow(1L, "Alice", 99.0, true, "insert", 2L),
      mixedRow(2L, "Bob", 87.3, false, "delete", 2L),  // carry-over
      mixedRow(2L, "Bob", 87.3, false, "insert", 2L))) // carry-over

    val rows = sql(
      s"SELECT id, name, score, active, _change_type FROM $catalogName.$mixedTable " +
      s"CHANGES FROM VERSION 1 TO VERSION 2 WITH (computeUpdates = 'true')")
      .orderBy("_commit_version", "id", "_change_type")
      .collect()

    val descs = rows.map(r => s"${r.getLong(0)}:${r.getString(4)}")
    assert(descs.contains("1:update_preimage"))
    assert(descs.contains("1:update_postimage"))
    assert(!descs.contains("2:delete"),
      s"Bob carry-over must be dropped despite DOUBLE/BOOLEAN. Got: ${descs.mkString(",")}")

    // Verify the score values
    val pre = rows.find(r => r.getLong(0) == 1L && r.getString(4) == "update_preimage").get
    val post = rows.find(r => r.getLong(0) == 1L && r.getString(4) == "update_postimage").get
    assert(pre.getDouble(2) == 95.5)
    assert(post.getDouble(2) == 99.0)
  }

  // ===========================================================================
  // Iterator correctness: state-machine paths + null handling in dataColumnsEqual
  // ===========================================================================

  // Pins the "pendingDelete always null, input is all inserts" path of CarryOverIterator.
  // Even with containsCarryoverRows=true, a stream of only inserts must pass through
  // unchanged; the iterator should never enter the delete/insert pairing branch.
  test("carry-over iterator passes append-only stream through unchanged") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("_commit_version")))

    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", "insert", 1L),
      changeRow(2L, "Bob", "insert", 1L),
      changeRow(3L, "Charlie", "insert", 1L)))

    val rows = sql(
      s"SELECT id, name, _change_type FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 TO VERSION 1")
      .orderBy("id")
      .collect()

    assert(rows.length == 3, s"All 3 inserts must survive, got ${rows.length}")
    assert(rows.forall(_.getString(2) == "insert"),
      s"Every row must stay 'insert'. Got: ${rows.map(_.getString(2)).mkString(",")}")
  }

  // Pins null handling in CarryOverIterator.dataColumnsEqual:
  //   - both-null  -> equal -> carry-over dropped
  //   - one-null   -> not equal -> both rows survive
  test("carry-over removal handles null data values correctly") {
    catalog.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("_commit_version")))

    // id=1: data null throughout (both-null carry-over, must be dropped)
    // id=2: real change, "Bob" -> null (one-null, must survive as both rows)
    catalog.addChangeRows(ident, Seq(
      changeRow(1L, null, "insert", 1L),
      changeRow(2L, "Bob", "insert", 1L),
      changeRow(1L, null, "delete", 2L),   // both-null carry-over delete
      changeRow(1L, null, "insert", 2L),   // both-null carry-over insert (drop pair)
      changeRow(2L, "Bob", "delete", 2L),  // one-null vs value: real change pre
      changeRow(2L, null, "insert", 2L)))  // one-null vs value: real change post

    val rows = sql(
      s"SELECT id, name, _change_type, _commit_version FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 TO VERSION 2")
      .orderBy("_commit_version", "id", "_change_type")
      .collect()

    // Use a string that makes null visible in the description
    val descs = rows.map { r =>
      val nameStr = if (r.isNullAt(1)) "<null>" else r.getString(1)
      s"${r.getLong(0)}:$nameStr:${r.getString(2)}:v${r.getLong(3)}"
    }

    // v1 inserts preserved
    assert(descs.contains("1:<null>:insert:v1"), s"Got: ${descs.mkString(",")}")
    assert(descs.contains("2:Bob:insert:v1"))
    // id=1 both-null pair dropped as carry-over
    assert(!descs.contains("1:<null>:delete:v2"),
      s"both-null pair must be dropped as carry-over. Got: ${descs.mkString(",")}")
    assert(!descs.contains("1:<null>:insert:v2"),
      s"both-null pair must be dropped as carry-over. Got: ${descs.mkString(",")}")
    // id=2 value-vs-null real change survives
    assert(descs.contains("2:Bob:delete:v2"),
      s"value-vs-null pair must survive. Got: ${descs.mkString(",")}")
    assert(descs.contains("2:<null>:insert:v2"),
      s"value-vs-null pair must survive. Got: ${descs.mkString(",")}")
    assert(rows.length == 4,
      s"Expected 4 rows (2 v1 inserts + 2 id=2 v2 real change). Got ${rows.length}")
  }

  // ===========================================================================
  // Regression: BinaryType carry-over (reference-equality bug)
  // ===========================================================================

  // Two carry-over rows with equal-by-content but reference-unequal byte arrays.
  // Expectation: Bob's delete+insert pair is identical data and must be dropped.
  // If `dataColumnsEqual` uses `==` (reference equality) on `Array[Byte]`, this test fails
  // because the two payloads are different JVM objects.
  test("carry-over removal with BinaryType column drops content-equal pair") {
    val binTable = "events_binary"
    val binIdent = Identifier.of(Array.empty, binTable)
    val cat = catalog
    if (cat.tableExists(binIdent)) cat.dropTable(binIdent)
    cat.clearChangeRows(binIdent)
    cat.createTable(
      binIdent,
      Array(
        Column.create("id", LongType),
        Column.create("payload", BinaryType)),
      Array.empty[Transform],
      Collections.emptyMap[String, String]())
    cat.setChangelogProperties(binIdent, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("_commit_version")))

    def binRow(id: Long, payload: Array[Byte], ct: String, v: Long): InternalRow = {
      InternalRow(id, payload, UTF8String.fromString(ct), v, 0L)
    }

    // Bob's carry-over: two distinct byte[] instances holding identical bytes.
    val bobPayloadA = Array[Byte](1, 2, 3, 4)
    val bobPayloadB = Array[Byte](1, 2, 3, 4)
    assert(bobPayloadA ne bobPayloadB, "test precondition: distinct array instances")

    cat.addChangeRows(binIdent, Seq(
      binRow(1L, Array[Byte](9, 9), "insert", 1L),
      binRow(2L, bobPayloadA, "insert", 1L),
      // v2: real delete for Alice; Bob carry-over with distinct-but-equal byte arrays
      binRow(1L, Array[Byte](9, 9), "delete", 2L),
      binRow(2L, bobPayloadA, "delete", 2L),   // carry-over (same instance as v1 insert)
      binRow(2L, bobPayloadB, "insert", 2L)))  // carry-over (distinct instance, equal bytes)

    val rows = sql(
      s"SELECT id, _change_type, _commit_version FROM $catalogName.$binTable " +
      s"CHANGES FROM VERSION 1 TO VERSION 2")
      .orderBy("_commit_version", "id", "_change_type")
      .collect()

    val descs = rows.map(r => s"${r.getLong(0)}:${r.getString(1)}:v${r.getLong(2)}")

    // v1 inserts retained
    assert(descs.contains("1:insert:v1"), s"v1 insert for id=1. Got: ${descs.mkString(",")}")
    assert(descs.contains("2:insert:v1"), s"v1 insert for id=2. Got: ${descs.mkString(",")}")
    // v2 real delete retained
    assert(descs.contains("1:delete:v2"), "Real delete for id=1 retained")
    // v2 Bob carry-over must be dropped despite distinct byte[] instances
    assert(!descs.contains("2:delete:v2"),
      s"Bob BinaryType carry-over delete must be dropped. Got: ${descs.mkString(",")}")
    assert(!descs.contains("2:insert:v2"),
      s"Bob BinaryType carry-over insert must be dropped. Got: ${descs.mkString(",")}")
  }

  // ===========================================================================
  // Behavior divergence from getChangeRows: no-op UPDATE
  // ===========================================================================

  test("no-op UPDATE (unchanged data) is dropped as carry-over") {
    // SPIP B.6: connector-agnostic carry-over removal compares data columns. A no-op
    // UPDATE produces byte-identical delete+insert, indistinguishable from a CoW
    // carry-over, and is therefore dropped. Legacy getChangeRows would have emitted
    // update_preimage/update_postimage here.
    catalog.setChangelogProperties(ident, ChangelogProperties(
      containsCarryoverRows = true,
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("_commit_version")))

    catalog.addChangeRows(ident, Seq(
      changeRow(1L, "Alice", "insert", 1L),
      // v2 no-op update: identical delete + insert
      changeRow(1L, "Alice", "delete", 2L),
      changeRow(1L, "Alice", "insert", 2L)))

    val rows = sql(
      s"SELECT id, name, _change_type, _commit_version FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION 1 TO VERSION 2 WITH (computeUpdates = 'true')")
      .orderBy("_commit_version", "_change_type")
      .collect()

    val descs = rows.map(r =>
      s"${r.getLong(0)}:${r.getString(1)}:${r.getString(2)}:v${r.getLong(3)}")

    assert(descs.contains("1:Alice:insert:v1"))
    assert(!descs.exists(_.contains("update_")),
      "No-op UPDATE indistinguishable from carry-over, so it is dropped")
    assert(!descs.contains("1:Alice:delete:v2"))
    assert(!descs.contains("1:Alice:insert:v2"))
    assert(rows.length == 1, "Only v1 insert remains; v2 no-op UPDATE silently dropped")
  }
}
