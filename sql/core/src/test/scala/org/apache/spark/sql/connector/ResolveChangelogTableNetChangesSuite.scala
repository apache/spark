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

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{
  ChangelogProperties, Column, Identifier, InMemoryChangelogCatalog}
import org.apache.spark.sql.connector.catalog.Changelog.{
  CHANGE_TYPE_DELETE, CHANGE_TYPE_INSERT, CHANGE_TYPE_UPDATE_POSTIMAGE,
  CHANGE_TYPE_UPDATE_PREIMAGE}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Tests for the `netChanges` deduplication mode handled by
 * [[org.apache.spark.sql.catalyst.analysis.ResolveChangelogTable]].
 *
 * Setup convention: every test runs against an in-memory connector configured with
 * `containsIntermediateChanges = true` and `representsUpdateAsDeleteAndInsert = false`,
 * which means
 *   - netChanges is enabled (the only post-processing pass under test);
 *   - update detection is disabled (so the test directly controls the change_type
 *     labels reaching netChanges).
 *
 * Coverage:
 *   - Single-event tests: a lone `insert` or `delete` survives netChanges unchanged
 *     across a few version-range shapes (covers the `cnt=1` partitions of the
 *     `(insert, insert)` and `(delete, delete)` matrix cells).
 *   - Full matrix: all 9 `(first_change_type, last_change_type)` cells from the
 *     design plan, parameterised over `computeUpdates in {true, false}`. Some cells
 *     need 3 or 4 events to construct a partition with that first/last shape.
 *
 * The `(yes, yes)` cells follow the SPIP B.8 / SQL Ref Spec footnote: under
 * `cu=true` the two emitted rows are labelled `update_preimage + update_postimage`,
 * under `cu=false` they are labelled `delete + insert` -- regardless of which input
 * labels the partition started with.
 */
class ResolveChangelogTableNetChangesSuite
    extends QueryTest
    with SharedSparkSession
    with BeforeAndAfterEach {

  private val catalogName = "cdc_netchanges_catalog"
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
    if (cat.tableExists(ident)) cat.dropTable(ident)
    cat.clearChangeRows(ident)
    cat.createTable(
      ident,
      Array(
        Column.create("id", LongType),
        Column.create("name", StringType),
        Column.create("row_commit_version", LongType, false)),
      Array.empty[Transform],
      Collections.emptyMap[String, String]())
    cat.setChangelogProperties(ident, ChangelogProperties(
      containsIntermediateChanges = true,
      containsCarryoverRows = false,
      representsUpdateAsDeleteAndInsert = false,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))
  }

  private def catalog: InMemoryChangelogCatalog =
    spark.sessionState.catalogManager
      .catalog(catalogName)
      .asInstanceOf[InMemoryChangelogCatalog]

  private def ident = Identifier.of(Array.empty, testTableName)

  // ---------------------------------------------------------------------------
  // Input helpers
  // ---------------------------------------------------------------------------

  /**
   * Builds a single change row matching the table schema
   * `(id, name, row_commit_version, _change_type, _commit_version, _commit_timestamp)`.
   * `row_commit_version` is set to `commitVersion`; these tests do not exercise
   * carry-over removal, so the rcv value does not matter for assertions.
   */
  private def event(
      id: Long, name: String, changeType: String, commitVersion: Long): InternalRow = {
    InternalRow(
      id,
      UTF8String.fromString(name),
      commitVersion,
      UTF8String.fromString(changeType),
      commitVersion,
      0L)
  }

  private def addInsert(commitVersion: Long, id: Long, name: String): Unit =
    catalog.addChangeRows(ident, Seq(event(id, name, CHANGE_TYPE_INSERT, commitVersion)))

  private def addDelete(commitVersion: Long, id: Long, name: String): Unit =
    catalog.addChangeRows(ident, Seq(event(id, name, CHANGE_TYPE_DELETE, commitVersion)))

  private def addUpdatePre(commitVersion: Long, id: Long, name: String): Unit =
    catalog.addChangeRows(
      ident, Seq(event(id, name, CHANGE_TYPE_UPDATE_PREIMAGE, commitVersion)))

  private def addUpdatePost(commitVersion: Long, id: Long, name: String): Unit =
    catalog.addChangeRows(
      ident, Seq(event(id, name, CHANGE_TYPE_UPDATE_POSTIMAGE, commitVersion)))

  // ---------------------------------------------------------------------------
  // Expected output helpers
  // ---------------------------------------------------------------------------

  private def expectInsert(version: Long, id: Long, name: String): Row =
    Row(id, name, CHANGE_TYPE_INSERT, version)

  private def expectDelete(version: Long, id: Long, name: String): Row =
    Row(id, name, CHANGE_TYPE_DELETE, version)

  private def expectUpdatePre(version: Long, id: Long, name: String): Row =
    Row(id, name, CHANGE_TYPE_UPDATE_PREIMAGE, version)

  private def expectUpdatePost(version: Long, id: Long, name: String): Row =
    Row(id, name, CHANGE_TYPE_UPDATE_POSTIMAGE, version)

  /**
   * Expected two-row output for an `UPDATE(s)` collapse: the partition's first row
   * (representing the row's state when it entered the range) plus the last row
   * (state when it exits). Output labels follow the SQL Ref Spec footnote:
   * `update_preimage + update_postimage` under `cu=true`, `delete + insert` under
   * `cu=false`.
   */
  private def outputForUpdate(
      cu: Boolean,
      id: Long,
      preV: Long, oldName: String,
      postV: Long, newName: String): Seq[Row] = {
    if (cu) {
      Seq(expectUpdatePre(preV, id, oldName), expectUpdatePost(postV, id, newName))
    } else {
      Seq(expectDelete(preV, id, oldName), expectInsert(postV, id, newName))
    }
  }

  // ---------------------------------------------------------------------------
  // Query helper
  // ---------------------------------------------------------------------------

  private def runNetChanges(
      fromV: Long, toV: Long, computeUpdates: Boolean = false): DataFrame =
    sql(
      s"SELECT id, name, _change_type, _commit_version " +
      s"FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION $fromV TO VERSION $toV " +
      s"WITH (deduplicationMode = 'netChanges', computeUpdates = '$computeUpdates')")

  // ===========================================================================
  // Single-event: a lone insert or delete passes through netChanges
  // ===========================================================================
  //
  // Parameterised over `change_type` in {insert, delete} and three range shapes:
  // wide range, single-version range, and `cu=true` to confirm the lone event
  // is mode-independent.

  private val singleEventCases: Seq[(String, (Long => Unit), (Long => Row))] = Seq(
    ("insert",
      (v: Long) => addInsert(v, 1L, "Alice"),
      (v: Long) => expectInsert(v, 1L, "Alice")),
    ("delete",
      (v: Long) => addDelete(v, 1L, "Alice"),
      (v: Long) => expectDelete(v, 1L, "Alice")))

  singleEventCases.foreach { case (label, addFn, expectFn) =>
    test(s"single $label survives netChanges (wide range FROM 1 TO 10)") {
      addFn(3L)
      checkAnswer(runNetChanges(fromV = 1, toV = 10), Seq(expectFn(3L)))
    }

    test(s"single $label survives netChanges (single-version range FROM 3 TO 3)") {
      addFn(3L)
      checkAnswer(runNetChanges(fromV = 3, toV = 3), Seq(expectFn(3L)))
    }

    test(s"single $label survives netChanges (cu=true wide range)") {
      addFn(3L)
      checkAnswer(
        runNetChanges(fromV = 1, toV = 10, computeUpdates = true),
        Seq(expectFn(3L)))
    }
  }

  // ===========================================================================
  // Full matrix: all 9 (first_change_type, last_change_type) cells
  // ===========================================================================
  //
  // For each cell, `setup` adds the events that produce a partition with the
  // declared first/last change_type pair, and `expected(cu)` returns the rows
  // the netChanges output should contain. Some cells need more than two events
  // to construct (e.g. `(insert, update_postimage)` requires an insert plus an
  // update pre/post pair, i.e. 3 events).

  private case class MatrixCase(
      label: String,
      setup: () => Unit,
      expected: Boolean => Seq[Row])

  private val matrixCases: Seq[MatrixCase] = Seq(

    // (insert, delete): row inserted and deleted in the range. Cancels.
    MatrixCase(
      label = "(insert, delete) cancels out",
      setup = () => {
        addInsert(commitVersion = 2, id = 1L, name = "Alice")
        addDelete(commitVersion = 5, id = 1L, name = "Alice")
      },
      expected = (_: Boolean) => Seq.empty[Row]),

    // (insert, insert) with cnt > 1: insert + delete + re-insert; net is the
    // re-insert with the latest values. Mode-independent (label always insert).
    MatrixCase(
      label = "(insert, insert) emits the last insert",
      setup = () => {
        addInsert(commitVersion = 2, id = 1L, name = "Alice")
        addDelete(commitVersion = 3, id = 1L, name = "Alice")
        addInsert(commitVersion = 5, id = 1L, name = "Alice_v2")
      },
      expected = (_: Boolean) => Seq(expectInsert(5L, 1L, "Alice_v2"))),

    // (insert, update_postimage): inserted, then updated. Last event is the
    // update post; output is one row labelled `insert` with the post values.
    // Mode-independent (label always insert).
    MatrixCase(
      label = "(insert, update_post) emits last as insert",
      setup = () => {
        addInsert(commitVersion = 2, id = 1L, name = "Alice")
        addUpdatePre(commitVersion = 5, id = 1L, name = "Alice")
        addUpdatePost(commitVersion = 5, id = 1L, name = "Alice_v2")
      },
      expected = (_: Boolean) => Seq(expectInsert(5L, 1L, "Alice_v2"))),

    // (update_pre, update_post): pure UPDATE(s); 2 rows mode-dependent.
    MatrixCase(
      label = "(update_pre, update_post) emits PRE + POST",
      setup = () => {
        addUpdatePre(commitVersion = 3, id = 1L, name = "Alice")
        addUpdatePost(commitVersion = 3, id = 1L, name = "Alice_v2")
      },
      expected = (cu: Boolean) =>
        outputForUpdate(cu, 1L, preV = 3, "Alice", postV = 3, "Alice_v2")),

    // (update_pre, insert): pre-existed (via update), exists at end (via re-insert
    // after delete). 2 rows mode-dependent. Lifecycle:
    //   update_pre(2), update_post(2), delete(3), insert(5).
    MatrixCase(
      label = "(update_pre, insert) emits PRE + POST",
      setup = () => {
        addUpdatePre(commitVersion = 2, id = 1L, name = "Alice")
        addUpdatePost(commitVersion = 2, id = 1L, name = "Alice_v2")
        addDelete(commitVersion = 3, id = 1L, name = "Alice_v2")
        addInsert(commitVersion = 5, id = 1L, name = "Alice_resurrected")
      },
      expected = (cu: Boolean) =>
        outputForUpdate(cu, 1L, preV = 2, "Alice", postV = 5, "Alice_resurrected")),

    // (delete, update_post): pre-existed (via delete), exists at end (via update
    // post after re-insert). 2 rows mode-dependent. Lifecycle:
    //   delete(2), insert(3), update_pre(5), update_post(5).
    MatrixCase(
      label = "(delete, update_post) emits PRE + POST",
      setup = () => {
        addDelete(commitVersion = 2, id = 1L, name = "Alice")
        addInsert(commitVersion = 3, id = 1L, name = "Alice_v2")
        addUpdatePre(commitVersion = 5, id = 1L, name = "Alice_v2")
        addUpdatePost(commitVersion = 5, id = 1L, name = "Alice_v3")
      },
      expected = (cu: Boolean) =>
        outputForUpdate(cu, 1L, preV = 2, "Alice", postV = 5, "Alice_v3")),

    // (delete, insert): pre-existed and exists at end via raw delete + insert
    // (no native update labels). 2 rows mode-dependent.
    MatrixCase(
      label = "(delete, insert) emits PRE + POST",
      setup = () => {
        addDelete(commitVersion = 2, id = 1L, name = "Alice")
        addInsert(commitVersion = 5, id = 1L, name = "Alice_resurrected")
      },
      expected = (cu: Boolean) =>
        outputForUpdate(cu, 1L, preV = 2, "Alice", postV = 5, "Alice_resurrected")),

    // (update_pre, delete): pre-existed (via update), gone at end. 1 row labelled
    // `delete` with the values from the FIRST update_pre. Mode-independent (delete).
    MatrixCase(
      label = "(update_pre, delete) emits first as delete",
      setup = () => {
        addUpdatePre(commitVersion = 3, id = 1L, name = "Alice")
        addUpdatePost(commitVersion = 3, id = 1L, name = "Alice_v2")
        addDelete(commitVersion = 5, id = 1L, name = "Alice_v2")
      },
      expected = (_: Boolean) => Seq(expectDelete(3L, 1L, "Alice"))),

    // (delete, delete) with cnt > 1: delete + re-insert + delete; net is the
    // first delete (the row's state at range entry). Mode-independent (delete).
    MatrixCase(
      label = "(delete, delete) emits the first delete",
      setup = () => {
        addDelete(commitVersion = 2, id = 1L, name = "Alice")
        addInsert(commitVersion = 3, id = 1L, name = "Alice_v2")
        addDelete(commitVersion = 5, id = 1L, name = "Alice_v2")
      },
      expected = (_: Boolean) => Seq(expectDelete(2L, 1L, "Alice"))))

  matrixCases.foreach { mc =>
    Seq(true, false).foreach { cu =>
      test(s"matrix: ${mc.label} [cu=$cu]") {
        mc.setup()
        checkAnswer(
          runNetChanges(fromV = 1, toV = 10, computeUpdates = cu),
          mc.expected(cu))
      }
    }
  }

  // ===========================================================================
  // Range-narrowing: events outside the requested range must not show up
  // ===========================================================================
  //
  // Verifies the connector-side range filter and netChanges interact correctly:
  // narrow ranges drop events that would otherwise appear, even if those events
  // are part of the rowId's lifecycle outside the range.

  test("range narrowing: only events inside [from, to] reach netChanges") {
    // Lifecycle: insert(v2) -- update_pre/post(v5) -- delete(v8). The narrow
    // query [v3, v6] should see only the update at v5, which collapses to a
    // single PRE + POST pair.
    addInsert(commitVersion = 2, id = 1L, name = "Alice")
    addUpdatePre(commitVersion = 5, id = 1L, name = "Alice")
    addUpdatePost(commitVersion = 5, id = 1L, name = "Alice_v2")
    addDelete(commitVersion = 8, id = 1L, name = "Alice_v2")

    checkAnswer(
      runNetChanges(fromV = 3, toV = 6, computeUpdates = true),
      outputForUpdate(
        cu = true, id = 1L, preV = 5, oldName = "Alice", postV = 5, newName = "Alice_v2"))
  }
}
