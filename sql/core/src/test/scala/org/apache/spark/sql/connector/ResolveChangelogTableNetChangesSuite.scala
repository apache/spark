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
 * Shared test bodies for the `netChanges` deduplication mode handled by
 * [[org.apache.spark.sql.catalyst.analysis.ResolveChangelogTable]].
 *
 * Concrete subclasses fix the [[computeUpdates]] and
 * [[representsUpdateAsDeleteAndInsert]] flags. Test bodies use
 * [[computedPreUpdateLabel]] / [[computedPostUpdateLabel]] in their expected outputs.
 *
 * Setup convention: every test runs against an in-memory connector configured
 * with `containsIntermediateChanges = true` and
 * `containsCarryoverRows = false`, which means
 *   - netChanges is enabled;
 *   - carry-over removal is disabled (so the test directly controls events).
 *
 * When `representsUpdateAsDeleteAndInsert = true` AND `computeUpdates = true`, update
 * detection runs upstream of netChanges, exercising the chained pipeline. The
 * `addUpdatePre` / `addUpdatePost` helpers then emit raw `delete` / `insert` events
 * (decomposed updates) which update detection relabels back to update pre/post before
 * netChanges sees them. Output assertions stay identical because both paths produce
 * the same `_change_type` labels at the netChanges input.
 */
trait ResolveChangelogTableNetChangesTestsBase
    extends QueryTest
    with SharedSparkSession
    with BeforeAndAfterEach {

  /**
   * Value of the user-facing CDC option `computeUpdates` that this test run
   * exercises. Concrete subclasses pin this to `true` or `false`.
   */
  protected def computeUpdates: Boolean

  /**
   * Value of the connector capability `representsUpdateAsDeleteAndInsert`. When `true`
   * (combined with `computeUpdates = true`), update detection runs upstream of netChanges
   * and the test helpers `addUpdatePre` / `addUpdatePost` emit decomposed `delete` /
   * `insert` events instead of native pre/post events.
   */
  protected def representsUpdateAsDeleteAndInsert: Boolean = false

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
      representsUpdateAsDeleteAndInsert = representsUpdateAsDeleteAndInsert,
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
  private def cdcEntry(
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
    catalog.addChangeRows(ident, Seq(cdcEntry(id, name, CHANGE_TYPE_INSERT, commitVersion)))

  private def addDelete(commitVersion: Long, id: Long, name: String): Unit =
    catalog.addChangeRows(ident, Seq(cdcEntry(id, name, CHANGE_TYPE_DELETE, commitVersion)))

  private def addUpdatePre(commitVersion: Long, id: Long, name: String): Unit = {
    val changeType =
      if (representsUpdateAsDeleteAndInsert) CHANGE_TYPE_DELETE
      else CHANGE_TYPE_UPDATE_PREIMAGE
    catalog.addChangeRows(ident, Seq(cdcEntry(id, name, changeType, commitVersion)))
  }

  private def addUpdatePost(commitVersion: Long, id: Long, name: String): Unit = {
    val changeType =
      if (representsUpdateAsDeleteAndInsert) CHANGE_TYPE_INSERT
      else CHANGE_TYPE_UPDATE_POSTIMAGE
    catalog.addChangeRows(ident, Seq(cdcEntry(id, name, changeType, commitVersion)))
  }

  // ---------------------------------------------------------------------------
  // Expected output helpers
  // ---------------------------------------------------------------------------

  private def expectInsert(version: Long, id: Long, name: String): Row =
    Row(id, name, CHANGE_TYPE_INSERT, version)

  private def expectDelete(version: Long, id: Long, name: String): Row =
    Row(id, name, CHANGE_TYPE_DELETE, version)

  /**
   * Mode-dependent target label for the FIRST emitted row of a partition where
   * `existedBefore=true, existsAfter=true`. Mirrors the production rule's
   * `computedPreUpdateLabel` selection: `update_preimage` under
   * `computeUpdates = true`, `delete` under `computeUpdates = false`.
   */
  private def computedPreUpdateLabel: String =
    if (computeUpdates) CHANGE_TYPE_UPDATE_PREIMAGE else CHANGE_TYPE_DELETE

  /**
   * Mode-dependent target label for the LAST emitted row of a partition where
   * `existedBefore=true, existsAfter=true`. Mirrors the production rule's
   * `computedPostUpdateLabel` selection: `update_postimage` under
   * `computeUpdates = true`, `insert` under `computeUpdates = false`.
   */
  private def computedPostUpdateLabel: String =
    if (computeUpdates) CHANGE_TYPE_UPDATE_POSTIMAGE else CHANGE_TYPE_INSERT

  // ---------------------------------------------------------------------------
  // Query helper
  // ---------------------------------------------------------------------------

  private def runNetChanges(fromV: Long, toV: Long): DataFrame =
    sql(
      s"SELECT id, name, _change_type, _commit_version " +
      s"FROM $catalogName.$testTableName " +
      s"CHANGES FROM VERSION $fromV TO VERSION $toV " +
      s"WITH (deduplicationMode = 'netChanges', computeUpdates = '$computeUpdates')")

  // ===========================================================================
  // Single-event: a lone insert or delete passes through netChanges
  // ===========================================================================

  test("single insert survives netChanges (wide range FROM 1 TO 10)") {
    addInsert(commitVersion = 3L, id = 1L, name = "Alice")
    checkAnswer(
      runNetChanges(fromV = 1, toV = 10),
      Seq(expectInsert(3L, 1L, "Alice")))
  }

  test("single insert survives netChanges (single-version range FROM 3 TO 3)") {
    addInsert(commitVersion = 3L, id = 1L, name = "Alice")
    checkAnswer(
      runNetChanges(fromV = 3, toV = 3),
      Seq(expectInsert(3L, 1L, "Alice")))
  }

  test("single delete survives netChanges (wide range FROM 1 TO 10)") {
    addDelete(commitVersion = 3L, id = 1L, name = "Alice")
    checkAnswer(
      runNetChanges(fromV = 1, toV = 10),
      Seq(expectDelete(3L, 1L, "Alice")))
  }

  test("single delete survives netChanges (single-version range FROM 3 TO 3)") {
    addDelete(commitVersion = 3L, id = 1L, name = "Alice")
    checkAnswer(
      runNetChanges(fromV = 3, toV = 3),
      Seq(expectDelete(3L, 1L, "Alice")))
  }

  // ===========================================================================
  // Full matrix: all 9 (first_change_type, last_change_type) cells
  // ===========================================================================

  test("matrix: (insert, delete) cancels out") {
    addInsert(commitVersion = 2, id = 1L, name = "Alice")
    addDelete(commitVersion = 5, id = 1L, name = "Alice")
    checkAnswer(
      runNetChanges(fromV = 1, toV = 10),
      Seq.empty[Row])
  }

  test("matrix: (insert, insert) emits the last insert") {
    // Lifecycle: insert(2), delete(3), re-insert(5).
    addInsert(commitVersion = 2, id = 1L, name = "Alice")
    addDelete(commitVersion = 3, id = 1L, name = "Alice")
    addInsert(commitVersion = 5, id = 1L, name = "Alice_v2")
    checkAnswer(
      runNetChanges(fromV = 1, toV = 10),
      Seq(expectInsert(5L, 1L, "Alice_v2")))
  }

  test("matrix: (insert, update_post) emits last as insert") {
    // Lifecycle: insert(2), update_pre/post(5).
    addInsert(commitVersion = 2, id = 1L, name = "Alice")
    addUpdatePre(commitVersion = 5, id = 1L, name = "Alice")
    addUpdatePost(commitVersion = 5, id = 1L, name = "Alice_v2")
    checkAnswer(
      runNetChanges(fromV = 1, toV = 10),
      Seq(expectInsert(5L, 1L, "Alice_v2")))
  }

  test("matrix: (update_pre, update_post) emits PRE + POST") {
    // Lifecycle: update_pre/post(3) -- pure UPDATE(s) case.
    addUpdatePre(commitVersion = 3, id = 1L, name = "Alice")
    addUpdatePost(commitVersion = 3, id = 1L, name = "Alice_v2")
    checkAnswer(
      runNetChanges(fromV = 1, toV = 10),
      Seq(
        Row(1L, "Alice", computedPreUpdateLabel, 3L),
        Row(1L, "Alice_v2", computedPostUpdateLabel, 3L)))
  }

  test("matrix: (update_pre, insert) emits PRE + POST") {
    // Lifecycle: update_pre/post(2), delete(3), re-insert(5).
    addUpdatePre(commitVersion = 2, id = 1L, name = "Alice")
    addUpdatePost(commitVersion = 2, id = 1L, name = "Alice_v2")
    addDelete(commitVersion = 3, id = 1L, name = "Alice_v2")
    addInsert(commitVersion = 5, id = 1L, name = "Alice_resurrected")
    checkAnswer(
      runNetChanges(fromV = 1, toV = 10),
      Seq(
        Row(1L, "Alice", computedPreUpdateLabel, 2L),
        Row(1L, "Alice_resurrected", computedPostUpdateLabel, 5L)))
  }

  test("matrix: (delete, update_post) emits PRE + POST") {
    // Lifecycle: delete(2), insert(3), update_pre/post(5).
    addDelete(commitVersion = 2, id = 1L, name = "Alice")
    addInsert(commitVersion = 3, id = 1L, name = "Alice_v2")
    addUpdatePre(commitVersion = 5, id = 1L, name = "Alice_v2")
    addUpdatePost(commitVersion = 5, id = 1L, name = "Alice_v3")
    checkAnswer(
      runNetChanges(fromV = 1, toV = 10),
      Seq(
        Row(1L, "Alice", computedPreUpdateLabel, 2L),
        Row(1L, "Alice_v3", computedPostUpdateLabel, 5L)))
  }

  test("matrix: (delete, insert) emits PRE + POST") {
    // Lifecycle: delete(2), re-insert(5) -- raw delete + insert across versions.
    addDelete(commitVersion = 2, id = 1L, name = "Alice")
    addInsert(commitVersion = 5, id = 1L, name = "Alice_resurrected")
    checkAnswer(
      runNetChanges(fromV = 1, toV = 10),
      Seq(
        Row(1L, "Alice", computedPreUpdateLabel, 2L),
        Row(1L, "Alice_resurrected", computedPostUpdateLabel, 5L)))
  }

  test("matrix: (update_pre, delete) emits first as delete") {
    // Lifecycle: update_pre/post(3), delete(5).
    addUpdatePre(commitVersion = 3, id = 1L, name = "Alice")
    addUpdatePost(commitVersion = 3, id = 1L, name = "Alice_v2")
    addDelete(commitVersion = 5, id = 1L, name = "Alice_v2")
    checkAnswer(
      runNetChanges(fromV = 1, toV = 10),
      Seq(expectDelete(3L, 1L, "Alice")))
  }

  test("matrix: (delete, delete) emits the first delete") {
    // Lifecycle: delete(2), insert(3), delete(5).
    addDelete(commitVersion = 2, id = 1L, name = "Alice")
    addInsert(commitVersion = 3, id = 1L, name = "Alice_v2")
    addDelete(commitVersion = 5, id = 1L, name = "Alice_v2")
    checkAnswer(
      runNetChanges(fromV = 1, toV = 10),
      Seq(expectDelete(2L, 1L, "Alice")))
  }

  // ===========================================================================
  // Range-narrowing: events outside the requested range must not show up
  // ===========================================================================

  test("range narrowing: only events inside [from, to] reach netChanges") {
    // Lifecycle: insert(v2) -- update_pre/post(v5) -- delete(v8). The narrow
    // query [v3, v6] should see only the update at v5, which collapses to a
    // single PRE + POST pair.
    addInsert(commitVersion = 2, id = 1L, name = "Alice")
    addUpdatePre(commitVersion = 5, id = 1L, name = "Alice")
    addUpdatePost(commitVersion = 5, id = 1L, name = "Alice_v2")
    addDelete(commitVersion = 8, id = 1L, name = "Alice_v2")

    checkAnswer(
      runNetChanges(fromV = 3, toV = 6),
      Seq(
        Row(1L, "Alice", computedPreUpdateLabel, 5L),
        Row(1L, "Alice_v2", computedPostUpdateLabel, 5L)))
  }

  // ===========================================================================
  // Multi-rowId: each rowId's lifecycle collapses independently
  // ===========================================================================

  test("multi-rowId table lifecycle: each rowId collapses independently") {
    // v1: 4 inserts.
    addInsert(commitVersion = 1, id = 1L, name = "Alice")
    addInsert(commitVersion = 1, id = 2L, name = "Bob")
    addInsert(commitVersion = 1, id = 3L, name = "Carol")
    addInsert(commitVersion = 1, id = 4L, name = "Dave")

    // v2: update id=3 (emitted as native pre/post pair by the test connector).
    addUpdatePre(commitVersion = 2, id = 3L, name = "Carol")
    addUpdatePost(commitVersion = 2, id = 3L, name = "Carol_v2")

    // v3: 2 inserts.
    addInsert(commitVersion = 3, id = 5L, name = "Eve")
    addInsert(commitVersion = 3, id = 6L, name = "Frank")

    // v4: 2 deletes.
    addDelete(commitVersion = 4, id = 1L, name = "Alice")
    addDelete(commitVersion = 4, id = 2L, name = "Bob")

    checkAnswer(
      runNetChanges(fromV = 1, toV = 4),
      Seq(
        expectInsert(2L, 3L, "Carol_v2"),  // id=3: insert + update -> last as insert
        expectInsert(1L, 4L, "Dave"),      // id=4: lone insert
        expectInsert(3L, 5L, "Eve"),       // id=5: lone insert
        expectInsert(3L, 6L, "Frank")))    // id=6: lone insert
  }

  test("multi-rowId hitting different mode-dependent cells in one query") {
    addDelete(commitVersion = 2, id = 1L, name = "Alice")
    addInsert(commitVersion = 5, id = 1L, name = "Alice_resurrected")

    addUpdatePre(commitVersion = 3, id = 2L, name = "Bob")
    addUpdatePost(commitVersion = 3, id = 2L, name = "Bob_v2")

    addInsert(commitVersion = 4, id = 3L, name = "Carol")
    addDelete(commitVersion = 6, id = 3L, name = "Carol")

    checkAnswer(
      runNetChanges(fromV = 1, toV = 10),
      Seq(
        // id=1: (delete, insert) -- first + last with mode-dependent labels.
        Row(1L, "Alice", computedPreUpdateLabel, 2L),
        Row(1L, "Alice_resurrected", computedPostUpdateLabel, 5L),
        // id=2: (update_pre, update_post) -- PRE + POST with mode-dependent labels.
        Row(2L, "Bob", computedPreUpdateLabel, 3L),
        Row(2L, "Bob_v2", computedPostUpdateLabel, 3L)
        // id=3: (insert, delete) -- cancel, no rows.
      ))
  }
}

/**
 * Runs the netChanges test bodies with `computeUpdates = true`:
 * `existedBefore=true, existsAfter=true` partitions emit `update_preimage` +
 * `update_postimage`.
 */
class ResolveChangelogTableNetChangesWithComputeUpdatesSuite
    extends ResolveChangelogTableNetChangesTestsBase {
  override protected def computeUpdates: Boolean = true
}

/**
 * Runs the netChanges test bodies with `computeUpdates = false`:
 * `existedBefore=true, existsAfter=true` partitions emit `delete` + `insert`
 * (per SQL Ref Spec footnote).
 */
class ResolveChangelogTableNetChangesWithoutComputeUpdatesSuite
    extends ResolveChangelogTableNetChangesTestsBase {
  override protected def computeUpdates: Boolean = false
}

/**
 * Runs the netChanges test bodies against a connector with
 * `representsUpdateAsDeleteAndInsert = true` and `computeUpdates = true`. Update
 * detection runs upstream of netChanges, exercising the chained post-processing
 * pipeline end-to-end. The `addUpdatePre` / `addUpdatePost` helpers emit decomposed
 * `delete` / `insert` events that update detection relabels back to update pre/post
 * before netChanges sees them, so the same expected outputs hold.
 */
class ResolveChangelogTableNetChangesWithUpdateDetectionSuite
    extends ResolveChangelogTableNetChangesTestsBase {
  override protected def computeUpdates: Boolean = true
  override protected def representsUpdateAsDeleteAndInsert: Boolean = true
}
