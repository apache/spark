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

package org.apache.spark.sql.pipelines.autocdc

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{functions => F, AnalysisException, QueryTest, Row}
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * End-to-end unit tests for [[Scd2ForeachBatchHandler]]. Unlike the focused unit suites that
 * exercise individual [[Scd2BatchProcessor]] stages in isolation, these drive the entire
 * microbatch reconciliation pipeline - validation, preprocessing, affected-row pull-in from both
 * side tables, decomposition, start/end reconciliation, aux routing, and the two `MERGE INTO`
 * writes - through the public `execute` entrypoint against an in-memory v2 catalog.
 *
 * They are the first place the cross-microbatch stateful behaviors (out-of-order arrivals, no-op
 * run coalescing across batches, tombstone-driven suppression, and auxiliary-table garbage
 * collection) can be observed against materialized target and auxiliary tables, so the idempotency
 * / GC / cross-batch scenarios are emphasized here.
 *
 * The default flow tracks every persisted user column (`value`) under key `id`, sequences by
 * `seq`, and treats `is_delete = true` rows as deletes.
 */
class Scd2ForeachBatchHandlerSuite
    extends QueryTest
    with SharedSparkSession
    with BeforeAndAfter
    with AutoCdcCatalogExecutionTestBase {

  private val sourceSchema = new StructType()
    .add("id", IntegerType)
    .add("value", StringType)
    .add("seq", LongType)
    .add("is_delete", BooleanType)

  /** The SCD2 cdc-metadata struct carries a single `recordStartAt` field (unlike SCD1's two). */
  private val scd2MetadataSchema: StructType = Scd2BatchProcessor.cdcMetadataColSchema(LongType)

  /** Canonical SCD2 row schema: persisted user columns + framework start/end + cdc metadata. */
  private val canonicalSchema = new StructType()
    .add("id", IntegerType)
    .add("value", StringType)
    .add(Scd2BatchProcessor.startAtColName, LongType, nullable = true)
    .add(Scd2BatchProcessor.endAtColName, LongType, nullable = true)
    .add(AutoCdcReservedNames.cdcMetadataColName, scd2MetadataSchema, nullable = false)

  /** Auxiliary table schema: canonical schema plus the aux-only logical-delete marker column. */
  private val auxSchema = canonicalSchema
    .add(Scd2BatchProcessor.deletedByBatchIdColName, LongType, nullable = true)

  /** Target table schema is exactly the canonical schema. */
  private val targetSchema = canonicalSchema

  private val processor = Scd2BatchProcessor(
    changeArgs = ChangeArgs(
      keys = Seq(UnqualifiedColumnName("id")),
      sequencing = F.col("seq"),
      storedAsScdType = ScdType.Type2,
      deleteCondition = Some(F.col("is_delete")),
      // Persist only id + value; seq / is_delete are control columns and must not be stored.
      columnSelection = Some(
        ColumnSelection.ExcludeColumns(
          Seq(UnqualifiedColumnName("seq"), UnqualifiedColumnName("is_delete"))
        )
      )
    ),
    resolvedSequencingType = LongType
  )

  private def createAuxTable(seedRows: Row*): Unit =
    createTable(defaultAuxIdent, defaultAuxTableIdentifier, auxSchema, seedRows: _*)

  private def createTargetTable(seedRows: Row*): Unit =
    createTable(defaultTargetIdent, defaultTargetTableIdentifier, targetSchema, seedRows: _*)

  private def auxTable: DataFrame = spark.read.table(defaultAuxTableIdentifier.quotedString)

  private def targetTable: DataFrame = spark.read.table(defaultTargetTableIdentifier.quotedString)

  private def execWith(p: Scd2BatchProcessor): Scd2ForeachBatchHandler = Scd2ForeachBatchHandler(
    batchProcessor = p,
    auxiliaryTableIdentifier = defaultAuxTableIdentifier,
    targetTableIdentifier = defaultTargetTableIdentifier
  )

  private def exec: Scd2ForeachBatchHandler = execWith(processor)

  /** A source UPSERT event: `(id, value, seq, is_delete = false)`. */
  private def upsert(id: Int, value: String, seq: Long): Row = Row(id, value, seq, false)

  /** A source DELETE event: `(id, null, seq, is_delete = true)`. */
  private def del(id: Int, seq: Long): Row = Row(id, null, seq, true)

  /** The cdc-metadata struct value for a given `recordStartAt`. */
  private def meta(recordStartAt: Long): Row = Row(recordStartAt)

  /** A canonical target row `(id, value, startAt, endAt, meta(recordStartAt))`. */
  private def targetRow(
      id: Int,
      value: String,
      startAt: java.lang.Long,
      endAt: java.lang.Long,
      recordStartAt: Long): Row =
    Row(id, value, startAt, endAt, meta(recordStartAt))

  /** A canonical aux row `(id, value, startAt, endAt, meta(recordStartAt), deletedByBatchId)`. */
  private def auxRow(
      id: Int,
      value: String,
      startAt: java.lang.Long,
      endAt: java.lang.Long,
      recordStartAt: Long,
      deletedByBatchId: java.lang.Long): Row =
    Row(id, value, startAt, endAt, meta(recordStartAt), deletedByBatchId)

  /** Run a microbatch of source rows through the default handler. */
  private def runBatch(batchId: Long)(rows: Row*): Unit =
    exec.execute(microbatchOf(sourceSchema)(rows: _*), batchId)

  /**
   * Run `rows` as batch `batchId`, capture both tables, then replay the identical batch under the
   * same `batchId` and assert both tables are byte-for-byte unchanged. Models a crash/redelivery
   * where a committed microbatch is reprocessed.
   */
  private def assertReplayStable(batchId: Long)(rows: Row*): Unit = {
    runBatch(batchId)(rows: _*)
    val targetAfterFirst = targetTable.collect().toSeq
    val auxAfterFirst = auxTable.collect().toSeq

    runBatch(batchId)(rows: _*)
    checkAnswer(targetTable, targetAfterFirst)
    checkAnswer(auxTable, auxAfterFirst)
  }

  test("a record with a null sequencing value fails the microbatch without applying any changes") {
    createAuxTable()
    createTargetTable(targetRow(1, "old", 10L, null, 10L))

    val batch = microbatchOf(sourceSchema)(Row(1, "bad", null, false))

    checkError(
      exception = intercept[AnalysisException] {
        exec.execute(batch, batchId = 77L)
      },
      condition = "AUTOCDC_MICROBATCH_VALIDATION.NULL_SEQUENCE",
      sqlState = "22000",
      parameters = Map(
        "tableName" -> defaultTargetTableIdentifier.quotedString,
        "batchId" -> "77",
        "nullCount" -> "1"
      )
    )

    assert(auxTable.collect().isEmpty)
    checkAnswer(targetTable, targetRow(1, "old", 10L, null, 10L))
  }

  test("a record with a null key fails the microbatch without applying any changes") {
    createAuxTable()
    createTargetTable(targetRow(1, "old", 10L, null, 10L))

    val batch = microbatchOf(sourceSchema)(Row(null, "bad", 10L, false))

    checkError(
      exception = intercept[AnalysisException] {
        exec.execute(batch, batchId = 7L)
      },
      condition = "AUTOCDC_MICROBATCH_VALIDATION.NULL_KEY",
      sqlState = "22000",
      parameters = Map(
        "tableName" -> defaultTargetTableIdentifier.quotedString,
        "batchId" -> "7",
        "nullKeyCounts" -> "`id`=1"
      )
    )

    assert(auxTable.collect().isEmpty)
    checkAnswer(targetTable, targetRow(1, "old", 10L, null, 10L))
  }

  test("an empty microbatch leaves both tables unchanged") {
    createAuxTable()
    createTargetTable(targetRow(1, "a", 10L, null, 10L))

    runBatch(2L)() // zero source rows

    checkAnswer(targetTable, targetRow(1, "a", 10L, null, 10L))
    assert(auxTable.collect().isEmpty)
  }

  test("an empty microbatch garbage-collects a stale aux row from a prior batch") {
    // Batch 1: a late upsert logically deletes a tombstone, stamping deletedByBatchId=1.
    createAuxTable()
    createTargetTable()
    runBatch(1L)(del(1, 20L))
    runBatch(2L)(upsert(1, "x", 10L))
    checkAnswer(auxTable, auxRow(1, null, 20L, 20L, 20L, 2L)) // tombstone stamped, not yet GC'd

    // Batch 3: empty microbatch - no new work, but the GC clause still sweeps the aux table.
    // The tombstone (deletedByBatchId=2, not equal to current batchId=3) is physically removed.
    runBatch(3L)()

    assert(auxTable.collect().isEmpty)
    checkAnswer(targetTable, targetRow(1, "x", 10L, 20L, 10L))
  }

  test("inserting a new key creates an open current record") {
    createAuxTable()
    createTargetTable()

    runBatch(1L)(upsert(1, "a", 10L))

    // Open interval [10, null); nothing routed to the aux table.
    checkAnswer(targetTable, targetRow(1, "a", 10L, null, 10L))
    assert(auxTable.collect().isEmpty)
  }

  test("two updates to a key in one batch produce a closed record followed by the open record") {
    createAuxTable()
    createTargetTable()

    runBatch(1L)(upsert(1, "a", 10L), upsert(1, "b", 20L))

    // a closes at b's start; b stays open. No hidden rows (every event changed the value).
    checkAnswer(
      targetTable,
      Seq(
        targetRow(1, "a", 10L, 20L, 10L),
        targetRow(1, "b", 20L, null, 20L)
      )
    )
    assert(auxTable.collect().isEmpty)
  }

  test("an insert and a later delete in the same batch leave a single closed record") {
    createAuxTable()
    createTargetTable()

    runBatch(1L)(upsert(1, "a", 10L), del(1, 20L))

    // The closed interval [10, 20) already encodes the deletion boundary at 20, so the delete's
    // tombstone is redundant and dropped during reconciliation - nothing lands in the aux table.
    checkAnswer(targetTable, targetRow(1, "a", 10L, 20L, 10L))
    assert(auxTable.collect().isEmpty)
  }

  test("an insert, update, delete, and re-insert for one key in a batch build the full history") {
    createAuxTable()
    createTargetTable()

    // Unlike SCD1 - which would collapse these to the single latest state for the key - SCD2 keeps
    // every event: each distinct value gets its own interval, the delete ends the active record,
    // and the re-insert opens a fresh record after the deletion gap.
    runBatch(1L)(
      upsert(1, "a", 10L),
      upsert(1, "b", 20L),
      del(1, 30L),
      upsert(1, "c", 40L)
    )

    // a [10, 20), b [20, 30) (closed by the delete), a deletion gap over [30, 40), then c [40, ..).
    // The delete leaves no tombstone: b's closed interval already carries the boundary at 30.
    checkAnswer(
      targetTable,
      Seq(
        targetRow(1, "a", 10L, 20L, 10L),
        targetRow(1, "b", 20L, 30L, 20L),
        targetRow(1, "c", 40L, null, 40L)
      )
    )
    assert(auxTable.collect().isEmpty)
  }

  test("repeating a key's value keeps one current record effective from its first occurrence") {
    createAuxTable()
    createTargetTable()

    runBatch(1L)(upsert(1, "a", 10L), upsert(1, "a", 20L))

    // The run [10, 20] coalesces: the visible tail carries the run-head START_AT (10) but the
    // tail's own recordStartAt (20). The head becomes a hidden no-op row in the aux table.
    checkAnswer(targetTable, targetRow(1, "a", 10L, null, 20L))
    checkAnswer(auxTable, auxRow(1, "a", 10L, null, 10L, null))
  }

  test("deleting a key that has no current record leaves the dimension table empty") {
    createAuxTable()
    createTargetTable()

    runBatch(1L)(del(1, 5L))

    // No preceding upsert closes on the boundary, so the tombstone survives as aux side state.
    assert(targetTable.collect().isEmpty)
    checkAnswer(auxTable, auxRow(1, null, 5L, 5L, 5L, null))
  }

  test("updating an existing key closes its current record and opens a new one") {
    createAuxTable()
    createTargetTable(targetRow(1, "a", 10L, null, 10L))

    runBatch(2L)(upsert(1, "b", 20L))

    checkAnswer(
      targetTable,
      Seq(
        targetRow(1, "a", 10L, 20L, 10L),
        targetRow(1, "b", 20L, null, 20L)
      )
    )
    assert(auxTable.collect().isEmpty)
  }

  test("deleting an existing key closes its current record with no open record remaining") {
    createAuxTable()
    createTargetTable(targetRow(1, "a", 10L, null, 10L))

    runBatch(2L)(del(1, 20L))

    // The resulting closed interval carries the deletion boundary; no tombstone needed.
    checkAnswer(targetTable, targetRow(1, "a", 10L, 20L, 10L))
    assert(auxTable.collect().isEmpty)
  }

  test("an update preserves already-closed historical records") {
    createAuxTable()
    createTargetTable(
      targetRow(1, "a", 5L, 10L, 5L), // closed and settled well before the incoming event
      targetRow(1, "b", 10L, null, 10L) // currently active
    )

    runBatch(3L)(upsert(1, "c", 20L))

    // Only the active interval is pulled in and closed; the settled [5, 10) row is never touched.
    checkAnswer(
      targetTable,
      Seq(
        targetRow(1, "a", 5L, 10L, 5L),
        targetRow(1, "b", 10L, 20L, 10L),
        targetRow(1, "c", 20L, null, 20L)
      )
    )
    assert(auxTable.collect().isEmpty)
  }

  test("a late event older than all existing history is inserted as the earliest record") {
    createAuxTable()
    createTargetTable(targetRow(1, "a", 10L, null, 10L))

    // b arrives late with seq=5, strictly before the seeded interval's start.
    runBatch(2L)(upsert(1, "b", 5L))

    checkAnswer(
      targetTable,
      Seq(
        targetRow(1, "b", 5L, 10L, 5L),
        targetRow(1, "a", 10L, null, 10L)
      )
    )
    assert(auxTable.collect().isEmpty)
  }

  test("a late update landing inside an existing record splits it around the new value") {
    createAuxTable()
    createTargetTable(
      targetRow(1, "a", 10L, 20L, 10L),
      targetRow(1, "c", 20L, null, 20L)
    )

    // b arrives late at seq=15, inside the closed [10, 20) interval.
    runBatch(3L)(upsert(1, "b", 15L))

    checkAnswer(
      targetTable,
      Seq(
        targetRow(1, "a", 10L, 15L, 10L),
        targetRow(1, "b", 15L, 20L, 15L),
        targetRow(1, "c", 20L, null, 20L)
      )
    )
    assert(auxTable.collect().isEmpty)
  }

  test("a late delete landing inside an existing record shortens it to end at the deletion") {
    createAuxTable()
    createTargetTable(
      targetRow(1, "a", 10L, 30L, 10L),
      targetRow(1, "b", 30L, null, 30L)
    )

    // Delete arrives late at seq=20, inside the closed [10, 30) interval.
    runBatch(4L)(del(1, 20L))

    // a is decomposed and re-closed at the delete boundary (20); b is unaffected. The delete is
    // covered by the new closed interval [10, 20), so it leaves no aux tombstone.
    checkAnswer(
      targetTable,
      Seq(
        targetRow(1, "a", 10L, 20L, 10L),
        targetRow(1, "b", 30L, null, 30L)
      )
    )
    assert(auxTable.collect().isEmpty)
  }

  test("two late events in one batch each bisect a distinct closed target row") {
    createAuxTable()
    createTargetTable(
      targetRow(1, "a", 1L, 5L, 1L),
      targetRow(1, "b", 5L, 10L, 5L),
      targetRow(1, "c", 10L, 20L, 10L),
      targetRow(1, "d", 20L, null, 20L)
    )

    // Late x at seq=7 bisects [5,10); late y at seq=15 bisects [10,20) — both in the same batch.
    runBatch(5L)(upsert(1, "x", 7L), upsert(1, "y", 15L))

    checkAnswer(
      targetTable,
      Seq(
        targetRow(1, "a", 1L, 5L, 1L),
        targetRow(1, "b", 5L, 7L, 5L),
        targetRow(1, "x", 7L, 10L, 7L),
        targetRow(1, "c", 10L, 15L, 10L),
        targetRow(1, "y", 15L, 20L, 15L),
        targetRow(1, "d", 20L, null, 20L)
      )
    )
    assert(auxTable.collect().isEmpty)
  }

  test("re-inserting a key after it was deleted opens a new current record") {
    createAuxTable(auxRow(1, null, 20L, 20L, 20L, null))
    createTargetTable()

    // Revival strictly after the recorded deletion at 20.
    runBatch(5L)(upsert(1, "x", 30L))

    // The revival opens a fresh interval; the deletion boundary at 20 stays in the aux table since
    // no visible interval closes on it (there is a real gap [20, 30) where the key was absent).
    checkAnswer(targetTable, targetRow(1, "x", 30L, null, 30L))
    checkAnswer(auxTable, auxRow(1, null, 20L, 20L, 20L, null))
  }

  test("a value repeated across batches stays one record until a later change closes it") {
    createAuxTable()
    createTargetTable()

    // Batch 1: establish the run head.
    runBatch(1L)(upsert(1, "a", 10L))
    checkAnswer(targetTable, targetRow(1, "a", 10L, null, 10L))
    assert(auxTable.collect().isEmpty)

    // Batch 2: a same-value upsert extends the run. The previously-visible head is demoted to the
    // aux table and the new tail becomes the visible row (START_AT pinned to the run head, 10).
    runBatch(2L)(upsert(1, "a", 20L))
    checkAnswer(targetTable, targetRow(1, "a", 10L, null, 20L))
    checkAnswer(auxTable, auxRow(1, "a", 10L, null, 10L, null))

    // Batch 3: a real value change closes the "a" run and opens "b". The hidden head is retained
    // as aux side state for any future bisecting event.
    runBatch(3L)(upsert(1, "b", 30L))
    checkAnswer(
      targetTable,
      Seq(
        targetRow(1, "a", 10L, 30L, 20L),
        targetRow(1, "b", 30L, null, 30L)
      )
    )
    checkAnswer(auxTable, auxRow(1, "a", 10L, null, 10L, null))
  }

  test("a late event arriving within an unchanged period splits the surrounding history") {
    createAuxTable()
    createTargetTable()

    // Build the Alice run [5, 10, 15] then Charlie at 20.
    runBatch(1L)(upsert(1, "Alice", 5L), upsert(1, "Alice", 10L), upsert(1, "Alice", 15L))
    runBatch(2L)(upsert(1, "Charlie", 20L))

    // Alice's run is [5, 20); the visible tail is the latest Alice event (15) with START_AT=5.
    checkAnswer(
      targetTable,
      Seq(
        targetRow(1, "Alice", 5L, 20L, 15L),
        targetRow(1, "Charlie", 20L, null, 20L)
      )
    )
    checkAnswer(
      auxTable,
      Seq(
        auxRow(1, "Alice", 5L, null, 5L, null),
        auxRow(1, "Alice", 5L, null, 10L, null)
      )
    )

    // Late Bob at 12 splits the Alice run: Alice [5, 12) (tail now the 10 event), Bob [12, 15),
    // Alice [15, 20) (a fresh size-1 run).
    runBatch(3L)(upsert(1, "Bob", 12L))
    checkAnswer(
      targetTable,
      Seq(
        targetRow(1, "Alice", 5L, 12L, 10L),
        targetRow(1, "Bob", 12L, 15L, 12L),
        targetRow(1, "Alice", 15L, 20L, 15L),
        targetRow(1, "Charlie", 20L, null, 20L)
      )
    )
    // The hidden run head (recordStartAt=5) survives as side state. The other previously-hidden
    // no-op (recordStartAt=10) is promoted to the visible tail of [5, 12); it leaves the aux table
    // logically (stamped with this batch's id), to be physically garbage-collected by a later
    // unrelated batch.
    checkAnswer(
      auxTable,
      Seq(
        auxRow(1, "Alice", 5L, null, 5L, null),
        auxRow(1, "Alice", 5L, null, 10L, 3L)
      )
    )
  }

  test("reprocessing an update microbatch leaves both tables unchanged") {
    createAuxTable()
    createTargetTable(targetRow(1, "a", 10L, null, 10L))

    assertReplayStable(2L)(upsert(1, "b", 20L))

    checkAnswer(
      targetTable,
      Seq(
        targetRow(1, "a", 10L, 20L, 10L),
        targetRow(1, "b", 20L, null, 20L)
      )
    )
    assert(auxTable.collect().isEmpty)
  }

  test("reprocessing a delete microbatch leaves both tables unchanged") {
    createAuxTable()
    createTargetTable(targetRow(1, "a", 10L, null, 10L))

    assertReplayStable(2L)(del(1, 20L))

    checkAnswer(targetTable, targetRow(1, "a", 10L, 20L, 10L))
    assert(auxTable.collect().isEmpty)
  }

  test("reprocessing a microbatch of repeated values leaves both tables unchanged") {
    createAuxTable()
    createTargetTable()

    assertReplayStable(1L)(upsert(1, "a", 10L), upsert(1, "a", 20L), upsert(1, "a", 30L))

    // A single run of same-value events at sequences 10, 20, 30; the latest (30) is the visible
    // tail (open from startAt 10), the earlier two are hidden.
    checkAnswer(targetTable, targetRow(1, "a", 10L, null, 30L))
    checkAnswer(
      auxTable,
      Seq(
        auxRow(1, "a", 10L, null, 10L, null),
        auxRow(1, "a", 10L, null, 20L, null)
      )
    )
  }

  test("reprocessing a delete of an unknown key leaves both tables unchanged") {
    createAuxTable()
    createTargetTable()

    assertReplayStable(7L)(del(1, 5L))

    assert(targetTable.collect().isEmpty)
    checkAnswer(auxTable, auxRow(1, null, 5L, 5L, 5L, null))
  }

  test("byte-identical duplicate events in one microbatch collapse to a single record") {
    createAuxTable()
    createTargetTable()

    // Two fully identical events (same key, value, and sequence). Preprocessing keeps both 1:1;
    // because they share a recordStartAt, reconciliation collapses them to one. The result is a
    // single open record - and notably no hidden aux row, unlike a run of same-value events at
    // *distinct* sequences (where the non-tail members are retained as side state).
    runBatch(1L)(upsert(1, "a", 10L), upsert(1, "a", 10L))

    checkAnswer(targetTable, targetRow(1, "a", 10L, null, 10L))
    assert(auxTable.collect().isEmpty)
  }

  test("redelivering the same event in a later microbatch leaves both tables unchanged") {
    createAuxTable()
    createTargetTable()

    runBatch(1L)(upsert(1, "a", 10L))
    val targetAfterFirst = targetTable.collect().toSeq
    val auxAfterFirst = auxTable.collect().toSeq

    // A genuinely new microbatch (different batch id) carries a duplicate of an already-processed
    // event - same key and sequence. It collides with the persisted record at recordStartAt=10 and
    // is absorbed, so the dimension is identical to having seen the event exactly once.
    runBatch(2L)(upsert(1, "a", 10L))

    checkAnswer(targetTable, targetAfterFirst)
    checkAnswer(auxTable, auxAfterFirst)
    checkAnswer(targetTable, targetRow(1, "a", 10L, null, 10L))
    assert(auxTable.collect().isEmpty)
  }

  test("a late event predating a recorded deletion becomes a record ending at the deletion") {
    // Batch 1 records a standalone tombstone for a never-seen key.
    createAuxTable()
    createTargetTable()
    runBatch(1L)(del(1, 20L))
    checkAnswer(auxTable, auxRow(1, null, 20L, 20L, 20L, null))
    assert(targetTable.collect().isEmpty)

    // Batch 2: an upsert strictly before the delete. It materializes as the closed interval
    // [10, 20) in the target, which now carries the deletion boundary, so the tombstone is no
    // longer needed and is logically deleted (stamped with batch id 2).
    runBatch(2L)(upsert(1, "x", 10L))
    checkAnswer(targetTable, targetRow(1, "x", 10L, 20L, 10L))
    checkAnswer(auxTable, auxRow(1, null, 20L, 20L, 20L, 2L))
  }

  test("a logically-deleted tombstone is physically garbage-collected by a later unrelated batch") {
    createAuxTable()
    createTargetTable()
    runBatch(1L)(del(1, 20L))

    // Batch 2's late upsert reconciles to the closed interval [10, 20), which itself ends exactly
    // on the deletion boundary at 20. That closed upsert now encodes the deletion, making the
    // standalone tombstone redundant - so the tombstone is logically deleted (stamped deletedBy=2).
    // It is not physically removed yet: the marker must survive so a replay of batch 2 reproduces
    // the same state, and reconciliation keeps excluding it. Physical removal is deferred.
    runBatch(2L)(upsert(1, "x", 10L))
    checkAnswer(auxTable, auxRow(1, null, 20L, 20L, 20L, 2L))

    // Batch 3 touches a different key. The tombstone is now matched by neither the source nor the
    // current batch id (deletedBy=2 != 3), so the aux GC sweep finally hard-deletes it.
    runBatch(3L)(upsert(2, "y", 30L))

    assert(auxTable.collect().isEmpty)
    checkAnswer(
      targetTable,
      Seq(
        targetRow(1, "x", 10L, 20L, 10L),
        targetRow(2, "y", 30L, null, 30L)
      )
    )
  }

  test("updates, deletes, and inserts for different keys in one batch reconcile independently") {
    createAuxTable()
    createTargetTable(
      targetRow(1, "a", 10L, null, 10L),
      targetRow(2, "p", 10L, null, 10L)
    )

    // key 1: value change; key 2: delete; key 3: brand new insert - all in one microbatch.
    runBatch(4L)(upsert(1, "b", 20L), del(2, 20L), upsert(3, "z", 20L))

    checkAnswer(
      targetTable,
      Seq(
        targetRow(1, "a", 10L, 20L, 10L),
        targetRow(1, "b", 20L, null, 20L),
        targetRow(2, "p", 10L, 20L, 10L),
        targetRow(3, "z", 20L, null, 20L)
      )
    )
    assert(auxTable.collect().isEmpty)
  }

  // Source/target carry id + name + score, but only `name` is tracked: a change in the untracked
  // `score` alone is a no-op run continuation, while a change in `name` opens a new interval.
  private val trackedSourceSchema = new StructType()
    .add("id", IntegerType)
    .add("name", StringType)
    .add("score", IntegerType)
    .add("seq", LongType)
    .add("is_delete", BooleanType)

  private val trackedCanonicalSchema = new StructType()
    .add("id", IntegerType)
    .add("name", StringType)
    .add("score", IntegerType)
    .add(Scd2BatchProcessor.startAtColName, LongType, nullable = true)
    .add(Scd2BatchProcessor.endAtColName, LongType, nullable = true)
    .add(AutoCdcReservedNames.cdcMetadataColName, scd2MetadataSchema, nullable = false)

  private val trackedAuxSchema = trackedCanonicalSchema
    .add(Scd2BatchProcessor.deletedByBatchIdColName, LongType, nullable = true)

  private val trackedProcessor = Scd2BatchProcessor(
    changeArgs = ChangeArgs(
      keys = Seq(UnqualifiedColumnName("id")),
      sequencing = F.col("seq"),
      storedAsScdType = ScdType.Type2,
      deleteCondition = Some(F.col("is_delete")),
      columnSelection = Some(
        ColumnSelection.ExcludeColumns(
          Seq(UnqualifiedColumnName("seq"), UnqualifiedColumnName("is_delete"))
        )
      ),
      trackHistorySelection = Some(
        ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("name")))
      )
    ),
    resolvedSequencingType = LongType
  )

  test("changing only an untracked column updates the current record without adding history") {
    createTable(defaultAuxIdent, defaultAuxTableIdentifier, trackedAuxSchema)
    createTable(defaultTargetIdent, defaultTargetTableIdentifier, trackedCanonicalSchema)

    execWith(trackedProcessor).execute(
      microbatchOf(trackedSourceSchema)(
        Row(1, "alice", 100, 10L, false),
        Row(1, "alice", 200, 20L, false) // only score changed -> no-op run continuation
      ),
      batchId = 1L
    )

    // Visible tail reflects the latest values (score=200) with the run-head START_AT (10).
    checkAnswer(targetTable, Row(1, "alice", 200, 10L, null, meta(20L)))
    checkAnswer(auxTable, Row(1, "alice", 100, 10L, null, meta(10L), null))

    // A second untracked-only change in a separate microbatch keeps extending the same
    // record: it still starts at 10 and now reflects score=300, with no new history opened.
    // The previous tail (score=200) joins the original head as hidden side state.
    execWith(trackedProcessor).execute(
      microbatchOf(trackedSourceSchema)(
        Row(1, "alice", 300, 30L, false) // still name=alice, only score changed
      ),
      batchId = 2L
    )

    checkAnswer(targetTable, Row(1, "alice", 300, 10L, null, meta(30L)))
    checkAnswer(
      auxTable,
      Seq(
        Row(1, "alice", 100, 10L, null, meta(10L), null),
        Row(1, "alice", 200, 10L, null, meta(20L), null)
      )
    )
  }

  test("changing a tracked column opens a new historical record") {
    createTable(defaultAuxIdent, defaultAuxTableIdentifier, trackedAuxSchema)
    createTable(defaultTargetIdent, defaultTargetTableIdentifier, trackedCanonicalSchema)

    execWith(trackedProcessor).execute(
      microbatchOf(trackedSourceSchema)(
        Row(1, "alice", 100, 10L, false),
        Row(1, "bob", 100, 20L, false) // tracked name changed -> new interval
      ),
      batchId = 1L
    )

    checkAnswer(
      targetTable,
      Seq(
        Row(1, "alice", 100, 10L, 20L, meta(10L)),
        Row(1, "bob", 100, 20L, null, meta(20L))
      )
    )
    assert(auxTable.collect().isEmpty)

    // A second tracked change in a separate microbatch closes the now-current record at the new
    // event and opens another, leaving the earlier history (alice) untouched.
    execWith(trackedProcessor).execute(
      microbatchOf(trackedSourceSchema)(
        Row(1, "carol", 100, 30L, false) // tracked name changed again -> another new interval
      ),
      batchId = 2L
    )

    checkAnswer(
      targetTable,
      Seq(
        Row(1, "alice", 100, 10L, 20L, meta(10L)),
        Row(1, "bob", 100, 20L, 30L, meta(20L)),
        Row(1, "carol", 100, 30L, null, meta(30L))
      )
    )
    assert(auxTable.collect().isEmpty)
  }

  test("multiple new keys in one batch each build their own history independently") {
    createAuxTable()
    createTargetTable()

    runBatch(1L)(
      upsert(1, "a", 10L),
      upsert(2, "p", 10L),
      upsert(1, "b", 20L), // key 1 changes value
      upsert(2, "p", 20L) // key 2 no-op run
    )

    checkAnswer(
      targetTable,
      Seq(
        targetRow(1, "a", 10L, 20L, 10L),
        targetRow(1, "b", 20L, null, 20L),
        targetRow(2, "p", 10L, null, 20L)
      )
    )
    checkAnswer(auxTable, auxRow(2, "p", 10L, null, 10L, null))
  }

  test("a composite key distinguishes rows that share a single key component") {
    val compositeSourceSchema = new StructType()
      .add("country", StringType)
      .add("city", StringType)
      .add("population", StringType)
      .add("seq", LongType)
      .add("is_delete", BooleanType)
    val compositeCanonicalSchema = new StructType()
      .add("country", StringType)
      .add("city", StringType)
      .add("population", StringType)
      .add(Scd2BatchProcessor.startAtColName, LongType, nullable = true)
      .add(Scd2BatchProcessor.endAtColName, LongType, nullable = true)
      .add(AutoCdcReservedNames.cdcMetadataColName, scd2MetadataSchema, nullable = false)
    val compositeAuxSchema = compositeCanonicalSchema
      .add(Scd2BatchProcessor.deletedByBatchIdColName, LongType, nullable = true)

    val compositeProcessor = Scd2BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("country"), UnqualifiedColumnName("city")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type2,
        deleteCondition = Some(F.col("is_delete")),
        columnSelection = Some(
          ColumnSelection.ExcludeColumns(
            Seq(UnqualifiedColumnName("seq"), UnqualifiedColumnName("is_delete"))
          )
        )
      ),
      resolvedSequencingType = LongType
    )

    createTable(defaultAuxIdent, defaultAuxTableIdentifier, compositeAuxSchema)
    createTable(defaultTargetIdent, defaultTargetTableIdentifier, compositeCanonicalSchema)

    execWith(compositeProcessor).execute(
      microbatchOf(compositeSourceSchema)(
        // Same city name, different country: distinct identities, never coalesced.
        Row("US", "Springfield", "100", 10L, false),
        Row("CA", "Springfield", "200", 10L, false),
        Row("US", "Springfield", "150", 20L, false) // updates only the US identity
      ),
      batchId = 1L
    )

    checkAnswer(
      targetTable,
      Seq(
        Row("US", "Springfield", "100", 10L, 20L, meta(10L)),
        Row("US", "Springfield", "150", 20L, null, meta(20L)),
        Row("CA", "Springfield", "200", 10L, null, meta(10L))
      )
    )
    assert(auxTable.collect().isEmpty)
  }

  test("a key referenced with different casing resolves under case-insensitive analysis") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val caseProcessor = Scd2BatchProcessor(
        changeArgs = ChangeArgs(
          keys = Seq(UnqualifiedColumnName("ID")), // upper-case reference to lower-case `id`
          sequencing = F.col("seq"),
          storedAsScdType = ScdType.Type2,
          deleteCondition = Some(F.col("is_delete")),
          columnSelection = Some(
            ColumnSelection.ExcludeColumns(
              Seq(UnqualifiedColumnName("seq"), UnqualifiedColumnName("is_delete"))
            )
          )
        ),
        resolvedSequencingType = LongType
      )

      createAuxTable()
      createTargetTable()

      execWith(caseProcessor).execute(
        microbatchOf(sourceSchema)(upsert(1, "a", 10L), upsert(1, "b", 20L)),
        batchId = 1L
      )

      checkAnswer(
        targetTable,
        Seq(
          targetRow(1, "a", 10L, 20L, 10L),
          targetRow(1, "b", 20L, null, 20L)
        )
      )
      assert(auxTable.collect().isEmpty)
    }
  }

  test("a key referenced with non-matching casing fails under case-sensitive analysis") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val caseProcessor = Scd2BatchProcessor(
        changeArgs = ChangeArgs(
          keys = Seq(UnqualifiedColumnName("ID")), // does not match lower-case `id`
          sequencing = F.col("seq"),
          storedAsScdType = ScdType.Type2,
          deleteCondition = Some(F.col("is_delete")),
          columnSelection = Some(
            ColumnSelection.ExcludeColumns(
              Seq(UnqualifiedColumnName("seq"), UnqualifiedColumnName("is_delete"))
            )
          )
        ),
        resolvedSequencingType = LongType
      )

      createAuxTable()
      createTargetTable()

      intercept[AnalysisException] {
        execWith(caseProcessor).execute(
          microbatchOf(sourceSchema)(upsert(1, "a", 10L)),
          batchId = 1L
        )
      }
    }
  }
}
