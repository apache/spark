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

import org.apache.spark.sql.{functions => F, QueryTest, Row}
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Catalog-backed unit tests for the SCD2 `MERGE INTO` routines
 * [[Scd2BatchProcessor.mergeRowsIntoAuxiliaryTable]] and
 * [[Scd2BatchProcessor.mergeRowsIntoTargetTable]]. These exercise every merge branch (insert,
 * update, delete, and - for the aux table - logical delete and garbage collection) against an
 * in-memory v2 row-level catalog.
 */
class Scd2BatchProcessorMergeSuite
    extends QueryTest
    with SharedSparkSession
    with BeforeAndAfter
    with AutoCdcCatalogExecutionTestBase {

  private val userSchema = new StructType()
    .add("id", IntegerType)
    .add("value", StringType)

  /** Canonical SCD2 row schema: user columns + framework start/end + cdc metadata struct. */
  private val canonicalSchema = userSchema
    .add(Scd2BatchProcessor.startAtColName, LongType, nullable = true)
    .add(Scd2BatchProcessor.endAtColName, LongType, nullable = true)
    .add(
      AutoCdcReservedNames.cdcMetadataColName,
      Scd2BatchProcessor.cdcMetadataColSchema(LongType),
      nullable = false
    )

  /** Auxiliary table schema: canonical schema plus the aux-only logical-delete marker column. */
  private val auxSchema = canonicalSchema
    .add(Scd2BatchProcessor.deletedByBatchIdColName, LongType, nullable = true)

  /** Tagged schema consumed by the merges: canonical schema plus the routing flag. */
  private val taggedSchema = canonicalSchema
    .add(Scd2BatchProcessor.shouldRouteToAuxTableColName, BooleanType, nullable = false)

  private val processor = Scd2BatchProcessor(
    changeArgs = ChangeArgs(
      keys = Seq(UnqualifiedColumnName("id")),
      sequencing = F.col("seq"),
      storedAsScdType = ScdType.Type2
    ),
    resolvedSequencingType = LongType
  )

  /** Build a tagged-rows [[DataFrame]] (canonical columns + routing flag) for the merges. */
  private def taggedOf(rows: Row*): DataFrame = microbatchOf(taggedSchema)(rows: _*)

  /** Build a canonical SCD2 [[DataFrame]] (e.g. the `findAffected*` outputs) for the merges. */
  private def canonicalOf(rows: Row*): DataFrame = microbatchOf(canonicalSchema)(rows: _*)

  private def createAuxTable(seedRows: Row*): Unit =
    createTable(defaultAuxIdent, defaultAuxTableIdentifier, auxSchema, seedRows: _*)

  private def createTargetTable(seedRows: Row*): Unit =
    createTable(defaultTargetIdent, defaultTargetTableIdentifier, canonicalSchema, seedRows: _*)

  private def auxTable: DataFrame =
    spark.read.table(defaultAuxTableIdentifier.quotedString)

  private def targetTable: DataFrame =
    spark.read.table(defaultTargetTableIdentifier.quotedString)

  // ===========================================================================================
  // mergeRowsIntoAuxiliaryTable tests
  // ===========================================================================================

  test("mergeRowsIntoAuxiliaryTable inserts a routed row absent from the aux table") {
    createAuxTable()

    // A freshly-routed tombstone with no matching aux row is inserted live (no logical-delete
    // marker).
    val tagged = taggedOf(Row(1, "x", 5L, 5L, Row(5L), true))
    val affected = canonicalOf()

    processor.mergeRowsIntoAuxiliaryTable(tagged, affected, defaultAuxTableIdentifier, batchId = 1L)

    checkAnswer(auxTable, Row(1, "x", 5L, 5L, Row(5L), null))
  }

  test("mergeRowsIntoAuxiliaryTable updates a surviving routed row in place") {
    // An existing hidden no-op upsert at recordStartAt=5 that survives reconciliation.
    createAuxTable(Row(1, "old", 5L, null, Row(5L), null))

    val tagged = taggedOf(Row(1, "new", 5L, null, Row(5L), true))
    // The affected row survives (still present in the routed set), so it is not logically
    // deleted; only its non-key columns are refreshed.
    val affected = canonicalOf(Row(1, "old", 5L, null, Row(5L)))

    processor.mergeRowsIntoAuxiliaryTable(tagged, affected, defaultAuxTableIdentifier, batchId = 2L)

    checkAnswer(auxTable, Row(1, "new", 5L, null, Row(5L), null))
  }

  test("mergeRowsIntoAuxiliaryTable logically deletes an affected row that did not survive") {
    // A tombstone pulled in as affected but absent from this batch's routed set.
    createAuxTable(Row(1, "gone", 7L, 7L, Row(7L), null))

    // The single tagged row is not routed to the aux table, so the affected aux row has no
    // surviving counterpart and must be logically deleted.
    val tagged = taggedOf(Row(1, "vis", 10L, null, Row(10L), false))
    val affected = canonicalOf(Row(1, "gone", 7L, 7L, Row(7L)))

    processor.mergeRowsIntoAuxiliaryTable(tagged, affected, defaultAuxTableIdentifier, batchId = 3L)

    // Logical, not physical: the row stays but is stamped with this batch's id.
    checkAnswer(auxTable, Row(1, "gone", 7L, 7L, Row(7L), 3L))
  }

  test("mergeRowsIntoAuxiliaryTable garbage-collects rows logically deleted by an older batch") {
    createAuxTable(
      // Logically deleted by an older batch (2 != 5) -> physically garbage-collected.
      Row(1, "gc-old", 7L, 7L, Row(7L), 2L),
      // Still live (no marker) -> retained.
      Row(2, "live", 3L, null, Row(3L), null),
      // Logically deleted by the current batch (5 == 5) -> retained for replay-stability.
      Row(3, "this-batch", 4L, 4L, Row(4L), 5L)
    )

    // One brand-new routed insert keeps the merge source non-empty; none of the seeded rows are
    // in the affected set, so they are all evaluated by the not-matched-by-source GC clause.
    val tagged = taggedOf(Row(10, "newtomb", 8L, 8L, Row(8L), true))
    val affected = canonicalOf()

    processor.mergeRowsIntoAuxiliaryTable(tagged, affected, defaultAuxTableIdentifier, batchId = 5L)

    checkAnswer(
      auxTable,
      Seq(
        Row(2, "live", 3L, null, Row(3L), null),
        Row(3, "this-batch", 4L, 4L, Row(4L), 5L),
        Row(10, "newtomb", 8L, 8L, Row(8L), null)
      )
    )
  }

  // ===========================================================================================
  // mergeRowsIntoTargetTable tests
  // ===========================================================================================

  test("mergeRowsIntoTargetTable inserts a visible upsert absent from the target table") {
    createTargetTable()

    val tagged = taggedOf(Row(1, "v", 5L, null, Row(5L), false))
    val affected = canonicalOf()

    processor.mergeRowsIntoTargetTable(tagged, affected, defaultTargetTableIdentifier)

    checkAnswer(targetTable, Row(1, "v", 5L, null, Row(5L)))
  }

  test("mergeRowsIntoTargetTable updates a visible row matched at the same recordStartAt") {
    createTargetTable(Row(1, "old", 5L, null, Row(5L)))

    // The run head at recordStartAt=5 is now closed at 20 with refreshed data; it matches and
    // updates the existing target row in place.
    val tagged = taggedOf(Row(1, "new", 5L, 20L, Row(5L), false))
    val affected = canonicalOf(Row(1, "old", 5L, null, Row(5L)))

    processor.mergeRowsIntoTargetTable(tagged, affected, defaultTargetTableIdentifier)

    checkAnswer(targetTable, Row(1, "new", 5L, 20L, Row(5L)))
  }

  test("mergeRowsIntoTargetTable deletes an affected row reconciled away") {
    createTargetTable(Row(1, "old", 5L, null, Row(5L)))

    // The only tagged row routes to the aux table (e.g. closed into a tombstone), so no visible
    // row survives for key 1: the previously-affected target row must be deleted.
    val tagged = taggedOf(Row(1, "x", 5L, 5L, Row(5L), true))
    val affected = canonicalOf(Row(1, "old", 5L, null, Row(5L)))

    processor.mergeRowsIntoTargetTable(tagged, affected, defaultTargetTableIdentifier)

    assert(targetTable.collect().isEmpty)
  }

  test("mergeRowsIntoTargetTable only writes visible upsert rows, filtering aux/non-upsert rows") {
    createTargetTable()

    // Only the open upsert (key 1) is visible. The tombstone (routed) and hidden no-op (routed)
    // are filtered by the routing flag; the decomposition tail is filtered because it is not an
    // upsert-representing row.
    val tagged = taggedOf(
      Row(1, "vis", 5L, null, Row(5L), false),
      Row(2, "tomb", 7L, 7L, Row(7L), true),
      Row(3, "tail", null, 9L, Row(null), false),
      Row(4, "hidden", 5L, null, Row(5L), true)
    )
    val affected = canonicalOf()

    processor.mergeRowsIntoTargetTable(tagged, affected, defaultTargetTableIdentifier)

    checkAnswer(targetTable, Row(1, "vis", 5L, null, Row(5L)))
  }

  test("mergeRowsIntoTargetTable applies insert, update, and delete branches in one merge") {
    createTargetTable(
      Row(1, "old1", 5L, null, Row(5L)),
      Row(2, "old2", 8L, null, Row(8L))
    )

    val tagged = taggedOf(
      // Matches key 1 at recordStartAt=5 -> update.
      Row(1, "new1", 5L, 30L, Row(5L), false),
      // New key 3 -> insert.
      Row(3, "ins3", 12L, null, Row(12L), false)
      // Key 2 has no surviving visible row -> delete.
    )
    val affected = canonicalOf(
      Row(1, "old1", 5L, null, Row(5L)),
      Row(2, "old2", 8L, null, Row(8L))
    )

    processor.mergeRowsIntoTargetTable(tagged, affected, defaultTargetTableIdentifier)

    checkAnswer(
      targetTable,
      Seq(
        Row(1, "new1", 5L, 30L, Row(5L)),
        Row(3, "ins3", 12L, null, Row(12L))
      )
    )
  }
}
