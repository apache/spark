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

import org.apache.spark.sql.{functions => F, AnalysisException, Row}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Tests for [[Scd1BatchProcessor]] methods that perform a `MERGE INTO` against a registered
 * v2 table. These tests require a v2 catalog that supports row-level operations
 * (set up by [[AutoCdcCatalogExecutionTestBase]]) and run actual writes through Catalyst's
 * row-level-operations machinery, so they are kept separate from the pure-DataFrame-transform
 * tests in [[Scd1BatchProcessorSuite]].
 */
class Scd1BatchProcessorMergeSuite
    extends QueryTest
    with SharedSparkSession
    with BeforeAndAfter
    with AutoCdcCatalogExecutionTestBase {

  /**
   * Minimal valid shape for both the auxiliary table and microbatch inputs in these tests:
   * a single key column `id` plus the CDC metadata struct. The auxiliary table genuinely
   * has only this shape in production, and the merge function reduces its microbatch input
   * down to keys + `_cdc_metadata` regardless of incoming data columns -- so most tests can
   * use this single schema for both ends.
   */
  private val minimalSchema: StructType = new StructType()
    .add("id", IntegerType)
    .add(AutoCdcReservedNames.cdcMetadataColName, cdcMetadataColSchemaType())

  /** Minimal target-table shape: one key, one data column, and CDC metadata. */
  private val targetSchema: StructType = new StructType()
    .add("id", IntegerType)
    .add("value", StringType)
    .add(AutoCdcReservedNames.cdcMetadataColName, cdcMetadataColSchemaType())

  /**
   * A processor with a single key column `id`. `sequencing` is irrelevant for
   * merge functions in this suite: they operate entirely on the already-computed CDC metadata
   * column, never on the raw sequencing expression.
   */
  private val processor = Scd1BatchProcessor(
    changeArgs = ChangeArgs(
      keys = Seq(UnqualifiedColumnName("id")),
      sequencing = F.lit(0L),
      storedAsScdType = ScdType.Type1
    ),
    resolvedSequencingType = LongType
  )

  /** Create the auxiliary table using [[minimalSchema]], optionally seeded with `seedRows`. */
  private def createAuxTable(seedRows: Row*): Unit =
    createTable(defaultAuxIdent, defaultAuxTableIdentifier, minimalSchema, seedRows: _*)

  /** Create the target table using [[targetSchema]], optionally seeded with `seedRows`. */
  private def createTargetTable(seedRows: Row*): Unit =
    createTable(defaultTargetIdent, defaultTargetTableIdentifier, targetSchema, seedRows: _*)

  /**
   * Build an auxiliary-table schema with the given key columns followed by the standard CDC
   * metadata struct. Used by tests that need a non-trivial key shape (composite or dotted).
   */
  private def customKeyAuxSchema(keyColumns: Seq[(String, DataType)]): StructType = {
    val withKeys = keyColumns.foldLeft(new StructType()) { case (s, (name, dt)) =>
      s.add(name, dt)
    }
    withKeys.add(AutoCdcReservedNames.cdcMetadataColName, cdcMetadataColSchemaType())
  }

  /**
   * Create the auxiliary table at [[defaultAuxIdent]] using `schema` and optionally seed it
   * with `seedRows`. Used by tests that need a non-trivial key shape (composite or dotted).
   */
  private def createAuxTableWithSchema(schema: StructType, seedRows: Row*): Unit =
    createTable(defaultAuxIdent, defaultAuxTableIdentifier, schema, seedRows: _*)

  /**
   * `(name, dataType)` pairs of `schema`'s fields, used to compare two schemas for structural
   * equivalence while deliberately ignoring nullability and metadata.
   */
  private def columnNamesAndDataTypes(schema: StructType): Seq[(String, DataType)] =
    schema.fields.map(f => (f.name, f.dataType)).toSeq

  // =============== mergeMicrobatchOntoAuxiliaryTable tests ===============

  test("mergeMicrobatchOntoAuxiliaryTable replaces an existing tombstone with a newer " +
    "microbatch tombstone, dropping any microbatch-only data columns") {
    createAuxTable(Row(1, cdcMetadataRow(deleteSeq = Some(10L), upsertSeq = None)))

    // The microbatch carries an extra `value` data column that has no place in the auxiliary
    // table. mergeMicrobatchOntoAuxiliaryTable must project it away before merging, both to
    // satisfy MergeIntoTable's schema requirements and to keep the auxiliary table free of
    // unrelated columns.
    val microbatchSchema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add(
        AutoCdcReservedNames.cdcMetadataColName,
        new StructType()
          .add(Scd1BatchProcessor.cdcDeleteSequenceFieldName, LongType)
          .add(Scd1BatchProcessor.cdcUpsertSequenceFieldName, LongType)
      )
    val microbatch = microbatchOf(microbatchSchema)(
      Row(1, "data-leak", cdcMetadataRow(deleteSeq = Some(20L), upsertSeq = None))
    )

    processor.mergeMicrobatchOntoAuxiliaryTable(microbatch, defaultAuxTableIdentifier)

    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    // Tombstone advanced to delete=20, with exactly one row per key (no duplicate tombstones).
    checkAnswer(resultAuxTable, Row(1, Row(20L, null)))
    // Schema strictly matches minimalSchema; the `value` column was dropped, not smuggled in.
    assert(columnNamesAndDataTypes(resultAuxTable.schema) == columnNamesAndDataTypes(minimalSchema))
  }

  test("mergeMicrobatchOntoAuxiliaryTable deletes an existing tombstone when superseded by a " +
    "newer microbatch upsert") {
    createAuxTable(Row(1, cdcMetadataRow(deleteSeq = Some(10L), upsertSeq = None)))

    val microbatch = microbatchOf(minimalSchema)(
      Row(1, cdcMetadataRow(deleteSeq = None, upsertSeq = Some(20L)))
    )

    processor.mergeMicrobatchOntoAuxiliaryTable(microbatch, defaultAuxTableIdentifier)

    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    assert(resultAuxTable.collect().isEmpty)
  }

  test("mergeMicrobatchOntoAuxiliaryTable inserts a new tombstone for a previously-untracked " +
    "key") {
    createAuxTable()

    val microbatch = microbatchOf(minimalSchema)(
      Row(1, cdcMetadataRow(deleteSeq = Some(10L), upsertSeq = None))
    )

    processor.mergeMicrobatchOntoAuxiliaryTable(microbatch, defaultAuxTableIdentifier)

    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    checkAnswer(resultAuxTable, Row(1, Row(10L, null)))
  }

  test("mergeMicrobatchOntoAuxiliaryTable leaves rows for unrelated keys untouched") {
    createAuxTable(Row(1, cdcMetadataRow(deleteSeq = Some(10L), upsertSeq = None)))

    // Microbatch event affects a different key entirely; the existing tombstone for id=1 must
    // not be touched even though the new tombstone's sequence is much larger.
    val microbatch = microbatchOf(minimalSchema)(
      Row(2, cdcMetadataRow(deleteSeq = Some(100L), upsertSeq = None))
    )

    processor.mergeMicrobatchOntoAuxiliaryTable(microbatch, defaultAuxTableIdentifier)

    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    checkAnswer(resultAuxTable, Seq(
      Row(1, Row(10L, null)),
      Row(2, Row(100L, null))
    ))
  }

  test("mergeMicrobatchOntoAuxiliaryTable ignores microbatch deletes whose sequence is older " +
    "than the existing tombstone") {
    // This documents that mergeMicrobatchOntoAuxiliaryTable's contract is stronger than just
    // relying on applyTombstonesToMicrobatch having filtered out stale events upstream: even
    // an unfiltered stale incoming delete must not regress the high-water mark.
    createAuxTable(Row(1, cdcMetadataRow(deleteSeq = Some(10L), upsertSeq = None)))

    val microbatch = microbatchOf(minimalSchema)(
      Row(1, cdcMetadataRow(deleteSeq = Some(5L), upsertSeq = None))
    )

    processor.mergeMicrobatchOntoAuxiliaryTable(microbatch, defaultAuxTableIdentifier)

    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    checkAnswer(resultAuxTable, Row(1, Row(10L, null)))
  }

  test("mergeMicrobatchOntoAuxiliaryTable ignores microbatch upserts whose sequence is older " +
    "than the existing tombstone") {
    createAuxTable(Row(1, cdcMetadataRow(deleteSeq = Some(10L), upsertSeq = None)))

    val microbatch = microbatchOf(minimalSchema)(
      Row(1, cdcMetadataRow(deleteSeq = None, upsertSeq = Some(5L)))
    )

    processor.mergeMicrobatchOntoAuxiliaryTable(microbatch, defaultAuxTableIdentifier)

    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    checkAnswer(resultAuxTable, Row(1, Row(10L, null)))
  }

  test("mergeMicrobatchOntoAuxiliaryTable applies the tied-sequence asymmetry: equal deletes " +
    "are kept, equal upserts delete the tombstone") {
    // On a delete<->upsert sequencing tie, upsert events are given priority over deletes;
    // therefore an incoming upsert with the same sequence as a tombstone should delete the
    // tombstone. On a delete<->delete sequencing tie, the effect is a no-op. This is an
    // internal SCD1 tie-breaking convention, not a publicly documented contract.
    createAuxTable(
      Row(1, cdcMetadataRow(deleteSeq = Some(10L), upsertSeq = None)),
      Row(2, cdcMetadataRow(deleteSeq = Some(20L), upsertSeq = None))
    )

    val microbatch = microbatchOf(minimalSchema)(
      Row(1, cdcMetadataRow(deleteSeq = Some(10L), upsertSeq = None)),
      Row(2, cdcMetadataRow(deleteSeq = None, upsertSeq = Some(20L)))
    )

    processor.mergeMicrobatchOntoAuxiliaryTable(microbatch, defaultAuxTableIdentifier)

    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    // Row 1's tombstone remains the same, but row 2's tombstone should be marked as stale and
    // deleted.
    checkAnswer(resultAuxTable, Row(1, Row(10L, null)))
  }

  test("mergeMicrobatchOntoAuxiliaryTable upsert event for different key does not affect " +
    "tombstone") {
    createAuxTable(Row(2, cdcMetadataRow(deleteSeq = Some(5L), upsertSeq = None)))

    val microbatch = microbatchOf(minimalSchema)(
      // Although the upsert seq is 10, this is for key=1; tombstone for key=2 should be unaffected.
      Row(1, cdcMetadataRow(deleteSeq = None, upsertSeq = Some(10L)))
    )

    processor.mergeMicrobatchOntoAuxiliaryTable(microbatch, defaultAuxTableIdentifier)

    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    checkAnswer(resultAuxTable, Row(2, Row(5L, null)))
  }

  test("mergeMicrobatchOntoAuxiliaryTable is idempotent across a microbatch that exercises " +
    "every merge clause") {
    // The auxiliary table starts with three tombstones; the microbatch then exercises every
    // merge clause simultaneously:
    //   - id=1: aux tombstone superseded by a microbatch upsert
    //   - id=2: aux tombstone advanced by a newer microbatch delete
    //   - id=3: untouched by the microbatch
    //   - id=4: new tombstone for an untracked key
    createAuxTable(
      Row(1, cdcMetadataRow(deleteSeq = Some(10L), upsertSeq = None)),
      Row(2, cdcMetadataRow(deleteSeq = Some(20L), upsertSeq = None)),
      Row(3, cdcMetadataRow(deleteSeq = Some(30L), upsertSeq = None))
    )

    val microbatch = microbatchOf(minimalSchema)(
      Row(1, cdcMetadataRow(deleteSeq = None, upsertSeq = Some(15L))),
      Row(2, cdcMetadataRow(deleteSeq = Some(25L), upsertSeq = None)),
      Row(4, cdcMetadataRow(deleteSeq = Some(40L), upsertSeq = None))
    )

    val expectedAfterMerge = Seq(
      Row(2, Row(25L, null)),
      Row(3, Row(30L, null)),
      Row(4, Row(40L, null))
    )

    // First merge applies all three clauses exactly once.
    processor.mergeMicrobatchOntoAuxiliaryTable(microbatch, defaultAuxTableIdentifier)
    val auxTableAfterFirstMerge = spark.read.table(defaultAuxTableIdentifier.quotedString)
    checkAnswer(auxTableAfterFirstMerge, expectedAfterMerge)

    // Re-applying the same microbatch is a no-op:
    //   - id=1 is absent from aux; whenNotMatched is gated on delete events => skipped.
    //   - id=2 has tied delete (incoming==aux); strict `>` in the update clause fails.
    //   - id=4 has tied delete (incoming==aux); same reason.
    processor.mergeMicrobatchOntoAuxiliaryTable(microbatch, defaultAuxTableIdentifier)
    val auxTableAfterSecondMerge = spark.read.table(defaultAuxTableIdentifier.quotedString)
    checkAnswer(auxTableAfterSecondMerge, expectedAfterMerge)
  }

  test("mergeMicrobatchOntoAuxiliaryTable correctly inserts tombstones for composite key") {
    // Composite key: (region, customer_id). The merge join condition is the AND of every key
    // column equality, so an aux row sharing only `region` with the microbatch must NOT be
    // touched, while the microbatch row must be inserted as a new tombstone.
    val compositeSchema = customKeyAuxSchema(Seq(
      "region" -> StringType,
      "customer_id" -> IntegerType
    ))
    createAuxTableWithSchema(
      compositeSchema,
      Row("US", 99, cdcMetadataRow(deleteSeq = Some(50L), upsertSeq = None))
    )

    val compositeKeyProcessor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("region"), UnqualifiedColumnName("customer_id")),
        sequencing = F.lit(0L),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    val microbatch = microbatchOf(compositeSchema)(
      Row("US", 1, cdcMetadataRow(deleteSeq = Some(10L), upsertSeq = None))
    )

    compositeKeyProcessor.mergeMicrobatchOntoAuxiliaryTable(microbatch, defaultAuxTableIdentifier)

    checkAnswer(spark.read.table(defaultAuxTableIdentifier.quotedString), Seq(
      Row("US", 99, Row(50L, null)),
      Row("US", 1, Row(10L, null))
    ))
  }

  test("mergeMicrobatchOntoAuxiliaryTable correctly merges for backticked/dotted keys") {
    // Even though the column is a backticked identifier in user-facing DDL, Spark drops the
    // backticks during schema resolution so the field name is the literal `user.id`. The merge
    // path must propagate the user's quoted identifier through `k.quoted` so the join condition
    // and update target both resolve to the same physical column.
    val dottedKeySchema = customKeyAuxSchema(Seq("user.id" -> IntegerType))
    createAuxTableWithSchema(
      dottedKeySchema,
      Row(1, cdcMetadataRow(deleteSeq = Some(10L), upsertSeq = None))
    )

    val dottedKeyProcessor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("`user.id`")),
        sequencing = F.lit(0L),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    // We expect the existing tombstone with del seq=10 to be advanced to 20 if the merge matches
    // dotted keys correctly.
    val microbatch = microbatchOf(dottedKeySchema)(
      Row(1, cdcMetadataRow(deleteSeq = Some(20L), upsertSeq = None))
    )

    dottedKeyProcessor.mergeMicrobatchOntoAuxiliaryTable(microbatch, defaultAuxTableIdentifier)

    checkAnswer(spark.read.table(defaultAuxTableIdentifier.quotedString), Row(1, Row(20L, null)))
  }

  // =============== mergeMicrobatchOntoTarget tests ===============

  test("mergeMicrobatchOntoTarget updates an existing row with a newer upsert") {
    createTargetTable(Row(1, "old", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(10L))))

    val microbatch = microbatchOf(targetSchema)(
      Row(1, "new", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(20L)))
    )

    processor.mergeMicrobatchOntoTarget(microbatch, defaultTargetTableIdentifier)

    val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
    checkAnswer(resultTargetTable, Row(1, "new", Row(null, 20L)))
    assert(columnNamesAndDataTypes(resultTargetTable.schema) ==
      columnNamesAndDataTypes(targetSchema))
  }

  test("mergeMicrobatchOntoTarget deletes an existing row with a newer delete") {
    createTargetTable(
      Row(1, "delete-me", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(10L))),
      Row(2, "keep-me", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(20L)))
    )

    val microbatch = microbatchOf(targetSchema)(
      Row(1, "unused", cdcMetadataRow(deleteSeq = Some(15L), upsertSeq = None))
    )

    processor.mergeMicrobatchOntoTarget(microbatch, defaultTargetTableIdentifier)

    val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
    checkAnswer(resultTargetTable, Row(2, "keep-me", Row(null, 20L)))
  }

  test("mergeMicrobatchOntoTarget inserts new upserts but not new (tombstone) deletes") {
    createTargetTable(Row(1, "existing", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(10L))))

    val microbatch = microbatchOf(targetSchema)(
      Row(2, "insert-me", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(20L))),
      Row(3, "do-not-insert", cdcMetadataRow(deleteSeq = Some(30L), upsertSeq = None))
    )

    processor.mergeMicrobatchOntoTarget(microbatch, defaultTargetTableIdentifier)

    val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
    checkAnswer(resultTargetTable, Seq(
      Row(1, "existing", Row(null, 10L)),
      Row(2, "insert-me", Row(null, 20L))
    ))
  }

  test("mergeMicrobatchOntoTarget ignores stale upserts and stale deletes") {
    createTargetTable(
      Row(1, "target-delete-tie", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(10L))),
      Row(2, "target-newer", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(20L)))
    )

    val microbatch = microbatchOf(targetSchema)(
      Row(1, "delete-tie", cdcMetadataRow(deleteSeq = Some(10L), upsertSeq = None)),
      Row(2, "older-upsert", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(15L)))
    )

    processor.mergeMicrobatchOntoTarget(microbatch, defaultTargetTableIdentifier)

    val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
    checkAnswer(resultTargetTable, Seq(
      Row(1, "target-delete-tie", Row(null, 10L)),
      Row(2, "target-newer", Row(null, 20L))
    ))
  }

  test("mergeMicrobatchOntoTarget gives tied upserts priority over the target row") {
    createTargetTable(Row(1, "old", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(10L))))

    val microbatch = microbatchOf(targetSchema)(
      Row(1, "same-sequence-upsert", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(10L)))
    )

    processor.mergeMicrobatchOntoTarget(microbatch, defaultTargetTableIdentifier)

    val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
    checkAnswer(resultTargetTable, Row(1, "same-sequence-upsert", Row(null, 10L)))
  }

  test("mergeMicrobatchOntoTarget correctly matches escaped key column names") {
    // The raw key name contains special characters that would require being escaped on name
    // resolution.
    val rawKeyName = "a`b"
    val schemaWithSpecialKeyCharacters = new StructType()
      // The schema always stores the backtick consumed column name, so unticked the raw name here.
      .add(rawKeyName, IntegerType)
      .add("value", StringType)
      .add(AutoCdcReservedNames.cdcMetadataColName, cdcMetadataColSchemaType())

    createTable(
      defaultTargetIdent,
      defaultTargetTableIdentifier,
      schemaWithSpecialKeyCharacters,
      Row(1, "old", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(10L)))
    )

    val processorForCustomKeySchema = processor.copy(
      changeArgs = processor.changeArgs.copy(
        keys = Seq(UnqualifiedColumnName(QuotingUtils.quoteIdentifier(rawKeyName)))
      )
    )
    val microbatch = microbatchOf(schemaWithSpecialKeyCharacters)(
      Row(1, "new", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(20L)))
    )

    processorForCustomKeySchema.mergeMicrobatchOntoTarget(
      microbatch,
      defaultTargetTableIdentifier
    )

    val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
    checkAnswer(resultTargetTable, Row(1, "new", Row(null, 20L)))
  }

  gridTest(
    "mergeMicrobatchOntoTarget key column comparison respects spark session case sensitivity"
  )(Seq(false, true)) { caseSensitive =>
    withSQLConf("spark.sql.caseSensitive" -> caseSensitive.toString) {
      createTargetTable(Row(1, "old", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(10L))))

      val processorWithUpperCaseKey = processor.copy(
        changeArgs = processor.changeArgs.copy(
          keys = Seq(UnqualifiedColumnName("ID"))
        )
      )

      val microbatch = microbatchOf(targetSchema)(
        Row(1, "new", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(20L)))
      )

      if (caseSensitive) {
        val ex = intercept[AnalysisException] {
          processorWithUpperCaseKey.mergeMicrobatchOntoTarget(
            microbatch,
            defaultTargetTableIdentifier
          )
        }
        // Intentionally not using checkError here, to avoid asserting on a brittle query context
        // and long message parmeters list.
        assert(ex.errorClass.contains("UNRESOLVED_COLUMN.WITH_SUGGESTION"))
      } else {
        processorWithUpperCaseKey.mergeMicrobatchOntoTarget(
          microbatch,
          defaultTargetTableIdentifier
        )
        val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
        checkAnswer(resultTargetTable, Row(1, "new", Row(null, 20L)))
      }
    }
  }

  test("mergeMicrobatchOntoTarget is idempotent across a microbatch") {
    createTargetTable(
      Row(1, "delete-me", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(10L))),
      Row(2, "update-me", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(20L))),
      Row(3, "untouched", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(30L)))
    )

    val microbatch = microbatchOf(targetSchema)(
      Row(1, "delete-event", cdcMetadataRow(deleteSeq = Some(15L), upsertSeq = None)),
      Row(2, "updated", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(25L))),
      Row(4, "inserted", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(40L))),
      Row(5, "absent-delete", cdcMetadataRow(deleteSeq = Some(50L), upsertSeq = None))
    )

    val expectedAfterMerge = Seq(
      Row(2, "updated", Row(null, 25L)),
      Row(3, "untouched", Row(null, 30L)),
      Row(4, "inserted", Row(null, 40L))
    )

    processor.mergeMicrobatchOntoTarget(microbatch, defaultTargetTableIdentifier)
    val targetTableAfterFirstMerge = spark.read.table(defaultTargetTableIdentifier.quotedString)
    checkAnswer(targetTableAfterFirstMerge, expectedAfterMerge)

    processor.mergeMicrobatchOntoTarget(microbatch, defaultTargetTableIdentifier)
    val targetTableAfterSecondMerge = spark.read.table(defaultTargetTableIdentifier.quotedString)
    checkAnswer(targetTableAfterSecondMerge, expectedAfterMerge)
  }
}
