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
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Tests for [[Scd1BatchProcessor]] methods and [[Scd1LeafLevelTargetMerge]], which perform a
 * `MERGE INTO` against a registered v2 table. These tests require a v2 catalog that supports
 * row-level operations (set up by [[AutoCdcCatalogExecutionTestBase]]) and run actual writes
 * through Catalyst's row-level-operations machinery, so they are kept separate from the
 * pure-DataFrame-transform tests in [[Scd1RowLevelReconciliationSuite]].
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

  private val leafChangeArgs: ChangeArgs = processor.changeArgs.copy(
    ignoreNullSelection = Some(ColumnSelection.ExcludeColumns(Seq.empty)))

  private val leafTargetSchema: StructType = new StructType()
    .add("id", IntegerType)
    .add("left", StringType)
    .add("right", StringType)
    .add(AutoCdcReservedNames.cdcMetadataColName, cdcMetadataColSchemaType())

  private def versionMap(entries: (Seq[String], Long)*): Map[String, Long] =
    entries.map { case (path, sequence) => Scd1VersionMap.serializeKey(path) -> sequence }.toMap

  private def unauthoredVersionMap(paths: Seq[String]*): Map[String, java.lang.Long] =
    paths.map(path => Scd1VersionMap.serializeKey(path) -> null).toMap

  /** Merges `microbatch` onto the target table at [[defaultTargetTableIdentifier]] per leaf. */
  private def mergeMicrobatchOntoTargetPerLeaf(
      microbatch: DataFrame,
      changeArgs: ChangeArgs = leafChangeArgs): Unit =
    new Scd1LeafLevelTargetMerge(
      changeArgs = changeArgs,
      resolvedSequencingType = LongType,
      reconciledMicrobatchDf = microbatch,
      targetTableIdentifier = defaultTargetTableIdentifier
    ).merge()

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
      .add(AutoCdcReservedNames.cdcMetadataColName, cdcMetadataColSchemaType())
    val microbatch = microbatchOf(microbatchSchema)(
      Row(1, "data-leak", cdcMetadataRow(deleteSeq = Some(20L), upsertSeq = None))
    )

    processor.mergeMicrobatchOntoAuxiliaryTable(microbatch, defaultAuxTableIdentifier)

    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    // Tombstone advanced to delete=20, with exactly one row per key (no duplicate tombstones).
    checkAnswer(resultAuxTable, Row(1, Row(20L, null, null)))
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
    checkAnswer(resultAuxTable, Row(1, Row(10L, null, null)))
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
      Row(1, Row(10L, null, null)),
      Row(2, Row(100L, null, null))
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
    checkAnswer(resultAuxTable, Row(1, Row(10L, null, null)))
  }

  test("mergeMicrobatchOntoAuxiliaryTable ignores microbatch upserts whose sequence is older " +
    "than the existing tombstone") {
    createAuxTable(Row(1, cdcMetadataRow(deleteSeq = Some(10L), upsertSeq = None)))

    val microbatch = microbatchOf(minimalSchema)(
      Row(1, cdcMetadataRow(deleteSeq = None, upsertSeq = Some(5L)))
    )

    processor.mergeMicrobatchOntoAuxiliaryTable(microbatch, defaultAuxTableIdentifier)

    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    checkAnswer(resultAuxTable, Row(1, Row(10L, null, null)))
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
    checkAnswer(resultAuxTable, Row(1, Row(10L, null, null)))
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
    checkAnswer(resultAuxTable, Row(2, Row(5L, null, null)))
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
      Row(2, Row(25L, null, null)),
      Row(3, Row(30L, null, null)),
      Row(4, Row(40L, null, null))
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
      Row("US", 99, Row(50L, null, null)),
      Row("US", 1, Row(10L, null, null))
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

    checkAnswer(
      spark.read.table(defaultAuxTableIdentifier.quotedString),
      Row(1, Row(20L, null, null)))
  }

  // =============== mergeMicrobatchOntoTarget tests ===============

  test("mergeMicrobatchOntoTarget updates an existing row with a newer upsert") {
    createTargetTable(Row(1, "old", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(10L))))

    val microbatch = microbatchOf(targetSchema)(
      Row(1, "new", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(20L)))
    )

    processor.mergeMicrobatchOntoTarget(microbatch, defaultTargetTableIdentifier)

    val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
    checkAnswer(resultTargetTable, Row(1, "new", Row(null, 20L, null)))
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
    checkAnswer(resultTargetTable, Row(2, "keep-me", Row(null, 20L, null)))
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
      Row(1, "existing", Row(null, 10L, null)),
      Row(2, "insert-me", Row(null, 20L, null))
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
      Row(1, "target-delete-tie", Row(null, 10L, null)),
      Row(2, "target-newer", Row(null, 20L, null))
    ))
  }

  test("mergeMicrobatchOntoTarget gives tied upserts priority over the target row") {
    createTargetTable(Row(1, "old", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(10L))))

    val microbatch = microbatchOf(targetSchema)(
      Row(1, "same-sequence-upsert", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(10L)))
    )

    processor.mergeMicrobatchOntoTarget(microbatch, defaultTargetTableIdentifier)

    val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
    checkAnswer(resultTargetTable, Row(1, "same-sequence-upsert", Row(null, 10L, null)))
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
    checkAnswer(resultTargetTable, Row(1, "new", Row(null, 20L, null)))
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
        checkAnswer(resultTargetTable, Row(1, "new", Row(null, 20L, null)))
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
      Row(2, "updated", Row(null, 25L, null)),
      Row(3, "untouched", Row(null, 30L, null)),
      Row(4, "inserted", Row(null, 40L, null))
    )

    processor.mergeMicrobatchOntoTarget(microbatch, defaultTargetTableIdentifier)
    val targetTableAfterFirstMerge = spark.read.table(defaultTargetTableIdentifier.quotedString)
    checkAnswer(targetTableAfterFirstMerge, expectedAfterMerge)

    processor.mergeMicrobatchOntoTarget(microbatch, defaultTargetTableIdentifier)
    val targetTableAfterSecondMerge = spark.read.table(defaultTargetTableIdentifier.quotedString)
    checkAnswer(targetTableAfterSecondMerge, expectedAfterMerge)
  }

  // =============== Scd1LeafLevelTargetMerge tests ===============

  test("per-leaf target merge independently selects incoming and target leaf authorships") {
    createTable(
      defaultTargetIdent,
      defaultTargetTableIdentifier,
      leafTargetSchema,
      Row(1, "old-left", "newer-right", cdcMetadataRow(
        deleteSeq = None,
        upsertSeq = Some(30L),
        versionMap = versionMap(Seq("left") -> 10L, Seq("right") -> 30L)))
    )
    val microbatch = microbatchOf(leafTargetSchema)(
      Row(1, "new-left", "stale-right", cdcMetadataRow(
        deleteSeq = None,
        upsertSeq = Some(40L),
        versionMap = versionMap(Seq("left") -> 40L, Seq("right") -> 20L)))
    )

    mergeMicrobatchOntoTargetPerLeaf(microbatch)

    checkAnswer(
      spark.read.table(defaultTargetTableIdentifier.quotedString),
      Row(1, "new-left", "newer-right",
        Row(null, 40L, versionMap(Seq("left") -> 40L, Seq("right") -> 30L))))
  }

  test("per-leaf target merge advances the upsert sequence when no incoming leaf is authored") {
    val targetVersionMap = versionMap(Seq("left") -> 10L, Seq("right") -> 10L)
    createTable(
      defaultTargetIdent,
      defaultTargetTableIdentifier,
      leafTargetSchema,
      Row(1, "old-left", "old-right", cdcMetadataRow(
        deleteSeq = None,
        upsertSeq = Some(10L),
        versionMap = targetVersionMap))
    )
    val microbatch = microbatchOf(leafTargetSchema)(
      Row(1, null, null,
        Row(null, 20L, unauthoredVersionMap(Seq("left"), Seq("right"))))
    )

    mergeMicrobatchOntoTargetPerLeaf(microbatch)

    checkAnswer(
      spark.read.table(defaultTargetTableIdentifier.quotedString),
      Row(1, "old-left", "old-right", Row(null, 20L, targetVersionMap)))
  }

  test("per-leaf target merge handles legacy and schema-evolved target version maps") {
    createTable(
      defaultTargetIdent,
      defaultTargetTableIdentifier,
      leafTargetSchema,
      Row(1, "legacy-left", "legacy-right",
        cdcMetadataRow(deleteSeq = None, upsertSeq = Some(10L))),
      Row(2, "newer-left", null, cdcMetadataRow(
        deleteSeq = None,
        upsertSeq = Some(30L),
        versionMap = versionMap(Seq("left") -> 30L)))
    )
    val microbatch = microbatchOf(leafTargetSchema)(
      Row(1, "tied-left", "stale-right", cdcMetadataRow(
        deleteSeq = None,
        upsertSeq = Some(10L),
        versionMap = versionMap(Seq("left") -> 10L, Seq("right") -> 5L))),
      Row(2, "stale-left", "new-right", cdcMetadataRow(
        deleteSeq = None,
        upsertSeq = Some(20L),
        versionMap = versionMap(Seq("left") -> 20L, Seq("right") -> 1L)))
    )

    mergeMicrobatchOntoTargetPerLeaf(microbatch)

    checkAnswer(spark.read.table(defaultTargetTableIdentifier.quotedString), Seq(
      Row(1, "tied-left", "legacy-right",
        Row(null, 10L, versionMap(Seq("left") -> 10L, Seq("right") -> 10L))),
      Row(2, "newer-left", "new-right",
        Row(null, 30L, versionMap(Seq("left") -> 30L, Seq("right") -> 1L)))
    ))
  }

  test("per-leaf target merge authors nulls without allowing deletes to win ties") {
    createTable(
      defaultTargetIdent,
      defaultTargetTableIdentifier,
      leafTargetSchema,
      Row(1, "delete-me", "preserve-me", cdcMetadataRow(
        deleteSeq = None,
        upsertSeq = Some(30L),
        versionMap = versionMap(Seq("left") -> 10L, Seq("right") -> 30L))),
      Row(2, "tie-left", "tie-right", cdcMetadataRow(
        deleteSeq = None,
        upsertSeq = Some(10L),
        versionMap = versionMap(Seq("left") -> 10L, Seq("right") -> 10L)))
    )
    val microbatch = microbatchOf(leafTargetSchema)(
      // A net upsert at 40 whose `left` a delete at 20 authored null, and which leaves `right`
      // unauthored.
      Row(1, null, null, cdcMetadataRow(
        deleteSeq = None,
        upsertSeq = Some(40L),
        versionMap = versionMap(Seq("left") -> 20L))),
      // A net delete tied with the target row's upsert.
      Row(2, null, null, cdcMetadataRow(deleteSeq = Some(10L), upsertSeq = None))
    )

    mergeMicrobatchOntoTargetPerLeaf(microbatch)

    checkAnswer(spark.read.table(defaultTargetTableIdentifier.quotedString), Seq(
      Row(1, null, "preserve-me",
        Row(null, 40L, versionMap(Seq("left") -> 20L, Seq("right") -> 30L))),
      Row(2, "tie-left", "tie-right",
        Row(null, 10L, versionMap(Seq("left") -> 10L, Seq("right") -> 10L)))
    ))
  }

  test("per-leaf target merge authors nulls for leaves won by an older net delete") {
    createTable(
      defaultTargetIdent,
      defaultTargetTableIdentifier,
      leafTargetSchema,
      Row(1, "delete-me", "preserve-me", cdcMetadataRow(
        deleteSeq = None,
        upsertSeq = Some(30L),
        versionMap = versionMap(Seq("left") -> 10L, Seq("right") -> 30L)))
    )
    // A net delete at 20 carries the values of its delete event. It is older than the target
    // row, so it only authors `left`, as null.
    val microbatch = microbatchOf(leafTargetSchema)(
      Row(1, "delete-left", "delete-right", cdcMetadataRow(deleteSeq = Some(20L), upsertSeq = None))
    )

    mergeMicrobatchOntoTargetPerLeaf(microbatch)

    checkAnswer(
      spark.read.table(defaultTargetTableIdentifier.quotedString),
      Row(1, null, "preserve-me",
        Row(null, 30L, versionMap(Seq("left") -> 20L, Seq("right") -> 30L))))
  }

  test("per-leaf target merge gives target upserts priority over tied persisted tombstones") {
    createTable(
      defaultTargetIdent,
      defaultTargetTableIdentifier,
      leafTargetSchema,
      Row(1, "target-left", "target-right", cdcMetadataRow(
        deleteSeq = None,
        upsertSeq = Some(20L),
        versionMap = versionMap(Seq("left") -> 20L, Seq("right") -> 20L)))
    )
    // This row represents an ignore-null upsert at 30 reconciled with a persisted tombstone at
    // 20. The tombstone authors `left` as null, but does not appear in the row-wide delete
    // sequence; the upsert authors `right`.
    val microbatch = microbatchOf(leafTargetSchema)(
      Row(1, null, "new-right", cdcMetadataRow(
        deleteSeq = None,
        upsertSeq = Some(30L),
        versionMap = versionMap(Seq("left") -> 20L, Seq("right") -> 30L)))
    )

    mergeMicrobatchOntoTargetPerLeaf(microbatch)

    checkAnswer(
      spark.read.table(defaultTargetTableIdentifier.quotedString),
      Row(1, "target-left", "new-right",
        Row(null, 30L, versionMap(Seq("left") -> 20L, Seq("right") -> 30L))))
  }

  test("per-leaf target merge lets only deletes author a whole column the microbatch omits") {
    val targetVersionMap = versionMap(Seq("left") -> 20L, Seq("right") -> 20L)
    val tiedVersionMap = versionMap(Seq("left") -> 30L, Seq("right") -> 30L)
    createTable(
      defaultTargetIdent,
      defaultTargetTableIdentifier,
      leafTargetSchema,
      Row(1, "target-left", "target-right", cdcMetadataRow(
        deleteSeq = None, upsertSeq = Some(20L), versionMap = targetVersionMap)),
      Row(2, "target-left", "target-right", cdcMetadataRow(
        deleteSeq = None, upsertSeq = Some(20L), versionMap = targetVersionMap)),
      Row(3, "target-left", "target-right", cdcMetadataRow(
        deleteSeq = None, upsertSeq = Some(20L), versionMap = targetVersionMap)),
      Row(4, "target-left", "target-right", cdcMetadataRow(
        deleteSeq = None, upsertSeq = Some(30L), versionMap = tiedVersionMap))
    )
    // Only `right` is selected for ignore-null, so `left` is a whole column. These net upserts at
    // 30 come from a microbatch that omits `left`, so only a delete or tombstone authors it: none
    // for key 1, a tombstone tied with the target's authorship for key 2, a newer delete for key 3,
    // and for key 4 a tombstone at the net upsert's own sequence, tied with the target's
    // authorship.
    val changeArgs = leafChangeArgs.copy(ignoreNullSelection =
      Some(ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("right")))))
    val microbatch = microbatchOf(leafTargetSchema)(
      Row(1, null, "new-right", cdcMetadataRow(
        deleteSeq = None, upsertSeq = Some(30L), versionMap = versionMap(Seq("right") -> 30L))),
      Row(2, null, "new-right", cdcMetadataRow(
        deleteSeq = None,
        upsertSeq = Some(30L),
        versionMap = versionMap(Seq("left") -> 20L, Seq("right") -> 30L))),
      Row(3, null, "new-right", cdcMetadataRow(
        deleteSeq = None,
        upsertSeq = Some(30L),
        versionMap = versionMap(Seq("left") -> 25L, Seq("right") -> 30L))),
      Row(4, null, null, cdcMetadataRow(
        deleteSeq = None, upsertSeq = Some(30L), versionMap = tiedVersionMap))
    )

    mergeMicrobatchOntoTargetPerLeaf(microbatch, changeArgs)

    checkAnswer(spark.read.table(defaultTargetTableIdentifier.quotedString), Seq(
      Row(1, "target-left", "new-right",
        Row(null, 30L, versionMap(Seq("left") -> 20L, Seq("right") -> 30L))),
      Row(2, "target-left", "new-right",
        Row(null, 30L, versionMap(Seq("left") -> 20L, Seq("right") -> 30L))),
      Row(3, null, "new-right",
        Row(null, 30L, versionMap(Seq("left") -> 25L, Seq("right") -> 30L))),
      Row(4, "target-left", "target-right", Row(null, 30L, tiedVersionMap))
    ))
  }

  test("per-leaf target merge deletes rows and inserts net upserts") {
    createTable(
      defaultTargetIdent,
      defaultTargetTableIdentifier,
      leafTargetSchema,
      Row(1, "remove", "row", cdcMetadataRow(
        deleteSeq = None,
        upsertSeq = Some(10L),
        versionMap = versionMap(Seq("left") -> 10L, Seq("right") -> 10L)))
    )
    val insertedVersionMap = versionMap(Seq("left") -> 30L, Seq("right") -> 30L)
    val microbatch = microbatchOf(leafTargetSchema)(
      Row(1, null, null, cdcMetadataRow(deleteSeq = Some(20L), upsertSeq = None)),
      Row(2, "inserted", "row", cdcMetadataRow(
        deleteSeq = None,
        upsertSeq = Some(30L),
        versionMap = insertedVersionMap)),
      Row(3, null, null, cdcMetadataRow(deleteSeq = Some(40L), upsertSeq = None))
    )

    mergeMicrobatchOntoTargetPerLeaf(microbatch)

    checkAnswer(
      spark.read.table(defaultTargetTableIdentifier.quotedString),
      Row(2, "inserted", "row", Row(null, 30L, insertedVersionMap)))
  }

  test("per-leaf target merge materializes a nullable struct when an incoming child wins") {
    val detailsType = new StructType().add("first", StringType).add("second", StringType)
    val nestedSchema = new StructType()
      .add("id", IntegerType)
      .add("details", detailsType)
      .add(AutoCdcReservedNames.cdcMetadataColName, cdcMetadataColSchemaType())
    createTable(
      defaultTargetIdent,
      defaultTargetTableIdentifier,
      nestedSchema,
      Row(1, null, cdcMetadataRow(
        deleteSeq = None,
        upsertSeq = Some(10L),
        versionMap = versionMap(
          Seq("details", "first") -> 10L,
          Seq("details", "second") -> 10L)))
    )
    val microbatch = microbatchOf(nestedSchema)(
      Row(1, Row("new-first", null), cdcMetadataRow(
        deleteSeq = None,
        upsertSeq = Some(20L),
        versionMap = versionMap(Seq("details", "first") -> 20L)))
    )

    mergeMicrobatchOntoTargetPerLeaf(microbatch)

    checkAnswer(
      spark.read.table(defaultTargetTableIdentifier.quotedString),
      Row(1, Row("new-first", null), Row(null, 20L, versionMap(
        Seq("details", "first") -> 20L,
        Seq("details", "second") -> 10L))))
  }

  test("per-leaf target merge rebuilds structs whose leaves a tombstone authored null") {
    val locationType = new StructType().add("city", StringType).add("zip", StringType)
    val profileType = new StructType().add("location", locationType).add("name", StringType)
    val nestedSchema = new StructType()
      .add("id", IntegerType)
      .add("profile", profileType)
      .add(AutoCdcReservedNames.cdcMetadataColName, cdcMetadataColSchemaType())
    val city = Seq("profile", "location", "city")
    val zip = Seq("profile", "location", "zip")
    val name = Seq("profile", "name")
    val targetVersionMap = versionMap(city -> 10L, zip -> 10L, name -> 10L)
    createTable(
      defaultTargetIdent,
      defaultTargetTableIdentifier,
      nestedSchema,
      Row(1, Row(Row("SF", "94107"), "Ada"), cdcMetadataRow(
        deleteSeq = None, upsertSeq = Some(10L), versionMap = targetVersionMap)),
      Row(2, Row(Row("SF", "94107"), "Ada"), cdcMetadataRow(
        deleteSeq = None, upsertSeq = Some(10L), versionMap = targetVersionMap))
    )
    // Net upserts at 30 whose leaves a tombstone at 20 authored null: the location leaves for key
    // 1, which leaves `name` to the target, and every leaf for key 2.
    val microbatch = microbatchOf(nestedSchema)(
      Row(1, null, cdcMetadataRow(
        deleteSeq = None, upsertSeq = Some(30L), versionMap = versionMap(city -> 20L, zip -> 20L))),
      Row(2, null, cdcMetadataRow(
        deleteSeq = None,
        upsertSeq = Some(30L),
        versionMap = versionMap(city -> 20L, zip -> 20L, name -> 20L)))
    )

    mergeMicrobatchOntoTargetPerLeaf(microbatch)

    checkAnswer(spark.read.table(defaultTargetTableIdentifier.quotedString), Seq(
      Row(1, Row(Row(null, null), "Ada"),
        Row(null, 30L, versionMap(city -> 20L, zip -> 20L, name -> 10L))),
      Row(2, Row(Row(null, null), null),
        Row(null, 30L, versionMap(city -> 20L, zip -> 20L, name -> 20L)))
    ))
  }

  test("per-leaf target merge keeps nulls authored before a column joins the selection") {
    val detailsType = new StructType().add("first", StringType).add("second", StringType)
    val nestedSchema = new StructType()
      .add("id", IntegerType)
      .add("details", detailsType)
      .add("value", StringType)
      .add(AutoCdcReservedNames.cdcMetadataColName, cdcMetadataColSchemaType())
    val first = Seq("details", "first")
    val second = Seq("details", "second")
    val value = Seq("value")
    // Written while only `value` was selected, so the upsert at 30 authored the null `details`
    // whole, while `value` kept its value from 10.
    createTable(
      defaultTargetIdent,
      defaultTargetTableIdentifier,
      nestedSchema,
      Row(1, null, "old-value", cdcMetadataRow(
        deleteSeq = None,
        upsertSeq = Some(30L),
        versionMap = versionMap(first -> 30L, second -> 30L, value -> 10L)))
    )
    // A late upsert at 20 once `details` is selected too.
    val microbatch = microbatchOf(nestedSchema)(
      Row(1, Row("late-first", null), "new-value", cdcMetadataRow(
        deleteSeq = None,
        upsertSeq = Some(20L),
        versionMap = versionMap(first -> 20L, value -> 20L)))
    )

    mergeMicrobatchOntoTargetPerLeaf(microbatch)

    checkAnswer(
      spark.read.table(defaultTargetTableIdentifier.quotedString),
      Row(1, Row(null, null), "new-value",
        Row(null, 30L, versionMap(first -> 30L, second -> 30L, value -> 20L))))
  }
}
