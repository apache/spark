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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.{functions => F, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryRowLevelOperationTableCatalog, TableInfo}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Tests for [[Scd1BatchProcessor]] methods that perform a `MERGE INTO` against a registered
 * v2 table. These tests require a v2 catalog that supports row-level operations
 * ([[InMemoryRowLevelOperationTableCatalog]]) and run actual writes through Catalyst's
 * row-level-operations machinery, so they are kept separate from the pure-DataFrame-transform
 * tests in [[Scd1BatchProcessorSuite]].
 */
class Scd1BatchProcessorMergeSuite
    extends QueryTest with SharedSparkSession with BeforeAndAfter {

  private val auxCatalogName = "cat"
  private val auxNamespace = "ns1"
  private val auxTableName = "aux_table"

  /** v2 [[Identifier]] used for direct catalog API calls (CREATE TABLE). */
  private val auxIdent = Identifier.of(Array(auxNamespace), auxTableName)

  /** Three-part [[TableIdentifier]] passed to the function under test. */
  private val auxTableIdentifier = TableIdentifier(
    table = auxTableName,
    database = Some(auxNamespace),
    catalog = Some(auxCatalogName)
  )

  /**
   * Minimal valid shape for both the auxiliary table and microbatch inputs in these tests:
   * a single key column `id` plus the CDC metadata struct. The auxiliary table genuinely
   * has only this shape in production, and the merge function reduces its microbatch input
   * down to keys + `_cdc_metadata` regardless of incoming data columns -- so most tests can
   * use this single schema for both ends.
   */
  private val minimalSchema: StructType = new StructType()
    .add("id", IntegerType)
    .add(
      Scd1BatchProcessor.cdcMetadataColName,
      new StructType()
        .add(Scd1BatchProcessor.cdcDeleteSequenceFieldName, LongType)
        .add(Scd1BatchProcessor.cdcUpsertSequenceFieldName, LongType)
    )

  /**
   * A processor with a single key column `id`. `sequencing` is irrelevant for
   * `mergeMicrobatchOntoAuxiliaryTable`: that function operates entirely on the already-
   * computed CDC metadata column, never on the raw sequencing expression.
   */
  private val processor = Scd1BatchProcessor(
    changeArgs = ChangeArgs(
      keys = Seq(UnqualifiedColumnName("id")),
      sequencing = F.lit(0L),
      storedAsScdType = ScdType.Type1
    ),
    resolvedSequencingType = LongType
  )

  before {
    spark.conf.set(
      s"spark.sql.catalog.$auxCatalogName",
      classOf[InMemoryRowLevelOperationTableCatalog].getName
    )
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.unsetConf(s"spark.sql.catalog.$auxCatalogName")
  }

  /**
   * Build an auxiliary-table schema with the given key columns followed by the standard CDC
   * metadata struct. Used by tests that need a non-trivial key shape (composite or dotted).
   */
  private def customKeyAuxSchema(keyColumns: Seq[(String, DataType)]): StructType = {
    val withKeys = keyColumns.foldLeft(new StructType()) { case (s, (name, dt)) =>
      s.add(name, dt)
    }
    withKeys.add(
      Scd1BatchProcessor.cdcMetadataColName,
      new StructType()
        .add(Scd1BatchProcessor.cdcDeleteSequenceFieldName, LongType)
        .add(Scd1BatchProcessor.cdcUpsertSequenceFieldName, LongType)
    )
  }

  /**
   * Create an auxiliary table in the test catalog using `schema` and seed it with `seedRows`.
   * The table is unpartitioned and supports row-level operations. Pass no rows to create an
   * empty table.
   */
  private def createAuxTableWithSchema(schema: StructType, seedRows: Row*): Unit = {
    val tableInfo = new TableInfo.Builder()
      .withSchema(schema)
      .build()
    spark.sessionState.catalogManager
      .catalog(auxCatalogName)
      .asTableCatalog
      .createTable(auxIdent, tableInfo)

    if (seedRows.nonEmpty) {
      val df = spark.createDataFrame(spark.sparkContext.parallelize(seedRows), schema)
      df.writeTo(auxTableIdentifier.quotedString).append()
    }
  }

  /** Create an auxiliary table using [[minimalSchema]] (single `id` key). */
  private def createAuxTable(seedRows: Row*): Unit =
    createAuxTableWithSchema(minimalSchema, seedRows: _*)

  /** Read the current contents of the auxiliary table. */
  private def readAuxTable(): DataFrame = spark.read.table(auxTableIdentifier.quotedString)

  /** Build a microbatch [[DataFrame]] from explicit `rows` and an explicit `schema`. */
  private def microbatchOf(schema: StructType)(rows: Row*): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

  /**
   * Build a row matching the [[Scd1BatchProcessor.cdcMetadataColName]] struct's two fields, in
   * the order produced by [[Scd1BatchProcessor.constructCdcMetadataCol]]:
   * `(deleteSequence, upsertSequence)`. Pass `None` for the side that doesn't apply.
   */
  private def cdcMetadataRow(deleteSeq: Option[Long], upsertSeq: Option[Long]): Row =
    Row(deleteSeq.getOrElse(null), upsertSeq.getOrElse(null))

  /**
   * `(name, dataType)` pairs of `schema`'s fields, used to compare two schemas for structural
   * equivalence while deliberately ignoring nullability and metadata.
   */
  private def columnNamesAndDataTypes(schema: StructType): Seq[(String, DataType)] =
    schema.fields.map(f => (f.name, f.dataType)).toSeq

  // =============== mergeMicrobatchOntoAuxiliaryTable tests ===============

  test("mergeMicrobatchOntoAuxiliaryTable replaces an existing tombstone with a newer " +
    "microbatch tombstone, dropping any microbatch-only data columns") {
    createAuxTable(Row(1, cdcMetadataRow(deleteSeq = Some(10), upsertSeq = None)))

    // The microbatch carries an extra `value` data column that has no place in the auxiliary
    // table. mergeMicrobatchOntoAuxiliaryTable must project it away before merging, both to
    // satisfy MergeIntoTable's schema requirements and to keep the auxiliary table free of
    // unrelated columns.
    val microbatchSchema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add(
        Scd1BatchProcessor.cdcMetadataColName,
        new StructType()
          .add(Scd1BatchProcessor.cdcDeleteSequenceFieldName, LongType)
          .add(Scd1BatchProcessor.cdcUpsertSequenceFieldName, LongType)
      )
    val microbatch = microbatchOf(microbatchSchema)(
      Row(1, "data-leak", cdcMetadataRow(deleteSeq = Some(20), upsertSeq = None))
    )

    processor.mergeMicrobatchOntoAuxiliaryTable(microbatch, auxTableIdentifier)

    val result = readAuxTable()
    // Tombstone advanced to delete=20, with exactly one row per key (no duplicate tombstones).
    checkAnswer(result, Row(1, Row(20L, null)))
    // Schema strictly matches minimalSchema; the `value` column was dropped, not smuggled in.
    assert(columnNamesAndDataTypes(result.schema) == columnNamesAndDataTypes(minimalSchema))
  }

  test("mergeMicrobatchOntoAuxiliaryTable deletes an existing tombstone when superseded by a " +
    "newer microbatch upsert") {
    createAuxTable(Row(1, cdcMetadataRow(deleteSeq = Some(10), upsertSeq = None)))

    val microbatch = microbatchOf(minimalSchema)(
      Row(1, cdcMetadataRow(deleteSeq = None, upsertSeq = Some(20)))
    )

    processor.mergeMicrobatchOntoAuxiliaryTable(microbatch, auxTableIdentifier)

    assert(readAuxTable().collect().isEmpty)
  }

  test("mergeMicrobatchOntoAuxiliaryTable inserts a new tombstone for a previously-untracked " +
    "key") {
    createAuxTable()

    val microbatch = microbatchOf(minimalSchema)(
      Row(1, cdcMetadataRow(deleteSeq = Some(10), upsertSeq = None))
    )

    processor.mergeMicrobatchOntoAuxiliaryTable(microbatch, auxTableIdentifier)

    checkAnswer(readAuxTable(), Row(1, Row(10L, null)))
  }

  test("mergeMicrobatchOntoAuxiliaryTable leaves rows for unrelated keys untouched") {
    createAuxTable(Row(1, cdcMetadataRow(deleteSeq = Some(10), upsertSeq = None)))

    // Microbatch event affects a different key entirely; the existing tombstone for id=1 must
    // not be touched even though the new tombstone's sequence is much larger.
    val microbatch = microbatchOf(minimalSchema)(
      Row(2, cdcMetadataRow(deleteSeq = Some(100), upsertSeq = None))
    )

    processor.mergeMicrobatchOntoAuxiliaryTable(microbatch, auxTableIdentifier)

    checkAnswer(readAuxTable(), Seq(
      Row(1, Row(10L, null)),
      Row(2, Row(100L, null))
    ))
  }

  test("mergeMicrobatchOntoAuxiliaryTable ignores microbatch deletes whose sequence is older " +
    "than the existing tombstone") {
    // This documents that mergeMicrobatchOntoAuxiliaryTable's contract is stronger than just
    // relying on applyTombstonesToMicrobatch having filtered out stale events upstream: even
    // an unfiltered stale incoming delete must not regress the high-water mark.
    createAuxTable(Row(1, cdcMetadataRow(deleteSeq = Some(10), upsertSeq = None)))

    val microbatch = microbatchOf(minimalSchema)(
      Row(1, cdcMetadataRow(deleteSeq = Some(5), upsertSeq = None))
    )

    processor.mergeMicrobatchOntoAuxiliaryTable(microbatch, auxTableIdentifier)

    checkAnswer(readAuxTable(), Row(1, Row(10L, null)))
  }

  test("mergeMicrobatchOntoAuxiliaryTable ignores microbatch upserts whose sequence is older " +
    "than the existing tombstone") {
    createAuxTable(Row(1, cdcMetadataRow(deleteSeq = Some(10), upsertSeq = None)))

    val microbatch = microbatchOf(minimalSchema)(
      Row(1, cdcMetadataRow(deleteSeq = None, upsertSeq = Some(5)))
    )

    processor.mergeMicrobatchOntoAuxiliaryTable(microbatch, auxTableIdentifier)

    checkAnswer(readAuxTable(), Row(1, Row(10L, null)))
  }

  test("mergeMicrobatchOntoAuxiliaryTable applies the tied-sequence asymmetry: equal deletes " +
    "are kept, equal upserts delete the tombstone") {
    // On a delete<->upsert sequencing tie, upsert events are given priority over deletes;
    // therefore an incoming upsert with the same sequence as a tombstone should delete the
    // tombstone. On a delete<->delete sequencing tie, the effect is a no-op. This is an
    // internal SCD1 tie-breaking convention, not a publicly documented contract.
    createAuxTable(
      Row(1, cdcMetadataRow(deleteSeq = Some(10), upsertSeq = None)),
      Row(2, cdcMetadataRow(deleteSeq = Some(20), upsertSeq = None))
    )

    val microbatch = microbatchOf(minimalSchema)(
      Row(1, cdcMetadataRow(deleteSeq = Some(10), upsertSeq = None)),
      Row(2, cdcMetadataRow(deleteSeq = None, upsertSeq = Some(20)))
    )

    processor.mergeMicrobatchOntoAuxiliaryTable(microbatch, auxTableIdentifier)

    // Row 1's tombstone remains the same, but row 2's tombstone should be marked as stale and
    // deleted.
    checkAnswer(readAuxTable(), Row(1, Row(10L, null)))
  }

  test("mergeMicrobatchOntoAuxiliaryTable upsert event for different key does not affect " +
    "tombstone") {
    createAuxTable(Row(2, cdcMetadataRow(deleteSeq = Some(5), upsertSeq = None)))

    val microbatch = microbatchOf(minimalSchema)(
      // Although the upsert seq is 10, this is for key=1; tombstone for key=2 should be unaffected.
      Row(1, cdcMetadataRow(deleteSeq = None, upsertSeq = Some(10)))
    )

    processor.mergeMicrobatchOntoAuxiliaryTable(microbatch, auxTableIdentifier)

    checkAnswer(readAuxTable(), Row(2, Row(5L, null)))
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
      Row(1, cdcMetadataRow(deleteSeq = Some(10), upsertSeq = None)),
      Row(2, cdcMetadataRow(deleteSeq = Some(20), upsertSeq = None)),
      Row(3, cdcMetadataRow(deleteSeq = Some(30), upsertSeq = None))
    )

    val microbatch = microbatchOf(minimalSchema)(
      Row(1, cdcMetadataRow(deleteSeq = None, upsertSeq = Some(15))),
      Row(2, cdcMetadataRow(deleteSeq = Some(25), upsertSeq = None)),
      Row(4, cdcMetadataRow(deleteSeq = Some(40), upsertSeq = None))
    )

    val expectedAfterMerge = Seq(
      Row(2, Row(25L, null)),
      Row(3, Row(30L, null)),
      Row(4, Row(40L, null))
    )

    // First merge applies all three clauses exactly once.
    processor.mergeMicrobatchOntoAuxiliaryTable(microbatch, auxTableIdentifier)
    checkAnswer(readAuxTable(), expectedAfterMerge)

    // Re-applying the same microbatch is a no-op:
    //   - id=1 is absent from aux; whenNotMatched is gated on delete events => skipped.
    //   - id=2 has tied delete (incoming==aux); strict `>` in the update clause fails.
    //   - id=4 has tied delete (incoming==aux); same reason.
    processor.mergeMicrobatchOntoAuxiliaryTable(microbatch, auxTableIdentifier)
    checkAnswer(readAuxTable(), expectedAfterMerge)
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
      Row("US", 99, cdcMetadataRow(deleteSeq = Some(50), upsertSeq = None))
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
      Row("US", 1, cdcMetadataRow(deleteSeq = Some(10), upsertSeq = None))
    )

    compositeKeyProcessor.mergeMicrobatchOntoAuxiliaryTable(microbatch, auxTableIdentifier)

    checkAnswer(readAuxTable(), Seq(
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
      Row(1, cdcMetadataRow(deleteSeq = Some(10), upsertSeq = None))
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
      Row(1, cdcMetadataRow(deleteSeq = Some(20), upsertSeq = None))
    )

    dottedKeyProcessor.mergeMicrobatchOntoAuxiliaryTable(microbatch, auxTableIdentifier)

    checkAnswer(readAuxTable(), Row(1, Row(20L, null)))
  }
}
