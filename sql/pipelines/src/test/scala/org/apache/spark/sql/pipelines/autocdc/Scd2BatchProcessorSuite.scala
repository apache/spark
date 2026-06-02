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

import org.apache.spark.sql.{functions => F, Column, QueryTest, Row}
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class Scd2BatchProcessorSuite extends QueryTest with SharedSparkSession {

  /** Build a microbatch [[DataFrame]] from explicit rows and an explicit schema. */
  private def microbatchOf(schema: StructType)(rows: Row*): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

  /**
   * Build an aux-table [[DataFrame]] from explicit user rows + framework column values.
   *
   * Each input [[Row]] carries the user columns followed by:
   *   - the row's `__START_AT` value
   *   - the row's `__END_AT` value (null for non-tombstone rows)
   *   - the row's `_cdc_metadata` struct as a [[Row]]
   *     (e.g., `Row(recordStartAt, deletedByBatchId)`)
   */
  private def auxTableOf(
      userSchema: StructType,
      sequencingType: DataType = LongType
  )(rows: Row*): DataFrame = {
    val schema = userSchema
      .add(Scd2BatchProcessor.startAtColName, sequencingType, nullable = true)
      .add(Scd2BatchProcessor.endAtColName, sequencingType, nullable = true)
      .add(
        AutoCdcReservedNames.cdcMetadataColName,
        Scd2BatchProcessor.auxCdcMetadataColSchema(sequencingType),
        nullable = false
      )
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  /**
   * Build a target-table [[DataFrame]] from explicit user rows + framework column values.
   *
   * Each input [[Row]] carries the user columns followed by:
   *   - the row's `__START_AT` value
   *   - the row's `__END_AT` value (null IFF the row is currently active)
   *   - the row's `_cdc_metadata` struct as a [[Row]] (e.g., `Row(recordStartAt)`)
   */
  private def targetTableOf(
      userSchema: StructType,
      sequencingType: DataType = LongType
  )(rows: Row*): DataFrame = {
    val schema = userSchema
      .add(Scd2BatchProcessor.startAtColName, sequencingType, nullable = true)
      .add(Scd2BatchProcessor.endAtColName, sequencingType, nullable = true)
      .add(
        AutoCdcReservedNames.cdcMetadataColName,
        Scd2BatchProcessor.targetCdcMetadataColSchema(sequencingType),
        nullable = false
      )
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  /**
   * Build a minimum-sequence-per-key [[DataFrame]] used by the `findAffected*` functions.
   *
   * Each input [[Row]] carries the key columns followed by the per-key minimum sequence.
   */
  private def minSeqOf(
      keySchema: StructType,
      sequencingType: DataType = LongType
  )(rows: Row*): DataFrame = {
    val schema = keySchema.add(
      Scd2BatchProcessor.minSequencingColName,
      sequencingType,
      nullable = false
    )
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  /**
   * Build a [[Scd2BatchProcessor]] suitable for `findAffected*` and
   * `computeMinimumSequencePerKey` tests. The `sequencing` is fixed to `F.col("seq")`,
   * so the input microbatch must include a `seq` column. `deleteCondition` is optional
   * and only needed by tests that exercise both event kinds.
   */
  private def processorWithKeys(
      keys: Seq[String],
      deleteCondition: Option[Column] = None
  ): Scd2BatchProcessor =
    Scd2BatchProcessor(
      changeArgs = ChangeArgs(
        keys = keys.map(UnqualifiedColumnName(_)),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type2,
        deleteCondition = deleteCondition
      ),
      resolvedSequencingType = LongType
    )

  /** Key-only schema for single-key `findAffected*` tests' minSeq dataframes. */
  private val singleKeyKeySchema: StructType = new StructType()
    .add("id", IntegerType)

  /** User schema for single-key `findAffected*` tests: the key column plus a `value` column. */
  private val singleKeyUserSchema: StructType = singleKeyKeySchema
    .add("value", StringType)

  // =============== preprocessMicrobatch tests ===============

  test("preprocessMicrobatch appends framework columns __START_AT, __END_AT, " +
    "_cdc_metadata at the end of the schema in that order") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("seq", LongType)
      .add("value", StringType)

    val batch = microbatchOf(schema)(Row(1, 10L, "a"))

    val processor = Scd2BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type2
      ),
      resolvedSequencingType = LongType
    )

    val result = processor.preprocessMicrobatch(batch)

    assert(result.schema.fieldNames.toSeq == Seq(
      "id", "seq", "value",
      Scd2BatchProcessor.startAtColName,
      Scd2BatchProcessor.endAtColName,
      AutoCdcReservedNames.cdcMetadataColName
    ))
  }

  test("preprocessMicrobatch returns an empty DataFrame with the full preprocessed schema") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("seq", LongType)
      .add("value", StringType)

    val batch = microbatchOf(schema)()

    val processor = Scd2BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type2
      ),
      resolvedSequencingType = LongType
    )

    val result = processor.preprocessMicrobatch(batch)

    assert(result.collect().isEmpty)
    assert(result.schema.fieldNames.toSeq == Seq(
      "id", "seq", "value",
      Scd2BatchProcessor.startAtColName,
      Scd2BatchProcessor.endAtColName,
      AutoCdcReservedNames.cdcMetadataColName
    ))
  }

  test("preprocessMicrobatch stamps __START_AT, __END_AT, and __RECORD_START_AT correctly " +
    "across delete and upsert events for the same key") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("seq", LongType)
      .add("value", StringType)
      .add("is_delete", BooleanType)

    // All three events target the same key. SCD2 must preserve every event in the output -
    // unlike SCD1, no per-key deduplication is performed; this also implicitly pins the
    // no-dedup contract of preprocessMicrobatch.
    val batch = microbatchOf(schema)(
      Row(1, 10L, "first-upsert", false),
      Row(1, 20L, "second-upsert", false),
      Row(1, 30L, null, true)
    )

    val processor = Scd2BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type2,
        deleteCondition = Some(F.col("is_delete"))
      ),
      resolvedSequencingType = LongType
    )

    // Per-row contract for the framework columns:
    //   - __START_AT      = sequencing for every row (the active-from time)
    //   - __END_AT        = sequencing for delete rows; null for upserts (mutual exclusion)
    //   - __RECORD_START_AT = sequencing for every row, regardless of delete vs upsert
    //                        (lineage preserved into the merge step)
    checkAnswer(
      df = processor.preprocessMicrobatch(batch),
      expectedAnswer = Seq(
        Row(1, 10L, "first-upsert", false, 10L, null, Row(10L)),
        Row(1, 20L, "second-upsert", false, 20L, null, Row(20L)),
        Row(1, 30L, null, true, 30L, 30L, Row(30L))
      )
    )
  }

  test("preprocessMicrobatch preserves byte-identical full-event duplicates") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("seq", LongType)
      .add("value", StringType)
      .add("is_delete", BooleanType)

    // Two byte-identical events for the same key: same key, same sequencing, same data, same
    // delete flag. SCD2 preprocessing intentionally preserves every event verbatim, including
    // full-event duplicates. Cross-event redundancy elimination (collapsing duplicates before
    // they could reconcile to a zero-width visible row) is the responsibility of downstream
    // reconciliation, not preprocessing.
    val batch = microbatchOf(schema)(
      Row(1, 10L, "alice", false),
      Row(1, 10L, "alice", false)
    )

    val processor = Scd2BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type2,
        deleteCondition = Some(F.col("is_delete"))
      ),
      resolvedSequencingType = LongType
    )

    // Both rows must survive verbatim.
    checkAnswer(
      df = processor.preprocessMicrobatch(batch),
      expectedAnswer = Seq(
        Row(1, 10L, "alice", false, 10L, null, Row(10L)),
        Row(1, 10L, "alice", false, 10L, null, Row(10L))
      )
    )
  }

  test("preprocessMicrobatch leaves __END_AT null on every row when deleteCondition is None") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("seq", LongType)
      .add("value", StringType)

    val batch = microbatchOf(schema)(
      Row(1, 10L, "a"),
      Row(2, 20L, "b")
    )

    val processor = Scd2BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type2,
        deleteCondition = None
      ),
      resolvedSequencingType = LongType
    )

    checkAnswer(
      df = processor.preprocessMicrobatch(batch).select(
        F.col(Scd2BatchProcessor.endAtColName)
      ),
      expectedAnswer = Seq(Row(null), Row(null))
    )
  }

  test("preprocessMicrobatch treats null deleteCondition results as upsert " +
    "(__END_AT stays null)") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("seq", LongType)
      .add("is_delete", BooleanType)

    val batch = microbatchOf(schema)(
      // is_delete is null - the delete condition evaluates to null, which Spark treats as the
      // otherwise branch, so the row is classified as an upsert.
      Row(1, 10L, null)
    )

    val processor = Scd2BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type2,
        deleteCondition = Some(F.col("is_delete"))
      ),
      resolvedSequencingType = LongType
    )

    checkAnswer(
      df = processor.preprocessMicrobatch(batch).select(
        F.col(Scd2BatchProcessor.endAtColName)
      ),
      expectedAnswer = Row(null)
    )
  }

  test("preprocessMicrobatch evaluates an arbitrary sequencing expression per-row") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("seq", LongType)
      .add("alt_seq", LongType)
      .add("value", StringType)

    // Sequencing is a function call referencing multiple columns, not a bare identifier. Locks
    // in that the framework columns evaluate the full expression per-row rather than treating
    // `sequencing` as a single column reference.
    val batch = microbatchOf(schema)(
      // greatest(10, 30) = 30
      Row(1, 10L, 30L, "row1"),
      // greatest(40, 20) = 40
      Row(2, 40L, 20L, "row2")
    )

    val processor = Scd2BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.greatest(F.col("seq"), F.col("alt_seq")),
        storedAsScdType = ScdType.Type2
      ),
      resolvedSequencingType = LongType
    )

    val result = processor.preprocessMicrobatch(batch)

    checkAnswer(
      df = result.select(
        F.col(Scd2BatchProcessor.startAtColName),
        F.col(s"${AutoCdcReservedNames.cdcMetadataColName}." +
          s"${Scd2BatchProcessor.recordStartAtFieldName}")
      ),
      expectedAnswer = Seq(
        Row(30L, 30L),
        Row(40L, 40L)
      )
    )
  }

  /** Schema reused by columnSelection tests: id (key), name, age, seq (sequencing). */
  private val multiUserColSchema: StructType = new StructType()
    .add("id", IntegerType)
    .add("name", StringType)
    .add("age", IntegerType)
    .add("seq", LongType)

  test("preprocessMicrobatch keeps every user column when columnSelection is None") {
    val batch = microbatchOf(multiUserColSchema)(
      Row(1, "alice", 30, 10L)
    )

    val processor = Scd2BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type2,
        columnSelection = None
      ),
      resolvedSequencingType = LongType
    )

    val result = processor.preprocessMicrobatch(batch)

    assert(result.schema.fieldNames.toSeq == Seq(
      "id", "name", "age", "seq",
      Scd2BatchProcessor.startAtColName,
      Scd2BatchProcessor.endAtColName,
      AutoCdcReservedNames.cdcMetadataColName
    ))
  }

  test("preprocessMicrobatch retains framework columns even when IncludeColumns omits them") {
    val batch = microbatchOf(multiUserColSchema)(
      Row(1, "alice", 30, 10L)
    )

    val processor = Scd2BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type2,
        columnSelection = Some(ColumnSelection.IncludeColumns(
          Seq(UnqualifiedColumnName("id"), UnqualifiedColumnName("age"))
        ))
      ),
      resolvedSequencingType = LongType
    )

    val result = processor.preprocessMicrobatch(batch)

    assert(result.schema.fieldNames.toSeq == Seq(
      "id", "age",
      Scd2BatchProcessor.startAtColName,
      Scd2BatchProcessor.endAtColName,
      AutoCdcReservedNames.cdcMetadataColName
    ))
    checkAnswer(
      df = result,
      expectedAnswer = Row(1, 30, 10L, null, Row(10L))
    )
  }

  test("preprocessMicrobatch drops user columns listed in ExcludeColumns; " +
    "framework columns survive") {
    val batch = microbatchOf(multiUserColSchema)(
      Row(1, "alice", 30, 10L)
    )

    val processor = Scd2BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type2,
        columnSelection = Some(ColumnSelection.ExcludeColumns(
          Seq(UnqualifiedColumnName("name"))
        ))
      ),
      resolvedSequencingType = LongType
    )

    val result = processor.preprocessMicrobatch(batch)

    assert(result.schema.fieldNames.toSeq == Seq(
      "id", "age", "seq",
      Scd2BatchProcessor.startAtColName,
      Scd2BatchProcessor.endAtColName,
      AutoCdcReservedNames.cdcMetadataColName
    ))
    checkAnswer(
      df = result,
      expectedAnswer = Row(1, 30, 10L, 10L, null, Row(10L))
    )
  }

  test("preprocessMicrobatch preserves the microbatch schema's user-column order, " +
    "ignoring the order of IncludeColumns") {
    val batch = microbatchOf(multiUserColSchema)(
      Row(1, "alice", 30, 10L)
    )

    val processor = Scd2BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type2,
        // User specifies (age, id) - intentionally different from the schema order (id, age).
        columnSelection = Some(ColumnSelection.IncludeColumns(
          Seq(UnqualifiedColumnName("age"), UnqualifiedColumnName("id"))
        ))
      ),
      resolvedSequencingType = LongType
    )

    val result = processor.preprocessMicrobatch(batch)

    // Output column order follows the microbatch schema (id before age), not the user's listing
    // order in IncludeColumns. Framework columns are always appended last.
    assert(result.schema.fieldNames.toSeq == Seq(
      "id", "age",
      Scd2BatchProcessor.startAtColName,
      Scd2BatchProcessor.endAtColName,
      AutoCdcReservedNames.cdcMetadataColName
    ))
  }

  test("preprocessMicrobatch resolves columnSelection case-insensitively " +
    "when SQLConf.CASE_SENSITIVE=false") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val batch = microbatchOf(multiUserColSchema)(
        Row(1, "alice", 30, 10L)
      )

      val processor = Scd2BatchProcessor(
        changeArgs = ChangeArgs(
          keys = Seq(UnqualifiedColumnName("id")),
          sequencing = F.col("seq"),
          storedAsScdType = ScdType.Type2,
          // User columns intentionally use a different case than the schema (id, age).
          columnSelection = Some(ColumnSelection.IncludeColumns(
            Seq(UnqualifiedColumnName("ID"), UnqualifiedColumnName("AGE"))
          ))
        ),
        resolvedSequencingType = LongType
      )

      val result = processor.preprocessMicrobatch(batch)

      // Output column names follow the microbatch schema's casing, not the user's casing.
      assert(result.schema.fieldNames.toSeq == Seq(
        "id", "age",
        Scd2BatchProcessor.startAtColName,
        Scd2BatchProcessor.endAtColName,
        AutoCdcReservedNames.cdcMetadataColName
      ))
    }
  }

  test("preprocessMicrobatch handles backticked column names containing a literal dot") {
    val schema = new StructType()
      .add("id", IntegerType)
      // Even if a column is created with backticks via DDL, those backticks are consumed by Spark
      // before resolving the schema; they won't show up in the schema field.
      .add("user.id", StringType)
      .add("seq", LongType)

    val batch = microbatchOf(schema)(
      Row(1, "u-100", 10L)
    )

    val processor = Scd2BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type2,
        columnSelection = Some(ColumnSelection.IncludeColumns(
          Seq(
            UnqualifiedColumnName("id"),
            UnqualifiedColumnName("`user.id`")
          )
        ))
      ),
      resolvedSequencingType = LongType
    )

    val result = processor.preprocessMicrobatch(batch)

    assert(result.schema.fieldNames.toSeq == Seq(
      "id", "user.id",
      Scd2BatchProcessor.startAtColName,
      Scd2BatchProcessor.endAtColName,
      AutoCdcReservedNames.cdcMetadataColName
    ))
    checkAnswer(
      df = result,
      expectedAnswer = Row(1, "u-100", 10L, null, Row(10L))
    )
  }

  test("preprocessMicrobatch correctly populates framework columns even when ExcludeColumns " +
    "drops the columns referenced by sequencing and deleteCondition") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      // Both seq and is_delete are referenced by the flow's sequencing / deleteCondition
      // expressions, but the user wants them excluded from the target table.
      .add("seq", LongType)
      .add("is_delete", BooleanType)

    val batch = microbatchOf(schema)(
      Row(1, "alice", 10L, false),
      Row(1, null, 20L, true)
    )

    val processor = Scd2BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type2,
        deleteCondition = Some(F.col("is_delete")),
        columnSelection = Some(ColumnSelection.ExcludeColumns(
          Seq(UnqualifiedColumnName("seq"), UnqualifiedColumnName("is_delete"))
        ))
      ),
      resolvedSequencingType = LongType
    )

    // The orchestrator runs row-extension steps before column selection, so the framework
    // columns reference seq / is_delete fully even though the final projection drops them.
    val result = processor.preprocessMicrobatch(batch)

    assert(result.schema.fieldNames.toSeq == Seq(
      "id", "value",
      Scd2BatchProcessor.startAtColName,
      Scd2BatchProcessor.endAtColName,
      AutoCdcReservedNames.cdcMetadataColName
    ))
    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row(1, "alice", 10L, null, Row(10L)),
        Row(1, null, 20L, 20L, Row(20L))
      )
    )
  }

  // =============== computeMinimumSequencePerKey tests ===============

  test("computeMinimumSequencePerKey returns one row per distinct key with the per-key " +
    "minimum __RECORD_START_AT, aggregating across both upsert and delete events") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("seq", LongType)
      .add("is_delete", BooleanType)

    val processor = processorWithKeys(
      keys = Seq("id"),
      deleteCondition = Some(F.col("is_delete"))
    )

    // Two keys, each with multiple events including at least one delete and at least one
    // out-of-order sequence. Delete events must feed into the per-key min exactly like
    // upserts: `preprocessMicrobatch` stamps `__RECORD_START_AT = sequencing` on every
    // row regardless of kind, so the min computation cannot legitimately ignore deletes.
    // (If it did, the early-delete-bisects-late-upsert reconciliation case would silently
    // lose its anchor pull-in via the find* paths.)
    val raw = microbatchOf(schema)(
      Row(1, 30L, false),  // out-of-order: appears before lower sequences in the input
      Row(1, 10L, true),   // delete - smallest sequence for key=1
      Row(1, 20L, false),
      Row(2, 50L, false),
      Row(2, 40L, true)    // delete - smallest sequence for key=2
    )

    val preprocessed = processor.preprocessMicrobatch(raw)
    val result = processor.computeMinimumSequencePerKey(preprocessed)

    assert(result.schema.fieldNames.toSeq == Seq(
      "id", Scd2BatchProcessor.minSequencingColName
    ))
    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row(1, 10L),
        Row(2, 40L)
      )
    )
  }

  test("computeMinimumSequencePerKey computes the minimum per composite-key tuple, " +
    "not per single key column") {
    val schema = new StructType()
      .add("region", StringType)
      .add("customer_id", IntegerType)
      .add("seq", LongType)

    val processor = processorWithKeys(keys = Seq("region", "customer_id"))

    // Three composite-key tuples that share their first or second key column. If the
    // function mistakenly grouped by `region` alone, (US, 1) and (US, 2) would collapse
    // and we'd see only two output rows; if it grouped by `customer_id` alone,
    // (US, 1) and (EU, 1) would collapse.
    val raw = microbatchOf(schema)(
      Row("US", 1, 100L),
      Row("US", 1,  50L),  // smaller sequence for (US, 1)
      Row("US", 2, 200L),
      Row("EU", 1,  30L)
    )

    val preprocessed = processor.preprocessMicrobatch(raw)
    val result = processor.computeMinimumSequencePerKey(preprocessed)

    assert(result.schema.fieldNames.toSeq == Seq(
      "region", "customer_id", Scd2BatchProcessor.minSequencingColName
    ))
    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row("US", 1,  50L),
        Row("US", 2, 200L),
        Row("EU", 1,  30L)
      )
    )
  }

  test("computeMinimumSequencePerKey returns an empty result for an empty preprocessed " +
    "microbatch") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("seq", LongType)

    val processor = processorWithKeys(keys = Seq("id"))

    val raw = microbatchOf(schema)()
    val preprocessed = processor.preprocessMicrobatch(raw)
    val result = processor.computeMinimumSequencePerKey(preprocessed)

    assert(result.collect().isEmpty)
  }

  test("computeMinimumSequencePerKey resolves key columns containing a literal dot") {
    // Symmetric to the dotted-name test for findAffectedRowsFromAuxiliaryTable: the
    // `groupBy(keysQuoted.map(F.col): _*)` site relies on `keysQuoted` correctly
    // backtick-quoting "a.b" so that F.col parses it as a literal column name (rather
    // than struct-field access). Pins the F.col axis of the keysQuoted vs keysRaw split.
    val schema = new StructType()
      .add("a.b", IntegerType)
      .add("seq", LongType)

    val processor = processorWithKeys(keys = Seq("`a.b`"))

    val raw = microbatchOf(schema)(
      Row(1, 30L),
      Row(1, 10L)
    )
    val preprocessed = processor.preprocessMicrobatch(raw)
    val result = processor.computeMinimumSequencePerKey(preprocessed)

    assert(result.schema.fieldNames.toSeq == Seq(
      "a.b", Scd2BatchProcessor.minSequencingColName
    ))
    checkAnswer(
      df = result,
      expectedAnswer = Seq(Row(1, 10L))
    )
  }

  // =============== findAffectedRowsFromAuxiliaryTable tests ===============

  test("findAffectedRowsFromAuxiliaryTable selects per-key anchor (max recordStartAt " +
    "strictly less than minSeq), preserves rows at or after minSeq, and drops older " +
    "anchor candidates") {
    val processor = processorWithKeys(Seq("id"))

    // Two keys to demonstrate per-key anchor isolation.
    //
    // Input row shape per `auxTableOf`:
    //   (id, value, __START_AT, __END_AT, Row(recordStartAt, deletedByBatchId))
    //
    // Key 1: aux rows at recordStartAt 3, 5, 10. minSeq = 10.
    //   - 3  -> older than the anchor; dropped.
    //   - 5  -> anchor (max < 10); included.
    //   - 10 -> at minSeq; included via the >= branch (NOT as anchor; selection is strict <).
    // Key 2: only one aux row at 7, minSeq = 7.
    //   - 7  -> at minSeq; included via >= branch. No anchor (no rows < 7 for this key).
    val aux = auxTableOf(singleKeyUserSchema)(
      Row(1, "v1.3",  3L,  null, Row(3L,  null)),
      Row(1, "v1.5",  5L,  null, Row(5L,  null)),
      Row(1, "v1.10", 10L, null, Row(10L, null)),
      Row(2, "v2.7",  7L,  null, Row(7L,  null))
    )
    val minSeq = minSeqOf(singleKeyKeySchema)(
      Row(1, 10L),
      Row(2, 7L)
    )

    val result = processor.findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf = aux,
      minimumSequencePerKeyInMicrobatchDf = minSeq,
      batchId = 100L
    )

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row(1, "v1.5",  5L,  null, Row(5L)),    // anchor for key=1
        Row(1, "v1.10", 10L, null, Row(10L)),   // >= minSeq for key=1
        Row(2, "v2.7",  7L,  null, Row(7L))     // >= minSeq for key=2 (no anchor)
      )
    )
  }

  test("findAffectedRowsFromAuxiliaryTable selects the anchor regardless of row kind " +
    "(tombstone vs no-op upsert continuation) and preserves both kinds in the affected set") {
    val processor = processorWithKeys(Seq("id"))

    // Aux carries a mix of row kinds for one key. The find function does NOT distinguish
    // between them - it filters purely on `recordStartAt` - so a tombstone, a no-op upsert
    // run head, and a continuation are all eligible anchor candidates and all eligible for
    // the >= minSeq inclusion branch.
    val aux = auxTableOf(singleKeyUserSchema)(
      // Tombstone at recordStartAt = 3 (deleted at sequence 3): startAt = endAt = 3.
      // Older than the anchor; dropped.
      Row(1, null,    3L, 3L,   Row(3L,  null)),
      // No-op upsert continuation at recordStartAt = 7: startAt inherits its run head's
      // recordStartAt, endAt is null. Anchor for minSeq=10 (max < 10).
      Row(1, "alice", 5L, null, Row(7L,  null)),
      // Tombstone at recordStartAt = 12: at-or-after minSeq, included via >= branch.
      Row(1, null,    12L, 12L, Row(12L, null)),
      // No-op upsert continuation at recordStartAt = 15: included via >= branch.
      Row(1, "bob",   13L, null, Row(15L, null))
    )
    val minSeq = minSeqOf(singleKeyKeySchema)(Row(1, 10L))

    val result = processor.findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf = aux,
      minimumSequencePerKeyInMicrobatchDf = minSeq,
      batchId = 100L
    )

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row(1, "alice", 5L,  null, Row(7L)),
        Row(1, null,    12L, 12L,  Row(12L)),
        Row(1, "bob",   13L, null, Row(15L))
      )
    )
  }

  test("findAffectedRowsFromAuxiliaryTable pulls in both no-op upsert continuations " +
    "when the microbatch's minSeq strictly bisects them (so reconciliation can promote " +
    "them to visible target rows if the bisecting event makes them no-longer no-ops)") {
    val processor = processorWithKeys(Seq("id"))

    // Three no-op upsert continuations of a single run for key=1 (run head was at
    // sequence 2 - that head lives in the target table, not the aux). Each continuation
    // has startAt=2 (the head's recordStartAt) and endAt=null, and they all carry the
    // same user data ("alice"), which is what made them no-ops when observed.
    //
    // The incoming microbatch's minSeq for key=1 is 10, which strictly bisects the
    // continuations at recordStartAt=8 and recordStartAt=12. Both must surface in the
    // affected set so a later reconciliation stage can decide whether to promote them
    // to visible target rows - e.g., if the bisecting event carries different user data,
    // the previously-no-op continuations become honest history with non-trivial
    // [startAt, endAt] boundaries.
    val aux = auxTableOf(singleKeyUserSchema)(
      Row(1, "alice", 2L, null, Row(3L,  null)),  // older continuation; dropped (< anchor)
      Row(1, "alice", 2L, null, Row(8L,  null)),  // anchor: max recordStartAt strictly < 10
      Row(1, "alice", 2L, null, Row(12L, null))   // included via >= branch (>= minSeq=10)
    )
    val minSeq = minSeqOf(singleKeyKeySchema)(Row(1, 10L))

    val result = processor.findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf = aux,
      minimumSequencePerKeyInMicrobatchDf = minSeq,
      batchId = 100L
    )

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row(1, "alice", 2L, null, Row(8L)),    // anchor - left context for the run
        Row(1, "alice", 2L, null, Row(12L))    // bisected by microbatch event at seq 10
      )
    )
  }

  test("findAffectedRowsFromAuxiliaryTable pulls in tombstones on both sides of minSeq, " +
    "including a tombstone-as-anchor and multiple tombstones at-or-after minSeq") {
    val processor = processorWithKeys(Seq("id"))

    // Four tombstones for key=1 at recordStartAt = 3, 7, 12, 18 (delete events).
    // Tombstones obey: startAt = endAt = recordStartAt and carry no user data.
    //
    // The microbatch's minSeq for key=1 is 10. Expected:
    //   - Tombstone at 3:  older than the anchor; dropped.
    //   - Tombstone at 7:  anchor (max recordStartAt strictly < 10); included.
    //   - Tombstone at 12: at-or-after minSeq; included via the >= branch.
    //   - Tombstone at 18: at-or-after minSeq; included via the >= branch.
    //
    // Why both sides matter for downstream reconciliation: the left-side tombstone tells
    // reconciliation the prior run was already closed by a delete (so an incoming upsert
    // starts a fresh run, not a continuation), while the right-side tombstones bound the
    // visible interval of any new upsert run against subsequent deletes.
    val aux = auxTableOf(singleKeyUserSchema)(
      Row(1, null, 3L,  3L,  Row(3L,  null)),
      Row(1, null, 7L,  7L,  Row(7L,  null)),
      Row(1, null, 12L, 12L, Row(12L, null)),
      Row(1, null, 18L, 18L, Row(18L, null))
    )
    val minSeq = minSeqOf(singleKeyKeySchema)(Row(1, 10L))

    val result = processor.findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf = aux,
      minimumSequencePerKeyInMicrobatchDf = minSeq,
      batchId = 100L
    )

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row(1, null, 7L,  7L,  Row(7L)),    // anchor - last delete before minSeq
        Row(1, null, 12L, 12L, Row(12L)),   // delete at seq 12
        Row(1, null, 18L, 18L, Row(18L))    // delete at seq 18
      )
    )
  }

  test("findAffectedRowsFromAuxiliaryTable filters logically-deleted aux rows by " +
    "__DELETED_BY_BATCH_ID: keeps null and current-batch, drops different-batch") {
    val processor = processorWithKeys(Seq("id"))

    val currentBatchId = 100L
    val differentBatchId = 99L

    // All three rows would be eligible by recordStartAt alone (>= minSeq=10), but the
    // idempotency filter drops the one logically deleted by a different batch.
    val aux = auxTableOf(singleKeyUserSchema)(
      Row(1, "live",    10L, null, Row(10L, null)),             // not deleted -> kept
      Row(1, "retried", 11L, null, Row(11L, currentBatchId)),   // deleted by current -> kept
      Row(1, "ignored", 12L, null, Row(12L, differentBatchId))  // deleted by another -> dropped
    )
    val minSeq = minSeqOf(singleKeyKeySchema)(Row(1, 10L))

    val result = processor.findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf = aux,
      minimumSequencePerKeyInMicrobatchDf = minSeq,
      batchId = currentBatchId
    )

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row(1, "live",    10L, null, Row(10L)),
        Row(1, "retried", 11L, null, Row(11L))
      )
    )
  }

  test("findAffectedRowsFromAuxiliaryTable still selects the anchor when that anchor row " +
    "was logically deleted by the current batch (idempotent retry)") {
    val processor = processorWithKeys(Seq("id"))

    // Models an idempotent retry: a previous attempt of this same batch already logically
    // deleted the anchor row in the aux table, but mid-retry we still need to treat it as
    // the left context. The idempotency filter retains rows deleted by `currentBatchId`,
    // and anchor selection ignores `__DELETED_BY_BATCH_ID` entirely.
    //
    // Strict-sequence-uniqueness is not a public guarantee of AutoCDC, but this test pins
    // the *current* behavior so the contract change is intentional rather than accidental.
    val currentBatchId = 100L
    val aux = auxTableOf(singleKeyUserSchema)(
      Row(1, "anchor", 5L, null, Row(5L, currentBatchId))
    )
    val minSeq = minSeqOf(singleKeyKeySchema)(Row(1, 10L))

    val result = processor.findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf = aux,
      minimumSequencePerKeyInMicrobatchDf = minSeq,
      batchId = currentBatchId
    )

    checkAnswer(
      df = result,
      expectedAnswer = Seq(Row(1, "anchor", 5L, null, Row(5L)))
    )
  }

  test("findAffectedRowsFromAuxiliaryTable narrows the CDC metadata column from the aux " +
    "schema to the target schema (drops __DELETED_BY_BATCH_ID)") {
    val processor = processorWithKeys(Seq("id"))

    // Pre-condition: aux's `_cdc_metadata` carries __RECORD_START_AT and __DELETED_BY_BATCH_ID.
    // The find function must strip the aux-only field so the result is union-compatible
    // with target-table rows and preprocessed-microbatch rows downstream.
    val aux = auxTableOf(singleKeyUserSchema)(Row(1, "v", 5L, null, Row(5L, null)))
    val minSeq = minSeqOf(singleKeyKeySchema)(Row(1, 10L))

    val result = processor.findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf = aux,
      minimumSequencePerKeyInMicrobatchDf = minSeq,
      batchId = 100L
    )

    val cdcMetadataField = result.schema(AutoCdcReservedNames.cdcMetadataColName)
    assert(cdcMetadataField.dataType == Scd2BatchProcessor.targetCdcMetadataColSchema(LongType))
  }

  test("findAffectedRowsFromAuxiliaryTable resolves key columns containing a literal dot") {
    // Backticks tell the SQL parser that "a.b" is a single identifier literally named
    // "a.b" (rather than struct-field access). The schema field, the join key (via
    // `keysRaw`), and the F.col reference (via `keysQuoted`) must all consistently
    // resolve to the literal "a.b" column for the find-* path to work end-to-end.
    //
    // This is the regression guard for the `keysRaw` vs `keysQuoted` split: passing
    // backtick-quoted names to `.join(_, usingColumns)` matches schema fields literally
    // and would fail to find a column named `a.b`, while passing raw names to F.col
    // would parse the dot as struct-field access and also fail.
    val processor = processorWithKeys(Seq("`a.b`"))
    val keySchema = new StructType().add("a.b", IntegerType)
    val userSchema = keySchema.add("value", StringType)

    val aux = auxTableOf(userSchema)(Row(1, "v", 5L, null, Row(5L, null)))
    val minSeq = minSeqOf(keySchema)(Row(1, 10L))

    val result = processor.findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf = aux,
      minimumSequencePerKeyInMicrobatchDf = minSeq,
      batchId = 100L
    )

    // The lone aux row is the anchor (recordStartAt=5 < minSeq=10, no other candidates).
    checkAnswer(
      df = result,
      expectedAnswer = Seq(Row(1, "v", 5L, null, Row(5L)))
    )
  }

  test("findAffectedRowsFromAuxiliaryTable respects all key columns when computing " +
    "per-key anchors with a composite key") {
    val keySchema = new StructType()
      .add("region", StringType)
      .add("customer_id", IntegerType)
    val userSchema = keySchema.add("name", StringType)

    val processor = processorWithKeys(Seq("region", "customer_id"))

    // Three composite keys: (US, 1), (EU, 1), (US, 2). Each is independent.
    //   (US, 1): anchor at 3; row at 10 included via >=.
    //   (EU, 1): anchor at 4; no rows at or after 12 -> only the anchor.
    //   (US, 2): no aux rows -> contributes nothing.
    val aux = auxTableOf(userSchema)(
      Row("US", 1, "us1.3",  3L,  null, Row(3L,  null)),
      Row("US", 1, "us1.10", 10L, null, Row(10L, null)),
      Row("EU", 1, "eu1.4",  4L,  null, Row(4L,  null))
    )
    val minSeq = minSeqOf(keySchema)(
      Row("US", 1, 10L),
      Row("EU", 1, 12L),
      Row("US", 2, 100L)
    )

    val result = processor.findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf = aux,
      minimumSequencePerKeyInMicrobatchDf = minSeq,
      batchId = 100L
    )

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row("US", 1, "us1.3",  3L,  null, Row(3L)),
        Row("US", 1, "us1.10", 10L, null, Row(10L)),
        Row("EU", 1, "eu1.4",  4L,  null, Row(4L))
      )
    )
  }

  test("findAffectedRowsFromAuxiliaryTable returns an empty result when the aux table is empty") {
    val processor = processorWithKeys(Seq("id"))

    val aux = auxTableOf(singleKeyUserSchema)()
    val minSeq = minSeqOf(singleKeyKeySchema)(Row(1, 10L))

    val result = processor.findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf = aux,
      minimumSequencePerKeyInMicrobatchDf = minSeq,
      batchId = 100L
    )

    assert(result.collect().isEmpty)
  }

  test("findAffectedRowsFromAuxiliaryTable returns no rows for a microbatch key that has " +
    "no rows in the aux table") {
    val processor = processorWithKeys(Seq("id"))

    // Aux only has rows for key=1. Microbatch only sees key=2.
    val aux = auxTableOf(singleKeyUserSchema)(Row(1, "v", 5L, null, Row(5L, null)))
    val minSeq = minSeqOf(singleKeyKeySchema)(Row(2, 10L))

    val result = processor.findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf = aux,
      minimumSequencePerKeyInMicrobatchDf = minSeq,
      batchId = 100L
    )

    assert(result.collect().isEmpty)
  }

  test("findAffectedRowsFromAuxiliaryTable excludes aux rows for keys not in the microbatch") {
    val processor = processorWithKeys(Seq("id"))

    // Aux has rows for keys 1 and 2. Microbatch only mentions key=1, so key=2's aux rows
    // must be dropped (the inner join with minSeq strips them).
    val aux = auxTableOf(singleKeyUserSchema)(
      Row(1, "v1", 5L, null, Row(5L, null)),
      Row(2, "v2", 7L, null, Row(7L, null))
    )
    val minSeq = minSeqOf(singleKeyKeySchema)(Row(1, 10L))

    val result = processor.findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf = aux,
      minimumSequencePerKeyInMicrobatchDf = minSeq,
      batchId = 100L
    )

    checkAnswer(
      df = result,
      expectedAnswer = Seq(Row(1, "v1", 5L, null, Row(5L)))
    )
  }

  // =============== findAffectedRowsFromTargetTable tests ===============

  test("findAffectedRowsFromTargetTable includes the currently active row and any closed " +
    "row whose __END_AT is at or after minSeq; older closed rows are excluded") {
    val processor = processorWithKeys(Seq("id"))

    // Single key with four target rows. Schema for input: (id, value, __START_AT, __END_AT,
    // _cdc_metadata{__RECORD_START_AT}).
    //   - closed at endAt=5  -> < minSeq=10 -> excluded
    //   - closed at endAt=10 -> = minSeq=10 -> included (>=)
    //   - closed at endAt=15 -> > minSeq=10 -> included
    //   - active (endAt=null)              -> always included
    val target = targetTableOf(singleKeyUserSchema)(
      Row(1, "old",    1L,  5L,   Row(1L)),
      Row(1, "edge",   5L,  10L,  Row(5L)),
      Row(1, "recent", 10L, 15L,  Row(10L)),
      Row(1, "active", 15L, null, Row(15L))
    )
    val minSeq = minSeqOf(singleKeyKeySchema)(Row(1, 10L))

    val result = processor.findAffectedRowsFromTargetTable(
      targetTableDf = target,
      minimumSequencePerKeyInMicrobatchDf = minSeq
    )

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row(1, "edge",   5L,  10L,  Row(5L)),
        Row(1, "recent", 10L, 15L,  Row(10L)),
        Row(1, "active", 15L, null, Row(15L))
      )
    )
  }

  test("findAffectedRowsFromTargetTable computes inclusion independently per key") {
    val processor = processorWithKeys(Seq("id"))

    // Two keys with overlapping endAt ranges but different per-key minSeqs. Each key is
    // reconciled independently against its own minSeq.
    val target = targetTableOf(singleKeyUserSchema)(
      // Key 1: minSeq=10. "active" (null) and "recent" (15) are at/after 10.
      Row(1, "k1.old",    1L,  5L,   Row(1L)),
      Row(1, "k1.recent", 5L,  15L,  Row(5L)),
      Row(1, "k1.active", 15L, null, Row(15L)),
      // Key 2: minSeq=20. Only "active" (null) is at/after 20.
      Row(2, "k2.old",    1L,  10L,  Row(1L)),
      Row(2, "k2.recent", 10L, 18L,  Row(10L)),
      Row(2, "k2.active", 18L, null, Row(18L))
    )
    val minSeq = minSeqOf(singleKeyKeySchema)(
      Row(1, 10L),
      Row(2, 20L)
    )

    val result = processor.findAffectedRowsFromTargetTable(
      targetTableDf = target,
      minimumSequencePerKeyInMicrobatchDf = minSeq
    )

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row(1, "k1.recent", 5L,  15L,  Row(5L)),
        Row(1, "k1.active", 15L, null, Row(15L)),
        Row(2, "k2.active", 18L, null, Row(18L))
      )
    )
  }

  test("findAffectedRowsFromTargetTable respects all key columns with a composite key") {
    val keySchema = new StructType()
      .add("region", StringType)
      .add("customer_id", IntegerType)
    val userSchema = keySchema.add("name", StringType)

    val processor = processorWithKeys(Seq("region", "customer_id"))

    // (US, 1) and (EU, 1) are distinct composite keys. (US, 1)'s active row is included
    // for minSeq=10; (EU, 1)'s active row is included for minSeq=12; (EU, 1)'s old closed
    // row at endAt=5 is excluded (5 < 12). (US, 2) has no target rows.
    val target = targetTableOf(userSchema)(
      Row("US", 1, "us1",     1L, null, Row(1L)),
      Row("EU", 1, "eu1.old", 1L, 5L,   Row(1L)),
      Row("EU", 1, "eu1",     5L, null, Row(5L))
    )
    val minSeq = minSeqOf(keySchema)(
      Row("US", 1, 10L),
      Row("EU", 1, 12L),
      Row("US", 2, 100L)
    )

    val result = processor.findAffectedRowsFromTargetTable(
      targetTableDf = target,
      minimumSequencePerKeyInMicrobatchDf = minSeq
    )

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row("US", 1, "us1", 1L, null, Row(1L)),
        Row("EU", 1, "eu1", 5L, null, Row(5L))
      )
    )
  }

  test("findAffectedRowsFromTargetTable returns an empty result when the target table is empty") {
    val processor = processorWithKeys(Seq("id"))

    val target = targetTableOf(singleKeyUserSchema)()
    val minSeq = minSeqOf(singleKeyKeySchema)(Row(1, 10L))

    val result = processor.findAffectedRowsFromTargetTable(
      targetTableDf = target,
      minimumSequencePerKeyInMicrobatchDf = minSeq
    )

    assert(result.collect().isEmpty)
  }

  test("findAffectedRowsFromTargetTable returns no rows for a microbatch key that has " +
    "no rows in the target table") {
    val processor = processorWithKeys(Seq("id"))

    // Target only has rows for key=1. Microbatch only sees key=2.
    val target = targetTableOf(singleKeyUserSchema)(Row(1, "v", 1L, null, Row(1L)))
    val minSeq = minSeqOf(singleKeyKeySchema)(Row(2, 10L))

    val result = processor.findAffectedRowsFromTargetTable(
      targetTableDf = target,
      minimumSequencePerKeyInMicrobatchDf = minSeq
    )

    assert(result.collect().isEmpty)
  }
}
