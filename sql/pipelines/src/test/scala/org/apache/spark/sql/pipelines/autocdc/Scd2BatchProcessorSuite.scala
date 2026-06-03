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
      Scd2BatchProcessor.minSequenceColName,
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

  // =============== orderChronologicallyPerKeyWindow tests ===============

  test("orderChronologicallyPerKeyWindow sorts both decomposition tails and non-tails by " +
    "effective recordStartAt ascending") {
    val processor = processorWithKeys(Seq("id"))
    val userSchema = new StructType().add("id", IntegerType).add("value", StringType)

    // Three rows at distinct effective recordStartAts. The decomposition tail
    // (recordStartAt = null, endAt = 10) takes its endAt as its effective ordering sequence,
    // so the expected per-key window order is 5, tail(10), 15.
    val df = targetTableOf(userSchema)(
      Row(1, "v15",  15L,  null, Row(15L)),
      Row(1, "tail", null, 10L,  Row(null)),
      Row(1, "v5",   5L,   null, Row(5L))
    )

    val withRn = df.withColumn(
      "rn", F.row_number().over(processor.orderChronologicallyPerKeyWindow)
    )

    checkAnswer(
      df = withRn,
      expectedAnswer = Seq(
        Row(1, "v5",   5L,   null, Row(5L),    1),
        Row(1, "tail", null, 10L,  Row(null),  2),
        Row(1, "v15",  15L,  null, Row(15L),   3)
      )
    )
  }

  test("orderChronologicallyPerKeyWindow places tails before non-tails, then open intervals " +
    "before closed intervals, when effective recordStartAt ties") {
    val processor = processorWithKeys(Seq("id"))
    val userSchema = new StructType().add("id", IntegerType).add("value", StringType)

    // Three rows all at effective recordStartAt = 10:
    //   - decomposition tail: recordStartAt = null, endAt = 10
    //   - open upsert:        recordStartAt = 10,   endAt = null
    //   - tombstone:          recordStartAt = 10,   endAt = 10
    // Expected order:
    //   1. tail (tail-first tiebreaker)
    //   2. open upsert (open-before-closed tiebreaker among non-tails)
    //   3. tombstone
    val df = targetTableOf(userSchema)(
      Row(1, "tomb", 10L,  10L,  Row(10L)),
      Row(1, "tail", null, 10L,  Row(null)),
      Row(1, "open", 10L,  null, Row(10L))
    )

    val withRn = df.withColumn(
      "rn", F.row_number().over(processor.orderChronologicallyPerKeyWindow)
    )

    checkAnswer(
      df = withRn,
      expectedAnswer = Seq(
        Row(1, "tail", null, 10L,  Row(null), 1),
        Row(1, "open", 10L,  null, Row(10L),  2),
        Row(1, "tomb", 10L,  10L,  Row(10L),  3)
      )
    )
  }

  test("orderChronologicallyPerKeyWindow orders rows independently per key") {
    val processor = processorWithKeys(Seq("id"))
    val userSchema = new StructType().add("id", IntegerType).add("value", StringType)

    // Two keys with interleaved input order. Each key's window must be sorted independently
    // by effective recordStartAt - rows from key 2 must not influence row_number positions
    // of rows for key 1, and vice versa.
    val df = targetTableOf(userSchema)(
      Row(1, "k1-15", 15L, null, Row(15L)),
      Row(2, "k2-7",  7L,  null, Row(7L)),
      Row(1, "k1-5",  5L,  null, Row(5L)),
      Row(2, "k2-3",  3L,  null, Row(3L)),
      Row(1, "k1-10", 10L, null, Row(10L)),
      Row(2, "k2-20", 20L, null, Row(20L))
    )

    val withRn = df.withColumn(
      "rn", F.row_number().over(processor.orderChronologicallyPerKeyWindow)
    )

    checkAnswer(
      df = withRn,
      expectedAnswer = Seq(
        Row(1, "k1-5",  5L,  null, Row(5L),  1),
        Row(1, "k1-10", 10L, null, Row(10L), 2),
        Row(1, "k1-15", 15L, null, Row(15L), 3),
        Row(2, "k2-3",  3L,  null, Row(3L),  1),
        Row(2, "k2-7",  7L,  null, Row(7L),  2),
        Row(2, "k2-20", 20L, null, Row(20L), 3)
      )
    )
  }

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

  test("computeMinimumSequencePerKey returns one row per distinct key and aggregates across " +
    "both upsert and delete events") {
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
      "id", Scd2BatchProcessor.minSequenceColName
    ))
    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row(1, 10L),
        Row(2, 40L)
      )
    )
  }

  test("computeMinimumSequencePerKey is compatible with composite keys") {
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
      "region", "customer_id", Scd2BatchProcessor.minSequenceColName
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

  test("computeMinimumSequencePerKey returns an empty result for an empty microbatch") {
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
      "a.b", Scd2BatchProcessor.minSequenceColName
    ))
    checkAnswer(
      df = result,
      expectedAnswer = Seq(Row(1, 10L))
    )
  }

  // =============== findAffectedRowsFromAuxiliaryTable tests ===============

  test("findAffectedRowsFromAuxiliaryTable returns the anchor row per key") {
    val processor = processorWithKeys(Seq("id"))
    val keySchema = new StructType().add("id", IntegerType)
    val userSchema = keySchema.add("value", StringType)

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
    val aux = auxTableOf(userSchema)(
      Row(1, "v1.3",  3L,  null, Row(3L,  null)),
      Row(1, "v1.5",  5L,  null, Row(5L,  null)),
      Row(1, "v1.10", 10L, null, Row(10L, null)),
      Row(2, "v2.7",  7L,  null, Row(7L,  null))
    )
    val minSeq = minSeqOf(keySchema)(
      Row(1, 10L),
      Row(2, 7L)
    )

    val result = processor.findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf = aux,
      perKeyMinimumSequenceInMicrobatch = minSeq,
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

  test("findAffectedRowsFromAuxiliaryTable pulls in both tombstone and no-op upsert rows") {
    val processor = processorWithKeys(Seq("id"))
    val keySchema = new StructType().add("id", IntegerType)
    val userSchema = keySchema.add("value", StringType)

    // Aux carries a mix of row kinds for one key. The find function does NOT distinguish
    // between them - it filters purely on `recordStartAt` - so a tombstone, a no-op upsert
    // run head, and a continuation are all eligible anchor candidates and all eligible for
    // the >= minSeq inclusion branch.
    val aux = auxTableOf(userSchema)(
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
    val minSeq = minSeqOf(keySchema)(Row(1, 10L))

    val result = processor.findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf = aux,
      perKeyMinimumSequenceInMicrobatch = minSeq,
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

  test("findAffectedRowsFromAuxiliaryTable pulls in both consecutive no-op upsert events " +
    "being interleaved by incoming microbatch row") {
    val processor = processorWithKeys(Seq("id"))
    val keySchema = new StructType().add("id", IntegerType)
    val userSchema = keySchema.add("value", StringType)

    val aux = auxTableOf(userSchema)(
      Row(1, "alice", 2L, null, Row(8L,  null)),
      Row(1, "alice", 2L, null, Row(12L, null))
    )
    val minSeq = minSeqOf(keySchema)(Row(1, 10L))

    val result = processor.findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf = aux,
      perKeyMinimumSequenceInMicrobatch = minSeq,
      batchId = 100L
    )

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        // Row with record start at of 8 gets pulled in as an anchor,
        Row(1, "alice", 2L, null, Row(8L)),
        // Row with record start at of 12 gets pulled in as a regular affected row.
        Row(1, "alice", 2L, null, Row(12L))
      )
    )
  }

  test("findAffectedRowsFromAuxiliaryTable selects tombstones as anchor if applicable") {
    val processor = processorWithKeys(Seq("id"))
    val keySchema = new StructType().add("id", IntegerType)
    val userSchema = keySchema.add("value", StringType)

    // Tombstone-as-anchor is incidental: the find function selects the anchor purely on
    // `max recordStartAt < minSeq`, so a tombstone qualifies just like any other row kind.
    // Downstream reconciliation does not actually rely on the anchor when it is a
    // tombstone (a delete already closed the prior run, so any subsequent incoming event
    // is necessarily a fresh run head regardless of whether the anchor is surfaced). We
    // still pull it in as a harmless side effect of the range filter, and this behavior is
    // documented via test.
    val aux = auxTableOf(userSchema)(
      Row(1, null, 7L,  7L,  Row(7L,  null)),
      Row(1, null, 12L, 12L, Row(12L, null))
    )
    val minSeq = minSeqOf(keySchema)(Row(1, 10L))

    val result = processor.findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf = aux,
      perKeyMinimumSequenceInMicrobatch = minSeq,
      batchId = 100L
    )

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        // Pulled in as anchor.
        Row(1, null, 7L,  7L,  Row(7L)),
        // Pulled in as regular affected row.
        Row(1, null, 12L, 12L, Row(12L))
      )
    )
  }

  test("findAffectedRowsFromAuxiliaryTable filters logically-deleted aux rows") {
    val processor = processorWithKeys(Seq("id"))
    val keySchema = new StructType().add("id", IntegerType)
    val userSchema = keySchema.add("value", StringType)

    val currentBatchId = 100L
    val differentBatchId = 99L

    // The idempotency filter retains rows deleted by `currentBatchId` (so a mid-flight
    // retry sees its own prior writes) and drops rows deleted by any other batch. This
    // applies uniformly to both the anchor and non-anchor affected rows.
    val aux = auxTableOf(userSchema)(
      // Anchor candidate (recordStartAt < minSeq):
      Row(1, "anchor",  5L,  null, Row(5L,  currentBatchId)),    // deleted by current -> kept
      // At-or-after minSeq:
      Row(1, "live",    10L, null, Row(10L, null)),              // not deleted -> kept
      Row(1, "retried", 11L, null, Row(11L, currentBatchId)),    // deleted by current -> kept
      Row(1, "ignored", 12L, null, Row(12L, differentBatchId))   // deleted by another -> dropped
    )
    val minSeq = minSeqOf(keySchema)(Row(1, 10L))

    val result = processor.findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf = aux,
      perKeyMinimumSequenceInMicrobatch = minSeq,
      batchId = currentBatchId
    )

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row(1, "anchor",  5L,  null, Row(5L)),
        Row(1, "live",    10L, null, Row(10L)),
        Row(1, "retried", 11L, null, Row(11L))
      )
    )
  }

  test("findAffectedRowsFromAuxiliaryTable narrows CDC metadata column to match target's") {
    val processor = processorWithKeys(Seq("id"))
    val keySchema = new StructType().add("id", IntegerType)
    val userSchema = keySchema.add("value", StringType)

    // Pre-condition: aux's `_cdc_metadata` carries __RECORD_START_AT and __DELETED_BY_BATCH_ID.
    // The find function must strip the aux-only field so the result is union-compatible
    // with target-table rows and preprocessed-microbatch rows downstream.
    val aux = auxTableOf(userSchema)(Row(1, "v", 5L, null, Row(5L, null)))
    val minSeq = minSeqOf(keySchema)(Row(1, 10L))

    val result = processor.findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf = aux,
      perKeyMinimumSequenceInMicrobatch = minSeq,
      batchId = 100L
    )

    val cdcMetadataField = result.schema(AutoCdcReservedNames.cdcMetadataColName)
    assert(cdcMetadataField.dataType == Scd2BatchProcessor.targetCdcMetadataColSchema(LongType))
  }

  test("findAffectedRowsFromAuxiliaryTable resolves key columns containing a literal dot") {
    val processor = processorWithKeys(Seq("`a.b`"))
    val keySchema = new StructType().add("a.b", IntegerType)
    val userSchema = keySchema.add("value", StringType)

    val aux = auxTableOf(userSchema)(Row(1, "v", 5L, null, Row(5L, null)))
    val minSeq = minSeqOf(keySchema)(Row(1, 10L))

    val result = processor.findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf = aux,
      perKeyMinimumSequenceInMicrobatch = minSeq,
      batchId = 100L
    )

    // The lone aux row is the anchor (recordStartAt=5 < minSeq=10, no other candidates).
    checkAnswer(
      df = result,
      expectedAnswer = Seq(Row(1, "v", 5L, null, Row(5L)))
    )
  }

  test("findAffectedRowsFromAuxiliaryTable respects composite keys") {
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
      perKeyMinimumSequenceInMicrobatch = minSeq,
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
    val keySchema = new StructType().add("id", IntegerType)
    val userSchema = keySchema.add("value", StringType)

    val aux = auxTableOf(userSchema)()
    val minSeq = minSeqOf(keySchema)(Row(1, 10L))

    val result = processor.findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf = aux,
      perKeyMinimumSequenceInMicrobatch = minSeq,
      batchId = 100L
    )

    assert(result.collect().isEmpty)
  }

  test("findAffectedRowsFromAuxiliaryTable returns no rows for a microbatch key that has " +
    "no rows in the aux table") {
    val processor = processorWithKeys(Seq("id"))
    val keySchema = new StructType().add("id", IntegerType)
    val userSchema = keySchema.add("value", StringType)

    // Aux only has rows for key=1. Microbatch only sees key=2.
    val aux = auxTableOf(userSchema)(Row(1, "v", 5L, null, Row(5L, null)))
    val minSeq = minSeqOf(keySchema)(Row(2, 10L))

    val result = processor.findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf = aux,
      perKeyMinimumSequenceInMicrobatch = minSeq,
      batchId = 100L
    )

    assert(result.collect().isEmpty)
  }

  test("findAffectedRowsFromAuxiliaryTable excludes aux rows for keys not in the microbatch") {
    val processor = processorWithKeys(Seq("id"))
    val keySchema = new StructType().add("id", IntegerType)
    val userSchema = keySchema.add("value", StringType)

    // Aux has rows for keys 1 and 2. Microbatch only mentions key=1, so key=2's aux rows
    // must be dropped (the inner join with minSeq strips them).
    val aux = auxTableOf(userSchema)(
      Row(1, "v1", 5L, null, Row(5L, null)),
      Row(2, "v2", 7L, null, Row(7L, null))
    )
    val minSeq = minSeqOf(keySchema)(Row(1, 10L))

    val result = processor.findAffectedRowsFromAuxiliaryTable(
      rawAuxiliaryTableDf = aux,
      perKeyMinimumSequenceInMicrobatch = minSeq,
      batchId = 100L
    )

    checkAnswer(
      df = result,
      expectedAnswer = Seq(Row(1, "v1", 5L, null, Row(5L)))
    )
  }

  // =============== findAffectedRowsFromTargetTable tests ===============

  test("findAffectedRowsFromTargetTable includes both closed and active affected rows") {
    val processor = processorWithKeys(Seq("id"))
    val keySchema = new StructType().add("id", IntegerType)
    val userSchema = keySchema.add("value", StringType)

    // Single key with four target rows:
    //   - row closed at endAt=5  -> < minSeq=10 -> excluded
    //   - row closed at endAt=10 -> = minSeq=10 -> included (>=)
    //   - row closed at endAt=15 -> > minSeq=10 -> included
    //   - row active (endAt=null)              -> always included
    val target = targetTableOf(userSchema)(
      Row(1, "old",    1L,  5L,   Row(1L)),
      Row(1, "edge",   5L,  10L,  Row(5L)),
      Row(1, "recent", 10L, 15L,  Row(10L)),
      Row(1, "active", 15L, null, Row(15L))
    )
    val minSeq = minSeqOf(keySchema)(Row(1, 10L))

    val result = processor.findAffectedRowsFromTargetTable(
      targetTableDf = target,
      perKeyMinimumSequenceInMicrobatch = minSeq
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
    val keySchema = new StructType().add("id", IntegerType)
    val userSchema = keySchema.add("value", StringType)

    // Two keys with overlapping endAt ranges but different per-key minSeqs. Each key is
    // reconciled independently against its own minSeq.
    val target = targetTableOf(userSchema)(
      // Key 1: minSeq=10. "active" (null) and "recent" (15) are at/after 10.
      Row(1, "k1.old",    1L,  5L,   Row(1L)),
      Row(1, "k1.recent", 5L,  15L,  Row(5L)),
      Row(1, "k1.active", 15L, null, Row(15L)),
      // Key 2: minSeq=20. Only "active" (null) is at/after 20.
      Row(2, "k2.old",    1L,  10L,  Row(1L)),
      Row(2, "k2.recent", 10L, 18L,  Row(10L)),
      Row(2, "k2.active", 18L, null, Row(18L))
    )
    val minSeq = minSeqOf(keySchema)(
      Row(1, 10L),
      Row(2, 20L)
    )

    val result = processor.findAffectedRowsFromTargetTable(
      targetTableDf = target,
      perKeyMinimumSequenceInMicrobatch = minSeq
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

  test("findAffectedRowsFromTargetTable respects composite keys") {
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
      perKeyMinimumSequenceInMicrobatch = minSeq
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
    val keySchema = new StructType().add("id", IntegerType)
    val userSchema = keySchema.add("value", StringType)

    val target = targetTableOf(userSchema)()
    val minSeq = minSeqOf(keySchema)(Row(1, 10L))

    val result = processor.findAffectedRowsFromTargetTable(
      targetTableDf = target,
      perKeyMinimumSequenceInMicrobatch = minSeq
    )

    assert(result.collect().isEmpty)
  }

  test("findAffectedRowsFromTargetTable returns no rows for a microbatch key that has " +
    "no rows in the target table") {
    val processor = processorWithKeys(Seq("id"))
    val keySchema = new StructType().add("id", IntegerType)
    val userSchema = keySchema.add("value", StringType)

    // Target only has rows for key=1. Microbatch only sees key=2.
    val target = targetTableOf(userSchema)(Row(1, "v", 1L, null, Row(1L)))
    val minSeq = minSeqOf(keySchema)(Row(2, 10L))

    val result = processor.findAffectedRowsFromTargetTable(
      targetTableDf = target,
      perKeyMinimumSequenceInMicrobatch = minSeq
    )

    assert(result.collect().isEmpty)
  }

  // =============== decomposeOutOfOrderRows tests ===============

  test("decomposeOutOfOrderRows passes through open rows, tombstones, and " +
    "last-in-partition closed rows") {
    val processor = processorWithKeys(Seq("id"))
    val userSchema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add("amount", IntegerType)

    // None of these rows is eligible for decomposition:
    //   - "open":  endAt is null, so it is not a closed row
    //   - "tomb":  startAt == endAt, so it is excluded by the strict `<` closed check
    //   - "last":  closed, but is the last row in its window partition (no successor)
    val df = targetTableOf(userSchema)(
      Row(1, "open", 100, 5L,  null, Row(5L)),
      Row(1, "tomb", 200, 10L, 10L,  Row(10L)),
      Row(1, "last", 300, 15L, 25L,  Row(15L))
    )

    val result = processor.decomposeOutOfOrderRows(df)

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row(1, "open", 100, 5L,  null, Row(5L)),
        Row(1, "tomb", 200, 10L, 10L,  Row(10L)),
        Row(1, "last", 300, 15L, 25L,  Row(15L))
      )
    )
  }

  test("decomposeOutOfOrderRows leaves a closed row alone when the next event arrives at " +
    "exactly its endAt") {
    val processor = processorWithKeys(Seq("id"))
    val userSchema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add("amount", IntegerType)

    // Closed [5, 10] would decompose if the successor's recordStartAt were strictly less
    // than 10. Here the successor lands at exactly 10, which means it doesn't actually
    // bisect the closed row and therefore shouldn't decompose it.
    val df = targetTableOf(userSchema)(
      Row(1, "alice", 42, 5L,  10L,  Row(5L)),
      Row(1, "bob",   99, 10L, null, Row(10L))
    )

    val result = processor.decomposeOutOfOrderRows(df)

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row(1, "alice", 42, 5L,  10L,  Row(5L)),
        Row(1, "bob",   99, 10L, null, Row(10L))
      )
    )
  }

  test("decomposeOutOfOrderRows splits a bisected row into head + tail, " +
    "both inheriting parent data") {
    val processor = processorWithKeys(Seq("id"))
    val userSchema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add("amount", IntegerType)

    // Closed [5, 30] is bisected by a successor at recordStartAt = 15 (15 < 30). Expected:
    //   - head: parent with endAt set to null
    //   - tail: parent with startAt set to null and recordStartAt (in cdcMetadata) set to null
    //   - successor: passes through
    // Both head and tail must carry the parent's data columns (value="alice", amount=42)
    // identically.
    val df = targetTableOf(userSchema)(
      Row(1, "alice", 42, 5L,  30L,  Row(5L)),
      Row(1, "bob",   99, 15L, null, Row(15L))
    )

    val result = processor.decomposeOutOfOrderRows(df)

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row(1, "alice", 42, 5L,   null, Row(5L)),    // head
        Row(1, "alice", 42, null, 30L,  Row(null)),  // tail
        Row(1, "bob",   99, 15L,  null, Row(15L))    // bisecting successor
      )
    )
  }

  test("decomposeOutOfOrderRows triggers on any kind of bisecting successor") {
    val processor = processorWithKeys(Seq("id"))
    val userSchema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add("amount", IntegerType)

    // Three independent keys, each with a closed parent [5, 50] bisected by a successor
    // of a different kind. All three parents must decompose, regardless of the successor's
    // own row kind (the bisection check looks only at recordStartAt < parent.endAt).
    val df = targetTableOf(userSchema)(
      // Key 1: bisected by an open upsert.
      Row(1, "alice", 1, 5L,  50L,  Row(5L)),
      Row(1, "bob",   2, 10L, null, Row(10L)),

      // Key 2: bisected by a tombstone.
      Row(2, "carol", 3, 5L,  50L, Row(5L)),
      Row(2, "dave",  4, 20L, 20L, Row(20L)),

      // Key 3: bisected by another closed non-tombstone.
      Row(3, "eve",   5, 5L,  50L, Row(5L)),
      Row(3, "frank", 6, 30L, 40L, Row(30L))
    )

    val result = processor.decomposeOutOfOrderRows(df)

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        // Key 1.
        Row(1, "alice", 1, 5L,   null, Row(5L)),
        Row(1, "alice", 1, null, 50L,  Row(null)),
        Row(1, "bob",   2, 10L,  null, Row(10L)),
        // Key 2.
        Row(2, "carol", 3, 5L,   null, Row(5L)),
        Row(2, "carol", 3, null, 50L,  Row(null)),
        Row(2, "dave",  4, 20L,  20L,  Row(20L)),
        // Key 3.
        Row(3, "eve",   5, 5L,   null, Row(5L)),
        Row(3, "eve",   5, null, 50L,  Row(null)),
        Row(3, "frank", 6, 30L,  40L,  Row(30L))
      )
    )
  }

  test("decomposeOutOfOrderRows uses chronological window order, not input order") {
    val processor = processorWithKeys(Seq("id"))
    val userSchema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add("amount", IntegerType)

    // Same data as the basic split test but with the input rows shuffled into a
    // non-chronological order. The window orders rows by effective recordStartAt, so the
    // result must still recognize that [5, 30] is bisected by the row at recordStartAt = 15.
    val df = targetTableOf(userSchema)(
      Row(1, "bob",   99, 15L, null, Row(15L)),  // appears first in input
      Row(1, "alice", 42, 5L,  30L,  Row(5L))    // appears last in input but lower in window
    )

    val result = processor.decomposeOutOfOrderRows(df)

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row(1, "alice", 42, 5L,   null, Row(5L)),
        Row(1, "alice", 42, null, 30L,  Row(null)),
        Row(1, "bob",   99, 15L,  null, Row(15L))
      )
    )
  }

  test("decomposeOutOfOrderRows isolates per-key partitions") {
    val processor = processorWithKeys(Seq("id"))
    val userSchema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add("amount", IntegerType)

    // Key 1 has a bisecting pair. Key 2 only contains a single closed row whose recordStartAt
    // happens to coincide with key 1's parent. The window partitions by key, so key 1's
    // bisecting successor must NOT bleed into key 2's partition.
    val df = targetTableOf(userSchema)(
      // Key 1: closed [5, 30] bisected by recordStartAt = 15.
      Row(1, "alice", 42, 5L,  30L,  Row(5L)),
      Row(1, "bob",   99, 15L, null, Row(15L)),

      // Key 2: a single closed [5, 30] with no successor in its own partition.
      Row(2, "carol", 7, 5L, 30L, Row(5L))
    )

    val result = processor.decomposeOutOfOrderRows(df)

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        // Key 1 decomposes.
        Row(1, "alice", 42, 5L,   null, Row(5L)),
        Row(1, "alice", 42, null, 30L,  Row(null)),
        Row(1, "bob",   99, 15L,  null, Row(15L)),
        // Key 2 passes through.
        Row(2, "carol", 7, 5L, 30L, Row(5L))
      )
    )
  }

  test("decomposeOutOfOrderRows handles a cascade of consecutive bisected rows") {
    val processor = processorWithKeys(Seq("id"))
    val userSchema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add("amount", IntegerType)

    // A nested cascade: each closed row (except the innermost) is bisected by the next.
    // Decomposition decisions are independent and based on each row's immediate successor:
    //   [5, 30]  bisected by [10, 25]   -> decomposes
    //   [10, 25] bisected by [15, 20]   -> decomposes
    //   [15, 20] is the last row        -> passes through
    val df = targetTableOf(userSchema)(
      Row(1, "outer",  1, 5L,  30L, Row(5L)),
      Row(1, "middle", 2, 10L, 25L, Row(10L)),
      Row(1, "inner",  3, 15L, 20L, Row(15L))
    )

    val result = processor.decomposeOutOfOrderRows(df)

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        // outer decomposes.
        Row(1, "outer",  1, 5L,   null, Row(5L)),
        Row(1, "outer",  1, null, 30L,  Row(null)),
        // middle decomposes.
        Row(1, "middle", 2, 10L,  null, Row(10L)),
        Row(1, "middle", 2, null, 25L,  Row(null)),
        // inner passes through.
        Row(1, "inner",  3, 15L,  20L,  Row(15L))
      )
    )
  }

  test("decomposeOutOfOrderRows returns empty for empty input") {
    val processor = processorWithKeys(Seq("id"))
    val userSchema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add("amount", IntegerType)

    val df = targetTableOf(userSchema)()

    val result = processor.decomposeOutOfOrderRows(df)

    assert(result.collect().isEmpty)
    assert(result.columns.toSeq == df.columns.toSeq)
  }

  test("decomposeOutOfOrderRows preserves per-column schema attributes through " +
    "head + tail decomposition") {
    val processor = processorWithKeys(Seq("id"))

    def commentMetadata(comment: String): Metadata =
      new MetadataBuilder().putString("comment", comment).build()
    
    val cdcMetadataInnerSchema = new StructType().add(
      Scd2BatchProcessor.recordStartAtFieldName,
      LongType,
      nullable = true,
      metadata = commentMetadata("inner __RECORD_START_AT")
    )
    
    val schema = new StructType()
      .add("id", IntegerType, nullable = false, metadata = commentMetadata("user key"))
      .add("value", StringType, nullable = true, metadata = commentMetadata("user data"))
      .add(
        Scd2BatchProcessor.startAtColName, LongType, nullable = true,
        metadata = commentMetadata("framework __START_AT"))
      .add(
        Scd2BatchProcessor.endAtColName, LongType, nullable = true,
        metadata = commentMetadata("framework __END_AT"))
      .add(
        AutoCdcReservedNames.cdcMetadataColName, cdcMetadataInnerSchema, nullable = false,
        metadata = commentMetadata("framework _cdc_metadata"))

    // Closed [5, 30] bisected by recordStartAt = 15.
    val df = microbatchOf(schema)(
      Row(1, "alice", 5L,  30L,  Row(5L)),
      Row(1, "bob",   15L, null, Row(15L))
    )

    val result = processor.decomposeOutOfOrderRows(df)

    // Output schema must match the input field-for-field on name, dataType (which carries
    // inner-struct field metadata), nullable, and outer metadata.
    schema.fields.zip(result.schema.fields).foreach { case (in, out) =>
      assert(in.name == out.name)
      assert(in.dataType == out.dataType)
      assert(in.nullable == out.nullable)
      assert(in.metadata == out.metadata)
    }
  }

  // =============== dropRedundantRowsPostDecomposition tests ===============

  test("dropRedundantRowsPostDecomposition passes through events with distinct sequences") {
    val processor = processorWithKeys(Seq("id"))
    val userSchema = new StructType().add("id", IntegerType).add("value", StringType)

    // Distinct effective recordStartAts within the dataframe, so no redundancies - identity
    // transformation expected.
    val df = targetTableOf(userSchema)(
      Row(1, "v5",  5L,  null, Row(5L)),
      Row(1, "v10", 10L, null, Row(10L)),
      Row(1, "v15", 15L, 20L,  Row(15L))
    )

    val result = processor.dropRedundantRowsPostDecomposition(df)

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row(1, "v5",  5L,  null, Row(5L)),
        Row(1, "v10", 10L, null, Row(10L)),
        Row(1, "v15", 15L, 20L,  Row(15L))
      )
    )
  }

  test("dropRedundantRowsPostDecomposition drops a same-event duplicate") {
    val processor = processorWithKeys(Seq("id"))
    val userSchema = new StructType().add("id", IntegerType).add("value", StringType)

    // Two open upserts with identical sequencing dimensions
    // (effectiveRecordStartAt=10, startAt=10, endAt=null). The chronologically-leading
    // copy is dropped by Rule 1; exactly one copy survives. We do not assert which user-data
    // variant survives because the window's tiebreaker among truly-identical rows is
    // intentionally undefined.
    val df = targetTableOf(userSchema)(
      Row(1, "first",  10L, null, Row(10L)),
      Row(1, "second", 10L, null, Row(10L))
    )

    val result = processor.dropRedundantRowsPostDecomposition(df)
    val survivors = result.collect()

    assert(survivors.length == 1)
    assert(Set("first", "second").contains(survivors.head.getString(1)))
  }

  test("dropRedundantRowsPostDecomposition drops an open upsert overtaken at the same instant") {
    val processor = processorWithKeys(Seq("id"))
    val userSchema = new StructType().add("id", IntegerType).add("value", StringType)

    // Open upsert at recordStartAt=10 followed by a tombstone at the same instant
    // (effectiveRecordStartAt=10). The upsert opens at an instant the tombstone already
    // overtakes, leaving it 0-width: Rule 2 drops it. The tombstone (last in partition)
    // survives.
    val df = targetTableOf(userSchema)(
      Row(1, "open", 10L, null, Row(10L)),
      Row(1, "tomb", 10L, 10L,  Row(10L))
    )

    val result = processor.dropRedundantRowsPostDecomposition(df)

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row(1, "tomb", 10L, 10L, Row(10L))
      )
    )
  }

  test("dropRedundantRowsPostDecomposition drops a decomposition tail coincident with " +
    "an incoming event") {
    val processor = processorWithKeys(Seq("id"))
    val userSchema = new StructType().add("id", IntegerType).add("value", StringType)

    // Decomposition tail closing at endAt=30 followed by a non-tail event at
    // recordStartAt=30. The synthetic delete the tail encodes is already represented by
    // the coincident event, so Rule 3 drops the tail. The event survives.
    val df = targetTableOf(userSchema)(
      Row(1, "tail",  null, 30L,  Row(null)),
      Row(1, "event", 30L,  null, Row(30L))
    )

    val result = processor.dropRedundantRowsPostDecomposition(df)

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row(1, "event", 30L, null, Row(30L))
      )
    )
  }

  test("dropRedundantRowsPostDecomposition preserves rows when the next event arrives at " +
    "a strictly later instant") {
    val processor = processorWithKeys(Seq("id"))
    val userSchema = new StructType().add("id", IntegerType).add("value", StringType)

    // Open upsert at recordStartAt=10 and decomposition tail closing at endAt=30 are each
    // followed by a row at a strictly later effective sequence. None of Rule 2 or Rule 3
    // fires (their equality conditions don't hold), and Rule 1 doesn't fire (sequencing
    // dimensions differ). Every row survives.
    val df = targetTableOf(userSchema)(
      Row(1, "open",  10L,  null, Row(10L)),
      Row(1, "next1", 15L,  null, Row(15L)),
      Row(1, "tail",  null, 30L,  Row(null)),
      Row(1, "next2", 35L,  null, Row(35L))
    )

    val result = processor.dropRedundantRowsPostDecomposition(df)

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row(1, "open",  10L,  null, Row(10L)),
        Row(1, "next1", 15L,  null, Row(15L)),
        Row(1, "tail",  null, 30L,  Row(null)),
        Row(1, "next2", 35L,  null, Row(35L))
      )
    )
  }

  test("dropRedundantRowsPostDecomposition preserves the last row in every per-key partition") {
    val processor = processorWithKeys(Seq("id"))
    val userSchema = new StructType().add("id", IntegerType).add("value", StringType)

    // Each key has a single row; LEAD(1) is null at the end of every partition and all
    // three rules are vacuous. If per-key isolation were broken, key 1's row would see
    // key 2's row as its successor and trigger Rule 1 (the rows are identical on
    // sequencing dimensions); proper partitioning preserves both.
    val df = targetTableOf(userSchema)(
      Row(1, "v10", 10L, null, Row(10L)),
      Row(2, "v10", 10L, null, Row(10L))
    )

    val result = processor.dropRedundantRowsPostDecomposition(df)

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row(1, "v10", 10L, null, Row(10L)),
        Row(2, "v10", 10L, null, Row(10L))
      )
    )
  }

  test("dropRedundantRowsPostDecomposition cascades across consecutive duplicate rows") {
    val processor = processorWithKeys(Seq("id"))
    val userSchema = new StructType().add("id", IntegerType).add("value", StringType)

    // Three identical open upserts followed by a distinct event. Each adjacent same-event
    // pair triggers Rule 1: the chronologically-leading two copies are dropped, leaving
    // exactly one duplicate plus the distinct event. We don't assert which user-data
    // variant of the duplicate survives.
    val df = targetTableOf(userSchema)(
      Row(1, "dup1",     5L,  null, Row(5L)),
      Row(1, "dup2",     5L,  null, Row(5L)),
      Row(1, "dup3",     5L,  null, Row(5L)),
      Row(1, "different", 10L, null, Row(10L))
    )

    val result = processor.dropRedundantRowsPostDecomposition(df)
    val survivors = result.collect()

    assert(survivors.length == 2)
    val dupSurvivor = survivors.find(_.getLong(2) == 5L)
    assert(dupSurvivor.isDefined)
    assert(Set("dup1", "dup2", "dup3").contains(dupSurvivor.get.getString(1)))
    assert(survivors.exists(r => r.getString(1) == "different"))
  }

  test("dropRedundantRowsPostDecomposition applies different rules to different rows " +
    "in one partition") {
    val processor = processorWithKeys(Seq("id"))
    val userSchema = new StructType().add("id", IntegerType).add("value", StringType)

    // All three rows tie at effectiveRecordStartAt=10. Window tiebreakers order them as:
    //   1. tail (tails-first)
    //   2. open upsert (open-before-closed among non-tails)
    //   3. tombstone
    // Then:
    //   - tail.endAt=10 == next.effective=10 -> Rule 3 drops the tail
    //   - open is an open upsert with effective=next.effective -> Rule 2 drops the upsert
    //   - tombstone is the last row in the partition -> survives
    val df = targetTableOf(userSchema)(
      Row(1, "tail", null, 10L,  Row(null)),
      Row(1, "open", 10L,  null, Row(10L)),
      Row(1, "tomb", 10L,  10L,  Row(10L))
    )

    val result = processor.dropRedundantRowsPostDecomposition(df)

    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row(1, "tomb", 10L, 10L, Row(10L))
      )
    )
  }
}
