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

import org.apache.spark.sql.{functions => F, AnalysisException, QueryTest, Row}
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class Scd1BatchProcessorSuite extends QueryTest with SharedSparkSession {

  /**
   * Test Schema for a microbatch that already has the SCD1 CDC metadata column projected.
   */
  private val microbatchWithCdcMetadataSchema: StructType = new StructType()
    .add("id", IntegerType)
    .add("name", StringType)
    .add("age", IntegerType)
    .add(
      AutoCdcReservedNames.cdcMetadataColName,
      new StructType()
        .add(Scd1BatchProcessor.cdcDeleteSequenceFieldName, LongType)
        .add(Scd1BatchProcessor.cdcUpsertSequenceFieldName, LongType)
    )

  /** DataType for the CDC metadata column, where sequencing type is Long. */
  private val cdcMetadataColSchemaType: DataType = new StructType()
    .add(Scd1BatchProcessor.cdcDeleteSequenceFieldName, LongType)
    .add(Scd1BatchProcessor.cdcUpsertSequenceFieldName, LongType)

  /**
   * Helper to construct a CDC metadata column row, following [[cdcMetadataColSchemaType]].
   */
  private def cdcMetadataRow(deleteSeq: Option[Long], upsertSeq: Option[Long]): Row =
    Row(deleteSeq.getOrElse(null), upsertSeq.getOrElse(null))


  /** Build a microbatch [[DataFrame]] from explicit rows and an explicit schema. */
  private def microbatchOf(schema: StructType)(rows: Row*): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

  /**
   * Returns the `(name, dataType)` pairs of `schema`'s fields. Used to compare two schemas for
   * structural equivalence while deliberately ignoring nullability and metadata, which can shift
   * benignly when columns are unpacked from a struct.
   */
  private def columnNamesAndDataTypes(schema: StructType): Seq[(String, DataType)] =
    schema.fields.map(f => (f.name, f.dataType)).toSeq

  // =============== deduplicateMicrobatch tests ===============

  test("deduplicateMicrobatch keeps only the row with the largest sequence value per key") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("seq", LongType)
      .add("value", StringType)

    val batch = microbatchOf(schema)(
      Row(1, 10L, "first"),
      Row(1, 30L, "winner"),
      Row(1, 20L, "middle")
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    checkAnswer(
      df = processor.deduplicateMicrobatch(batch),
      expectedAnswer = Row(1, 30L, "winner")
    )
  }

  test("deduplicateMicrobatch is no-op if there's a single event for a key") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("seq", LongType)
      .add("value", StringType)

    val batch = microbatchOf(schema)(
      Row(1, 10L, "only-row")
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    checkAnswer(
      df = processor.deduplicateMicrobatch(batch),
      expectedAnswer = Row(1, 10L, "only-row")
    )
  }

  test("deduplicateMicrobatch handles equal sequencing values for the same key") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("seq", LongType)
      .add("value", StringType)

    val batch = microbatchOf(schema)(
      Row(1, 10L, "first-tied-row"),
      Row(1, 10L, "second-tied-row")
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    // On equal sequence number events for the same key we provide no guarantee on which event will
    // survive, but the contract is _one_ event will survive - assert that below.
    val result = processor.deduplicateMicrobatch(batch).collect()
    assert(result.length == 1)
    assert(result.head.getInt(0) == 1)
    assert(result.head.getLong(1) == 10L)
    assert(Set("first-tied-row", "second-tied-row").contains(result.head.getString(2)))
  }

  test("deduplicateMicrobatch ignores rows with null sequencing when a non-null value exists") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("seq", LongType)
      .add("value", StringType)

    val batch = microbatchOf(schema)(
      // In production the expectation is the microbatch will have been validated to not contain
      // any null sequence values, but demonstrate that null sequence rows are de-prioritized in
      // deduplication.
      Row(1, null, "null-sequence"),
      Row(1, 10L, "non-null-sequence")
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    checkAnswer(
      df = processor.deduplicateMicrobatch(batch),
      expectedAnswer = Row(1, 10L, "non-null-sequence")
    )
  }

  test(
    "deduplicateMicrobatch returns a null row when all sequencing values for a key are null"
  ) {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("seq", LongType)
      .add("value", StringType)
    val batch = microbatchOf(schema)(
      // In production the expectation is the microbatch will have been validated to not contain
      // any null sequence values, but demonstrate that a null row will be returned by
      // deduplication if all rows contain a null sequence in the microbatch.
      Row(1, null, "null-sequence")
    )
    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )
    checkAnswer(
      df = processor.deduplicateMicrobatch(batch),
      expectedAnswer = Row(null, null, null)
    )
  }

  test("deduplicateMicrobatch processes multiple keys independently") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("seq", LongType)
      .add("value", StringType)

    val batch = microbatchOf(schema)(
      Row(1, 10L, "a1"),
      Row(2, 50L, "b1-winner"),
      Row(1, 20L, "a2-winner"),
      Row(2, 40L, "b2-loser"),
      Row(3, 1L, "c1-only")
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    checkAnswer(
      df = processor.deduplicateMicrobatch(batch),
      expectedAnswer = Seq(
        Row(1, 20L, "a2-winner"),
        Row(2, 50L, "b1-winner"),
        Row(3, 1L, "c1-only")
      )
    )
  }

  test("deduplicateMicrobatch carries non-key, non-sequence columns from the winning row") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("seq", LongType)
      .add("name", StringType)
      .add("amount", IntegerType)

    val batch = microbatchOf(schema)(
      Row(1, 10L, "old-name", 100),
      Row(1, 20L, "winning-name", 200)
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    // All non-key columns must come from the row with the largest sequence value, never
    // a mix of values from multiple rows.
    checkAnswer(
      df = processor.deduplicateMicrobatch(batch),
      expectedAnswer = Row(1, 20L, "winning-name", 200)
    )
  }

  test("deduplicateMicrobatch carries nested columns correctly from the winning row") {
    val payloadType = new StructType()
      .add("name", StringType)
      .add("amount", IntegerType)
    val schema = new StructType()
      .add("id", IntegerType)
      .add("seq", LongType)
      .add("payload", payloadType)

    val batch = microbatchOf(schema)(
      Row(1, 10L, Row("old", 100)),
      Row(1, 20L, Row("new", 200))
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    checkAnswer(
      df = processor.deduplicateMicrobatch(batch),
      expectedAnswer = Row(1, 20L, Row("new", 200))
    )
  }

  test("deduplicateMicrobatch supports composite (multi-column) keys") {
    val schema = new StructType()
      .add("region", StringType)
      .add("customer_id", IntegerType)
      .add("seq", LongType)
      .add("value", StringType)

    val batch = microbatchOf(schema)(
      Row("US", 1, 10L, "us1-old"),
      Row("US", 1, 20L, "us1-new"),
      // Same customer_id as above but different region: independent group.
      Row("EU", 1, 5L, "eu1-only"),
      // Same region as above but different customer_id: independent group.
      Row("US", 2, 99L, "us2-only")
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("region"), UnqualifiedColumnName("customer_id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    checkAnswer(
      df = processor.deduplicateMicrobatch(batch),
      expectedAnswer = Seq(
        Row("US", 1, 20L, "us1-new"),
        Row("EU", 1, 5L, "eu1-only"),
        Row("US", 2, 99L, "us2-only")
      )
    )
  }

  test("deduplicateMicrobatch supports an arbitrary sequencing expression") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("seq", LongType)
      .add("alt_seq", LongType)
      .add("value", StringType)

    // The sequencing expression is a function call referencing multiple columns, not a bare
    // identifier. Locks in that `max_by(..., changeArgs.sequencing)` evaluates the full
    // expression per-row rather than treating `sequencing` as a single column reference.
    val batch = microbatchOf(schema)(
      // greatest(10, 30) = 30 - winner under the expression.
      Row(1, 10L, 30L, "winner"),
      // greatest(25, 20) = 25 - would win under `seq` alone, but loses under `greatest`.
      Row(1, 25L, 20L, "would-win-on-seq-alone"),
      Row(1, 15L, 15L, "always-loses")
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.greatest(F.col("seq"), F.col("alt_seq")),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    checkAnswer(
      df = processor.deduplicateMicrobatch(batch),
      expectedAnswer = Row(1, 10L, 30L, "winner")
    )
  }

  test("deduplicateMicrobatch supports literal-dot column names") {
    val schema = new StructType()
      .add("user.id", IntegerType)
      .add("seq", LongType)
      .add("event.value", StringType)

    val batch = microbatchOf(schema)(
      Row(1, 10L, "old"),
      Row(1, 20L, "new")
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("`user.id`")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    checkAnswer(
      df = processor.deduplicateMicrobatch(batch),
      expectedAnswer = Row(1, 20L, "new")
    )
  }

  test(
    "deduplicateMicrobatch fails when a key column collides with the reserved name"
  ) {
    val reservedColName = Scd1BatchProcessor.winningRowColName

    val schema = new StructType()
      .add(reservedColName, StringType)
      .add("seq", LongType)
      .add("value", StringType)

    val batch = microbatchOf(schema)(
      Row("k1", 10L, "loser"),
      Row("k1", 20L, "winner")
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName(reservedColName)),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    checkError(
      exception = intercept[AnalysisException] {
        processor.deduplicateMicrobatch(batch).collect()
      },
      condition = "AMBIGUOUS_REFERENCE",
      sqlState = "42704",
      parameters = Map(
        "name" -> s"`$reservedColName`",
        "referenceNames" -> s"[`$reservedColName`, `$reservedColName`]"
      ),
      context = ExpectedContext(fragment = "col", callSitePattern = "")
    )
  }

  test("deduplicateMicrobatch preserves the input column names, types, and ordering") {
    val schema = new StructType()
      .add("a", StringType)
      .add("id", IntegerType)
      .add("z", DoubleType)
      .add("seq", LongType)
      .add("flag", BooleanType)

    val batch = microbatchOf(schema)(
      Row("a1", 1, 1.5, 10L, true),
      Row("a2", 1, 2.5, 20L, false)
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    // Field names and dataTypes must match the input exactly, in the original order.
    assert(
      columnNamesAndDataTypes(processor.deduplicateMicrobatch(batch).schema) ==
        columnNamesAndDataTypes(schema))
  }

  test("deduplicateMicrobatch returns an empty DataFrame with preserved schema") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("seq", LongType)
      .add("value", StringType)

    val batch = microbatchOf(schema)()

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    val result = processor.deduplicateMicrobatch(batch)
    assert(result.collect().isEmpty)
    assert(columnNamesAndDataTypes(result.schema) == columnNamesAndDataTypes(schema))
  }

  // =============== extendMicrobatchRowsWithCdcMetadata tests ===============

  test("extendMicrobatchRowsWithCdcMetadata classifies each row as a delete or an upsert " +
    "per deleteCondition") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("seq", LongType)
      .add("is_delete", BooleanType)

    val batch = microbatchOf(schema)(
      Row(1, 10L, false),
      Row(2, 20L, true),
      Row(3, 30L, false),
      Row(4, 40L, true)
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1,
        deleteCondition = Some(F.col("is_delete") === true)
      ),
      resolvedSequencingType = LongType
    )

    // Mutual-exclusivity invariant: each row's CDC metadata struct has exactly one of
    // (deleteSequence, upsertSequence) non-null, and the non-null side carries the row's
    // sequence value.
    checkAnswer(
      df = processor.extendMicrobatchRowsWithCdcMetadata(batch),
      expectedAnswer = Seq(
        Row(1, 10L, false, Row(null, 10L)),
        Row(2, 20L, true, Row(20L, null)),
        Row(3, 30L, false, Row(null, 30L)),
        Row(4, 40L, true, Row(40L, null))
      )
    )
  }

  test("extendMicrobatchRowsWithCdcMetadata treats null deleteCondition results as upserts") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("seq", LongType)
      .add("is_delete", BooleanType)

    val batch = microbatchOf(schema)(
      Row(1, 10L, null)
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1,
        deleteCondition = Some(F.col("is_delete"))
      ),
      resolvedSequencingType = LongType
    )

    checkAnswer(
      df = processor.extendMicrobatchRowsWithCdcMetadata(batch),
      expectedAnswer = Row(1, 10L, null, Row(null, 10L))
    )
  }

  test("extendMicrobatchRowsWithCdcMetadata treats every row as an upsert " +
    "when deleteCondition is None") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("seq", LongType)
      .add("value", StringType)

    val batch = microbatchOf(schema)(
      Row(1, 10L, "a"),
      Row(2, 20L, "b")
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1,
        deleteCondition = None
      ),
      resolvedSequencingType = LongType
    )

    checkAnswer(
      df = processor.extendMicrobatchRowsWithCdcMetadata(batch),
      expectedAnswer = Seq(
        Row(1, 10L, "a", Row(null, 10L)),
        Row(2, 20L, "b", Row(null, 20L))
      )
    )
  }

  test("extendMicrobatchRowsWithCdcMetadata appends CDC metadata as the last column") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("seq", LongType)
      .add("value", StringType)

    val batch = microbatchOf(schema)(
      Row(1, 10L, "a")
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    val result = processor.extendMicrobatchRowsWithCdcMetadata(batch)

    // Original columns are preserved in their original order, with CDC metadata appended at
    // the very end.
    assert(result.schema.fieldNames.toSeq ==
      schema.fieldNames.toSeq :+ AutoCdcReservedNames.cdcMetadataColName)
  }

  test("extendMicrobatchRowsWithCdcMetadata casts delete / upsert sequence fields to " +
    "resolvedSequencingType") {
    val schema = new StructType()
      .add("id", IntegerType)
      // Microbatch's sequencing column is IntegerType, but the flow's resolved sequencing type
      // will be LongType. This should be upcasted in the projected CDC metadata column.
      .add("seq", IntegerType)
      .add("value", StringType)

    val batch = microbatchOf(schema)(
      Row(1, 10, "a")
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    val resultDf = processor.extendMicrobatchRowsWithCdcMetadata(batch)

    val cdcMetadataDataType =
      resultDf.schema(AutoCdcReservedNames.cdcMetadataColName).dataType.asInstanceOf[StructType]
    assert(columnNamesAndDataTypes(cdcMetadataDataType) == Seq(
      Scd1BatchProcessor.cdcDeleteSequenceFieldName -> LongType,
      Scd1BatchProcessor.cdcUpsertSequenceFieldName -> LongType))

    // The cast must also succeed at runtime: upsertSequence is materialized as a Long value, not
    // an Int.
    checkAnswer(
      df = resultDf,
      expectedAnswer = Row(1, 10, "a", Row(null, 10L))
    )
  }

  test("extendMicrobatchRowsWithCdcMetadata fails fast when the microbatch's sequencing column " +
    "is incompatible with resolvedSequencingType") {
    val schema = new StructType()
      .add("id", IntegerType)
      // Microbatch's sequencing column is a struct, whereas the flow's resolved sequencing type
      // will be LongType. These are incompatible and should throw.
      .add(
        "seq",
        new StructType()
          .add("major", LongType)
          .add("minor", LongType))

    val batch = microbatchOf(schema)(
      Row(1, Row(1L, 0L))
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    val ex = intercept[AnalysisException] {
      // .schema forces analysis of the underlying logical plan, surfacing the invalid cast.
      processor.extendMicrobatchRowsWithCdcMetadata(batch).schema
    }
    assert(ex.getCondition == "DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION")
  }

  test("projectTargetColumnsOntoMicrobatch keeps every user column and the CDC metadata column " +
    "when columnSelection is None") {
    val batch = microbatchOf(microbatchWithCdcMetadataSchema)(
      Row(1, "alice", 30, Row(null, 10L)),
      Row(2, "bob", 25, Row(20L, null))
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1,
        columnSelection = None
      ),
      resolvedSequencingType = LongType
    )

    val result = processor.projectTargetColumnsOntoMicrobatch(batch)

    // None selection is no-op on the user columns, and the CDC metadata column is unconditionally
    // re-projected last, so the output shape exactly matches the input.
    assert(result.schema.fieldNames.toSeq == microbatchWithCdcMetadataSchema.fieldNames.toSeq)
    checkAnswer(
      df = result,
      expectedAnswer = Seq(
        Row(1, "alice", 30, Row(null, 10L)),
        Row(2, "bob", 25, Row(20L, null))
      )
    )
  }

  test("projectTargetColumnsOntoMicrobatch retains the CDC metadata column even when " +
    "IncludeColumns does not contain it") {
    val batch = microbatchOf(microbatchWithCdcMetadataSchema)(
      Row(1, "alice", 30, Row(null, 10L))
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1,
        columnSelection = Some(
          ColumnSelection.IncludeColumns(
            Seq(UnqualifiedColumnName("id"), UnqualifiedColumnName("age"))
          )
        )
      ),
      resolvedSequencingType = LongType
    )

    val result = processor.projectTargetColumnsOntoMicrobatch(batch)

    assert(result.schema.fieldNames.toSeq ==
      Seq("id", "age", AutoCdcReservedNames.cdcMetadataColName))
    checkAnswer(
      df = result,
      expectedAnswer = Row(1, 30, Row(null, 10L))
    )
  }

  test("projectTargetColumnsOntoMicrobatch respects exclude column") {
    val batch = microbatchOf(microbatchWithCdcMetadataSchema)(
      Row(1, "alice", 30, Row(null, 10L))
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1,
        columnSelection = Some(
          ColumnSelection.ExcludeColumns(
            Seq(UnqualifiedColumnName("age"))
          )
        )
      ),
      resolvedSequencingType = LongType
    )

    val result = processor.projectTargetColumnsOntoMicrobatch(batch)

    assert(
      result.schema.fieldNames.toSeq ==
        Seq("id", "name", AutoCdcReservedNames.cdcMetadataColName)
    )
    checkAnswer(
      df = result,
      expectedAnswer = Row(1, "alice", Row(null, 10L))
    )
  }

  test("projectTargetColumnsOntoMicrobatch preserves the microbatch schema order") {
    val batch = microbatchOf(microbatchWithCdcMetadataSchema)(
      Row(1, "alice", 30, Row(null, 10L))
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1,
        // User specifies (age, id) -- intentionally different from the schema order (id, age).
        columnSelection = Some(ColumnSelection.IncludeColumns(
          Seq(UnqualifiedColumnName("age"), UnqualifiedColumnName("id"))
        ))
      ),
      resolvedSequencingType = LongType
    )

    val result = processor.projectTargetColumnsOntoMicrobatch(batch)

    // Output column order follows the original microbatch schema (id before age), not the order
    // in which the user listed columns in IncludeColumns. The CDC metadata column is appended
    // last as always.
    assert(result.schema.fieldNames.toSeq ==
      Seq("id", "age", AutoCdcReservedNames.cdcMetadataColName))

    checkAnswer(
      df = result,
      expectedAnswer = Row(1, 30, Row(null, 10L))
    )
  }

  test("projectTargetColumnsOntoMicrobatch handles backticked column names containing a " +
    "literal dot") {
    val schema = new StructType()
      .add("id", IntegerType)
      // Even if a column is created with backticks via DDL, those backticks are consumed by Spark
      // before resolving the schema; they won't show up in the schema field.
      .add("user.id", StringType)
      .add(AutoCdcReservedNames.cdcMetadataColName, cdcMetadataColSchemaType)

    val batch = microbatchOf(schema)(
      Row(1, "u-100", Row(null, 10L))
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1,
        columnSelection = Some(
          ColumnSelection.IncludeColumns(
            Seq(
              UnqualifiedColumnName("id"),
              UnqualifiedColumnName("`user.id`")
            )
          )
        )
      ),
      resolvedSequencingType = LongType
    )

    val result = processor.projectTargetColumnsOntoMicrobatch(batch)

    assert(result.schema.fieldNames.toSeq ==
      Seq("id", "user.id", AutoCdcReservedNames.cdcMetadataColName))
    checkAnswer(
      df = result,
      expectedAnswer = Row(1, "u-100", Row(null, 10L))
    )
  }

  test("projectTargetColumnsOntoMicrobatch resolves columnSelection case-insensitively " +
    "when SQLConf.CASE_SENSITIVE=false") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val batch = microbatchOf(microbatchWithCdcMetadataSchema)(
        Row(1, "alice", 30, Row(null, 10L))
      )

      val processor = Scd1BatchProcessor(
        changeArgs = ChangeArgs(
          keys = Seq(UnqualifiedColumnName("id")),
          sequencing = F.col("seq"),
          storedAsScdType = ScdType.Type1,
          // User columns intentionally use a different case than the schema (id, age).
          columnSelection = Some(
            ColumnSelection.IncludeColumns(
              Seq(UnqualifiedColumnName("ID"), UnqualifiedColumnName("AGE"))
            )
          )
        ),
        resolvedSequencingType = LongType
      )

      val result = processor.projectTargetColumnsOntoMicrobatch(batch)

      // Output column names follow the microbatch schema's casing, not the casing in the user's
      // columnSelection. The CDC metadata column is appended last as always.
      assert(result.schema.fieldNames.toSeq ==
        Seq("id", "age", AutoCdcReservedNames.cdcMetadataColName))
      checkAnswer(
        df = result,
        expectedAnswer = Row(1, 30, Row(null, 10L))
      )
    }
  }

  // =============== applyTombstonesToMicrobatch tests ===============

  /**
   * Schema for the microbatch input to [[Scd1BatchProcessor.applyTombstonesToMicrobatch]]
   * tests.
   */
  private val applyTombstonesToMicrobatchTestMicrobatchSchema: StructType = new StructType()
    // Key column.
    .add("id", IntegerType)
    // Data column.
    .add("value", StringType)
    // CDC metadata column.
    .add(AutoCdcReservedNames.cdcMetadataColName, cdcMetadataColSchemaType)

  /**
   * Schema for the auxiliary input to [[Scd1BatchProcessor.applyTombstonesToMicrobatch]] tests.
   *
   * In practice for SCD1 the auxiliary table only carries key columns and the CDC metadata
   * column -- never user data columns -- so we mirror that production-side asymmetry here,
   * even though the function's API contract would allow a single shared schema.
   */
  private val applyTombstonesToMicrobatchTestAuxiliarySchema: StructType = new StructType()
    // Key column.
    .add("id", IntegerType)
    // CDC metadata column.
    .add(AutoCdcReservedNames.cdcMetadataColName, cdcMetadataColSchemaType)

  test("applyTombstonesToMicrobatch drops late-arriving deletes and upserts when a matching " +
    "tombstone exists for the same key") {
    // Both microbatch events have an effective sequence strictly less than the tombstone's
    // delete sequence, so they must be anti-joined out of the microbatch regardless of whether
    // they are deletes or upserts.
    val microbatch = microbatchOf(applyTombstonesToMicrobatchTestMicrobatchSchema)(
      Row(1, "stale-upsert", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(5))),
      Row(1, "stale-delete", cdcMetadataRow(deleteSeq = Some(7), upsertSeq = None))
    )
    val auxiliary = microbatchOf(applyTombstonesToMicrobatchTestAuxiliarySchema)(
      Row(1, cdcMetadataRow(deleteSeq = Some(10), upsertSeq = None))
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        // Sequencing is irrelevant for applyTombstonesToMicrobatch; it is already encoded
        // into the CDC metadata column.
        sequencing = F.lit(0L),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    val result = processor.applyTombstonesToMicrobatch(microbatch, auxiliary)
    assert(result.collect().isEmpty)
  }

  test("applyTombstonesToMicrobatch keeps a microbatch row whose effective sequence ties the " +
    "tombstone's delete sequence") {
    // The join uses strict `<`, so a microbatch row with the same effective sequence as the
    // tombstone is kept. This is an internal tie-breaking convention for SCD1 only, and is
    // *not* a publicly documented contract: if external callers ever start relying on it, both
    // this test and the join condition in applyTombstonesToMicrobatch should move together.
    val microbatch = microbatchOf(applyTombstonesToMicrobatchTestMicrobatchSchema)(
      Row(1, "tied-upsert", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(10)))
    )
    val auxiliary = microbatchOf(applyTombstonesToMicrobatchTestAuxiliarySchema)(
      Row(1, cdcMetadataRow(deleteSeq = Some(10), upsertSeq = None))
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        // Sequencing is irrelevant for applyTombstonesToMicrobatch; it is already encoded
        // into the CDC metadata column.
        sequencing = F.lit(0L),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    checkAnswer(
      df = processor.applyTombstonesToMicrobatch(microbatch, auxiliary),
      expectedAnswer = Row(1, "tied-upsert", Row(null, 10L))
    )
  }

  test("applyTombstonesToMicrobatch keeps microbatch rows whose effective sequence exceeds the " +
    "tombstone's delete sequence") {
    val microbatch = microbatchOf(applyTombstonesToMicrobatchTestMicrobatchSchema)(
      Row(1, "fresher-upsert", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(15))),
      Row(1, "fresher-delete", cdcMetadataRow(deleteSeq = Some(20), upsertSeq = None))
    )
    val auxiliary = microbatchOf(applyTombstonesToMicrobatchTestAuxiliarySchema)(
      Row(1, cdcMetadataRow(deleteSeq = Some(10), upsertSeq = None))
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        // Sequencing is irrelevant for applyTombstonesToMicrobatch; it is already encoded
        // into the CDC metadata column.
        sequencing = F.lit(0L),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    checkAnswer(
      df = processor.applyTombstonesToMicrobatch(microbatch, auxiliary),
      expectedAnswer = Seq(
        Row(1, "fresher-upsert", Row(null, 15L)),
        Row(1, "fresher-delete", Row(20L, null))
      )
    )
  }

  test("applyTombstonesToMicrobatch leaves microbatch rows untouched when the tombstone targets " +
    "a different key") {
    val microbatch = microbatchOf(applyTombstonesToMicrobatchTestMicrobatchSchema)(
      Row(1, "stays", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(5)))
    )
    // Tombstone on a different key with a much larger sequence; the key match must guard
    // against cross-key application no matter how stale the microbatch row's sequence is.
    val auxiliary = microbatchOf(applyTombstonesToMicrobatchTestAuxiliarySchema)(
      Row(2, cdcMetadataRow(deleteSeq = Some(1000), upsertSeq = None))
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        // Sequencing is irrelevant for applyTombstonesToMicrobatch; it is already encoded
        // into the CDC metadata column.
        sequencing = F.lit(0L),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    checkAnswer(
      df = processor.applyTombstonesToMicrobatch(microbatch, auxiliary),
      expectedAnswer = Row(1, "stays", Row(null, 5L))
    )
  }

  test("applyTombstonesToMicrobatch with composite keys requires every key column to match") {
    val schema = new StructType()
      .add("region", StringType)
      .add("customer_id", IntegerType)
      .add(AutoCdcReservedNames.cdcMetadataColName, cdcMetadataColSchemaType)

    val microbatch = microbatchOf(schema)(
      Row("US", 1, cdcMetadataRow(deleteSeq = None, upsertSeq = Some(5))),
      Row("US", 2, cdcMetadataRow(deleteSeq = None, upsertSeq = Some(5)))
    )
    // Tombstone matches on `region` only; `customer_id` differs from every microbatch row.
    // The join condition is the AND of all key column equalities, so neither microbatch row
    // should be dropped.
    val auxiliary = microbatchOf(schema)(
      Row("US", 99, cdcMetadataRow(deleteSeq = Some(1000), upsertSeq = None))
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("region"), UnqualifiedColumnName("customer_id")),
        // Sequencing is irrelevant for applyTombstonesToMicrobatch; it is already encoded
        // into the CDC metadata column.
        sequencing = F.lit(0L),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    checkAnswer(
      df = processor.applyTombstonesToMicrobatch(microbatch, auxiliary),
      expectedAnswer = Seq(
        Row("US", 1, Row(null, 5L)),
        Row("US", 2, Row(null, 5L))
      )
    )
  }

  test("applyTombstonesToMicrobatch supports backticked key names containing a literal dot") {
    val schema = new StructType()
      .add("user.id", IntegerType)
      .add(AutoCdcReservedNames.cdcMetadataColName, cdcMetadataColSchemaType)

    val microbatch = microbatchOf(schema)(
      Row(1, cdcMetadataRow(deleteSeq = None, upsertSeq = Some(5)))
    )
    val auxiliary = microbatchOf(schema)(
      Row(1, cdcMetadataRow(deleteSeq = Some(10), upsertSeq = None))
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("`user.id`")),
        // Sequencing is irrelevant for applyTombstonesToMicrobatch; it is already encoded
        // into the CDC metadata column.
        sequencing = F.lit(0L),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    val result = processor.applyTombstonesToMicrobatch(microbatch, auxiliary)
    assert(result.collect().isEmpty)
  }

  test("applyTombstonesToMicrobatch is a no-op when the auxiliary table is empty") {
    val microbatch = microbatchOf(applyTombstonesToMicrobatchTestMicrobatchSchema)(
      Row(1, "kept-upsert", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(5))),
      Row(2, "kept-delete", cdcMetadataRow(deleteSeq = Some(7), upsertSeq = None))
    )

    // Empty auxiliary: no rows means the left-anti join cannot match any microbatch row, so the
    // microbatch passes through untouched regardless of its contents.

    // Conceptually, this means there are no tombstones that could potentially have delete-matched
    // against incoming rows in the microbatch.
    val auxiliary = microbatchOf(applyTombstonesToMicrobatchTestAuxiliarySchema)()

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        // Sequencing is irrelevant for applyTombstonesToMicrobatch; it is already encoded
        // into the CDC metadata column.
        sequencing = F.lit(0L),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    checkAnswer(
      df = processor.applyTombstonesToMicrobatch(microbatch, auxiliary),
      expectedAnswer = Seq(
        Row(1, "kept-upsert", Row(null, 5L)),
        Row(2, "kept-delete", Row(7L, null))
      )
    )
  }

  test("applyTombstonesToMicrobatch keeps microbatch rows when the matching aux row has a " +
    "null deleteSequence") {
    // SCD1's tombstone-merge invariant guarantees aux rows always have a non-null
    // deleteSequence, but if a corrupt aux row ever does carry a null deleteSequence, the
    // join's `<` predicate evaluates to null (SQL 3-valued logic) and the microbatch row is
    // retained -- a safe fallback that never silently drops data.
    val microbatch = microbatchOf(applyTombstonesToMicrobatchTestMicrobatchSchema)(
      Row(1, "kept-upsert", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(5)))
    )
    val auxiliary = microbatchOf(applyTombstonesToMicrobatchTestAuxiliarySchema)(
      Row(1, cdcMetadataRow(deleteSeq = None, upsertSeq = None))
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        // Sequencing is irrelevant for applyTombstonesToMicrobatch; it is already encoded
        // into the CDC metadata column.
        sequencing = F.lit(0L),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    checkAnswer(
      df = processor.applyTombstonesToMicrobatch(microbatch, auxiliary),
      expectedAnswer = Row(1, "kept-upsert", Row(null, 5L))
    )
  }

  test("applyTombstonesToMicrobatch is unaffected by  stale tombstones in auxiliary table") {
    // SCD1's tombstone-merge invariant guarantees at most one tombstone per key in the
    // auxiliary, but if multiple ever coexist for the same key, the left-anti semantics drop
    // the microbatch row whenever *any* matching tombstone has a strictly greater
    // deleteSequence.
    val microbatch = microbatchOf(applyTombstonesToMicrobatchTestMicrobatchSchema)(
      Row(1, "stale-upsert", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(8)))
    )
    // Two tombstones on key=1: one stale (deleteSeq=5, doesn't dominate the microbatch row's
    // effective seq of 8), one fresh (deleteSeq=10, dominates). The fresh one alone is enough
    // to drop the microbatch row.
    val auxiliary = microbatchOf(applyTombstonesToMicrobatchTestAuxiliarySchema)(
      Row(1, cdcMetadataRow(deleteSeq = Some(5), upsertSeq = None)),
      Row(1, cdcMetadataRow(deleteSeq = Some(10), upsertSeq = None))
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        // Sequencing is irrelevant for applyTombstonesToMicrobatch; it is already encoded
        // into the CDC metadata column.
        sequencing = F.lit(0L),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    val result = processor.applyTombstonesToMicrobatch(microbatch, auxiliary)
    assert(result.collect().isEmpty)
  }

  test("applyTombstonesToMicrobatch ignores the aux row's upsertSequence even when it is set") {
    // SCD1's tombstone-merge invariant guarantees aux rows always have a null upsertSequence
    // (by definition, an aux row is an unswallowed tombstone). But if a corrupt aux row ever
    // has both fields set, only its deleteSequence is read by the join condition; the
    // upsertSequence is never inspected, so the row continues to behave as a pure tombstone.
    val microbatch = microbatchOf(applyTombstonesToMicrobatchTestMicrobatchSchema)(
      Row(1, "stale-upsert", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(5)))
    )
    // Aux row with both fields populated; only deleteSeq=10 drives the tombstone-drop decision.
    val auxiliary = microbatchOf(applyTombstonesToMicrobatchTestAuxiliarySchema)(
      Row(1, cdcMetadataRow(deleteSeq = Some(10), upsertSeq = Some(20)))
    )

    val processor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        // Sequencing is irrelevant for applyTombstonesToMicrobatch; it is already encoded
        // into the CDC metadata column.
        sequencing = F.lit(0L),
        storedAsScdType = ScdType.Type1
      ),
      resolvedSequencingType = LongType
    )

    val result = processor.applyTombstonesToMicrobatch(microbatch, auxiliary)
    assert(result.collect().isEmpty)
  }
}
