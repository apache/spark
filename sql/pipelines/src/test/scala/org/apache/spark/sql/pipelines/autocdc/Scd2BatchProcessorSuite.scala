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

import org.apache.spark.sql.{functions => F, QueryTest, Row}
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class Scd2BatchProcessorSuite extends QueryTest with SharedSparkSession {

  /** Build a microbatch [[DataFrame]] from explicit rows and an explicit schema. */
  private def microbatchOf(schema: StructType)(rows: Row*): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

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
}
