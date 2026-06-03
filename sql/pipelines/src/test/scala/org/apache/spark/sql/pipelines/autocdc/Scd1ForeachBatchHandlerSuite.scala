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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * E2E unit tests for the Scd1ForeachBatchHandler class.
 */
class Scd1ForeachBatchHandlerSuite
    extends QueryTest
    with SharedSparkSession
    with BeforeAndAfter
    with AutoCdcCatalogExecutionTestBase {

  private val sourceSchema = new StructType()
    .add("id", IntegerType)
    .add("value", StringType)
    .add("seq", LongType)
    .add("is_delete", BooleanType)

  private val auxiliarySchema = new StructType()
    .add("id", IntegerType)
    .add(Scd1BatchProcessor.cdcMetadataColName, cdcMetadataColSchemaType())

  private val targetSchema = new StructType()
    .add("id", IntegerType)
    .add("value", StringType)
    .add(Scd1BatchProcessor.cdcMetadataColName, cdcMetadataColSchemaType())

  private val processor = Scd1BatchProcessor(
    changeArgs = ChangeArgs(
      keys = Seq(UnqualifiedColumnName("id")),
      sequencing = F.col("seq"),
      storedAsScdType = ScdType.Type1,
      deleteCondition = Some(F.col("is_delete")),
      columnSelection = Some(
        ColumnSelection.IncludeColumns(
          Seq(UnqualifiedColumnName("id"), UnqualifiedColumnName("value"))
        )
      )
    ),
    resolvedSequencingType = LongType
  )

  /** Create the auxiliary table using [[auxiliarySchema]], optionally seeded with `seedRows`. */
  private def createAuxTable(seedRows: Row*): Unit =
    createTable(defaultAuxIdent, defaultAuxTableIdentifier, auxiliarySchema, seedRows: _*)

  /** Create the target table using [[targetSchema]], optionally seeded with `seedRows`. */
  private def createTargetTable(seedRows: Row*): Unit =
    createTable(defaultTargetIdent, defaultTargetTableIdentifier, targetSchema, seedRows: _*)

  private def exec: Scd1ForeachBatchHandler = Scd1ForeachBatchHandler(
    batchProcessor = processor,
    auxiliaryTableIdentifier = defaultAuxTableIdentifier,
    targetTableIdentifier = defaultTargetTableIdentifier
  )

  // ===========================================================================================
  // Microbatch validation tests
  // ===========================================================================================

  test(
    "Scd1ForeachBatchHandler invalidates rows with null sequencing before merging to aux/target " +
    "tables."
  ) {
    createAuxTable()
    createTargetTable(Row(1, "old", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(10L))))

    val batch = microbatchOf(sourceSchema)(
      Row(1, "invalid", null, false)
    )

    checkError(
      exception = intercept[AnalysisException] {
        exec.execute(batch, batchId = 123L)
      },
      condition = "AUTOCDC_MICROBATCH_VALIDATION.NULL_SEQUENCE",
      sqlState = "22000",
      parameters = Map(
        "tableName" -> defaultTargetTableIdentifier.quotedString,
        "batchId" -> "123",
        "nullCount" -> "1"
      )
    )
    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
    assert(resultAuxTable.collect().isEmpty)
    checkAnswer(resultTargetTable, Row(1, "old", Row(null, 10L)))
  }

  test(
    "Scd1ForeachBatchHandler invalidates rows with a null key column before merging to aux/target"
  ) {
    createAuxTable()
    createTargetTable(Row(1, "old", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(10L))))

    // Two rows have a null id; one row is well-formed. The validator must surface a count
    // of two without writing anything to the aux or target table.
    val batch = microbatchOf(sourceSchema)(
      Row(null, "no-id-1", 5L, false),
      Row(2, "ok", 6L, false),
      Row(null, "no-id-2", 7L, true)
    )

    checkError(
      exception = intercept[AnalysisException] {
        exec.execute(batch, batchId = 13L)
      },
      condition = "AUTOCDC_MICROBATCH_VALIDATION.NULL_KEY",
      sqlState = "22000",
      parameters = Map(
        "tableName" -> defaultTargetTableIdentifier.quotedString,
        "batchId" -> "13",
        "nullKeyCounts" -> "`id`=2"
      )
    )
    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
    assert(resultAuxTable.collect().isEmpty)
    checkAnswer(resultTargetTable, Row(1, "old", Row(null, 10L)))
  }

  test(
    "Scd1ForeachBatchHandler invalidates rows when any column of a composite key is null"
  ) {
    // Composite [country, city] key. The validator must report per-column null counts in
    // the configured key order (country before city).
    val compositeSourceSchema = new StructType()
      .add("country", StringType)
      .add("city", StringType)
      .add("seq", LongType)
      .add("is_delete", BooleanType)
    val compositeAuxSchema = new StructType()
      .add("country", StringType)
      .add("city", StringType)
      .add(Scd1BatchProcessor.cdcMetadataColName, cdcMetadataColSchemaType())
    val compositeTargetSchema = new StructType()
      .add("country", StringType)
      .add("city", StringType)
      .add(Scd1BatchProcessor.cdcMetadataColName, cdcMetadataColSchemaType())

    val compositeProcessor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("country"), UnqualifiedColumnName("city")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1,
        deleteCondition = Some(F.col("is_delete"))
      ),
      resolvedSequencingType = LongType
    )
    val compositeExec = Scd1ForeachBatchHandler(
      batchProcessor = compositeProcessor,
      auxiliaryTableIdentifier = defaultAuxTableIdentifier,
      targetTableIdentifier = defaultTargetTableIdentifier
    )

    createTable(defaultAuxIdent, defaultAuxTableIdentifier, compositeAuxSchema)
    createTable(defaultTargetIdent, defaultTargetTableIdentifier, compositeTargetSchema)

    // country is null in 2 rows, city is null in 2 rows (one row has both null).
    val batch = microbatchOf(compositeSourceSchema)(
      Row(null, "Boston", 1L, false),
      Row("US", null, 2L, false),
      Row(null, null, 3L, false)
    )

    checkError(
      exception = intercept[AnalysisException] {
        compositeExec.execute(batch, batchId = 7L)
      },
      condition = "AUTOCDC_MICROBATCH_VALIDATION.NULL_KEY",
      sqlState = "22000",
      parameters = Map(
        "tableName" -> defaultTargetTableIdentifier.quotedString,
        "batchId" -> "7",
        "nullKeyCounts" -> "`country`=2, `city`=2"
      )
    )
    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
    assert(resultAuxTable.collect().isEmpty)
    assert(resultTargetTable.collect().isEmpty)
  }

  test(
    "Scd1ForeachBatchHandler surfaces the null-sequence error before the null-key error"
  ) {
    // A single row has both a null sequence and a null id. The validator must surface the
    // sequence error first to preserve the existing precedence.
    createAuxTable()
    createTargetTable()

    val batch = microbatchOf(sourceSchema)(
      Row(null, "bad", null, false)
    )

    checkError(
      exception = intercept[AnalysisException] {
        exec.execute(batch, batchId = 99L)
      },
      condition = "AUTOCDC_MICROBATCH_VALIDATION.NULL_SEQUENCE",
      sqlState = "22000",
      parameters = Map(
        "tableName" -> defaultTargetTableIdentifier.quotedString,
        "batchId" -> "99",
        "nullCount" -> "1"
      )
    )
  }

  test(
    "Scd1ForeachBatchHandler validates that the microbatch's sequencing column is orderable"
  ) {
    createAuxTable()
    createTargetTable(Row(1, "old", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(10L))))

    val batchSchema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add("seq", MapType(StringType, IntegerType))
      .add("is_delete", BooleanType)
    val batch = microbatchOf(batchSchema)(
      Row(1, "invalid", Map("k" -> 1), false)
    )

    checkError(
      exception = intercept[AnalysisException] {
        exec.execute(batch, batchId = 124L)
      },
      condition = "AUTOCDC_MICROBATCH_VALIDATION.NON_ORDERABLE_SEQUENCE",
      sqlState = "22000",
      parameters = Map(
        "tableName" -> defaultTargetTableIdentifier.quotedString,
        "batchId" -> "124",
        "dataType" -> "map<string,int>"
      )
    )
    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
    assert(resultAuxTable.collect().isEmpty)
    checkAnswer(resultTargetTable, Row(1, "old", Row(null, 10L)))
  }

  // ===========================================================================================
  // Core SCD1 transformation tests
  // ===========================================================================================

  test(
    "Scd1ForeachBatchHandler drops stale microbatch rows using auxiliary tombstones and writes " +
    "fresh upserts"
  ) {
    createAuxTable(Row(1, cdcMetadataRow(deleteSeq = Some(10L), upsertSeq = None)))
    createTargetTable()

    val batch = microbatchOf(sourceSchema)(
      Row(1, "stale", 5L, false),
      Row(2, "fresh", 20L, false)
    )

    exec.execute(batch, batchId = 0L)

    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
    checkAnswer(resultAuxTable, Row(1, Row(10L, null)))
    checkAnswer(resultTargetTable, Row(2, "fresh", Row(null, 20L)))
  }

  test(
    "Scd1ForeachBatchHandler persists a newer delete as a tombstone and removes the target row"
  ) {
    createAuxTable()
    createTargetTable(Row(1, "old", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(10L))))

    val batch = microbatchOf(sourceSchema)(
      Row(1, "unused", 20L, true)
    )

    exec.execute(batch, batchId = 1L)

    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
    checkAnswer(resultAuxTable, Row(1, Row(20L, null)))
    assert(resultTargetTable.collect().isEmpty)
  }

  test(
    "Scd1ForeachBatchHandler deduplicates the raw microbatch before merging into the target"
  ) {
    createAuxTable()
    createTargetTable(Row(1, "old", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(10L))))

    val batch = microbatchOf(sourceSchema)(
      Row(1, "ignored-older", 15L, false),
      Row(1, "newer", 20L, false)
    )

    exec.execute(batch, batchId = 2L)

    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
    assert(resultAuxTable.collect().isEmpty)
    checkAnswer(resultTargetTable, Row(1, "newer", Row(null, 20L)))
  }

  test(
    "Scd1ForeachBatchHandler reconciles out-of-order events when ExcludeColumns hides the " +
    "sequencing column"
  ) {
    // ExcludeColumns omits the sequencing column ("seq") and the delete marker ("is_delete")
    // from persisted rows. The sequencing expression itself still drives CDC reconciliation;
    // this test verifies that several out-of-order events across six batches converge to the
    // correct target state without ever materializing those columns.
    val excludeProcessor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1,
        deleteCondition = Some(F.col("is_delete")),
        columnSelection = Some(
          ColumnSelection.ExcludeColumns(
            Seq(UnqualifiedColumnName("seq"), UnqualifiedColumnName("is_delete"))
          )
        )
      ),
      resolvedSequencingType = LongType
    )
    val excludeExec = Scd1ForeachBatchHandler(
      batchProcessor = excludeProcessor,
      auxiliaryTableIdentifier = defaultAuxTableIdentifier,
      targetTableIdentifier = defaultTargetTableIdentifier
    )

    createAuxTable()
    createTargetTable()

    // Batch 1: highest-seq event in the batch wins on insert.
    excludeExec.execute(
      microbatchOf(sourceSchema)(
        Row(1, "alice", 1L, false),
        Row(1, "bob", 3L, false)
      ),
      batchId = 0L
    )

    // Batch 2: out-of-order older upsert (seq=2) must not overwrite the live row at seq=3.
    excludeExec.execute(microbatchOf(sourceSchema)(Row(1, "carol", 2L, false)), batchId = 1L)

    // Batch 3: even-newer upsert wins.
    excludeExec.execute(microbatchOf(sourceSchema)(Row(1, "dave", 4L, false)), batchId = 2L)

    // Batch 4: out-of-order older delete (seq=2) must not erase the live row at seq=4.
    excludeExec.execute(microbatchOf(sourceSchema)(Row(1, null, 2L, true)), batchId = 3L)

    val targetTableAfterBatch4 = spark.read.table(defaultTargetTableIdentifier.quotedString)
    checkAnswer(targetTableAfterBatch4, Row(1, "dave", Row(null, 4L)))

    // Batch 5: newer delete (seq=5) wipes the row from the target.
    excludeExec.execute(microbatchOf(sourceSchema)(Row(1, null, 5L, true)), batchId = 4L)

    // Batch 6: out-of-order pre-delete upsert (seq=4) is suppressed by the tombstone.
    excludeExec.execute(microbatchOf(sourceSchema)(Row(1, "ghost", 4L, false)), batchId = 5L)

    val auxTableAfterBatch6 = spark.read.table(defaultAuxTableIdentifier.quotedString)
    val targetTableAfterBatch6 = spark.read.table(defaultTargetTableIdentifier.quotedString)
    assert(targetTableAfterBatch6.collect().isEmpty)
    checkAnswer(auxTableAfterBatch6, Row(1, Row(5L, null)))
  }

  test(
    "Scd1ForeachBatchHandler upserts an existing target row when a higher-sequenced event arrives"
  ) {
    createAuxTable()
    createTargetTable(Row(1, "alice", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(1L))))

    exec.execute(microbatchOf(sourceSchema)(Row(1, "bob", 2L, false)), batchId = 0L)

    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
    checkAnswer(resultTargetTable, Row(1, "bob", Row(null, 2L)))
    assert(resultAuxTable.collect().isEmpty)
  }

  test(
    "Scd1ForeachBatchHandler records an aux tombstone for a delete on a nonexistent key without" +
      " affecting the target"
  ) {
    // A delete event for a key that never existed in the target must still be recorded in
    // the auxiliary table, because a strictly older upsert for the same key arriving in a
    // later batch must be suppressed by that tombstone.
    createAuxTable()
    createTargetTable()

    exec.execute(microbatchOf(sourceSchema)(Row(99, null, 1L, true)), batchId = 0L)

    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
    assert(resultTargetTable.collect().isEmpty)
    checkAnswer(resultAuxTable, Row(99, Row(1L, null)))
  }

  test(
    "Scd1ForeachBatchHandler ignores a late-arriving upsert with a sequence below the target's" +
      " last upsert"
  ) {
    createAuxTable()
    createTargetTable(Row(1, "alice", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(5L))))

    exec.execute(microbatchOf(sourceSchema)(Row(1, "bob", 2L, false)), batchId = 0L)

    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
    checkAnswer(resultTargetTable, Row(1, "alice", Row(null, 5L)))
    assert(resultAuxTable.collect().isEmpty)
  }

  test(
    "Scd1ForeachBatchHandler ignores a late-arriving lower-seq delete but still records the aux" +
      " tombstone"
  ) {
    // The auxiliary table records every incoming delete event regardless of whether it
    // displaces a target row, so future events at or below the same sequence are filtered
    // consistently.
    createAuxTable()
    createTargetTable(Row(1, "alice", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(5L))))

    exec.execute(microbatchOf(sourceSchema)(Row(1, "alice", 2L, true)), batchId = 0L)

    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
    checkAnswer(resultTargetTable, Row(1, "alice", Row(null, 5L)))
    checkAnswer(resultAuxTable, Row(1, Row(2L, null)))
  }

  test(
    "Scd1ForeachBatchHandler resolves a within-batch delete and higher-sequenced upsert as an" +
      " upsert insert"
  ) {
    // Within-batch dedup picks the highest-sequenced event regardless of kind. Here an
    // upsert at seq=3 beats a delete at seq=2, so the row is inserted into the target and
    // no auxiliary tombstone is recorded for the per-key winner.
    createAuxTable()
    createTargetTable()

    exec.execute(
      microbatchOf(sourceSchema)(
        Row(1, "alice", 2L, true),
        Row(1, "bob", 3L, false)
      ),
      batchId = 0L
    )

    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
    checkAnswer(resultTargetTable, Row(1, "bob", Row(null, 3L)))
    assert(resultAuxTable.collect().isEmpty)
  }

  test(
    "Scd1ForeachBatchHandler treats a composite key as a single identifier and isolates rows by" +
      " full key"
  ) {
    // Composite [country, city] key. Three rows that overlap on country (US, US, UK) but
    // never on the full key must remain three distinct identities in the target.
    val compositeSourceSchema = new StructType()
      .add("country", StringType)
      .add("city", StringType)
      .add("population", LongType)
      .add("seq", LongType)
      .add("is_delete", BooleanType)
    val compositeAuxSchema = new StructType()
      .add("country", StringType)
      .add("city", StringType)
      .add(Scd1BatchProcessor.cdcMetadataColName, cdcMetadataColSchemaType())
    val compositeTargetSchema = new StructType()
      .add("country", StringType)
      .add("city", StringType)
      .add("population", LongType)
      .add(Scd1BatchProcessor.cdcMetadataColName, cdcMetadataColSchemaType())

    val compositeProcessor = Scd1BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("country"), UnqualifiedColumnName("city")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type1,
        deleteCondition = Some(F.col("is_delete")),
        columnSelection = Some(
          ColumnSelection.ExcludeColumns(
            Seq(UnqualifiedColumnName("seq"), UnqualifiedColumnName("is_delete"))
          )
        )
      ),
      resolvedSequencingType = LongType
    )
    val compositeExec = Scd1ForeachBatchHandler(
      batchProcessor = compositeProcessor,
      auxiliaryTableIdentifier = defaultAuxTableIdentifier,
      targetTableIdentifier = defaultTargetTableIdentifier
    )

    createTable(defaultAuxIdent, defaultAuxTableIdentifier, compositeAuxSchema)
    createTable(defaultTargetIdent, defaultTargetTableIdentifier, compositeTargetSchema)

    compositeExec.execute(
      microbatchOf(compositeSourceSchema)(
        Row("US", "New York", 8000000L, 1L, false),
        Row("US", "Los Angeles", 4000000L, 1L, false),
        Row("UK", "London", 9000000L, 1L, false)
      ),
      batchId = 0L
    )

    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
    checkAnswer(
      resultTargetTable.orderBy("country", "city"),
      Seq(
        Row("UK", "London", 9000000L, Row(null, 1L)),
        Row("US", "Los Angeles", 4000000L, Row(null, 1L)),
        Row("US", "New York", 8000000L, Row(null, 1L))
      )
    )
    assert(resultAuxTable.collect().isEmpty)
  }

  test(
    "Scd1ForeachBatchHandler leaves unrelated target rows untouched when only one key is updated"
  ) {
    createAuxTable()
    createTargetTable(
      Row(1, "alice", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(1L))),
      Row(2, "bob", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(1L)))
    )

    exec.execute(microbatchOf(sourceSchema)(Row(1, "alice-updated", 2L, false)), batchId = 0L)

    val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
    val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
    checkAnswer(
      resultTargetTable.orderBy("id"),
      Seq(
        Row(1, "alice-updated", Row(null, 2L)),
        Row(2, "bob", Row(null, 1L))
      )
    )
    assert(resultAuxTable.collect().isEmpty)
  }

  // ===========================================================================================
  // Case-sensitivity tests
  // ===========================================================================================

  // A processor that intentionally references columns in UPPERCASE while the suite's source,
  // auxiliary, and target schemas use lowercase. The case-sensitivity tests below run the
  // same execute() with this processor under different SQLConf settings to verify the
  // session's case-sensitivity flag drives every stage of the pipeline.
  private val mixedCaseProcessor = Scd1BatchProcessor(
    changeArgs = ChangeArgs(
      keys = Seq(UnqualifiedColumnName("ID")),
      sequencing = F.col("SEQ"),
      storedAsScdType = ScdType.Type1,
      deleteCondition = Some(F.col("IS_DELETE")),
      columnSelection = Some(
        ColumnSelection.IncludeColumns(
          Seq(UnqualifiedColumnName("ID"), UnqualifiedColumnName("VALUE"))
        )
      )
    ),
    resolvedSequencingType = LongType
  )

  private def mixedCaseExec: Scd1ForeachBatchHandler = Scd1ForeachBatchHandler(
    batchProcessor = mixedCaseProcessor,
    auxiliaryTableIdentifier = defaultAuxTableIdentifier,
    targetTableIdentifier = defaultTargetTableIdentifier
  )

  test(
    "Scd1ForeachBatchHandler honors case-insensitive analysis from the batch dataframe's session"
  ) {
    // Every stage of execute (validation, dedup, project target columns, tombstone
    // application, merge to aux, merge to target) must resolve the UPPERCASE column refs in
    // ChangeArgs against the lowercase schema and produce the correct target+aux state.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      createAuxTable()
      createTargetTable(Row(1, "alice", cdcMetadataRow(deleteSeq = None, upsertSeq = Some(1L))))

      mixedCaseExec.execute(
        microbatchOf(sourceSchema)(Row(1, "bob", 2L, false)),
        batchId = 0L
      )

      val resultAuxTable = spark.read.table(defaultAuxTableIdentifier.quotedString)
      val resultTargetTable = spark.read.table(defaultTargetTableIdentifier.quotedString)
      checkAnswer(resultTargetTable, Row(1, "bob", Row(null, 2L)))
      assert(resultAuxTable.collect().isEmpty)
    }
  }

  test(
    "Scd1ForeachBatchHandler honors case-sensitive analysis from the batch dataframe's session"
  ) {
    // With case-sensitive analysis, the same UPPERCASE ChangeArgs references against a
    // lowercase schema must not be silently normalized. Execute must surface an
    // AnalysisException rather than fall back to case-insensitive matching anywhere.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      createAuxTable()
      createTargetTable()

      intercept[AnalysisException] {
        mixedCaseExec.execute(
          microbatchOf(sourceSchema)(Row(1, "bob", 2L, false)),
          batchId = 0L
        )
      }
    }
  }
}
