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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.{functions => F, Row}
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class Scd1BatchProcessorSuite extends QueryTest with SharedSparkSession {

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
      )
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
      )
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
      )
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
      )
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
      )
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
      )
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
      )
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
      )
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
      )
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
      )
    )

    checkAnswer(
      df = processor.deduplicateMicrobatch(batch),
      expectedAnswer = Row(1, 20L, "new")
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
      )
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
      )
    )

    val result = processor.deduplicateMicrobatch(batch)
    assert(result.collect().isEmpty)
    assert(columnNamesAndDataTypes(result.schema) == columnNamesAndDataTypes(schema))
  }
}
