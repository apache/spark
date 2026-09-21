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
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class Scd2CoalesceIgnoredNullsSuite extends QueryTest with SharedSparkSession {

  private def includeColumns(columnNames: String*): ColumnSelection =
    ColumnSelection.IncludeColumns(columnNames.map(UnqualifiedColumnName(_)))

  private def excludeColumns(columnNames: String*): ColumnSelection =
    ColumnSelection.ExcludeColumns(columnNames.map(UnqualifiedColumnName(_)))

  private def versionMap(entries: (Seq[String], Boolean)*): Map[String, Boolean] =
    entries.map { case (path, authored) =>
      QuotingUtils.quoteNameParts(path) -> authored
    }.toMap

  private def cdcMetadata(
      recordStartAt: java.lang.Long,
      versionMap: Map[String, Boolean]): Row =
    Row(recordStartAt, versionMap)

  private def targetTableOf(
      userSchema: StructType,
      cdcColumnMetadata: Metadata = Metadata.empty)(rows: Row*): DataFrame = {
    val schema = StructType(userSchema.fields.toSeq ++ Seq(
      StructField(Scd2BatchProcessor.startAtColName, LongType, nullable = true),
      StructField(Scd2BatchProcessor.endAtColName, LongType, nullable = true),
      StructField(
        AutoCdcReservedNames.cdcMetadataColName,
        Scd2BatchProcessor.cdcMetadataColSchema(LongType),
        nullable = false,
        cdcColumnMetadata)
    ))
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  private def processor(ignoreNullSelection: ColumnSelection): Scd2BatchProcessor =
    Scd2BatchProcessor(
      changeArgs = ChangeArgs(
        keys = Seq(UnqualifiedColumnName("id")),
        sequencing = F.col("seq"),
        storedAsScdType = ScdType.Type2,
        ignoreNullSelection = Some(ignoreNullSelection)
      ),
      resolvedSequencingType = LongType
    )

  private def coalesce(
      df: DataFrame,
      ignoreNullSelection: ColumnSelection): DataFrame =
    processor(ignoreNullSelection).coalesceIgnoredNulls(df, ignoreNullSelection)

  test("rows without version maps establish authorship using the current selection") {
    val selection = includeColumns("selected")
    val schema = new StructType()
      .add("id", IntegerType)
      .add("selected", StringType)
      .add("unselected", StringType)
    val existingMap = versionMap(Seq("unselected") -> true)
    val initializedMap = versionMap(
      Seq("selected") -> false,
      Seq("unselected") -> true)

    val input = targetTableOf(schema)(
      Row(1, "source", null, 10L, null, cdcMetadata(10L, existingMap)),
      Row(1, null, null, 20L, null, cdcMetadata(20L, null))
    )

    checkAnswer(
      coalesce(input, selection),
      Seq(
        Row(1, "source", null, 10L, null, cdcMetadata(10L, existingMap)),
        Row(1, "source", null, 20L, null, cdcMetadata(20L, initializedMap))
      )
    )
  }

  test("current selection gates reconciliation while preserving existing map authorship") {
    // Existing maps remain the source of truth for ingestion-time authorship. The current
    // selection only chooses which leaves to reconcile: adding a leaf consults its recorded
    // authorship, while removing a leaf stops coalescing it without rewriting its map entry.
    val selection = includeColumns("selectedNow")
    val schema = new StructType()
      .add("id", IntegerType)
      .add("selectedNow", StringType)
      .add("removedNow", StringType)
    val emptyMap = versionMap()
    val authoredSelectedMap = versionMap(
      Seq("selectedNow") -> true,
      Seq("removedNow") -> false)
    val unauthoredSelectedMap = versionMap(
      Seq("selectedNow") -> false,
      Seq("removedNow") -> false)

    val input = targetTableOf(schema)(
      Row(1, "selected-1", "removed-1", 10L, null, cdcMetadata(10L, emptyMap)),
      Row(1, null, null, 20L, null, cdcMetadata(20L, authoredSelectedMap)),
      Row(2, "selected-2", "removed-2", 10L, null, cdcMetadata(10L, emptyMap)),
      Row(2, null, null, 20L, null, cdcMetadata(20L, unauthoredSelectedMap))
    )

    checkAnswer(
      coalesce(input, selection),
      Seq(
        Row(1, "selected-1", "removed-1", 10L, null, cdcMetadata(10L, emptyMap)),
        // The newly selected leaf retains its previously recorded authored null.
        Row(1, null, null, 20L, null, cdcMetadata(20L, authoredSelectedMap)),
        Row(2, "selected-2", "removed-2", 10L, null, cdcMetadata(10L, emptyMap)),
        // The selected leaf inherits; the removed leaf is no longer reconciled.
        Row(2, "selected-2", null, 20L, null, cdcMetadata(20L, unauthoredSelectedMap))
      )
    )
  }

  test("schema-evolved leaves gain entries only when they inherit non-null values") {
    val selection = includeColumns("evolved")
    val schema = new StructType()
      .add("id", IntegerType)
      .add("evolved", StringType)
    val emptyMap = versionMap()
    val unauthoredMap = versionMap(Seq("evolved") -> false)

    val input = targetTableOf(schema)(
      Row(1, "source", 10L, null, cdcMetadata(10L, emptyMap)),
      // No entry models a leaf added after this row's non-null map was established.
      Row(1, null, 20L, null, cdcMetadata(20L, emptyMap)),
      // Without a preceding value, the absent entry remains the sparse unauthored signal.
      Row(2, null, 20L, null, cdcMetadata(20L, emptyMap))
    )

    checkAnswer(
      coalesce(input, selection),
      Seq(
        Row(1, "source", 10L, null, cdcMetadata(10L, emptyMap)),
        Row(1, "source", 20L, null, cdcMetadata(20L, unauthoredMap)),
        Row(2, null, 20L, null, cdcMetadata(20L, emptyMap))
      )
    )
  }

  test("each post-decomposition boundary resets inheritance") {
    val selection = includeColumns("value")
    val schema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
    val emptyMap = versionMap()
    val unauthoredMap = versionMap(Seq("value") -> false)

    val input = targetTableOf(schema)(
      // Key 1: a leading tombstone resets values inherited before the affected suffix.
      Row(1, null, 20L, 20L, cdcMetadata(20L, null)),
      Row(1, "stale", 10L, null, cdcMetadata(30L, unauthoredMap)),
      // Key 2: a decomposition tail represents the same kind of delete boundary.
      Row(2, null, null, 20L, cdcMetadata(null, null)),
      Row(2, "stale", 10L, null, cdcMetadata(30L, unauthoredMap)),
      // Key 3: a closed interval followed by a visibility gap also ends inheritance.
      Row(3, "before-gap", 10L, 20L, cdcMetadata(10L, emptyMap)),
      Row(3, "stale", 30L, null, cdcMetadata(30L, unauthoredMap))
    )

    checkAnswer(
      coalesce(input, selection),
      Seq(
        Row(1, null, 20L, 20L, cdcMetadata(20L, null)),
        Row(1, null, 10L, null, cdcMetadata(30L, unauthoredMap)),
        Row(2, null, null, 20L, cdcMetadata(null, null)),
        Row(2, null, 10L, null, cdcMetadata(30L, unauthoredMap)),
        Row(3, "before-gap", 10L, 20L, cdcMetadata(10L, emptyMap)),
        Row(3, null, 30L, null, cdcMetadata(30L, unauthoredMap))
      )
    )
  }

  test("an authored value restarts inheritance after a reset") {
    val selection = includeColumns("value")
    val schema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
    val emptyMap = versionMap()
    val unauthoredMap = versionMap(Seq("value") -> false)

    val input = targetTableOf(schema)(
      Row(1, "old", 10L, null, cdcMetadata(10L, emptyMap)),
      Row(1, null, 20L, 20L, cdcMetadata(20L, null)),
      Row(1, "new", 30L, null, cdcMetadata(30L, emptyMap)),
      Row(1, null, 40L, null, cdcMetadata(40L, unauthoredMap))
    )

    checkAnswer(
      coalesce(input, selection),
      Seq(
        Row(1, "old", 10L, null, cdcMetadata(10L, emptyMap)),
        Row(1, null, 20L, 20L, cdcMetadata(20L, null)),
        Row(1, "new", 30L, null, cdcMetadata(30L, emptyMap)),
        Row(1, "new", 40L, null, cdcMetadata(40L, unauthoredMap))
      )
    )
  }

  test("unselected data and framework columns retain their values and metadata") {
    val selection = includeColumns("selected")
    def commentMetadata(comment: String): Metadata =
      new MetadataBuilder().putString("comment", comment).build()

    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = true, commentMetadata("key")),
      StructField("selected", StringType, nullable = true, commentMetadata("selected")),
      StructField("unselected", StringType, nullable = true, commentMetadata("unselected"))
    ))
    val cdcColumnMetadata = commentMetadata("cdc")
    val emptyMap = versionMap()
    val unauthoredMap = versionMap(Seq("selected") -> false)
    val input = targetTableOf(schema, cdcColumnMetadata)(
      Row(1, "source", "keep-1", 10L, 20L, cdcMetadata(10L, emptyMap)),
      Row(1, null, "keep-2", 20L, 60L, cdcMetadata(20L, unauthoredMap))
    )

    val result = coalesce(input, selection)

    assert(result.schema == input.schema)
    checkAnswer(
      result,
      Seq(
        Row(1, "source", "keep-1", 10L, 20L, cdcMetadata(10L, emptyMap)),
        Row(1, "source", "keep-2", 20L, 60L, cdcMetadata(20L, unauthoredMap))
      )
    )
  }

  gridTest("column selection honors the configured resolver")(
    Seq(
      (false, "VALUE"),
      (true, "Value")
    )
  ) { case (caseSensitive, selectedColumnName) =>
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
      val selection = includeColumns(selectedColumnName)
      val schema = new StructType()
        .add("id", IntegerType)
        .add("Value", StringType)
      val emptyMap = versionMap()
      val unauthoredMap = versionMap(Seq("Value") -> false)
      val input = targetTableOf(schema)(
        Row(1, "source", 10L, null, cdcMetadata(10L, emptyMap)),
        Row(1, null, 20L, null, cdcMetadata(20L, unauthoredMap))
      )

      checkAnswer(
        coalesce(input, selection),
        Seq(
          Row(1, "source", 10L, null, cdcMetadata(10L, emptyMap)),
          Row(1, "source", 20L, null, cdcMetadata(20L, unauthoredMap))
        )
      )
    }
  }

  test("case-sensitive selection rejects mismatched column casing") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val selection = includeColumns("VALUE")
      val schema = new StructType()
        .add("id", IntegerType)
        .add("Value", StringType)
      val input = targetTableOf(schema)(
        Row(1, "source", 10L, null, cdcMetadata(10L, versionMap()))
      )

      val exception = try {
        coalesce(input, selection).collect()
        throw new IllegalStateException("Expected a case-sensitive column-resolution failure")
      } catch {
        case e: AnalysisException => e
      }
      assert(exception.getCondition == "AUTOCDC_COLUMNS_NOT_FOUND_IN_SCHEMA")
    }
  }

  test("coalescing is idempotent and a previously coalesced first row supplies carry-in") {
    val selection = includeColumns("value")
    val schema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
    val emptyMap = versionMap()
    val unauthoredMap = versionMap(Seq("value") -> false)
    val input = targetTableOf(schema)(
      Row(1, "source", 10L, null, cdcMetadata(10L, emptyMap)),
      Row(1, null, 20L, null, cdcMetadata(20L, unauthoredMap))
    )

    val firstPass = coalesce(input, selection)
    val firstPassRows = Seq(
      Row(1, "source", 10L, null, cdcMetadata(10L, emptyMap)),
      Row(1, "source", 20L, null, cdcMetadata(20L, unauthoredMap))
    )
    checkAnswer(firstPass, firstPassRows)
    checkAnswer(coalesce(firstPass, selection), firstPassRows)

    val affectedSuffix = firstPass.filter(
      F.col(AutoCdcReservedNames.cdcMetadataColName)
        .getField(Scd2BatchProcessor.recordStartAtFieldName) >= 20L)
    val incoming = targetTableOf(schema)(
      Row(1, null, 30L, null, cdcMetadata(30L, unauthoredMap))
    )

    checkAnswer(
      coalesce(affectedSuffix.unionByName(incoming), selection),
      Seq(
        Row(1, "source", 20L, null, cdcMetadata(20L, unauthoredMap)),
        Row(1, "source", 30L, null, cdcMetadata(30L, unauthoredMap))
      )
    )
  }

  test("incoming authored values replace carry-in without unauthored rows interrupting it") {
    val selection = includeColumns("value")
    val schema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
    val emptyMap = versionMap()
    val unauthoredMap = versionMap(Seq("value") -> false)
    val input = targetTableOf(schema)(
      // The affected suffix starts with an existing, previously coalesced value.
      Row(1, "old", 10L, null, cdcMetadata(10L, unauthoredMap)),
      Row(1, "new", 20L, null, cdcMetadata(20L, emptyMap)),
      // Neither a previously inherited non-null nor an unauthored null contributes a candidate.
      Row(1, "stale", 30L, null, cdcMetadata(30L, unauthoredMap)),
      Row(1, null, 40L, null, cdcMetadata(40L, unauthoredMap))
    )

    checkAnswer(
      coalesce(input, selection),
      Seq(
        Row(1, "old", 10L, null, cdcMetadata(10L, unauthoredMap)),
        Row(1, "new", 20L, null, cdcMetadata(20L, emptyMap)),
        Row(1, "new", 30L, null, cdcMetadata(30L, unauthoredMap)),
        Row(1, "new", 40L, null, cdcMetadata(40L, unauthoredMap))
      )
    )
  }

  test("an authored null replaces an older non-null inheritance candidate") {
    val selection = includeColumns("value")
    val schema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
    val emptyMap = versionMap()
    val authoredNullMap = versionMap(Seq("value") -> true)
    val unauthoredMap = versionMap(Seq("value") -> false)
    val input = targetTableOf(schema)(
      Row(1, "old", 10L, null, cdcMetadata(10L, emptyMap)),
      Row(1, null, 20L, null, cdcMetadata(20L, authoredNullMap)),
      Row(1, "stale", 30L, null, cdcMetadata(30L, unauthoredMap))
    )

    checkAnswer(
      coalesce(input, selection),
      Seq(
        Row(1, "old", 10L, null, cdcMetadata(10L, emptyMap)),
        Row(1, null, 20L, null, cdcMetadata(20L, authoredNullMap)),
        Row(1, null, 30L, null, cdcMetadata(30L, unauthoredMap))
      )
    )
  }

  test("each selected leaf maintains an independent inheritance chain") {
    val selection = includeColumns("left", "right")
    val schema = new StructType()
      .add("id", IntegerType)
      .add("left", StringType)
      .add("right", StringType)
    val emptyMap = versionMap()
    val leftUnauthoredMap = versionMap(Seq("left") -> false)
    val rightUnauthoredMap = versionMap(Seq("right") -> false)
    val input = targetTableOf(schema)(
      Row(1, "left-1", "right-1", 10L, null, cdcMetadata(10L, emptyMap)),
      Row(1, null, "right-2", 20L, null, cdcMetadata(20L, leftUnauthoredMap)),
      Row(1, "left-3", null, 30L, null, cdcMetadata(30L, rightUnauthoredMap))
    )

    checkAnswer(
      coalesce(input, selection),
      Seq(
        Row(1, "left-1", "right-1", 10L, null, cdcMetadata(10L, emptyMap)),
        Row(1, "left-1", "right-2", 20L, null, cdcMetadata(20L, leftUnauthoredMap)),
        Row(1, "left-3", "right-2", 30L, null, cdcMetadata(30L, rightUnauthoredMap))
      )
    )
  }

  test("nested leaves inherit without turning null-over-null parents into non-null structs") {
    // "Null over null" means both the stored parent struct and the value available to inherit
    // are null. The parent must stay null: rebuilding it would turn that null into a non-null
    // struct whose fields are all null, even though no non-null value was inherited.
    val selection = includeColumns("profile")
    val profileSchema = new StructType()
      .add("display.name", StringType)
      .add("age", IntegerType)
    val schema = new StructType()
      .add("id", IntegerType)
      .add("profile", profileSchema)
      .add("outside", StringType)
    val authoredAgeMap = versionMap(Seq("profile", "age") -> true)
    val authoredProfileMap = versionMap(
      Seq("profile", "display.name") -> true,
      Seq("profile", "age") -> true)
    val unauthoredProfileMap = versionMap(
      Seq("profile", "display.name") -> false,
      Seq("profile", "age") -> false)
    val input = targetTableOf(schema)(
      Row(1, Row("Alice", null), "keep-1", 10L, null, cdcMetadata(10L, authoredAgeMap)),
      Row(1, null, "keep-2", 20L, null, cdcMetadata(20L, unauthoredProfileMap)),
      Row(2, null, "keep-3", 10L, null, cdcMetadata(10L, authoredProfileMap)),
      Row(2, null, "keep-4", 20L, null, cdcMetadata(20L, unauthoredProfileMap))
    )

    checkAnswer(
      coalesce(input, selection),
      Seq(
        Row(1, Row("Alice", null), "keep-1", 10L, null,
          cdcMetadata(10L, authoredAgeMap)),
        Row(1, Row("Alice", null), "keep-2", 20L, null,
          cdcMetadata(20L, unauthoredProfileMap)),
        Row(2, null, "keep-3", 10L, null, cdcMetadata(10L, authoredProfileMap)),
        Row(2, null, "keep-4", 20L, null, cdcMetadata(20L, unauthoredProfileMap))
      )
    )
  }

  test("arrays and maps are inherited as opaque leaves") {
    val selection = includeColumns("tags", "properties")
    val schema = new StructType()
      .add("id", IntegerType)
      .add("tags", ArrayType(StringType))
      .add("properties", MapType(StringType, IntegerType))
    val emptyMap = versionMap()
    val unauthoredMap = versionMap(
      Seq("tags") -> false,
      Seq("properties") -> false)
    val input = targetTableOf(schema)(
      Row(1, Seq("one", "two"), Map("x" -> 1), 10L, null, cdcMetadata(10L, emptyMap)),
      Row(1, null, null, 20L, null, cdcMetadata(20L, unauthoredMap))
    )

    checkAnswer(
      coalesce(input, selection),
      Seq(
        Row(1, Seq("one", "two"), Map("x" -> 1), 10L, null,
          cdcMetadata(10L, emptyMap)),
        Row(1, Seq("one", "two"), Map("x" -> 1), 20L, null,
          cdcMetadata(20L, unauthoredMap))
      )
    )
  }

  test("inheritance follows chronological order independently for each key") {
    val selection = includeColumns("value")
    val schema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
    val emptyMap = versionMap()
    val unauthoredMap = versionMap(Seq("value") -> false)
    val input = targetTableOf(schema)(
      // Physical input order deliberately differs from chronological order and interleaves keys.
      Row(1, null, 20L, null, cdcMetadata(20L, unauthoredMap)),
      Row(2, "key-2", 5L, null, cdcMetadata(5L, emptyMap)),
      Row(1, "key-1", 10L, null, cdcMetadata(10L, emptyMap)),
      Row(2, null, 15L, null, cdcMetadata(15L, unauthoredMap))
    )

    checkAnswer(
      coalesce(input, selection),
      Seq(
        Row(1, "key-1", 10L, null, cdcMetadata(10L, emptyMap)),
        Row(1, "key-1", 20L, null, cdcMetadata(20L, unauthoredMap)),
        Row(2, "key-2", 5L, null, cdcMetadata(5L, emptyMap)),
        Row(2, "key-2", 15L, null, cdcMetadata(15L, unauthoredMap))
      )
    )
  }

  test("a selection resolving to no leaves initializes upserts but leaves deletes unchanged") {
    val selection = excludeColumns("value", "other")
    val schema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add("other", IntegerType)
    val authoredNullsMap = versionMap(
      Seq("value") -> true,
      Seq("other") -> true)
    val input = targetTableOf(schema)(
      Row(1, null, null, 10L, null, cdcMetadata(10L, null)),
      Row(2, null, null, 20L, 20L, cdcMetadata(20L, null))
    )

    checkAnswer(
      coalesce(input, selection),
      Seq(
        Row(1, null, null, 10L, null, cdcMetadata(10L, authoredNullsMap)),
        Row(2, null, null, 20L, 20L, cdcMetadata(20L, null))
      )
    )
  }
}
