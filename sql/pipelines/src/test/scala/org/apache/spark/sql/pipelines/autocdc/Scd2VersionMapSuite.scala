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

import org.apache.spark.{SparkException, SparkRuntimeException}
import org.apache.spark.sql.{functions => F, AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class Scd2VersionMapSuite extends QueryTest with SharedSparkSession {

  private def resolver = spark.sessionState.conf.resolver

  // ---- Schema helpers ----

  private val flatSchema = new StructType()
    .add("a", IntegerType)
    .add("b", StringType)
    .add("c", DoubleType)

  private val nestedSchema = new StructType()
    .add("x", IntegerType)
    .add("address", new StructType()
      .add("city", StringType)
      .add("zip", IntegerType))

  private val deeplyNestedSchema = new StructType()
    .add("top", new StructType()
      .add("mid", new StructType()
        .add("leaf", StringType)))

  private val arrayAndMapSchema = new StructType()
    .add("tags", ArrayType(StringType))
    .add("props", MapType(StringType, IntegerType))
    .add("plain", IntegerType)

  // Schemas with special-char leaves nested inside a normal top-level struct, so that
  // ColumnSelection (which operates on top-level field names) can still select them.
  private val specialCharSchema = new StructType()
    .add("normal", IntegerType)
    .add("wrapper", new StructType()
      .add("has space", StringType))

  private val periodInNameSchema = new StructType()
    .add("wrapper", new StructType()
      .add("a.b", IntegerType))
    .add("c", StringType)

  private val hyphenInNameSchema = new StructType()
    .add("wrapper", new StructType()
      .add("col-one", IntegerType))
    .add("col_two", StringType)

  // ---- Row helper ----

  private def singleRow(schema: StructType)(values: Any*) =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row.fromSeq(values))), schema)

  private def encodedPath(path: String*): String = QuotingUtils.quoteNameParts(path)

  private val operationSchema = new StructType()
    .add("caseName", StringType, nullable = false)
    .add("versionMap", Scd2VersionMap.mapType, nullable = true)
    .add("currentValue", StringType, nullable = true)
    .add("valueToInherit", StringType, nullable = true)

  private case class VersionMapOperationInput(
      caseName: String,
      versionMap: Option[Map[String, Boolean]],
      currentValue: Option[String],
      valueToInherit: Option[String] = None) {

    def toRow: Row = Row(
      caseName,
      versionMap.orNull,
      currentValue.orNull,
      valueToInherit.orNull)
  }

  private def operationRows(inputs: VersionMapOperationInput*) =
    spark.createDataFrame(
      spark.sparkContext.parallelize(inputs.map(_.toRow)),
      operationSchema)

  // =========================================================================
  // mapType
  // =========================================================================

  test("mapType is Map(String, Boolean)") {
    assert(Scd2VersionMap.mapType ===
      MapType(StringType, BooleanType, valueContainsNull = false))
  }

  // =========================================================================
  // buildVersionMap: version map contract cases
  // =========================================================================

  // Contract case 1: null in event + not part of ignore-null -> (column, true) = authored null
  test("contract case 1 - null leaf not in ignore-null selection is authored (true)") {
    val df = singleRow(flatSchema)(null, "hello", 2.0)
    val selection = ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("b")))
    val result = df.select(
      Scd2VersionMap.buildVersionMap(flatSchema, selection, resolver).as("vm"))
    // a is null + not in ignore-null -> true (authored null)
    checkAnswer(result, Row(Map(encodedPath("a") -> true)))
  }

  // Contract case 2: null in event + part of ignore-null -> false because event did not author null
  test("contract case 2 - event leaves null leaf unauthored under ignore-null (false)") {
    val df = singleRow(flatSchema)(null, "hello", 2.0)
    val selection = ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("a")))
    val result = df.select(
      Scd2VersionMap.buildVersionMap(flatSchema, selection, resolver).as("vm"))
    // a is null + in ignore-null -> false (the upsert event left a unauthored)
    checkAnswer(result, Row(Map(encodedPath("a") -> false)))
  }

  // Contract case 3: column not in the event schema (schema evolution later adds it with null).
  // At ingest time this manifests as no entry for the column. We verify absence by constructing
  // a narrower schema that omits the column.
  test("contract case 3 - column absent from schema has no entry (schema evolution)") {
    // Simulate ingest with a narrower schema that does not yet include column "b".
    val narrowSchema = new StructType().add("a", IntegerType)
    val df = singleRow(narrowSchema)(null)
    val selection = ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("a")))
    val result = df.select(
      Scd2VersionMap.buildVersionMap(narrowSchema, selection, resolver).as("vm"))
    // Only "a" appears (the upsert event left it unauthored); a future schema-evolved "b" has no
    // entry.
    checkAnswer(result, Row(Map(encodedPath("a") -> false)))
  }

  // Non-null values are always considered authored and produce no entry.
  test("contract - non-null values produce no entry regardless of ignore-null membership") {
    val df = singleRow(flatSchema)(1, "hello", 2.0)
    val selection = ColumnSelection.IncludeColumns(
      Seq(UnqualifiedColumnName("a"), UnqualifiedColumnName("b"), UnqualifiedColumnName("c")))
    val result = df.select(
      Scd2VersionMap.buildVersionMap(flatSchema, selection, resolver).as("vm"))
    checkAnswer(result, Row(Map.empty[String, Boolean]))
  }

  // =========================================================================
  // buildVersionMap: flat schema variations
  // =========================================================================

  test("flat schema - all null, none in ignore-null -> all authored (true)") {
    val df = singleRow(flatSchema)(null, null, null)
    val selection = ColumnSelection.ExcludeColumns(
      Seq(UnqualifiedColumnName("a"), UnqualifiedColumnName("b"), UnqualifiedColumnName("c")))
    val result = df.select(
      Scd2VersionMap.buildVersionMap(flatSchema, selection, resolver).as("vm"))
    checkAnswer(result, Row(Map(
      encodedPath("a") -> true,
      encodedPath("b") -> true,
      encodedPath("c") -> true)))
  }

  test("flat schema - all null, all in ignore-null -> event leaves all unauthored (false)") {
    val df = singleRow(flatSchema)(null, null, null)
    val selection = ColumnSelection.IncludeColumns(
      Seq(UnqualifiedColumnName("a"), UnqualifiedColumnName("b"), UnqualifiedColumnName("c")))
    val result = df.select(
      Scd2VersionMap.buildVersionMap(flatSchema, selection, resolver).as("vm"))
    checkAnswer(result, Row(Map(
      encodedPath("a") -> false,
      encodedPath("b") -> false,
      encodedPath("c") -> false)))
  }

  test("flat schema - mixed nulls with partial ignore-null") {
    // a=null (ignore-null), b="hi" (non-null), c=null (not ignore-null)
    val df = singleRow(flatSchema)(null, "hi", null)
    val selection = ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("a")))
    val result = df.select(
      Scd2VersionMap.buildVersionMap(flatSchema, selection, resolver).as("vm"))
    checkAnswer(result, Row(Map(encodedPath("a") -> false, encodedPath("c") -> true)))
  }

  test("flat schema - multiple rows produce independent per-row maps") {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, null, 3.0),
        Row(null, "x", null))),
      flatSchema)
    val selection = ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("b")))
    val result = df.select(
      Scd2VersionMap.buildVersionMap(flatSchema, selection, resolver).as("vm"))
    checkAnswer(result, Seq(
      Row(Map(encodedPath("b") -> false)), // b=null + ignore-null
      // a,c=null + not ignore-null
      Row(Map(
        encodedPath("a") -> true,
        encodedPath("c") -> true))))
  }

  // =========================================================================
  // buildVersionMap: nested schemas
  // =========================================================================

  test("nested schema - null nested leaf tracked with multipart key") {
    // x=1, address.city=null, address.zip=100
    // ColumnSelection operates on top-level fields; include "address" struct.
    val df = singleRow(nestedSchema)(1, Row(null, 100))
    val selection = ColumnSelection.IncludeColumns(
      Seq(UnqualifiedColumnName("address")))
    val result = df.select(
      Scd2VersionMap.buildVersionMap(nestedSchema, selection, resolver).as("vm"))
    // address.city is null + ignore-null leaf -> false
    // address.zip is null-checked but non-null -> no entry
    checkAnswer(result, Row(Map(encodedPath("address", "city") -> false)))
  }

  test("nested schema - entire struct null makes all nested leaves null") {
    val df = singleRow(nestedSchema)(null, null)
    // Include "address" in ignore-null; "x" is not included.
    val selection = ColumnSelection.IncludeColumns(
      Seq(UnqualifiedColumnName("address")))
    val result = df.select(
      Scd2VersionMap.buildVersionMap(nestedSchema, selection, resolver).as("vm"))
    checkAnswer(result, Row(Map(
      encodedPath("x") -> true, // null + not ignore-null -> authored
      encodedPath("address", "city") -> false, // event left null unauthored under ignore-null
      encodedPath("address", "zip") -> false))) // event left null unauthored under ignore-null
  }

  test("deeply nested schema - three-level path tracked correctly") {
    val df = singleRow(deeplyNestedSchema)(Row(Row(null)))
    // Include the top-level "top" struct -> all leaves under it are ignore-null.
    val selection = ColumnSelection.IncludeColumns(
      Seq(UnqualifiedColumnName("top")))
    val result = df.select(
      Scd2VersionMap.buildVersionMap(deeplyNestedSchema, selection, resolver).as("vm"))
    checkAnswer(result, Row(Map(encodedPath("top", "mid", "leaf") -> false)))
  }

  // =========================================================================
  // buildVersionMap: arrays and maps treated as opaque leaves
  // =========================================================================

  test("array and map columns are tracked as opaque leaves") {
    // tags=null, props=null, plain=null
    val df = singleRow(arrayAndMapSchema)(null, null, null)
    val selection = ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("tags")))
    val result = df.select(
      Scd2VersionMap.buildVersionMap(arrayAndMapSchema, selection, resolver).as("vm"))
    checkAnswer(result, Row(Map(
      encodedPath("tags") -> false, // event left null unauthored under ignore-null
      encodedPath("props") -> true, // null + not ignore-null -> authored
      encodedPath("plain") -> true))) // null + not ignore-null -> authored
  }

  // =========================================================================
  // buildVersionMap: special character column names
  // =========================================================================

  test("column name with space is encoded in version map key") {
    // normal=null, wrapper."has space"=null
    val df = singleRow(specialCharSchema)(null, Row(null))
    // Include "wrapper" struct -> its leaf "has space" is ignore-null.
    val selection = ColumnSelection.IncludeColumns(
      Seq(UnqualifiedColumnName("wrapper")))
    val result = df.select(
      Scd2VersionMap.buildVersionMap(
        specialCharSchema, selection, resolver).as("vm"))
    checkAnswer(result, Row(Map(
      encodedPath("normal") -> true,
      encodedPath("wrapper", "has space") -> false)))
  }

  test("column name with period is encoded in version map key") {
    // wrapper."a.b"=null, c=null
    val df = singleRow(periodInNameSchema)(Row(null), null)
    val selection = ColumnSelection.IncludeColumns(
      Seq(UnqualifiedColumnName("wrapper")))
    val result = df.select(
      Scd2VersionMap.buildVersionMap(
        periodInNameSchema, selection, resolver).as("vm"))
    checkAnswer(result, Row(Map(
      encodedPath("wrapper", "a.b") -> false,
      encodedPath("c") -> true)))
  }

  test("column name with hyphen is encoded in version map key") {
    // wrapper."col-one"=null, col_two=null
    val df = singleRow(hyphenInNameSchema)(Row(null), null)
    val selection = ColumnSelection.IncludeColumns(
      Seq(UnqualifiedColumnName("wrapper")))
    val result = df.select(
      Scd2VersionMap.buildVersionMap(
        hyphenInNameSchema, selection, resolver).as("vm"))
    checkAnswer(result, Row(Map(
      encodedPath("wrapper", "col-one") -> false,
      encodedPath("col_two") -> true)))
  }

  // Version map keys are persisted in target tables and must remain stable across Spark releases.
  // This regression test guards against changes to QuotingUtils inadvertently breaking that format.
  test("version map keys have a stable persisted serialization") {
    val keyPathsAndExpectedSerialization = Seq(
      Seq("simple") -> "`simple`",
      Seq("a.b") -> "`a.b`",
      Seq("a", "b") -> "`a`.`b`",
      Seq("wrapper", "leaf") -> "`wrapper`.`leaf`",
      Seq("wrapper", "a.b") -> "`wrapper`.`a.b`",
      Seq("wrapper", "has space") -> "`wrapper`.`has space`",
      Seq("wrapper", "col-one") -> "`wrapper`.`col-one`",
      Seq("wrapper", "back`tick") -> "`wrapper`.`back``tick`",
      Seq("wrapper", "`already quoted`") -> "`wrapper`.```already quoted```",
      Seq("wrapper", "single'quote") -> "`wrapper`.`single'quote`",
      Seq("wrapper", "double\"quote") -> "`wrapper`.`double\"quote`",
      Seq("wrapper", "back\\slash") -> "`wrapper`.`back\\slash`",
      Seq("wrapper", "") -> "`wrapper`.``",
      Seq("wrapper", "null" + 0.toChar + "byte") ->
        ("`wrapper`.`null" + 0.toChar + "byte`"))
    // Extract wrapper's leaf names for the nested schema; "wrapper" remains in the persisted key.
    val structLeafs = keyPathsAndExpectedSerialization.collect {
      case (Seq("wrapper", name), _) => name
    }
    val schema = new StructType()
      .add("simple", StringType)
      .add("a.b", StringType)
      .add("a", new StructType().add("b", StringType))
      .add("wrapper", StructType(structLeafs.map(StructField(_, StringType))))
    val df = singleRow(schema)(
      null, null, Row(null), Row.fromSeq(Seq.fill[Any](structLeafs.size)(null)))

    // Make every column an ignore-null column, so all leafs with a null value get persisted in the
    // version map.
    val selection = ColumnSelection.ExcludeColumns(Seq.empty)

    val result = df.select(
      Scd2VersionMap.buildVersionMap(schema, selection, resolver).as("vm"))

    val expectedVersionMap = keyPathsAndExpectedSerialization.map {
      // Since we populated every column with nulls and every column is included in the ignore-null
      // selection, every leaf should appear in the constructed version map with the expected name
      // serialization.
      case (_, serializedKey) => serializedKey -> false
    }.toMap
    checkAnswer(result, Row(expectedVersionMap))
  }

  test("special-character path parts round-trip through version map keys") {
    val specialNames =
      Seq("a.b", "has space", "back`tick", "quote\"", "back\\slash", "null" + 0.toChar + "byte")
    val schema = new StructType()
      .add("wrapper", StructType(specialNames.map(StructField(_, StringType))))
    val df = singleRow(schema)(Row.fromSeq(Seq.fill[Any](specialNames.size)(null)))
    val selection = ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("wrapper")))

    val encodedKeys = df
      .select(F.map_keys(Scd2VersionMap.buildVersionMap(schema, selection, resolver)))
      .head()
      .getSeq[String](0)
    val decodedPaths = encodedKeys.map(key => CatalystSqlParser.parseMultipartIdentifier(key))

    val expectedPaths = specialNames.map(name => Seq("wrapper", name)).toSet
    assert(decodedPaths.toSet === expectedPaths)
  }

  // =========================================================================
  // buildVersionMap: empty schema
  // =========================================================================

  test("empty schema produces an empty version map") {
    val emptySchema = new StructType()
    val df = singleRow(emptySchema)()
    val selection = ColumnSelection.ExcludeColumns(Seq.empty)
    val result = df.select(
      Scd2VersionMap.buildVersionMap(emptySchema, selection, resolver).as("vm"))
    checkAnswer(result, Row(Map.empty[String, Boolean]))
  }

  // =========================================================================
  // buildVersionMap: ExcludeColumns variations
  // =========================================================================

  test("ExcludeColumns - excluded column is NOT ignore-null, others are") {
    val df = singleRow(flatSchema)(null, null, null)
    // ExcludeColumns("b") means ignore-null = everything except b.
    val selection = ColumnSelection.ExcludeColumns(Seq(UnqualifiedColumnName("b")))
    val result = df.select(
      Scd2VersionMap.buildVersionMap(flatSchema, selection, resolver).as("vm"))
    // The event left a,c unauthored under ignore-null (false), but authored b's null (true).
    checkAnswer(result, Row(Map(
      encodedPath("a") -> false,
      encodedPath("b") -> true,
      encodedPath("c") -> false)))
  }

  test("ExcludeColumns - empty exclude list -> ignore-null covers all columns") {
    val df = singleRow(flatSchema)(null, null, null)
    val selection = ColumnSelection.ExcludeColumns(Seq.empty)
    val result = df.select(
      Scd2VersionMap.buildVersionMap(flatSchema, selection, resolver).as("vm"))
    // Exclude nothing -> ignore-null applies to all columns -> event leaves all nulls unauthored.
    checkAnswer(result, Row(Map(
      encodedPath("a") -> false,
      encodedPath("b") -> false,
      encodedPath("c") -> false)))
  }


  // =========================================================================
  // buildVersionMap: case sensitivity
  // =========================================================================

  test("case-insensitive resolver matches ignore-null columns regardless of case") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val df = singleRow(flatSchema)(null, null, null)
      // Selection uses uppercase "A" but schema has lowercase "a".
      val selection = ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("A")))
      val caseInsensitiveResolver = spark.sessionState.conf.resolver
      val result = df.select(
        Scd2VersionMap.buildVersionMap(
          flatSchema, selection, caseInsensitiveResolver).as("vm"))
      checkAnswer(result, Row(Map(
        encodedPath("a") -> false,
        encodedPath("b") -> true,
        encodedPath("c") -> true)))
    }
  }

  test("case-sensitive resolver does not match differently-cased column names") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val caseSensitiveResolver = spark.sessionState.conf.resolver
      val df = singleRow(flatSchema)(null, null, null)
      // Selection uses uppercase "A" but schema has lowercase "a" -> "A" is not found.
      val selection =
        ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("A")))
      checkError(
        exception = intercept[AnalysisException] {
          df.select(
            Scd2VersionMap.buildVersionMap(
              flatSchema, selection, caseSensitiveResolver).as("vm")
          ).collect()
        },
        condition = "AUTOCDC_COLUMNS_NOT_FOUND_IN_SCHEMA",
        sqlState = "42703",
        parameters = Map(
          "caseSensitivity" -> "case-sensitive",
          "schemaName" -> "ignoreNullSelection",
          "missingColumns" -> "A",
          "availableColumns" -> "a, b, c"
        )
      )
    }
  }

  test("case-sensitive resolver matches exact-case column names") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val caseSensitiveResolver = spark.sessionState.conf.resolver
      val df = singleRow(flatSchema)(null, null, null)
      val selection = ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("a")))
      val result = df.select(
        Scd2VersionMap.buildVersionMap(
          flatSchema, selection, caseSensitiveResolver).as("vm"))
      checkAnswer(result, Row(Map(
        encodedPath("a") -> false,
        encodedPath("b") -> true,
        encodedPath("c") -> true)))
    }
  }

  // =========================================================================
  // Version map authorship operations
  // =========================================================================

  test("resolveIgnoreNullLeafPaths returns canonical leaves in schema order") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("Profile", new StructType()
        .add("Display Name", StringType)
        .add("Contact", new StructType().add("e.mail", StringType)))
      .add("Events", ArrayType(new StructType().add("kind", StringType)))
      .add("Lookup", MapType(StringType, new StructType().add("value", StringType)))

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val includeSelection = ColumnSelection.IncludeColumns(Seq(
        UnqualifiedColumnName("lookup"),
        UnqualifiedColumnName("profile"),
        UnqualifiedColumnName("events")))
      assert(Scd2VersionMap.resolveIgnoreNullLeafPaths(
        schema, includeSelection, resolver) === Seq(
        Seq("Profile", "Display Name"),
        Seq("Profile", "Contact", "e.mail"),
        Seq("Events"),
        Seq("Lookup")))

      val excludeSelection =
        ColumnSelection.ExcludeColumns(Seq(UnqualifiedColumnName("profile")))
      assert(Scd2VersionMap.resolveIgnoreNullLeafPaths(
        schema, excludeSelection, resolver) === Seq(
        Seq("id"),
        Seq("Events"),
        Seq("Lookup")))
    }
  }

  test("resolveIgnoreNullLeafPaths respects case sensitivity") {
    val schema = new StructType()
      .add("Profile", new StructType().add("Name", StringType))

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val exactSelection =
        ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("Profile")))
      assert(Scd2VersionMap.resolveIgnoreNullLeafPaths(
        schema, exactSelection, resolver) === Seq(Seq("Profile", "Name")))

      val mismatchedSelection =
        ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("profile")))
      intercept[AnalysisException] {
        Scd2VersionMap.resolveIgnoreNullLeafPaths(schema, mismatchedSelection, resolver)
      }
    }
  }

  test("resolveIgnoreNullLeafPaths preserves raw special-character names") {
    val schema = new StructType()
      .add("wrapper.with.dot", new StructType()
        .add("back`tick", StringType)
        .add("space name", StringType))
    val selection = ColumnSelection.IncludeColumns(Seq(
      UnqualifiedColumnName(QuotingUtils.quoteIdentifier("wrapper.with.dot"))))

    assert(Scd2VersionMap.resolveIgnoreNullLeafPaths(
      schema, selection, resolver) === Seq(
      Seq("wrapper.with.dot", "back`tick"),
      Seq("wrapper.with.dot", "space name")))
  }

  test("entryValue distinguishes values from absent entries") {
    val path = Seq("wrapper", "a.b")
    val key = encodedPath(path: _*)
    val df = operationRows(
      VersionMapOperationInput("authored", Some(Map(key -> true)), None),
      VersionMapOperationInput("unauthored", Some(Map(key -> false)), None),
      VersionMapOperationInput("absent entry", Some(Map.empty), None),
      VersionMapOperationInput("absent map", None, None))

    checkAnswer(
      df.select(
        F.col("caseName"),
        Scd2VersionMap.entryValue(F.col("versionMap"), path).as("entry")),
      Seq(
        Row("authored", true),
        Row("unauthored", false),
        Row("absent entry", null),
        Row("absent map", null)))
  }

  test("entryValue requires the exact raw path spelling") {
    val path = Seq("Wrapper", "a.b")
    val df = operationRows(
      VersionMapOperationInput(
        "case-sensitive key",
        Some(Map(encodedPath(path: _*) -> false)),
        None))

    checkAnswer(
      df.select(
        Scd2VersionMap.entryValue(F.col("versionMap"), path).as("exact"),
        Scd2VersionMap.entryValue(
          F.col("versionMap"), Seq("wrapper", "a.b")).as("differentCase")),
      Row(false, null))
  }

  test("buildVersionMapEntry serializes paths and authorship") {
    val path = Seq("outer.with.dot", "back`tick")
    val result = spark.range(1).select(
      Scd2VersionMap.buildVersionMapEntry(path, authored = true).as("authored"),
      Scd2VersionMap.buildVersionMapEntry(path, authored = false).as("unauthored"))

    checkAnswer(
      result,
      Row(
        Row(encodedPath(path: _*), true),
        Row(encodedPath(path: _*), false)))
  }

  test("buildVersionMapEntry round-trips through entryValue") {
    val path = Seq("outer.with.dot", "back`tick")
    val versionMap = F.map_from_entries(F.array(
      Scd2VersionMap.buildVersionMapEntry(path, authored = false)))
    val result = spark.range(1)
      .select(versionMap.as("versionMap"))
      .select(Scd2VersionMap.entryValue(F.col("versionMap"), path).as("entry"))

    checkAnswer(result, Row(false))
  }

  test("validateAuthoredNullEntry accepts valid pairings") {
    val schema = new StructType()
      .add("caseName", StringType, nullable = false)
      .add("authorshipEntry", BooleanType, nullable = true)
      .add("currentValue", StringType, nullable = true)
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("authored null", true, null),
        Row("unauthored null", false, null),
        Row("unauthored non-null", false, "value"),
        Row("absent entry", null, "value"))),
      schema)

    checkAnswer(
      df.select(
        F.col("caseName"),
        Scd2VersionMap.validateAuthoredNullEntry(
          F.col("authorshipEntry"),
          F.col("currentValue"),
          Seq("value")).as("valid")),
      Seq(
        Row("authored null", true),
        Row("unauthored null", true),
        Row("unauthored non-null", true),
        Row("absent entry", true)))
  }

  test("validateAuthoredNullEntry rejects a non-null authored null") {
    val schema = new StructType()
      .add("authorshipEntry", BooleanType, nullable = false)
      .add("currentValue", StringType, nullable = false)
    val df = singleRow(schema)(true, "value")

    val wrapper = intercept[SparkException] {
      df.select(Scd2VersionMap.validateAuthoredNullEntry(
        F.col("authorshipEntry"),
        F.col("currentValue"),
        Seq("value"))).collect()
    }
    assert(wrapper.getCause.isInstanceOf[SparkRuntimeException],
      s"Expected SparkRuntimeException cause, got ${wrapper.getCause}")
    checkError(
      exception = wrapper.getCause.asInstanceOf[SparkRuntimeException],
      condition = "INTERNAL_ERROR",
      parameters = Map(
        "message" -> ("Version map entry is true (authored null) " +
          "but stored value is non-null for column `value`")))
  }

  test("isAuthored covers the version map contract truth table") {
    val path = Seq("value")
    val key = encodedPath(path: _*)
    val df = operationRows(
      VersionMapOperationInput("null map and null value", None, None),
      VersionMapOperationInput("null map and non-null value", None, Some("value")),
      VersionMapOperationInput("authored null", Some(Map(key -> true)), None),
      VersionMapOperationInput("unauthored null", Some(Map(key -> false)), None),
      VersionMapOperationInput("schema-evolved null", Some(Map.empty), None),
      VersionMapOperationInput("authored non-null", Some(Map.empty), Some("value")),
      VersionMapOperationInput(
        "inherited non-null", Some(Map(key -> false)), Some("value")))

    checkAnswer(
      df.select(
        F.col("caseName"),
        Scd2VersionMap.isAuthored(
          F.col("versionMap"),
          F.col("currentValue"),
          path).as("authored")),
      Seq(
        Row("null map and null value", true),
        Row("null map and non-null value", true),
        Row("authored null", true),
        Row("unauthored null", false),
        Row("schema-evolved null", false),
        Row("authored non-null", true),
        Row("inherited non-null", false)))
  }

  test("isAuthored validates authored-null entries") {
    val path = Seq("value")
    val df = operationRows(
      VersionMapOperationInput(
        "invalid authored null",
        Some(Map(encodedPath(path: _*) -> true)),
        Some("value")))

    val wrapper = intercept[SparkException] {
      df.select(Scd2VersionMap.isAuthored(
        F.col("versionMap"),
        F.col("currentValue"),
        path)).collect()
    }
    assert(wrapper.getCause.isInstanceOf[SparkRuntimeException],
      s"Expected SparkRuntimeException cause, got ${wrapper.getCause}")
    assert(wrapper.getCause.asInstanceOf[SparkRuntimeException].getCondition == "INTERNAL_ERROR")
  }

  test("isAuthored interprets maps built from ignore-null selections") {
    val schema = new StructType()
      .add("included", StringType, nullable = true)
      .add("excluded", StringType, nullable = true)
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(null, null),
        Row("included value", "excluded value"))),
      schema)
    val selection =
      ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("included")))
    val withVersionMap =
      df.withColumn("versionMap", Scd2VersionMap.buildVersionMap(schema, selection, resolver))

    checkAnswer(
      withVersionMap.select(
        F.col("included"),
        Scd2VersionMap.isAuthored(
          F.col("versionMap"), F.col("included"), Seq("included")).as("includedAuthored"),
        F.col("excluded"),
        Scd2VersionMap.isAuthored(
          F.col("versionMap"), F.col("excluded"), Seq("excluded")).as("excludedAuthored")),
      Seq(
        Row(null, false, null, true),
        Row("included value", true, "excluded value", true)))
  }

  test("needsSchemaEvolutionEntry requires every condition") {
    val path = Seq("value")
    val key = encodedPath(path: _*)
    val df = operationRows(
      VersionMapOperationInput(
        "schema evolution with value", Some(Map.empty), None, Some("previous")),
      VersionMapOperationInput("ignore null off", None, None, Some("previous")),
      VersionMapOperationInput(
        "current value non-null", Some(Map.empty), Some("current"), Some("previous")),
      VersionMapOperationInput(
        "authored entry exists", Some(Map(key -> true)), None, Some("previous")),
      VersionMapOperationInput(
        "unauthored entry exists", Some(Map(key -> false)), None, Some("previous")),
      VersionMapOperationInput("nothing to inherit", Some(Map.empty), None))

    checkAnswer(
      df.select(
        F.col("caseName"),
        Scd2VersionMap.needsSchemaEvolutionEntry(
          F.col("versionMap"),
          F.col("currentValue"),
          path,
          F.col("valueToInherit")).as("needsEntry")),
      Seq(
        Row("schema evolution with value", true),
        Row("ignore null off", false),
        Row("current value non-null", false),
        Row("authored entry exists", false),
        Row("unauthored entry exists", false),
        Row("nothing to inherit", false)))
  }
}
