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

import org.json4s.JsonAST.{JArray, JString}
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.sql.{functions => F, QueryTest, Row}
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

  private def encodedPath(path: String*): String = Scd2VersionMap.encodePath(path)

  // =========================================================================
  // extractLeafPaths
  // =========================================================================

  test("extractLeafPaths - flat columns produce single-element paths") {
    assert(Scd2VersionMap.extractLeafPaths(flatSchema) ===
      Seq(Seq("a"), Seq("b"), Seq("c")))
  }

  test("extractLeafPaths - nested struct produces only leaf paths, not intermediaries") {
    assert(Scd2VersionMap.extractLeafPaths(nestedSchema) ===
      Seq(Seq("x"), Seq("address", "city"), Seq("address", "zip")))
  }

  test("extractLeafPaths - deeply nested struct produces full multi-part paths") {
    assert(Scd2VersionMap.extractLeafPaths(deeplyNestedSchema) ===
      Seq(Seq("top", "mid", "leaf")))
  }

  test("extractLeafPaths - arrays and maps are opaque leaves") {
    assert(Scd2VersionMap.extractLeafPaths(arrayAndMapSchema) ===
      Seq(Seq("tags"), Seq("props"), Seq("plain")))
  }

  test("extractLeafPaths - empty schema produces empty seq") {
    assert(Scd2VersionMap.extractLeafPaths(new StructType()) === Seq.empty)
  }

  // =========================================================================
  // encodePath
  // =========================================================================

  test("encodePath - writes name parts as a compact JSON array") {
    assert(Scd2VersionMap.encodePath(Seq("a")) === """["a"]""")
    assert(Scd2VersionMap.encodePath(Seq("address", "city")) ===
      """["address","city"]""")
  }

  test("encodePath - distinguishes nested paths from names containing periods") {
    assert(Scd2VersionMap.encodePath(Seq("a", "b")) === """["a","b"]""")
    assert(Scd2VersionMap.encodePath(Seq("a.b")) === """["a.b"]""")
  }

  test("encodePath - escapes JSON delimiters and control characters") {
    val encoded = Scd2VersionMap.encodePath(
      Seq("quote\"", "back\\slash", "null" + 0.toChar + "byte"))
    assert(encoded === "[\"quote\\\"\",\"back\\\\slash\",\"null\\" + "u0000byte\"]")
  }

  test("encodePath - round trips arbitrary name parts") {
    val path = Seq("", "a.b", "has space", "back`tick", "quote\"", "back\\slash")
    assert(parse(Scd2VersionMap.encodePath(path)) === JArray(path.map(JString(_)).toList))
  }

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

  // Contract case 2: null in event + part of ignore-null -> (column, false) = unauthored null
  test("contract case 2 - null leaf in ignore-null selection is declined (false)") {
    val df = singleRow(flatSchema)(null, "hello", 2.0)
    val selection = ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("a")))
    val result = df.select(
      Scd2VersionMap.buildVersionMap(flatSchema, selection, resolver).as("vm"))
    // a is null + in ignore-null -> false (declined)
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
    // Only "a" appears (declined); a future column "b" added by schema evolution has no entry.
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

  test("flat schema - all null, all in ignore-null -> all declined (false)") {
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
      encodedPath("address", "city") -> false, // null + ignore-null -> declined
      encodedPath("address", "zip") -> false))) // null + ignore-null -> declined
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
      encodedPath("tags") -> false, // null + ignore-null -> declined
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

  test("special-character path parts survive version map encoding") {
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
    val decodedPaths = encodedKeys.map { encodedKey =>
      parse(encodedKey) match {
        case JArray(parts) =>
          parts.map {
            case JString(part) => part
            case other => fail(s"Expected a JSON string path part, but found $other")
          }
        case other => fail(s"Expected a JSON array version map key, but found $other")
      }
    }

    val expectedPaths = specialNames.map(name => Seq("wrapper", name)).toSet
    assert(decodedPaths.map(_.toSeq).toSet === expectedPaths)
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
    // a,c are in ignore-null -> declined (false); b is NOT -> authored (true).
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
    // Exclude nothing -> ignore-null applies to all columns -> all nulls are declined.
    checkAnswer(result, Row(Map(
      encodedPath("a") -> false,
      encodedPath("b") -> false,
      encodedPath("c") -> false)))
  }

  // =========================================================================
  // buildVersionMap: IncludeColumns variations
  // =========================================================================

  test("IncludeColumns - empty include list means no columns are ignore-null") {
    val df = singleRow(flatSchema)(null, null, null)
    val selection = ColumnSelection.IncludeColumns(Seq.empty)
    val result = df.select(
      Scd2VersionMap.buildVersionMap(flatSchema, selection, resolver).as("vm"))
    // No columns included in ignore-null -> all nulls are authored.
    checkAnswer(result, Row(Map(
      encodedPath("a") -> true,
      encodedPath("b") -> true,
      encodedPath("c") -> true)))
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
      val selection = ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("A")))
      val e = intercept[Exception] {
        df.select(
          Scd2VersionMap.buildVersionMap(
            flatSchema, selection, caseSensitiveResolver).as("vm")).collect()
      }
      assert(e.getMessage.contains("A"))
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
}
