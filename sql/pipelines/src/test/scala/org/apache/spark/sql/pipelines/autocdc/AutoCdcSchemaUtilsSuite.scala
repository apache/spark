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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{caseInsensitiveResolution, caseSensitiveResolution}
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.types._

class AutoCdcSchemaUtilsSuite extends SparkFunSuite {

  test("excludeColumns removes named columns and preserves remaining field order") {
    val schema = new StructType()
      .add("first", IntegerType)
      .add("excluded1", StringType)
      .add("middle", StringType)
      .add("excluded2", StringType)
      .add("last", DoubleType)

    assert(AutoCdcSchemaUtils.excludeColumns(
      schema,
      Seq("excluded1", "excluded2"),
      caseSensitiveResolution).fieldNames.toSeq == Seq("first", "middle", "last"))
  }

  test("excludeColumns respects resolver case sensitivity") {
    val schema = new StructType()
      .add("Value", StringType)
      .add("value", StringType)
      .add("other", IntegerType)

    assert(AutoCdcSchemaUtils.excludeColumns(
      schema,
      Seq("value"),
      caseSensitiveResolution).fieldNames.toSeq == Seq("Value", "other"))
    assert(AutoCdcSchemaUtils.excludeColumns(
      schema,
      Seq("value"),
      caseInsensitiveResolution).fieldNames.toSeq == Seq("other"))
  }

  test("flattenStructFieldPaths returns single-element paths for flat columns") {
    val schema = new StructType()
      .add("a", IntegerType)
      .add("b", StringType)
      .add("c", DoubleType)

    assert(AutoCdcSchemaUtils.flattenStructFieldPaths(schema) ===
      Seq(Seq("a"), Seq("b"), Seq("c")))
  }

  test("flattenStructFieldPaths returns nested fields rather than intermediate structs") {
    val schema = new StructType()
      .add("x", IntegerType)
      .add("address", new StructType()
        .add("city", StringType)
        .add("zip", IntegerType))

    assert(AutoCdcSchemaUtils.flattenStructFieldPaths(schema) ===
      Seq(Seq("x"), Seq("address", "city"), Seq("address", "zip")))
  }

  test("flattenStructFieldPaths returns full paths for deeply nested structs") {
    val schema = new StructType()
      .add("top", new StructType()
        .add("mid", new StructType()
          .add("leaf", StringType)))

    assert(AutoCdcSchemaUtils.flattenStructFieldPaths(schema) ===
      Seq(Seq("top", "mid", "leaf")))
  }

  test("flattenStructFieldPaths does not traverse arrays or maps") {
    val schema = new StructType()
      .add("tags", ArrayType(StringType))
      .add("props", MapType(StringType, IntegerType))
      .add("plain", IntegerType)

    assert(AutoCdcSchemaUtils.flattenStructFieldPaths(schema) ===
      Seq(Seq("tags"), Seq("props"), Seq("plain")))
  }

  test("flattenStructFieldPaths returns an empty sequence for an empty schema") {
    assert(AutoCdcSchemaUtils.flattenStructFieldPaths(new StructType()) === Seq.empty)
  }

  test("resolveIgnoreNullLeafPaths returns canonical leaves in schema order") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("Profile", new StructType()
        .add("Display Name", StringType)
        .add("Contact", new StructType().add("e.mail", StringType)))
      .add("Events", ArrayType(new StructType().add("kind", StringType)))
      .add("Lookup", MapType(StringType, new StructType().add("value", StringType)))

    val includeSelection = ColumnSelection.IncludeColumns(Seq(
      UnqualifiedColumnName("lookup"),
      UnqualifiedColumnName("profile"),
      UnqualifiedColumnName("events")))
    assert(AutoCdcSchemaUtils.resolveIgnoreNullLeafPaths(
      schema, includeSelection, caseInsensitiveResolution) === Seq(
      Seq("Profile", "Display Name"),
      Seq("Profile", "Contact", "e.mail"),
      Seq("Events"),
      Seq("Lookup")))

    val excludeSelection =
      ColumnSelection.ExcludeColumns(Seq(UnqualifiedColumnName("profile")))
    assert(AutoCdcSchemaUtils.resolveIgnoreNullLeafPaths(
      schema, excludeSelection, caseInsensitiveResolution) === Seq(
      Seq("id"),
      Seq("Events"),
      Seq("Lookup")))
  }

  test("resolveIgnoreNullLeafPaths respects case sensitivity") {
    val schema = new StructType()
      .add("Profile", new StructType().add("Name", StringType))

    val exactSelection =
      ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("Profile")))
    assert(AutoCdcSchemaUtils.resolveIgnoreNullLeafPaths(
      schema, exactSelection, caseSensitiveResolution) === Seq(Seq("Profile", "Name")))

    val mismatchedSelection =
      ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("profile")))
    checkError(
      exception = intercept[AnalysisException] {
        AutoCdcSchemaUtils.resolveIgnoreNullLeafPaths(
          schema, mismatchedSelection, caseSensitiveResolution)
      },
      condition = "AUTOCDC_COLUMNS_NOT_FOUND_IN_SCHEMA",
      sqlState = "42703",
      parameters = Map(
        "caseSensitivity" -> "case-sensitive",
        "schemaName" -> "ignoreNullSelection",
        "missingColumns" -> "profile",
        "availableColumns" -> "Profile"
      )
    )
  }

  test("resolveIgnoreNullLeafPaths preserves raw special-character names") {
    val schema = new StructType()
      .add("wrapper.with.dot", new StructType()
        .add("back`tick", StringType)
        .add("space name", StringType))
    val selection = ColumnSelection.IncludeColumns(Seq(
      UnqualifiedColumnName(QuotingUtils.quoteIdentifier("wrapper.with.dot"))))

    assert(AutoCdcSchemaUtils.resolveIgnoreNullLeafPaths(
      schema, selection, caseInsensitiveResolution) === Seq(
      Seq("wrapper.with.dot", "back`tick"),
      Seq("wrapper.with.dot", "space name")))
  }
}
