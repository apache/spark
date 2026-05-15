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
import org.apache.spark.sql.{functions => F, AnalysisException, Row}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

class ChangeArgsSuite extends SparkFunSuite with SharedSparkSession {

  private val sourceSchema = new StructType()
    .add("id", IntegerType, nullable = false)
    .add("Name", StringType)
    .add("age", IntegerType)

  test("ColumnSelection None leaves schema unchanged") {
    assert(
      ColumnSelection.applyToSchema(
        schemaName = "test",
        schema = sourceSchema,
        columnSelection = None,
        ignoreCase = false
      ) == sourceSchema)
  }

  test("ColumnSelection IncludeColumns filters by exact name in schema order") {
    val filteredSchema = ColumnSelection.applyToSchema(
      schemaName = "test",
      schema = sourceSchema,
      columnSelection = Some(
        ColumnSelection.IncludeColumns(
          Seq(UnqualifiedColumnName("age"), UnqualifiedColumnName("Name"))
        )
      ),
      ignoreCase = false
    )

    assert(filteredSchema == new StructType()
      .add("Name", StringType)
      .add("age", IntegerType))
  }

  test("ColumnSelection ExcludeColumns filters by exact name") {
    val filteredSchema = ColumnSelection.applyToSchema(
      schemaName = "test",
      schema = sourceSchema,
      columnSelection = Some(
        ColumnSelection.ExcludeColumns(Seq(UnqualifiedColumnName("id")))
      ),
      ignoreCase = false
    )

    assert(filteredSchema == new StructType()
      .add("Name", StringType)
      .add("age", IntegerType))
  }

  test("ColumnSelection IncludeColumns fails for columns not present in schema") {
    checkError(
      exception = intercept[AnalysisException] {
        ColumnSelection.applyToSchema(
          schemaName = "test",
          schema = sourceSchema,
          // Under ignoreCase = false, "name" will not match the schema field "Name".
          columnSelection = Some(
            ColumnSelection.IncludeColumns(
              Seq(UnqualifiedColumnName("name"), UnqualifiedColumnName("missing"))
            )
          ),
          ignoreCase = false
        )
      },
      condition = "AUTOCDC_COLUMNS_NOT_FOUND_IN_SCHEMA",
      sqlState = "42703",
      parameters = Map(
        "caseSensitivity" -> CaseSensitivityLabels.CaseSensitive,
        "schemaName" -> "test",
        "missingColumns" -> "name, missing",
        "availableColumns" -> "id, Name, age"
      )
    )
  }

  test("ColumnSelection ExcludeColumns fails for columns not present in schema") {
    checkError(
      exception = intercept[AnalysisException] {
        ColumnSelection.applyToSchema(
          schemaName = "test",
          schema = sourceSchema,
          // Under ignoreCase = false, "NAME" will not match the schema field "Name".
          columnSelection = Some(
            ColumnSelection.ExcludeColumns(
              Seq(UnqualifiedColumnName("NAME"), UnqualifiedColumnName("missing"))
            )
          ),
          ignoreCase = false
        )
      },
      condition = "AUTOCDC_COLUMNS_NOT_FOUND_IN_SCHEMA",
      sqlState = "42703",
      parameters = Map(
        "caseSensitivity" -> CaseSensitivityLabels.CaseSensitive,
        "schemaName" -> "test",
        "missingColumns" -> "NAME, missing",
        "availableColumns" -> "id, Name, age"
      )
    )
  }

  test("ColumnSelection IncludeColumns matches case-insensitively under ignoreCase=true") {
    // "NAME" and "AGE" do not exactly match the schema fields "Name" and "age", but
    // ignoreCase = true folds both sides to lowercase before comparing.
    val filteredSchema = ColumnSelection.applyToSchema(
      schemaName = "test",
      schema = sourceSchema,
      columnSelection = Some(
        ColumnSelection.IncludeColumns(
          Seq(UnqualifiedColumnName("AGE"), UnqualifiedColumnName("NAME"))
        )
      ),
      ignoreCase = true
    )

    // The retained fields keep their original casing from the schema, not the user's input.
    assert(filteredSchema == new StructType()
      .add("Name", StringType)
      .add("age", IntegerType))
  }

  test("ColumnSelection deduplicates user-provided columns that normalize to the same name") {
    // Under ignoreCase = true, "name" and "NAME" both fold to "name" and refer to the same
    // schema field. The returned schema must include "Name" once, not twice. Output ordering
    // and casing follow the schema, not the user's input.
    val filteredSchema = ColumnSelection.applyToSchema(
      schemaName = "test",
      schema = sourceSchema,
      columnSelection = Some(
        ColumnSelection.IncludeColumns(
          Seq(UnqualifiedColumnName("name"), UnqualifiedColumnName("NAME"))
        )
      ),
      ignoreCase = true
    )

    assert(filteredSchema == new StructType().add("Name", StringType))
  }

  test("ColumnSelection ExcludeColumns matches case-insensitively under ignoreCase=true") {
    val filteredSchema = ColumnSelection.applyToSchema(
      schemaName = "test",
      schema = sourceSchema,
      columnSelection = Some(
        ColumnSelection.ExcludeColumns(Seq(UnqualifiedColumnName("name")))
      ),
      ignoreCase = true
    )

    assert(filteredSchema == new StructType()
      .add("id", IntegerType, nullable = false)
      .add("age", IntegerType))
  }

  test("ColumnSelection missing-column error under ignoreCase=true preserves user casing") {
    checkError(
      exception = intercept[AnalysisException] {
        ColumnSelection.applyToSchema(
          schemaName = "test",
          schema = sourceSchema,
          // "NAME" matches "Name" under ignoreCase=true, but "Missing" has no schema match.
          // The error message reports the user's original casing for the missing column and
          // the schema's original casing for the available columns.
          columnSelection = Some(
            ColumnSelection.IncludeColumns(
              Seq(UnqualifiedColumnName("NAME"), UnqualifiedColumnName("Missing"))
            )
          ),
          ignoreCase = true
        )
      },
      condition = "AUTOCDC_COLUMNS_NOT_FOUND_IN_SCHEMA",
      sqlState = "42703",
      parameters = Map(
        "caseSensitivity" -> CaseSensitivityLabels.CaseInsensitive,
        "schemaName" -> "test",
        "missingColumns" -> "Missing",
        "availableColumns" -> "id, Name, age"
      )
    )
  }

  test("UnqualifiedColumnName accepts a simple single-part identifier") {
    assert(UnqualifiedColumnName("col").name == "col")
  }

  test("UnqualifiedColumnName accepts a backtick-quoted name containing a literal dot") {
    // Backticks make the dot part of a single name part, so this passes validation. The
    // stored name is the parsed (unquoted) form so it matches the actual schema field name.
    assert(UnqualifiedColumnName("`a.b`").name == "a.b")
  }

  test("UnqualifiedColumnName accepts redundant backticks around a single-part name") {
    // Backticks around an already-single-part identifier are decorative; the parser strips them
    // so the stored name has no surrounding back-ticks.
    assert(UnqualifiedColumnName("`col`").name == "col")
  }

  test("UnqualifiedColumnName.quoted is safe to pass to functions.col for literal-dot names") {
    val schema = new StructType()
      .add("a.b", IntegerType)
      .add("c", IntegerType)

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, 2), Row(3, 4))),
      schema
    )

    val key = UnqualifiedColumnName("`a.b`")

    // Sanity-check: the unquoted `name` is not safe to pass to `functions.col`. The string is
    // re-parsed and the literal dot is interpreted as a nested-field path separator, so the
    // analyzer fails to resolve `a`.`b` against the available top-level columns.
    checkError(
      exception = intercept[AnalysisException] {
        df.select(F.col(key.name)).collect()
      },
      condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      sqlState = "42703",
      parameters = Map(
        "objectName" -> "`a`.`b`",
        "proposal" -> "`a.b`, `c`"
      ),
      context = ExpectedContext(
        fragment = "col",
        callSitePattern = ""
      )
    )

    // The `quoted` form wraps the name in back-ticks so the re-parser treats the whole thing
    // as a single identifier, resolving to the top-level "a.b" column.
    assert(df.select(F.col(key.quoted)).collect().toSeq == Seq(Row(1), Row(3)))
  }

  test("IncludeColumns correctly matches a backtick-quoted literal-dot column") {
    val schema = new StructType()
      .add("a.b", IntegerType)
      .add("c", StringType)

    // The user writes `a.b` to refer to the literal-dot column "a.b" in the schema. After
    // construction, the [[UnqualifiedColumnName]] holds "a.b", which matches the field name
    // exactly and the column is included in the filtered schema.
    val filteredSchema = ColumnSelection.applyToSchema(
      schemaName = "test",
      schema = schema,
      columnSelection = Some(
        ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("`a.b`")))
      ),
      ignoreCase = false
    )

    assert(filteredSchema == new StructType().add("a.b", IntegerType))
  }

  test("UnqualifiedColumnName rejects a dotted (multi-part) identifier") {
    checkError(
      exception = intercept[AnalysisException] {
        UnqualifiedColumnName("a.b")
      },
      condition = "AUTOCDC_MULTIPART_COLUMN_IDENTIFIER",
      sqlState = "42703",
      parameters = Map(
        "columnName" -> "a.b",
        "nameParts" -> "a, b"
      )
    )
  }

  test("UnqualifiedColumnName rejects a qualified column reference") {
    checkError(
      exception = intercept[AnalysisException] {
        UnqualifiedColumnName("src.x")
      },
      condition = "AUTOCDC_MULTIPART_COLUMN_IDENTIFIER",
      sqlState = "42703",
      parameters = Map(
        "columnName" -> "src.x",
        "nameParts" -> "src, x"
      )
    )
  }

  test("UnqualifiedColumnName rejects an identifier with three or more parts") {
    checkError(
      exception = intercept[AnalysisException] {
        UnqualifiedColumnName("a.b.c")
      },
      condition = "AUTOCDC_MULTIPART_COLUMN_IDENTIFIER",
      sqlState = "42703",
      parameters = Map(
        "columnName" -> "a.b.c",
        "nameParts" -> "a, b, c"
      )
    )
  }

  test("UnqualifiedColumnName lets a ParseException from the SQL parser propagate") {
    checkError(
      exception = intercept[ParseException] {
        UnqualifiedColumnName("")
      },
      condition = "PARSE_EMPTY_STATEMENT",
      sqlState = Some("42617")
    )
  }
}
