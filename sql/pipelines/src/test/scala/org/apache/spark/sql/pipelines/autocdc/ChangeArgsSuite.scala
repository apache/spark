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
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

class ChangeArgsSuite extends SparkFunSuite {

  private val sourceSchema = new StructType()
    .add("id", IntegerType, nullable = false)
    .add("Name", StringType)
    .add("age", IntegerType)

  test("ColumnSelection None leaves schema unchanged") {
    assert(ColumnSelection.applyToSchema(sourceSchema, None) == sourceSchema)
  }

  test("ColumnSelection IncludeColumns filters by exact name in schema order") {
    val filteredSchema = ColumnSelection.applyToSchema(
      sourceSchema,
      Some(ColumnSelection.IncludeColumns(
        Seq(UnqualifiedColumnName("age"), UnqualifiedColumnName("Name")))))

    assert(filteredSchema == new StructType()
      .add("Name", StringType)
      .add("age", IntegerType))
  }

  test("ColumnSelection ExcludeColumns filters by exact name") {
    val filteredSchema = ColumnSelection.applyToSchema(
      sourceSchema,
      Some(ColumnSelection.ExcludeColumns(Seq(UnqualifiedColumnName("id")))))

    assert(filteredSchema == new StructType()
      .add("Name", StringType)
      .add("age", IntegerType))
  }

  test("ColumnSelection IncludeColumns fails for columns not present in schema") {
    checkError(
      exception = intercept[AnalysisException] {
        ColumnSelection.applyToSchema(
          sourceSchema,
          // Column inclusion is case-sensitive; "name" will not match against "Name".
          Some(ColumnSelection.IncludeColumns(
            Seq(UnqualifiedColumnName("name"), UnqualifiedColumnName("missing"))))
        )
      },
      condition = "AUTOCDC_INVALID_COLUMN_SELECTION.COLUMNS_NOT_FOUND",
      sqlState = "42703",
      parameters = Map(
        "missingColumns" -> "name, missing",
        "availableColumns" -> "id, Name, age"
      )
    )
  }

  test("ColumnSelection ExcludeColumns fails for columns not present in schema") {
    checkError(
      exception = intercept[AnalysisException] {
        ColumnSelection.applyToSchema(
          sourceSchema,
          // Column exclusion is case-sensitive; "NAME" will not match against "Name".
          Some(ColumnSelection.ExcludeColumns(
            Seq(UnqualifiedColumnName("NAME"), UnqualifiedColumnName("missing"))))
        )
      },
      condition = "AUTOCDC_INVALID_COLUMN_SELECTION.COLUMNS_NOT_FOUND",
      sqlState = "42703",
      parameters = Map(
        "missingColumns" -> "NAME, missing",
        "availableColumns" -> "id, Name, age"
      )
    )
  }

  test("UnqualifiedColumnName accepts a simple single-part identifier") {
    assert(UnqualifiedColumnName("col").name == "col")
  }

  test("UnqualifiedColumnName accepts a backtick-quoted name containing a literal dot") {
    // Backticks make the dot part of a single name part, so this passes validation.
    assert(UnqualifiedColumnName("`a.b`").name == "`a.b`")
  }

  test("UnqualifiedColumnName rejects a dotted (multi-part) identifier") {
    checkError(
      exception = intercept[AnalysisException] {
        UnqualifiedColumnName("a.b")
      },
      condition = "AUTOCDC_INVALID_COLUMN_SELECTION.MULTIPART_COLUMN_IDENTIFIER",
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
      condition = "AUTOCDC_INVALID_COLUMN_SELECTION.MULTIPART_COLUMN_IDENTIFIER",
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
      condition = "AUTOCDC_INVALID_COLUMN_SELECTION.MULTIPART_COLUMN_IDENTIFIER",
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
