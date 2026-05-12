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
      Some(ColumnSelection.IncludeColumns(Seq("age", "Name"))))

    assert(filteredSchema == new StructType()
      .add("Name", StringType)
      .add("age", IntegerType))
  }

  test("ColumnSelection ExcludeColumns filters by exact name") {
    val filteredSchema = ColumnSelection.applyToSchema(
      sourceSchema,
      Some(ColumnSelection.ExcludeColumns(Seq("id"))))

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
          Some(ColumnSelection.IncludeColumns(Seq("name", "missing")))
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
          Some(ColumnSelection.ExcludeColumns(Seq("NAME", "missing")))
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
}
