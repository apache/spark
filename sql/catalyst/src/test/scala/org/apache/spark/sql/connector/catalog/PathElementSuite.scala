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

package org.apache.spark.sql.connector.catalog

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connector.catalog.CatalogManager.{
  CurrentSchemaEntry, LiteralPathEntry
}

/**
 * Direct unit tests for [[PathElement.validateNoStaticDuplicates]]. The end-to-end
 * `SetPathSuite` exercises this via SQL, but the duplicate-detection rules
 * (literal-vs-literal, current_schema-vs-current_schema, case-sensitivity) are pure
 * data and benefit from focused tests close to the implementation.
 */
class PathElementSuite extends SparkFunSuite {

  private def literal(parts: String*): LiteralPathEntry = LiteralPathEntry(parts.toSeq)

  test("validateNoStaticDuplicates: no duplicates returns the input unchanged") {
    val entries = Seq(
      literal("spark_catalog", "default"),
      literal("system", "builtin"),
      CurrentSchemaEntry)
    assert(PathElement.validateNoStaticDuplicates(entries, caseSensitive = false) === entries)
  }

  test("validateNoStaticDuplicates: duplicate literal under case-insensitive collation") {
    val entries = Seq(
      literal("spark_catalog", "default"),
      literal("Spark_Catalog", "DEFAULT"))
    val e = intercept[AnalysisException] {
      PathElement.validateNoStaticDuplicates(entries, caseSensitive = false)
    }
    assert(e.getCondition == "DUPLICATE_SQL_PATH_ENTRY")
    assert(e.getMessageParameters.get("pathEntry") == "Spark_Catalog.DEFAULT")
  }

  test("validateNoStaticDuplicates: case-sensitive mode keeps differently cased entries") {
    val entries = Seq(
      literal("spark_catalog", "DEFAULT"),
      literal("spark_catalog", "default"))
    assert(PathElement.validateNoStaticDuplicates(entries, caseSensitive = true) === entries)
  }

  test("validateNoStaticDuplicates: repeated CurrentSchemaEntry is rejected") {
    val entries = Seq(CurrentSchemaEntry, CurrentSchemaEntry)
    val e = intercept[AnalysisException] {
      PathElement.validateNoStaticDuplicates(entries, caseSensitive = false)
    }
    assert(e.getCondition == "DUPLICATE_SQL_PATH_ENTRY")
    assert(e.getMessageParameters.get("pathEntry") == "current_schema")
  }

  test("validateNoStaticDuplicates: literal-vs-CurrentSchemaEntry collision is tolerated") {
    // The CurrentSchemaEntry marker resolves dynamically against USE SCHEMA, so a literal
    // that happens to match the live current schema is intentionally not flagged here.
    val entries = Seq(
      literal("spark_catalog", "default"),
      CurrentSchemaEntry,
      literal("system", "builtin"))
    assert(PathElement.validateNoStaticDuplicates(entries, caseSensitive = false) === entries)
  }

  test("validateNoStaticDuplicates: identifier containing a dot is quoted in the error") {
    val entries = Seq(
      literal("spark_catalog", "weird.schema"),
      literal("spark_catalog", "weird.schema"))
    val e = intercept[AnalysisException] {
      PathElement.validateNoStaticDuplicates(entries, caseSensitive = false)
    }
    assert(e.getMessageParameters.get("pathEntry") == "spark_catalog.`weird.schema`")
  }

  test("validateNoStaticDuplicates: multi-level namespace duplicate is flagged") {
    val entries = Seq(
      literal("cat", "db", "ns"),
      literal("cat", "db", "ns"))
    val e = intercept[AnalysisException] {
      PathElement.validateNoStaticDuplicates(entries, caseSensitive = false)
    }
    assert(e.getMessageParameters.get("pathEntry") == "cat.db.ns")
  }
}
