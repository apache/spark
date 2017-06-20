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

package org.apache.spark.sql.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.types.StructType

class SchemaUtilsSuite extends SparkFunSuite {

  test("Check column name duplication in case-sensitive cases") {
    var msg = intercept[AnalysisException] {
      SchemaUtils.checkSchemaColumnNameDuplication(
        StructType.fromDDL("a INT, b INT, a INT"), "in SchemaUtilsSuite",
        caseSensitiveAnalysis = true)
    }.getMessage
    assert(msg.contains("Found duplicate column(s) in SchemaUtilsSuite: `a`;"))
    msg = intercept[AnalysisException] {
      SchemaUtils.checkColumnNameDuplication(
        "a" :: "b" :: "a" :: Nil, "in SchemaUtilsSuite", caseSensitiveResolution)
    }.getMessage
    assert(msg.contains("Found duplicate column(s) in SchemaUtilsSuite: `a`;"))

    msg = intercept[AnalysisException] {
      SchemaUtils.checkSchemaColumnNameDuplication(
        StructType.fromDDL("a INT, b INT, a INT, a INT"), "in SchemaUtilsSuite",
        caseSensitiveAnalysis = true)
    }.getMessage
    assert(msg.contains("Found duplicate column(s) in SchemaUtilsSuite: `a`;"))
    msg = intercept[AnalysisException] {
      SchemaUtils.checkColumnNameDuplication(
        "a" :: "b" :: "a" :: "a" :: Nil, "in SchemaUtilsSuite", caseSensitiveResolution)
    }.getMessage
    assert(msg.contains("Found duplicate column(s) in SchemaUtilsSuite: `a`;"))

    msg = intercept[AnalysisException] {
      SchemaUtils.checkSchemaColumnNameDuplication(
        StructType.fromDDL("a INT, b INT, a INT, b INT"), "in SchemaUtilsSuite",
        caseSensitiveAnalysis = true)
    }.getMessage
    assert(msg.contains("Found duplicate column(s) in SchemaUtilsSuite: `a`, `b`;"))
    msg = intercept[AnalysisException] {
      SchemaUtils.checkColumnNameDuplication(
        "a" :: "b" :: "a" :: "b" :: Nil, "in SchemaUtilsSuite", caseSensitiveResolution)
    }.getMessage
    assert(msg.contains("Found duplicate column(s) in SchemaUtilsSuite: `a`, `b`;"))

    msg = intercept[AnalysisException] {
      SchemaUtils.checkSchemaColumnNameDuplication(
        StructType.fromDDL("a INT, c INT, b INT, a INT, b INT, c INT"), "in SchemaUtilsSuite",
        caseSensitiveAnalysis = true)
    }.getMessage
    assert(msg.contains("Found duplicate column(s) in SchemaUtilsSuite: `a`, `c`, `b`;"))
    msg = intercept[AnalysisException] {
      SchemaUtils.checkColumnNameDuplication(
        "a" :: "c" :: "b" :: "a" :: "b" :: "c" :: Nil, "in SchemaUtilsSuite",
        caseSensitiveResolution)
    }.getMessage
    assert(msg.contains("Found duplicate column(s) in SchemaUtilsSuite: `a`, `c`, `b`;"))

    // Check no exception thrown
    SchemaUtils.checkSchemaColumnNameDuplication(
      StructType.fromDDL("a INT, b INT, c INT"), "in SchemaUtilsSuite",
      caseSensitiveAnalysis = true)
    SchemaUtils.checkColumnNameDuplication(
      "a" :: "b" :: "c" :: Nil, "in SchemaUtilsSuite", caseSensitiveResolution)
    SchemaUtils.checkSchemaColumnNameDuplication(
      StructType.fromDDL("Aa INT, b INT, aA INT"), "in SchemaUtilsSuite",
      caseSensitiveAnalysis = true)
    SchemaUtils.checkColumnNameDuplication(
      "Aa" :: "b" :: "aA" :: Nil, "in SchemaUtilsSuite", caseSensitiveResolution)
  }

  test("Check column name duplication in case-insensitive cases") {
    var msg = intercept[AnalysisException] {
      SchemaUtils.checkSchemaColumnNameDuplication(
        StructType.fromDDL("Aa INT, b INT, Aa INT"), "in SchemaUtilsSuite",
        caseSensitiveAnalysis = false)
    }.getMessage
    assert(msg.contains("Found duplicate column(s) in SchemaUtilsSuite: `Aa`;"))
    msg = intercept[AnalysisException] {
      SchemaUtils.checkColumnNameDuplication(
        "Aa" :: "b" :: "Aa" :: Nil, "in SchemaUtilsSuite", caseInsensitiveResolution)
    }.getMessage
    assert(msg.contains("Found duplicate column(s) in SchemaUtilsSuite: `Aa`;"))

    msg = intercept[AnalysisException] {
      SchemaUtils.checkSchemaColumnNameDuplication(
        StructType.fromDDL("a INT, bB INT, Bb INT"), "in SchemaUtilsSuite",
        caseSensitiveAnalysis = false)
    }.getMessage
    assert(msg.contains("Found duplicate column(s) in SchemaUtilsSuite: `bB`;"))
    msg = intercept[AnalysisException] {
      SchemaUtils.checkColumnNameDuplication(
        "a" :: "bB" :: "Bb" :: Nil, "in SchemaUtilsSuite", caseInsensitiveResolution)
    }.getMessage
    assert(msg.contains("Found duplicate column(s) in SchemaUtilsSuite: `bB`;"))

    msg = intercept[AnalysisException] {
      SchemaUtils.checkSchemaColumnNameDuplication(
        StructType.fromDDL("Aa INT, b INT, Aa INT, c INT, aa INT"), "in SchemaUtilsSuite",
        caseSensitiveAnalysis = false)
    }.getMessage
    assert(msg.contains("Found duplicate column(s) in SchemaUtilsSuite: `Aa`;"))
    msg = intercept[AnalysisException] {
      SchemaUtils.checkColumnNameDuplication(
        "Aa" :: "b" :: "Aa" :: "c" :: "aa" :: Nil, "in SchemaUtilsSuite", caseInsensitiveResolution)
    }.getMessage
    assert(msg.contains("Found duplicate column(s) in SchemaUtilsSuite: `Aa`;"))

    msg = intercept[AnalysisException] {
      SchemaUtils.checkSchemaColumnNameDuplication(
        StructType.fromDDL("Aa INT, bB INT, Bb INT, aa INT"), "in SchemaUtilsSuite",
        caseSensitiveAnalysis = false)
    }.getMessage
    assert(msg.contains("Found duplicate column(s) in SchemaUtilsSuite: `Aa`, `bB`;"))
    msg = intercept[AnalysisException] {
      SchemaUtils.checkColumnNameDuplication(
        "Aa" :: "bB" :: "Bb" :: "aa" :: Nil, "in SchemaUtilsSuite", caseInsensitiveResolution)
    }.getMessage
    assert(msg.contains("Found duplicate column(s) in SchemaUtilsSuite: `Aa`, `bB`;"))

    msg = intercept[AnalysisException] {
      SchemaUtils.checkSchemaColumnNameDuplication(
        StructType.fromDDL("Aa INT, cc INT, bB INT, cC INT, Bb INT, aa INT"), "in SchemaUtilsSuite",
        caseSensitiveAnalysis = false)
    }.getMessage
    assert(msg.contains("Found duplicate column(s) in SchemaUtilsSuite: `Aa`, `cc`, `bB`;"))
    msg = intercept[AnalysisException] {
      SchemaUtils.checkColumnNameDuplication(
        "Aa" :: "cc" :: "bB" :: "cC" :: "Bb" :: "aa" :: Nil, "in SchemaUtilsSuite",
        caseInsensitiveResolution)
    }.getMessage
    assert(msg.contains("Found duplicate column(s) in SchemaUtilsSuite: `Aa`, `cc`, `bB`;"))

    // Check no exception thrown
    SchemaUtils.checkSchemaColumnNameDuplication(
      StructType.fromDDL("a INT, b INT, c INT"), "in SchemaUtilsSuite",
      caseSensitiveAnalysis = false)
    SchemaUtils.checkColumnNameDuplication(
      "a" :: "b" :: "c" :: Nil, "in SchemaUtilsSuite", caseInsensitiveResolution)
  }
}
