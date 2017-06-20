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
    def checkCaseSensitiveExceptionCases(schemaStr: String, duplicatedColumns: String): Unit = {
      val expectedErrorMsg = s"Found duplicate column(s) in SchemaUtilsSuite: $duplicatedColumns"
      val schema = StructType.fromDDL(schemaStr)
      var msg = intercept[AnalysisException] {
        SchemaUtils.checkSchemaColumnNameDuplication(
          schema, "in SchemaUtilsSuite", caseSensitiveAnalysis = true)
      }.getMessage
      assert(msg.contains(expectedErrorMsg))
      msg = intercept[AnalysisException] {
        SchemaUtils.checkColumnNameDuplication(
          schema.map(_.name), "in SchemaUtilsSuite", caseSensitiveResolution)
      }.getMessage
      assert(msg.contains(expectedErrorMsg))
      msg = intercept[AnalysisException] {
        SchemaUtils.checkColumnNameDuplication(
          schema.map(_.name), "in SchemaUtilsSuite", caseSensitiveAnalysis = true)
      }.getMessage
      assert(msg.contains(expectedErrorMsg))
    }

    checkCaseSensitiveExceptionCases("a INT, b INT, a INT", "`a`")
    checkCaseSensitiveExceptionCases("a INT, b INT, a INT, a INT", "`a`")
    checkCaseSensitiveExceptionCases("a INT, b INT, a INT, b INT", "`b`, `a`")
    checkCaseSensitiveExceptionCases("a INT, c INT, b INT, a INT, b INT, c INT", "`b`, `a`, `c`")

    // Check no exception thrown
    def checkCaseSensitiveNoExceptionCases(schemaStr: String): Unit = {
      val schema = StructType.fromDDL(schemaStr)
      SchemaUtils.checkSchemaColumnNameDuplication(
        schema, "in SchemaUtilsSuite", caseSensitiveAnalysis = true)
      SchemaUtils.checkColumnNameDuplication(
        schema.map(_.name), "in SchemaUtilsSuite", caseSensitiveResolution)
      SchemaUtils.checkColumnNameDuplication(
        schema.map(_.name), "in SchemaUtilsSuite", caseSensitiveAnalysis = true)
    }

    checkCaseSensitiveNoExceptionCases("a INT, b INT, c INT")
    checkCaseSensitiveNoExceptionCases("Aa INT, b INT, aA INT")
  }

  test("Check column name duplication in case-insensitive cases") {
     def checkCaseInsensitiveExceptionCases(schemaStr: String, duplicatedColumns: String): Unit = {
      val expectedErrorMsg = s"Found duplicate column(s) in SchemaUtilsSuite: $duplicatedColumns"
      val schema = StructType.fromDDL(schemaStr)
      var msg = intercept[AnalysisException] {
        SchemaUtils.checkSchemaColumnNameDuplication(
          schema, "in SchemaUtilsSuite", caseSensitiveAnalysis = false)
      }.getMessage
      assert(msg.contains(expectedErrorMsg))
      msg = intercept[AnalysisException] {
        SchemaUtils.checkColumnNameDuplication(
          schema.map(_.name), "in SchemaUtilsSuite", caseInsensitiveResolution)
      }.getMessage
      assert(msg.contains(expectedErrorMsg))
      msg = intercept[AnalysisException] {
        SchemaUtils.checkColumnNameDuplication(
          schema.map(_.name), "in SchemaUtilsSuite", caseSensitiveAnalysis = false)
      }.getMessage
      assert(msg.contains(expectedErrorMsg))
    }

    checkCaseInsensitiveExceptionCases("Aa INT, b INT, Aa INT", "`aa`")
    checkCaseInsensitiveExceptionCases("a INT, bB INT, Bb INT", "`bb`")
    checkCaseInsensitiveExceptionCases("Aa INT, b INT, Aa INT, c INT, aa INT", "`aa`")
    checkCaseInsensitiveExceptionCases("Aa INT, bB INT, Bb INT, aa INT", "`bb`, `aa`")
    checkCaseInsensitiveExceptionCases(
      "Aa INT, cc INT, bB INT, cC INT, Bb INT, aa INT", "`bb`, `cc`, `aa`")

    // Check no exception thrown
    val schema = StructType.fromDDL("a INT, b INT, c INT")
    SchemaUtils.checkSchemaColumnNameDuplication(
      schema, "in SchemaUtilsSuite", caseSensitiveAnalysis = false)
    SchemaUtils.checkColumnNameDuplication(
      schema.map(_.name), "in SchemaUtilsSuite", caseInsensitiveResolution)
    SchemaUtils.checkColumnNameDuplication(
      schema.map(_.name), "in SchemaUtilsSuite", caseSensitiveAnalysis = false)
  }
}
