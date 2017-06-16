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
    val msg1 = intercept[AnalysisException] {
      SchemaUtils.checkSchemaColumnNameDuplication(
        StructType.fromDDL("a INT, b INT, a INT"), "SchemaUtilsSuite", caseSensitiveAnalysis = true)
    }.getMessage
    assert(msg1.contains("""Found duplicate column(s) in SchemaUtilsSuite: `a`;"""))
    val msg2 = intercept[AnalysisException] {
      SchemaUtils.checkColumnNameDuplication(
        "a" :: "b" :: "a" :: Nil, "SchemaUtilsSuite", caseSensitiveResolution)
    }.getMessage
    assert(msg2.contains("""Found duplicate column(s) in SchemaUtilsSuite: `a`;"""))

    // Check no exception thrown
    SchemaUtils.checkSchemaColumnNameDuplication(
      StructType.fromDDL("a INT, b INT, c INT"), "SchemaUtilsSuite", caseSensitiveAnalysis = true)
    SchemaUtils.checkColumnNameDuplication(
      "a" :: "b" :: "c" :: Nil, "SchemaUtilsSuite", caseSensitiveResolution)
    SchemaUtils.checkSchemaColumnNameDuplication(
      StructType.fromDDL("Aa INT, b INT, aA INT"), "SchemaUtilsSuite", caseSensitiveAnalysis = true)
    SchemaUtils.checkColumnNameDuplication(
      "Aa" :: "b" :: "aA" :: Nil, "SchemaUtilsSuite", caseSensitiveResolution)
  }

  test("Check column name duplication in case-insensitive cases") {
    val msg3 = intercept[AnalysisException] {
      SchemaUtils.checkSchemaColumnNameDuplication(
        StructType.fromDDL("Aa INT, b INT, Aa INT"), "SchemaUtilsSuite",
        caseSensitiveAnalysis = false)
    }.getMessage
    assert(msg3.contains("""Found duplicate column(s) in SchemaUtilsSuite: `Aa`;"""))
    val msg4 = intercept[AnalysisException] {
      SchemaUtils.checkColumnNameDuplication(
        "Aa" :: "b" :: "Aa" :: Nil, "SchemaUtilsSuite", caseInsensitiveResolution)
    }.getMessage
    assert(msg4.contains("""Found duplicate column(s) in SchemaUtilsSuite: `Aa`;"""))

    val msg5 = intercept[AnalysisException] {
      SchemaUtils.checkSchemaColumnNameDuplication(
        StructType.fromDDL("a INT, bB INT, Bb INT"), "SchemaUtilsSuite",
        caseSensitiveAnalysis = false)
    }.getMessage
    assert(msg5.contains("""Found duplicate column(s) in SchemaUtilsSuite: `bB`;"""))
    val msg6 = intercept[AnalysisException] {
      SchemaUtils.checkColumnNameDuplication(
        "a" :: "bB" :: "Bb" :: Nil, "SchemaUtilsSuite", caseInsensitiveResolution)
    }.getMessage
    assert(msg6.contains("""Found duplicate column(s) in SchemaUtilsSuite: `bB`;"""))

    // Check no exception thrown
    SchemaUtils.checkSchemaColumnNameDuplication(
      StructType.fromDDL("a INT, b INT, c INT"), "SchemaUtilsSuite", caseSensitiveAnalysis = false)
    SchemaUtils.checkColumnNameDuplication(
      "a" :: "b" :: "c" :: Nil, "SchemaUtilsSuite", caseInsensitiveResolution)
  }
}
