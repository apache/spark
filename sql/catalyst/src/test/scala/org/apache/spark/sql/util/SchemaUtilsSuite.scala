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

import java.util.Locale

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.types.StructType

class SchemaUtilsSuite extends SparkFunSuite {

  private def resolver(caseSensitiveAnalysis: Boolean): Resolver = {
    if (caseSensitiveAnalysis) {
      caseSensitiveResolution
    } else {
      caseInsensitiveResolution
    }
  }

  Seq((true, ("a", "a"), ("b", "b")), (false, ("a", "A"), ("b", "B"))).foreach {
      case (caseSensitive, (a0, a1), (b0, b1)) =>

    val testType = if (caseSensitive) "case-sensitive" else "case-insensitive"
    test(s"Check column name duplication in $testType cases") {
      def checkExceptionCases(schemaStr: String, duplicatedColumns: Seq[String]): Unit = {
        val expectedErrorMsg = "Found duplicate column(s) in SchemaUtilsSuite: " +
          duplicatedColumns.map(c => s"`${c.toLowerCase(Locale.ROOT)}`").mkString(", ")
        val schema = StructType.fromDDL(schemaStr)
        var msg = intercept[AnalysisException] {
          SchemaUtils.checkSchemaColumnNameDuplication(
            schema, "in SchemaUtilsSuite", caseSensitiveAnalysis = caseSensitive)
        }.getMessage
        assert(msg.contains(expectedErrorMsg))
        msg = intercept[AnalysisException] {
          SchemaUtils.checkColumnNameDuplication(
            schema.map(_.name), "in SchemaUtilsSuite", resolver(caseSensitive))
        }.getMessage
        assert(msg.contains(expectedErrorMsg))
        msg = intercept[AnalysisException] {
          SchemaUtils.checkColumnNameDuplication(
            schema.map(_.name), "in SchemaUtilsSuite", caseSensitiveAnalysis = caseSensitive)
        }.getMessage
        assert(msg.contains(expectedErrorMsg))
      }

      checkExceptionCases(s"$a0 INT, b INT, $a1 INT", a0 :: Nil)
      checkExceptionCases(s"$a0 INT, b INT, $a1 INT, $a0 INT", a0 :: Nil)
      checkExceptionCases(s"$a0 INT, $b0 INT, $a1 INT, $a0 INT, $b1 INT", b0 :: a0 :: Nil)
    }
  }

  test("Check no exception thrown for valid schemas") {
    def checkNoExceptionCases(schemaStr: String, caseSensitive: Boolean): Unit = {
      val schema = StructType.fromDDL(schemaStr)
      SchemaUtils.checkSchemaColumnNameDuplication(
        schema, "in SchemaUtilsSuite", caseSensitiveAnalysis = caseSensitive)
      SchemaUtils.checkColumnNameDuplication(
        schema.map(_.name), "in SchemaUtilsSuite", resolver(caseSensitive))
      SchemaUtils.checkColumnNameDuplication(
        schema.map(_.name), "in SchemaUtilsSuite", caseSensitiveAnalysis = caseSensitive)
    }

    checkNoExceptionCases("a INT, b INT, c INT", caseSensitive = true)
    checkNoExceptionCases("Aa INT, b INT, aA INT", caseSensitive = true)

    checkNoExceptionCases("a INT, b INT, c INT", caseSensitive = false)
  }

  test(s"Test checkDataTypeMatchesForSameColumnName") {
    def compareSchemas(schema1_str: String, schema2_str: String,
      caseSensitive: Boolean, shouldRaiseException: Boolean): Unit = {
      val schema1 = StructType.fromDDL(schema1_str)
      val schema2 = StructType.fromDDL(schema2_str)

      if (shouldRaiseException) {
        val msg = intercept[AnalysisException] {
          SchemaUtils.checkDataTypeMatchesForSameColumnName(schema1, schema2, caseSensitive)
        }.getMessage
        assert(msg.contains("type doesn't match between schemas"))
      }
      else SchemaUtils.checkDataTypeMatchesForSameColumnName(schema1, schema2, caseSensitive)

    }
    // pass when datatype is the same
    compareSchemas("a int, b string", "a int, B string",
      caseSensitive = false, shouldRaiseException = false)
    compareSchemas("a int, b string, B int", "a int, b string, B int",
      caseSensitive = true, shouldRaiseException = false)

    // fail when there's at least one mismatch
    compareSchemas("a int, b string", "a string, b string",
      caseSensitive = false, shouldRaiseException = true)
    compareSchemas("a int, b string", "a int, b string, B int",
      caseSensitive = false, shouldRaiseException = true)

    // work as expected when schemas structures differ
    compareSchemas("a int, b string", "c string, D int, A int",
      caseSensitive = true, shouldRaiseException = false)
    compareSchemas("a int, b string", "b string",
      caseSensitive = false, shouldRaiseException = false)
    compareSchemas("a int, b string", "B string",
      caseSensitive = false, shouldRaiseException = false)
    compareSchemas("a int, b string", "a string",
      caseSensitive = true, shouldRaiseException = true)
    compareSchemas("a int, b string", "A string",
      caseSensitive = false, shouldRaiseException = true)
    compareSchemas("a int", "a int, A string",
      caseSensitive = true, shouldRaiseException = false)
    compareSchemas("b string", "b string, B int",
      caseSensitive = false, shouldRaiseException = true)
  }
}
