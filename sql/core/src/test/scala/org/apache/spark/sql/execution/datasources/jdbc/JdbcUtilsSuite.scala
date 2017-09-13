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

package org.apache.spark.sql.execution.datasources.jdbc

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.types._

class JdbcUtilsSuite extends SparkFunSuite {

  val tableSchema = StructType(Seq(
    StructField("C1", StringType, false), StructField("C2", IntegerType, false)))
  val caseSensitive = org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
  val caseInsensitive = org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution

  test("Parse user specified column types") {
    assert(
      JdbcUtils.getCustomSchema(tableSchema, "C1 DATE, C2 STRING", caseInsensitive) ===
      StructType(Seq(StructField("C1", DateType, true), StructField("C2", StringType, true))))
    assert(JdbcUtils.getCustomSchema(tableSchema, "C1 DATE, C2 STRING", caseSensitive) ===
      StructType(Seq(StructField("C1", DateType, true), StructField("C2", StringType, true))))
    assert(
      JdbcUtils.getCustomSchema(tableSchema, "c1 DATE, C2 STRING", caseInsensitive) ===
        StructType(Seq(StructField("c1", DateType, true), StructField("C2", StringType, true))))
    assert(JdbcUtils.getCustomSchema(
      tableSchema, "c1 DECIMAL(38, 0), C2 STRING", caseInsensitive) ===
      StructType(Seq(StructField("c1", DecimalType(38, 0), true),
        StructField("C2", StringType, true))))

    // Throw AnalysisException
    val duplicate = intercept[AnalysisException]{
      JdbcUtils.getCustomSchema(tableSchema, "c1 DATE, c1 STRING", caseInsensitive) ===
        StructType(Seq(StructField("c1", DateType, true), StructField("c1", StringType, true)))
    }
    assert(duplicate.getMessage.contains(
      "Found duplicate column(s) in the customSchema option value"))

    val allColumns = intercept[AnalysisException]{
      JdbcUtils.getCustomSchema(tableSchema, "C1 STRING", caseSensitive) ===
        StructType(Seq(StructField("C1", DateType, true)))
    }
    assert(allColumns.getMessage.contains("Please provide all the columns,"))

    val caseSensitiveColumnNotFound = intercept[AnalysisException]{
      JdbcUtils.getCustomSchema(tableSchema, "c1 DATE, C2 STRING", caseSensitive) ===
        StructType(Seq(StructField("c1", DateType, true), StructField("C2", StringType, true)))
    }
    assert(caseSensitiveColumnNotFound.getMessage.contains(
      "Please provide all the columns, all columns are: C1,C2;"))

    val caseInsensitiveColumnNotFound = intercept[AnalysisException]{
      JdbcUtils.getCustomSchema(tableSchema, "c3 DATE, C2 STRING", caseInsensitive) ===
        StructType(Seq(StructField("c3", DateType, true), StructField("C2", StringType, true)))
    }
    assert(caseInsensitiveColumnNotFound.getMessage.contains(
      "Please provide all the columns, all columns are: C1,C2;"))

    // Throw ParseException
    val dataTypeNotSupported = intercept[ParseException]{
      JdbcUtils.getCustomSchema(tableSchema, "c3 DATEE, C2 STRING", caseInsensitive) ===
        StructType(Seq(StructField("c3", DateType, true), StructField("C2", StringType, true)))
    }
    assert(dataTypeNotSupported.getMessage.contains("DataType datee is not supported"))

    val mismatchedInput = intercept[ParseException]{
      JdbcUtils.getCustomSchema(tableSchema, "c3 DATE. C2 STRING", caseInsensitive) ===
        StructType(Seq(StructField("c3", DateType, true), StructField("C2", StringType, true)))
    }
    assert(mismatchedInput.getMessage.contains("mismatched input '.' expecting"))
  }
}
