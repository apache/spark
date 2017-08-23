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

  val schema = StructType(Seq(
    StructField("C1", StringType, false), StructField("C2", IntegerType, false)))
  val caseSensitive = org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
  val caseInsensitive = org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution

  test("Parse user specified column types") {
    assert(JdbcUtils.parseUserSpecifiedColumnTypes(schema, "C1 string", caseInsensitive) ===
      StructType(Seq(StructField("C1", StringType, true))))
    assert(JdbcUtils.parseUserSpecifiedColumnTypes(schema, "C2 string", caseInsensitive) ===
      StructType(Seq(StructField("C2", StringType, true))))
    assert(
      JdbcUtils.parseUserSpecifiedColumnTypes(schema, "C1 date, C2 string", caseInsensitive) ===
      StructType(Seq(StructField("C1", DateType, true), StructField("C2", StringType, true))))
    assert(JdbcUtils.parseUserSpecifiedColumnTypes(schema, "C1 date, C2 string", caseSensitive) ===
      StructType(Seq(StructField("C1", DateType, true), StructField("C2", StringType, true))))
    assert(
      JdbcUtils.parseUserSpecifiedColumnTypes(schema, "c1 date, C2 string", caseInsensitive) ===
        StructType(Seq(StructField("c1", DateType, true), StructField("C2", StringType, true))))
    assert(JdbcUtils.parseUserSpecifiedColumnTypes(schema, "c1 decimal(38, 0)", caseInsensitive) ===
      StructType(Seq(StructField("c1", DecimalType(38, 0), true))))

    // Throw AnalysisException
    val exception1 = intercept[AnalysisException]{
      JdbcUtils.parseUserSpecifiedColumnTypes(schema, "c1 date, C2 string", caseSensitive) ===
        StructType(Seq(StructField("c1", DateType, true), StructField("C2", StringType, true)))
    }
    assert(exception1.getMessage.contains(
      s"${JDBCOptions.JDBC_CUSTOM_DATAFRAME_COLUMN_TYPES} option column c1 not found in schema"))
    val exception2 = intercept[AnalysisException]{
      JdbcUtils.parseUserSpecifiedColumnTypes(schema, "c3 date, C2 string", caseInsensitive) ===
        StructType(Seq(StructField("c3", DateType, true), StructField("C2", StringType, true)))
    }
    assert(exception2.getMessage.contains(
      s"${JDBCOptions.JDBC_CUSTOM_DATAFRAME_COLUMN_TYPES} option column c3 not found in schema"))

    // Throw ParseException
    val exception3 = intercept[ParseException]{
      JdbcUtils.parseUserSpecifiedColumnTypes(schema, "c3 datee, C2 string", caseInsensitive) ===
        StructType(Seq(StructField("c3", DateType, true), StructField("C2", StringType, true)))
    }
    assert(exception3.getMessage.contains("DataType datee is not supported"))
    val exception4 = intercept[ParseException]{
      JdbcUtils.parseUserSpecifiedColumnTypes(schema, "c3 date. C2 string", caseInsensitive) ===
        StructType(Seq(StructField("c3", DateType, true), StructField("C2", StringType, true)))
    }
    assert(exception4.getMessage.contains("mismatched input '.' expecting"))
  }
}
