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

package org.apache.spark.sql.crossdbms

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.crossdbms.PostgresDialectConverter.{convertDataType, convertValuesClause}

class PostgresDialectConverterSuite extends SparkFunSuite {

  case class DialectConvertTestCase(
    input: String, expected: String, converterFunc: String => String) {
    val converted: String = converterFunc(input)
    assert(converted == expected)
  }

  test("data type conversion") {
    val int = "0"
    val exponential = "20E2"
    val float = "float(15.0)"
    val smallInt = "6S"
    val double = "20D"
    val doubleDecimal = "10.00D"
    val bigDecimal = "20E2BD"
    val long = "10L"
    val date = "date \"2001-01-01\""
    val timestamp = "timestamp '2014-05-04 01:01:00.000'"

    DialectConvertTestCase(int, int, convertDataType)
    DialectConvertTestCase(float, "CAST(15.0 AS REAL)", convertDataType)
    DialectConvertTestCase(smallInt, "CAST(6 AS SMALLINT)", convertDataType)
    DialectConvertTestCase(double, "CAST(20 AS DOUBLE PRECISION)", convertDataType)
    DialectConvertTestCase(doubleDecimal, "CAST(10.00 AS DOUBLE PRECISION)", convertDataType)
    DialectConvertTestCase(bigDecimal, "CAST(20E2 AS DECIMAL)", convertDataType)
    DialectConvertTestCase(long, "CAST(10 AS BIGINT)", convertDataType)
    DialectConvertTestCase(date, date, convertDataType)
    DialectConvertTestCase(timestamp, timestamp, convertDataType)
    DialectConvertTestCase(exponential, exponential, convertDataType)
  }

  test("values clause conversion") {
    DialectConvertTestCase("VALUES (1), (2), (3)", "(VALUES (1), (2), (3))", convertValuesClause)
    DialectConvertTestCase("VALUES ('col1', 20E2) AS tbl1", "(VALUES ('col1', 20E2)) AS tbl1",
      convertValuesClause)
    DialectConvertTestCase("VALUES ('col1', 20E2) AS tbl1(col1, col2)",
      "(VALUES ('col1', 20E2)) AS tbl1(col1, col2)", convertValuesClause)
    DialectConvertTestCase("VALUES (103,102,100,101,104)", "(VALUES (103,102,100,101,104))",
      convertValuesClause)
    DialectConvertTestCase("SELECT * FROM VALUES ('col1', 20E2) AS tbl1",
      "SELECT * FROM (VALUES ('col1', 20E2)) AS tbl1", convertValuesClause)
    DialectConvertTestCase("SELECT * FROM VALUES ('col1', 20E2) AS tbl1(col1, col2)",
      "SELECT * FROM (VALUES ('col1', 20E2)) AS tbl1(col1, col2)", convertValuesClause)
    DialectConvertTestCase("SELECT * FROM VALUES (1), (2), (3)",
      "SELECT * FROM (VALUES (1), (2), (3))", convertValuesClause)
//    val testCasesWithWhiteSpaces = testCases
//    testCasesWithWhiteSpaces.map(tc => DialectConvertTestCase(tc._1, tc._2, convertValuesClause))
  }
}
