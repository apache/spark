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
package org.apache.spark.sql.errors

import org.apache.spark._
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.SQLConf

// Test suite for all the execution errors that requires enable ANSI SQL mode.
class QueryExecutionAnsiErrorsSuite extends QueryTest with QueryErrorsSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")

  private val ansiConf = "\"" + SQLConf.ANSI_ENABLED.key + "\""

  test("CAST_OVERFLOW: from timestamp to int") {
    checkError(
      exception = intercept[SparkArithmeticException] {
        sql("select CAST(TIMESTAMP '9999-12-31T12:13:14.56789Z' AS INT)").collect()
      },
      errorClass = "CAST_OVERFLOW",
      parameters = Map("value" -> "TIMESTAMP '9999-12-31 04:13:14.56789'",
        "sourceType" -> "\"TIMESTAMP\"",
        "targetType" -> "\"INT\"",
        "ansiConfig" -> ansiConf),
      sqlState = "22005")
  }

  test("DIVIDE_BY_ZERO: can't divide an integer by zero") {
    checkError(
      exception = intercept[SparkArithmeticException] {
        sql("select 6/0").collect()
      },
      errorClass = "DIVIDE_BY_ZERO",
      sqlState = "22012",
      parameters = Map("config" -> ansiConf))
  }

  test("INTERVAL_DIVIDED_BY_ZERO: interval divided by zero") {
    checkError(
      exception = intercept[SparkArithmeticException] {
        sql("select interval 1 day / 0").collect()
      },
      errorClass = "INTERVAL_DIVIDED_BY_ZERO",
      parameters = Map.empty
    )
  }

  test("INVALID_FRACTION_OF_SECOND: in the function make_timestamp") {
    checkError(
      exception = intercept[SparkDateTimeException] {
        sql("select make_timestamp(2012, 11, 30, 9, 19, 60.66666666)").collect()
      },
      errorClass = "INVALID_FRACTION_OF_SECOND",
      sqlState = "22023",
      parameters = Map("ansiConfig" -> ansiConf))
  }

  test("CANNOT_CHANGE_DECIMAL_PRECISION: cast string to decimal") {
    checkError(
      exception = intercept[SparkArithmeticException] {
        sql("select CAST('66666666666666.666' AS DECIMAL(8, 1))").collect()
      },
      errorClass = "CANNOT_CHANGE_DECIMAL_PRECISION",
      sqlState = "22005",
      parameters = Map(
        "value" -> "Decimal(expanded, 66666666666666.666, 17, 3)",
        "precision" -> "8",
        "scale" -> "1",
        "config" -> ansiConf))
  }

  test("INVALID_ARRAY_INDEX: get element from array") {
    checkError(
      exception = intercept[SparkArrayIndexOutOfBoundsException] {
        sql("select array(1, 2, 3, 4, 5)[8]").collect()
      },
      errorClass = "INVALID_ARRAY_INDEX",
      parameters = Map("indexValue" -> "8", "arraySize" -> "5", "ansiConfig" -> ansiConf)
    )
  }

  test("INVALID_ARRAY_INDEX_IN_ELEMENT_AT: element_at from array") {
    checkError(
      exception = intercept[SparkArrayIndexOutOfBoundsException] {
        sql("select element_at(array(1, 2, 3, 4, 5), 8)").collect()
      },
      errorClass = "INVALID_ARRAY_INDEX_IN_ELEMENT_AT",
      parameters = Map("indexValue" -> "8", "arraySize" -> "5", "ansiConfig" -> ansiConf)
    )
  }

  test("MAP_KEY_DOES_NOT_EXIST: key does not exist in element_at") {
    checkError(
      exception = intercept[SparkNoSuchElementException] {
        sql("select element_at(map(1, 'a', 2, 'b'), 3)").collect()
      },
      errorClass = "MAP_KEY_DOES_NOT_EXIST",
      parameters = Map(
        "keyValue" -> "3",
        "config" -> ansiConf))
  }

  test("CAST_INVALID_INPUT: cast string to double") {
    checkError(
      exception = intercept[SparkNumberFormatException] {
        sql("select CAST('111111111111xe23' AS DOUBLE)").collect()
      },
      errorClass = "CAST_INVALID_INPUT",
      parameters = Map(
        "expression" -> "'111111111111xe23'",
        "sourceType" -> "\"STRING\"",
        "targetType" -> "\"DOUBLE\"",
        "ansiConfig" -> ansiConf))
  }

  test("CANNOT_PARSE_TIMESTAMP: parse string to timestamp") {
    checkError(
      exception = intercept[SparkDateTimeException] {
        sql("select to_timestamp('abc', 'yyyy-MM-dd HH:mm:ss')").collect()
      },
      errorClass = "CANNOT_PARSE_TIMESTAMP",
      parameters = Map(
        "message" -> "Text 'abc' could not be parsed at index 0",
        "ansiConfig" -> ansiConf)
    )
  }

  test("CAST_OVERFLOW_IN_TABLE_INSERT: overflow during table insertion") {
    Seq("TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL(7,2)").foreach { targetType =>
      val tableName = "overflowTable"
      withTable(tableName) {
        sql(s"CREATE TABLE $tableName(i $targetType) USING parquet")
        checkError(
          exception = intercept[SparkException] {
            sql(s"insert into $tableName values 12345678901234567890D")
          }.getCause.getCause.getCause.asInstanceOf[SparkThrowable],
          errorClass = "CAST_OVERFLOW_IN_TABLE_INSERT",
          parameters = Map(
            "sourceType" -> "\"DOUBLE\"",
            "targetType" -> ("\"" + targetType + "\""),
            "columnName" -> "`i`")
        )
      }
    }
  }
}
