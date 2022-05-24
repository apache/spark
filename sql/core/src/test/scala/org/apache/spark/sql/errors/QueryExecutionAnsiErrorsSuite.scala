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

import org.apache.spark.{SparkArithmeticException, SparkArrayIndexOutOfBoundsException, SparkConf, SparkDateTimeException, SparkNoSuchElementException, SparkNumberFormatException}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.SQLConf

// Test suite for all the execution errors that requires enable ANSI SQL mode.
class QueryExecutionAnsiErrorsSuite extends QueryTest with QueryErrorsSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")

  private val ansiConf = "\"" + SQLConf.ANSI_ENABLED.key + "\""

  test("CAST_OVERFLOW: from timestamp to int") {
    checkErrorClass(
      exception = intercept[SparkArithmeticException] {
        sql("select CAST(TIMESTAMP '9999-12-31T12:13:14.56789Z' AS INT)").collect()
      },
      errorClass = "CAST_OVERFLOW",
      msg =
        "The value TIMESTAMP '9999-12-31 04:13:14.56789' of the type \"TIMESTAMP\" cannot be cast" +
        " to \"INT\" due to an overflow. To return NULL instead, use `try_cast`. " +
        s"""If necessary set $ansiConf to "false" to bypass this error.""",
      sqlState = Some("22005"))
  }

  test("DIVIDE_BY_ZERO: can't divide an integer by zero") {
    checkErrorClass(
      exception = intercept[SparkArithmeticException] {
        sql("select 6/0").collect()
      },
      errorClass = "DIVIDE_BY_ZERO",
      msg =
        "Division by zero. To return NULL instead, use `try_divide`. If necessary set " +
        s"""$ansiConf to "false" (except for ANSI interval type) to bypass this error.""" +
        """
          |== SQL(line 1, position 7) ==
          |select 6/0
          |       ^^^
          |""".stripMargin,
      sqlState = Some("22012"))
  }

  test("INVALID_FRACTION_OF_SECOND: in the function make_timestamp") {
    checkErrorClass(
      exception = intercept[SparkDateTimeException] {
        sql("select make_timestamp(2012, 11, 30, 9, 19, 60.66666666)").collect()
      },
      errorClass = "INVALID_FRACTION_OF_SECOND",
      msg = "The fraction of sec must be zero. Valid range is [0, 60]. " +
        s"""If necessary set $ansiConf to "false" to bypass this error. """,
      sqlState = Some("22023"))
  }

  test("CANNOT_CHANGE_DECIMAL_PRECISION: cast string to decimal") {
    checkErrorClass(
      exception = intercept[SparkArithmeticException] {
        sql("select CAST('66666666666666.666' AS DECIMAL(8, 1))").collect()
      },
      errorClass = "CANNOT_CHANGE_DECIMAL_PRECISION",
      msg =
        "Decimal(expanded, 66666666666666.666, 17, 3) cannot be represented as Decimal(8, 1). " +
        s"""If necessary set $ansiConf to "false" to bypass this error.""" +
        """
          |== SQL(line 1, position 7) ==
          |select CAST('66666666666666.666' AS DECIMAL(8, 1))
          |       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
          |""".stripMargin,
      sqlState = Some("22005"))
  }

  test("INVALID_ARRAY_INDEX: get element from array") {
    checkErrorClass(
      exception = intercept[SparkArrayIndexOutOfBoundsException] {
        sql("select array(1, 2, 3, 4, 5)[8]").collect()
      },
      errorClass = "INVALID_ARRAY_INDEX",
      msg = "The index 8 is out of bounds. The array has 5 elements. " +
        s"""If necessary set $ansiConf to "false" to bypass this error."""
    )
  }

  test("INVALID_ARRAY_INDEX_IN_ELEMENT_AT: element_at from array") {
    checkErrorClass(
      exception = intercept[SparkArrayIndexOutOfBoundsException] {
        sql("select element_at(array(1, 2, 3, 4, 5), 8)").collect()
      },
      errorClass = "INVALID_ARRAY_INDEX_IN_ELEMENT_AT",
      msg = "The index 8 is out of bounds. The array has 5 elements. " +
        "To return NULL instead, use `try_element_at`. " +
        s"""If necessary set $ansiConf to "false" to bypass this error."""
    )
  }

  test("MAP_KEY_DOES_NOT_EXIST: key does not exist in element_at") {
    val e = intercept[SparkNoSuchElementException] {
      sql("select element_at(map(1, 'a', 2, 'b'), 3)").collect()
    }
    checkErrorClass(
      exception = e,
      errorClass = "MAP_KEY_DOES_NOT_EXIST",
      msg = "Key 3 does not exist. To return NULL instead, use `try_element_at`. " +
        s"""If necessary set $ansiConf to "false" to bypass this error.""" +
        """
          |== SQL(line 1, position 7) ==
          |select element_at(map(1, 'a', 2, 'b'), 3)
          |       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
          |""".stripMargin
    )
  }

  test("CAST_INVALID_INPUT: cast string to double") {
    checkErrorClass(
      exception = intercept[SparkNumberFormatException] {
        sql("select CAST('111111111111xe23' AS DOUBLE)").collect()
      },
      errorClass = "CAST_INVALID_INPUT",
      msg = """The value '111111111111xe23' of the type "STRING" cannot be cast to "DOUBLE" """ +
        "because it is malformed. Correct the value as per the syntax, " +
        "or change its target type. To return NULL instead, use `try_cast`. If necessary set " +
        s"""$ansiConf to \"false\" to bypass this error.
          |== SQL(line 1, position 7) ==
          |select CAST('111111111111xe23' AS DOUBLE)
          |       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
          |""".stripMargin)
  }
}
