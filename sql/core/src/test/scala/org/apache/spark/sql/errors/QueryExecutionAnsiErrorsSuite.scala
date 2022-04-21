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

import org.apache.spark.{SparkArithmeticException, SparkConf, SparkDateTimeException}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

// Test suite for all the execution errors that requires enable ANSI SQL mode.
class QueryExecutionAnsiErrorsSuite extends QueryTest with SharedSparkSession {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")

  test("CAST_CAUSES_OVERFLOW: from timestamp to int") {
    val e = intercept[SparkArithmeticException] {
      sql("select CAST(TIMESTAMP '9999-12-31T12:13:14.56789Z' AS INT)").collect()
    }
    assert(e.getErrorClass === "CAST_CAUSES_OVERFLOW")
    assert(e.getSqlState === "22005")
    assert(e.getMessage === "Casting 253402258394567890L to INT causes overflow. " +
      "To return NULL instead, use 'try_cast'. " +
      "If necessary set spark.sql.ansi.enabled to false to bypass this error.")
  }

  test("DIVIDE_BY_ZERO: can't divide an integer by zero") {
    val e = intercept[SparkArithmeticException] {
      sql("select 6/0").collect()
    }
    assert(e.getErrorClass === "DIVIDE_BY_ZERO")
    assert(e.getSqlState === "22012")
    assert(e.getMessage ===
      "divide by zero. To return NULL instead, use 'try_divide'. If necessary set " +
        "spark.sql.ansi.enabled to false (except for ANSI interval type) to bypass this error." +
        """
          |== SQL(line 1, position 7) ==
          |select 6/0
          |       ^^^
          |""".stripMargin)
  }

  test("INVALID_FRACTION_OF_SECOND: in the function make_timestamp") {
    val e = intercept[SparkDateTimeException] {
      sql("select make_timestamp(2012, 11, 30, 9, 19, 60.66666666)").collect()
    }
    assert(e.getErrorClass === "INVALID_FRACTION_OF_SECOND")
    assert(e.getSqlState === "22023")
    assert(e.getMessage === "The fraction of sec must be zero. Valid range is [0, 60]. " +
      "If necessary set spark.sql.ansi.enabled to false to bypass this error. ")
  }

  test("CANNOT_CHANGE_DECIMAL_PRECISION: cast string to decimal") {
    val e = intercept[SparkArithmeticException] {
      sql("select CAST('66666666666666.666' AS DECIMAL(8, 1))").collect()
    }
    assert(e.getErrorClass === "CANNOT_CHANGE_DECIMAL_PRECISION")
    assert(e.getSqlState === "22005")
    assert(e.getMessage ===
      "Decimal(expanded,66666666666666.666,17,3}) cannot be represented as Decimal(8, 1). " +
        "If necessary set spark.sql.ansi.enabled to false to bypass this error." +
        """
          |== SQL(line 1, position 7) ==
          |select CAST('66666666666666.666' AS DECIMAL(8, 1))
          |       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
          |""".stripMargin)
  }
}
