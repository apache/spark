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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.test.SharedSparkSession

class QueryParsingErrorsSuite extends QueryTest with SharedSparkSession {

  test("MORE_THAN_ONE_FROM_TO_UNIT_IN_INTERVAL_LITERAL: from-to unit in the interval literal") {
    val e = intercept[ParseException] {
      spark.sql("SELECT INTERVAL 1 to 3 year to month AS col")
    }
    assert(e.getErrorClass === "MORE_THAN_ONE_FROM_TO_UNIT_IN_INTERVAL_LITERAL")
    assert(e.getMessage.contains(
      "Can only have a single from-to unit in the interval literal syntax"))
  }

  test("INVALID_INTERVAL_LITERAL: invalid interval literal") {
    val e = intercept[ParseException] {
      spark.sql("SELECT INTERVAL DAY")
    }
    assert(e.getErrorClass === "INVALID_INTERVAL_LITERAL")
    assert(e.getMessage.contains(
      "at least one time unit should be given for interval literal"))
  }

  test("INVALID_FROM_TO_UNIT_VALUE: value of from-to unit must be a string") {
    val e = intercept[ParseException] {
      spark.sql("SELECT INTERVAL -2021 YEAR TO MONTH")
    }
    assert(e.getErrorClass === "INVALID_FROM_TO_UNIT_VALUE")
    assert(e.getMessage.contains(
      "The value of from-to unit must be a string"))
  }

  test("UNSUPPORTED_FROM_TO_INTERVAL: Unsupported from-to interval") {
    val e = intercept[ParseException] {
      spark.sql("SELECT extract(MONTH FROM INTERVAL '2021-11' YEAR TO DAY)")
    }
    assert(e.getErrorClass === "UNSUPPORTED_FROM_TO_INTERVAL")
    assert(e.getMessage.contains(
      "Intervals FROM YEAR TO DAY are not supported."))
  }

  test("MIXED_INTERVAL_UNITS: Cannot mix year-month and day-time fields") {
    val e = intercept[ParseException] {
      spark.sql("SELECT INTERVAL 1 MONTH 2 HOUR")
    }
    assert(e.getErrorClass === "MIXED_INTERVAL_UNITS")
    assert(e.getMessage.contains(
      "Cannot mix year-month and day-time fields"))
  }

  test("INVALID_INTERVAL_FORM: invalid interval form") {
    val e = intercept[ParseException] {
      spark.sql("SELECT INTERVAL '1 DAY 2' HOUR")
    }
    assert(e.getErrorClass === "INVALID_INTERVAL_FORM")
    assert(e.getMessage.contains(
      "numbers in the interval value part for multiple unit value pairs interval form"))
  }
}
