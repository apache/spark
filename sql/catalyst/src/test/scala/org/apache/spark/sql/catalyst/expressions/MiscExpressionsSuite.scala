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

package org.apache.spark.sql.catalyst.expressions

import java.sql.{Date, Timestamp}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

class MiscExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("assert_true") {
    intercept[RuntimeException] {
      checkEvaluation(AssertTrue(Literal.create(false, BooleanType)), null)
    }
    intercept[RuntimeException] {
      checkEvaluation(AssertTrue(Cast(Literal(0), BooleanType)), null)
    }
    intercept[RuntimeException] {
      checkEvaluation(AssertTrue(Literal.create(null, NullType)), null)
    }
    intercept[RuntimeException] {
      checkEvaluation(AssertTrue(Literal.create(null, BooleanType)), null)
    }
    checkEvaluation(AssertTrue(Literal.create(true, BooleanType)), null)
    checkEvaluation(AssertTrue(Cast(Literal(1), BooleanType)), null)
  }

  test("uuid") {
    checkEvaluation(Length(Uuid()), 36)
    assert(evaluate(Uuid()) !== evaluate(Uuid()))
  }

  test("trunc numeric") {
    def test(input: Double, fmt: Int, expected: Double): Unit = {
      checkEvaluation(Trunc(Literal.create(input, DoubleType),
        Literal.create(fmt, IntegerType)),
        expected)
      checkEvaluation(Trunc(Literal.create(input, DoubleType),
        NonFoldableLiteral.create(fmt, IntegerType)),
        expected)
    }

    test(1234567891.1234567891, 4, 1234567891.1234)
    test(1234567891.1234567891, -4, 1234560000)
    test(1234567891.1234567891, 0, 1234567891)
    test(0.123, -1, 0)
    test(0.123, 0, 0)

    checkEvaluation(Trunc(Literal.create(1D, DoubleType),
      NonFoldableLiteral.create(null, IntegerType)),
      null)
    checkEvaluation(Trunc(Literal.create(null, DoubleType),
      NonFoldableLiteral.create(1, IntegerType)),
      null)
    checkEvaluation(Trunc(Literal.create(null, DoubleType),
      NonFoldableLiteral.create(null, IntegerType)),
      null)
  }

  test("trunc date") {
    def testDate(input: Date, fmt: String, expected: Date): Unit = {
      checkEvaluation(Trunc(Literal.create(input, DateType), Literal.create(fmt, StringType)),
        expected)
      checkEvaluation(
        Trunc(Literal.create(input, DateType), NonFoldableLiteral.create(fmt, StringType)),
        expected)
    }

    def testString(input: String, fmt: String, expected: Date): Unit = {
      checkEvaluation(Trunc(Literal.create(input, StringType), Literal.create(fmt, StringType)),
        expected)
      checkEvaluation(
        Trunc(Literal.create(input, StringType), NonFoldableLiteral.create(fmt, StringType)),
        expected)
    }

    def testTimestamp(input: Timestamp, fmt: String, expected: Date): Unit = {
      checkEvaluation(Trunc(Literal.create(input, TimestampType), Literal.create(fmt, StringType)),
        expected)
      checkEvaluation(
        Trunc(Literal.create(input, TimestampType), NonFoldableLiteral.create(fmt, StringType)),
        expected)
    }

    val dateStr = "2015-07-22"
    val date = Date.valueOf(dateStr)
    val ts = new Timestamp(date.getTime)

    Seq("yyyy", "YYYY", "year", "YEAR", "yy", "YY").foreach { fmt =>
      testDate(date, fmt, Date.valueOf("2015-01-01"))
      testString(dateStr, fmt, Date.valueOf("2015-01-01"))
      testTimestamp(ts, fmt, Date.valueOf("2015-01-01"))
    }
    Seq("month", "MONTH", "mon", "MON", "mm", "MM").foreach { fmt =>
      testDate(date, fmt, Date.valueOf("2015-07-01"))
      testString(dateStr, fmt, Date.valueOf("2015-07-01"))
      testTimestamp(ts, fmt, Date.valueOf("2015-07-01"))
    }
    testDate(date, "DD", null)
    testDate(date, null, null)
    testDate(null, "MON", null)
    testDate(null, null, null)

    testString(dateStr, "DD", null)
    testString(dateStr, null, null)
    testString(null, "MON", null)
    testString(null, null, null)

    testTimestamp(ts, "DD", null)
    testTimestamp(ts, null, null)
    testTimestamp(null, "MON", null)
    testTimestamp(null, null, null)
  }
}
