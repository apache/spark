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

import java.sql.Date

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

  test("trunc") {
    // numeric
    def testTruncNumber(input: Double, fmt: Int, expected: Double): Unit = {
      checkEvaluation(Trunc(Literal.create(input, DoubleType),
        Literal.create(fmt, IntegerType)),
        expected)
      checkEvaluation(Trunc(Literal.create(input, DoubleType),
        NonFoldableLiteral.create(fmt, IntegerType)),
        expected)
    }

    testTruncNumber(1234567891.1234567891, 4, 1234567891.1234)
    testTruncNumber(1234567891.1234567891, -4, 1234560000)
    testTruncNumber(1234567891.1234567891, 0, 1234567891)

    checkEvaluation(Trunc(Literal.create(1D, DoubleType),
      NonFoldableLiteral.create(null, IntegerType)),
      null)
    checkEvaluation(Trunc(Literal.create(null, DoubleType),
      NonFoldableLiteral.create(1, IntegerType)),
      null)
    checkEvaluation(Trunc(Literal.create(null, DoubleType),
      NonFoldableLiteral.create(null, IntegerType)),
      null)

    // date
    def testTruncDate(input: Date, fmt: String, expected: Date): Unit = {
      checkEvaluation(Trunc(Literal.create(input, DateType), Literal.create(fmt, StringType)),
        expected)
      checkEvaluation(
        Trunc(Literal.create(input, DateType), NonFoldableLiteral.create(fmt, StringType)),
        expected)
    }
    val date = Date.valueOf("2015-07-22")
    Seq("yyyy", "YYYY", "year", "YEAR", "yy", "YY").foreach { fmt =>
      testTruncDate(date, fmt, Date.valueOf("2015-01-01"))
    }
    Seq("month", "MONTH", "mon", "MON", "mm", "MM").foreach { fmt =>
      testTruncDate(date, fmt, Date.valueOf("2015-07-01"))
    }
    testTruncDate(date, "DD", null)
    testTruncDate(date, null, null)
    testTruncDate(null, "MON", null)
    testTruncDate(null, null, null)
  }
}
