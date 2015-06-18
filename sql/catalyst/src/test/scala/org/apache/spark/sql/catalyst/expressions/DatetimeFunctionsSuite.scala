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
import org.apache.spark.sql.types.IntegerType

class DatetimeFunctionsSuite extends SparkFunSuite with ExpressionEvalHelper {
  test("date_add") {
    checkEvaluation(
      DateAdd(Literal(Date.valueOf("2016-02-28")), Literal(1)),
      "2016-02-29", Row.empty)
    checkEvaluation(
      DateAdd(Literal("2016-03-01"), Literal(-1)), "2016-02-29", Row.empty)
    checkEvaluation(
      DateAdd(Literal(Timestamp.valueOf("2016-03-01 23:59:59")), Literal(-2)),
      "2016-02-28", Row.empty)
    checkEvaluation(
      DateAdd(Literal("2016-03-01 23:59:59"), Literal(-3)),
      "2016-02-27", Row.empty)
    checkEvaluation(DateAdd(Literal(null), Literal(-1)), null, Row.empty)
  }

  test("date_sub") {
    checkEvaluation(
      DateSub(Literal("2015-01-01"), Literal(1)), "2014-12-31", Row.empty)
    checkEvaluation(
      DateSub(Literal(Date.valueOf("2015-01-01")), Literal(-1)), "2015-01-02", Row.empty)
    checkEvaluation(
      DateSub(Literal(Timestamp.valueOf("2015-01-01 01:00:00")), Literal(-1)),
      "2015-01-02", Row.empty)
    checkEvaluation(
      DateSub(Literal("2015-01-01 01:00:00"), Literal(0)), "2015-01-01", Row.empty)
    checkEvaluation(
      DateSub(Literal("2015-01-01"), Literal.create(null, IntegerType)), null, Row.empty)
  }

}
