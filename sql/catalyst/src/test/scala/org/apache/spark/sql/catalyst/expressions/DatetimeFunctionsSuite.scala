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
import org.apache.spark.sql.catalyst.util.DateUtils

class DatetimeFunctionsSuite extends SparkFunSuite with ExpressionEvalHelper {
  test("date_add") {
    checkEvaluation(
      DateAdd(Literal(Date.valueOf("2016-02-28")), Literal(1)),
      DateUtils.fromJavaDate(Date.valueOf("2016-02-29")), create_row(null))
    checkEvaluation(
      DateAdd(Literal(Date.valueOf("2016-03-01")), Literal(-1)),
      DateUtils.fromJavaDate(Date.valueOf("2016-02-29")), create_row(null))
    checkEvaluation(DateAdd(Literal(null), Literal(-1)), null, create_row(null))
  }

  test("date_sub") {
    checkEvaluation(
      DateSub(Literal(Date.valueOf("2015-01-01")), Literal(1)),
      DateUtils.fromJavaDate(Date.valueOf("2014-12-31")), create_row(null))
    checkEvaluation(
      DateSub(Literal(Date.valueOf("2015-01-01")), Literal(-1)),
      DateUtils.fromJavaDate(Date.valueOf("2015-01-02")), create_row(null))
    checkEvaluation(
      DateSub(Literal(Date.valueOf("2015-01-01")), Literal(null)), null, create_row(null))
  }

  test("date_diff") {
    checkEvaluation(
      DateDiff(Literal(Date.valueOf("2015-01-01")), Literal(Date.valueOf("2015-01-01"))),
      0, create_row(null))
    checkEvaluation(
      DateDiff(Literal(Date.valueOf("2015-06-01")), Literal(Date.valueOf("2015-06-12"))),
      -11, create_row(null))
    checkEvaluation(
      DateDiff(Literal(Date.valueOf("2015-06-01")), Literal(Date.valueOf("2015-05-31"))),
      1, create_row(null))
    checkEvaluation(DateDiff(Literal(null), Literal(null)), null, create_row(null))
  }
}
