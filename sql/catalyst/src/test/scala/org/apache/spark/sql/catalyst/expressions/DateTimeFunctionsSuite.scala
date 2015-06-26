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

import java.sql.{Timestamp, Date}
import java.text.SimpleDateFormat

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.{StringType, DateType}

class DateTimeFunctionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val d = new Date(sdf.parse("2015-04-08 13:10:15").getTime)
  val ts = new Timestamp(sdf.parse("2013-11-08 13:10:15").getTime)

  test("DateFormat") {
    checkEvaluation(DateFormatClass(Literal.create(null, DateType), Literal("y")), null)
    checkEvaluation(DateFormatClass(Literal(d), Literal.create(null, StringType)), null)
    checkEvaluation(DateFormatClass(Literal(d), Literal("y")), "2015")
    checkEvaluation(DateFormatClass(Literal(sdf.format(d)), Literal("y")), "2015")
    checkEvaluation(DateFormatClass(Literal(ts), Literal("y")), "2013")
  }

  test("Year") {
    checkEvaluation(Year(Literal.create(null, DateType)), null)
    checkEvaluation(Year(Literal(d)), 2015)
    checkEvaluation(Year(Literal(sdf.format(d))), 2015)
    checkEvaluation(Year(Literal(ts)), 2013)
  }

  test("Quarter") {
    checkEvaluation(Quarter(Literal.create(null, DateType)), null)
    checkEvaluation(Quarter(Literal(d)), 2)
    checkEvaluation(Quarter(Literal(sdf.format(d))), 2)
    checkEvaluation(Quarter(Literal(ts)), 4)
  }

  test("Month") {
    checkEvaluation(Month(Literal.create(null, DateType)), null)
    checkEvaluation(Month(Literal(d)), 4)
    checkEvaluation(Month(Literal(sdf.format(d))), 4)
    checkEvaluation(Month(Literal(ts)), 11)
  }

  test("Day") {
    checkEvaluation(Day(Literal.create(null, DateType)), null)
    checkEvaluation(Day(Literal(d)), 8)
    checkEvaluation(Day(Literal(sdf.format(d))), 8)
    checkEvaluation(Day(Literal(ts)), 8)
  }

  test("Hour") {
    checkEvaluation(Hour(Literal.create(null, DateType)), null)
    checkEvaluation(Hour(Literal(d)), 0)
    checkEvaluation(Hour(Literal(sdf.format(d))), 13)
    checkEvaluation(Hour(Literal(ts)), 13)
  }

  test("Minute") {
    checkEvaluation(Minute(Literal.create(null, DateType)), null)
    checkEvaluation(Minute(Literal(d)), 0)
    checkEvaluation(Minute(Literal(sdf.format(d))), 10)
    checkEvaluation(Minute(Literal(ts)), 10)
  }

  test("Seconds") {
    checkEvaluation(Second(Literal.create(null, DateType)), null)
    checkEvaluation(Second(Literal(d)), 0)
    checkEvaluation(Second(Literal(sdf.format(d))), 15)
    checkEvaluation(Second(Literal(ts)), 15)
  }

  test("WeekOfYear") {
    checkEvaluation(WeekOfYear(Literal.create(null, DateType)), null)
    checkEvaluation(WeekOfYear(Literal(d)), 15)
    checkEvaluation(WeekOfYear(Literal(sdf.format(d))), 15)
    checkEvaluation(WeekOfYear(Literal(ts)), 45)
  }

}
