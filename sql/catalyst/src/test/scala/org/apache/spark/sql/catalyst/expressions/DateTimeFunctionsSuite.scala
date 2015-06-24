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

class DateTimeFunctionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  val sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
  val d = new Date(sdf.parse("2015/04/08 13:10:15").getTime)
  val ts = new Timestamp(sdf.parse("2013/04/08 13:10:15").getTime)

  test("DateFormat") {
    checkEvaluation(DateFormatClass(Literal(d), Literal("y")), "2015")
    checkEvaluation(DateFormatClass(Literal(d.toString), Literal("y")), "2015")
    checkEvaluation(DateFormatClass(Literal(ts), Literal("y")), "2013")
  }

  test("Year") {
    checkEvaluation(Year(Literal(d)), 2015)
    checkEvaluation(Year(Literal(d.toString)), 2015)
    checkEvaluation(Year(Literal(ts)), 2013)
  }

  test("Month") {
    checkEvaluation(Month(Literal(d)), 4)
    checkEvaluation(Month(Literal(d.toString)), 4)
    checkEvaluation(Month(Literal(ts)), 4)
  }

  test("Day") {
    checkEvaluation(Day(Literal(d)), 8)
    checkEvaluation(Day(Literal(d.toString)), 8)
    checkEvaluation(Day(Literal(ts)), 8)
  }

  test("Hour") {
    checkEvaluation(Hour(Literal(d)), 0)
    checkEvaluation(Hour(Literal(d.toString)), 0)
    checkEvaluation(Hour(Literal(ts)), 13)
  }

  test("Minute") {
    checkEvaluation(Minute(Literal(d)), 0)
    checkEvaluation(Minute(Literal(d.toString)), 0)
    checkEvaluation(Minute(Literal(ts)), 10)
  }

  test("Seconds") {
    checkEvaluation(Second(Literal(d)), 0)
    checkEvaluation(Second(Literal(d.toString)), 0)
    checkEvaluation(Second(Literal(ts)), 15)
  }

  test("WeekOfYear") {
    checkEvaluation(WeekOfYear(Literal(d)), 15)
    checkEvaluation(WeekOfYear(Literal(d.toString)), 15)
    checkEvaluation(WeekOfYear(Literal(ts)), 15)
  }

}
