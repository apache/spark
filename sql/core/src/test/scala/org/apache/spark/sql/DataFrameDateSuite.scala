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

package org.apache.spark.sql

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import org.apache.spark.sql.functions._

class DataFrameDateTimeSuite extends QueryTest {

  private lazy val ctx = org.apache.spark.sql.test.TestSQLContext
  import ctx.implicits._

  test("timestamp comparison with date strings") {
    val df = Seq(
      (1, Timestamp.valueOf("2015-01-01 00:00:00")),
      (2, Timestamp.valueOf("2014-01-01 00:00:00"))).toDF("i", "t")

    checkAnswer(
      df.select("t").filter($"t" <= "2014-06-01"),
      Row(Timestamp.valueOf("2014-01-01 00:00:00")) :: Nil)


    checkAnswer(
      df.select("t").filter($"t" >= "2014-06-01"),
      Row(Timestamp.valueOf("2015-01-01 00:00:00")) :: Nil)
  }

  test("date comparison with date strings") {
    val df = Seq(
      (1, Date.valueOf("2015-01-01")),
      (2, Date.valueOf("2014-01-01"))).toDF("i", "t")

    checkAnswer(
      df.select("t").filter($"t" <= "2014-06-01"),
      Row(Date.valueOf("2014-01-01")) :: Nil)


    checkAnswer(
      df.select("t").filter($"t" >= "2015"),
      Row(Date.valueOf("2015-01-01")) :: Nil)
  }



  test("date format") {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val d = new Date(sdf.parse("2015-04-08 13:10:15").getTime)
    val ts = new Timestamp(sdf.parse("2013-04-08 13:10:15").getTime)

    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(dateFormat("a", "y"), dateFormat("b", "y"), dateFormat("c", "y")),
      Row("2015", "2015", "2013"))

    checkAnswer(
      df.selectExpr("dateFormat(a, 'y')", "dateFormat(b, 'y')", "dateFormat(c, 'y')"),
      Row("2015", "2015", "2013"))
  }

  test("year") {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val d = new Date(sdf.parse("2015-04-08 13:10:15").getTime)
    val ts = new Timestamp(sdf.parse("2013-04-08 13:10:15").getTime)

    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(year("a"), year("b"), year("c")),
      Row(2015, 2015, 2013))

    checkAnswer(
      df.selectExpr("year(a)", "year(b)", "year(c)"),
      Row(2015, 2015, 2013))
  }

  test("quarter") {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val d = new Date(sdf.parse("2015-04-08 13:10:15").getTime)
    val ts = new Timestamp(sdf.parse("2013-11-08 13:10:15").getTime)

    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(quarter("a"), quarter("b"), quarter("c")),
      Row(2, 2, 4))

    checkAnswer(
      df.selectExpr("quarter(a)", "quarter(b)", "quarter(c)"),
      Row(2, 2, 4))
  }

  test("month") {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val d = new Date(sdf.parse("2015-04-08 13:10:15").getTime)
    val ts = new Timestamp(sdf.parse("2013-04-08 13:10:15").getTime)

    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(month("a"), month("b"), month("c")),
      Row(4, 4, 4))

    checkAnswer(
      df.selectExpr("month(a)", "month(b)", "month(c)"),
      Row(4, 4, 4))
  }

  test("day") {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val d = new Date(sdf.parse("2015-04-08 13:10:15").getTime)
    val ts = new Timestamp(sdf.parse("2013-04-08 13:10:15").getTime)

    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(day("a"), day("b"), day("c")),
      Row(8, 8, 8))

    checkAnswer(
      df.selectExpr("day(a)", "day(b)", "day(c)"),
      Row(8, 8, 8))
  }

  test("hour") {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val d = new Date(sdf.parse("2015-04-08 13:10:15").getTime)
    val ts = new Timestamp(sdf.parse("2013-04-08 13:10:15").getTime)

    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(hour("a"), hour("b"), hour("c")),
      Row(0, 13, 13))

    checkAnswer(
      df.selectExpr("hour(a)", "hour(b)", "hour(c)"),
      Row(0, 13, 13))
  }

  test("minute") {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val d = new Date(sdf.parse("2015-04-08 13:10:15").getTime)
    val ts = new Timestamp(sdf.parse("2013-04-08 13:10:15").getTime)

    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(minute("a"), minute("b"), minute("c")),
      Row(0, 10, 10))

    checkAnswer(
      df.selectExpr("minute(a)", "minute(b)", "minute(c)"),
      Row(0, 10, 10))
  }

  test("second") {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val d = new Date(sdf.parse("2015-04-08 13:10:15").getTime)
    val ts = new Timestamp(sdf.parse("2013-04-08 13:10:15").getTime)

    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(second("a"), second("b"), second("c")),
      Row(0, 15, 15))

    checkAnswer(
      df.selectExpr("second(a)", "second(b)", "second(c)"),
      Row(0, 15, 15))
  }

  test("weekOfYear") {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val d = new Date(sdf.parse("2015-04-08 13:10:15").getTime)
    val ts = new Timestamp(sdf.parse("2013-04-08 13:10:15").getTime)

    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(weekOfYear("a"), weekOfYear("b"), weekOfYear("c")),
      Row(15, 15, 15))

    checkAnswer(
      df.selectExpr("weekOfYear(a)", "weekOfYear(b)", "weekOfYear(c)"),
      Row(15, 15, 15))
  }

}
