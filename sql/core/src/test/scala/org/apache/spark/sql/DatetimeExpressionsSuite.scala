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

import java.sql.{Timestamp, Date}
import java.text.SimpleDateFormat

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.functions._

class DatetimeExpressionsSuite extends QueryTest {
  private lazy val ctx = org.apache.spark.sql.test.TestSQLContext

  import ctx.implicits._

  lazy val df1 = Seq((1, 2), (3, 1)).toDF("a", "b")

  test("function current_date") {
    val d0 = DateTimeUtils.millisToDays(System.currentTimeMillis())
    val d1 = DateTimeUtils.fromJavaDate(df1.select(current_date()).collect().head.getDate(0))
    val d2 = DateTimeUtils.fromJavaDate(
      ctx.sql("""SELECT CURRENT_DATE()""").collect().head.getDate(0))
    val d3 = DateTimeUtils.millisToDays(System.currentTimeMillis())
    assert(d0 <= d1 && d1 <= d2 && d2 <= d3 && d3 - d0 <= 1)
  }

  test("function current_timestamp") {
    checkAnswer(df1.select(countDistinct(current_timestamp())), Row(1))
    // Execution in one query should return the same value
    checkAnswer(ctx.sql("""SELECT CURRENT_TIMESTAMP() = CURRENT_TIMESTAMP()"""),
      Row(true))
    assert(math.abs(ctx.sql("""SELECT CURRENT_TIMESTAMP()""").collect().head.getTimestamp(
      0).getTime - System.currentTimeMillis()) < 5000)
  }



  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val sdfDate = new SimpleDateFormat("yyyy-MM-dd")
  val d = new Date(sdf.parse("2015-04-08 13:10:15").getTime)
  val ts = new Timestamp(sdf.parse("2013-04-08 13:10:15").getTime)


  test("date format") {
    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(dateFormat("a", "y"), dateFormat("b", "y"), dateFormat("c", "y")),
      Row("2015", "2015", "2013"))

    checkAnswer(
      df.selectExpr("dateFormat(a, 'y')", "dateFormat(b, 'y')", "dateFormat(c, 'y')"),
      Row("2015", "2015", "2013"))
  }

  test("year") {
    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(year("a"), year("b"), year("c")),
      Row(2015, 2015, 2013))

    checkAnswer(
      df.selectExpr("year(a)", "year(b)", "year(c)"),
      Row(2015, 2015, 2013))
  }

  test("quarter") {
    val ts = new Timestamp(sdf.parse("2013-11-08 13:10:15").getTime)

    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(quarter("a"), quarter("b"), quarter("c")),
      Row(2, 2, 4))

    checkAnswer(
      df.selectExpr("quarter(a)", "quarter(b)", "quarter(c)"),
      Row(2, 2, 4))
  }

  test("month") {
    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(month("a"), month("b"), month("c")),
      Row(4, 4, 4))

    checkAnswer(
      df.selectExpr("month(a)", "month(b)", "month(c)"),
      Row(4, 4, 4))
  }

  test("day") {
    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(day("a"), day("b"), day("c")),
      Row(8, 8, 8))

    checkAnswer(
      df.selectExpr("day(a)", "day(b)", "day(c)"),
      Row(8, 8, 8))

    checkAnswer(
      df.selectExpr("day(CAST(\"2008-11-01 15:32:20\" AS DATE))"),
      Row(1)
    )
  }

  test("dayInYear") {
    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(dayInYear("a"), dayInYear("b"), dayInYear("c")),
      Row(98, 98, 98))

    checkAnswer(
      df.selectExpr("dayInYear(a)", "dayInYear(b)", "dayInYear(c)"),
      Row(98, 98, 98))
  }

  test("hour") {
    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(hour("a"), hour("b"), hour("c")),
      Row(0, 13, 13))

    checkAnswer(
      df.selectExpr("hour(a)", "hour(b)", "hour(c)"),
      Row(0, 13, 13))
  }

  test("minute") {
    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(minute("a"), minute("b"), minute("c")),
      Row(0, 10, 10))

    checkAnswer(
      df.selectExpr("minute(a)", "minute(b)", "minute(c)"),
      Row(0, 10, 10))
  }

  test("second") {
    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(second("a"), second("b"), second("c")),
      Row(0, 15, 15))

    checkAnswer(
      df.selectExpr("second(a)", "second(b)", "second(c)"),
      Row(0, 15, 15))
  }

  test("weekOfYear") {
    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(weekOfYear("a"), weekOfYear("b"), weekOfYear("c")),
      Row(15, 15, 15))

    checkAnswer(
      df.selectExpr("weekOfYear(a)", "weekOfYear(b)", "weekOfYear(c)"),
      Row(15, 15, 15))
  }
}
