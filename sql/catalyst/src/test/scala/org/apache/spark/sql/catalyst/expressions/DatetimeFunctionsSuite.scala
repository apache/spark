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
import java.text.SimpleDateFormat
import java.util.{TimeZone, Calendar}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{StringType, DateType, TimestampType}

class DatetimeFunctionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  val oldDefault = TimeZone.getDefault

  test("datetime function current_date") {
    val d0 = DateTimeUtils.millisToDays(System.currentTimeMillis())
    val cd = CurrentDate().eval(EmptyRow).asInstanceOf[Int]
    val d1 = DateTimeUtils.millisToDays(System.currentTimeMillis())
    assert(d0 <= cd && cd <= d1 && d1 - d0 <= 1)
  }

  test("datetime function current_timestamp") {
    val ct = DateTimeUtils.toJavaTimestamp(CurrentTimestamp().eval(EmptyRow).asInstanceOf[Long])
    val t1 = System.currentTimeMillis()
    assert(math.abs(t1 - ct.getTime) < 5000)
  }

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val sdfDate = new SimpleDateFormat("yyyy-MM-dd")
  val d = new Date(sdf.parse("2015-04-08 13:10:15").getTime)
  val ts = new Timestamp(sdf.parse("2013-11-08 13:10:15").getTime)

  test("Year") {
    checkEvaluation(Year(Literal.create(null, DateType)), null)
    checkEvaluation(Year(Cast(Literal(d), DateType)), 2015)
    checkEvaluation(Year(Cast(Literal(sdfDate.format(d)), DateType)), 2015)
    checkEvaluation(Year(Cast(Literal(ts), DateType)), 2013)

    val c = Calendar.getInstance()
    (2000 to 2010).foreach { y =>
      (0 to 11 by 11).foreach { m =>
        c.set(y, m, 28)
        (0 to 5 * 24).foreach { i =>
          c.add(Calendar.HOUR_OF_DAY, 1)
          checkEvaluation(Year(Cast(Literal(new Date(c.getTimeInMillis)), DateType)),
            c.get(Calendar.YEAR))
        }
      }
    }
  }

  test("millis to date") {
    var l: Long = 970383600696L
    var lBase = l + TimeZone.getDefault.getOffset(l)
    System.out.println(new Date(l))
    assert(DateTimeUtils.millisToDays(l) == lBase / 3600 / 24 / 1000)

    l = 970383600977L
    lBase = l + TimeZone.getDefault.getOffset(l)
    System.out.println(new Date(l))
    System.out.println(DateTimeUtils.millisToDays(l))
    println(new SimpleDateFormat("D").format(new Date(l)))
    println(lBase / 3600 / 24 / 1000)
    assert(DateTimeUtils.millisToDays(l) == lBase / 3600 / 24 / 1000)
  }

  test("Quarter") {
    checkEvaluation(Quarter(Literal.create(null, DateType)), null)
    checkEvaluation(Quarter(Cast(Literal(d), DateType)), 2)
    checkEvaluation(Quarter(Cast(Literal(sdfDate.format(d)), DateType)), 2)
    checkEvaluation(Quarter(Cast(Literal(ts), DateType)), 4)

    val c = Calendar.getInstance()
    (2003 to 2004).foreach { y =>
      (0 to 11 by 3).foreach { m =>
        c.set(y, m, 28, 0, 0, 0)
        (0 to 5 * 24).foreach { i =>
          c.add(Calendar.HOUR_OF_DAY, 1)
          checkEvaluation(Quarter(Cast(Literal(new Date(c.getTimeInMillis)), DateType)),
            c.get(Calendar.MONTH) / 3 + 1)
        }
      }
    }
  }

  test("Month") {
    /*checkEvaluation(Month(Literal.create(null, DateType)), null)
    checkEvaluation(Month(Cast(Literal(d), DateType)), 4)
    checkEvaluation(Month(Cast(Literal(sdfDate.format(d)), DateType)), 4)
    checkEvaluation(Month(Cast(Literal(ts), DateType)), 11)*/

    val x = Calendar.getInstance()

    x.setTimeInMillis(946713600707L)
    println(x.getTime)
    println(Cast(Literal(new Date(x.getTimeInMillis)), DateType).eval(null))
    checkEvaluation(Month(Cast(Literal(new Date(x.getTimeInMillis)), DateType)),
      x.get(Calendar.MONTH) + 1)

    x.setTimeInMillis(1072944000077L)
    println(x.getTime)
    println(Cast(Literal(new Date(x.getTimeInMillis)), DateType).eval(null))
    checkEvaluation(Month(Cast(Literal(new Date(x.getTimeInMillis)), DateType)),
      x.get(Calendar.MONTH) + 1)

    x.setTimeInMillis(981014400345L)
    println(x.getTime)
    println(Cast(Literal(new Date(x.getTimeInMillis)), DateType).eval(null))
    checkEvaluation(Month(Cast(Literal(new Date(x.getTimeInMillis)), DateType)),
      x.get(Calendar.MONTH) + 1)

    x.setTimeInMillis(978249600433L)
    println(x.getTime)
    println(Cast(Literal(new Date(x.getTimeInMillis)), DateType).eval(null))
    checkEvaluation(Month(Cast(Literal(new Date(x.getTimeInMillis)), DateType)),
      x.get(Calendar.MONTH) + 1)

    x.setTimeInMillis(970383600000L)
    println(x.getTime)
    println(Cast(Literal(new Date(x.getTimeInMillis)), DateType).eval(null))
    checkEvaluation(Month(Cast(Literal(new Date(x.getTimeInMillis)), DateType)),
      x.get(Calendar.MONTH) + 1)

    x.setTimeInMillis(954489600409L)
    println(x.getTime)
    println(Cast(Literal(new Date(x.getTimeInMillis)), DateType).eval(null))
    checkEvaluation(Month(Cast(Literal(new Date(x.getTimeInMillis)), DateType)),
      x.get(Calendar.MONTH) + 1)

    (2003 to 2004).foreach { y =>
      (0 to 11).foreach { m =>
        (0 to 5 * 24).foreach { i =>
          val c = Calendar.getInstance()
          c.set(y, m, 28, 0, 0, 0)
          c.add(Calendar.HOUR_OF_DAY, i)
          checkEvaluation(Month(Cast(Literal(new Date(c.getTimeInMillis)), DateType)),
            c.get(Calendar.MONTH) + 1)
        }
      }
    }

    val sdf = new SimpleDateFormat("D")
    (1998 to 2005).foreach { y =>
      (0 to 11).foreach { m =>
        (0 to 5 * 24).foreach { i =>
          val c = Calendar.getInstance()
          c.set(y, m, 28, 0, 0, 0)
          c.add(Calendar.HOUR_OF_DAY, i)
          println(Cast(Literal(new Date(c.getTimeInMillis)), DateType).eval(null) + " " + sdf.format(c.getTime) + " " + c.getTimeInMillis)
          checkEvaluation(Month(Cast(Literal(new Date(c.getTimeInMillis)), DateType)),
            c.get(Calendar.MONTH) + 1)
        }
      }
    }
  }

  test("Day") {
    checkEvaluation(Day(Literal.create(null, DateType)), null)
    checkEvaluation(Day(Cast(Literal(d), DateType)), 8)
    checkEvaluation(Day(Cast(Literal(sdfDate.format(d)), DateType)), 8)
    checkEvaluation(Day(Cast(Literal(ts), DateType)), 8)
  }

  test("Seconds") {
    checkEvaluation(Second(Literal.create(null, DateType)), null)
    checkEvaluation(Second(Cast(Literal(d), TimestampType)), 0)
    checkEvaluation(Second(Cast(Literal(sdf.format(d)), TimestampType)), 15)
    checkEvaluation(Second(Literal(ts)), 15)

    val c = Calendar.getInstance()
    (0 to 60 by 5).foreach { s =>
      c.set(2015, 18, 3, 3, 5, s)
      checkEvaluation(Second(Cast(Literal(new Timestamp(c.getTimeInMillis)), TimestampType)),
        c.get(Calendar.SECOND))
    }
  }

  testWithTimezone(TimeZone.getTimeZone("GMT-14:00"))
  testWithTimezone(TimeZone.getTimeZone("GMT+04:30"))
  testWithTimezone(TimeZone.getTimeZone("GMT+12:00"))

  def testWithTimezone(tz: TimeZone) {
    TimeZone.setDefault(tz)
    test("DateFormat - " + tz.getID) {
      checkEvaluation(DateFormatClass(Literal.create(null, TimestampType), Literal("y")), null)
      checkEvaluation(DateFormatClass(Cast(Literal(d), TimestampType),
        Literal.create(null, StringType)), null)
      checkEvaluation(DateFormatClass(Cast(Literal(d), TimestampType),
        Literal("y")), "2015")
      checkEvaluation(DateFormatClass(Literal(ts), Literal("y")), "2013")
    }

    test("Hour - " + tz.getID) {
      checkEvaluation(Hour(Literal.create(null, DateType)), null)
      checkEvaluation(Hour(Cast(Literal(d), TimestampType)), 0)
      checkEvaluation(Hour(Cast(Literal(sdf.format(d)), TimestampType)), 13)
      checkEvaluation(Hour(Literal(ts)), 13)

      val c = Calendar.getInstance()
      (0 to 24).foreach { h =>
        (0 to 60 by 15).foreach { m =>
          (0 to 60 by 15).foreach { s =>
            c.set(2015, 18, 3, h, m, s)
            checkEvaluation(Hour(Cast(Literal(new Timestamp(c.getTimeInMillis)), TimestampType)),
              c.get(Calendar.HOUR_OF_DAY))
          }
        }
      }
    }

    test("Minute - " + tz.getID) {
      checkEvaluation(Minute(Literal.create(null, DateType)), null)
      checkEvaluation(Minute(Cast(Literal(d), TimestampType)), 0)
      checkEvaluation(Minute(Cast(Literal(sdf.format(d)), TimestampType)), 10)
      checkEvaluation(Minute(Literal(ts)), 10)

      val c = Calendar.getInstance()
      (0 to 60 by 5).foreach { m =>
        (0 to 60 by 15).foreach { s =>
          c.set(2015, 18, 3, 3, m, s)
          checkEvaluation(Minute(Cast(Literal(new Timestamp(c.getTimeInMillis)), TimestampType)),
            c.get(Calendar.MINUTE))
        }
      }
    }

    test("WeekOfYear - " + tz.getID) {
      checkEvaluation(WeekOfYear(Literal.create(null, DateType)), null)
      checkEvaluation(WeekOfYear(Cast(Literal(d), DateType)), 15)
      checkEvaluation(WeekOfYear(Cast(Literal(sdfDate.format(d)), DateType)), 15)
      checkEvaluation(WeekOfYear(Cast(Literal(ts), DateType)), 45)
    }

    TimeZone.setDefault(oldDefault)
  }

}
