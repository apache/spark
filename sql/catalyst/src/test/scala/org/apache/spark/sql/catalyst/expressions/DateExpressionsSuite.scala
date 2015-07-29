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
import java.util.Calendar

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{StringType, TimestampType, DateType}

class DateExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val sdfDate = new SimpleDateFormat("yyyy-MM-dd")
  val d = new Date(sdf.parse("2015-04-08 13:10:15").getTime)
  val ts = new Timestamp(sdf.parse("2013-11-08 13:10:15").getTime)

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

  test("DayOfYear") {
    val sdfDay = new SimpleDateFormat("D")
    (2002 to 2004).foreach { y =>
      (0 to 11).foreach { m =>
        (0 to 5).foreach { i =>
          val c = Calendar.getInstance()
          c.set(y, m, 28, 0, 0, 0)
          c.add(Calendar.DATE, i)
          checkEvaluation(DayOfYear(Literal(new Date(c.getTimeInMillis))),
            sdfDay.format(c.getTime).toInt)
        }
      }
    }

    (1998 to 2002).foreach { y =>
      (0 to 11).foreach { m =>
        (0 to 5).foreach { i =>
          val c = Calendar.getInstance()
          c.set(y, m, 28, 0, 0, 0)
          c.add(Calendar.DATE, i)
          checkEvaluation(DayOfYear(Literal(new Date(c.getTimeInMillis))),
            sdfDay.format(c.getTime).toInt)
        }
      }
    }

    (1969 to 1970).foreach { y =>
      (0 to 11).foreach { m =>
        (0 to 5).foreach { i =>
          val c = Calendar.getInstance()
          c.set(y, m, 28, 0, 0, 0)
          c.add(Calendar.DATE, i)
          checkEvaluation(DayOfYear(Literal(new Date(c.getTimeInMillis))),
            sdfDay.format(c.getTime).toInt)
        }
      }
    }

    (2402 to 2404).foreach { y =>
      (0 to 11).foreach { m =>
        (0 to 5).foreach { i =>
          val c = Calendar.getInstance()
          c.set(y, m, 28, 0, 0, 0)
          c.add(Calendar.DATE, i)
          checkEvaluation(DayOfYear(Literal(new Date(c.getTimeInMillis))),
            sdfDay.format(c.getTime).toInt)
        }
      }
    }

    (2398 to 2402).foreach { y =>
      (0 to 11).foreach { m =>
        (0 to 5).foreach { i =>
          val c = Calendar.getInstance()
          c.set(y, m, 28, 0, 0, 0)
          c.add(Calendar.DATE, i)
          checkEvaluation(DayOfYear(Literal(new Date(c.getTimeInMillis))),
            sdfDay.format(c.getTime).toInt)
        }
      }
    }
    checkEvaluation(DayOfYear(Literal.create(null, DateType)), null)
  }

  test("Year") {
    checkEvaluation(Year(Literal.create(null, DateType)), null)
    checkEvaluation(Year(Literal(d)), 2015)
    checkEvaluation(Year(Cast(Literal(sdfDate.format(d)), DateType)), 2015)
    checkEvaluation(Year(Cast(Literal(ts), DateType)), 2013)

    val c = Calendar.getInstance()
    (2000 to 2010).foreach { y =>
      (0 to 11 by 11).foreach { m =>
        c.set(y, m, 28)
        (0 to 5 * 24).foreach { i =>
          c.add(Calendar.HOUR_OF_DAY, 1)
          checkEvaluation(Year(Literal(new Date(c.getTimeInMillis))),
            c.get(Calendar.YEAR))
        }
      }
    }
  }

  test("Quarter") {
    checkEvaluation(Quarter(Literal.create(null, DateType)), null)
    checkEvaluation(Quarter(Literal(d)), 2)
    checkEvaluation(Quarter(Cast(Literal(sdfDate.format(d)), DateType)), 2)
    checkEvaluation(Quarter(Cast(Literal(ts), DateType)), 4)

    val c = Calendar.getInstance()
    (2003 to 2004).foreach { y =>
      (0 to 11 by 3).foreach { m =>
        c.set(y, m, 28, 0, 0, 0)
        (0 to 5 * 24).foreach { i =>
          c.add(Calendar.HOUR_OF_DAY, 1)
          checkEvaluation(Quarter(Literal(new Date(c.getTimeInMillis))),
            c.get(Calendar.MONTH) / 3 + 1)
        }
      }
    }
  }

  test("Month") {
    checkEvaluation(Month(Literal.create(null, DateType)), null)
    checkEvaluation(Month(Literal(d)), 4)
    checkEvaluation(Month(Cast(Literal(sdfDate.format(d)), DateType)), 4)
    checkEvaluation(Month(Cast(Literal(ts), DateType)), 11)

    (2003 to 2004).foreach { y =>
      (0 to 11).foreach { m =>
        (0 to 5 * 24).foreach { i =>
          val c = Calendar.getInstance()
          c.set(y, m, 28, 0, 0, 0)
          c.add(Calendar.HOUR_OF_DAY, i)
          checkEvaluation(Month(Literal(new Date(c.getTimeInMillis))),
            c.get(Calendar.MONTH) + 1)
        }
      }
    }

    (1999 to 2000).foreach { y =>
      (0 to 11).foreach { m =>
        (0 to 5 * 24).foreach { i =>
          val c = Calendar.getInstance()
          c.set(y, m, 28, 0, 0, 0)
          c.add(Calendar.HOUR_OF_DAY, i)
          checkEvaluation(Month(Literal(new Date(c.getTimeInMillis))),
            c.get(Calendar.MONTH) + 1)
        }
      }
    }
  }

  test("Day / DayOfMonth") {
    checkEvaluation(DayOfMonth(Cast(Literal("2000-02-29"), DateType)), 29)
    checkEvaluation(DayOfMonth(Literal.create(null, DateType)), null)
    checkEvaluation(DayOfMonth(Literal(d)), 8)
    checkEvaluation(DayOfMonth(Cast(Literal(sdfDate.format(d)), DateType)), 8)
    checkEvaluation(DayOfMonth(Cast(Literal(ts), DateType)), 8)

    (1999 to 2000).foreach { y =>
      val c = Calendar.getInstance()
      c.set(y, 0, 1, 0, 0, 0)
      (0 to 365).foreach { d =>
        c.add(Calendar.DATE, 1)
        checkEvaluation(DayOfMonth(Literal(new Date(c.getTimeInMillis))),
          c.get(Calendar.DAY_OF_MONTH))
      }
    }
  }

  test("Seconds") {
    checkEvaluation(Second(Literal.create(null, DateType)), null)
    checkEvaluation(Second(Cast(Literal(d), TimestampType)), 0)
    checkEvaluation(Second(Cast(Literal(sdf.format(d)), TimestampType)), 15)
    checkEvaluation(Second(Literal(ts)), 15)

    val c = Calendar.getInstance()
    (0 to 60 by 5).foreach { s =>
      c.set(2015, 18, 3, 3, 5, s)
      checkEvaluation(Second(Literal(new Timestamp(c.getTimeInMillis))),
        c.get(Calendar.SECOND))
    }
  }

  test("WeekOfYear") {
    checkEvaluation(WeekOfYear(Literal.create(null, DateType)), null)
    checkEvaluation(WeekOfYear(Literal(d)), 15)
    checkEvaluation(WeekOfYear(Cast(Literal(sdfDate.format(d)), DateType)), 15)
    checkEvaluation(WeekOfYear(Cast(Literal(ts), DateType)), 45)
    checkEvaluation(WeekOfYear(Cast(Literal("2011-05-06"), DateType)), 18)
  }

  test("DateFormat") {
    checkEvaluation(DateFormatClass(Literal.create(null, TimestampType), Literal("y")), null)
    checkEvaluation(DateFormatClass(Cast(Literal(d), TimestampType),
      Literal.create(null, StringType)), null)
    checkEvaluation(DateFormatClass(Cast(Literal(d), TimestampType),
      Literal("y")), "2015")
    checkEvaluation(DateFormatClass(Literal(ts), Literal("y")), "2013")
  }

  test("Hour") {
    checkEvaluation(Hour(Literal.create(null, DateType)), null)
    checkEvaluation(Hour(Cast(Literal(d), TimestampType)), 0)
    checkEvaluation(Hour(Cast(Literal(sdf.format(d)), TimestampType)), 13)
    checkEvaluation(Hour(Literal(ts)), 13)

    val c = Calendar.getInstance()
    (0 to 24).foreach { h =>
      (0 to 60 by 15).foreach { m =>
        (0 to 60 by 15).foreach { s =>
          c.set(2015, 18, 3, h, m, s)
          checkEvaluation(Hour(Literal(new Timestamp(c.getTimeInMillis))),
            c.get(Calendar.HOUR_OF_DAY))
        }
      }
    }
  }

  test("Minute") {
    checkEvaluation(Minute(Literal.create(null, DateType)), null)
    checkEvaluation(Minute(Cast(Literal(d), TimestampType)), 0)
    checkEvaluation(Minute(Cast(Literal(sdf.format(d)), TimestampType)), 10)
    checkEvaluation(Minute(Literal(ts)), 10)

    val c = Calendar.getInstance()
    (0 to 60 by 5).foreach { m =>
      (0 to 60 by 15).foreach { s =>
        c.set(2015, 18, 3, 3, m, s)
        checkEvaluation(Minute(Literal(new Timestamp(c.getTimeInMillis))),
          c.get(Calendar.MINUTE))
      }
    }
  }

  test("last_day") {
    checkEvaluation(LastDay(Literal(Date.valueOf("2015-02-28"))), Date.valueOf("2015-02-28"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2015-03-27"))), Date.valueOf("2015-03-31"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2015-04-26"))), Date.valueOf("2015-04-30"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2015-05-25"))), Date.valueOf("2015-05-31"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2015-06-24"))), Date.valueOf("2015-06-30"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2015-07-23"))), Date.valueOf("2015-07-31"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2015-08-01"))), Date.valueOf("2015-08-31"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2015-09-02"))), Date.valueOf("2015-09-30"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2015-10-03"))), Date.valueOf("2015-10-31"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2015-11-04"))), Date.valueOf("2015-11-30"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2015-12-05"))), Date.valueOf("2015-12-31"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2016-01-06"))), Date.valueOf("2016-01-31"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2016-02-07"))), Date.valueOf("2016-02-29"))
    checkEvaluation(LastDay(Literal.create(null, DateType)), null)
  }

  test("next_day") {
    def testNextDay(input: String, dayOfWeek: String, output: String): Unit = {
      checkEvaluation(
        NextDay(Literal(Date.valueOf(input)), NonFoldableLiteral(dayOfWeek)),
        DateTimeUtils.fromJavaDate(Date.valueOf(output)))
      checkEvaluation(
        NextDay(Literal(Date.valueOf(input)), Literal(dayOfWeek)),
        DateTimeUtils.fromJavaDate(Date.valueOf(output)))
    }
    testNextDay("2015-07-23", "Mon", "2015-07-27")
    testNextDay("2015-07-23", "mo", "2015-07-27")
    testNextDay("2015-07-23", "Tue", "2015-07-28")
    testNextDay("2015-07-23", "tu", "2015-07-28")
    testNextDay("2015-07-23", "we", "2015-07-29")
    testNextDay("2015-07-23", "wed", "2015-07-29")
    testNextDay("2015-07-23", "Thu", "2015-07-30")
    testNextDay("2015-07-23", "TH", "2015-07-30")
    testNextDay("2015-07-23", "Fri", "2015-07-24")
    testNextDay("2015-07-23", "fr", "2015-07-24")

    checkEvaluation(NextDay(Literal(Date.valueOf("2015-07-23")), Literal("xx")), null)
    checkEvaluation(NextDay(Literal.create(null, DateType), Literal("xx")), null)
    checkEvaluation(
      NextDay(Literal(Date.valueOf("2015-07-23")), Literal.create(null, StringType)), null)
  }
}
