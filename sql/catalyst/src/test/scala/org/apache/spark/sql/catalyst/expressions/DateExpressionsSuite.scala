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
import org.apache.spark.sql.types.{StringType, TimestampType, DateType}

class DateExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val sdfDate = new SimpleDateFormat("yyyy-MM-dd")
  val d = new Date(sdf.parse("2015-04-08 13:10:15").getTime)
  val ts = new Timestamp(sdf.parse("2013-11-08 13:10:15").getTime)

  test("DayOfYear") {
    val sdfDay = new SimpleDateFormat("D")
    (2002 to 2004).foreach { y =>
      (0 to 11).foreach { m =>
        (0 to 5).foreach { i =>
          val c = Calendar.getInstance()
          c.set(y, m, 28, 0, 0, 0)
          c.add(Calendar.DATE, i)
          checkEvaluation(DayOfYear(Cast(Literal(new Date(c.getTimeInMillis)), DateType)),
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
          checkEvaluation(DayOfYear(Cast(Literal(new Date(c.getTimeInMillis)), DateType)),
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
          checkEvaluation(DayOfYear(Cast(Literal(new Date(c.getTimeInMillis)), DateType)),
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
          checkEvaluation(DayOfYear(Cast(Literal(new Date(c.getTimeInMillis)), DateType)),
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
          checkEvaluation(DayOfYear(Cast(Literal(new Date(c.getTimeInMillis)), DateType)),
            sdfDay.format(c.getTime).toInt)
        }
      }
    }
  }

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
    checkEvaluation(Month(Literal.create(null, DateType)), null)
    checkEvaluation(Month(Cast(Literal(d), DateType)), 4)
    checkEvaluation(Month(Cast(Literal(sdfDate.format(d)), DateType)), 4)
    checkEvaluation(Month(Cast(Literal(ts), DateType)), 11)

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

    (1999 to 2000).foreach { y =>
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
  }

  test("Day / DayOfMonth") {
    checkEvaluation(DayOfMonth(Cast(Literal("2000-02-29"), DateType)), 29)
    checkEvaluation(DayOfMonth(Literal.create(null, DateType)), null)
    checkEvaluation(DayOfMonth(Cast(Literal(d), DateType)), 8)
    checkEvaluation(DayOfMonth(Cast(Literal(sdfDate.format(d)), DateType)), 8)
    checkEvaluation(DayOfMonth(Cast(Literal(ts), DateType)), 8)

    (1999 to 2000).foreach { y =>
      val c = Calendar.getInstance()
      c.set(y, 0, 1, 0, 0, 0)
      (0 to 365).foreach { d =>
        c.add(Calendar.DATE, 1)
        checkEvaluation(DayOfMonth(Cast(Literal(new Date(c.getTimeInMillis)), DateType)),
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
      checkEvaluation(Second(Cast(Literal(new Timestamp(c.getTimeInMillis)), TimestampType)),
        c.get(Calendar.SECOND))
    }
  }

  test("WeekOfYear") {
    checkEvaluation(WeekOfYear(Literal.create(null, DateType)), null)
    checkEvaluation(WeekOfYear(Cast(Literal(d), DateType)), 15)
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
          checkEvaluation(Hour(Cast(Literal(new Timestamp(c.getTimeInMillis)), TimestampType)),
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
        checkEvaluation(Minute(Cast(Literal(new Timestamp(c.getTimeInMillis)), TimestampType)),
          c.get(Calendar.MINUTE))
      }
    }
  }

}
