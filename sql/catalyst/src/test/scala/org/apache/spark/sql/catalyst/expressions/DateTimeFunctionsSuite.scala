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
import java.util.{TimeZone, Calendar}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.{TimestampType, StringType, DateType}

class DateTimeFunctionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val d = new Date(sdf.parse("2015-04-08 13:10:15").getTime)
  val ts = new Timestamp(sdf.parse("2013-11-08 13:10:15").getTime)

  test("DateFormat") {
    checkEvaluation(DateFormatClass(Literal.create(null, TimestampType), Literal("y")), null)
    checkEvaluation(DateFormatClass(Cast(Literal(d), TimestampType),
      Literal.create(null, StringType)), null)
    checkEvaluation(DateFormatClass(Cast(Literal(d), TimestampType),
      Literal("y")), "2015")
    checkEvaluation(Year(Cast(Literal(d), TimestampType)), 2015)
    checkEvaluation(DateFormatClass(Literal(ts), Literal("y")), "2013")
  }

  test("Year") {
    checkEvaluation(Year(Literal.create(null, DateType)), null)
    checkEvaluation(Year(Cast(Literal(d), TimestampType)), 2015)
    checkEvaluation(Year(Cast(Literal(sdf.format(d)), TimestampType)), 2015)
    checkEvaluation(Year(Literal(ts)), 2013)

    val c = Calendar.getInstance()
    (2000 to 2010).foreach { y =>
      (1 to 12 by 11).foreach { m =>
        c.set(y, m, 28)
        (0 to 5 * 24).foreach { i =>
          c.add(Calendar.HOUR_OF_DAY, 1)
          checkEvaluation(Year(Cast(Literal(new Date(c.getTimeInMillis)), TimestampType)),
            c.get(Calendar.YEAR))
        }
      }
    }
  }

  test("Quarter") {
    checkEvaluation(Quarter(Literal.create(null, DateType)), null)
    checkEvaluation(Quarter(Cast(Literal(d), TimestampType)), 2)
    checkEvaluation(Quarter(Cast(Literal(sdf.format(d)), TimestampType)), 2)
    checkEvaluation(Quarter(Literal(ts)), 4)

    val c = Calendar.getInstance()
    (2003 to 2004).foreach { y =>
      (1 to 12 by 3).foreach { m =>
        c.set(y, m, 28)
        (0 to 5 * 24).foreach { i =>
          c.add(Calendar.HOUR_OF_DAY, 1)
          checkEvaluation(Quarter(Cast(Literal(new Date(c.getTimeInMillis)), TimestampType)),
            c.get(Calendar.MONTH) / 3 + 1)
        }
      }
    }
  }

  test("Month") {
    checkEvaluation(Month(Literal.create(null, DateType)), null)
    checkEvaluation(Month(Cast(Literal(d), TimestampType)), 4)
    checkEvaluation(Month(Cast(Literal(sdf.format(d)), TimestampType)), 4)
    checkEvaluation(Month(Literal(ts)), 11)

    val c = Calendar.getInstance()
    (2003 to 2004).foreach { y =>
      (1 to 12).foreach { m =>
        c.set(y, m, 28)
        (0 to 5 * 24).foreach { i =>
          c.add(Calendar.HOUR_OF_DAY, 1)
          checkEvaluation(Month(Cast(Literal(new Date(c.getTimeInMillis)), TimestampType)),
            c.get(Calendar.MONTH) + 1)
        }
      }
    }
  }

  test("Day") {
    checkEvaluation(Day(Literal.create(null, DateType)), null)
    checkEvaluation(Day(Cast(Literal(d), TimestampType)), 8)
    checkEvaluation(Day(Cast(Literal(sdf.format(d)), TimestampType)), 8)
    checkEvaluation(Day(Literal(ts)), 8)
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
    checkEvaluation(WeekOfYear(Cast(Literal(d), TimestampType)), 15)
    checkEvaluation(WeekOfYear(Cast(Literal(sdf.format(d)), TimestampType)), 15)
    checkEvaluation(WeekOfYear(Literal(ts)), 45)
  }

}
