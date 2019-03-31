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

package org.apache.spark.sql.util

import java.time.LocalDate

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.internal.SQLConf

class DateFormatterSuite extends SparkFunSuite with SQLHelper {
  test("parsing dates") {
    DateTimeTestUtils.outstandingTimezonesIds.foreach { timeZone =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timeZone) {
        val formatter = DateFormatter()
        val daysSinceEpoch = formatter.parse("2018-12-02")
        assert(daysSinceEpoch === 17867)
      }
    }
  }

  test("format dates") {
    DateTimeTestUtils.outstandingTimezonesIds.foreach { timeZone =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timeZone) {
        val formatter = DateFormatter()
        val date = formatter.format(17867)
        assert(date === "2018-12-02")
      }
    }
  }

  test("roundtrip date -> days -> date") {
    Seq(
      "0050-01-01",
      "0953-02-02",
      "1423-03-08",
      "1969-12-31",
      "1972-08-25",
      "1975-09-26",
      "2018-12-12",
      "2038-01-01",
      "5010-11-17").foreach { date =>
      DateTimeTestUtils.outstandingTimezonesIds.foreach { timeZone =>
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timeZone) {
          val formatter = DateFormatter()
          val days = formatter.parse(date)
          val formatted = formatter.format(days)
          assert(date === formatted)
        }
      }
    }
  }

  test("roundtrip days -> date -> days") {
    Seq(
      -701265,
      -371419,
      -199722,
      -1,
      0,
      967,
      2094,
      17877,
      24837,
      1110657).foreach { days =>
      DateTimeTestUtils.outstandingTimezonesIds.foreach { timeZone =>
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timeZone) {
          val formatter = DateFormatter()
          val date = formatter.format(days)
          val parsed = formatter.parse(date)
          assert(days === parsed)
        }
      }
    }
  }

  test("parsing date without explicit day") {
    val formatter = DateFormatter("yyyy MMM")
    val daysSinceEpoch = formatter.parse("2018 Dec")
    assert(daysSinceEpoch === LocalDate.of(2018, 12, 1).toEpochDay)
  }
}
