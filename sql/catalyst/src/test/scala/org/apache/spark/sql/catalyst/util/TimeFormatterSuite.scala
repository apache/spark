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

package org.apache.spark.sql.catalyst.util

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.DateTimeUtils.microsToLocalTime

class TimeFormatterSuite extends SparkFunSuite with SQLHelper {

  test("time parsing") {
    Seq(
      ("12", "HH") -> 12 * 3600 * 1000000L,
      ("01:02", "HH:mm") -> (1 * 3600 + 2 * 60) * 1000000L,
      ("10:20", "HH:mm") -> (10 * 3600 + 20 * 60) * 1000000L,
      ("00:00:00", "HH:mm:ss") -> 0L,
      ("01:02:03", "HH:mm:ss") -> (1 * 3600 + 2 * 60 + 3) * 1000000L,
      ("23:59:59", "HH:mm:ss") -> (23 * 3600 + 59 * 60 + 59) * 1000000L,
      ("00:00:00.000000", "HH:mm:ss.SSSSSS") -> 0L,
      ("12:34:56.789012", "HH:mm:ss.SSSSSS") -> ((12 * 3600 + 34 * 60 + 56) * 1000000L + 789012),
      ("23:59:59.000000", "HH:mm:ss.SSSSSS") -> (23 * 3600 + 59 * 60 + 59) * 1000000L,
      ("23:59:59.999999", "HH:mm:ss.SSSSSS") -> ((23 * 3600 + 59 * 60 + 59) * 1000000L + 999999)
    ).foreach { case ((inputStr, pattern), expectedMicros) =>
      val formatter = TimeFormatter(format = Some(pattern), isParsing = true)
      assert(formatter.parse(inputStr) === expectedMicros)
    }
  }

  test("time formatting") {
    Seq(
      (12 * 3600 * 1000000L, "HH") -> "12",
      ((1 * 3600 + 2 * 60) * 1000000L, "HH:mm") -> "01:02",
      ((10 * 3600 + 20 * 60) * 1000000L, "HH:mm") -> "10:20",
      (0L, "HH:mm:ss") -> "00:00:00",
      ((1 * 3600 + 2 * 60 + 3) * 1000000L, "HH:mm:ss") -> "01:02:03",
      ((23 * 3600 + 59 * 60 + 59) * 1000000L, "HH:mm:ss") -> "23:59:59",
      (0L, "HH:mm:ss.SSSSSS") -> "00:00:00.000000",
      ((12 * 3600 + 34 * 60 + 56) * 1000000L + 789012, "HH:mm:ss.SSSSSS") -> "12:34:56.789012",
      ((23 * 3600 + 59 * 60 + 59) * 1000000L, "HH:mm:ss.SSSSSS") -> "23:59:59.000000",
      ((23 * 3600 + 59 * 60 + 59) * 1000000L + 999999, "HH:mm:ss.SSSSSS") -> "23:59:59.999999"
    ).foreach { case ((micros, pattern), expectedStr) =>
      val formatter = TimeFormatter(format = Some(pattern), isParsing = false)
      assert(formatter.format(micros) === expectedStr)
    }
  }

  test("round trip with the default pattern: format -> parse") {
    val data = Seq.tabulate(10) { _ => Random.between(0, 24 * 60 * 60 * 1000000L) }
    val pattern = Some("HH:mm:ss.SSSSSS")
    val (formatter, parser) =
      (TimeFormatter(pattern, isParsing = false), TimeFormatter(pattern, isParsing = true))
    data.foreach { micros =>
      assert(parser.parse(formatter.format(micros)) === micros, s"micros = $micros")
    }
  }

  test("format fraction of second") {
    val formatter = new FractionTimeFormatter()
    Seq(
      0 -> "00:00:00",
      1 -> "00:00:00.000001",
      1000 -> "00:00:00.001",
      900000 -> "00:00:00.9",
      1000000 -> "00:00:01").foreach { case (micros, tsStr) =>
      assert(formatter.format(micros) === tsStr)
      assert(formatter.format(microsToLocalTime(micros)) === tsStr)
    }
  }
}
