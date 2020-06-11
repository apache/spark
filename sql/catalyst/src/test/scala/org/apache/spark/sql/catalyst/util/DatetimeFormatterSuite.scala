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

import org.scalatest.Matchers

import org.apache.spark.{SparkFunSuite, SparkUpgradeException}
import org.apache.spark.sql.catalyst.plans.SQLHelper

trait DatetimeFormatterSuite extends SparkFunSuite with SQLHelper with Matchers {
  import DateTimeFormatterHelper._
  def checkFormatterCreation(pattern: String, isParsing: Boolean): Unit

  test("explicitly forbidden datetime patterns") {

    Seq(true, false).foreach { isParsing =>
      // not support by the legacy one too
      val unsupportedBoth = Seq("QQQQQ", "qqqqq", "eeeee", "A", "c", "n", "N", "p", "e")
      unsupportedBoth.foreach { pattern =>
        intercept[IllegalArgumentException](checkFormatterCreation(pattern, isParsing))
      }
      // supported by the legacy one, then we will suggest users with SparkUpgradeException
      ((weekBasedLetters ++ unsupportedLetters).map(_.toString)
        ++ unsupportedPatternLengths -- unsupportedBoth).foreach {
        pattern => intercept[SparkUpgradeException](checkFormatterCreation(pattern, isParsing))
      }
    }

    // not support by the legacy one too
    val unsupportedBoth = Seq("q", "Q")
    unsupportedBoth.foreach { pattern =>
      intercept[IllegalArgumentException](checkFormatterCreation(pattern, true))
    }
    // supported by the legacy one, then we will suggest users with SparkUpgradeException
    (unsupportedLettersForParsing.map(_.toString) -- unsupportedBoth).foreach {
      pattern => intercept[SparkUpgradeException](checkFormatterCreation(pattern, true))
    }
  }
}
