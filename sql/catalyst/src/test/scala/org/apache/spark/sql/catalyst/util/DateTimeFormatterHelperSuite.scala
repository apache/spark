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

import org.apache.spark.{SparkFunSuite, SparkIllegalArgumentException}
import org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper._

class DateTimeFormatterHelperSuite extends SparkFunSuite {

  private def convertIncompatiblePattern(pattern: String): String = {
    DateTimeFormatterHelper.convertIncompatiblePattern(pattern, isParsing = false)
  }

  test("check incompatible pattern") {
    assert(convertIncompatiblePattern("yyyy-MM-dd'T'HH:mm:ss.SSSz")
      === "uuuu-MM-dd'T'HH:mm:ss.SSSz")
    assert(convertIncompatiblePattern("yyyy-MM'y contains in quoted text'HH:mm:ss")
      === "uuuu-MM'y contains in quoted text'HH:mm:ss")
    assert(convertIncompatiblePattern("yyyy-MM'u contains in quoted text'HH:mm:ss")
      === "uuuu-MM'u contains in quoted text'HH:mm:ss")
    assert(convertIncompatiblePattern("yyyy-MM'u contains in quoted text'''''HH:mm:ss")
      === "uuuu-MM'u contains in quoted text'''''HH:mm:ss")
    assert(convertIncompatiblePattern("yyyy-MM-dd'T'HH:mm:ss.SSSz G")
      === "yyyy-MM-dd'T'HH:mm:ss.SSSz G")
    weekBasedLetters.foreach { l =>
      checkError(
        exception = intercept[SparkIllegalArgumentException] {
          convertIncompatiblePattern(s"yyyy-MM-dd $l G")
        },
        errorClass = "INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_WEEK_BASED_PATTERN",
        parameters = Map("c" -> l.toString))
    }
    unsupportedLetters.foreach { l =>
      checkError(
        exception = intercept[SparkIllegalArgumentException] {
          convertIncompatiblePattern(s"yyyy-MM-dd $l G")
        },
        errorClass = "INVALID_DATETIME_PATTERN.ILLEGAL_CHARACTER",
        parameters = Map(
          "c" -> l.toString,
          "pattern" -> s"yyyy-MM-dd $l G"))
    }
    unsupportedLettersForParsing.foreach { l =>
      checkError(
        exception = intercept[SparkIllegalArgumentException] {
          DateTimeFormatterHelper.convertIncompatiblePattern(s"$l", isParsing = true)
        },
        errorClass = "INVALID_DATETIME_PATTERN.ILLEGAL_CHARACTER",
        parameters = Map(
          "c" -> l.toString,
          "pattern" -> s"$l"))
    }
    unsupportedPatternLengths.foreach { style =>
      checkError(
        exception = intercept[SparkIllegalArgumentException] {
          convertIncompatiblePattern(s"yyyy-MM-dd $style")
        },
        errorClass = "INVALID_DATETIME_PATTERN.LENGTH",
        parameters = Map("pattern" -> style))
      checkError(
        exception = intercept[SparkIllegalArgumentException] {
          convertIncompatiblePattern(s"yyyy-MM-dd $style${style.head}")
        },
        errorClass = "INVALID_DATETIME_PATTERN.LENGTH",
        parameters = Map("pattern" -> style))
    }
    assert(convertIncompatiblePattern("yyyy-MM-dd EEEE") === "uuuu-MM-dd EEEE")
    assert(convertIncompatiblePattern("yyyy-MM-dd'e'HH:mm:ss") === "uuuu-MM-dd'e'HH:mm:ss")
    assert(convertIncompatiblePattern("yyyy-MM-dd'T'") === "uuuu-MM-dd'T'")
  }
}
