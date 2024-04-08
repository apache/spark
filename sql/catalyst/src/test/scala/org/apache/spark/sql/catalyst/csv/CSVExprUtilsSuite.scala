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

package org.apache.spark.sql.catalyst.csv

import org.scalatest.prop.TableDrivenPropertyChecks._

import org.apache.spark.{SparkFunSuite, SparkIllegalArgumentException}

class CSVExprUtilsSuite extends SparkFunSuite {
  test("Can parse escaped characters") {
    assert(CSVExprUtils.toChar("""\t""") === '\t')
    assert(CSVExprUtils.toChar("""\r""") === '\r')
    assert(CSVExprUtils.toChar("""\b""") === '\b')
    assert(CSVExprUtils.toChar("""\f""") === '\f')
    assert(CSVExprUtils.toChar("""\"""") === '\"')
    assert(CSVExprUtils.toChar("""\'""") === '\'')
    assert(CSVExprUtils.toChar("\u0000") === '\u0000')
    assert(CSVExprUtils.toChar("""\\""") === '\\')
  }

  test("Does not accept delimiter larger than one character") {
    checkError(
      exception = intercept[SparkIllegalArgumentException]{
        CSVExprUtils.toChar("ab")
      },
      errorClass = "INVALID_DELIMITER_VALUE.DELIMITER_LONGER_THAN_EXPECTED",
      parameters = Map("str" -> "ab"))
  }

  test("Throws exception for unsupported escaped characters") {
    checkError(
      exception = intercept[SparkIllegalArgumentException]{
        CSVExprUtils.toChar("""\1""")
      },
      errorClass = "INVALID_DELIMITER_VALUE.UNSUPPORTED_SPECIAL_CHARACTER",
      parameters = Map("str" -> """\1"""))
  }

  test("string with one backward slash is prohibited") {
    checkError(
      exception = intercept[SparkIllegalArgumentException]{
        CSVExprUtils.toChar("""\""")
      },
      errorClass = "INVALID_DELIMITER_VALUE.SINGLE_BACKSLASH",
      parameters = Map.empty)
  }

  test("output proper error message for empty string") {
    checkError(
      exception = intercept[SparkIllegalArgumentException]{
        CSVExprUtils.toChar("")
      },
      errorClass = "INVALID_DELIMITER_VALUE.EMPTY_STRING",
      parameters = Map.empty)
  }

  val testCases = Table(
    ("input", "separatorStr", "expectedErrorMsg"),
    // normal tab
    ("""\t""", Some("\t"), None),
    // backslash, then tab
    ("""\\t""", Some("""\t"""), None),
    // invalid special character (dot)
    ("""\.""", None, Some("INVALID_DELIMITER_VALUE.UNSUPPORTED_SPECIAL_CHARACTER")),
    // backslash, then dot
    ("""\\.""", Some("""\."""), None),
    // nothing special, just straight conversion
    ("""foo""", Some("foo"), None),
    // tab in the middle of some other letters
    ("""ba\tr""", Some("ba\tr"), None),
    // null character, expressed in Unicode literal syntax
    ("\u0000", Some("\u0000"), None),
    // and specified directly
    ("\u0000", Some("\u0000"), None)
  )

  test("should correctly produce separator strings, or exceptions, from input") {
    forAll(testCases) { (input, separatorStr, expectedErrorClass) =>
      try {
        val separator = CSVExprUtils.toDelimiterStr(input)
        assert(separatorStr.isDefined)
        assert(expectedErrorClass.isEmpty)
        assert(separator.equals(separatorStr.get))
      } catch {
        case e: SparkIllegalArgumentException =>
          assert(separatorStr.isEmpty)
          assert(e.getErrorClass === expectedErrorClass.get)
      }
    }
  }
}
