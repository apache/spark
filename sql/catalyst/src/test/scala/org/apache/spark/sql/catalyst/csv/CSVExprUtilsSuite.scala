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

import org.apache.spark.SparkFunSuite

class CSVExprUtilsSuite extends SparkFunSuite {
  test("Can parse escaped characters") {
    assert(CSVExprUtils.toChar("""\t""") === '\t')
    assert(CSVExprUtils.toChar("""\r""") === '\r')
    assert(CSVExprUtils.toChar("""\b""") === '\b')
    assert(CSVExprUtils.toChar("""\f""") === '\f')
    assert(CSVExprUtils.toChar("""\"""") === '\"')
    assert(CSVExprUtils.toChar("""\'""") === '\'')
    assert(CSVExprUtils.toChar("""\u0000""") === '\u0000')
    assert(CSVExprUtils.toChar("""\\""") === '\\')
  }

  test("Does not accept delimiter larger than one character") {
    val exception = intercept[IllegalArgumentException]{
      CSVExprUtils.toChar("ab")
    }
    assert(exception.getMessage.contains("cannot be more than one character"))
  }

  test("Throws exception for unsupported escaped characters") {
    val exception = intercept[IllegalArgumentException]{
      CSVExprUtils.toChar("""\1""")
    }
    assert(exception.getMessage.contains("Unsupported special character for delimiter"))
  }

  test("string with one backward slash is prohibited") {
    val exception = intercept[IllegalArgumentException]{
      CSVExprUtils.toChar("""\""")
    }
    assert(exception.getMessage.contains("Single backslash is prohibited"))
  }

  test("output proper error message for empty string") {
    val exception = intercept[IllegalArgumentException]{
      CSVExprUtils.toChar("")
    }
    assert(exception.getMessage.contains("Delimiter cannot be empty string"))
  }

  val testCases = Table(
    ("input", "separatorStr", "expectedErrorMsg"),
    // normal tab
    ("""\t""", Some("\t"), None),
    // backslash, then tab
    ("""\\t""", Some("""\t"""), None),
    // invalid special character (dot)
    ("""\.""", None, Some("Unsupported special character for delimiter")),
    // backslash, then dot
    ("""\\.""", Some("""\."""), None),
    // nothing special, just straight conversion
    ("""foo""", Some("foo"), None),
    // tab in the middle of some other letters
    ("""ba\tr""", Some("ba\tr"), None),
    // null character, expressed in Unicode literal syntax
    ("""\u0000""", Some("\u0000"), None),
    // and specified directly
    ("\u0000", Some("\u0000"), None)
  )

  test("should correctly produce separator strings, or exceptions, from input") {
    forAll(testCases) { (input, separatorStr, expectedErrorMsg) =>
      try {
        val separator = CSVExprUtils.toDelimiterStr(input)
        assert(separatorStr.isDefined)
        assert(expectedErrorMsg.isEmpty)
        assert(separator.equals(separatorStr.get))
      } catch {
        case e: IllegalArgumentException =>
          assert(separatorStr.isEmpty)
          assert(expectedErrorMsg.isDefined)
          assert(e.getMessage.contains(expectedErrorMsg.get))
      }
    }
  }
}
