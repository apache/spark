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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

class NumberFormatterSuite extends SparkFunSuite {

  private def invalidNumberFormat(numberFormat: String, errorMsg: String): Unit = {
    val testNumberFormatter = new TestNumberFormatter(numberFormat)
    val e = intercept[AnalysisException](testNumberFormatter.checkWithException())
    assert(e.getMessage.contains(errorMsg))
  }

  private def failParseWithInvalidInput(
      input: UTF8String, numberFormat: String, errorMsg: String): Unit = {
    val testNumberFormatter = new TestNumberFormatter(numberFormat)
    val e = intercept[IllegalArgumentException](testNumberFormatter.parse(input))
    assert(e.getMessage.contains(errorMsg))
  }

  test("parse") {
    invalidNumberFormat("", "Number format cannot be empty")

    // Test '9' and '0'
    failParseWithInvalidInput(UTF8String.fromString("454"), "9",
      "The input string '454' does not match the given number format: '9'")
    failParseWithInvalidInput(UTF8String.fromString("454"), "99",
      "The input string '454' does not match the given number format: '99'")

    Seq(
      ("454", "999") -> Decimal(454),
      ("054", "999") -> Decimal(54),
      ("54", "999") -> Decimal(54),
      ("404", "999") -> Decimal(404),
      ("450", "999") -> Decimal(450),
      ("454", "9999") -> Decimal(454),
      ("054", "9999") -> Decimal(54),
      ("404", "9999") -> Decimal(404),
      ("450", "9999") -> Decimal(450)
    ).foreach { case ((str, format), expected) =>
      val builder = new TestNumberFormatter(format)
      builder.check()
      assert(builder.parse(UTF8String.fromString(str)) === expected)
    }

    failParseWithInvalidInput(UTF8String.fromString("454"), "0",
      "The input string '454' does not match the given number format: '0'")
    failParseWithInvalidInput(UTF8String.fromString("454"), "00",
      "The input string '454' does not match the given number format: '00'")

    Seq(
      ("454", "000") -> Decimal(454),
      ("054", "000") -> Decimal(54),
      ("54", "000") -> Decimal(54),
      ("404", "000") -> Decimal(404),
      ("450", "000") -> Decimal(450),
      ("454", "0000") -> Decimal(454),
      ("054", "0000") -> Decimal(54),
      ("404", "0000") -> Decimal(404),
      ("450", "0000") -> Decimal(450)
    ).foreach { case ((str, format), expected) =>
      val builder = new TestNumberFormatter(format)
      builder.check()
      assert(builder.parse(UTF8String.fromString(str)) === expected)
    }

    // Test '.' and 'D'
    failParseWithInvalidInput(UTF8String.fromString("454.2"), "999",
      "The input string '454.2' does not match the given number format: '999'")
    failParseWithInvalidInput(UTF8String.fromString("454.23"), "999.9",
      "The input string '454.23' does not match the given number format: '999.9'")

    Seq(
      ("454.2", "999.9") -> Decimal(454.2),
      ("454.2", "000.0") -> Decimal(454.2),
      ("454.2", "999D9") -> Decimal(454.2),
      ("454.2", "000D0") -> Decimal(454.2),
      ("454.23", "999.99") -> Decimal(454.23),
      ("454.23", "000.00") -> Decimal(454.23),
      ("454.23", "999D99") -> Decimal(454.23),
      ("454.23", "000D00") -> Decimal(454.23),
      ("454.0", "999.9") -> Decimal(454),
      ("454.0", "000.0") -> Decimal(454),
      ("454.0", "999D9") -> Decimal(454),
      ("454.0", "000D0") -> Decimal(454),
      ("454.00", "999.99") -> Decimal(454),
      ("454.00", "000.00") -> Decimal(454),
      ("454.00", "999D99") -> Decimal(454),
      ("454.00", "000D00") -> Decimal(454),
      (".4542", ".9999") -> Decimal(0.4542),
      (".4542", ".0000") -> Decimal(0.4542),
      (".4542", "D9999") -> Decimal(0.4542),
      (".4542", "D0000") -> Decimal(0.4542),
      ("4542.", "9999.") -> Decimal(4542),
      ("4542.", "0000.") -> Decimal(4542),
      ("4542.", "9999D") -> Decimal(4542),
      ("4542.", "0000D") -> Decimal(4542)
    ).foreach { case ((str, format), expected) =>
      val builder = new TestNumberFormatter(format)
      builder.check()
      assert(builder.parse(UTF8String.fromString(str)) === expected)
    }

    invalidNumberFormat(
      "999.9.9", "At most one 'D' or '.' is allowed in the number format: '999.9.9'")
    invalidNumberFormat(
      "999D9D9", "At most one 'D' or '.' is allowed in the number format: '999D9D9'")
    invalidNumberFormat(
      "999.9D9", "At most one 'D' or '.' is allowed in the number format: '999.9D9'")
    invalidNumberFormat(
      "999D9.9", "At most one 'D' or '.' is allowed in the number format: '999D9.9'")

    // Test ',' and 'G'
    Seq(
      ("12,454", "99,999") -> Decimal(12454),
      ("12,454", "00,000") -> Decimal(12454),
      ("12,454", "99G999") -> Decimal(12454),
      ("12,454", "00G000") -> Decimal(12454),
      ("12,454,367", "99,999,999") -> Decimal(12454367),
      ("12,454,367", "00,000,000") -> Decimal(12454367),
      ("12,454,367", "99G999G999") -> Decimal(12454367),
      ("12,454,367", "00G000G000") -> Decimal(12454367),
      ("12,454,", "99,999,") -> Decimal(12454),
      ("12,454,", "00,000,") -> Decimal(12454),
      ("12,454,", "99G999G") -> Decimal(12454),
      ("12,454,", "00G000G") -> Decimal(12454),
      (",454,367", ",999,999") -> Decimal(454367),
      (",454,367", ",000,000") -> Decimal(454367),
      (",454,367", "G999G999") -> Decimal(454367),
      (",454,367", "G000G000") -> Decimal(454367),
      (",454,367", "999,999") -> Decimal(454367),
      (",454,367", "000,000") -> Decimal(454367),
      (",454,367", "999G999") -> Decimal(454367),
      (",454,367", "000G000") -> Decimal(454367)
    ).foreach { case ((str, format), expected) =>
      val builder = new TestNumberFormatter(format)
      builder.check()
      assert(builder.parse(UTF8String.fromString(str)) === expected)
    }

    // Test '$'
    Seq(
      ("$78.12", "$99.99") -> Decimal(78.12),
      ("$78.12", "$00.00") -> Decimal(78.12),
      ("78.12$", "99.99$") -> Decimal(78.12),
      ("78.12$", "00.00$") -> Decimal(78.12)
    ).foreach { case ((str, format), expected) =>
      val builder = new TestNumberFormatter(format)
      builder.check()
      assert(builder.parse(UTF8String.fromString(str)) === expected)
    }

    invalidNumberFormat(
      "99$.99", "'$' must be the first or last char in the number format: '99$.99'")
    invalidNumberFormat("$99.99$", "At most one '$' is allowed in the number format: '$99.99$'")

    // Test '-' and 'S'
    Seq(
      ("454-", "999-") -> Decimal(-454),
      ("454-", "999S") -> Decimal(-454),
      ("-454", "-999") -> Decimal(-454),
      ("-454", "S999") -> Decimal(-454),
      ("454-", "000-") -> Decimal(-454),
      ("454-", "000S") -> Decimal(-454),
      ("-454", "-000") -> Decimal(-454),
      ("-454", "S000") -> Decimal(-454),
      ("12,454.8-", "99G999D9S") -> Decimal(-12454.8),
      ("00,454.8-", "99G999.9S") -> Decimal(-454.8)
    ).foreach { case ((str, format), expected) =>
      val builder = new TestNumberFormatter(format)
      builder.check()
      assert(builder.parse(UTF8String.fromString(str)) === expected)
    }

    invalidNumberFormat(
      "9S99", "'S' or '-' must be the first or last char in the number format: '9S99'")
    invalidNumberFormat(
      "9-99", "'S' or '-' must be the first or last char in the number format: '9-99'")
    invalidNumberFormat(
      "999D9SS", "At most one 'S' or '-' is allowed in the number format: '999D9SS'")
  }

  test("format") {

    // Test '9' and '0'
    Seq(
      (Decimal(454), "9") -> "#",
      (Decimal(454), "99") -> "##",
      (Decimal(454), "999") -> "454",
      (Decimal(54), "999") -> "54",
      (Decimal(404), "999") -> "404",
      (Decimal(450), "999") -> "450",
      (Decimal(454), "9999") -> "454",
      (Decimal(54), "9999") -> "54",
      (Decimal(404), "9999") -> "404",
      (Decimal(450), "9999") -> "450",
      (Decimal(454), "0") -> "#",
      (Decimal(454), "00") -> "##",
      (Decimal(454), "000") -> "454",
      (Decimal(54), "000") -> "054",
      (Decimal(404), "000") -> "404",
      (Decimal(450), "000") -> "450",
      (Decimal(454), "0000") -> "0454",
      (Decimal(54), "0000") -> "0054",
      (Decimal(404), "0000") -> "0404",
      (Decimal(450), "0000") -> "0450"
    ).foreach { case ((decimal, format), expected) =>
      val builder = new TestNumberFormatter(format, false)
      builder.check()
      assert(builder.format(decimal) === expected)
    }

    // Test '.' and 'D'
    Seq(
      (Decimal(454.2), "999.9") -> "454.2",
      (Decimal(454.2), "000.0") -> "454.2",
      (Decimal(454.2), "999D9") -> "454.2",
      (Decimal(454.2), "000D0") -> "454.2",
      (Decimal(454), "999.9") -> "454.0",
      (Decimal(454), "000.0") -> "454.0",
      (Decimal(454), "999D9") -> "454.0",
      (Decimal(454), "000D0") -> "454.0",
      (Decimal(454), "999.99") -> "454.00",
      (Decimal(454), "000.00") -> "454.00",
      (Decimal(454), "999D99") -> "454.00",
      (Decimal(454), "000D00") -> "454.00",
      (Decimal(0.4542), ".9999") -> ".####",
      (Decimal(0.4542), ".0000") -> ".####",
      (Decimal(0.4542), "D9999") -> ".####",
      (Decimal(0.4542), "D0000") -> ".####",
      (Decimal(4542), "9999.") -> "4542.",
      (Decimal(4542), "0000.") -> "4542.",
      (Decimal(4542), "9999D") -> "4542.",
      (Decimal(4542), "0000D") -> "4542."
    ).foreach { case ((decimal, format), expected) =>
      val builder = new TestNumberFormatter(format, false)
      builder.check()
      assert(builder.format(decimal) === expected)
    }

    // Test ',' and 'G'
    Seq(
      (Decimal(12454), "99,999") -> "12,454",
      (Decimal(12454), "00,000") -> "12,454",
      (Decimal(12454), "99G999") -> "12,454",
      (Decimal(12454), "00G000") -> "12,454",
      (Decimal(12454367), "99,999,999") -> "12,454,367",
      (Decimal(12454367), "00,000,000") -> "12,454,367",
      (Decimal(12454367), "99G999G999") -> "12,454,367",
      (Decimal(12454367), "00G000G000") -> "12,454,367",
      (Decimal(12454), "99,999,") -> "12,454,",
      (Decimal(12454), "00,000,") -> "12,454,",
      (Decimal(12454), "99G999G") -> "12,454,",
      (Decimal(12454), "00G000G") -> "12,454,",
      (Decimal(454367), ",999,999") -> ",454,367",
      (Decimal(454367), ",000,000") -> ",454,367",
      (Decimal(454367), "G999G999") -> ",454,367",
      (Decimal(454367), "G000G000") -> ",454,367"
    ).foreach { case ((decimal, format), expected) =>
      val builder = new TestNumberFormatter(format, false)
      builder.check()
      assert(builder.format(decimal) === expected)
    }

    // Test '$'
    Seq(
      (Decimal(78.12), "$99.99") -> "$78.12",
      (Decimal(78.12), "$00.00") -> "$78.12",
      (Decimal(78.12), "99.99$") -> "78.12$",
      (Decimal(78.12), "00.00$") -> "78.12$"
    ).foreach { case ((decimal, format), expected) =>
      val builder = new TestNumberFormatter(format, false)
      builder.check()
      assert(builder.format(decimal) === expected)
    }

    // Test '-' and 'S'
    Seq(
      (Decimal(-454), "999-") -> "454-",
      (Decimal(-454), "999S") -> "454-",
      (Decimal(-454), "-999") -> "-454",
      (Decimal(-454), "S999") -> "-454",
      (Decimal(-454), "000-") -> "454-",
      (Decimal(-454), "000S") -> "454-",
      (Decimal(-454), "-000") -> "-454",
      (Decimal(-454), "S000") -> "-454",
      (Decimal(-12454.8), "99G999D9S") -> "12,454.8-",
      (Decimal(-454.8), "99G999.9S") -> "454.8-"
    ).foreach { case ((decimal, format), expected) =>
      val builder = new TestNumberFormatter(format, false)
      builder.check()
      assert(builder.format(decimal) === expected)
    }
  }

}
