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
package org.apache.spark.util

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

class SparkStringUtilsSuite extends AnyFunSuite { // scalastyle:ignore funsuite

  test("isBlank treats null and whitespace-only strings as blank") {
    assert(SparkStringUtils.isBlank(null))
    assert(SparkStringUtils.isBlank(""))
    assert(SparkStringUtils.isBlank("   "))
    assert(!SparkStringUtils.isBlank("a"))
    assert(SparkStringUtils.isNotBlank("a"))
    assert(!SparkStringUtils.isNotBlank("   "))
  }

  test("leftPad and rightPad pad with spaces up to the requested width") {
    assert(SparkStringUtils.leftPad("hi", 5) === "   hi")
    assert(SparkStringUtils.rightPad("hi", 5) === "hi   ")
    // A width that is not larger than the input is a no-op, and null is passed through.
    assert(SparkStringUtils.leftPad("hello", 5) === "hello")
    assert(SparkStringUtils.rightPad("hello", 3) === "hello")
    assert(SparkStringUtils.leftPad(null, 5) === null)
    assert(SparkStringUtils.rightPad(null, 5) === null)
  }

  test("rightPad repeats the pad string and truncates the last repetition") {
    assert(SparkStringUtils.rightPad("x", 6, "*") === "x*****")
    assert(SparkStringUtils.rightPad("a", 5, "xy") === "axyxy")
    // Only the first character of the final "xy" fits within the width.
    assert(SparkStringUtils.rightPad("a", 4, "xy") === "axyx")
    assert(SparkStringUtils.rightPad("hello", 3, "*") === "hello")
    assert(SparkStringUtils.rightPad(null, 5, "*") === null)
  }

  test("rightPad with an empty pad string fails when padding is required") {
    // The pad length is used as a divisor, so an empty pad string divides by zero.
    // Documented here rather than guarded, since callers are expected to pass a
    // non-empty pad string.
    intercept[ArithmeticException] {
      SparkStringUtils.rightPad("a", 5, "")
    }
    // No padding is needed in these cases, so the divisor is never reached.
    assert(SparkStringUtils.rightPad("hello", 5, "") === "hello")
    assert(SparkStringUtils.rightPad("hello", 3, "") === "hello")
    assert(SparkStringUtils.rightPad(null, 5, "") === null)
  }

  test("abbreviate truncates with the marker and leaves short inputs untouched") {
    assert(SparkStringUtils.abbreviate("hello world", 8) === "hello...")
    assert(SparkStringUtils.abbreviate("abc", 8) === "abc")
    assert(SparkStringUtils.abbreviate("abcdefgh", "..", 4) === "ab..")
    // An empty marker makes abbreviate a plain truncation.
    assert(SparkStringUtils.abbreviate("abcdefgh", "", 3) === "abc")
    assert(SparkStringUtils.abbreviate(null, 5) === null)
    assert(SparkStringUtils.abbreviate("abc", null, 2) === null)
  }

  test("strip removes the given prefix and suffix when present") {
    assert(SparkStringUtils.strip("\"path\"", "\"") === "path")
    assert(SparkStringUtils.strip("path", "\"") === "path")
    assert(SparkStringUtils.strip(null, "\"") === null)
    assert(SparkStringUtils.strip("path", null) === "path")
  }

  test("stringToSeq trims entries and drops empty ones") {
    assert(SparkStringUtils.stringToSeq(" a, b ,,c ") === Seq("a", "b", "c"))
    assert(SparkStringUtils.stringToSeq("") === Seq.empty)
  }
}
