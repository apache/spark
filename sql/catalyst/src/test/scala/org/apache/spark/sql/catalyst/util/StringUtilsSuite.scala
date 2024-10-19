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
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.StringUtils._
import org.apache.spark.sql.internal.SQLConf

class StringUtilsSuite extends SparkFunSuite with SQLHelper {

  test("escapeLikeRegex") {
    val expectedEscapedStrOne = "(?s)\\Qa\\E\\Qb\\E\\Qd\\E\\Qe\\E\\Qf\\E"
    val expectedEscapedStrTwo = "(?s)\\Qa\\E\\Q_\\E.\\Qb\\E"
    val expectedEscapedStrThree = "(?s)\\Qa\\E..*\\Qb\\E"
    val expectedEscapedStrFour = "(?s)\\Qa\\E.*\\Q%\\E\\Qb\\E"
    val expectedEscapedStrFive = "(?s)\\Qa\\E.*"
    val expectedEscapedStrSix = "(?s)\\Q*\\E\\Q*\\E"
    val expectedEscapedStrSeven = "(?s)\\Qa\\E.\\Qb\\E"
    assert(escapeLikeRegex("abdef", '\\') === expectedEscapedStrOne)
    assert(escapeLikeRegex("abdef", '/') === expectedEscapedStrOne)
    assert(escapeLikeRegex("abdef", '\"') === expectedEscapedStrOne)
    assert(escapeLikeRegex("a\\__b", '\\') === expectedEscapedStrTwo)
    assert(escapeLikeRegex("a/__b", '/') === expectedEscapedStrTwo)
    assert(escapeLikeRegex("a\"__b", '\"') === expectedEscapedStrTwo)
    assert(escapeLikeRegex("a_%b", '\\') === expectedEscapedStrThree)
    assert(escapeLikeRegex("a_%b", '/') === expectedEscapedStrThree)
    assert(escapeLikeRegex("a_%b", '\"') === expectedEscapedStrThree)
    assert(escapeLikeRegex("a%\\%b", '\\') === expectedEscapedStrFour)
    assert(escapeLikeRegex("a%/%b", '/') === expectedEscapedStrFour)
    assert(escapeLikeRegex("a%\"%b", '\"') === expectedEscapedStrFour)
    assert(escapeLikeRegex("a%", '\\') === expectedEscapedStrFive)
    assert(escapeLikeRegex("a%", '/') === expectedEscapedStrFive)
    assert(escapeLikeRegex("a%", '\"') === expectedEscapedStrFive)
    assert(escapeLikeRegex("**", '\\') === expectedEscapedStrSix)
    assert(escapeLikeRegex("**", '/') === expectedEscapedStrSix)
    assert(escapeLikeRegex("**", '\"') === expectedEscapedStrSix)
    assert(escapeLikeRegex("a_b", '\\') === expectedEscapedStrSeven)
    assert(escapeLikeRegex("a_b", '/') === expectedEscapedStrSeven)
    assert(escapeLikeRegex("a_b", '\"') === expectedEscapedStrSeven)
  }

  test("filter pattern") {
    val names = Seq("a1", "a2", "b2", "c3")
    assert(filterPattern(names, " * ") === Seq("a1", "a2", "b2", "c3"))
    assert(filterPattern(names, "*a*") === Seq("a1", "a2"))
    assert(filterPattern(names, " *a* ") === Seq("a1", "a2"))
    assert(filterPattern(names, " a* ") === Seq("a1", "a2"))
    assert(filterPattern(names, " a.* ") === Seq("a1", "a2"))
    assert(filterPattern(names, " B.*|a* ") === Seq("a1", "a2", "b2"))
    assert(filterPattern(names, " a. ") === Seq("a1", "a2"))
    assert(filterPattern(names, " d* ") === Nil)
  }

  test("string concatenation") {
    def concat(seq: String*): String = {
      seq.foldLeft(new StringConcat()) { (acc, s) => acc.append(s); acc }.toString
    }

    assert(new StringConcat().toString == "")
    assert(concat("") === "")
    assert(concat(null) === "")
    assert(concat("a") === "a")
    assert(concat("1", "2") === "12")
    assert(concat("abc", "\n", "123") === "abc\n123")
  }

  test("string concatenation with limit") {
    def concat(seq: String*): String = {
      seq.foldLeft(new StringConcat(7)) { (acc, s) => acc.append(s); acc }.toString
    }
    assert(concat("under") === "under")
    assert(concat("under", "over", "extra") === "underov")
    assert(concat("underover") === "underov")
    assert(concat("under", "ov") === "underov")
  }

  test("string concatenation return value") {
    def checkLimit(s: String): Boolean = {
      val sc = new StringConcat(7)
      sc.append(s)
      sc.atLimit
    }
    assert(!checkLimit("under"))
    assert(checkLimit("1234567"))
    assert(checkLimit("1234567890"))
  }

  test("SPARK-31916: StringConcat doesn't overflow on many inputs") {
    val concat = new StringConcat(maxLength = 100)
    val stringToAppend = "Test internal index of StringConcat does not overflow with many " +
      "append calls"
    0.to((Integer.MAX_VALUE / stringToAppend.length) + 1).foreach { _ =>
      concat.append(stringToAppend)
    }
    assert(concat.toString.length === 100)
  }

  test("SPARK-31916: verify that PlanStringConcat's output shows the actual length of the plan") {
    withSQLConf(SQLConf.MAX_PLAN_STRING_LENGTH.key -> "0") {
      val concat = new PlanStringConcat()
      0.to(3).foreach { i =>
        concat.append(s"plan fragment $i")
      }
      assert(concat.toString === "Truncated plan of 60 characters")
    }

    withSQLConf(SQLConf.MAX_PLAN_STRING_LENGTH.key -> "60") {
      val concat = new PlanStringConcat()
      0.to(2).foreach { i =>
        concat.append(s"plan fragment $i")
      }
      assert(concat.toString === "plan fragment 0plan fragment 1... 15 more characters")
    }
  }

  test("SPARK-43841: mix of multipart and single-part identifiers") {
    val baseString = "b"
    // mix of multipart and single-part
    val testStrings = Seq(Seq("c1"), Seq("v1", "c2"), Seq("v2.c2"))
    val expectedOutput = Seq("`c1`", "`v2.c2`", "`v1`.`c2`")
    assert(orderSuggestedIdentifiersBySimilarity(baseString, testStrings) === expectedOutput)
  }
}
