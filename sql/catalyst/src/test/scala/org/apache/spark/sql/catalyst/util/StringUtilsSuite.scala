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

  test("SPARK-34872: quoteIfNeeded should quote a string which contains non-word characters") {
    assert(quoteIfNeeded("a b") === "`a b`")
    assert(quoteIfNeeded("a*b") === "`a*b`")
    assert(quoteIfNeeded("123") === "`123`")
    assert(quoteIfNeeded("1a") === "1a")
    assert(quoteIfNeeded("_ab_") === "_ab_")
    assert(quoteIfNeeded("_") === "_")
    assert(quoteIfNeeded("") === "``")
  }

  test("SPARK-43841: mix of multipart and single-part identifiers") {
    val baseString = "b"
    // mix of multipart and single-part
    val testStrings = Seq(Seq("c1"), Seq("v1", "c2"), Seq("v2.c2"))
    val expectedOutput = Seq("`c1`", "`v2.c2`", "`v1`.`c2`")
    assert(orderSuggestedIdentifiersBySimilarity(baseString, testStrings) === expectedOutput)
  }
}
