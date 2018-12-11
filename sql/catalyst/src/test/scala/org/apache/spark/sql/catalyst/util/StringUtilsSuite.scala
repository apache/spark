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
import org.apache.spark.sql.catalyst.util.StringUtils._

class StringUtilsSuite extends SparkFunSuite {

  test("escapeLikeRegex") {
    assert(escapeLikeRegex("abdef") === "(?s)\\Qa\\E\\Qb\\E\\Qd\\E\\Qe\\E\\Qf\\E")
    assert(escapeLikeRegex("a\\__b") === "(?s)\\Qa\\E\\Q_\\E.\\Qb\\E")
    assert(escapeLikeRegex("a_%b") === "(?s)\\Qa\\E..*\\Qb\\E")
    assert(escapeLikeRegex("a%\\%b") === "(?s)\\Qa\\E.*\\Q%\\E\\Qb\\E")
    assert(escapeLikeRegex("a%") === "(?s)\\Qa\\E.*")
    assert(escapeLikeRegex("**") === "(?s)\\Q*\\E\\Q*\\E")
    assert(escapeLikeRegex("a_b") === "(?s)\\Qa\\E.\\Qb\\E")
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

  test("split a SQL") {
    val statement = "select * from tmp.dada;"
    assert(StringUtils.split(statement) === Array("select * from tmp.dada"))

    val statements = "select * from tmp.dada;;select * from tmp.ada;"
    assert(StringUtils.split(statements) ===
      Array("select * from tmp.dada", "select * from tmp.ada"))

    val semicolonInStr =
      """
        |select "^;^"
      """.stripMargin.trim
    assert(StringUtils.split(semicolonInStr) === Array(semicolonInStr))

    val inlineComments =
      """
        |select 1; --;;;;;;;;
        |select "---";
      """.stripMargin
    val select1 = "select 1"
    val selectComments =
      """
        |select "---"
      """.stripMargin.trim
    assert(StringUtils.split(inlineComments) === Array(select1, selectComments))
  }

  test("string concatenation") {
    def concat(seq: String*): String = {
      seq.foldLeft(new StringConcat())((acc, s) => {acc.append(s); acc}).toString
    }

    assert(new StringConcat().toString == "")
    assert(concat("") == "")
    assert(concat(null) == "")
    assert(concat("a") == "a")
    assert(concat("1", "2") == "12")
    assert(concat("abc", "\n", "123") == "abc\n123")
  }
}
