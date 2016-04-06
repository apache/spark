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
package org.apache.spark.sql.catalyst.parser

import org.apache.spark.SparkFunSuite

class ParserUtilsSuite extends SparkFunSuite {

  import ParserUtils._

  test("unescapeSQLString") {
    // scalastyle:off nonascii

    // String not including escaped characters and enclosed by double quotes.
    assert(unescapeSQLString(""""abcdefg"""") == "abcdefg")

    // String enclosed by single quotes.
    assert(unescapeSQLString("""'C0FFEE'""") == "C0FFEE")

    // Strings including single escaped characters.
    assert(unescapeSQLString("""'\0'""") == "\u0000")
    assert(unescapeSQLString(""""\'"""") == "\'")
    assert(unescapeSQLString("""'\"'""") == "\"")
    assert(unescapeSQLString(""""\b"""") == "\b")
    assert(unescapeSQLString("""'\n'""") == "\n")
    assert(unescapeSQLString(""""\r"""") == "\r")
    assert(unescapeSQLString("""'\t'""") == "\t")
    assert(unescapeSQLString(""""\Z"""") == "\u001A")
    assert(unescapeSQLString("""'\\'""") == "\\")
    assert(unescapeSQLString(""""\%"""") == "\\%")
    assert(unescapeSQLString("""'\_'""") == "\\_")

    // String including '\000' style literal characters.
    assert(unescapeSQLString("""'3 + 5 = \070'""") == "3 + 5 = \u0038")
    assert(unescapeSQLString(""""\000"""") == "\u0000")

    // String including invalid '\000' style literal characters.
    assert(unescapeSQLString(""""\256"""") == "256")

    // String including a '\u0000' style literal characters (\u732B is a cat in Kanji).
    assert(unescapeSQLString(""""How cute \u732B are"""")  == "How cute \u732B are")

    // String including a surrogate pair character
    // (\uD867\uDE3D is Okhotsk atka mackerel in Kanji).
    assert(unescapeSQLString(""""\uD867\uDE3D is a fish"""") == "\uD867\uDE3D is a fish")

    // scalastyle:on nonascii
  }

  // TODO: Add test cases for other methods in ParserUtils
}
