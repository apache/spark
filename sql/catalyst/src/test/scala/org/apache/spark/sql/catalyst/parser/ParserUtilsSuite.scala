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

import org.antlr.v4.runtime.CommonTokenStream

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.trees.Origin

class ParserUtilsSuite extends SparkFunSuite {

  import ParserUtils._

  val setConfContext = buildContext("set example.setting.name=setting.value") { parser =>
    parser.statement().asInstanceOf[SetConfigurationContext]
  }

  val showFuncContext = buildContext("show functions foo.bar") { parser =>
    parser.statement().asInstanceOf[ShowFunctionsContext]
  }

  val descFuncContext = buildContext("describe function extended bar") { parser =>
    parser.statement().asInstanceOf[DescribeFunctionContext]
  }

  val showDbsContext = buildContext("show databases like 'identifier_with_wildcards'") { parser =>
    parser.statement().asInstanceOf[ShowDatabasesContext]
  }

  val createDbContext = buildContext(
    """
      |CREATE DATABASE IF NOT EXISTS database_name
      |COMMENT 'database_comment' LOCATION '/home/user/db'
      |WITH DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')
    """.stripMargin
  ) { parser =>
    parser.statement().asInstanceOf[CreateDatabaseContext]
  }

  private def buildContext[T](command: String)(toResult: SqlBaseParser => T): T = {
    val lexer = new SqlBaseLexer(new ANTLRNoCaseStringStream(command))
    val tokenStream = new CommonTokenStream(lexer)
    val parser = new SqlBaseParser(tokenStream)
    toResult(parser)
  }

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

  test("command") {
    assert(command(setConfContext) == "set example.setting.name=setting.value")
    assert(command(showFuncContext) == "show functions foo.bar")
    assert(command(descFuncContext) == "describe function extended bar")
    assert(command(showDbsContext) == "show databases like 'identifier_with_wildcards'")
  }

  test("source") {
    assert(source(setConfContext) == "set example.setting.name=setting.value")
    assert(source(showFuncContext) == "show functions foo.bar")
    assert(source(descFuncContext) == "describe function extended bar")
    assert(source(showDbsContext) == "show databases like 'identifier_with_wildcards'")
  }

  test("remainder") {
    assert(remainder(setConfContext) == "")
    assert(remainder(showFuncContext) == "")
    assert(remainder(descFuncContext) == "")
    assert(remainder(showDbsContext) == "")

    assert(remainder(setConfContext.SET.getSymbol) == " example.setting.name=setting.value")
    assert(remainder(showFuncContext.FUNCTIONS.getSymbol) == " foo.bar")
    assert(remainder(descFuncContext.EXTENDED.getSymbol) == " bar")
    assert(remainder(showDbsContext.LIKE.getSymbol) == " 'identifier_with_wildcards'")
  }

  test("string") {
    assert(string(showDbsContext.pattern) == "identifier_with_wildcards")
    assert(string(createDbContext.comment) == "database_comment")

    assert(string(createDbContext.locationSpec.STRING) == "/home/user/db")
  }

  test("position") {
    assert(position(setConfContext.start) == Origin(Some(1), Some(0)))
    assert(position(showFuncContext.stop) == Origin(Some(1), Some(19)))
    assert(position(descFuncContext.describeFuncName.start) == Origin(Some(1), Some(27)))
  }
}
