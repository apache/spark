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

import scala.jdk.CollectionConverters._

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream, ParserRuleContext}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}

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
    parser.statement().asInstanceOf[ShowNamespacesContext]
  }

  val createDbContext = buildContext(
    """
      |CREATE DATABASE IF NOT EXISTS database_name
      |COMMENT 'database_comment' LOCATION '/home/user/db'
      |WITH DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')
    """.stripMargin
  ) { parser =>
    parser.statement().asInstanceOf[CreateNamespaceContext]
  }

  val castClause =
    """
      |CAST(1 /* Convert
      |   INT
      |   AS
      |   String */ as STRING)""".stripMargin.trim

  val castQuery =
    s"""
       |SELECT
       |$castClause /* SHOULD NOT INCLUDE THIS */
       | AS s
       |""".stripMargin

  val castQueryContext = buildContext(castQuery) { parser =>
    parser.statement().asInstanceOf[StatementDefaultContext]
  }

  val emptyContext = buildContext("") { parser =>
    parser.statement
  }

  private def buildContext[T](command: String)(toResult: SqlBaseParser => T): T = {
    val lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(command)))
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
    assert(unescapeSQLString("\"How cute \\u732B are\"")  == "How cute \u732B are")

    // String including a surrogate pair character
    // (\uD867\uDE3D is Okhotsk atka mackerel in Kanji).
    assert(unescapeSQLString("\"\\uD867\\uDE3D is a fish\"") == "\uD867\uDE3D is a fish")

    // String including a '\U00000000' style literal characters (\u732B is a cat in Kanji).
    assert(unescapeSQLString("\"\\U00000041B\\U000000312\\U0000732B\"") == "AB12\u732B")

    // String including surrogate pair characters (U+1F408 is a cat and U+1F415 is a dog in Emoji).
    assert(unescapeSQLString("\"\\U0001F408 \\U0001F415\"") == "\uD83D\uDC08 \uD83D\uDC15")

    // String including escaped normal characters.
    assert(unescapeSQLString(
      """"ab\
        |cd\ef"""".stripMargin) ==
      """ab
        |cdef""".stripMargin)

    // String with an invalid '\' as the last character.
    assert(unescapeSQLString(""""abc\"""") == "abc\\")

    // Strings containing invalid Unicode escapes with non-hex characters.
    assert(unescapeSQLString("\"abc\\uXXXXa\"") == "abcuXXXXa")
    assert(unescapeSQLString("\"abc\\uxxxxa\"") == "abcuxxxxa")
    assert(unescapeSQLString("\"abc\\UXXXXXXXXa\"") == "abcUXXXXXXXXa")
    assert(unescapeSQLString("\"abc\\Uxxxxxxxxa\"") == "abcUxxxxxxxxa")
    // Guard against off-by-one errors in the "all chars are hex" routine:
    assert(unescapeSQLString("\"abc\\uAAAXa\"") == "abcuAAAXa")

    // scalastyle:on nonascii
  }

  test("command") {
    assert(command(setConfContext) == "set example.setting.name=setting.value")
    assert(command(showFuncContext) == "show functions foo.bar")
    assert(command(descFuncContext) == "describe function extended bar")
    assert(command(showDbsContext) == "show databases like 'identifier_with_wildcards'")
  }

  test("operationNotAllowed") {
    val errorMessage = "parse.fail.operation.not.allowed.error.message"
    checkError(
      exception = intercept[ParseException] {
        operationNotAllowed(errorMessage, showFuncContext)
      },
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> errorMessage))
  }

  test("checkDuplicateKeys") {
    val properties = Seq(("a", "a"), ("b", "b"), ("c", "c"))
    checkDuplicateKeys[String](properties, createDbContext)

    val properties2 = Seq(("a", "a"), ("b", "b"), ("a", "c"))
    checkError(
      exception = intercept[ParseException] {
        checkDuplicateKeys(properties2, createDbContext)
      },
      errorClass = "DUPLICATE_KEY",
      parameters = Map("keyColumn" -> "`a`"))
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
    assert(string(showDbsContext.pattern.STRING_LITERAL()) == "identifier_with_wildcards")
    assert(string(createDbContext.commentSpec().get(0).stringLit().STRING_LITERAL()) ==
      "database_comment")

    assert(string(createDbContext.locationSpec.asScala.head.stringLit().STRING_LITERAL()) ==
      "/home/user/db")
  }

  test("position") {
    assert(position(setConfContext.start) == Origin(Some(1), Some(0)))
    assert(position(showFuncContext.stop) == Origin(Some(1), Some(19)))
    assert(position(descFuncContext.describeFuncName.start) == Origin(Some(1), Some(27)))
    assert(position(createDbContext.locationSpec.asScala.head.start) == Origin(Some(3), Some(27)))
    assert(position(emptyContext.stop) == Origin(None, None))
  }

  test("validate") {
    val f1 = { ctx: ParserRuleContext =>
      ctx.children != null && !ctx.children.isEmpty
    }
    val message = "ParserRuleContext should not be empty."
    validate(f1(showFuncContext), message, showFuncContext)

    checkError(
      exception = intercept[ParseException] {
        validate(f1(emptyContext), message, emptyContext)
      },
      errorClass = "_LEGACY_ERROR_TEMP_0064",
      parameters = Map("msg" -> message))
  }

  test("withOrigin") {
    val ctx = createDbContext.locationSpec.asScala.head
    val current = CurrentOrigin.get
    val (location, origin) = withOrigin(ctx) {
      (string(ctx.stringLit().STRING_LITERAL), CurrentOrigin.get)
    }
    assert(location == "/home/user/db")
    assert(origin == Origin(Some(3), Some(27)))
    assert(CurrentOrigin.get == current)
  }

  private def findCastContext(ctx: ParserRuleContext): Option[CastContext] = {
    ctx match {
      case context: CastContext =>
        Some(context)
      case _ =>
        val it = ctx.children.iterator()
        while(it.hasNext) {
          it.next() match {
            case p: ParserRuleContext =>
              val childResult = findCastContext(p)
              if (childResult.isDefined) {
                return childResult
              }
            case _ =>
          }
        }
        None
    }
  }

  test("withOrigin: setting SQL text") {
    withOrigin(castQueryContext, Some(castQuery)) {
      assert(CurrentOrigin.get.sqlText.contains(castQuery))
      val castContext = findCastContext(castQueryContext)
      assert(castContext.isDefined)
      withOrigin(castContext.get) {
        val current = CurrentOrigin.get
        assert(current.sqlText.contains(castQuery))
        assert(current.startIndex.isDefined)
        assert(current.stopIndex.isDefined)
        // With sqlText, startIndex, stopIndex, we can get the corresponding SQL text of the
        // Cast clause.
        assert(current.sqlText.get.substring(current.startIndex.get, current.stopIndex.get + 1) ==
          castClause)
      }
    }
  }
}
