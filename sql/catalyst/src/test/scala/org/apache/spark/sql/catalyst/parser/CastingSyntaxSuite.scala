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

package org.apache.spark.sql.catalyst.parser;

import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedAttribute, UnresolvedFunction, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException, ParserInterface}
import org.apache.spark.sql.types.{DoubleType, IntegerType}

class CastingSyntaxSuite extends AnalysisTest {
  import org.apache.spark.sql.catalyst.dsl.expressions._
  val defaultParser = CatalystSqlParser

  def assertEqual(
      sqlCommand: String,
      e: Expression,
      parser: ParserInterface = defaultParser): Unit = {
    compareExpressions(parser.parseExpression(sqlCommand), e)
  }

  def assertFails(sql: String, errorMsg: String): Unit = {
    val e = intercept[ParseException](defaultParser.parseExpression(sql))
    assert(e.getMessage.contains(errorMsg))
  }

  test("literals") {
    assertEqual("123::double", Cast(Literal(123), DoubleType))
    assertEqual("'123'::double", Cast(Literal("123"), DoubleType))
    assertEqual("'123'::int", Cast(Literal("123"), IntegerType))
    assertEqual("'123.0'::double", Cast(Literal("123.0"), DoubleType))
    assertEqual("'123.0' :: double", Cast(Literal("123.0"), DoubleType))
    assertEqual("`123`::double", Cast(UnresolvedAttribute(Seq("123")), DoubleType))

    assertEqual("`123::double`", UnresolvedAttribute(Seq("123::double")))
  }

  test("named expressions") {
    assertEqual("123::double as v", Alias(Cast(Literal(123), DoubleType), "v")())
    assertEqual("123::double v", Alias(Cast(Literal(123), DoubleType), "v")())
    assertEqual("123 :: double v", Alias(Cast(Literal(123), DoubleType), "v")())
    assertEqual("abc::double v", Alias(Cast(UnresolvedAttribute("abc"), DoubleType), "v")())
    assertEqual("`abc`::double v", Alias(Cast(UnresolvedAttribute("abc"), DoubleType), "v")())
    assertEqual("abc.def::double v",
      Alias(Cast(UnresolvedAttribute(Seq("abc", "def")), DoubleType), "v")())
    assertEqual("`abc.def`::double v",
      Alias(Cast(UnresolvedAttribute(Seq("abc.def")), DoubleType), "v")())
  }

  test("boolean expressions") {
    assertEqual("(a and b) :: int", Cast(Symbol("a") && Symbol("b"), IntegerType))
    assertEqual("(a or b) :: int", Cast(Symbol("a") || Symbol("b"), IntegerType))
  }

  test("arithmetic expressions") {
    assertEqual("(a - b) :: int", Cast(Symbol("a") - Symbol("b"), IntegerType))
    assertEqual("(a * b) :: int", Cast(Symbol("a") * Symbol("b"), IntegerType))
    assertEqual("a + b :: int", Symbol("a") + Cast(Symbol("b"), IntegerType))
  }

  test("star expansion") {
    // While these don't make sense, they're not against the parser. Should they be? They work
    // with normal casting too
    assertEqual("* :: int", Cast(UnresolvedStar(None), IntegerType))
    assertEqual("str.* :: int", Cast(UnresolvedStar(Some(Seq("str"))), IntegerType))
  }

  test("functions") {
    assertEqual(
      "get_json_object(blob, '$.field')::int",
      Cast(UnresolvedFunction("get_json_object",
        Seq(UnresolvedAttribute("blob"), Literal("$.field")),
        isDistinct = false), IntegerType))

    assertEqual(
      "max(value::double)",
      UnresolvedFunction("max",
        Seq(Cast(UnresolvedAttribute("value"), DoubleType)),
        isDistinct = false))

    assertEqual(
      "cast(value::int as double)",
      Cast(Cast(UnresolvedAttribute("value"), IntegerType), DoubleType))

    assertEqual(
      "value::int::double",
      Cast(Cast(UnresolvedAttribute("value"), IntegerType), DoubleType))
  }
}
