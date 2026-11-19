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

import org.antlr.v4.runtime.CommonToken

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin, SQLQueryContext}

/**
 * Test suite for the new parameter substitution architecture. Tests the unified parameter
 * handling components.
 */
class ParameterSubstitutionSuite extends SparkFunSuite {

  test("ParameterHandler - basic named parameter substitution") {
    val context = NamedParameterContext(Map("param1" -> Literal(42)))
    val (result, _) = ParameterHandler.substituteParameters("SELECT :param1", context)
    assert(result === "SELECT 42")
  }

  test("ParameterHandler - basic positional parameter substitution") {
    val context = PositionalParameterContext(Seq(Literal(42)))
    val (result, _) = ParameterHandler.substituteParameters("SELECT ?", context)
    assert(result === "SELECT 42")
  }

  test("ParameterHandler - auto rule detection") {
    val context = NamedParameterContext(Map("param1" -> Literal(42)))

    // Regular statement
    val (result1, _) = ParameterHandler.substituteParameters("SELECT :param1", context)
    assert(result1 === "SELECT 42")

    // SQL scripting block
    val (result2, _) = ParameterHandler.substituteParameters("BEGIN SELECT :param1; END", context)
    assert(result2.contains("SELECT 42"))
  }

  test("ParameterHandler - mixed parameter detection during substitution") {

    // Test that named parameters work
    val (result1, _) = ParameterHandler.substituteParameters(
      "SELECT :param1",
      NamedParameterContext(Map("param1" -> Literal("value"))))
    assert(result1.contains("'value'"))

    // Test that positional parameters work
    val (result2, _) = ParameterHandler.substituteParameters(
      "SELECT ?",
      PositionalParameterContext(List(Literal(42))))
    assert(result2.contains("42"))

    // Test that no parameters works
    val (result3, _) =
      ParameterHandler.substituteParameters("SELECT 1", NamedParameterContext(Map.empty))
    assert(result3 == "SELECT 1")
  }

  test("ParameterHandler - named parameters direct") {

    val (result, _) =
      ParameterHandler.substituteNamedParameters("SELECT :param1", Map("param1" -> Literal(42)))
    assert(result === "SELECT 42")
  }

  test("ParameterHandler - positional parameters direct") {

    val (result, _) =
      ParameterHandler.substitutePositionalParameters("SELECT ?", Seq(Literal(42)))
    assert(result === "SELECT 42")
  }

  test("ParameterHandler - empty parameters") {

    val (result1, _) = ParameterHandler.substituteNamedParameters("SELECT 1", Map.empty)
    assert(result1 === "SELECT 1")

    val (result2, _) = ParameterHandler.substitutePositionalParameters("SELECT 1", Seq.empty)
    assert(result2 === "SELECT 1")
  }

  test("Integration - complex parameter substitution") {
    import org.apache.spark.sql.types._

    val arrayData = Array(1, 2, 3)
    val mapData = Map("status" -> "active")

    val context = NamedParameterContext(
      Map(
        "id" -> Literal(42),
        "name" -> Literal("John"),
        "ids" -> Literal.create(arrayData, ArrayType(IntegerType)),
        "metadata" -> Literal.create(mapData, MapType(StringType, StringType))))

    val sql = "SELECT :id, :name WHERE id IN (SELECT explode(:ids)) AND meta = :metadata"
    val (result, _) = ParameterHandler.substituteParameters(sql, context)

    assert(result.contains("42"))
    assert(result.contains("'John'"))
    assert(result.contains("ARRAY(1, 2, 3)"))
    assert(result.contains("MAP("))
    assert(result.contains("'status', 'active'"))
  }

  test("Error handling - mixed parameters validation") {

    // Mixed parameters should be detected during substitution itself
    intercept[Exception] {
      ParameterHandler.substituteParameters(
        "SELECT :named, ?",
        NamedParameterContext(Map("named" -> Literal("value"))))
    }
  }

  test("ParameterHandler - named parameter with emoji in SQL") {
    val emoji = new String(Character.toChars(0x1f4aa)) // supplementary char (2 UTF-16 code units)
    val context = NamedParameterContext(Map("team" -> Literal("abc")))
    val sql = s"SELECT '${emoji}' AS a FROM T WHERE :team IS NULL"
    val (result, mapper) = ParameterHandler.substituteParameters(sql, context)
    assert(result === s"SELECT '${emoji}' AS a FROM T WHERE 'abc' IS NULL")
    val whereStart = result.codePointCount(0, result.indexOf("WHERE"))
    val whereStop = result.codePointCount(0, result.length) - 1
    assert(mapper.originalFragment(whereStart, whereStop) === "WHERE :team IS NULL")
  }

  test("ParameterHandler - positional parameter with emoji in SQL") {
    val emoji = new String(Character.toChars(0x1f4aa))
    val context = PositionalParameterContext(Seq(Literal("abc")))
    val sql = s"SELECT '${emoji}' AS a FROM T WHERE ? IS NULL"
    val (result, _) = ParameterHandler.substituteParameters(sql, context)
    assert(result === s"SELECT '${emoji}' AS a FROM T WHERE 'abc' IS NULL")
  }

  test("ParameterHandler - multiple params with emoji in SQL and replacement values") {
    val flexed = new String(Character.toChars(0x1f4aa))
    val tada = new String(Character.toChars(0x1f389))
    val context = NamedParameterContext(Map("p1" -> Literal(tada), "p2" -> Literal(42)))
    val sql = s"SELECT '${flexed}', :p1, '${flexed}', :p2"
    val (result, _) = ParameterHandler.substituteParameters(sql, context)
    assert(result === s"SELECT '${flexed}', '${tada}', '${flexed}', 42")
  }

  test("ParameterHandler - positional params with multiple emojis") {
    val flexed = new String(Character.toChars(0x1f4aa))
    val tada = new String(Character.toChars(0x1f389))
    val context = PositionalParameterContext(Seq(Literal(tada), Literal(99)))
    val sql = s"SELECT '${flexed}', ?, '${flexed}${flexed}', ?"
    val (result, _) = ParameterHandler.substituteParameters(sql, context)
    assert(result === s"SELECT '${flexed}', '${tada}', '${flexed}${flexed}', 99")
  }

  test("whole-text origin stopIndex after substitution is a code-point offset") {
    val emoji = new String(Character.toChars(0x1f600))
    val original = s"SELECT '$emoji' AS a, :p"
    val (substituted, mapper) = ParameterHandler.substituteParameters(
      original,
      NamedParameterContext(Map("p" -> Literal(1))))
    val origin = Origin(
      sqlText = Some(substituted),
      startIndex = Some(0),
      stopIndex = Some(substituted.codePointCount(0, substituted.length) - 1),
      positionMapper = Some(mapper))
    val ctx = origin.context.asInstanceOf[SQLQueryContext]
    assert(ctx.isValid)
    assert(ctx.fragment === original)
    assert(ctx.startIndex === 0)
    assert(ctx.stopIndex === original.codePointCount(0, original.length) - 1)
  }

  test("origin range inside a shorter or longer replacement maps to the parameter marker") {
    Seq("longParameter" -> "x", "p" -> "a much longer value").foreach { case (name, value) =>
      val marker = s":$name"
      val original = s"SELECT $marker"
      val (substituted, mapper) = ParameterHandler.substituteParameters(
        original,
        NamedParameterContext(Map(name -> Literal(value))))
      val replacementStart = "SELECT ".length
      val origin = Origin(
        sqlText = Some(substituted),
        startIndex = Some(replacementStart),
        stopIndex = Some(substituted.length - 1),
        positionMapper = Some(mapper))
      val context = origin.context.asInstanceOf[SQLQueryContext]
      assert(context.fragment === marker)
      assert(context.startIndex === original.indexOf(marker))
      assert(context.stopIndex === original.length - 1)
    }
  }

  test("PositionMapper maps line and column in Unicode and multiline SQL") {
    val emoji = new String(Character.toChars(0x1f600))
    val marker = ":longParameter"
    val replacement = "'first\nsecond'"
    val original = s"SELECT '$emoji'\nFROM $marker, missing"
    val substituted = s"SELECT '$emoji'\nFROM $replacement, missing"
    val markerStart = original.codePointCount(0, original.indexOf(marker))
    val mapper = PositionMapper(
      original,
      substituted,
      List(
        Substitution(ParameterLocation(markerStart, markerStart + marker.length), replacement)))
    val missingCharIndex = substituted.indexOf("missing")
    val missingStart = substituted.codePointCount(0, missingCharIndex)
    val substitutedLineStart = substituted.lastIndexOf('\n', missingCharIndex - 1) + 1
    val substitutedLine = substituted.substring(0, substitutedLineStart).count(_ == '\n') + 1
    val substitutedPosition = substituted.codePointCount(substitutedLineStart, missingCharIndex)
    val linePrefix = original.substring(original.indexOf("FROM"), original.indexOf("missing"))
    val expectedPosition = linePrefix.codePointCount(0, linePrefix.length)
    assert(mapper.mapToOriginalLineAndPosition(missingStart) === (2, expectedPosition))
    assert(
      mapper.mapLineAndPositionToOriginal(substitutedLine, substitutedPosition) ===
        (2, expectedPosition))
  }

  test("ParseErrorListener maps positions through the parameter mapper") {
    val original = "SELECT 'prefix':longParameter, missing"
    Seq[Any](1, "a much longer value").foreach { value =>
      val (substituted, mapper) = ParameterHandler.substituteParameters(
        original,
        NamedParameterContext(Map("longParameter" -> Literal(value))))
      val substitutedStart = substituted.indexOf("missing")
      val token = new CommonToken(0)
      token.setStartIndex(substitutedStart)
      token.setLine(1)
      token.setCharPositionInLine(substitutedStart)

      val exception = CurrentOrigin.withOrigin(Origin(positionMapper = Some(mapper))) {
        intercept[ParseException] {
          ParseErrorListener.syntaxError(
            null,
            token,
            1,
            substitutedStart,
            "test syntax error",
            null)
        }
      }
      assert(mapper.mapToOriginal(substitutedStart) === original.indexOf("missing"))
      assert(exception.line === Some(1))
      assert(exception.startPosition === Some(original.indexOf("missing")))
    }
  }

  test("PositionMapper maps past a supplementary replacement in code points") {
    val emoji = new String(Character.toChars(0x1f600))
    val original = "SELECT :p AS a, DATE 'bad'"
    val (substituted, mapper) = ParameterHandler.substituteParameters(
      original,
      NamedParameterContext(Map("p" -> Literal(emoji))))
    val dateInSubstituted = substituted.codePointCount(0, substituted.indexOf("DATE"))
    assert(mapper.mapToOriginal(dateInSubstituted) === original.indexOf("DATE"))
  }

  test("PositionMapper accounts for quote-adjacent separator spaces") {
    val original = "SELECT 'prefix':hello, missing"
    val (substituted, mapper) = ParameterHandler.substituteParameters(
      original,
      NamedParameterContext(Map("hello" -> Literal("value"))))
    assert(substituted === "SELECT 'prefix' 'value', missing")
    val missingStart = substituted.indexOf("missing")
    assert(mapper.mapToOriginal(missingStart) === original.indexOf("missing"))
    assert(mapper.originalFragment(0, substituted.length - 1) === original)
  }

  test("Large parameter set") {

    val largeParamMap = (1 to 100).map(i => s"param$i" -> Literal(i)).toMap
    val context = NamedParameterContext(largeParamMap)
    val paramRefs = (1 to 100).map(i => s":param$i").mkString(", ")
    val sql = s"SELECT $paramRefs"

    val (result, _) = ParameterHandler.substituteParameters(sql, context)

    assert(result.contains("1, 2, 3"))
    assert(result.contains("98, 99, 100"))
  }
}
