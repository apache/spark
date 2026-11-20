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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StructField, StructType}

/**
 * Tests [[UnresolvedStarExceptOrReplace]] replacements, with and without
 * `appendMissingReplacements`, including nodes that hold several EXCEPT names. Pipe `|> SET` and
 * `|> EVAL` build one node per assignment, so SQL tests only reach the single-name case.
 */
class UnresolvedStarExceptOrReplaceSuite extends SparkFunSuite {

  private val structColumn = AttributeReference(
    "col", StructType(Seq(StructField("i1", IntegerType), StructField("i2", IntegerType))))()

  test("replaces present names in place and appends missing names in EXCEPT list order") {
    val x = $"x".int
    val y = $"y".string
    val newB = Alias(Literal(1), "b")()
    val newX = Alias(Literal(2), "x")()
    val newC = Alias(Literal(3), "c")()
    val star = UnresolvedStarExceptOrReplace(
      target = None,
      excepts = Seq(Seq("b"), Seq("x"), Seq("c")),
      replacements = Some(Seq(newB, newX, newC)),
      appendMissingReplacements = true)

    assert(star.expand(LocalRelation(x, y), caseInsensitiveResolution) ==
      Seq(newX, y, newB, newC))
  }

  test("each replacement takes the place of the column that its EXCEPT name matches") {
    val x = $"x".int
    val y = $"y".string
    val newY = Alias(Literal(1), "y")()
    val newX = Alias(Literal(2), "x")()
    val star = UnresolvedStarExceptOrReplace(
      target = None, excepts = Seq(Seq("y"), Seq("x")), replacements = Some(Seq(newY, newX)))
    assert(star.expand(LocalRelation(x, y), caseInsensitiveResolution) == Seq(newX, newY))
  }

  test("a replacement for a name that two copies of one column share is ambiguous") {
    val x = $"x".int
    Seq(false, true).foreach { appendMissingReplacements =>
      val star = UnresolvedStarExceptOrReplace(
        target = None,
        excepts = Seq(Seq("x")),
        replacements = Some(Seq(Alias(Literal(1), "x")())),
        appendMissingReplacements = appendMissingReplacements)
      checkError(
        exception = intercept[AnalysisException] {
          star.expand(LocalRelation(x, x), caseInsensitiveResolution)
        },
        condition = "AMBIGUOUS_REFERENCE",
        parameters = Map("name" -> "`x`", "referenceNames" -> "[`x`, `x`]"))
    }
  }

  test("a replacement for a name that a copy from a UNION branch shares is ambiguous") {
    // DeduplicateUnionChildOutput gives the second copy of a column in a UNION branch a new ExprId
    // and the `__is_duplicate` tag, so name resolution finds only the first copy.
    val x = $"x".int.withQualifier(Seq("u"))
    val unionCopy = AttributeReference(
      "x", IntegerType, metadata = new MetadataBuilder().putNull("__is_duplicate").build())(
      qualifier = Seq("u"))
    for {
      (exceptParts, name) <- Seq((Seq("x"), "`x`"), (Seq("u", "x"), "`u`.`x`"))
      appendMissingReplacements <- Seq(false, true)
    } {
      val star = UnresolvedStarExceptOrReplace(
        target = None,
        excepts = Seq(exceptParts),
        replacements = Some(Seq(Alias(Literal(1), "x")())),
        appendMissingReplacements = appendMissingReplacements)
      checkError(
        exception = intercept[AnalysisException] {
          star.expand(LocalRelation(x, unionCopy), caseInsensitiveResolution)
        },
        condition = "AMBIGUOUS_REFERENCE",
        parameters = Map("name" -> name, "referenceNames" -> "[`u`.`x`, `u`.`x`]"))
    }

    // A tagged column that the name doesn't match isn't a copy.
    val tx = $"x".int.withQualifier(Seq("t"))
    val newX = Alias(Literal(1), "x")()
    val star = UnresolvedStarExceptOrReplace(
      target = None, excepts = Seq(Seq("t", "x")), replacements = Some(Seq(newX)))
    assert(star.expand(LocalRelation(tx, unionCopy), caseInsensitiveResolution) ==
      Seq(newX, unionCopy))
  }

  test("appending missing replacements rejects a name that only tagged copies match") {
    // Name resolution skips tagged columns when the name matches others, so it finds none here.
    val tag = new MetadataBuilder().putNull("__is_duplicate").build()
    val star = UnresolvedStarExceptOrReplace(
      target = None,
      excepts = Seq(Seq("x")),
      replacements = Some(Seq(Alias(Literal(1), "x")())),
      appendMissingReplacements = true)
    checkError(
      exception = intercept[AnalysisException] {
        star.expand(
          LocalRelation(
            AttributeReference("x", IntegerType, metadata = tag)(),
            AttributeReference("x", IntegerType, metadata = tag)()),
          caseInsensitiveResolution)
      },
      condition = "AMBIGUOUS_REFERENCE",
      parameters = Map("name" -> "`x`", "referenceNames" -> "[`x`, `x`]"))
  }

  test("an EXCEPT name without a replacement excludes every copy of the column it resolves to") {
    val x = $"x".int
    val y = $"y".string
    val star = UnresolvedStarExceptOrReplace(
      target = None, excepts = Seq(Seq("x")), replacements = None)
    assert(star.expand(LocalRelation(x, x, y), caseInsensitiveResolution) == Seq(y))
  }

  test("overlapping EXCEPT names fail the same way with and without replacements") {
    val x = $"x".int
    Seq(
      (Seq(Seq("col"), Seq("col", "i1")), LocalRelation(structColumn), "col, col.i1"),
      (Seq(Seq("x"), Seq("x")), LocalRelation(x, x), "x, x")
    ).foreach { case (excepts, relation, columns) =>
      val replacements = Some(excepts.map(exceptParts => Alias(Literal(1), exceptParts.last)()))
      Seq(
        UnresolvedStarExceptOrReplace(target = None, excepts = excepts, replacements = None),
        UnresolvedStarExceptOrReplace(
          target = None, excepts = excepts, replacements = replacements),
        UnresolvedStarExceptOrReplace(
          target = None,
          excepts = excepts,
          replacements = replacements,
          appendMissingReplacements = true)
      ).foreach { star =>
        checkError(
          exception = intercept[AnalysisException] {
            star.expand(relation, caseInsensitiveResolution)
          },
          condition = "EXCEPT_OVERLAPPING_COLUMNS",
          parameters = Map("columns" -> columns))
      }
    }
  }

  test("a replacement for a nested EXCEPT name is an internal error") {
    Seq(false, true).foreach { appendMissingReplacements =>
      val star = UnresolvedStarExceptOrReplace(
        target = None,
        excepts = Seq(Seq("col", "i1")),
        replacements = Some(Seq(Alias(Literal(42), "i1")())),
        appendMissingReplacements = appendMissingReplacements)
      checkError(
        exception = intercept[SparkException] {
          star.expand(LocalRelation(structColumn), caseInsensitiveResolution)
        },
        condition = "INTERNAL_ERROR",
        parameters = Map(
          "message" -> "An EXCEPT name with a replacement must resolve to a top-level column"))
    }
  }

  test("a qualified EXCEPT name with a replacement replaces its column") {
    val x = $"x".int.withQualifier(Seq("t"))
    val y = $"y".string.withQualifier(Seq("t"))
    val newX = Alias(Literal(1), "x")()
    val star = UnresolvedStarExceptOrReplace(
      target = None, excepts = Seq(Seq("t", "x")), replacements = Some(Seq(newX)))
    assert(star.expand(LocalRelation(x, y), caseInsensitiveResolution) == Seq(newX, y))
  }

  test("appending missing replacements requires one replacement per EXCEPT name") {
    Seq(None, Some(Seq.empty[NamedExpression])).foreach { replacements =>
      val star = UnresolvedStarExceptOrReplace(
        target = None,
        excepts = Seq(Seq("x")),
        replacements = replacements,
        appendMissingReplacements = true)
      checkError(
        exception = intercept[SparkException] {
          star.expand(LocalRelation($"x".int), caseInsensitiveResolution)
        },
        condition = "INTERNAL_ERROR",
        parameters = Map(
          "message" -> "Appending missing replacements requires one replacement per except"))
    }
  }
}
