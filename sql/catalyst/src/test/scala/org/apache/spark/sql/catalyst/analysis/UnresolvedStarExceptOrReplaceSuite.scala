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
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
 * Tests [[UnresolvedStarExceptOrReplace]] replacements, including a node that holds several EXCEPT
 * names. Pipe `|> SET` builds one node per assignment, so SQL tests only reach the single-name
 * case.
 */
class UnresolvedStarExceptOrReplaceSuite extends SparkFunSuite {

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
    val star = UnresolvedStarExceptOrReplace(
      target = None, excepts = Seq(Seq("x")), replacements = Some(Seq(Alias(Literal(1), "x")())))
    checkError(
      exception = intercept[AnalysisException] {
        star.expand(LocalRelation(x, x), caseInsensitiveResolution)
      },
      condition = "AMBIGUOUS_REFERENCE",
      parameters = Map("name" -> "`x`", "referenceNames" -> "[`x`, `x`]"))
  }

  test("an EXCEPT name without a replacement excludes every copy of its column") {
    val x = $"x".int
    val y = $"y".string
    val star = UnresolvedStarExceptOrReplace(
      target = None, excepts = Seq(Seq("x")), replacements = None)
    assert(star.expand(LocalRelation(x, x, y), caseInsensitiveResolution) == Seq(y))
  }

  test("a replacement for a nested EXCEPT name is an internal error") {
    val col = AttributeReference(
      "col", StructType(Seq(StructField("i1", IntegerType), StructField("i2", IntegerType))))()
    val star = UnresolvedStarExceptOrReplace(
      target = None,
      excepts = Seq(Seq("col", "i1")),
      replacements = Some(Seq(Alias(Literal(42), "i1")())))
    checkError(
      exception = intercept[SparkException] {
        star.expand(LocalRelation(col), caseInsensitiveResolution)
      },
      condition = "INTERNAL_ERROR",
      parameters = Map(
        "message" -> "An EXCEPT name with a replacement must resolve to a top-level column"))
  }

  test("a qualified EXCEPT name with a replacement replaces its column") {
    val x = $"x".int.withQualifier(Seq("t"))
    val y = $"y".string.withQualifier(Seq("t"))
    val newX = Alias(Literal(1), "x")()
    val star = UnresolvedStarExceptOrReplace(
      target = None, excepts = Seq(Seq("t", "x")), replacements = Some(Seq(newX)))
    assert(star.expand(LocalRelation(x, y), caseInsensitiveResolution) == Seq(newX, y))
  }
}
