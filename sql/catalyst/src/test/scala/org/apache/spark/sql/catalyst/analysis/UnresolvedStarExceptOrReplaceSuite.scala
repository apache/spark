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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation

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
}
