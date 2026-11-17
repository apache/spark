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
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation

/**
 * Tests [[UnresolvedStarExceptOrReplace]] with `appendMissingReplacements` on a node that holds
 * several EXCEPT names. Pipe `|> EVAL` builds one node per assignment, so SQL tests only reach the
 * single-name case.
 */
class UnresolvedStarExceptOrReplaceSuite extends SparkFunSuite {

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
}
