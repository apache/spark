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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation

/**
 * Test suite for moving non-deterministic expressions into Project.
 */
class PullOutNondeterministicSuite extends AnalysisTest {

  private lazy val a = 'a.int
  private lazy val b = 'b.int
  private lazy val c = 'c.double

  private lazy val r = LocalRelation(a, b)
  private lazy val left = LocalRelation(c)
  private lazy val right = LocalRelation(a)
  private lazy val rnd = Rand(10).as('_nondeterministic)
  private lazy val rndref = rnd.toAttribute

  test("no-op on filter") {
    checkAnalysis(
      r.where(Rand(10) > Literal(1.0)),
      r.where(Rand(10) > Literal(1.0))
    )
  }

  test("sort") {
    checkAnalysis(
      r.sortBy(SortOrder(Rand(10), Ascending)),
      r.select(a, b, rnd).sortBy(SortOrder(rndref, Ascending)).select(a, b)
    )
  }

  test("aggregate") {
    checkAnalysis(
      r.groupBy(Rand(10))(Rand(10).as("rnd")),
      r.select(a, b, rnd).groupBy(rndref)(rndref.as("rnd"))
    )
  }

  test("join") {
    val rnd = Rand(a).as('_nondeterministic)
    val rndref = rnd.toAttribute

    // Deterministic joining keys and non-deterministic conditions: not push down
    val m1 = intercept[AnalysisException] {
      val plan = left.join(right,
        Inner, Some(And(EqualTo(Ceil(c), a), EqualTo(Rand(a), Literal(10.1))))).analyze
      SimpleAnalyzer.checkAnalysis(plan)
    }.getMessage
    assert(m1.contains("nondeterministic expressions are only allowed in"))

    // Non-determinstic joining keys and non-determinstic conditions: not push down
    val m2 = intercept[AnalysisException] {
      val plan = left.join(right,
        Inner, Some(And(EqualTo(c, Rand(a)), EqualTo(Rand(a), Literal(10.1)))))
      SimpleAnalyzer.checkAnalysis(plan)
    }.getMessage
    assert(m2.contains("nondeterministic expressions are only allowed in"))

    // Non-determinstic joining keys and empty conditions: push down
    checkAnalysis(
      left.join(right, Inner, Some(EqualTo(c, Rand(a)))),
      left.select(c).join(right.select(a, rnd), Inner, Some(EqualTo(c, rndref)))
       .select(c, a)
    )
    // Non-determinstic joining keys and deterministic conditions: push down
    checkAnalysis(
      left.join(right, Inner, Some(And(EqualTo(c, Rand(a)), EqualTo(a, Literal(10))))),
      left.select(c).join(
        right.select(a, rnd),
        Inner,
        Some(And(EqualTo(c, rndref), EqualTo(a, Literal(10)))))
      .select(c, a)
    )
    // Empty joining keys and non-deterministic conditions: push down
    checkAnalysis(
      left.join(right, Inner, Some(GreaterThan(c, Rand(a)))),
      left.select(c).join(right.select(a, rnd), Inner, Some(GreaterThan(c, rndref)))
       .select(c, a)
    )
  }
}
