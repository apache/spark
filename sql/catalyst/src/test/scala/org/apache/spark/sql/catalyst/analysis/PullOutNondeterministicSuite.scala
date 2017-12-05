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
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.internal.SQLConf

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
    val conf = new SQLConf().copy(SQLConf.NON_DETERMINISTIC_JOIN_ENABLED -> true)
    val catalog = new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry, conf)
    val analyzer = new Analyzer(catalog, conf)

    val rnd = Rand(a).as('_nondeterministic)
    val rndref = rnd.toAttribute

    // Don't pull non-deterministic conditions:
    // With deterministic joining keys
    val m1 = intercept[AnalysisException] {
      val plan = analyzer.execute(
        left.join(right,
          Inner, Some(And(EqualTo(Ceil(c), a), EqualTo(Rand(a), Literal(10.1))))))
      analyzer.checkAnalysis(plan)
    }.getMessage
    assert(m1.contains("For Join, nondeterministic expressions are only allowed in equi join keys"))

    // With non-determinstic joining keys
    val m2 = intercept[AnalysisException] {
      val plan = analyzer.execute(
        left.join(right,
          Inner, Some(And(EqualTo(c, Rand(a)), EqualTo(Rand(a), Literal(10.1))))))
      analyzer.checkAnalysis(plan)
    }.getMessage
    assert(m2.contains("For Join, nondeterministic expressions are only allowed in equi join keys"))

    // With empty joining keys
    val m3 = intercept[AnalysisException] {
      val plan = analyzer.execute(left.join(right, Inner, Some(GreaterThan(c, Rand(a)))))
      analyzer.checkAnalysis(plan)
    }.getMessage
    assert(m3.contains("For Join, nondeterministic expressions are only allowed in equi join keys"))

    // Pull non-determinstic joining keys:
    // With empty conditions
    comparePlans(
      analyzer.execute(
        left.join(right, Inner, Some(EqualTo(c, Rand(a))))),
      left.select(c).join(right.select(a, rnd), Inner, Some(EqualTo(c, rndref)))
       .select(c, a)
    )
    // With deterministic conditions
    comparePlans(
      analyzer.execute(
        left.join(right, Inner, Some(And(EqualTo(c, Rand(a)), EqualTo(a, Literal(10)))))),
      left.select(c).join(
        right.select(a, rnd),
        Inner,
        Some(And(EqualTo(c, rndref), EqualTo(a, Literal(10)))))
      .select(c, a)
    )

    // When the config is disabled (default), non-deterministic joining keys are disallowed too.
    intercept[AnalysisException] {
      val plan = SimpleAnalyzer.execute(
        left.join(right, Inner, Some(EqualTo(c, Rand(a)))))
      SimpleAnalyzer.checkAnalysis(plan)
    }
  }
}
