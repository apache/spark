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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf.COMBINE_DISJUNCTIVE_IN_PREDICATES_ENABLED

class CombineDisjunctiveInPredicatesSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("AnalysisNodes", Once,
        EliminateSubqueryAliases) ::
      Batch("Optimize", FixedPoint(10),
        NullPropagation,
        ConstantFolding,
        BooleanSimplification,
        CombineDisjunctiveInPredicates,
        OptimizeIn) :: Nil
  }

  private val testRelation = LocalRelation($"a".int, $"b".int, $"c".int)
  private val a = $"a"
  private val b = $"b"
  private val c = $"c"

  private def assertRewrite(input: Expression, expected: Expression): Unit = {
    val optimized = Optimize.execute(testRelation.where(input).analyze)
    val correct = testRelation.where(expected).analyze
    comparePlans(optimized, correct)
  }

  private def assertUnchanged(input: Expression): Unit = {
    val query = testRelation.where(input).analyze
    comparePlans(Optimize.execute(query), query)
  }

  test("merge an OR-chain of equalities into In") {
    assertRewrite(a === 1 || a === 2 || a === 3,
      In(a, Seq(Literal(1), Literal(2), Literal(3))))
  }

  test("merge into InSet above the conversion threshold") {
    assertRewrite((1 to 11).map(i => a === i).reduce(_ || _), InSet(a, (1 to 11).toSet))
  }

  test("commuted equality is merged") {
    assertRewrite(Literal(1) === a || a === 2, In(a, Seq(Literal(1), Literal(2))))
  }

  test("merge OR'd IN lists on the same column") {
    assertRewrite(
      In(a, Seq(Literal(1), Literal(2))) || In(a, Seq(Literal(3), Literal(4))),
      In(a, Seq(Literal(1), Literal(2), Literal(3), Literal(4))))
  }

  test("merge an equality with an IN list") {
    assertRewrite(
      a === 1 || In(a, Seq(Literal(2), Literal(3))),
      In(a, Seq(Literal(1), Literal(2), Literal(3))))
  }

  test("merge three IN lists") {
    assertRewrite(
      In(a, Seq(Literal(1), Literal(2))) ||
        In(a, Seq(Literal(3), Literal(4))) ||
        In(a, Seq(Literal(5), Literal(6))),
      In(a, Seq(Literal(1), Literal(2), Literal(3), Literal(4), Literal(5), Literal(6))))
  }

  test("partial OR keeps non-membership disjuncts") {
    assertRewrite(
      In(a, Seq(Literal(1), Literal(2))) || b > 5 || a === 3,
      In(a, Seq(Literal(1), Literal(2), Literal(3))) || b > 5)
  }

  test("distinct subjects are merged independently") {
    assertRewrite(
      a === 1 || In(b, Seq(Literal(1), Literal(2))) || a === 2 || b === 3,
      In(a, Seq(Literal(1), Literal(2))) || In(b, Seq(Literal(1), Literal(2), Literal(3))))
  }

  test("non-literal members are merged and stay as In") {
    assertRewrite(a === b || a === c, In(a, Seq(b, c)))
  }

  test("a single membership per subject is untouched") {
    assertUnchanged(a === 1 || b === 2)
  }

  test("a lone IN list is untouched") {
    assertUnchanged(In(a, Seq(Literal(1), Literal(2))))
  }

  test("a non-deterministic subject is not merged") {
    assertUnchanged(Rand(0) === 1.0 || Rand(0) === 2.0)
  }

  test("a foldable subject is not merged into a constant IN") {
    assertUnchanged(a === 1 || b === 1)
  }

  test("no rewrite when the rule is disabled") {
    withSQLConf(COMBINE_DISJUNCTIVE_IN_PREDICATES_ENABLED.key -> "false") {
      assertUnchanged(a === 1 || a === 2 || a === 3)
    }
  }
}
