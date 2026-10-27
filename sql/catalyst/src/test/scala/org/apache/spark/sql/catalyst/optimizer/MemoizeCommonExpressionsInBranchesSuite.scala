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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf

class MemoizeCommonExpressionsInBranchesSuite extends PlanTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Memoize common expressions in branches", Once,
        MemoizeCommonExpressionsInBranches) :: Nil
  }

  private val relation = LocalRelation($"a".int, $"b".int)
  // Resolved attributes, so that the expressions built here are the ones the analyzed plan holds.
  private val Seq(a, b) = relation.output
  private val common = Multiply(a, b)

  private def optimize(plan: LogicalPlan, enabled: Boolean = true): LogicalPlan = {
    withSQLConf(SQLConf.MEMOIZE_COMMON_EXPRESSIONS_IN_BRANCHES.key -> enabled.toString) {
      Optimize.execute(plan)
    }
  }

  private def withExprs(plan: LogicalPlan): Seq[With] =
    plan.expressions.flatMap(_.collect { case w: With => w })

  test("a subexpression repeated in one branch body is memoized") {
    val plan = relation
      .select(CaseWhen(Seq((GreaterThan(a, Literal(0)), Add(common, common))), Literal(0)).as("r"))
      .analyze
    val optimized = optimize(plan)
    val memoized = withExprs(optimized)
    assert(memoized.length == 1, s"expected one With: $optimized")
    val definitions = memoized.head.defs.map(_.child)
    assert(definitions == Seq(common), s"memoized the wrong expression: $definitions")
    val references = memoized.head.child.collect { case r: CommonExpressionRef => r }
    assert(references.length == 2, s"expected two references: ${memoized.head.child}")
    // The `With` has to sit inside the branch. Above the `CaseWhen` it would be evaluated for every
    // row, which is what subexpression elimination already does and what this cannot do.
    val branchValues = optimized.expressions.flatMap(_.collect {
      case c: CaseWhen => c.branches.map(_._2)
    }.flatten)
    assert(branchValues.forall(_.isInstanceOf[With]), s"the With left the branch: $optimized")
  }

  test("the plan is untouched while the config is off") {
    val plan = relation
      .select(CaseWhen(Seq((GreaterThan(a, Literal(0)), Add(common, common))), Literal(0)).as("r"))
      .analyze
    comparePlans(optimize(plan, enabled = false), plan)
  }

  test("a cheap subexpression is left alone") {
    // Reading back a memoized value costs a field read and a flag check, so a foldable expression
    // -- or an attribute, or anything else `CollapseProject.isCheap` accepts -- gains nothing.
    val cheap = Add(Literal(1), Literal(2))
    val plan = relation
      .select(CaseWhen(Seq((GreaterThan(a, Literal(0)), Add(cheap, cheap))), Literal(0)).as("r"))
      .analyze
    assert(withExprs(optimize(plan)).isEmpty, "a cheap expression was memoized")
  }

  test("the first condition of a case when is left to subexpression elimination") {
    // It is evaluated for every row that reaches the conditional, so it is already covered by
    // `ConditionalExpression.alwaysEvaluatedInputs`, where elimination costs no flag check.
    val plan = relation
      .select(CaseWhen(
        Seq((GreaterThan(Add(common, common), Literal(0)), Literal(1))), Literal(0)).as("r"))
      .analyze
    assert(withExprs(optimize(plan)).isEmpty, "the always-evaluated condition was memoized")
  }

  test("a branch body holding an aggregate expression is left alone") {
    // `PhysicalAggregation` gives each aggregate expression its own operator, so a `With` wrapped
    // around one would no longer be above the reference when that reference is evaluated. The
    // candidate here (`a * b`) is fine on its own; what rules the body out is the aggregate above
    // it.
    val aggregate = sum(common)
    val plan = relation
      .groupBy(b)(CaseWhen(
        Seq((GreaterThan(b, Literal(0)), Add(aggregate, aggregate))), Literal(0)).as("r"))
    assert(withExprs(optimize(plan)).isEmpty, "a body holding an aggregate was memoized")
  }

  test("a branch that already holds a With is left alone") {
    // `RewriteWithExpression` defers a nested `With` to its next pass, and that rule has already
    // run, so a definition put inside one would never be looked at again.
    val existing = With(common) { case Seq(ref) => Add(ref, ref) }
    val plan = relation
      .select(CaseWhen(
        Seq((GreaterThan(a, Literal(0)), Add(existing, Multiply(a, a)))), Literal(0)).as("r"))
      .analyze
    val memoized = withExprs(optimize(plan))
    assert(memoized.length == 1, s"expected the existing With and nothing more: $memoized")
    assert(memoized.head.fastEquals(existing), s"the branch was rewritten: ${memoized.head}")
  }
}
