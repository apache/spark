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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.objects.LambdaVariable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{CASE_WHEN, IF}
import org.apache.spark.sql.internal.SQLConf

/**
 * Rewrites a subexpression that occurs more than once inside a single branch of an `if` or a
 * `case when` into a [[With]], so that the rows reaching that branch evaluate it once.
 *
 * This is the one place subexpression elimination structurally cannot reach. It evaluates its
 * candidates before the projection, so `EquivalentExpressions` only collects expressions that are
 * always evaluated (`ConditionalExpression.alwaysEvaluatedInputs`) plus those shared by every
 * branch of a group (`branchGroups`), which is an intersection: a subexpression repeated inside one
 * branch body and nowhere else is in no group and is never eliminated. A `With` covers it because
 * it memoizes per evaluation rather than ahead of it -- nothing is computed for a row that does not
 * reach the branch.
 *
 * The rule runs after the simplification rules, so what it memoizes is what survives them, and it
 * leaves the `With` in the branch: `RewriteWithExpression` has already run by then, and a
 * definition inside a conditional branch is one that rule keeps anyway.
 */
object MemoizeCommonExpressionsInBranches extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.MEMOIZE_COMMON_EXPRESSIONS_IN_BRANCHES)) {
      plan
    } else {
      plan.transformWithPruning(_.containsAnyPattern(IF, CASE_WHEN)) {
        case p => p.transformExpressionsUpWithPruning(_.containsAnyPattern(IF, CASE_WHEN)) {
          case i: If => i.copy(trueValue = memoize(i.trueValue), falseValue = memoize(i.falseValue))
          case c: CaseWhen =>
            // The first condition is always evaluated, so it is subexpression elimination's to
            // take; every other condition and every value is reached only for some rows.
            val branches = c.branches.zipWithIndex.map { case ((cond, value), i) =>
              (if (i == 0) cond else memoize(cond), memoize(value))
            }
            c.copy(branches = branches, elseValue = c.elseValue.map(memoize))
        }
      }
    }
  }

  private def memoize(body: Expression): Expression = {
    if (skipBody(body)) {
      body
    } else {
      val equivalence = new EquivalentExpressions
      equivalence.addExprTree(body)
      // `getCommonSubexpressions` is ordered by height, so the last one that qualifies is the
      // tallest: memoizing it subsumes every repeated subtree inside it.
      equivalence.getCommonSubexpressions.reverse.find(worthMemoizing) match {
        case Some(common) =>
          With(common) { case Seq(ref) =>
            body.transformDown { case e if e.semanticEquals(common) => ref }
          }
        case None => body
      }
    }
  }

  private def skipBody(body: Expression): Boolean = body.exists {
    // A body that already holds a `With` is left alone: `RewriteWithExpression` defers a nested
    // `With` to its next pass and that rule is behind us, so a nested definition would never be
    // looked at again.
    case _: With => true
    // An aggregate, window or generator expression anywhere in the body rules the whole body out,
    // not just candidates holding one. The planner takes those expressions out of the tree they
    // stand in -- `PhysicalAggregation` gives an aggregate its own operator -- and a reference left
    // behind would be evaluated with its `With`, and so its definition, no longer above it.
    case _: AggregateExpression | _: WindowExpression | _: Generator => true
    case _ => false
  }

  private def worthMemoizing(candidate: Expression): Boolean = {
    // Reading the value back has to cost less than computing it again.
    !CollapseProject.isCheap(candidate) &&
      !candidate.exists {
        // A reference belongs to the `With` that binds it and a lambda variable to its loop;
        // neither can be evaluated where the definition would sit. An aggregate, window or
        // generator expression has to stay where the planner looks for it, and a subquery
        // expression carries a plan that later rules still rewrite.
        case _: CommonExpressionRef | _: CommonExpressionDef => true
        case _: NamedLambdaVariable | _: LambdaVariable => true
        case _: AggregateExpression | _: WindowExpression | _: Generator => true
        case _: PlanExpression[_] => true
        case _ => false
      }
    // `stateful` is deliberately not a reason to refuse. A `ScalaUDF` is stateful because its
    // encoder reuses an `UnsafeRow`, and it is exactly what this rule exists for; the definition is
    // evaluated once and read back within the same row, which is what `RewriteWithExpression`
    // already does with a `With` a `nullif(udf(x), 0)` leaves in a branch. What would be unsafe is
    // an expression whose value changes per evaluation, and those are nondeterministic --
    // `EquivalentExpressions` never records one.
  }
}
