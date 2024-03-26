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

import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, PlanHelper, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{COMMON_EXPR_REF, WITH_EXPRESSION}

/**
 * Rewrites the `With` expressions by adding a `Project` to pre-evaluate the common expressions, or
 * just inline them if they are cheap.
 *
 * Note: For now we only use `With` in a few `RuntimeReplaceable` expressions. If we expand its
 *       usage, we should support aggregate/window functions as well.
 */
object RewriteWithExpression extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformDownWithSubqueriesAndPruning(_.containsPattern(WITH_EXPRESSION)) {
      case p if p.expressions.exists(_.containsPattern(WITH_EXPRESSION)) =>
        val inputPlans = p.children.toArray
        var newPlan: LogicalPlan = p.mapExpressions { expr =>
          rewriteWithExprAndInputPlans(expr, inputPlans)
        }
        newPlan = newPlan.withNewChildren(inputPlans.toIndexedSeq)
        // Since we add extra Projects with extra columns to pre-evaluate the common expressions,
        // the current operator may have extra columns if it inherits the output columns from its
        // child, and we need to project away the extra columns to keep the plan schema unchanged.
        assert(p.output.length <= newPlan.output.length)
        if (p.output.length < newPlan.output.length) {
          assert(p.outputSet.subsetOf(newPlan.outputSet))
          Project(p.output, newPlan)
        } else {
          newPlan
        }
    }
  }

  private def rewriteWithExprAndInputPlans(
      e: Expression,
      inputPlans: Array[LogicalPlan]): Expression = {
    if (!e.containsPattern(WITH_EXPRESSION)) return e
    e match {
      case w: With =>
        // Rewrite nested With expressions first
        val child = rewriteWithExprAndInputPlans(w.child, inputPlans)
        val defs = w.defs.map(rewriteWithExprAndInputPlans(_, inputPlans))
        val refToExpr = mutable.HashMap.empty[Long, Expression]
        val childProjections = Array.fill(inputPlans.length)(mutable.ArrayBuffer.empty[Alias])

        defs.zipWithIndex.foreach { case (CommonExpressionDef(child, id), index) =>
          if (child.containsPattern(COMMON_EXPR_REF)) {
            throw SparkException.internalError(
              "Common expression definition cannot reference other Common expression definitions")
          }

          if (CollapseProject.isCheap(child)) {
            refToExpr(id) = child
          } else {
            val childProjectionIndex = inputPlans.indexWhere(
              c => child.references.subsetOf(c.outputSet)
            )
            if (childProjectionIndex == -1) {
              // When we cannot rewrite the common expressions, force to inline them so that the
              // query can still run. This can happen if the join condition contains `With` and
              // the common expression references columns from both join sides.
              // TODO: things can go wrong if the common expression is nondeterministic. We
              //       don't fix it for now to match the old buggy behavior when certain
              //       `RuntimeReplaceable` did not use the `With` expression.
              // TODO: we should calculate the ref count and also inline the common expression
              //       if it's ref count is 1.
              refToExpr(id) = child
            } else {
              val alias = Alias(child, s"_common_expr_$index")()
              val fakeProj = Project(Seq(alias), inputPlans(childProjectionIndex))
              if (PlanHelper.specialExpressionsInUnsupportedOperator(fakeProj).nonEmpty) {
                // We have to inline the common expression if it cannot be put in a Project.
                refToExpr(id) = child
              } else {
                childProjections(childProjectionIndex) += alias
                refToExpr(id) = alias.toAttribute
              }
            }
          }
        }

        for (i <- inputPlans.indices) {
          val projectList = childProjections(i)
          if (projectList.nonEmpty) {
            inputPlans(i) = Project(inputPlans(i).output ++ projectList, inputPlans(i))
          }
        }

        child.transformWithPruning(_.containsPattern(COMMON_EXPR_REF)) {
          case ref: CommonExpressionRef =>
            if (!refToExpr.contains(ref.id)) {
              throw SparkException.internalError("Undefined common expression id " + ref.id)
            }
            refToExpr(ref.id)
        }

      case c: ConditionalExpression =>
        val newAlwaysEvaluatedInputs = c.alwaysEvaluatedInputs.map(
          rewriteWithExprAndInputPlans(_, inputPlans))
        val newExpr = c.withNewAlwaysEvaluatedInputs(newAlwaysEvaluatedInputs)
        // Use transformUp to handle nested With.
        newExpr.transformUpWithPruning(_.containsPattern(WITH_EXPRESSION)) {
          case With(child, defs) =>
            // For With in the conditional branches, they may not be evaluated at all and we can't
            // pull the common expressions into a project which will always be evaluated. Inline it.
            val refToExpr = defs.map(d => d.id -> d.child).toMap
            child.transformWithPruning(_.containsPattern(COMMON_EXPR_REF)) {
              case ref: CommonExpressionRef => refToExpr(ref.id)
            }
        }

      case other => other.mapChildren(rewriteWithExprAndInputPlans(_, inputPlans))
    }
  }
}
