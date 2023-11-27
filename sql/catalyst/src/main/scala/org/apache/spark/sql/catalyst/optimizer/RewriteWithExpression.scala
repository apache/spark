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

import org.apache.spark.sql.catalyst.expressions.{Alias, CommonExpressionDef, CommonExpressionRef, Expression, With}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
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
    plan.transformWithPruning(_.containsPattern(WITH_EXPRESSION)) {
      case p if p.expressions.exists(_.containsPattern(WITH_EXPRESSION)) =>
        var newChildren = p.children
        var newPlan: LogicalPlan = p.transformExpressionsUp {
          case With(child, defs) =>
            val refToExpr = mutable.HashMap.empty[Long, Expression]
            val childProjections = Array.fill(newChildren.size)(mutable.ArrayBuffer.empty[Alias])

            defs.zipWithIndex.foreach { case (CommonExpressionDef(child, id), index) =>
              if (CollapseProject.isCheap(child)) {
                refToExpr(id) = child
              } else {
                val childProjectionIndex = newChildren.indexWhere(
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
                  childProjections(childProjectionIndex) += alias
                  refToExpr(id) = alias.toAttribute
                }
              }
            }

            newChildren = newChildren.zip(childProjections).map { case (child, projections) =>
              if (projections.nonEmpty) {
                Project(child.output ++ projections, child)
              } else {
                child
              }
            }

            child.transformWithPruning(_.containsPattern(COMMON_EXPR_REF)) {
              case ref: CommonExpressionRef => refToExpr(ref.id)
            }
        }

        newPlan = newPlan.withNewChildren(newChildren)
        if (p.output == newPlan.output) {
          newPlan
        } else {
          Project(p.output, newPlan)
        }
    }
  }
}
