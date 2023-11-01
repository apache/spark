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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, CommonExpressionRef, Expression, With}
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
        val commonExprs = mutable.ArrayBuffer.empty[Alias]
        // `With` can be nested, we should only rewrite the leaf `With` expression, as the outer
        // `With` needs to add its own Project, in the next iteration when it becomes leaf.
        // This is done via "transform down" and check if the common expression definitions does not
        // contain nested `With`.
        var newPlan: LogicalPlan = p.transformExpressionsDown {
          case With(child, defs) if defs.forall(!_.containsPattern(WITH_EXPRESSION)) =>
            val idToCheapExpr = mutable.HashMap.empty[Long, Expression]
            val idToNonCheapExpr = mutable.HashMap.empty[Long, Alias]
            defs.foreach { commonExprDef =>
              if (CollapseProject.isCheap(commonExprDef.child)) {
                idToCheapExpr(commonExprDef.id) = commonExprDef.child
              } else {
                // TODO: we should calculate the ref count and also inline the common expression
                //       if it's ref count is 1.
                val alias = Alias(commonExprDef.child, s"_common_expr_${commonExprDef.id}")()
                commonExprs += alias
                idToNonCheapExpr(commonExprDef.id) = alias
              }
            }

            child.transformWithPruning(_.containsPattern(COMMON_EXPR_REF)) {
              case ref: CommonExpressionRef =>
                idToCheapExpr.getOrElse(ref.id, idToNonCheapExpr(ref.id).toAttribute)
            }
        }

        var exprsToAdd = commonExprs.toSeq
        val newChildren = p.children.map { child =>
          val (newExprs, others) = exprsToAdd.partition(_.references.subsetOf(child.outputSet))
          exprsToAdd = others
          if (newExprs.nonEmpty) {
            Project(child.output ++ newExprs, child)
          } else {
            child
          }
        }

        if (exprsToAdd.nonEmpty) {
          // If we cannot rewrite the common expressions, force to inline them so that the query
          // can still run. This can happen if the join condition contains `With` and the common
          // expression references columns from both join sides.
          // TODO: things can go wrong if the common expression is nondeterministic. We don't fix
          //       it for now to match the old buggy behavior when certain `RuntimeReplaceable`
          //       did not use the `With` expression.
          val attrToExpr = AttributeMap(exprsToAdd.map { alias =>
            alias.toAttribute -> alias.child
          })
          newPlan = newPlan.transformExpressionsUp {
            case a: Attribute => attrToExpr.getOrElse(a, a)
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
