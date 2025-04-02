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

import org.apache.spark.sql.catalyst.expressions.{Alias, DynamicPruningSubquery, Expression, GetMapValue, NamedExpression, OuterReference, SubExprUtils, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.SubExprUtils.stripOuterReference
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.internal.SQLConf

/**
 * This rule replaces outer references of type Map with the Map's members for subqueries that have
 * correlated conditions on Map's members.
 *
 * Without this rule, when a subquery is correlated on a condition like
 * `outer_map[1] = inner_map[1]`, DecorrelateInnerQuery generates a join on the map itself,
 * which is unsupported for some types like map and inefficient for other types like structs.
 *
 * This rule rewrites the query to project `outer_map[1]` as a new attribute in the outer plan,
 * and use that attribute in the correlation condition instead. This allows
 * DecorrelateInnerQuery to write the join on the extracted value instead of the entire map or
 * other object.
 *
 * Example: Here, we have outer table x and inner table y in a scalar subquery, correlated
 * on xm[1] = ym[1] where xm and ym are map columns.
 *
 * The plan before the rewrite is:
 *
 * Filter (scalar-subquery#50 [xm#11] > cast(2 as bigint))
 *  +- Aggregate [sum(y2#14) AS sum(y2)#52L]
 *     +- Filter (outer(xm#11)[1] = ym#13[1])
 *        +- Relation spark_catalog.default.y[ym#13,y2#14] parquet
 *  +- Relation spark_catalog.default.x[xm#11,x2#12] parquet
 *
 * The plan after the rewrite adds a projection for xm[1] to the outer plan, and replaces the outer
 * reference inside the subquery with that:
 *
 * Project [xm#11, x2#12]
 * +- Filter (scalar-subquery#50 [xm[1]#55] > cast(2 as bigint))
 *    :  +- Aggregate [sum(y2#14) AS sum(y2)#52L]
 *    :     +- Filter (outer(xm[1]#55) = ym#13[1])
 *    :        +- Relation spark_catalog.default.y[ym#13,y2#14] parquet
 *    +- Project [xm#11, x2#12, xm#11[1] AS xm[1]#55]
 *       +- Relation spark_catalog.default.x[xm#11,x2#12] parquet
 *
 * This is implemented as a separate rule from DecorrelateInnerQuery because it's much simpler
 * and safer, and also benefits us when the same nested data expression is used multiple times.
 * In particular:
 * - In DecorrelateInnerQuery, outer references is an AttributeSet, so it can't store general
 *   expressions. In principle we could change this but it would add substantial complexity.
 * - DecorrelateInnerQuery only manipulates the inner query, not the outer plan, whereas
 *   this rewrite needs to add projections to the outer plan
 */
object PullOutNestedDataOuterRefExpressions extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (SQLConf.get.getConf(SQLConf.PULL_OUT_NESTED_DATA_OUTER_REF_EXPRESSIONS_ENABLED)) {
      rewrite(plan)
    } else {
      plan
    }
  }

  def rewrite(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    _.containsAllPatterns(PLAN_EXPRESSION, OUTER_REFERENCE, EXTRACT_VALUE)) {
    case plan: UnaryNode =>
      // Map of canonicalized original expression to new outer projection
      val newExprMap = mutable.HashMap.empty[Expression, NamedExpression]
      val newPlan = plan.transformExpressionsWithPruning(
        _.containsAllPatterns(PLAN_EXPRESSION, OUTER_REFERENCE, EXTRACT_VALUE)) {
        case e: DynamicPruningSubquery => e // Skip this case
        case subqueryExpression: SubqueryExpression =>
          val innerPlan = subqueryExpression.plan
          val newInnerPlan = innerPlan.transformAllExpressionsWithPruning(
            _.containsAllPatterns(OUTER_REFERENCE, EXTRACT_VALUE)) {
            // We plan to extend to other ExtractValue and similar exprs in future PRs
            case e @ GetMapValue(outerRef: OuterReference, key) if e.references.isEmpty =>
              // e.references.isEmpty checks whether there are any inner references (since it
              // doesn't include outer references). The expression must reference the outer plan
              // only, not the inner plan.
              val key = e.canonicalized
              if (!newExprMap.contains(key)) {
                val projExpr = stripOuterReference(e)
                val name = toPrettySQL(projExpr)
                // Create a new project expression for the outer plan
                val outerProj = Alias(projExpr, name)()
                newExprMap(key) = outerProj
              }
              val outerProj = newExprMap(key)
              // Replace with the reference to the new outer projection
              OuterReference(outerProj.toAttribute)
          }
          // Note: Need to update outer attrs, otherwise the references of the subquery expr will
          // not include the new outer attr, which can lead to issues like ColumnPruning pruning
          // them from the project.
          subqueryExpression
            .withNewPlan(newInnerPlan)
            .withNewOuterAttrs(SubExprUtils.getOuterReferences(newInnerPlan))
      }
      if (newExprMap.isEmpty) {
        // Nothing to change
        plan
      } else {
        // Add Project node with the new attributes to the outer plan, below the subquery.
        // Add a Project above the subquery to remove the new attributes after the subquery is
        // done with them.
        //
        // Currently only subqueries in UnaryNode are supported, in the future we can support nodes
        // with multiple children e.g. joins as long as each expression only references only one
        // child.
        val newProjectExprs: Iterable[NamedExpression] = newExprMap.values
        Project(plan.output,
          newPlan.withNewChildren(Seq(
            Project(newPlan.child.output ++ newProjectExprs, newPlan.child)
          )))
      }
  }
}
