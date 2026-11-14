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

import org.apache.spark.sql.catalyst.expressions.{Alias, DynamicPruningSubquery, Expression, GetMapValue, GetStructField, NamedExpression, OuterReference, SubExprUtils, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.SubExprUtils.stripOuterReference
import org.apache.spark.sql.catalyst.expressions.variant.VariantGet
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.trees.TreePatternBits
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.internal.SQLConf

/**
 * This rule replaces outer references of nested data (map, struct, variant) with the extracted
 * member for subqueries that have correlated conditions on that member.
 *
 * Without this rule, when a subquery is correlated on a condition like
 * `outer_map[1] = inner_map[1]`, DecorrelateInnerQuery generates a join on the map itself,
 * which is unsupported for some types like map and inefficient for other types like structs.
 *
 * The analyzer resolves a nested access of an outer column inside-out, so `c.home.zip` becomes
 * `GetStructField(OuterReference(home), zip)` rather than `OuterReference(home.zip)` (an
 * OuterReference can only wrap a NamedExpression). Everything downstream matches on a bare
 * OuterReference, so the whole `home` struct is treated as the correlated key: a correlated
 * equality on the member is not recognized as an equality and goes through a domain join on the
 * whole struct, and a correlated join condition on the member fails decorrelation with an internal
 * error. This rule instead projects the extracted member (`home.zip`) onto the outer plan and
 * rewrites the correlation to a bare OuterReference on that projection, so the correlation keys on
 * the member and DecorrelateInnerQuery sees a plain outer reference:
 *
 * Filter (scalar-subquery#50 [home#11])
 * :  +- Aggregate [max(amount#14)]
 * :     +- Filter (ship#13.zip = outer(home#11).zip)
 * :        +- Relation o[ship#13, amount#14]
 * +- Relation c[id#10, home#11]
 *
 * becomes
 *
 * Project [id#10, home#11]
 * +- Filter (scalar-subquery#50 [home.zip#55])
 *    :  +- Aggregate [max(amount#14)]
 *    :     +- Filter (ship#13.zip = outer(home.zip#55))
 *    :        +- Relation o[ship#13, amount#14]
 *    +- Project [id#10, home#11, home#11.zip AS home.zip#55]
 *       +- Relation c[id#10, home#11]
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
 *
 * The rule runs under [[SQLConf.PULL_OUT_NESTED_DATA_OUTER_REF_EXPRESSIONS_ENABLED]] and is
 * non-excludable: a correlation it does not normalize is mis-keyed on the whole outer column.
 */
object PullOutNestedDataOuterRefExpressions extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (SQLConf.get.getConf(SQLConf.PULL_OUT_NESTED_DATA_OUTER_REF_EXPRESSIONS_ENABLED)) {
      rewrite(plan)
    } else {
      plan
    }
  }

  /**
   * A (possibly nested) extract-value / variant-get chain that bottoms out at an OuterReference and
   * references no inner attribute, i.e. a nested-data member of an outer column. `e.references` is
   * empty iff there is no inner reference, since outer references are not included in it.
   */
  private def isPullableOuterExtract(e: Expression): Boolean = {
    def rootsAtOuterRef(x: Expression): Boolean = x match {
      case _: OuterReference => true
      case g: GetStructField => rootsAtOuterRef(g.child)
      case v: VariantGet => rootsAtOuterRef(v.child)
      case m: GetMapValue => rootsAtOuterRef(m.child)
      case _ => false
    }
    e match {
      case _: GetMapValue | _: GetStructField | _: VariantGet =>
        e.references.isEmpty && rootsAtOuterRef(e)
      case _ => false
    }
  }

  private def hasOuterNestedDataAccess(tree: TreePatternBits): Boolean = {
    tree.containsAllPatterns(PLAN_EXPRESSION, OUTER_REFERENCE) &&
      tree.containsAnyPattern(EXTRACT_VALUE, VARIANT_GET)
  }

  def rewrite(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    hasOuterNestedDataAccess) {
    case plan: UnaryNode =>
      // Canonicalized original extract expression -> new outer projection alias.
      val newExprMap = mutable.HashMap.empty[Expression, NamedExpression]
      // Projections land on plan.child, so only members of a column available there are pullable.
      val outerPlanOutput = plan.child.outputSet
      val newPlan = plan.transformExpressionsWithPruning(hasOuterNestedDataAccess) {
        case e: DynamicPruningSubquery => e // Skip this case
        case subqueryExpression: SubqueryExpression =>
          val innerPlan = subqueryExpression.plan
          val newInnerPlan = innerPlan.transformAllExpressionsWithPruning(
            _.containsPattern(OUTER_REFERENCE)) {
            case e if isPullableOuterExtract(e) &&
                stripOuterReference(e).references.subsetOf(outerPlanOutput) =>
              val outerProj = newExprMap.getOrElseUpdate(e.canonicalized, {
                val projExpr = stripOuterReference(e)
                Alias(projExpr, toPrettySQL(projExpr))()
              })
              // Replace with the reference to the new outer projection
              OuterReference(outerProj.toAttribute)
          }
          // Note: Need to update outer attrs, otherwise the references of the subquery expr will
          // not include the new outer attr, which can lead to issues like ColumnPruning pruning
          // them from the project.
          subqueryExpression
            .withNewPlan(newInnerPlan)
            .withNewOuterAttrs(SubExprUtils.getOuterReferences(newInnerPlan).distinct)
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
