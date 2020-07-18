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

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, If, IsNotNull, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * If an aggregate query with filter clause, this rule will create a project node so as to filter
 * the output of aggregate's child in advance.
 *
 * First example: query with filter clauses (in sql):
 * {{{
 *   val data = Seq(
 *     (1, "a", "ca1", "cb1", 10),
 *     (2, "a", "ca1", "cb2", 5),
 *     (3, "b", "ca1", "cb1", 13))
 *     .toDF("id", "key", "cat1", "cat2", "value")
 *   data.createOrReplaceTempView("data")
 *
 *   SELECT
 *     COUNT(DISTINCT cat1) AS cat1_cnt,
 *     COUNT(DISTINCT cat2) FILTER (WHERE id > 1) AS cat2_cnt,
 *     SUM(value) AS total,
 *     SUM(value) FILTER (WHERE key = "a") AS total2
 *  FROM
 *    data
 *  GROUP BY
 *    key
 * }}}
 *
 * This translates to the following (pseudo) logical plan:
 * {{{
 * Aggregate(
 *    key = ['key]
 *    functions = [COUNT(DISTINCT 'cat1),
 *                 COUNT(DISTINCT 'cat2) with FILTER('id > 1),
 *                 SUM('value),
 *                 SUM('value) with FILTER('key = "a")]
 *    output = ['key, 'cat1_cnt, 'cat2_cnt, 'total, 'total2])
 *   LocalTableScan [...]
 * }}}
 *
 * This rule rewrites this logical plan to the following (pseudo) logical plan:
 * {{{
 *   Aggregate(
 *      key = ['key]
 *      functions = [COUNT(DISTINCT '_gen_attr_1),
 *                   COUNT(DISTINCT '_gen_attr_2) with FILTER('_gen_attr_2 is not null),
 *                   SUM('_gen_attr_3),
 *                   SUM('_gen_attr_4) with FILTER('_gen_attr_4 is not null)]
 *      output = ['key, 'cat1_cnt, 'cat2_cnt, 'total, 'total2])
 *     Project(
 *        projectionList = ['key,
 *                          'cat1,
 *                         if ('id > 1) 'cat2 else null,
 *                          cast('value as bigint),
 *                          if ('key = "a") cast('value as bigint) else null]
 *        output = ['key, '_gen_attr_1, '_gen_attr_2, '_gen_attr_3, '_gen_attr_4])
 *       LocalTableScan [...]
 * }}}
 *
 * The rule does the following things here:
 * 1. Project the output of the child of the aggregate query. There are two aggregation
 *    groups in this query:
 *    i. the group without filter clause;
 *    ii. the group with filter clause;
 *    When there is at least one aggregate function having the filter clause, we add a project
 *    node on the input plan.
 * 2. Avoid projections that may output the same attributes. There are three aggregation groups
 *    in this query:
 *    i. the non-distinct 'cat1 group without filter clause;
 *    ii. the distinct 'cat1 group without filter clause;
 *    iii. the distinct 'cat1 group with filter clause.
 *    The attributes referenced by different aggregate expressions are likely to overlap,
 *    and if no additional processing is performed, data loss will occur. If we directly output
 *    the attributes of the aggregate expression, we may get three attributes 'cat1. To prevent
 *    this, we generate new attributes (e.g. '_gen_attr_1) and replace the original ones.
 *
 * Why we need the first phase? guaranteed to compute filter clauses in the first aggregate
 * locally.
 * Note: after generate new attributes, the aggregate may have at least two distinct groups,
 * so may trigger [[RewriteDistinctAggregates]].
 */
object ProjectFilterInAggregates extends Rule[LogicalPlan] {

  private def collectAggregateExprs(exprs: Seq[Expression]): Seq[AggregateExpression] = {
    exprs.flatMap { _.collect {
      case ae: AggregateExpression => ae
    }}
  }

  private def mayNeedtoProject(a: Aggregate): Boolean = {
    if (collectAggregateExprs(a.aggregateExpressions).exists(_.filter.isDefined)) {
      var flag = true
      a resolveOperatorsUp {
        case p: Project =>
          if (p.output.exists(_.name.startsWith("_gen_attr_"))) {
            flag = false
          }
          p
        case other => other
      }
      flag
    } else {
      false
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case a: Aggregate if mayNeedtoProject(a) => project(a)
  }

  def project(a: Aggregate): Aggregate = {
    val aggExpressions = collectAggregateExprs(a.aggregateExpressions)
    // Constructs pairs between old and new expressions for aggregates.
    val aggExprs = aggExpressions.filter(e => e.children.exists(!_.foldable))
    var currentExprId = 0
    val (projections, aggPairs) = aggExprs.map {
      case ae @ AggregateExpression(af, _, _, filter, _) =>
      // First, In order to reduce costs, it is better to handle the filter clause locally.
      // e.g. COUNT (DISTINCT a) FILTER (WHERE id > 1), evaluate expression
      // If(id > 1) 'a else null first, and use the result as output.
      // Second, If at least two DISTINCT aggregate expression which may references the
      // same attributes. We need to construct the generated attributes so as the output not
      // lost. e.g. SUM (DISTINCT a), COUNT (DISTINCT a) FILTER (WHERE id > 1) will output
      // attribute '_gen_attr-1 and attribute '_gen_attr-2 instead of two 'a.
      // Note: The illusionary mechanism may result in at least two distinct groups, so the
      // RewriteDistinctAggregates may rewrite the logical plan.
      val unfoldableChildren = af.children.filter(!_.foldable)
      // Expand projection
      val projectionMap = unfoldableChildren.map {
        case e =>
          currentExprId += 1
          val ne = if (filter.isDefined) {
            If(filter.get, e, Literal.create(null, e.dataType))
          } else {
            e
          }
          // For convenience and unification, we always alias the column, even if
          // there is no filter.
          e -> Alias(ne, s"_gen_attr_$currentExprId")()
      }
      val projection = projectionMap.map(_._2)
      val exprAttrs = projectionMap.map { kv =>
        (kv._1, kv._2.toAttribute)
      }
      val exprAttrLookup = exprAttrs.toMap
      val newChildren = af.children.map(c => exprAttrLookup.getOrElse(c, c))
      val raf = af.withNewChildren(newChildren).asInstanceOf[AggregateFunction]
      val aggExpr = if (filter.isDefined) {
        //  When the filter execution result is false, the conditional expression will
        // output null, it will affect the results of those aggregate functions not
        // ignore nulls (e.g. count). So we add a new filter with IsNotNull.
        ae.copy(aggregateFunction = raf, filter = Some(IsNotNull(newChildren.last)))
      } else {
        ae.copy(aggregateFunction = raf, filter = None)
      }

      (projection, (ae, aggExpr))
    }.unzip
    // Construct the aggregate input projection.
    val namedGroupingProjection = a.groupingExpressions.flatMap { e =>
      e.collect {
        case ar: AttributeReference => ar
      }
    }
    val rewriteAggProjection = namedGroupingProjection ++ projections.flatten
    // Construct the project operator.
    val project = Project(rewriteAggProjection, a.child)
    val rewriteAggExprLookup = aggPairs.toMap
    val patchedAggExpressions = a.aggregateExpressions.map { e =>
      e.transformDown {
        case ae: AggregateExpression => rewriteAggExprLookup.getOrElse(ae, ae)
      }.asInstanceOf[NamedExpression]
    }
    Aggregate(a.groupingExpressions, patchedAggExpressions, project)
  }
}
