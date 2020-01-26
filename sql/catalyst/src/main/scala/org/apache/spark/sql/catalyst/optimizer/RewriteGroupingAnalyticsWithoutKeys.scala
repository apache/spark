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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, Literal, NamedExpression, VirtualColumn}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * In grouping analytics (e.g., `GROUPING SETS`) with an empty grouping set, we need to follow
 * the non-key aggregate semantics; it generates a single row having an initial aggregated value
 * (e.g., 0 for `COUNT`) from an empty input.
 * For example, a query below must return a row (NULL, NULL, 0) from
 * an empty input (`empty_input`):
 *
 * {{{
 *   SELECT k1, k2, COUNT(v)
 *     FROM empty_input t(k1, k2, v)
 *     GROUP BY GROUPING SETS ((), (k1, k2))
 * }}}
 *
 * To comply with the semantics, this rule rewrites the following (pseudo) resolved logical plan
 * as an union query of aggregates with/without keys as follows:
 *
 * {{{
 *   Aggregate(
 *     groupingExprs = ['k1, 'k2, 'spark_grouping_id],
 *     aggregateExprs = ['k1, 'k2, COUNT('v)],
 *     output = ['k1, 'k2, 'cnt],
 *     child = Expand(
 *       projections = [('k1, 'k2, 'v, 0), (null, null, 'v, 3)],
 *       output = ['k1, 'k2, 'v, 'spark_grouping_id]),
 *       child = LocalTableScan [...]))
 * }}}
 *
 * It is transformed into an union plan below:
 *
 * {{{
 *   Union([
 *     Aggregate(
 *       groupingExprs = ['k1, 'k2, 'spark_grouping_id],
 *       aggregateExprs = ['k1, 'k2, COUNT('v)],
 *       output = ['k1, 'k2, 'cnt],
 *       child = Expand(
 *         projections = [('k1, 'k2, 'v, 0)],
 *         output = ['k1, 'k2, 'v, 'spark_grouping_id]),
 *         child = LocalTableScan [...])),
 *     Aggregate(
 *       groupingExprs = [],
 *       aggregateExprs = [null, null, COUNT('v)],
 *       output = ['k1, 'k2, 'cnt],
 *       child = Project(
 *         projectList = ['v],
 *         child = LocalTableScan [...]))]
 * }}}
 */
object RewriteGroupingAnalyticsWithoutKeys extends Rule[LogicalPlan] {

  private val virtualGroupingColumnNames =
    Seq(VirtualColumn.groupingIdName, VirtualColumn.groupingPosName)

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case agg @ Aggregate(groupingExprs, aggExprs, expand @ Expand(proj, childOutput, child)) =>
      val groupByAttrMap = AttributeSet.fromAttributeSets(groupingExprs.map {
        case ar: AttributeReference if virtualGroupingColumnNames.contains(ar.name) =>
          AttributeSet.empty
        case e =>
          e.references
      })
      val (groupsWithoutKeys, groupsWithKeys) = {
        val groupByIndices = childOutput.zipWithIndex.filter { case (a, _) =>
          groupByAttrMap.contains(a)
        }
        proj.partition { groupingSet =>
          groupByIndices.map(_._2).map(groupingSet).forall {
            // Checks if all entries in `groupingSet` have NULL
            case Literal(null, _) => true
            case _ => false
          }
        }
      }
      if (groupsWithoutKeys.nonEmpty) {
        val aggsWithoutKeys = {
          val newAggExprs = aggExprs.map {
            case ne if ne.collectFirst { case ae: AggregateExpression => ae }.nonEmpty => ne
            // There are the two parts that we need to rewrite in non-aggregate exprs;
            //  1. Replace group-by attributes with literals with NULL
            //  2. Fill in a grouping ID attribute for an empty grouping set
            case ne => ne.transform {
              case a: Attribute if groupByAttrMap.contains(a) =>
                Alias(Literal.create(null, ne.dataType), a.name)(exprId = a.exprId)
              case a: Attribute if a.name == VirtualColumn.groupingIdName =>
                val gidIdx = childOutput.indexWhere(_.name == VirtualColumn.groupingIdName)
                Alias(groupsWithoutKeys.head(gidIdx), a.name)(exprId = a.exprId)
            }
          }

          // Prunes unnecessary references in child output
          val newChildOutput = {
            val aggRefMap = AttributeSet.fromAttributeSets(newAggExprs.flatMap { _.collect {
              case ae: AggregateExpression => ae.references
            }})
            childOutput.filter(aggRefMap.contains)
          }

          val agg = Aggregate(
            groupingExpressions = Nil,
            aggregateExpressions = newAggExprs.map(_.asInstanceOf[NamedExpression]),
            child = Project(newChildOutput, child))

          groupsWithoutKeys.indices.map(_ => agg)
        }
        if (groupsWithKeys.nonEmpty) {
          val aggWithKeys = agg.copy(child = expand.copy(projections = groupsWithKeys))
          Union(aggWithKeys +: aggsWithoutKeys)
        } else if (groupsWithoutKeys.size > 1) {
          // The case of multiple empty grouping sets
          Union(aggsWithoutKeys)
        } else {
          aggsWithoutKeys.head
        }
      } else {
        agg
      }
  }
}
