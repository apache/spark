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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AGGREGATE
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils

/**
 * This rule rewrites Aggregate grouping expressions to ensure that non-binary collated strings
 * are converted to their binary-stable collation keys (via [[CollationKey]]).
 *
 * This allows hash-based aggregation (e.g., ObjectHashAggregateExec) to work properly on data
 * with non-binary collations, avoiding full sorting and spilling.
 *
 * Any original grouping expression referenced in the aggregate expressions (output) is preserved
 * by wrapping it in `First(expr, ignoreNulls = false)`, an arbitrary representative of each
 * collation-equal group.
 */
object RewriteCollationAggregate extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.collationHashAggregationEnabled) {
      plan
    } else {
      plan.transformWithPruning(_.containsPattern(AGGREGATE)) {
        case a @ Aggregate(groupingExpressions, aggregateExpressions, child, _)
            if a.resolved &&
              groupingExpressions.exists(e => !UnsafeRowUtils.isBinaryStable(e.dataType)) =>
          val keyMapping = mutable.LinkedHashMap.empty[Expression, Expression]
          val newGroupingExpressions = groupingExpressions.map { ge =>
            val processed = CollationKey.injectCollationKey(ge)
            if (!processed.fastEquals(ge)) {
              keyMapping.put(ge.canonicalized, ge)
              processed
            } else {
              ge
            }
          }

          if (keyMapping.nonEmpty) {
            def replaceGroupingKeyReferences(e: Expression): Expression = {
              e match {
                case _: AggregateExpression => e
                case _ if e.foldable => e
                case _ if keyMapping.contains(e.canonicalized) =>
                  val origExpr = keyMapping(e.canonicalized)
                  First(origExpr, ignoreNulls = false).toAggregateExpression()
                case _ =>
                  e.mapChildren(replaceGroupingKeyReferences)
              }
            }

            val newAggregateExpressions = aggregateExpressions.map {
              case a @ Alias(child, name) =>
                val newChild = replaceGroupingKeyReferences(child)
                if (!newChild.fastEquals(child)) {
                  Alias(newChild, name)(
                    exprId = a.exprId,
                    qualifier = a.qualifier,
                    explicitMetadata = a.explicitMetadata)
                } else {
                  a
                }
              case attr: Attribute =>
                val newChild = replaceGroupingKeyReferences(attr)
                if (!newChild.fastEquals(attr)) {
                  Alias(newChild, attr.name)(exprId = attr.exprId, qualifier = attr.qualifier)
                } else {
                  attr
                }
              case other =>
                val newOther = replaceGroupingKeyReferences(other)
                newOther match {
                  case ne: NamedExpression => ne
                  case expr => Alias(expr, expr.prettyName)()
                }
            }

            a.copy(
              groupingExpressions = newGroupingExpressions,
              aggregateExpressions = newAggregateExpressions)
          } else {
            a
          }
      }
    }
  }
}
