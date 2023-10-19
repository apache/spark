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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeMap, Expression, NamedExpression, Or}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.{Cross, FullOuter, Inner, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LeafNode, LogicalPlan, Project, SerializeFromObject}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, ARRAY_CONTAINS, ARRAYS_OVERLAP, AT_LEAST_N_NON_NULLS, BLOOM_FILTER, DYNAMIC_PRUNING_EXPRESSION, DYNAMIC_PRUNING_SUBQUERY, EXISTS_SUBQUERY, HIGH_ORDER_FUNCTION, IN, IN_SUBQUERY, INSET, INVOKE, JOIN, JSON_TO_STRUCT, LIKE_FAMLIY, PYTHON_UDF, REGEXP_EXTRACT_FAMILY, REGEXP_REPLACE, SCALA_UDF, STRING_PREDICATE}

/**
 * This rule eliminates the [[Join]] if all the join side are [[Aggregate]]s by combine these
 * [[Aggregate]]s. This rule also support the nested [[Join]], as long as all the join sides for
 * every [[Join]] are [[Aggregate]]s.
 *
 * Note: this rule doesn't support following cases:
 * 1. The [[Aggregate]]s to be merged if at least one of them does not have a predicate or
 *    has low predicate selectivity.
 * 2. The upstream node of these [[Aggregate]]s to be merged exists [[Join]].
 */
object CombineJoinedAggregates extends Rule[LogicalPlan] with MergeScalarSubqueriesHelper {

  private def isSupportedJoinType(joinType: JoinType): Boolean =
    Seq(Inner, Cross, LeftOuter, RightOuter, FullOuter).contains(joinType)

  private def isCheapPredicate(e: Expression): Boolean = {
    !e.containsAnyPattern(PYTHON_UDF, SCALA_UDF, INVOKE, JSON_TO_STRUCT, LIKE_FAMLIY,
      REGEXP_EXTRACT_FAMILY, REGEXP_REPLACE, DYNAMIC_PRUNING_SUBQUERY, DYNAMIC_PRUNING_EXPRESSION,
      HIGH_ORDER_FUNCTION, IN_SUBQUERY, IN, INSET, EXISTS_SUBQUERY, STRING_PREDICATE,
      AT_LEAST_N_NON_NULLS, BLOOM_FILTER, ARRAY_CONTAINS, ARRAYS_OVERLAP) &&
      Option(e.apply(conf.maxTreeNodeNumOfPredicate)).isEmpty
  }

  /**
   * Try to merge two `Aggregate`s by traverse down recursively.
   *
   * @return The optional tuple as follows:
   *         1. the merged plan
   *         2. the attribute mapping from the old to the merged version
   *         3. optional filters of both plans that need to be propagated and merged in an
   *         ancestor `Aggregate` node if possible.
   */
  private def mergePlan(
      left: LogicalPlan,
      right: LogicalPlan): Option[(LogicalPlan, AttributeMap[Attribute], Seq[Expression])] = {
    (left, right) match {
      case (la: Aggregate, ra: Aggregate) =>
        mergePlan(la.child, ra.child).map { case (newChild, outputMap, filters) =>
          val rightAggregateExprs = ra.aggregateExpressions.map(mapAttributes(_, outputMap))

          val mergedAggregateExprs = if (filters.length == 2) {
            Seq(
              (la.aggregateExpressions, filters.head),
              (rightAggregateExprs, filters.last)
            ).flatMap { case (aggregateExpressions, propagatedFilter) =>
              aggregateExpressions.map { ne =>
                ne.transform {
                  case ae @ AggregateExpression(_, _, _, filterOpt, _) =>
                    val newFilter = filterOpt.map { filter =>
                      And(propagatedFilter, filter)
                    }.orElse(Some(propagatedFilter))
                    ae.copy(filter = newFilter)
                }.asInstanceOf[NamedExpression]
              }
            }
          } else {
            la.aggregateExpressions ++ rightAggregateExprs
          }

          (Aggregate(Seq.empty, mergedAggregateExprs, newChild), AttributeMap.empty, Seq.empty)
        }
      case (lp: Project, rp: Project) =>
        val mergedProjectList = ArrayBuffer[NamedExpression](lp.projectList: _*)

        mergePlan(lp.child, rp.child).map { case (newChild, outputMap, filters) =>
          val allFilterReferences = filters.flatMap(_.references)
          val newOutputMap = AttributeMap((rp.projectList ++ allFilterReferences).map { ne =>
            val mapped = mapAttributes(ne, outputMap)

            val withoutAlias = mapped match {
              case Alias(child, _) => child
              case e => e
            }

            val outputAttr = mergedProjectList.find {
              case Alias(child, _) => child semanticEquals withoutAlias
              case e => e semanticEquals withoutAlias
            }.getOrElse {
              mergedProjectList += mapped
              mapped
            }.toAttribute
            ne.toAttribute -> outputAttr
          })

          (Project(mergedProjectList.toSeq, newChild), newOutputMap, filters)
        }
      case (lf: Filter, rf: Filter)
        if isCheapPredicate(lf.condition) && isCheapPredicate(rf.condition) =>
        mergePlan(lf.child, rf.child).map {
          case (newChild, outputMap, filters) =>
            val mappedRightCondition = mapAttributes(rf.condition, outputMap)
            val (newLeftCondition, newRightCondition) = if (filters.length == 2) {
              (And(lf.condition, filters.head), And(mappedRightCondition, filters.last))
            } else {
              (lf.condition, mappedRightCondition)
            }
          val newCondition = Or(newLeftCondition, newRightCondition)

          (Filter(newCondition, newChild), outputMap, Seq(newLeftCondition, newRightCondition))
        }
      case (ll: LeafNode, rl: LeafNode) =>
        checkIdenticalPlans(rl, ll).map { outputMap =>
          (ll, outputMap, Seq.empty)
        }
      case (ls: SerializeFromObject, rs: SerializeFromObject) =>
        checkIdenticalPlans(rs, ls).map { outputMap =>
          (ls, outputMap, Seq.empty)
        }
      case _ => None
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.combineJoinedAggregatesEnabled) return plan

    plan.transformUpWithPruning(_.containsAnyPattern(JOIN, AGGREGATE), ruleId) {
      case j @ Join(left: Aggregate, right: Aggregate, joinType, None, _)
        if isSupportedJoinType(joinType) &&
          left.groupingExpressions.isEmpty && right.groupingExpressions.isEmpty =>
        val mergedAggregate = mergePlan(left, right)
        mergedAggregate.map(_._1).getOrElse(j)
    }
  }
}
