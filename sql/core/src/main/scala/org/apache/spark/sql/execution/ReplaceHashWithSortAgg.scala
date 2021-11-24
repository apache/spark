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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, Final, Partial}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.internal.SQLConf

/**
 * Replace [[HashAggregateExec]] with [[SortAggregateExec]] in the spark plan if:
 *
 * 1. The plan is a pair of partial and final [[HashAggregateExec]], and the child of partial
 *    aggregate satisfies the sort order of corresponding [[SortAggregateExec]].
 * or
 * 2. The plan is a [[HashAggregateExec]], and the child satisfies the sort order of
 *    corresponding [[SortAggregateExec]].
 *
 * Examples:
 * 1. aggregate after join:
 *
 *  HashAggregate(t1.i, SUM, final)
 *               |                         SortAggregate(t1.i, SUM, complete)
 * HashAggregate(t1.i, SUM, partial)   =>                |
 *               |                            SortMergeJoin(t1.i = t2.j)
 *    SortMergeJoin(t1.i = t2.j)
 *
 * 2. aggregate after sort:
 *
 * HashAggregate(t1.i, SUM, partial)         SortAggregate(t1.i, SUM, partial)
 *               |                     =>                  |
 *           Sort(t1.i)                                Sort(t1.i)
 *
 * [[HashAggregateExec]] can be replaced when its child satisfies the sort order of
 * corresponding [[SortAggregateExec]]. [[SortAggregateExec]] is faster in the sense that
 * it does not have hashing overhead of [[HashAggregateExec]].
 */
object ReplaceHashWithSortAgg extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.REPLACE_HASH_WITH_SORT_AGG_ENABLED)) {
      plan
    } else {
      replaceHashAgg(plan)
    }
  }

  /**
   * Replace [[HashAggregateExec]] with [[SortAggregateExec]].
   */
  private def replaceHashAgg(plan: SparkPlan): SparkPlan = {
    plan.transformDown {
      case hashAgg: HashAggregateExec =>
        val sortAgg = hashAgg.toSortAggregate
        hashAgg.child match {
          case partialAgg: HashAggregateExec if isPartialAgg(partialAgg, hashAgg) =>
            if (SortOrder.orderingSatisfies(
                partialAgg.child.outputOrdering, sortAgg.requiredChildOrdering.head)) {
              sortAgg.copy(
                aggregateExpressions = sortAgg.aggregateExpressions.map(_.copy(mode = Complete)),
                child = partialAgg.child)
            } else {
              hashAgg
            }
          case other =>
            if (SortOrder.orderingSatisfies(
                other.outputOrdering, sortAgg.requiredChildOrdering.head)) {
              sortAgg
            } else {
              hashAgg
            }
        }
      case other => other
    }
  }

  /**
   * Check if `partialAgg` to be partial aggregate of `finalAgg`.
   */
  private def isPartialAgg(partialAgg: HashAggregateExec, finalAgg: HashAggregateExec): Boolean = {
    val partialGroupExprs = partialAgg.groupingExpressions
    val finalGroupExprs = finalAgg.groupingExpressions
    val partialAggExprs = partialAgg.aggregateExpressions
    val finalAggExprs = finalAgg.aggregateExpressions
    val partialAggAttrs = partialAggExprs.flatMap(_.aggregateFunction.aggBufferAttributes)
    val finalAggAttrs = finalAggExprs.map(_.resultAttribute)
    val partialResultExprs = partialGroupExprs ++
      partialAggExprs.flatMap(_.aggregateFunction.inputAggBufferAttributes)

    val groupExprsEqual = partialGroupExprs.length == finalGroupExprs.length &&
      partialGroupExprs.zip(finalGroupExprs).forall {
        case (e1, e2) => e1.semanticEquals(e2)
      }
    val aggExprsEqual = partialAggExprs.length == finalAggExprs.length &&
      partialAggExprs.forall(_.mode == Partial) && finalAggExprs.forall(_.mode == Final) &&
      partialAggExprs.zip(finalAggExprs).forall {
        case (e1, e2) => e1.aggregateFunction.semanticEquals(e2.aggregateFunction)
      }
    val isPartialAggAttrsValid = partialAggAttrs.length == partialAgg.aggregateAttributes.length &&
      partialAggAttrs.zip(partialAgg.aggregateAttributes).forall {
        case (a1, a2) => a1.semanticEquals(a2)
      }
    val isFinalAggAttrsValid = finalAggAttrs.length == finalAgg.aggregateAttributes.length &&
      finalAggAttrs.zip(finalAgg.aggregateAttributes).forall {
        case (a1, a2) => a1.semanticEquals(a2)
      }
    val isPartialResultExprsValid =
      partialResultExprs.length == partialAgg.resultExpressions.length &&
        partialResultExprs.zip(partialAgg.resultExpressions).forall {
          case (a1, a2) => a1.semanticEquals(a2)
        }
    val isRequiredDistributionValid =
      partialAgg.requiredChildDistributionExpressions.isEmpty &&
      finalAgg.requiredChildDistributionExpressions.exists { exprs =>
        exprs.length == finalGroupExprs.length &&
          exprs.zip(finalGroupExprs).forall {
            case (e1, e2) => e1.semanticEquals(e2)
          }
      }

    groupExprsEqual && aggExprsEqual && isPartialAggAttrsValid && isFinalAggAttrsValid &&
      isPartialResultExprsValid && isRequiredDistributionValid
  }
}
