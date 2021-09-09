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

package org.apache.spark.sql.execution.dynamicpruning

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.BuildBloomFilter
import org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Dynamic bloom filter pruning optimization is performed based on the type and
 * selectivity of the join operation. During query optimization, we insert a
 * predicate on the filterable table using the filter from the other side of
 * the join and a custom wrapper called DynamicPruning.
 */
object DynamicBloomFilterPruning extends Rule[LogicalPlan]
  with PredicateHelper
  with JoinSelectionHelper
  with DynamicPruningHelper {

  private def getFilterableTableScan(
      exp: Expression,
      plan: LogicalPlan): Option[LogicalPlan] = plan match {
    case p @ Project(_, child) =>
      getFilterableTableScan(replaceAlias(exp, getAliasMap(p)), child)
    // we can unwrap only if there are row projections, and no aggregation operation
    case a @ Aggregate(_, _, child) =>
      getFilterableTableScan(replaceAlias(exp, getAliasMap(a)), child)
    case f @ Filter(_, l: LeafNode)
      if exp.references.subsetOf(f.outputSet) && exp.references.subsetOf(l.outputSet) =>
      Some(f)
    case l: LeafNode if exp.references.subsetOf(l.outputSet) =>
      Some(l)
    case other =>
      other.children.flatMap {
        child => if (exp.references.subsetOf(child.outputSet)) {
          getFilterableTableScan(exp, child)
        } else {
          None
        }
      }.headOption
  }

  private def rowCounts(plan: LogicalPlan): BigInt = {
    plan.stats.rowCount
      .getOrElse(plan.stats.sizeInBytes / EstimationUtils.getSizePerRow(plan.output))
  }

  /**
   * Insert a dynamic bloom filter pruning predicate on one side of the join using the filter on the
   * other side of the join.
   *  - to be able to identify this filter during query planning, we use a custom
   *    DynamicPruning expression that wraps a regular InBloomFilter expression
   */
  private def insertPredicate(
      pruningKey: Expression,
      pruningPlan: LogicalPlan,
      filteringKey: Expression,
      filteringPlan: LogicalPlan,
      joinKeys: Seq[Expression],
      distinctCnt: Option[BigInt],
      rowCount: BigInt): LogicalPlan = {
    val expectedNumItems = distinctCnt.getOrElse(rowCount)
    val coalescePartitions = math.max(math.ceil(expectedNumItems.toDouble / 4000000.0).toInt, 1)
    // Use EnsureRequirements shuffle origin to reuse the exchange.
    val repartition = RepartitionByExpression(joinKeys, filteringPlan,
      optNumPartitions = Some(conf.numShufflePartitions),
      withEnsureRequirementsShuffleOrigin = true)
    // Coalesce partitions to improve build bloom filter performance.
    val coalesce = Repartition(coalescePartitions, shuffle = false, repartition)
    val bloomFilter = Aggregate(Nil,
      Seq(BuildBloomFilter(filteringKey, expectedNumItems.toLong, distinctCnt.isEmpty, 0, 0)
        .toAggregateExpression()).map(e => Alias(e, e.sql)()), coalesce)

    Filter(
      DynamicBloomFilterPruningSubquery(
        pruningKey,
        bloomFilter,
        joinKeys,
        joinKeys.indexOf(filteringKey)),
      pruningPlan)
  }

  private def pruningHasBenefit(
       filteringRowCount: BigInt,
       filteringDistinctCnt: Option[BigInt],
       pruningDistinctCnt: Option[BigInt],
       pruningScan: LogicalPlan): Boolean = {
    val filterRatio = filteringDistinctCnt
      .flatMap(x => pruningDistinctCnt.map(x.toFloat / _.toFloat)).filter(_ < 1.0).map(1.0 - _)
      .getOrElse(conf.dynamicPartitionPruningPruningSideExtraFilterRatio)
    filteringRowCount.toFloat / (rowCounts(pruningScan).toFloat * filterRatio) < 0.04
  }

  /**
   * DynamicBloomFilterPruningSubquery cannot be well supported in AQE.
   */
  private def hasDynamicBloomFilterPruningSubquery(plan: LogicalPlan): Boolean = {
    plan.collect { case Filter(cond, _) => cond }.exists {
      _.find {
        case _: DynamicBloomFilterPruningSubquery => true
        case _ => false
      }.isDefined
    }
  }

  private def prune(plan: LogicalPlan): LogicalPlan = {
    plan transformUp {
      // Skip this rule if there's already a dynamic bloom filter pruning subquery.
      case j @ Join(Filter(_: DynamicBloomFilterPruningSubquery, _), _, _, _, _) => j
      case j @ Join(_, Filter(_: DynamicBloomFilterPruningSubquery, _), _, _, _) => j
      case j @ Join(left, right, joinType, Some(condition), hint)
          if !canPlanAsBroadcastHashJoin(j, conf) =>
        var newLeft = left
        var newRight = right
        var newHint = hint // Avoid convert to BHJ if stream side has bloom filter.

        val (leftKeys, rightKeys) = extractEquiJoinKeys(j)

        val leftRowCount = rowCounts(left)
        val rightRowCnt = rowCounts(right)
        val isLeftSideSmall = leftRowCount < rightRowCnt
        val isRightSideSmall = rightRowCnt < leftRowCount

        splitConjunctivePredicates(condition).foreach {
          case EqualTo(a: Expression, b: Expression)
              if fromDifferentSides(a, b, left, right) &&
                BuildBloomFilter.isSupportBuildBloomFilter(a) =>
            val (l, r) = if (a.references.subsetOf(left.outputSet) &&
              b.references.subsetOf(right.outputSet)) {
              a -> b
            } else {
              b -> a
            }

            val leftDistCnt = distinctCounts(l, left)
            val rightDistCnt = distinctCounts(r, right)

            if (isRightSideSmall && canPruneLeft(joinType) && rightRowCnt > 0 &&
              rightRowCnt <= conf.dynamicBloomFilterJoinPruningMaxBloomFilterEntries &&
              !hasDynamicBloomFilterPruningSubquery(right) && supportDynamicPruning(right) &&
              getFilterableTableScan(l, left).exists { p =>
                !hasSelectivePredicate(p) &&
                  pruningHasBenefit(rightRowCnt, rightDistCnt, leftDistCnt, p)
              }) {
              newLeft = insertPredicate(l, newLeft, r, right, rightKeys, rightDistCnt, rightRowCnt)
              newHint = newHint.copy(leftHint = Some(HintInfo(strategy = Some(NO_BROADCAST_HASH))))
            }

            if (isLeftSideSmall && canPruneRight(joinType) && leftRowCount > 0 &&
              leftRowCount <= conf.dynamicBloomFilterJoinPruningMaxBloomFilterEntries &&
              !hasDynamicBloomFilterPruningSubquery(left) && supportDynamicPruning(left) &&
              getFilterableTableScan(r, right).exists { p =>
                !hasSelectivePredicate(p) &&
                  pruningHasBenefit(leftRowCount, leftDistCnt, rightDistCnt, p)
              }) {
              newRight = insertPredicate(r, newRight, l, left, leftKeys, leftDistCnt, leftRowCount)
              newHint = newHint.copy(rightHint = Some(HintInfo(strategy = Some(NO_BROADCAST_HASH))))
            }
          case _ =>
        }

        Join(newLeft, newRight, joinType, Some(condition), newHint)
    }
  }


  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // Do not rewrite subqueries.
    case s: Subquery if s.correlated => plan
    case _ if !conf.dynamicBloomFilterJoinPruningEnabled => plan
    case _ => prune(plan)
  }
}
