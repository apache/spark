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

import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.ExtractV2Scan
/**
 * Dynamic partition pruning optimization is performed based on the type and
 * selectivity of the join operation. During query optimization, we insert a
 * predicate on the filterable table using the filter from the other side of
 * the join and a custom wrapper called DynamicPruning.
 *
 * The basic mechanism for DPP inserts a duplicated subquery with the filter from the other side,
 * when the following conditions are met:
 *    (1) the table to prune is filterable by the JOIN key
 *    (2) the join operation is one of the following types: INNER, LEFT SEMI,
 *    LEFT OUTER (partitioned on right), or RIGHT OUTER (partitioned on left)
 *
 * In order to enable partition pruning directly in broadcasts, we use a custom DynamicPruning
 * clause that incorporates the In clause with the subquery and the benefit estimation.
 * During query planning, when the join type is known, we use the following mechanism:
 *    (1) if the join is a broadcast hash join, we replace the duplicated subquery with the reused
 *    results of the broadcast,
 *    (2) else if the estimated benefit of partition pruning outweighs the overhead of running the
 *    subquery query twice, we keep the duplicated subquery
 *    (3) otherwise, we drop the subquery.
 */
object PartitionPruning extends Rule[LogicalPlan] with PredicateHelper with JoinSelectionHelper {

  /**
   * Searches for a table scan that can be filtered for a given column in a logical plan.
   *
   * This methods tries to find either a v1 or Hive serde partitioned scan for a given
   * partition column or a v2 scan that support runtime filtering on a given attribute.
   */
  def getFilterableTableScan(a: Expression, plan: LogicalPlan): Option[LogicalPlan] = {
    val srcInfo: Option[(Expression, LogicalPlan)] = findExpressionAndTrackLineageDown(a, plan)
    srcInfo.flatMap {
      case (resExp, l: LogicalRelation) =>
        l.relation match {
          case fs: HadoopFsRelation =>
            val partitionColumns = AttributeSet(
              l.resolve(fs.partitionSchema, fs.sparkSession.sessionState.analyzer.resolver))
            if (resExp.references.subsetOf(partitionColumns)) {
              return Some(l)
            } else {
              None
            }
          case _ => None
        }
      case (resExp, l: HiveTableRelation) =>
        if (resExp.references.subsetOf(AttributeSet(l.partitionCols))) {
          return Some(l)
        } else {
          None
        }
      case (resExp, r @ ExtractV2Scan(scan: SupportsRuntimeV2Filtering)) =>
        val filterAttrs = V2ExpressionUtils.resolveAttributeRefs(
          scan.filterAttributes, r.output)
        if (resExp.references.subsetOf(filterAttrs)) {
          Some(r)
        } else {
          None
        }
      case _ => None
    }
  }

  /**
   * Insert a dynamic partition pruning predicate on one side of the join using the filter on the
   * other side of the join.
   *  - to be able to identify this filter during query planning, we use a custom
   *    DynamicPruning expression that wraps a regular In expression
   *  - we also insert a flag that indicates if the subquery duplication is worthwhile and it
   *  should run regardless of the join strategy, or is too expensive and it should be run only if
   *  we can reuse the results of a broadcast
   */
  private def insertPredicate(
      pruningKey: Expression,
      pruningPlan: LogicalPlan,
      filteringKeys: Seq[Expression],
      filteringPlan: LogicalPlan,
      joinKeys: Seq[Expression],
      partScan: LogicalPlan): LogicalPlan = {
    val reuseEnabled = conf.exchangeReuseEnabled
    require(filteringKeys.size == 1, "DPP Filters should only have a single broadcasting key " +
      "since there are no usage for multiple broadcasting keys at the moment.")
    val indices = Seq(joinKeys.indexOf(filteringKeys.head))
    lazy val hasBenefit = pruningHasBenefit(
      pruningKey, partScan, filteringKeys.head, filteringPlan, hasSelectivePredicate(filteringPlan))
    if (reuseEnabled || hasBenefit) {
      // insert a DynamicPruning wrapper to identify the subquery during query planning
      Filter(
        DynamicPruningSubquery(
          pruningKey,
          filteringPlan,
          joinKeys,
          indices,
          conf.dynamicPartitionPruningReuseBroadcastOnly || !hasBenefit),
        pruningPlan)
    } else {
      // abort dynamic partition pruning
      pruningPlan
    }
  }

  /**
   * Given an estimated filtering ratio we assume the partition pruning has benefit if
   * the size in bytes of the partitioned plan after filtering is greater than the size
   * in bytes of the plan on the other side of the join. We estimate the filtering ratio
   * using column statistics if they are available, otherwise we use the config value of
   * `spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio`.
   *
   * The fallback ratio is only meaningful "when CBO stats are missing, but there is a predicate
   * that is likely to be selective" -- so it is used only when `hasSelectivePredicate` is true. A
   * filtering side that is eligible only because it is already materialized (a LocalRelation or a
   * checkpoint-derived LogicalRDD, SPARK-54593) carries no such predicate; for it we rely solely on
   * the statistics-based ratio and report no benefit when statistics are unavailable, so it is not
   * injected as a standalone always-applied subquery on a guessed ratio. A statistics-based ratio,
   * when available, is always honored regardless of `hasSelectivePredicate`.
   */
  private def pruningHasBenefit(
      partExpr: Expression,
      partPlan: LogicalPlan,
      otherExpr: Expression,
      otherPlan: LogicalPlan,
      hasSelectivePredicate: Boolean): Boolean = {

    // get the distinct counts of an attribute for a given table
    def distinctCounts(attr: Attribute, plan: LogicalPlan): Option[BigInt] = {
      plan.stats.attributeStats.get(attr).flatMap(_.distinctCount)
    }

    // the filtering ratio derived from column statistics, when reliable stats are available
    val statsBasedRatio: Option[Double] =
      (partExpr.references.toList, otherExpr.references.toList) match {
        // filter out expressions with more than one attribute on any side of the operator
        case (leftAttr :: Nil, rightAttr :: Nil)
          if conf.dynamicPartitionPruningUseStats =>
            // get the CBO stats for each attribute in the join condition
            val partDistinctCount = distinctCounts(leftAttr, partPlan)
            // A materialized filtering side (e.g. a LocalRelation) may carry no column statistics
            // but an exact `maxRows`, which is a conservative upper bound on its join-key NDV. Use
            // it when the column statistic is missing so a small, selective materialized side still
            // yields a statistics-based ratio rather than falling through to the gated fallback.
            // Restrict this to a cheaply-recomputable materialized side: DPP re-evaluates the
            // filtering side, so deriving a pruning benefit from the `maxRows` of an opaque plan
            // (e.g. one with a `mapPartitions`, which `isCheaplyRecomputableMaterializedPlan`
            // rejects) could inject a standalone subquery that prunes a partition the join's
            // re-evaluation then needs, giving wrong results for a hidden-non-deterministic side.
            val otherDistinctCount =
              distinctCounts(rightAttr, otherPlan).orElse {
                if (isCheaplyRecomputableMaterializedPlan(otherPlan)) {
                  otherPlan.maxRows.map(BigInt(_))
                } else {
                  None
                }
              }
            val availableStats = partDistinctCount.isDefined && partDistinctCount.get > 0 &&
              otherDistinctCount.isDefined
            if (!availableStats) {
              None
            } else if (partDistinctCount.get.toDouble <= otherDistinctCount.get.toDouble) {
              // there is likely an estimation error, so there is no reliable stats-based ratio
              None
            } else {
              Some(1 - otherDistinctCount.get.toDouble / partDistinctCount.get.toDouble)
            }
        case _ => None
      }

    // Without a reliable stats-based ratio, fall back to the configured ratio only when there is a
    // predicate likely to be selective; otherwise there is no evidence of a pruning benefit.
    val filterRatio = statsBasedRatio.orElse {
      if (hasSelectivePredicate) Some(conf.dynamicPartitionPruningFallbackFilterRatio) else None
    }

    filterRatio.exists { ratio =>
      ratio * partPlan.stats.sizeInBytes.toFloat > calculatePlanOverhead(otherPlan)
    }
  }

  /**
   * Calculates a heuristic overhead of a logical plan. Normally it returns the total
   * size in bytes of all scan relations. We don't count in-memory relation which uses
   * only memory.
   */
  private def calculatePlanOverhead(plan: LogicalPlan): Float = {
    val (cached, notCached) = plan.collectLeaves().partition(p => p match {
      case _: InMemoryRelation => true
      case _ => false
    })
    val scanOverhead = notCached.map(_.stats.sizeInBytes).sum.toFloat
    val cachedOverhead = cached.map {
      case m: InMemoryRelation if m.cacheBuilder.storageLevel.useDisk &&
          !m.cacheBuilder.storageLevel.useMemory =>
        m.stats.sizeInBytes.toFloat
      case m: InMemoryRelation if m.cacheBuilder.storageLevel.useDisk =>
        m.stats.sizeInBytes.toFloat * 0.2
      case m: InMemoryRelation if m.cacheBuilder.storageLevel.useMemory =>
        0.0
    }.sum.toFloat
    scanOverhead + cachedOverhead
  }


  private def hasSelectivePredicate(plan: LogicalPlan): Boolean = plan.exists {
    case f: Filter => isLikelySelective(f.condition)
    case _ => false
  }

  /**
   * Returns whether the filtering side is cheap enough to recompute that DPP is worthwhile even
   * without a selective predicate: its cost is dominated by an already-materialized input, with
   * only scan-cost-bound operators above it.
   *
   * This is the cost-side counterpart to `hasSelectivePredicate`. A selective predicate is
   * evidence of a high pruning ratio (the benefit term of `pruningHasBenefit`); an
   * already-materialized input is the complementary signal on the cost term -- a `LocalRelation`
   * (rows already local) or a checkpoint-derived `LogicalRDD` (`isCheckpointedInput` requires the
   * RDD to be actually checkpointed, so a lazy checkpoint does not qualify) is ~free to re-read,
   * so even a modest pruning ratio clears the benefit bar. `InMemoryRelation` is excluded because
   * cache()/persist() are lazy: its presence does not guarantee the data has been materialized,
   * and missing or evicted blocks may require recomputing the upstream plan.
   *
   * The operators above the materialized input are restricted to ones whose cost is dominated by
   * their input's scan bytes -- the only cost `calculatePlanOverhead` can see. `Project`/`Filter`
   * add negligible compute, a `Union`'s cost is the sum of its (materialized) children, and
   * `SubqueryAlias` is a no-op. `Aggregate`, joins, and opaque RDD operators (e.g. `mapPartitions`)
   * are excluded: they add compute or a shuffle the scan-bytes cost model cannot see, so treating
   * such a side as a cheap materialized input would overstate the pruning benefit. A `Project`/
   * `Filter` is likewise excluded when its expressions embed a subquery (which carries its own
   * plan) or an opaque user function (a UDF or a user-defined generator) -- both add recompute
   * cost `calculatePlanOverhead` does not account for.
   *
   * This is primarily a cost guard, but the eligible shapes are also repeatable in practice, which
   * matters because DPP duplicates the filtering side and must produce the same keys on
   * re-evaluation. Honest non-determinism does not slip through: a `rand()` (or a UDF marked
   * non-deterministic) above the materialized input makes the resulting `DynamicPruningSubquery`
   * non-deterministic (`PlanExpression.deterministic` folds in its build plan), so
   * `CleanupDynamicPruningFilters` rewrites the dynamic predicate to `true` before physical
   * planning rather than planning a standalone `SubqueryExec` -- it is never re-evaluated. That
   * rule is non-excludable (`SparkOptimizer.nonExcludableRules`), so this holds regardless of
   * `spark.sql.optimizer.excludedRules`. The
   * residual, DPP-wide limitation is *hidden* non-determinism left marked deterministic; the
   * opaque-expression exclusion above narrows it, and the rest is intentionally left to a future
   * system-level design rather than patched piecemeal here. The one materialized-input-specific
   * repeatability concern -- a checkpoint that has not been materialized yet -- is handled by
   * `LogicalRDD.isCheckpointedInput` requiring the RDD to be actually checkpointed.
   */
  private def isCheaplyRecomputableMaterializedPlan(plan: LogicalPlan): Boolean = {
    // An expression keeps the side cheap only if its cost is bounded by the input scan that
    // `calculatePlanOverhead` measures. A subquery embeds its own plan, and an opaque user
    // function (a Scala/Python UDF, a user-defined generator, or any other non-Catalyst
    // expression) adds CPU/IO the scan-bytes cost model cannot see -- recomputing either would
    // cost more than the materialized leaf suggests, so it disqualifies the side.
    def isScanCostBoundExpression(e: Expression): Boolean = {
      !SubqueryExpression.hasSubquery(e) && !e.exists {
        case _: NonSQLExpression | _: UserDefinedExpression | _: UserDefinedGenerator => true
        case _ => false
      }
    }

    plan match {
      case _: LocalRelation => true
      case r: LogicalRDD => r.isCheckpointedInput
      case Project(projectList, child) if projectList.forall(isScanCostBoundExpression) =>
        isCheaplyRecomputableMaterializedPlan(child)
      case Filter(condition, child) if isScanCostBoundExpression(condition) =>
        isCheaplyRecomputableMaterializedPlan(child)
      case u: Union => u.children.forall(isCheaplyRecomputableMaterializedPlan)
      case SubqueryAlias(_, child) => isCheaplyRecomputableMaterializedPlan(child)
      case _ => false
    }
  }

  /**
   * To be able to prune partitions on a join key, the filtering side needs to
   * meet the following requirements:
   *   (1) it can not be a stream
   *   (2) it needs to contain a selective predicate or a cheaply-recomputable materialized input
   */
  private def hasPartitionPruningFilter(plan: LogicalPlan): Boolean = {
    !plan.isStreaming &&
      (hasSelectivePredicate(plan) || isCheaplyRecomputableMaterializedPlan(plan))
  }

  private def prune(plan: LogicalPlan): LogicalPlan = {
    plan transformUp {
      // skip this rule if there's already a DPP subquery on the LHS of a join
      case j @ Join(Filter(_: DynamicPruningSubquery, _), _, _, _, _) => j
      case j @ Join(_, Filter(_: DynamicPruningSubquery, _), _, _, _) => j
      case j @ Join(left, right, joinType, Some(condition), hint) =>
        var newLeft = left
        var newRight = right

        // extract the left and right keys of the join condition
        val (leftKeys, rightKeys) = j match {
          case ExtractEquiJoinKeys(_, lkeys, rkeys, _, _, _, _, _) => (lkeys, rkeys)
          case _ => (Nil, Nil)
        }

        // checks if two expressions are on opposite sides of the join
        def fromDifferentSides(x: Expression, y: Expression): Boolean = {
          def fromLeftRight(x: Expression, y: Expression) =
            !x.references.isEmpty && x.references.subsetOf(left.outputSet) &&
              !y.references.isEmpty && y.references.subsetOf(right.outputSet)
          fromLeftRight(x, y) || fromLeftRight(y, x)
        }

        splitConjunctivePredicates(condition).foreach {
          case EqualTo(a: Expression, b: Expression)
              if fromDifferentSides(a, b) =>
            val (l, r) = if (a.references.subsetOf(left.outputSet) &&
              b.references.subsetOf(right.outputSet)) {
              a -> b
            } else {
              b -> a
            }

            // there should be a partitioned table and a filter on the dimension table,
            // otherwise the pruning will not trigger
            var filterableScan = getFilterableTableScan(l, left)
            if (filterableScan.isDefined && canPruneLeft(joinType) &&
                hasPartitionPruningFilter(right)) {
              newLeft = insertPredicate(l, newLeft, Seq(r), right, rightKeys, filterableScan.get)
            } else {
              filterableScan = getFilterableTableScan(r, right)
              if (filterableScan.isDefined && canPruneRight(joinType) &&
                  hasPartitionPruningFilter(left) ) {
                newRight = insertPredicate(r, newRight, Seq(l), left, leftKeys, filterableScan.get)
              }
            }
          case _ =>
        }
        Join(newLeft, newRight, joinType, Some(condition), hint)
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // Do not rewrite subqueries.
    case s: Subquery if s.correlated => plan
    case _ if !conf.dynamicPartitionPruningEnabled => plan
    case _ => prune(plan)
  }
}
