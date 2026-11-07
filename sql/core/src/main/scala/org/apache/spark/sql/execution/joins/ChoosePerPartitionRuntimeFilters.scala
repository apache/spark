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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, BloomFilterMightContain, Expression, ExprId, PlanExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.optimizer.InjectRuntimeFilter.{globalBloomFilterOf, perPartitionRuntimeFilter}
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
import org.apache.spark.sql.catalyst.plans.physical.{REPLICATED_FOR_JOIN, SinglePartition}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SnapshotUnionPreparationConf, SparkPlan, UnionConfSnapshot}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, GroupPartitionsExec}
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, Exchange, ShuffleExchangeLike}

/**
 * Chooses between the checks `InjectRuntimeFilter` adds to the application side of a join
 * with per-partition runtime bloom filters:
 *  - the per-partition one, a semi join with one bloom filter per partition key;
 *  - the global one, a filter with a bloom filter of the whole creation side.
 *
 * The per-partition check pays off only when it costs the application side nothing. The semi join
 * then adds no shuffle, and the stage that reads the application side runs at least as many tasks
 * as without it. That holds when the join above is a storage-partitioned join that groups the
 * application side's splits the way the semi join does. So this asks `ensureRequirements`, set up
 * like the one the preparation plans with, how it would plan the query with and without the semi
 * join. When the semi join is free, it stays and the global check of the same key goes. Otherwise
 * the semi join goes. It also goes when:
 *  - a scan applies the global check as a storage filter, since that scan already skips the rows
 *    the check rules out while it reads;
 *  - there is a global check, and the semi join's aggregate repeats, as under partially clustered
 *    distribution. Each repeat reads the creation side again, while the global check reads it
 *    once;
 *  - a join above repeats the side the semi join is on, as a partially clustered join repeats its
 *    smaller side. Each repeat runs the semi join again, its aggregate included.
 *
 * That planning asks a `UnionExec` what it reports, so this first records `unionConf` on each
 * union, as the preparation does ahead of `EnsureRequirements`.
 */
case class ChoosePerPartitionRuntimeFilters(
    ensureRequirements: EnsureRequirements,
    unionConf: UnionConfSnapshot) extends Rule[SparkPlan] with PredicateHelper {

  override def apply(plan: SparkPlan): SparkPlan = {
    val semiJoins = if (conf.runtimeBloomFilterPerPartitionEnabled) {
      plan.flatMap(PerPartitionSemiJoin.unapply(_))
    } else {
      Nil
    }
    if (semiJoins.isEmpty) {
      plan
    } else {
      new SnapshotUnionPreparationConf(unionConf).apply(plan)
      lazy val planned = new Cost(ensureRequirements.apply(plan))
      // Each semi join is judged against `plan`, before any check goes. A semi join that drops its
      // global check can rebuild the semi joins below it. So the verdicts are keyed by the bloom
      // filter column, which a rebuilt semi join keeps.
      val keeps = semiJoins.flatMap { case (filter, join, bloomFilter) =>
        val global = globalBloomFilterOf(bloomFilter)
        withoutSemiJoin(filter, join).map { without =>
          bloomFilter.exprId -> (!appliedByScan(join, global) &&
            !(global >= 0 && planned.repeatedAggregates(bloomFilter.exprId)) &&
            !planned.repeatedSemiJoins(bloomFilter.exprId) &&
            isFree(plan, planned, filter, without))
        }
      }.toMap
      // A semi join of a shape this rule does not know is not in `keeps`, and keeps both checks.
      plan.transformDown {
        case PerPartitionSemiJoin(filter, join, bloomFilter)
            if keeps.contains(bloomFilter.exprId) =>
          if (keeps(bloomFilter.exprId)) {
            filter.withNewChildren(Seq(withoutGlobalCheck(join, globalBloomFilterOf(bloomFilter))))
          } else {
            withoutSemiJoin(filter, join).getOrElse(filter)
          }
      }
    }
  }

  // The semi join of a per-partition bloom filter: the filter on the flag of its existence join,
  // that join, and the bloom filter column.
  private object PerPartitionSemiJoin {
    def unapply(plan: SparkPlan): Option[(FilterExec, ShuffledHashJoinExec, Attribute)] =
      plan match {
        case filter @ FilterExec(_, join: ShuffledHashJoinExec)
            if join.joinType.isInstanceOf[ExistenceJoin] && join.buildSide == BuildRight =>
          join.condition.flatMap(perPartitionRuntimeFilter).map {
            case (bloomFilter, _) => (filter, join, bloomFilter)
          }
        case _ => None
      }
  }

  // The shuffles of a plan, and the tasks of the stage that reads each of its leaves. Those are the
  // partitions of the topmost operator above the leaf that no exchange separates from it. An
  // operator that reports one partition or none, such as a limit, a write or an `Expand`, does not
  // tell how many tasks read its child, so the child counts instead.
  private class Cost(plan: SparkPlan) {
    val shuffles: Int = plan.collect { case s: ShuffleExchangeLike => s }.size

    private val tasks: Seq[(SparkPlan, Int)] = {
      def walk(node: SparkPlan, top: SparkPlan): Seq[(SparkPlan, Int)] = {
        if (node.children.isEmpty) {
          Seq(node -> top.outputPartitioning.numPartitions)
        } else {
          val countsChild = node.isInstanceOf[Exchange] || (node.outputPartitioning match {
            case SinglePartition => true
            case partitioning => partitioning.numPartitions == 0
          })
          node.children.flatMap(child => walk(child, if (countsChild) child else top))
        }
      }
      walk(plan, plan)
    }

    def tasksReading(leaf: SparkPlan): Option[Int] =
      tasks.collectFirst { case (l, n) if l eq leaf => n }

    // The bloom filter columns of the semi joins that repeat their aggregate, once for each split
    // of the application side, as under partially clustered distribution. An existence join never
    // repeats its application side (`ShuffledJoin.canDuplicateLeftSide`).
    lazy val repeatedAggregates: Set[ExprId] = plan.collect {
      case PerPartitionSemiJoin(_, join, bloomFilter) if isRepeated(join.right) =>
        bloomFilter.exprId
    }.toSet

    // The bloom filter columns of the semi joins on a side that a join above repeats, as a
    // partially clustered join repeats its smaller side. Each repeat computes that side again. So
    // it runs the semi join again, unless an exchange lies between them.
    lazy val repeatedSemiJoins: Set[ExprId] = {
      def semiJoinsIn(node: SparkPlan): Seq[ExprId] = node match {
        case _: Exchange => Nil
        case PerPartitionSemiJoin(_, _, bloomFilter) =>
          bloomFilter.exprId +: node.children.flatMap(semiJoinsIn)
        case _ => node.children.flatMap(semiJoinsIn)
      }
      plan.collect {
        case group if isRepeated(group) => group.children.flatMap(semiJoinsIn)
      }.flatten.toSet
    }

    // Whether `plan` reads a partition of its child more than once. Partially clustered
    // distribution marks the side it repeats with `REPLICATED_FOR_JOIN`. It does so also when each
    // key has a single split on the other side, and then nothing repeats.
    private def isRepeated(plan: SparkPlan): Boolean = plan match {
      case group: GroupPartitionsExec =>
        group.ungroupingOrigin.contains(REPLICATED_FOR_JOIN) &&
          group.grouping.numReplicatedPartitionReads > 0
      case _ => false
    }
  }

  // Whether the semi join adds no shuffle, and leaves the stage that reads its application side no
  // fewer tasks than it has without the semi join. `planned` is the cost of `plan` after
  // `ensureRequirements`. `semiJoin` is the filter of the semi join, and `without` is the semi
  // join's application side without it. When the tasks of a scan are not found, the semi join is
  // taken as not free.
  private def isFree(
      plan: SparkPlan,
      planned: Cost,
      semiJoin: FilterExec,
      without: SparkPlan): Boolean = {
    val withoutIt = new Cost(ensureRequirements.apply(plan.transformDown {
      case f: FilterExec if f eq semiJoin => without
    }))
    planned.shuffles <= withoutIt.shuffles &&
      without.collectLeaves().forall { scan =>
        (planned.tasksReading(scan), withoutIt.tasksReading(scan)) match {
          case (Some(tasks), Some(tasksWithout)) => tasks >= tasksWithout
          case _ => false
        }
      }
  }

  // Whether a scan applies the global check `global` as a storage filter.
  private def appliedByScan(join: ShuffledHashJoinExec, global: Long): Boolean =
    join.left.exists {
      case scan: BatchScanExec => scan.storageFilters.exists(isGlobalCheck(global))
      case _ => false
    }

  // Whether `e` checks the bloom filter of the subquery with the exprId `global`.
  private def isGlobalCheck(global: Long)(e: Expression): Boolean = e match {
    case BloomFilterMightContain(subquery: PlanExpression[_], _) => subquery.exprId.id == global
    case _ => false
  }

  // Drops the global check of the subquery with the exprId `global` from the semi join's
  // application side. A scan whose runtime filtering takes the key column holds a copy among its
  // runtime filters, which goes too. That copy prunes nothing, since a bloom has no V2 form and a
  // partition predicate needs an identity partition column.
  private def withoutGlobalCheck(join: ShuffledHashJoinExec, global: Long): SparkPlan = {
    val left = join.left.transformUp {
      case filter @ FilterExec(condition, child) =>
        val (checks, rest) = splitConjunctivePredicates(condition).partition(isGlobalCheck(global))
        if (checks.isEmpty) {
          filter
        } else {
          rest.reduceOption(And).map(FilterExec(_, child)).getOrElse(child)
        }
      case scan: BatchScanExec if scan.runtimeFilters.exists(isGlobalCheck(global)) =>
        scan.copy(runtimeFilters = scan.runtimeFilters.filterNot(isGlobalCheck(global)))
    }
    join.withNewChildren(Seq(left, join.right))
  }

  // The application side without the semi join, and without the partition key column it adds. The
  // conditions of `filter` other than the one on the join's flag, the last column of its output,
  // stay.
  private def withoutSemiJoin(filter: FilterExec, join: ShuffledHashJoinExec): Option[SparkPlan] = {
    val conditions = splitConjunctivePredicates(filter.condition)
      .filterNot(_.references.contains(join.output.last))
    (join.leftKeys, join.left) match {
      case (Seq(partitionKey: Attribute), ProjectExec(projectList, child))
          if projectList.exists(_.exprId == partitionKey.exprId) =>
        val rest = projectList.filterNot(_.exprId == partitionKey.exprId)
        val sameOutput = rest.map(_.exprId) == child.output.map(_.exprId)
        val without = if (sameOutput) child else ProjectExec(rest, child)
        Some(conditions.reduceOption(And).map(FilterExec(_, without)).getOrElse(without))
      case _ => None
    }
  }
}
