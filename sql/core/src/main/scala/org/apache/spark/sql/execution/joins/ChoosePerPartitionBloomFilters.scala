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

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, PredicateHelper}
import org.apache.spark.sql.catalyst.optimizer.{BuildRight, InjectRuntimeFilter}
import org.apache.spark.sql.catalyst.optimizer.InjectRuntimeFilter.isGlobalCheck
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FileSourceScanLike, FilterExec, ProjectExec, SnapshotUnionPreparationConf, SparkPlan, UnionConfSnapshot}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, Exchange, ShuffleExchangeLike}

/**
 * Chooses between the checks `InjectRuntimeFilter` adds to the application side of a join
 * with per-partition runtime bloom filters:
 *  - the per-partition one, a left semi join with one bloom filter per partition key;
 *  - the global one, a filter with a bloom filter of the whole creation side.
 *
 * The per-partition check pays off only when it costs the application side nothing. The semi join
 * then adds no shuffle, and the stage that reads the application side runs as many tasks as without
 * it. That holds when the join above is a storage-partitioned join that groups the application
 * side's splits the way the semi join does. So this asks `ensureRequirements`, set up like the one
 * the preparation plans with, how it would plan the query with and without the semi join. When the
 * semi join is free, it stays and the global check of the same key goes. Otherwise the semi join
 * goes. It also goes when a scan applies the global check as a storage filter, since that scan
 * already skips the rows the check rules out while it reads.
 *
 * That planning asks a `UnionExec` what it reports, so this first records `unionConf` on each
 * union, as the preparation does ahead of `EnsureRequirements`.
 */
case class ChoosePerPartitionBloomFilters(
    ensureRequirements: EnsureRequirements,
    unionConf: UnionConfSnapshot) extends Rule[SparkPlan] with PredicateHelper {

  override def apply(plan: SparkPlan): SparkPlan = {
    val semiJoins = if (conf.runtimeBloomFilterPerPartitionEnabled) {
      plan.collect { case join: ShuffledHashJoinExec => bloomFilterOf(join).map(join -> _) }.flatten
    } else {
      Nil
    }
    if (semiJoins.isEmpty) {
      plan
    } else {
      new SnapshotUnionPreparationConf(unionConf).apply(plan)
      val planned = new Cost(ensureRequirements.apply(plan))
      // Each semi join is judged against `plan`, before any check goes. A semi join that drops its
      // global check can rebuild the semi joins below it. So the verdicts are keyed by the bloom
      // filter column, which a rebuilt semi join keeps.
      val keeps = semiJoins.flatMap { case (join, (bloomFilter, global)) =>
        withoutSemiJoin(join).map { without =>
          bloomFilter.exprId ->
            (!appliedByScan(join, global) && isFree(plan, planned, join, without))
        }
      }.toMap
      plan.transformDown {
        case join: ShuffledHashJoinExec => bloomFilterOf(join) match {
          case Some((bloomFilter, global)) => keeps.get(bloomFilter.exprId) match {
            case Some(true) => withoutGlobalCheck(join, global)
            case Some(false) => withoutSemiJoin(join).getOrElse(join)
            // A shape this rule does not know, which keeps both checks.
            case None => join
          }
          case None => join
        }
      }
    }
  }

  // For a semi join that `InjectRuntimeFilter.injectPerPartitionBloomFilter` builds, its bloom
  // filter column and the exprId of the global bloom filter's subquery the column carries, or -1.
  private def bloomFilterOf(join: ShuffledHashJoinExec): Option[(Attribute, Long)] = {
    if (join.joinType != LeftSemi || join.buildSide != BuildRight) {
      None
    } else {
      join.condition.flatMap(InjectRuntimeFilter.perPartitionBloomFilter)
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
  }

  // Whether the semi join adds no shuffle, and leaves the stage that reads its application side no
  // fewer tasks than it has without the semi join. `planned` is the cost of `plan` after
  // `ensureRequirements`, and `without` is the semi join's application side without it. When the
  // tasks of a scan are not found, the semi join is taken as not free.
  private def isFree(
      plan: SparkPlan,
      planned: Cost,
      join: ShuffledHashJoinExec,
      without: SparkPlan): Boolean = {
    val withoutIt = new Cost(ensureRequirements.apply(plan.transformDown {
      case j: ShuffledHashJoinExec if j eq join => without
    }))
    planned.shuffles <= withoutIt.shuffles &&
      join.left.collectLeaves().forall { scan =>
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
      case scan: FileSourceScanLike => scan.storageFilters.exists(isGlobalCheck(global))
      case _ => false
    }

  // Drops the global check of the subquery with the exprId `global` from the semi join's
  // application side. A scan whose runtime filtering takes the key column holds a copy among its
  // runtime filters, which goes too. That copy prunes nothing, since a bloom has no V2 form and a
  // partition predicate needs an identity partition column.
  private def withoutGlobalCheck(join: ShuffledHashJoinExec, global: Long): SparkPlan = {
    val inFilter = join.left.exists {
      case FilterExec(condition, _) =>
        splitConjunctivePredicates(condition).exists(isGlobalCheck(global))
      case _ => false
    }
    if (!inFilter) {
      join
    } else {
      val left = join.left.transformUp {
        case filter @ FilterExec(condition, child) =>
          val (checks, rest) =
            splitConjunctivePredicates(condition).partition(isGlobalCheck(global))
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
  }

  // The application side without the semi join, and without the partition key column it adds.
  private def withoutSemiJoin(join: ShuffledHashJoinExec): Option[SparkPlan] = {
    (join.leftKeys, join.left) match {
      case (Seq(partitionKey: Attribute), ProjectExec(projectList, child))
          if projectList.exists(_.exprId == partitionKey.exprId) =>
        val rest = projectList.filterNot(_.exprId == partitionKey.exprId)
        val sameOutput = rest.map(_.exprId) == child.output.map(_.exprId)
        Some(if (sameOutput) child else ProjectExec(rest, child))
      case _ => None
    }
  }
}
