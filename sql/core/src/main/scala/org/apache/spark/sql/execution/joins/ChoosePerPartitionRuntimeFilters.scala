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

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, ExprId, PredicateHelper}
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.optimizer.PerPartitionRuntimeFilterSubquery.{perPartitionRuntimeFilter, sizingOf}
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
import org.apache.spark.sql.catalyst.plans.physical.{PartitioningCollection, SinglePartition}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SnapshotUnionPreparationConf, SparkPlan, UnionConfSnapshot}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, Exchange, ShuffleExchangeLike}

/**
 * Keeps each semi join of a per-partition runtime bloom filter that
 * `InsertPerPartitionRuntimeFilterJoins` built only when it costs the application side nothing,
 * and sizes its bloom filters.
 *
 * A semi join costs the application side nothing when it adds no shuffle, and the stage that reads
 * the application side runs at least as many tasks as without it. That holds when the join above
 * is a storage-partitioned join that groups the application side's splits the way the semi join
 * does. So this asks `ensureRequirements`, the one the preparation plans with, how it would plan
 * the query. It starts from the query without the semi joins, and adds them back one at a time.
 * Each stays when it is free against the plan built so far. So a semi join that goes cannot make
 * one that is free look costly. The rule judges every semi join it finds, whatever the conf says
 * now, since a plan optimized with the conf on can be planned with it off.
 *
 * That planning asks a `UnionExec` what it reports, so this first records `unionConf` on each
 * union, as the preparation does ahead of `EnsureRequirements`.
 *
 * Each bloom filter of a semi join that stays expects an even share of the creation side's items
 * over its partition keys. The semi join also goes when the creation side has too few partition
 * keys for each share to be within the size threshold of a bloom filter's input.
 */
case class ChoosePerPartitionRuntimeFilters(
    ensureRequirements: EnsureRequirements,
    unionConf: UnionConfSnapshot) extends Rule[SparkPlan] with PredicateHelper {

  override def apply(plan: SparkPlan): SparkPlan = {
    val semiJoins = plan.flatMap(PerPartitionSemiJoin.unapply(_))
    // The application side without each semi join this rule can take out, by its bloom filter
    // column, which stays when a semi join above goes. A semi join of a shape this rule does not
    // know stays as it is.
    val withouts = semiJoins.flatMap { case (filter, join, bloomFilter) =>
      withoutSemiJoin(filter, join).map(bloomFilter.exprId -> _)
    }.toMap
    if (withouts.isEmpty) {
      plan
    } else {
      new SnapshotUnionPreparationConf(unionConf).apply(plan)
      // `plan` with only those of the semi joins this rule can take out that are in `kept`.
      def withOnly(kept: Set[ExprId]): SparkPlan = plan.transformDown {
        case PerPartitionSemiJoin(_, _, bloomFilter)
            if withouts.contains(bloomFilter.exprId) && !kept(bloomFilter.exprId) =>
          withouts(bloomFilter.exprId)
      }
      // The expected number of items of each bloom filter of each semi join that stays.
      var numItems = Map.empty[ExprId, Long]
      var current = new Cost(ensureRequirements.apply(withOnly(Set.empty)))
      semiJoins.foreach { case (_, join, bloomFilter) =>
        val id = bloomFilter.exprId
        if (withouts.contains(id)) {
          numItemsOf(join, bloomFilter).foreach { n =>
            val trial = new Cost(ensureRequirements.apply(withOnly(numItems.keySet + id)))
            if (isFree(trial, current, withouts(id))) {
              numItems += id -> n
              current = trial
            }
          }
        }
      }
      plan.transformDown {
        case p @ ProjectExec(_, PerPartitionSemiJoin(_, _, bloomFilter))
            if withouts.contains(bloomFilter.exprId) && !numItems.contains(bloomFilter.exprId) =>
          withoutRestoringProjection(p, withouts(bloomFilter.exprId))
        case PerPartitionSemiJoin(filter, join, bloomFilter)
            if withouts.contains(bloomFilter.exprId) =>
          numItems.get(bloomFilter.exprId) match {
            case Some(n) => filter.withNewChildren(Seq(withNumItems(join, n)))
            case None => withouts(bloomFilter.exprId)
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
            if join.joinType.isInstanceOf[ExistenceJoin] =>
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
  }

  // Whether a semi join adds no shuffle, and leaves the stage that reads its application side no
  // fewer tasks than it has without the semi join. `withIt` and `withoutIt` are the costs of the
  // plan with and without it, and `without` is its application side without it. When the tasks of
  // a scan are not found, the semi join is taken as not free.
  private def isFree(withIt: Cost, withoutIt: Cost, without: SparkPlan): Boolean =
    withIt.shuffles <= withoutIt.shuffles &&
      without.collectLeaves().forall { scan =>
        (withIt.tasksReading(scan), withoutIt.tasksReading(scan)) match {
          case (Some(tasks), Some(tasksWithout)) => tasks >= tasksWithout
          case _ => false
        }
      }

  // The expected number of items of each bloom filter of the semi join, an even share of the
  // creation side's over its partition keys. None when the creation side has fewer partition keys
  // than it needs for each share to be within the size threshold, or reports none.
  private def numItemsOf(join: ShuffledHashJoinExec, bloomFilter: Attribute): Option[Long] = {
    val (numItems, minPartitionKeys) = sizingOf(bloomFilter)
    partitionKeysOf(join.right).filter(_ >= minPartitionKeys).map { keys =>
      ((numItems + keys - 1) / keys).max(1L)
    }
  }

  // The number of partition keys of the input of the aggregate `plan`, before any exchange.
  private def partitionKeysOf(plan: SparkPlan): Option[Long] = plan match {
    case aggregate: BaseAggregateExec => partitionKeysOf(aggregate.child)
    case input => PartitioningCollection.representativeOf(input.outputPartitioning).map { k =>
      (if (k.isGrouped) k.numPartitions else k.partitionKeys.distinct.size).toLong
    }
  }

  // The semi join with each bloom filter of its aggregate expecting `numItems` items.
  private def withNumItems(join: ShuffledHashJoinExec, numItems: Long): SparkPlan = {
    def sized(plan: SparkPlan): SparkPlan = plan match {
      case aggregate: BaseAggregateExec =>
        aggregate.transformExpressions {
          case b: BloomFilterAggregate =>
            b.withNewChildren(new BloomFilterAggregate(b.child, numItems).children)
        }.withNewChildren(Seq(sized(aggregate.child)))
      case other => other
    }
    join.withNewChildren(Seq(join.left, sized(join.right)))
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

  // The projection `p` above a semi join that goes, which restored the output, over `without`, the
  // application side without the semi join. When `without` ends in a projection of its columns,
  // the two become one. So that side has the plan it has without per-partition bloom filters, as a
  // DPP filter that reuses a broadcast of it expects.
  private def withoutRestoringProjection(p: ProjectExec, without: SparkPlan): SparkPlan =
    without match {
      case ProjectExec(columns, child) if columns.forall(_.isInstanceOf[Attribute]) =>
        ProjectExec(p.projectList, child)
      case _ => p.withNewChildren(Seq(without))
    }
}
