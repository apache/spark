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

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, ExprId, Literal, PredicateHelper}
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
 * over its partition keys. Without a row count, it also gets an even share of the creation side's
 * bits. The semi join also goes when the creation side has too few partition keys for each share
 * to be within the size threshold of a bloom filter's input.
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
      // `plan` with only those of the semi joins this rule can take out that are in `kept`, each
      // replaced by its entry there. For a semi join that goes, `withoutRestoringProjection`
      // rebuilds a projection above it over its side.
      def withOnly(kept: Map[ExprId, SparkPlan]): SparkPlan = {
        def goes(bloomFilter: Attribute): Boolean =
          withouts.contains(bloomFilter.exprId) && !kept.contains(bloomFilter.exprId)
        plan.transformDown {
          case p @ ProjectExec(_, ProjectExec(restoring, PerPartitionSemiJoin(_, _, bloomFilter)))
              if goes(bloomFilter) && restoring.forall(_.isInstanceOf[Attribute]) =>
            withoutRestoringProjection(p, withouts(bloomFilter.exprId))
          case p @ ProjectExec(_, PerPartitionSemiJoin(_, _, bloomFilter)) if goes(bloomFilter) =>
            withoutRestoringProjection(p, withouts(bloomFilter.exprId))
          case PerPartitionSemiJoin(_, _, bloomFilter) if withouts.contains(bloomFilter.exprId) =>
            kept.getOrElse(bloomFilter.exprId, withouts(bloomFilter.exprId))
        }
      }
      // Each semi join that stays, with its bloom filters sized.
      var kept = Map.empty[ExprId, SparkPlan]
      var current = new Cost(ensureRequirements.apply(withOnly(kept)))
      semiJoins.foreach { case (filter, join, bloomFilter) =>
        val id = bloomFilter.exprId
        if (withouts.contains(id)) {
          sizeOf(join, bloomFilter).foreach { case (numItems, numBits) =>
            val semiJoin = filter.withNewChildren(Seq(withSize(join, numItems, numBits)))
            val trial = new Cost(ensureRequirements.apply(withOnly(kept + (id -> semiJoin))))
            if (isFree(trial, current, withouts(id))) {
              kept += id -> semiJoin
              current = trial
            }
          }
        }
      }
      withOnly(kept)
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
      // `topTasks` is the number of partitions of that topmost operator, or None when it is `node`.
      // So each node is asked for its partitioning once. A union derives it again on each call.
      def walk(node: SparkPlan, topTasks: Option[Int]): Seq[(SparkPlan, Int)] = {
        lazy val partitioning = node.outputPartitioning
        lazy val tasks = topTasks.getOrElse(partitioning.numPartitions)
        if (node.children.isEmpty) {
          Seq(node -> tasks)
        } else {
          val countsChild = node.isInstanceOf[Exchange] || (partitioning match {
            case SinglePartition => true
            case p => p.numPartitions == 0
          })
          node.children.flatMap(walk(_, if (countsChild) None else Some(tasks)))
        }
      }
      walk(plan, None)
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
      applicationScans(without).forall { scan =>
        (withIt.tasksReading(scan), withoutIt.tasksReading(scan)) match {
          case (Some(tasks), Some(tasksWithout)) => tasks >= tasksWithout
          case _ => false
        }
      }

  // The leaves of an application side, but not those of the creation side of a semi join in it.
  // That semi join may still be judged, or may have gone, so the plans of the costs can lack them.
  private def applicationScans(side: SparkPlan): Seq[SparkPlan] = side match {
    case PerPartitionSemiJoin(_, join, _) => applicationScans(join.left)
    case leaf if leaf.children.isEmpty => Seq(leaf)
    case node => node.children.flatMap(applicationScans)
  }

  // The expected number of items of each bloom filter of the semi join, and its number of bits when
  // the creation side has no row count. Each is an even share of the creation side's over its
  // partition keys. None when the creation side has fewer partition keys than it needs for each
  // share to be within the size threshold, or reports none.
  private def sizeOf(
      join: ShuffledHashJoinExec,
      bloomFilter: Attribute): Option[(Long, Option[Long])] = {
    val sizing = sizingOf(bloomFilter)
    partitionKeysOf(join.right).filter(_ >= sizing.minPartitionKeys).map { keys =>
      def share(n: Long): Long = Math.ceilDiv(n, keys).max(1L)
      (share(sizing.numItems), sizing.numBits.map(share))
    }
  }

  // The number of partition keys of the input of the aggregate `plan`, before any exchange.
  private def partitionKeysOf(plan: SparkPlan): Option[Long] = plan match {
    case aggregate: BaseAggregateExec => partitionKeysOf(aggregate.child)
    case input => PartitioningCollection.representativeOf(input.outputPartitioning).map { k =>
      (if (k.isGrouped) k.numPartitions else k.partitionKeys.distinct.size).toLong
    }
  }

  // The semi join with each bloom filter of its aggregate expecting `numItems` items, with
  // `numBits` bits when set, or else the number of bits those items need.
  private def withSize(
      join: ShuffledHashJoinExec,
      numItems: Long,
      numBits: Option[Long]): SparkPlan = {
    def sized(plan: SparkPlan): SparkPlan = plan match {
      case aggregate: BaseAggregateExec =>
        aggregate.transformExpressions {
          case b: BloomFilterAggregate =>
            val size = numBits.fold(new BloomFilterAggregate(b.child, numItems)) { bits =>
              new BloomFilterAggregate(b.child, Literal(numItems), Literal(bits))
            }
            b.withNewChildren(size.children)
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

  // The projection `p` over `without`, the application side of a semi join that goes. `p` is the
  // projection that restored the output of the semi join, or a projection over that one. When
  // `without` ends in a projection of its columns, the two become one. Without the semi join,
  // physical planning makes them one as well. So that side has the plan it has without
  // per-partition bloom filters, as a DPP filter that reuses a broadcast of it expects. Over a
  // columnar scan of just the columns `p` keeps, physical planning adds no projection at all.
  // There `p` still stays.
  private def withoutRestoringProjection(p: ProjectExec, without: SparkPlan): SparkPlan =
    without match {
      case ProjectExec(columns, child) if columns.forall(_.isInstanceOf[Attribute]) =>
        p.withNewChildren(Seq(child))
      case _ => p.withNewChildren(Seq(without))
    }
}
