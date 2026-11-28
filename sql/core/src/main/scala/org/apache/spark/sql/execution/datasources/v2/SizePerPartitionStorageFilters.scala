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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.expressions.{ExprId, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.optimizer.{PerPartitionRuntimeFilterSubquery, PerPartitionSizing}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, PartitioningCollection, REPLICATED_FOR_JOIN, SPLIT_FOR_JOIN}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.PER_PARTITION_RUNTIME_FILTER_SUBQUERY
import org.apache.spark.sql.execution.{SnapshotUnionPreparationConf, SparkPlan, UnionConfSnapshot}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, Exchange}

/**
 * Sizes the per-partition bloom filters of each [[BatchScanExec]] that has one. It keeps them only
 * where a task reads all the splits of its keys, or where partially clustered distribution
 * spreads or repeats them.
 *
 * Each bloom filter expects an even share of the creation side's items over its partition keys.
 * Without a row count, it also gets an even share of the creation side's bits. The scan is planned
 * without them when the creation side reports no partition keys, or too few for each share to be
 * within the size threshold of a bloom filter's input. An aggregate this rule keeps reads a
 * creation side that reports its partition keys, so `EnsureRequirements` puts no exchange below it.
 * Rules that look for exchanges at the leaves of a plan rely on that, such as
 * `CoalesceShufflePartitions`.
 *
 * Each task reads the partitions of the aggregate with its splits' keys, and the creation side
 * under them. So the scan keeps its bloom filters where a task reads all the splits of its keys:
 *  - when the scan reports one split for each key;
 *  - when a `GroupPartitionsExec` above the scan groups them with no exchange in between, as below
 *    a storage-partitioned join.
 * Partially clustered distribution spreads or repeats the splits of a key over tasks, and the scan
 * keeps them there too. Turning it on accepts that each of those tasks reads one side's splits of
 * the key again. The aggregate is read again in each of them too. Under a join that shuffles a
 * scan with several splits for a key, a task reads only some of them, so the scan reads without
 * its bloom filters. To tell, this asks `ensureRequirements`, the one the preparation plans with,
 * how it would plan the query. That planning asks a `UnionExec` what it reports, so this first
 * records `unionConf` on each union, as the preparation does ahead of `EnsureRequirements`.
 *
 * A `PerPartitionRuntimeFilterSubquery` that no scan took gives way to `true` here, since it cannot
 * be evaluated. The join above it checks its rows anyway. `CleanupPerPartitionRuntimeFilters`
 * keeps only those the V2 scan strategy takes, so one is left when, for instance, an optimizer
 * rule a user added moved it away from its scan.
 */
case class SizePerPartitionStorageFilters(
    ensureRequirements: EnsureRequirements,
    unionConf: UnionConfSnapshot) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    val sized = plan.transformUp {
      case scan @ BatchScanExec(_, _, _, _, _, _, _, _, Some(filter), Some(aggregate)) =>
        sizeOf(aggregate, filter.sizing) match {
          case Some((numItems, numBits)) =>
            scan.withNewChildren(Seq(withSize(aggregate, numItems, numBits)))
          case None => scan.withoutPerPartitionBloomFilter
        }
    }
    readOnce(sized).transformAllExpressionsWithPruning(
        _.containsPattern(PER_PARTITION_RUNTIME_FILTER_SUBQUERY)) {
      case _: PerPartitionRuntimeFilterSubquery => Literal.TrueLiteral
    }
  }

  // The expected number of items of each bloom filter of `aggregate`, and its number of bits when
  // the creation side has no row count. Each is an even share of the creation side's over its
  // partition keys. None when the creation side has fewer partition keys than it needs for each
  // share to be within the size threshold, or reports none.
  private def sizeOf(
      aggregate: SparkPlan,
      sizing: PerPartitionSizing): Option[(Long, Option[Long])] = {
    partitionKeysOf(aggregate).filter(_ >= sizing.minPartitionKeys).map { keys =>
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

  // `plan` with each bloom filter of its aggregates expecting `numItems` items, with `numBits` bits
  // when set, or else the number of bits those items need.
  private def withSize(plan: SparkPlan, numItems: Long, numBits: Option[Long]): SparkPlan =
    plan match {
      case aggregate: BaseAggregateExec =>
        aggregate.transformExpressions {
          case b: BloomFilterAggregate =>
            val size = numBits.fold(new BloomFilterAggregate(b.child, numItems)) { bits =>
              new BloomFilterAggregate(b.child, Literal(numItems), Literal(bits))
            }
            b.withNewChildren(size.children)
        }.withNewChildren(Seq(withSize(aggregate.child, numItems, numBits)))
      case other => other
    }

  // `plan` with the per-partition bloom filters of only those scans that `scansReadOnce` finds in
  // `plan` as `ensureRequirements` plans it. The bloom filters do not change that planning, so one
  // planning tells it for every scan.
  private def readOnce(plan: SparkPlan): SparkPlan = {
    def hasBloomFilters(p: SparkPlan): Boolean = p match {
      case scan: BatchScanExec => scan.perPartitionBloomFilter.isDefined
      case _ => false
    }
    if (!plan.exists(hasBloomFilters)) {
      plan
    } else {
      new SnapshotUnionPreparationConf(unionConf).apply(plan)
      val kept = scansReadOnce(ensureRequirements.apply(plan), readOnceAbove = false).toSet
      plan.transformUp {
        case scan: BatchScanExec
            if scan.perPartitionBloomFilter.exists(f => !kept(f.bloomFilter.exprId)) =>
          scan.withoutPerPartitionBloomFilter
      }
    }
  }

  // The bloom filter columns of the scans in `plan` whose tasks read all the splits of a key
  // together, or whose splits partially clustered distribution spreads or repeats.
  // `readOnceAbove` tells whether the nearest `GroupPartitionsExec` above `plan`, with no exchange
  // in between, does either for the partitions of `plan`. No other node splits a partition of its
  // child over tasks.
  private def scansReadOnce(plan: SparkPlan, readOnceAbove: Boolean): Seq[ExprId] = plan match {
    case scan: BatchScanExec =>
      scan.perPartitionBloomFilter
        .filter(_ => readOnceAbove || isGrouped(scan.outputPartitioning))
        .map(_.bloomFilter.exprId).toSeq
    case g: GroupPartitionsExec =>
      scansReadOnce(g.child, isGrouped(g.outputPartitioning) ||
        g.ungroupingOrigin.exists(o => o == SPLIT_FOR_JOIN || o == REPLICATED_FOR_JOIN))
    case _: Exchange => plan.children.flatMap(scansReadOnce(_, readOnceAbove = false))
    case _ => plan.children.flatMap(scansReadOnce(_, readOnceAbove))
  }

  // Whether `partitioning` puts each partition key in one partition only.
  private def isGrouped(partitioning: Partitioning): Boolean =
    PartitioningCollection.representativeOf(partitioning).exists(_.isGrouped)
}
