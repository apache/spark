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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Partition, SparkException}
import org.apache.spark.rdd.{CoalescedRDD, PartitionCoalescer, PartitionGroup, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{KeyedPartitioning, Partitioning}
import org.apache.spark.sql.catalyst.util.InternalRowComparableWrapper
import org.apache.spark.sql.connector.catalog.functions.Reducer
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.DataType

/**
 * Physical operator that groups input partitions by their partition keys.
 *
 * This operator is used to coalesce partitions from bucketed/partitioned data sources
 * where multiple input partitions share the same partition key. It's commonly used in
 * storage-partitioned joins to align partitions from different sides of the join.
 *
 * @param child The child plan providing bucketed/partitioned input
 * @param joinKeyPositions Optional projection to select a subset of the partitioning key
 *                         for join compatibility (e.g., when join keys are a subset of
 *                         partition keys)
 * @param commonPartitionKeys Optional sequence of expected partition key values and their
 *                              split counts, used for partially clustered data
 * @param reducers Optional reducers to apply to partition keys for grouping compatibility
 * @param applyPartialClustering Whether to apply partial clustering for skewed data
 * @param replicatePartitions Whether to replicate partitions across multiple keys
 */
case class GroupPartitionsExec(
    child: SparkPlan,
    joinKeyPositions: Option[Seq[Int]] = None,
    commonPartitionKeys: Option[Seq[(InternalRow, Int)]] = None,
    reducers: Option[Seq[Option[Reducer[_, _]]]] = None,
    applyPartialClustering: Boolean = false,
    replicatePartitions: Boolean = false
  ) extends UnaryExecNode {

  override def outputPartitioning: Partitioning = {
    child.outputPartitioning match {
      case p: Partitioning with Expression =>
        p.transform {
          case k: KeyedPartitioning =>
            // There can be multiple `KeyedPartitioning` in an output partitioning of a join, but
            // they can only differ in `expressions`. `partitionKeys` must match so we can calculate
            // it only once via `groupedPartitions`.
            val projectedExpressions = joinKeyPositions.fold(k.expressions)(_.map(k.expressions))
            k.copy(expressions = projectedExpressions, partitionKeys = groupedPartitions.map(_._1))
        }.asInstanceOf[Partitioning]
      case o => o
    }
  }

  /**
   * Distributes partitions based on `commonPartitionKeys` and clustering mode.
   */
  private def distributeByCommonKeys(
      keyWrapperMap: Map[InternalRowComparableWrapper, Seq[Int]],
      comparableKeyWrapperFactory: InternalRow => InternalRowComparableWrapper
  ): Seq[(InternalRow, Seq[Int])] = {
    commonPartitionKeys.get.flatMap { case (key, numSplits) =>
      val splits = keyWrapperMap.getOrElse(comparableKeyWrapperFactory(key), Seq.empty)
      if (applyPartialClustering && !replicatePartitions) {
        // Distribute splits across expected partitions, padding with empty sequences
        val paddedSplits = splits.map(Seq(_)).padTo(numSplits, Seq.empty)
        paddedSplits.map((key, _))
      } else {
        // Replicate all splits to each expected partition
        Seq.fill(numSplits)((key, splits))
      }
    }
  }

  /**
   * Groups and sorts partitions by their keys in ascending order.
   */
  private def groupAndSortByKeys(
      keyWrapperMap: Map[InternalRowComparableWrapper, Seq[Int]],
      dataTypes: Seq[DataType]
  ): Seq[(InternalRow, Seq[Int])] = {
    val rowOrdering = RowOrdering.createNaturalAscendingOrdering(dataTypes)
    keyWrapperMap.toSeq
      .map { case (keyWrapper, indices) => (keyWrapper.row, indices) }
      .sorted(rowOrdering.on((t: (InternalRow, _)) => t._1))
  }

  /**
   * Computes the grouped partitions by:
   * 1. Projecting partition keys if joinKeyPositions is specified
   * 2. Reducing keys if reducers are specified
   * 3. Grouping input partition indices by their (possibly projected/reduced) keys
   * 4. Sorting or distributing based on whether partial clustering is enabled
   *
   * Returns a sequence of (partitionKey, inputPartitionIndices) pairs representing
   * how input partitions should be grouped together.
   */
  lazy val groupedPartitions = {
    // There must be a `KeyedPartitioning` in child's output partitioning as a
    // `GroupPartitionsExec` node is added to a plan only in that case.
    val keyedPartitioning = child.outputPartitioning
      .asInstanceOf[Partitioning with Expression]
      .collectFirst { case k: KeyedPartitioning => k }
      .getOrElse(
        throw new SparkException("GroupPartitionsExec requires a child with KeyedPartitioning"))

    // Project partition keys if join key positions are specified
    val (projectedDataTypes, projectedKeys) =
      joinKeyPositions.fold(
        (keyedPartitioning.expressionDataTypes, keyedPartitioning.partitionKeys)
      )(keyedPartitioning.projectKeys)

    // Reduce keys if reducers are specified
    val reducedKeys = reducers.fold(projectedKeys)(
      KeyedPartitioning.reduceKeys(projectedKeys, projectedDataTypes, _))

    // Create map from partition keys to their indices
    val comparableKeyWrapperFactory =
      InternalRowComparableWrapper.getInternalRowComparableWrapperFactory(projectedDataTypes)

    val keyWrapperToPartitionIndices = reducedKeys.zipWithIndex.groupMap {
      case (key, _) => comparableKeyWrapperFactory(key)
    }(_._2)

    if (commonPartitionKeys.isDefined) {
      distributeByCommonKeys(keyWrapperToPartitionIndices, comparableKeyWrapperFactory)
    } else {
      groupAndSortByKeys(keyWrapperToPartitionIndices, projectedDataTypes)
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val partitionCoalescer = new GroupedPartitionCoalescer(groupedPartitions.map(_._2))
    if (groupedPartitions.isEmpty) {
      sparkContext.emptyRDD
    } else {
      new CoalescedRDD(child.execute(), groupedPartitions.size, Some(partitionCoalescer))
    }
  }

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  override def outputOrdering: Seq[SortOrder] = {
    // when multiple partitions are grouped together, ordering inside partitions is not preserved
    if (groupedPartitions.forall(_._2.size <= 1)) {
      child.outputOrdering
    } else {
      super.outputOrdering
    }
  }
}

/**
 * A PartitionCoalescer that groups partitions according to a pre-computed grouping plan.
 *
 * Unlike Spark's default coalescer which tries to minimize data movement, this coalescer
 * groups partitions based on their partition keys to maintain the grouping semantics
 * required for storage-partitioned operations.
 *
 * @param groupedPartitions Sequence where each element is a sequence of input partition
 *                         indices that should be grouped together
 */
class GroupedPartitionCoalescer(
    val groupedPartitions: Seq[Seq[Int]]
  ) extends PartitionCoalescer with Serializable {

  override def coalesce(maxPartitions: Int, parent: RDD[_]): Array[PartitionGroup] = {
    groupedPartitions.map { partitionIndices =>
      val partitions = new ArrayBuffer[Partition](partitionIndices.size)
      val preferredLocations = new ArrayBuffer[String](partitionIndices.size)
      partitionIndices.foreach { partitionIndex =>
        val partition = parent.partitions(partitionIndex)
        partitions += partition
        preferredLocations ++= parent.preferredLocations(partition)
      }
      // Select the most common location as the preferred location
      val preferredLocation = preferredLocations
        .groupBy(identity)
        .view.mapValues(_.size)
        .maxByOption(_._2)
        .map(_._1)
      val partitionGroup = new PartitionGroup(preferredLocation)
      partitionGroup.partitions ++= partitions
      partitionGroup
    }.toArray
  }
}
