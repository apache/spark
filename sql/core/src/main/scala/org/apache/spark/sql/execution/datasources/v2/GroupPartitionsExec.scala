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
            val projectedExpressions = projectExpressions(k.expressions)
            val projectedDataTypes = projectedExpressions.map(_.dataType)
            k.copy(expressions = projectedExpressions,
              partitionKeys = groupedPartitions.map(_._1),
              originalPartitionKeys = projectKeys(k.originalPartitionKeys, projectedDataTypes))
        }.asInstanceOf[Partitioning]
      case o => o
    }
  }

  private def projectExpressions(expressions: Seq[Expression]) = {
    joinKeyPositions match {
      case Some(projectionPositions) =>
        projectionPositions.map(expressions)
      case _ => expressions
    }
  }

  private def projectKeys(keys: Seq[InternalRow], dataTypes: Seq[DataType]) = {
    joinKeyPositions match {
      case Some(projectionPositions) =>
        keys.map(KeyedPartitioning.projectKey(_, projectionPositions, dataTypes))
      case _ => keys
    }
  }

  /**
   * Extracts the first KeyedPartitioning from the child's output partitioning.
   * The child must have a KeyedPartitioning in its partitioning scheme.
   */
  lazy val firstKeyedPartitioning = {
    child.outputPartitioning.asInstanceOf[Partitioning with Expression].collectFirst {
      case k: KeyedPartitioning => k
    }.getOrElse(
      throw new SparkException("GroupPartitionsExec requires a child with KeyedPartitioning"))
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
  lazy val groupedPartitions: Seq[(InternalRow, Seq[Int])] = {
    // Also sort the input partitions according to their partition key order. This ensures
    // a canonical order from both sides of a bucketed join, for example.

    val (projectedDataTypes, projectedKeys) =
      joinKeyPositions match {
        case Some(projectionPositions) =>
          val projectedDataTypes =
            projectExpressions(firstKeyedPartitioning.expressions).map(_.dataType)
          val projectedKeys = firstKeyedPartitioning.partitionKeys
            .map(KeyedPartitioning.projectKey(_, projectionPositions, projectedDataTypes))
          (projectedDataTypes, projectedKeys)

        case _ =>
          val dataTypes = firstKeyedPartitioning.expressions.map(_.dataType)
          (dataTypes, firstKeyedPartitioning.partitionKeys)
      }

    val reducedKeys = reducers match {
      case Some(reducers) =>
        projectedKeys.map(KeyedPartitioning.reduceKey(_, reducers, projectedDataTypes))
      case _ => projectedKeys
    }

    val internalRowComparableWrapperFactory =
      InternalRowComparableWrapper.getInternalRowComparableWrapperFactory(projectedDataTypes)

    val map = reducedKeys.zipWithIndex.groupMap {
      case (key, _) => internalRowComparableWrapperFactory(key)
    }(_._2)

    // When partially clustered, the input partitions are not grouped by partition
    // values. Here we'll need to check `commonPartitionKeys` and decide how to group
    // and replicate splits within a partition.
    if (commonPartitionKeys.isDefined) {
      commonPartitionKeys.get.flatMap { case (key, numSplits) =>
        val splits = map.getOrElse(internalRowComparableWrapperFactory(key), Seq.empty)
        if (applyPartialClustering && !replicatePartitions) {
          val paddedSplits = splits.map(Seq(_)).padTo(numSplits, Seq.empty)
          paddedSplits.map((key, _))
        } else {
          Seq.fill(numSplits)((key, splits))
        }
      }
    } else {
      val rowOrdering = RowOrdering.createNaturalAscendingOrdering(projectedDataTypes)
      map.toSeq
        .map { case (keyWrapper, v) => (keyWrapper.row, v) }
        .sorted(rowOrdering.on((t: (InternalRow, _)) => t._1))
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
