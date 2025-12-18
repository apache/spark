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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.RowOrdering
import org.apache.spark.sql.catalyst.plans.physical.{KeyGroupedPartitioning, KeyGroupedShuffleSpec}
import org.apache.spark.sql.catalyst.util.InternalRowComparableWrapper
import org.apache.spark.sql.execution.joins.StoragePartitionJoinParams

/** Base trait for a data source scan capable of producing a key-grouped output. */
trait KeyGroupedPartitionedScan[T] {
  /**
   * The output partitioning of this scan after applying any pushed-down SPJ parameters.
   *
   * @param basePartitioning  The original key-grouped partitioning of the scan.
   * @param spjParams         SPJ parameters for the scan.
   */
  def getOutputKeyGroupedPartitioning(
      basePartitioning: KeyGroupedPartitioning,
      spjParams: StoragePartitionJoinParams): KeyGroupedPartitioning = {
    val expressions = spjParams.joinKeyPositions match {
      case Some(projectionPositions) =>
        projectionPositions.map(i => basePartitioning.expressions(i))
      case _ => basePartitioning.expressions
    }

    val newPartValues = spjParams.commonPartitionValues match {
      case Some(commonPartValues) =>
        // We allow duplicated partition values if
        // `spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled` is true
         commonPartValues.flatMap {
           case (partValue, numSplits) => Seq.fill(numSplits)(partValue)
         }
      case None =>
        spjParams.joinKeyPositions match {
          case Some(projectionPositions) =>
            val internalRowComparableWrapperFactory =
              InternalRowComparableWrapper.getInternalRowComparableWrapperFactory(
                expressions.map(_.dataType))
            basePartitioning.partitionValues.map { r =>
            val projectedRow = KeyGroupedPartitioning.project(expressions,
              projectionPositions, r)
            internalRowComparableWrapperFactory(projectedRow)
          }.distinct.map(_.row)
          case _ => basePartitioning.partitionValues
        }
    }
    basePartitioning.copy(expressions = expressions, numPartitions = newPartValues.length,
      partitionValues = newPartValues)
  }

  /**
   * Re-groups the input partitions for this scan based on the provided SPJ params, returning a list
   * of partitions to be scanned by each scan task.
   *
   * @param p                      The output KeyGroupedPartitioning of this scan.
   * @param spjParams              SPJ parameters for the scan.
   * @param filteredPartitions     The input partitions (after applying filtering) to be
   *                               re-grouped for this scan, initially grouped by partition value.
   * @param partitionValueAccessor Accessor for the partition values (as an [[InternalRow]])
   */
  def getInputPartitionGrouping(
      p: KeyGroupedPartitioning,
      spjParams: StoragePartitionJoinParams,
      filteredPartitions: Seq[Seq[T]],
      partitionValueAccessor: T => InternalRow): Seq[Seq[T]] = {
    assert(spjParams.keyGroupedPartitioning.isDefined)
    val expressions = spjParams.keyGroupedPartitioning.get

    // Re-group the input partitions if we are projecting on a subset of join keys
    val (groupedPartitions, partExpressions) = spjParams.joinKeyPositions match {
      case Some(projectPositions) =>
        val projectedExpressions = projectPositions.map(i => expressions(i))
        val internalRowComparableWrapperFactory =
          InternalRowComparableWrapper.getInternalRowComparableWrapperFactory(
            projectedExpressions.map(_.dataType))
        val parts = filteredPartitions.flatten.groupBy(part => {
          val row = partitionValueAccessor(part)
          val projectedRow = KeyGroupedPartitioning.project(
            expressions, projectPositions, row)
          internalRowComparableWrapperFactory(projectedRow)
        }).map { case (wrapper, splits) => (wrapper.row, splits) }.toSeq
        (parts, projectedExpressions)
      case _ =>
        val groupedParts = filteredPartitions.map(splits => {
          assert(splits.nonEmpty)
          (partitionValueAccessor(splits.head), splits)
        })
        (groupedParts, expressions)
    }

    // Also re-group the partitions if we are reducing compatible partition expressions
    val partitionDataTypes = partExpressions.map(_.dataType)
    val internalRowComparableWrapperFactory =
      InternalRowComparableWrapper.getInternalRowComparableWrapperFactory(partitionDataTypes)
    val finalGroupedPartitions = spjParams.reducers match {
      case Some(reducers) =>
        val result = groupedPartitions.groupBy { case (row, _) =>
          KeyGroupedShuffleSpec.reducePartitionValue(
            row, reducers, partitionDataTypes, internalRowComparableWrapperFactory)
        }.map { case (wrapper, splits) => (wrapper.row, splits.flatMap(_._2)) }.toSeq
        val rowOrdering = RowOrdering.createNaturalAscendingOrdering(
          partExpressions.map(_.dataType))
        result.sorted(rowOrdering.on((t: (InternalRow, _)) => t._1))
      case _ => groupedPartitions
    }

    // When partially clustered, the input partitions are not grouped by partition
    // values. Here we'll need to check `commonPartitionValues` and decide how to group
    // and replicate splits within a partition.
    if (spjParams.commonPartitionValues.isDefined && spjParams.applyPartialClustering) {
      // A mapping from the common partition values to how many splits the partition
      // should contain.
      val commonPartValuesMap = spjParams.commonPartitionValues
          .get
          .map(t => (internalRowComparableWrapperFactory(t._1), t._2))
          .toMap
      val filteredGroupedPartitions = finalGroupedPartitions.filter {
        case (partValues, _) =>
         commonPartValuesMap.keySet.contains(internalRowComparableWrapperFactory(partValues))
      }
      val nestGroupedPartitions = filteredGroupedPartitions.map { case (partValue, splits) =>
        // `commonPartValuesMap` should contain the part value since it's the super set.
        val numSplits = commonPartValuesMap.get(internalRowComparableWrapperFactory(partValue))
        assert(numSplits.isDefined, s"Partition value $partValue does not exist in " +
            "common partition values from Spark plan")

        val newSplits = if (spjParams.replicatePartitions) {
          // We need to also replicate partitions according to the other side of join
          Seq.fill(numSplits.get)(splits)
        } else {
          // Not grouping by partition values: this could be the side with partially
          // clustered distribution. Because of dynamic filtering, we'll need to check if
          // the final number of splits of a partition is smaller than the original
          // number, and fill with empty splits if so. This is necessary so that both
          // sides of a join will have the same number of partitions & splits.
          splits.map(Seq(_)).padTo(numSplits.get, Seq.empty)
        }
        (internalRowComparableWrapperFactory(partValue), newSplits)
      }

      // Now fill missing partition keys with empty partitions
      val partitionMapping = nestGroupedPartitions.toMap
      spjParams.commonPartitionValues.get.flatMap {
        case (partValue, numSplits) =>
          // Use empty partition for those partition values that are not present.
          partitionMapping.getOrElse(
            internalRowComparableWrapperFactory(partValue),
            Seq.fill(numSplits)(Seq.empty))
      }
    } else {
      // either `commonPartitionValues` is not defined, or it is defined but
      // `applyPartialClustering` is false.
      val partitionMapping = finalGroupedPartitions.map { case (partValue, splits) =>
        internalRowComparableWrapperFactory(partValue) -> splits
      }.toMap

      // In case `commonPartitionValues` is not defined (e.g., SPJ is not used), there
      // could exist duplicated partition values, as partition grouping is not done
      // at the beginning and postponed to this method. It is important to use unique
      // partition values here so that grouped partitions won't get duplicated.
      p.uniquePartitionValues.map { partValue =>
        // Use empty partition for those partition values that are not present
        partitionMapping.getOrElse(internalRowComparableWrapperFactory(partValue), Seq.empty)
      }
    }
  }
}
