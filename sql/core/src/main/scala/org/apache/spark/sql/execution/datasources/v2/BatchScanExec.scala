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

import com.google.common.base.Objects

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{KeyGroupedPartitioning, KeyGroupedShuffleSpec, Partitioning, SinglePartition}
import org.apache.spark.sql.catalyst.util.{truncatedString, InternalRowComparableWrapper}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.functions.Reducer
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.util.ArrayImplicits._

/**
 * Physical plan node for scanning a batch of data from a data source v2.
 */
case class BatchScanExec(
    output: Seq[AttributeReference],
    @transient scan: Scan,
    runtimeFilters: Seq[Expression],
    ordering: Option[Seq[SortOrder]] = None,
    @transient table: Table,
    spjParams: StoragePartitionJoinParams = StoragePartitionJoinParams()
  ) extends DataSourceV2ScanExecBase {

  // What this node plans from `scan`, see [[PlannedScan]]. `makeCopy` and `clone` hand it over to a
  // copy over the same `Scan`. Any other new node plans its own, including one the case-class
  // `copy` makes.
  @transient private var plannedScan: PlannedScan =
    if (scan == null) null else new PlannedScan(scan)

  def batch: Batch = if (scan == null) null else plannedScan.batch

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: BatchScanExec =>
      this.batch != null && this.batch == other.batch &&
          this.runtimeFilters == other.runtimeFilters &&
          this.spjParams == other.spjParams
    case _ =>
      false
  }

  override def hashCode(): Int = Objects.hashCode(batch, runtimeFilters)

  override def inputPartitions: Seq[InputPartition] = plannedScan.inputPartitions

  @transient private lazy val filteredPartitions: Seq[Seq[InputPartition]] = {
    val dataSourceFilters = runtimeFilters.flatMap {
      case DynamicPruningExpression(e) => DataSourceV2Strategy.translateRuntimeFilterV2(e)
      case _ => None
    }

    if (dataSourceFilters.nonEmpty) {
      // The partitions reported by the data source after filtering are compared below by their
      // full partition keys, so the partitioning they are compared against must not be the
      // projected one: `outputPartitioning` projects the partition keys down to the join keys and
      // replaces the partition values with the common ones of the join when SPJ params were pushed
      // down. Projecting and re-grouping the filtered partitions happens later, in `inputRDD`.
      val originalPartitioning = super.outputPartitioning

      // the cast is safe as runtime filters are only assigned if the scan can be filtered
      val filterableScan = scan.asInstanceOf[SupportsRuntimeV2Filtering]
      filterableScan.filter(dataSourceFilters.toArray)

      // call toBatch again to get filtered partitions
      val newPartitions = scan.toBatch.planInputPartitions()

      originalPartitioning match {
        case p: KeyGroupedPartitioning =>
          if (newPartitions.exists(!_.isInstanceOf[HasPartitionKey])) {
            throw new SparkException("Data source must have preserved the original partitioning " +
                "during runtime filtering: not all partitions implement HasPartitionKey after " +
                "filtering")
          }
          val newPartitionValues = newPartitions.map(partition =>
              InternalRowComparableWrapper(partition.asInstanceOf[HasPartitionKey], p.expressions))
            .toSet
          val oldPartitionValues = p.partitionValues
            .map(partition => InternalRowComparableWrapper(partition, p.expressions)).toSet
          // We require the new number of partition values to be equal or less than the old number
          // of partition values here. In the case of less than, empty partitions will be added for
          // those missing values that are not present in the new input partitions.
          if (oldPartitionValues.size < newPartitionValues.size) {
            throw new SparkException("During runtime filtering, data source must either report " +
                "the same number of partition values, or a subset of partition values from the " +
                s"original. Before: ${oldPartitionValues.size} partition values. " +
                s"After: ${newPartitionValues.size} partition values")
          }

          if (!newPartitionValues.forall(oldPartitionValues.contains)) {
            throw new SparkException("During runtime filtering, data source must not report new " +
                "partition values that are not present in the original partitioning.")
          }

          groupPartitions(newPartitions.toImmutableArraySeq)
            .map(_.groupedParts.map(_.parts)).getOrElse(Seq.empty)

        case _ =>
          // no validation is needed as the data source did not report any specific partitioning
          newPartitions.map(Seq(_)).toImmutableArraySeq
      }

    } else {
      partitions
    }
  }

  override def outputPartitioning: Partitioning = {
    super.outputPartitioning match {
      case k: KeyGroupedPartitioning =>
        val projectedExpressions = spjParams.joinKeyPositions match {
          case Some(projectionPositions) => projectionPositions.map(i => k.expressions(i))
          case _ => k.expressions
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
              case Some(projectionPositions) => k.partitionValues.map{r =>
                val projectedRow = KeyGroupedPartitioning.project(k.expressions,
                  projectionPositions, r)
                InternalRowComparableWrapper(projectedRow, projectedExpressions)
              }.distinct.map(_.row)
              case _ => k.partitionValues
            }
        }
        k.copy(expressions = projectedExpressions, numPartitions = newPartValues.length,
          partitionValues = newPartValues,
          originalPartitionValues = spjParams.joinKeyPositions match {
            case Some(projectionPositions) => k.originalPartitionValues.map(
              KeyGroupedPartitioning.project(k.expressions, projectionPositions, _))
            case None => k.originalPartitionValues
          },
          isPartiallyClustered = spjParams.applyPartialClustering)
      case p => p
    }
  }

  override def readerFactory: PartitionReaderFactory = plannedScan.readerFactory

  override lazy val inputRDD: RDD[InternalRow] = {
    val rdd = if (filteredPartitions.isEmpty && outputPartitioning == SinglePartition) {
      // return an empty RDD with 1 partition if dynamic filtering removed the only split
      sparkContext.parallelize(Array.empty[InternalRow].toImmutableArraySeq, 1)
    } else {
      val finalPartitions = outputPartitioning match {
        case p: KeyGroupedPartitioning =>
          assert(spjParams.keyGroupedPartitioning.isDefined)
          val expressions = spjParams.keyGroupedPartitioning.get

          // Re-group the input partitions if we are projecting on a subset of join keys
          val (groupedPartitions, partExpressions) = spjParams.joinKeyPositions match {
            case Some(projectPositions) =>
              val projectedExpressions = projectPositions.map(i => expressions(i))
              val parts = filteredPartitions.flatten.groupBy(part => {
                val row = part.asInstanceOf[HasPartitionKey].partitionKey()
                val projectedRow = KeyGroupedPartitioning.project(
                  expressions, projectPositions, row)
                InternalRowComparableWrapper(projectedRow, projectedExpressions)
              }).map { case (wrapper, splits) => (wrapper.row, splits) }.toSeq
              (parts, projectedExpressions)
            case _ =>
              val groupedParts = filteredPartitions.map(splits => {
                assert(splits.nonEmpty && splits.head.isInstanceOf[HasPartitionKey])
                (splits.head.asInstanceOf[HasPartitionKey].partitionKey(), splits)
              })
              (groupedParts, expressions)
          }

          // Also re-group the partitions if we are reducing compatible partition expressions
          val finalGroupedPartitions = spjParams.reducers match {
            case Some(reducers) =>
              val result = groupedPartitions.groupBy { case (row, _) =>
                KeyGroupedShuffleSpec.reducePartitionValue(row, partExpressions, reducers)
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
                .map(t => (InternalRowComparableWrapper(t._1, partExpressions), t._2))
                .toMap
            val filteredGroupedPartitions = finalGroupedPartitions.filter {
              case (partValues, _) =>
               commonPartValuesMap.keySet.contains(
                InternalRowComparableWrapper(partValues, partExpressions))
            }
            val nestGroupedPartitions = filteredGroupedPartitions.map { case (partValue, splits) =>
              // `commonPartValuesMap` should contain the part value since it's the super set.
              val numSplits = commonPartValuesMap
                  .get(InternalRowComparableWrapper(partValue, partExpressions))
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
              (InternalRowComparableWrapper(partValue, partExpressions), newSplits)
            }

            // Now fill missing partition keys with empty partitions
            val partitionMapping = nestGroupedPartitions.toMap
            spjParams.commonPartitionValues.get.flatMap {
              case (partValue, numSplits) =>
                // Use empty partition for those partition values that are not present.
                partitionMapping.getOrElse(
                  InternalRowComparableWrapper(partValue, partExpressions),
                  Seq.fill(numSplits)(Seq.empty))
            }
          } else {
            // either `commonPartitionValues` is not defined, or it is defined but
            // `applyPartialClustering` is false.
            val partitionMapping = finalGroupedPartitions.map { case (partValue, splits) =>
              InternalRowComparableWrapper(partValue, partExpressions) -> splits
            }.toMap

            // In case `commonPartitionValues` is not defined (e.g., SPJ is not used), there
            // could exist duplicated partition values, as partition grouping is not done
            // at the beginning and postponed to this method. It is important to use unique
            // partition values here so that grouped partitions won't get duplicated.
            p.uniquePartitionValues.map { partValue =>
              // Use empty partition for those partition values that are not present
              partitionMapping.getOrElse(
                InternalRowComparableWrapper(partValue, partExpressions), Seq.empty)
            }
          }

        case _ => filteredPartitions
      }

      new DataSourceRDD(
        sparkContext, finalPartitions, readerFactory, supportsColumnar, customMetrics)
    }
    postDriverMetrics()
    rdd
  }

  override def keyGroupedPartitioning: Option[Seq[Expression]] =
    spjParams.keyGroupedPartitioning

  override def makeCopy(newArgs: Array[AnyRef]): SparkPlan = sharePlanning(super.makeCopy(newArgs))

  // `TreeNode.clone` does not go through `makeCopy`.
  override def clone(): SparkPlan = sharePlanning(super.clone())

  private def sharePlanning(node: SparkPlan): SparkPlan = {
    node match {
      case c: BatchScanExec if c.scan eq scan => c.plannedScan = plannedScan
      case _ =>
    }
    node
  }

  override def doCanonicalize(): BatchScanExec = {
    this.copy(
      output = output.map(QueryPlan.normalizeExpressions(_, output)),
      runtimeFilters = QueryPlan.normalizePredicates(
        runtimeFilters.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral)),
        output))
  }

  override def simpleString(maxFields: Int): String = {
    val truncatedOutputString = truncatedString(output, "[", ", ", "]", maxFields)
    val runtimeFiltersString = s"RuntimeFilters: ${runtimeFilters.mkString("[", ",", "]")}"
    val result = s"$nodeName$truncatedOutputString ${scan.description()} $runtimeFiltersString"
    redact(result)
  }

  override def nodeName: String = {
    s"BatchScan ${table.name()}".trim
  }
}

case class StoragePartitionJoinParams(
    keyGroupedPartitioning: Option[Seq[Expression]] = None,
    joinKeyPositions: Option[Seq[Int]] = None,
    commonPartitionValues: Option[Seq[(InternalRow, Int)]] = None,
    reducers: Option[Seq[Option[Reducer[_, _]]]] = None,
    applyPartialClustering: Boolean = false,
    replicatePartitions: Boolean = false) {
  override def equals(other: Any): Boolean = other match {
    case other: StoragePartitionJoinParams =>
      this.commonPartitionValues == other.commonPartitionValues &&
      this.replicatePartitions == other.replicatePartitions &&
      this.applyPartialClustering == other.applyPartialClustering &&
      this.joinKeyPositions == other.joinKeyPositions
    case _ =>
      false
  }

  override def hashCode(): Int = Objects.hashCode(
    joinKeyPositions: Option[Seq[Int]],
    commonPartitionValues: Option[Seq[(InternalRow, Int)]],
    applyPartialClustering: java.lang.Boolean,
    replicatePartitions: java.lang.Boolean)
}

/**
 * A `Scan`, with the `Batch` and the input partitions a [[BatchScanExec]] plans from it once, when
 * a node first asks for them. The node hands it over to the copies `makeCopy` and `clone` make of
 * it, so they read the partitions the first node planned, with a reader factory from the `Batch`
 * that planned them. The partitions have to be the planned ones, because the `Scan` can be shared
 * with another node whose runtime filter prunes it first, e.g. the build side of a DPP subquery
 * (SPARK-60752). A pushed runtime filter replans them from a new `Batch`, but keeps the planned
 * `Batch`'s reader factory.
 */
private[v2] final class PlannedScan(val scan: Scan) {
  lazy val batch: Batch = scan.toBatch
  lazy val inputPartitions: Seq[InputPartition] = batch.planInputPartitions().toImmutableArraySeq
  lazy val readerFactory: PartitionReaderFactory = batch.createReaderFactory()
}
