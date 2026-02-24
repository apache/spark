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

import java.util.Objects

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{KeyedPartitioning, SinglePartition}
import org.apache.spark.sql.catalyst.util.{truncatedString, InternalRowComparableWrapper}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read._
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
    keyGroupedPartitioning: Option[Seq[Expression]] = None
  ) extends DataSourceV2ScanExecBase {

  @transient lazy val batch: Batch = if (scan == null) null else scan.toBatch

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: BatchScanExec =>
      this.batch != null && this.batch == other.batch &&
          this.runtimeFilters == other.runtimeFilters &&
          this.keyGroupedPartitioning == other.keyGroupedPartitioning
    case _ =>
      false
  }

  override def hashCode(): Int = Objects.hash(batch, runtimeFilters)

  @transient override lazy val inputPartitions: Seq[InputPartition] =
    batch.planInputPartitions().toImmutableArraySeq

  @transient private lazy val filteredPartitions: Seq[Option[InputPartition]] = {
    val dataSourceFilters = runtimeFilters.flatMap {
      case DynamicPruningExpression(e) => DataSourceV2Strategy.translateRuntimeFilterV2(e)
      case _ => None
    }

    val originalPartitioning = outputPartitioning
    if (dataSourceFilters.nonEmpty) {
      // the cast is safe as runtime filters are only assigned if the scan can be filtered
      val filterableScan = scan.asInstanceOf[SupportsRuntimeV2Filtering]
      filterableScan.filter(dataSourceFilters.toArray)

      // call toBatch again to get filtered partitions
      val newPartitions = scan.toBatch.planInputPartitions()

      originalPartitioning match {
        case p: KeyedPartitioning =>
          if (newPartitions.exists(!_.isInstanceOf[HasPartitionKey])) {
            throw new SparkException("Data source must have preserved the original partitioning " +
                "during runtime filtering: not all partitions implement HasPartitionKey after " +
                "filtering")
          }
          val newPartitionKeys = newPartitions.map(partition =>
              InternalRowComparableWrapper(partition.asInstanceOf[HasPartitionKey], p.expressions))
            .toSet
          val oldPartitionKeys = p.partitionKeys
            .map(partition => InternalRowComparableWrapper(partition, p.expressions)).toSet
          // We require the new number of partition values to be equal or less than the old number
          // of partition values here. In the case of less than, empty partitions will be added for
          // those missing values that are not present in the new input partitions.
          if (oldPartitionKeys.size < newPartitionKeys.size) {
            throw new SparkException("During runtime filtering, data source must either report " +
                "the same number of partition values, or a subset of partition values from the " +
                s"original. Before: ${oldPartitionKeys.size} partition values. " +
                s"After: ${newPartitionKeys.size} partition values")
          }

          if (!newPartitionKeys.forall(oldPartitionKeys.contains)) {
            throw new SparkException("During runtime filtering, data source must not report new " +
                "partition values that are not present in the original partitioning.")
          }

          val dataTypes = p.expressions.map(_.dataType)
          val rowOrdering = RowOrdering.createNaturalAscendingOrdering(dataTypes)
          val internalRowComparableWrapperFactory =
            InternalRowComparableWrapper.getInternalRowComparableWrapperFactory(dataTypes)

          val inputMap = inputPartitions.groupBy(p =>
            internalRowComparableWrapperFactory(p.asInstanceOf[HasPartitionKey].partitionKey())
          ).view.mapValues(_.size)
          val filteredMap = newPartitions.groupBy(p =>
            internalRowComparableWrapperFactory(p.asInstanceOf[HasPartitionKey].partitionKey())
          )
          inputMap.toSeq
            .sortBy { case (keyWrapper, _) => keyWrapper.row }(rowOrdering)
            .flatMap { case (keyWrapper, size) =>
              val fps = filteredMap.getOrElse(keyWrapper, Array.empty)
              assert(fps.size <= size)
              fps.map(Some).padTo(size, None)
            }

        case _ =>
          // no validation is needed as the data source did not report any specific partitioning
          newPartitions.toSeq.map(Some)
      }

    } else {
      (originalPartitioning match {
        case p: KeyedPartitioning =>
          val dataTypes = p.expressions.map(_.dataType)
          val rowOrdering = RowOrdering.createNaturalAscendingOrdering(dataTypes)
          inputPartitions.sortBy(_.asInstanceOf[HasPartitionKey].partitionKey())(rowOrdering)

        case _ => inputPartitions
      }).map(Some)
    }
  }

  override lazy val readerFactory: PartitionReaderFactory = batch.createReaderFactory()

  override lazy val inputRDD: RDD[InternalRow] = {
    val rdd = if (filteredPartitions.isEmpty && outputPartitioning == SinglePartition) {
      // return an empty RDD with 1 partition if dynamic filtering removed the only split
      sparkContext.parallelize(Array.empty[InternalRow].toImmutableArraySeq, 1)
    } else {
      new DataSourceRDD(
        sparkContext, filteredPartitions, readerFactory, supportsColumnar, customMetrics)
    }
    postDriverMetrics()
    rdd
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
