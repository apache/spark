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
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.DELETE
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLLastAttemptMetrics, SQLMetric, SQLMetrics}
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

  // What this node plans from `scan`, see [[PlannedScan]]. `makeCopy` and `clone` hand it over to a
  // copy over the same `Scan`. Any other new node plans its own, including one the case-class
  // `copy` makes.
  @transient private var plannedScan: PlannedScan =
    if (scan == null) null else new PlannedScan(scan)

  def batch: Batch = if (scan == null) null else plannedScan.batch

  override protected lazy val sparkMetrics: Map[String, SQLMetric] = {
    val name = "number of output rows"
    val metric = table match {
      // Use SLAM for the scan-output count when this scan reads on behalf of a row-level DELETE,
      // so that the driver-side derivation `numDeletedRows = numScannedRows - numCopiedRows` in
      // `ReplaceDataExec.getWriteSummary` stays correct under stage retries.
      case rlot: RowLevelOperationTable if rlot.operation.command() == DELETE =>
        SQLLastAttemptMetrics.createMetric(sparkContext, name)
      case _ =>
        SQLMetrics.createMetric(sparkContext, name)
    }
    Map("numOutputRows" -> metric)
  }

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: BatchScanExec =>
      this.batch != null && this.batch == other.batch &&
          this.runtimeFilters == other.runtimeFilters &&
          this.keyGroupedPartitioning == other.keyGroupedPartitioning
    case _ =>
      false
  }

  override def hashCode(): Int = Objects.hash(batch, runtimeFilters, keyGroupedPartitioning)

  override def inputPartitions: Seq[InputPartition] = plannedScan.inputPartitions

  // Visible for testing
  @transient private[sql] lazy val filteredPartitions: Seq[Option[InputPartition]] = {
    val originalPartitioning = outputPartitioning

    val filtered = PushDownUtils.pushRuntimeFilters(scan, runtimeFilters, table, output)
    if (filtered) {
      // call toBatch again to get filtered partitions
      val newPartitions = scan.toBatch.planInputPartitions()

      originalPartitioning match {
        case k: KeyedPartitioning =>
          if (newPartitions.exists(!_.isInstanceOf[HasPartitionKey])) {
            throw new SparkException("Data source must have preserved the original partitioning " +
                "during runtime filtering: not all partitions implement HasPartitionKey after " +
                "filtering")
          }

          val inputMap = k.partitionKeys.groupBy(identity).view.mapValues(_.size)
          val comparableKeyWrapperFactory = InternalRowComparableWrapper
            .getInternalRowComparableWrapperFactory(k.expressionDataTypes)
          val filteredMap = newPartitions.groupBy(
            p => comparableKeyWrapperFactory(p.asInstanceOf[HasPartitionKey].partitionKey())
          )

          if (!filteredMap.keySet.subsetOf(inputMap.keySet)) {
            throw new SparkException("During runtime filtering, data source must not report new " +
                "partition keys that are not present in the original partitioning.")
          }

          inputMap.toSeq
            .sortBy(_._1)(k.keyOrdering)
            .flatMap { case (key, size) =>
              // We require the new number of partitions to be equal or less than the old number of
              // partitions for a given key. In the case of less than, empty partitions are added.
              val fps = filteredMap.getOrElse(key, Array.empty)

              if (fps.size > size) {
                throw new SparkException("During runtime filtering, data source must not report " +
                  s"new partitions for a given key. Before: $size partitions. " +
                  s"After: ${fps.size} partitions")
              }

              fps.map(Some).padTo(size, None)
            }

        case _ =>
          // no validation is needed as the data source did not report any specific partitioning
          newPartitions.toSeq.map(Some)
      }

    } else {
      (originalPartitioning match {
        case k: KeyedPartitioning =>
          inputPartitions.sortBy(_.asInstanceOf[HasPartitionKey].partitionKey())(k.keyRowOrdering)

        case _ => inputPartitions
      }).map(Some)
    }
  }

  override def readerFactory: PartitionReaderFactory = plannedScan.readerFactory

  override lazy val inputRDD: RDD[InternalRow] = {
    val rdd = if (filteredPartitions.isEmpty && outputPartitioning == SinglePartition) {
      // return an empty RDD with 1 partition if dynamic filtering removed the only split
      sparkContext.parallelize(Array.empty[InternalRow].toImmutableArraySeq, 1)
    } else {
      new DataSourceRDD(
        sparkContext, filteredPartitions, readerFactory, supportsColumnar, customMetrics)
    }
    postDriverMetrics(scan.reportDriverMetrics())
    rdd
  }

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
        output),
      keyGroupedPartitioning = keyGroupedPartitioning.map(_.map(
        QueryPlan.normalizeExpressions(_, output))))
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
