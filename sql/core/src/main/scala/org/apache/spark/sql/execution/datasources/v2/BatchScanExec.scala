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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.DELETE
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.{EmptyRDDWithPartitions, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLLastAttemptMetrics, SQLMetric, SQLMetrics}
import org.apache.spark.util.ArrayImplicits._

/**
 * Physical plan node for scanning a batch of data from a data source v2.
 *
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
          this.prunedKeyGroupedPartitioning == other.prunedKeyGroupedPartitioning
    case _ =>
      false
  }

  override def hashCode(): Int = Objects.hash(batch, runtimeFilters, prunedKeyGroupedPartitioning)

  /**
   * The reported partitioning keys restricted to those still present in `output`. A key may
   * reference a column pruned out of the scan, which is kept as long as any key survives (see
   * V2ScanPartitioningAndOrdering). Such a dangling key carries no information, since the physical
   * outputPartitioning projects it away, so `doCanonicalize`, `equals` and `hashCode` all use
   * this view to stay consistent about ignoring it.
   */
  @transient lazy val prunedKeyGroupedPartitioning: Option[Seq[Expression]] =
    keyGroupedPartitioning.map(_.filter(_.references.subsetOf(outputSet)))

  override def inputPartitions: Seq[InputPartition] = plannedScan.inputPartitions

  // Visible for testing
  @transient private[sql] lazy val filteredPartitions: Seq[Option[InputPartition]] =
    PushDownUtils.replanWithRuntimeFilters(
      scan,
      runtimeFilters,
      table,
      output,
      reportedKeyedPartitioning,
      inputPartitions)

  override def readerFactory: PartitionReaderFactory = plannedScan.readerFactory

  override lazy val inputRDD: RDD[InternalRow] = {
    val rdd = if (filteredPartitions.isEmpty && outputPartitioning == SinglePartition) {
      // return an empty RDD with 1 partition if dynamic filtering removed the only split
      new EmptyRDDWithPartitions(sparkContext, 1)
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
      keyGroupedPartitioning = prunedKeyGroupedPartitioning.map(
        _.map(QueryPlan.normalizeExpressions(_, output))))
  }

  override def simpleString(maxFields: Int): String = {
    val truncatedOutputString = truncatedString(output, "[", ", ", "]", maxFields)
    val runtimeFiltersString = s"RuntimeFilters: ${runtimeFilters.mkString("[", ",", "]")}"
    val result = s"$nodeName$truncatedOutputString ${scan.description()} $runtimeFiltersString"
    redact(result)
  }

  // A DPP filter, and a fully pushed runtime filter, have no FilterExec above the scan, so the
  // formatted explain shows them in full only here.
  override def verboseStringWithOperatorId(): String = {
    val verbose = super.verboseStringWithOperatorId()
    val runtimeFiltersDescription = Option.when(runtimeFilters.nonEmpty) {
      s"RuntimeFilters: ${runtimeFilters.mkString("[", ",", "]")}"
    }
    runtimeFiltersDescription.map(d => verbose + redact(d) + "\n").getOrElse(verbose)
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
 * (SPARK-60752). A runtime filter replans them, through `planInputPartitionsWithRuntimeFilters` or
 * from a new `Batch` after a pushed `filter`, but keeps the planned `Batch`'s reader factory.
 *
 * A rule that rebuilds a node over the same `Scan` keeps the planned partitions through
 * `makeCopy`, e.g. with `transformExpressions`. The case-class `copy` plans them again.
 */
private[v2] final class PlannedScan(val scan: Scan) {
  lazy val batch: Batch = scan.toBatch
  lazy val inputPartitions: Seq[InputPartition] = batch.planInputPartitions().toImmutableArraySeq
  lazy val readerFactory: PartitionReaderFactory = batch.createReaderFactory()
}
