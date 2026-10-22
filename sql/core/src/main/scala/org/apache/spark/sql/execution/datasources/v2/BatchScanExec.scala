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
import org.apache.spark.sql.execution.{EmptyRDDWithPartitions, ScalarSubquery => ExecScalarSubquery}
import org.apache.spark.sql.execution.metric.{SQLLastAttemptMetrics, SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.connector.{StorageFilterImpl, StorageFilterRequestImpl}
import org.apache.spark.util.ArrayImplicits._

/**
 * Physical plan node for scanning a batch of data from a data source v2.
 *
 * @param storageFilters Predicates the scan's readers may evaluate, to skip reading the other
 *                       columns of the rows they reject, see [[SupportsStorageFilters]]. Non-empty
 *                       only for a scan that declared their columns. Each task's readers get them
 *                       ANDed into one filter, in a [[StorageFilterRequest]]. They stay in the
 *                       post-scan `Filter` too.
 * @param checkedColumn The column the readers mark the rows they checked in, last in `output`, when
 *                      the planner asked for one, see `StorageFilterPlanning.checkedColumn`.
 */
case class BatchScanExec(
    output: Seq[AttributeReference],
    @transient scan: Scan,
    runtimeFilters: Seq[Expression],
    ordering: Option[Seq[SortOrder]] = None,
    @transient table: Table,
    keyGroupedPartitioning: Option[Seq[Expression]] = None,
    storageFilters: Seq[Expression] = Nil,
    checkedColumn: Option[AttributeReference] = None
  ) extends DataSourceV2ScanExecBase {

  @transient lazy val batch: Batch = if (scan == null) null else scan.toBatch

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
          this.prunedKeyGroupedPartitioning == other.prunedKeyGroupedPartitioning &&
          this.storageFilters == other.storageFilters &&
          this.checkedColumn.isDefined == other.checkedColumn.isDefined
    case _ =>
      false
  }

  override def hashCode(): Int = Objects.hash(
    batch, runtimeFilters, prunedKeyGroupedPartitioning, storageFilters, checkedColumn.isDefined)

  /**
   * The reported partitioning keys restricted to those still present in `output`. A key may
   * reference a column pruned out of the scan, which is kept as long as any key survives (see
   * V2ScanPartitioningAndOrdering). Such a dangling key carries no information, since the physical
   * outputPartitioning projects it away, so `doCanonicalize`, `equals` and `hashCode` all use
   * this view to stay consistent about ignoring it.
   */
  @transient lazy val prunedKeyGroupedPartitioning: Option[Seq[Expression]] =
    keyGroupedPartitioning.map(_.filter(_.references.subsetOf(outputSet)))

  @transient override lazy val inputPartitions: Seq[InputPartition] =
    batch.planInputPartitions().toImmutableArraySeq

  // Visible for testing
  @transient private[sql] lazy val filteredPartitions: Seq[Option[InputPartition]] =
    PushDownUtils.replanWithRuntimeFilters(
      scan,
      runtimeFilters,
      table,
      output,
      reportedKeyedPartitioning,
      inputPartitions)

  override lazy val readerFactory: PartitionReaderFactory = batch.createReaderFactory()

  // What every task's readers get, the storage filters ANDed into one. A runtime bloom arrives as
  // a ScalarSubquery over the build side, which SparkPlan has computed by the time this is forced.
  // The readers get its literal, so the task does not carry the subquery's plan.
  @transient private lazy val storageFilterRequest: Option[StorageFilterRequest] =
    storageFilters.map(_.transform { case s: ExecScalarSubquery => s.toLiteral })
      .reduceOption(And)
      .map(c => StorageFilterRequestImpl(StorageFilterImpl(c, output), checkedColumn.isDefined))

  override lazy val inputRDD: RDD[InternalRow] = {
    val rdd = if (filteredPartitions.isEmpty && outputPartitioning == SinglePartition) {
      // return an empty RDD with 1 partition if dynamic filtering removed the only split
      new EmptyRDDWithPartitions(sparkContext, 1)
    } else {
      new DataSourceRDD(
        sparkContext, filteredPartitions, readerFactory, supportsColumnar, customMetrics,
        storageFilterRequest)
    }
    postDriverMetrics(scan.reportDriverMetrics())
    rdd
  }

  override def doCanonicalize(): BatchScanExec = {
    this.copy(
      output = output.map(QueryPlan.normalizeExpressions(_, output)),
      runtimeFilters = QueryPlan.normalizePredicates(
        runtimeFilters.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral)),
        output),
      keyGroupedPartitioning = prunedKeyGroupedPartitioning.map(
        _.map(QueryPlan.normalizeExpressions(_, output))),
      // Storage filters change what the scan returns, so a scan carrying one must not be reused for
      // a plain scan of the same table.
      storageFilters = QueryPlan.normalizePredicates(storageFilters, output),
      checkedColumn = checkedColumn.map(QueryPlan.normalizeExpressions(_, output)))
  }

  override def simpleString(maxFields: Int): String = {
    val truncatedOutputString = truncatedString(output, "[", ", ", "]", maxFields)
    val runtimeFiltersString = s"RuntimeFilters: ${runtimeFilters.mkString("[", ",", "]")}"
    // Only when the scan has some, since an empty one would change every other scan's description.
    val storageFiltersString = if (storageFilters.isEmpty) {
      ""
    } else {
      s" StorageFilters: ${storageFilters.mkString("[", ",", "]")}"
    }
    val result = s"$nodeName$truncatedOutputString ${scan.description()} " +
      s"$runtimeFiltersString$storageFiltersString"
    redact(result)
  }

  // As in `simpleString`, only when the scan has some.
  override def verboseStringWithOperatorId(): String = {
    val verbose = super.verboseStringWithOperatorId()
    if (storageFilters.isEmpty) {
      verbose
    } else {
      verbose + redact(s"StorageFilters: ${storageFilters.mkString("[", ",", "]")}") + "\n"
    }
  }

  override def nodeName: String = {
    s"BatchScan ${table.name()}".trim
  }
}
