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
import org.apache.spark.sql.catalyst.plans.physical.{KeyedPartitioning, Partitioning, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.DELETE
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.EmptyRDDWithPartitions
import org.apache.spark.sql.execution.datasources.StorageFilterPlanning
import org.apache.spark.sql.execution.metric.{SQLLastAttemptMetrics, SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.connector.{StorageFilterImpl, StorageFilterRequestImpl}
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.util.ArrayImplicits._

/**
 * Physical plan node for scanning a batch of data from a data source v2.
 *
 * @param storageFilters Predicates the scan's readers may evaluate, to skip reading the other
 *                       columns of the rows they reject, see [[SupportsStorageFilters]]. Non-empty
 *                       only for a scan that declared their columns. Each task's readers get them
 *                       ANDed into one filter, in a [[StorageFilterRequest]]. They stay in the
 *                       post-scan `Filter` too.
 * @param appendsCheckedColumn Whether the readers mark the rows they checked in a boolean column,
 *                             the last of `output`, see `StorageFilterPlanning.checkedColumn`.
 */
case class BatchScanExec(
    output: Seq[AttributeReference],
    @transient scan: Scan,
    runtimeFilters: Seq[Expression],
    ordering: Option[Seq[SortOrder]] = None,
    @transient table: Table,
    keyGroupedPartitioning: Option[Seq[Expression]] = None,
    storageFilters: Seq[Expression] = Nil,
    appendsCheckedColumn: Boolean = false,
    partitionPacking: Option[V2ScanPartitionPackingInfo] = None
  ) extends DataSourceV2ScanExecBase {

  // The readers are asked for the column only with storage filters, and append it last.
  require(!appendsCheckedColumn ||
    (storageFilters.nonEmpty && output.lastOption.exists(_.dataType == BooleanType)),
    "A checked column comes with storage filters, last in the output")

  // The output the scan was planned for, without the checked column the readers append.
  private def scanOutput: Seq[AttributeReference] =
    if (appendsCheckedColumn) output.init else output

  // Copies retaining the same Scan reuse its planned inputs. A connector rule replacing Scan
  // with a fresh variant must obtain that variant's batch, partitions and reader factory.
  @transient private lazy val inputSnapshot = partitionPacking.filter(_.scan eq scan)

  @transient lazy val batch: Batch = if (scan == null) null else {
    inputSnapshot.map(_.batch).getOrElse(scan.toBatch)
  }

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
          this.appendsCheckedColumn == other.appendsCheckedColumn &&
          this.partitionPacking.map(_.options) == other.partitionPacking.map(_.options)
    case _ =>
      false
  }

  override def hashCode(): Int = Objects.hash(
    batch, runtimeFilters, prunedKeyGroupedPartitioning, storageFilters, appendsCheckedColumn,
    partitionPacking.map(_.options))

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
    inputSnapshot.map(_.inputPartitions).getOrElse(
      batch.planInputPartitions().toImmutableArraySeq)

  private[sql] def withPartitionPacking(options: V2ScanPartitionPacking): BatchScanExec = {
    val snapshot = new V2ScanPartitionPackingInfo(
      options, scan, batch, inputPartitions, readerFactory, reportedKeyedPartitioning)
    val packed = copy(partitionPacking = Some(snapshot))
    packed.copyTagsFrom(this)
    packed
  }

  @transient override protected lazy val reportedKeyedPartitioning: Option[KeyedPartitioning] =
    inputSnapshot match {
      case Some(snapshot) => snapshot.keyedPartitioning
      case None => computeReportedKeyedPartitioning()
    }

  // Visible for testing. Runtime filtering resolves the scan's columns by name, which the checked
  // column must not take part in.
  @transient private[sql] lazy val filteredPartitions: Seq[Option[InputPartition]] =
    PushDownUtils.replanWithRuntimeFilters(
      scan,
      runtimeFilters,
      table,
      scanOutput,
      reportedKeyedPartitioning,
      inputPartitions)

  // Keep the source's full keyed metadata for runtime-filter validation and empty-slot padding,
  // but do not advertise properties that packing across inputs no longer preserves.
  @transient override lazy val outputPartitioning: Partitioning = if (partitionPacking.isDefined) {
    UnknownPartitioning(0)
  } else {
    scanOutputPartitioning
  }

  override def outputOrdering: Seq[SortOrder] = if (partitionPacking.isDefined) {
    Nil
  } else {
    super.outputOrdering
  }

  override lazy val readerFactory: PartitionReaderFactory =
    inputSnapshot.map(_.readerFactory).getOrElse(batch.createReaderFactory())

  // What every task's readers get, the storage filters ANDed into one. A runtime bloom arrives as
  // a ScalarSubquery over the build side, which SparkPlan has computed by the time this is forced.
  // The readers get its result, so the task does not carry the subquery's plan.
  @transient private lazy val storageFilterRequest: Option[StorageFilterRequest] =
    storageFilters.map(StorageFilterPlanning.withSubqueryResults)
      .reduceOption(And)
      .map(c => StorageFilterRequestImpl(StorageFilterImpl(c, scanOutput), appendsCheckedColumn))

  override lazy val inputRDD: RDD[InternalRow] = {
    val rdd = if ((filteredPartitions.isEmpty && outputPartitioning == SinglePartition) ||
        (partitionPacking.isDefined && filteredPartitions.forall(_.isEmpty))) {
      // Keep one empty task after pruning, without serializing padded slots in a packed scan.
      new EmptyRDDWithPartitions(sparkContext, 1)
    } else {
      // Padding remains in filteredPartitions for keyed runtime-filter validation. A packed
      // scan advertises no keyed layout, so its tasks need not serialize or compute empty slots.
      val inputs = if (partitionPacking.isDefined) {
        filteredPartitions.filter(_.nonEmpty)
      } else {
        filteredPartitions
      }
      val source = new DataSourceRDD(
        sparkContext, inputs, readerFactory, supportsColumnar, customMetrics,
        storageFilterRequest, Option.when(appendsCheckedColumn)(output.length))
      partitionPacking match {
        case Some(snapshot) =>
          val groups = snapshot.options.pack(inputs.flatten)
          if (groups.size < inputs.size && groups.nonEmpty) {
            source.coalesce(groups.size, shuffle = false,
              partitionCoalescer = Some(new V2ScanPartitionCoalescer(groups)))
          } else {
            source
          }
        case None => source
      }
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
      storageFilters = QueryPlan.normalizePredicates(storageFilters, output))
  }

  override def simpleString(maxFields: Int): String = {
    val truncatedOutputString = truncatedString(output, "[", ", ", "]", maxFields)
    val packingString = if (partitionPacking.isDefined) " PartitionPacking: true" else ""
    val runtimeFiltersString = s"RuntimeFilters: ${runtimeFilters.mkString("[", ",", "]")}"
    val storageFiltersString = storageFiltersDescription.map(" " + _).getOrElse("")
    val result = s"$nodeName$truncatedOutputString ${scan.description()} " +
      s"$runtimeFiltersString$storageFiltersString$packingString"
    redact(result)
  }

  override def verboseStringWithOperatorId(): String = {
    val verbose = super.verboseStringWithOperatorId()
    storageFiltersDescription.map(d => verbose + redact(d) + "\n").getOrElse(verbose)
  }

  // Only when the scan has some, since an empty one would change every other scan's description.
  private def storageFiltersDescription: Option[String] = Option.when(storageFilters.nonEmpty) {
    s"StorageFilters: ${storageFilters.mkString("[", ",", "]")}"
  }

  override def nodeName: String = {
    s"BatchScan ${table.name()}".trim
  }
}
