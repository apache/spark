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
import org.apache.spark.sql.catalyst.optimizer.PerPartitionSizing
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{PartitioningCollection, SinglePartition}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.DELETE
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.{EmptyRDDWithPartitions, SparkPlan}
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
 * @param perPartitionBloomFilter A per-partition runtime bloom filter the readers get as a storage
 *                                filter, see [[PerPartitionBloomFilter]]. The readers of each split
 *                                get it with the bloom filters of the partitions of
 *                                `bloomFilterAggregate` that report the split's partition key, ORed
 *                                into one. They get it ANDed with `storageFilters`. A split with no
 *                                bloom filter is not read, unless the partition keys cannot be
 *                                matched, see `perPartitionBloomFilterRDD`. The join above implies
 *                                the check, so nothing above the scan checks it.
 * @param bloomFilterAggregate The aggregate of the creation side of `perPartitionBloomFilter`, with
 *                             a bloom filter for each group of its partition transform. The only
 *                             child of the scan, set only with `perPartitionBloomFilter`.
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
    perPartitionBloomFilter: Option[PerPartitionBloomFilter] = None,
    bloomFilterAggregate: Option[SparkPlan] = None
  ) extends DataSourceV2ScanExecBase {

  // The readers are asked for the column only with storage filters, and append it last.
  require(!appendsCheckedColumn ||
    (storageFilters.nonEmpty && output.lastOption.exists(_.dataType == BooleanType)),
    "A checked column comes with storage filters, last in the output")

  require(perPartitionBloomFilter.isDefined == bloomFilterAggregate.isDefined,
    "A per-partition bloom filter comes with its aggregate")

  override def children: Seq[SparkPlan] = bloomFilterAggregate.toSeq

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    sharePlanning(copy(bloomFilterAggregate = newChildren.headOption))

  // The aggregate only holds the bloom filters, which a task reads in full before it opens a
  // reader.
  override def kWayMergeInputs: Seq[SparkPlan] = Nil

  // The output the scan was planned for, without the checked column the readers append.
  private def scanOutput: Seq[AttributeReference] =
    if (appendsCheckedColumn) output.init else output

  // The width of what the readers return when they append the checked column.
  private def checkedOutputWidth: Option[Int] = Option.when(appendsCheckedColumn)(output.length)

  // What this node plans from `scan`, see [[PlannedScan]]. `makeCopy`, `clone`, `withNewChildren`
  // and `withoutPerPartitionBloomFilter` hand it over to a copy over the same `Scan`. Any other
  // new node plans its own, including one the case-class `copy` makes.
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
    Map("numOutputRows" -> metric) ++ perPartitionBloomFilter.map { _ =>
      "numSkippedSplits" -> SQLMetrics.createMetric(sparkContext,
        "number of splits skipped by runtime filters")
    }
  }

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: BatchScanExec =>
      this.batch != null && this.batch == other.batch &&
          this.runtimeFilters == other.runtimeFilters &&
          this.prunedKeyGroupedPartitioning == other.prunedKeyGroupedPartitioning &&
          this.storageFilters == other.storageFilters &&
          this.appendsCheckedColumn == other.appendsCheckedColumn &&
          this.perPartitionBloomFilter == other.perPartitionBloomFilter &&
          this.bloomFilterAggregate == other.bloomFilterAggregate
    case _ =>
      false
  }

  override def hashCode(): Int = Objects.hash(batch, runtimeFilters, prunedKeyGroupedPartitioning,
    storageFilters, appendsCheckedColumn, perPartitionBloomFilter, bloomFilterAggregate)

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

  override def readerFactory: PartitionReaderFactory = plannedScan.readerFactory

  // The storage filters ANDed into one. A runtime bloom arrives as a ScalarSubquery over the build
  // side, which SparkPlan has computed by the time this is forced. The readers get its result, so
  // the task does not carry the subquery's plan.
  @transient private lazy val storageFilterCondition: Option[Expression] =
    storageFilters.map(StorageFilterPlanning.withSubqueryResults).reduceOption(And)

  // What the readers of every task get when the scan reads without per-partition bloom filters.
  @transient private lazy val storageFilterRequest: Option[StorageFilterRequest] =
    storageFilterCondition.map(c =>
      StorageFilterRequestImpl(StorageFilterImpl(c, scanOutput), appendsCheckedColumn))

  override lazy val inputRDD: RDD[InternalRow] = {
    val rdd = if (filteredPartitions.isEmpty && outputPartitioning == SinglePartition) {
      // return an empty RDD with 1 partition if dynamic filtering removed the only split
      new EmptyRDDWithPartitions(sparkContext, 1)
    } else {
      perPartitionBloomFilterRDD.getOrElse(new DataSourceRDD(
        sparkContext, filteredPartitions, readerFactory, supportsColumnar, customMetrics,
        storageFilterRequest, checkedOutputWidth))
    }
    postDriverMetrics(scan.reportDriverMetrics())
    rdd
  }

  // The RDD whose readers get the per-partition bloom filter of their split's partition key. None
  // without one, with no splits, or when the partition keys of the splits and of the aggregate
  // cannot be matched. The scan then reads every split without the bloom filters. The join above
  // drops the rows they would reject anyway.
  private def perPartitionBloomFilterRDD: Option[RDD[InternalRow]] =
    perPartitionBloomFilter.zip(bloomFilterAggregate).flatMap { case (filter, aggregate) =>
      val splitKeys = reportedKeyedPartitioning
        .map(_.partitionKeys).filter(_.length == filteredPartitions.length)
      val bloomKeys = PartitioningCollection.representativeOf(aggregate.outputPartitioning)
        .filterNot(_.mayContainUnknownPartitionKeys).map(_.partitionKeys)
      (splitKeys, bloomKeys) match {
        case (Some(splitKeys), Some(bloomKeys)) if splitKeys.isEmpty || bloomKeys.isEmpty ||
            splitKeys.head.dataTypes == bloomKeys.head.dataTypes =>
          val partitionsByKey = bloomKeys.zipWithIndex.groupMap(_._1)(_._2)
          val bloomPartitions =
            splitKeys.map(k => partitionsByKey.getOrElse(k, Nil).toArray).toArray
          // `ApplyColumnarRulesAndInsertTransitions` keeps the aggregate row-based.
          Some(new PerPartitionStorageFilterRDD(
            sparkContext, filteredPartitions, readerFactory, supportsColumnar, customMetrics,
            checkedOutputWidth, aggregate.execute(), bloomPartitions,
            aggregate.output.indexWhere(_.exprId == filter.bloomFilter.exprId),
            (storageFilterCondition.toSeq :+ filter.check).reduce(And),
            filter.bloomFilter, scanOutput, longMetric("numSkippedSplits")))
        case _ if filteredPartitions.isEmpty => None
        case _ =>
          logWarning("Reading a scan without its per-partition bloom filters, since the " +
            "partition keys of its splits and of its aggregate do not match")
          None
      }
    }

  override def makeCopy(newArgs: Array[AnyRef]): SparkPlan = sharePlanning(super.makeCopy(newArgs))

  /** This scan without its per-partition bloom filter, over the partitions this one planned. */
  private[execution] def withoutPerPartitionBloomFilter: BatchScanExec =
    sharePlanning(copy(perPartitionBloomFilter = None, bloomFilterAggregate = None))
      .asInstanceOf[BatchScanExec]

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
        _.map(QueryPlan.normalizeExpressions(_, output))),
      // Storage filters change what the scan returns, so a scan carrying one must not be reused for
      // a plain scan of the same table.
      storageFilters = QueryPlan.normalizePredicates(storageFilters, output),
      perPartitionBloomFilter = perPartitionBloomFilter.zip(bloomFilterAggregate).map {
        case (filter, aggregate) => filter.copy(
          check = QueryPlan.normalizeExpressions(filter.check, aggregate.output ++ output))
      },
      bloomFilterAggregate = bloomFilterAggregate.map(_.canonicalized))
  }

  override def simpleString(maxFields: Int): String = {
    val truncatedOutputString = truncatedString(output, "[", ", ", "]", maxFields)
    val runtimeFiltersString = s"RuntimeFilters: ${runtimeFilters.mkString("[", ",", "]")}"
    val storageFiltersString = storageFiltersDescription.map(" " + _).getOrElse("")
    val result = s"$nodeName$truncatedOutputString ${scan.description()} " +
      s"$runtimeFiltersString$storageFiltersString"
    redact(result)
  }

  // A DPP filter, and a fully pushed runtime filter, have no FilterExec above the scan, so the
  // formatted explain shows them in full only here.
  override def verboseStringWithOperatorId(): String = {
    val runtimeFiltersDescription = Option.when(runtimeFilters.nonEmpty) {
      s"RuntimeFilters: ${runtimeFilters.mkString("[", ",", "]")}"
    }
    (runtimeFiltersDescription ++ storageFiltersDescription)
      .foldLeft(super.verboseStringWithOperatorId())((verbose, d) => verbose + redact(d) + "\n")
  }

  // Only when the scan has some, since an empty one would change every other scan's description.
  private def storageFiltersDescription: Option[String] = {
    val filters = storageFilters ++ perPartitionBloomFilter.map(_.check)
    Option.when(filters.nonEmpty)(s"StorageFilters: ${filters.mkString("[", ",", "]")}")
  }

  override def nodeName: String = {
    s"BatchScan ${table.name()}".trim
  }
}

/**
 * The per-partition runtime bloom filter of a [[BatchScanExec]].
 *
 * @param check the check of each row of the scan, `might_contain` over the bloom filter column of
 *              the scan's `bloomFilterAggregate` and the hash of a key over the scan's columns.
 *              Each task puts a split's bloom filter in place of the column, so `check` is not
 *              evaluated as it is.
 * @param sizing the sizing of the bloom filters, for the whole creation side
 */
case class PerPartitionBloomFilter(check: Expression, sizing: PerPartitionSizing) {

  /** The bloom filter column of the aggregate. */
  def bloomFilter: Attribute = check match {
    case BloomFilterMightContain(a: Attribute, _) => a
    case _ => throw SparkException.internalError(s"Not a per-partition bloom filter check: $check")
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
