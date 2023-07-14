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
import org.apache.spark.sql.catalyst.plans.physical.{KeyGroupedPartitioning, SinglePartition}
import org.apache.spark.sql.catalyst.util.InternalRowSet
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition, PartitionReaderFactory, Scan, SupportsRuntimeFiltering}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.joins.ProxyBroadcastVarAndStageIdentifier



/**
 * Physical plan node for scanning a batch of data from a data source v2.
 */
case class BatchScanExec(
    output: Seq[AttributeReference],
    @transient scan: Scan,
    runtimeFilters: Seq[Expression],
    keyGroupedPartitioning: Option[Seq[Expression]] = None,
    @transient table: Table,
    @transient proxyForPushedBroadcastVar: Option[Seq[ProxyBroadcastVarAndStageIdentifier]] = None)
  extends DataSourceV2ScanExecBase {
  @transient @volatile private var filteredPartitions: Seq[Seq[InputPartition]] = null

  @transient lazy val batch = scan.toBatch

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: BatchScanExec =>
      val commonEquality = this.runtimeFilters == other.runtimeFilters &&
        this.proxyForPushedBroadcastVar == other.proxyForPushedBroadcastVar
      if (commonEquality) {
        (this, other) match {
          case (sr1: SupportsRuntimeFiltering, sr2: SupportsRuntimeFiltering) =>
            sr1.equalToIgnoreRuntimeFilters(sr2)
          case (sr1, sr2) => sr1.batch == sr2.batch
        }
      } else {
        false
      }
    case _ => false
  }

  override def hashCode(): Int = {
    val batchHashCode = batch match {
      case sr: SupportsRuntimeFiltering => sr.hashCodeIgnoreRuntimeFilters()
      case _ => batch.hashCode()
    }
    Objects.hashCode(Integer.valueOf(batchHashCode), runtimeFilters,
      this.proxyForPushedBroadcastVar)
  }

  @transient override lazy val inputPartitions: Seq[InputPartition] = batch.planInputPartitions()

   private def initFilteredPartitions(): Unit = {

    val dataSourceFilters = runtimeFilters.flatMap {
      case DynamicPruningExpression(e) => DataSourceStrategy.translateRuntimeFilter(e)
      case _ => None
    }

    val pushFiltersAndRefreshIter = dataSourceFilters.nonEmpty ||
      (scan.isInstanceOf[SupportsRuntimeFiltering]
      && scan.asInstanceOf[SupportsRuntimeFiltering].hasPushedBroadCastFilter)
    if (pushFiltersAndRefreshIter) {
      val originalPartitioning = outputPartitioning

      // the cast is safe as runtime filters are only assigned if the scan can be filtered
      val filterableScan = scan.asInstanceOf[SupportsRuntimeFiltering]
      if (dataSourceFilters.nonEmpty) {
        filterableScan.filter(dataSourceFilters.toArray)
      }
      filterableScan.callbackBeforeOpeningIterator()
      // call toBatch again to get filtered partitions
      val newPartitions = scan.toBatch.planInputPartitions()

      val newGroupedPartitions = originalPartitioning match {
        case p: KeyGroupedPartitioning =>
          if (newPartitions.exists(!_.isInstanceOf[HasPartitionKey])) {
            throw new SparkException("Data source must have preserved the original partitioning " +
                "during runtime filtering: not all partitions implement HasPartitionKey after " +
                "filtering")
          }

          val newRows = new InternalRowSet(p.expressions.map(_.dataType))
          newRows ++= newPartitions.map(_.asInstanceOf[HasPartitionKey].partitionKey())
          val oldRows = p.partitionValuesOpt.get

          if (oldRows.size != newRows.size) {
            throw new SparkException("Data source must have preserved the original partitioning " +
                "during runtime filtering: the number of unique partition values obtained " +
                s"through HasPartitionKey changed: before ${oldRows.size}, after ${newRows.size}")
          }

          if (!oldRows.forall(newRows.contains)) {
            throw new SparkException("Data source must have preserved the original partitioning " +
                "during runtime filtering: the number of unique partition values obtained " +
                s"through HasPartitionKey remain the same but do not exactly match")
          }

          groupPartitions(newPartitions).get.map(_._2)

        case _ =>
          // no validation is needed as the data source did not report any specific partitioning
          newPartitions.toSeq.map(Seq(_))
      }
      this.filteredPartitions = newGroupedPartitions
    } else {
      this.filteredPartitions = partitions
    }
  }

  override lazy val readerFactory: PartitionReaderFactory = batch.createReaderFactory()

  @transient @volatile private var inputRDDCached: RDD[InternalRow] = null

  override def inputRDD: RDD[InternalRow] = {
    var local = inputRDDCached
    if (local eq null) {
      local = this.synchronized {
        if (inputRDDCached eq null) {
          if (this.filteredPartitions eq null) {
            this.initFilteredPartitions()
          }
          this.inputRDDCached = if (filteredPartitions.isEmpty &&
            outputPartitioning == SinglePartition) {
            // return an empty RDD with 1 partition if dynamic filtering removed the only split
            sparkContext.parallelize(Array.empty[InternalRow], 1)
          } else {
            new DataSourceRDD(
              sparkContext, filteredPartitions, readerFactory, supportsColumnar, customMetrics)
          }
          this.inputRDDCached
        } else {
          inputRDDCached
        }
      }
    }
    local
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
    val broadcastVarFiltersString = this.proxyForPushedBroadcastVar.fold("")(proxies =>
      proxies.map(proxy => {
        val joinKeysStr = proxy.joiningKeysData.map(jkd => s"build side join" +
          s" key = ${jkd.buildSideJoinKeyAtJoin} and stream side join key =" +
          s" ${jkd.streamSideJoinKeyAtJoin}").mkString(";")
        s"for this buildleg : join col and streaming col = $joinKeysStr"
      }).mkString("\n"))
    val result = s"$nodeName$truncatedOutputString ${scan.description()} $runtimeFiltersString" +
      s" $broadcastVarFiltersString"
    redact(result)
  }

  def resetFilteredPartitionsAndInputRdd(): Unit = {
    this.synchronized {
      this.filteredPartitions = null
      this.inputRDDCached = null
    }
  }
}
