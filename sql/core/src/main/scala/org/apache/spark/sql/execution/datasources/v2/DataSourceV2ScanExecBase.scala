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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, RowOrdering, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.catalyst.plans.physical.KeyGroupedPartitioning
import org.apache.spark.sql.catalyst.util.{truncatedString, InternalRowComparableWrapper}
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.{ExplainUtils, LeafExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.SupportsMetadata
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

trait DataSourceV2ScanExecBase extends LeafExecNode {

  lazy val customMetrics: Map[String, SQLMetric] =
    SQLMetrics.createV2CustomMetrics(sparkContext, scan)

  override lazy val metrics: Map[String, SQLMetric] = {
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows")) ++
      customMetrics
  }

  def scan: Scan

  def readerFactory: PartitionReaderFactory

  /** Optional partitioning expressions provided by the V2 data sources, through
   * `SupportsReportPartitioning` */
  def keyGroupedPartitioning: Option[Seq[Expression]]

  /** Optional ordering expressions provided by the V2 data sources, through
   * `SupportsReportOrdering` */
  def ordering: Option[Seq[SortOrder]]

  protected def inputPartitions: Seq[InputPartition]

  override def simpleString(maxFields: Int): String = {
    val result =
      s"$nodeName${truncatedString(output, "[", ", ", "]", maxFields)} ${scan.description()}"
    redact(result)
  }

  def partitions: Seq[Seq[InputPartition]] = {
    groupedPartitions.map(_.groupedParts.map(_.parts)).getOrElse(inputPartitions.map(Seq(_)))
  }

  /**
   * Shorthand for calling redact() without specifying redacting rules
   */
  protected def redact(text: String): String = {
    Utils.redact(conf.stringRedactionPattern, text)
  }

  override def verboseStringWithOperatorId(): String = {
    val metaDataStr = scan match {
      case s: SupportsMetadata =>
        s.getMetaData().toSeq.sorted.flatMap {
          case (_, value) if value.isEmpty || value.equals("[]") => None
          case (key, value) => Some(s"$key: ${redact(value)}")
          case _ => None
        }
      case _ =>
        Seq(scan.description())
    }
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", output)}
       |${metaDataStr.mkString("\n")}
       |""".stripMargin
  }

  override def outputPartitioning: physical.Partitioning = {
    keyGroupedPartitioning match {
      case Some(exprs) if KeyGroupedPartitioning.supportsExpressions(exprs) =>
        groupedPartitions
          .map { keyGroupedPartsInfo =>
            val keyGroupedParts = keyGroupedPartsInfo.groupedParts
            KeyGroupedPartitioning(exprs, keyGroupedParts.size, keyGroupedParts.map(_.value),
              keyGroupedPartsInfo.originalParts.map(_.partitionKey()))
          }
          .getOrElse(super.outputPartitioning)
      case _ =>
        super.outputPartitioning
    }
  }

  @transient lazy val groupedPartitions: Option[KeyGroupedPartitionInfo] = {
    // Early check if we actually need to materialize the input partitions.
    keyGroupedPartitioning match {
      case Some(_) => groupPartitions(inputPartitions)
      case _ => None
    }
  }

  /**
   * Group partition values for all the input partitions. This returns `Some` iff:
   *   - [[SQLConf.V2_BUCKETING_ENABLED]] is turned on
   *   - all input partitions implement [[HasPartitionKey]]
   *   - `keyGroupedPartitioning` is set
   *
   * The result, if defined, is a [[KeyGroupedPartitionInfo]] which contains a list of
   * [[KeyGroupedPartition]], as well as a list of partition values from the original input splits,
   * sorted according to the partition keys in ascending order.
   *
   * A non-empty result means each partition is clustered on a single key and therefore eligible
   * for further optimizations to eliminate shuffling in some operations such as join and aggregate.
   */
  def groupPartitions(inputPartitions: Seq[InputPartition]): Option[KeyGroupedPartitionInfo] = {
    if (!SQLConf.get.v2BucketingEnabled) return None

    keyGroupedPartitioning.flatMap { expressions =>
      val results = inputPartitions.takeWhile {
        case _: HasPartitionKey => true
        case _ => false
      }.map(p => (p.asInstanceOf[HasPartitionKey].partitionKey(), p.asInstanceOf[HasPartitionKey]))

      if (results.length != inputPartitions.length || inputPartitions.isEmpty) {
        // Not all of the `InputPartitions` implements `HasPartitionKey`, therefore skip here.
        None
      } else {
        // also sort the input partitions according to their partition key order. This ensures
        // a canonical order from both sides of a bucketed join, for example.
        val partitionDataTypes = expressions.map(_.dataType)
        val rowOrdering = RowOrdering.createNaturalAscendingOrdering(partitionDataTypes)
        val sortedKeyToPartitions = results.sorted(rowOrdering.on((t: (InternalRow, _)) => t._1))
        val sortedGroupedPartitions = sortedKeyToPartitions
            .map(t => (InternalRowComparableWrapper(t._1, expressions), t._2))
            .groupBy(_._1)
            .toSeq
            .map { case (key, s) => KeyGroupedPartition(key.row, s.map(_._2)) }
            .sorted(rowOrdering.on((k: KeyGroupedPartition) => k.value))

        Some(KeyGroupedPartitionInfo(sortedGroupedPartitions, sortedKeyToPartitions.map(_._2)))
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = {
    // when multiple partitions are grouped together, ordering inside partitions is not preserved
    val partitioningPreservesOrdering = groupedPartitions
        .forall(_.groupedParts.forall(_.parts.length <= 1))
    ordering.filter(_ => partitioningPreservesOrdering).getOrElse(super.outputOrdering)
  }

  override def supportsColumnar: Boolean = {
    scan.columnarSupportMode() match {
      case Scan.ColumnarSupportMode.PARTITION_DEFINED =>
        require(
          inputPartitions.forall(readerFactory.supportColumnarReads) ||
            !inputPartitions.exists(readerFactory.supportColumnarReads),
          "Cannot mix row-based and columnar input partitions.")
        inputPartitions.exists(readerFactory.supportColumnarReads)
      case Scan.ColumnarSupportMode.SUPPORTED => true
      case Scan.ColumnarSupportMode.UNSUPPORTED => false
    }
  }

  def inputRDD: RDD[InternalRow]

  def inputRDDs(): Seq[RDD[InternalRow]] = Seq(inputRDD)

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    inputRDD.map { r =>
      numOutputRows += 1
      r
    }
  }

  protected def postDriverMetrics(): Unit = {
    SQLMetrics.postV2DriverMetrics(sparkContext, scan, metrics)
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].map { b =>
      numOutputRows += b.numRows()
      b
    }
  }
}

/**
 * A key-grouped Spark partition, which could consist of multiple input splits
 *
 * @param value the partition value shared by all the input splits
 * @param parts the input splits that are grouped into a single Spark partition
 */
private[v2] case class KeyGroupedPartition(value: InternalRow, parts: Seq[InputPartition])

/**
 * Information about key-grouped partitions, which contains a list of grouped partitions as well
 * as the original input partitions before the grouping.
 */
private[v2] case class KeyGroupedPartitionInfo(
    groupedParts: Seq[KeyGroupedPartition],
    originalParts: Seq[HasPartitionKey])
