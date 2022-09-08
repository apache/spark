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
import org.apache.spark.sql.catalyst.plans.physical.{KeyGroupedPartitioning, SinglePartition}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.{ExplainUtils, LeafExecNode, SQLExecution}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.SupportsMetadata
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

trait DataSourceV2ScanExecBase extends LeafExecNode {

  lazy val customMetrics = scan.supportedCustomMetrics().map { customMetric =>
    customMetric.name() -> SQLMetrics.createV2CustomMetric(sparkContext, customMetric)
  }.toMap

  override lazy val metrics = {
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

  def partitions: Seq[Seq[InputPartition]] =
    groupedPartitions.map(_.map(_._2)).getOrElse(inputPartitions.map(Seq(_)))

  /**
   * Shorthand for calling redact() without specifying redacting rules
   */
  protected def redact(text: String): String = {
    Utils.redact(session.sessionState.conf.stringRedactionPattern, text)
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
    if (partitions.length == 1) {
      SinglePartition
    } else {
      keyGroupedPartitioning match {
        case Some(exprs) if KeyGroupedPartitioning.supportsExpressions(exprs) =>
          groupedPartitions.map { partitionValues =>
            KeyGroupedPartitioning(exprs, partitionValues.size, Some(partitionValues.map(_._1)))
          }.getOrElse(super.outputPartitioning)
        case _ =>
          super.outputPartitioning
      }
    }
  }

  @transient lazy val groupedPartitions: Option[Seq[(InternalRow, Seq[InputPartition])]] =
    groupPartitions(inputPartitions)

  /**
   * Group partition values for all the input partitions. This returns `Some` iff:
   *   - [[SQLConf.V2_BUCKETING_ENABLED]] is turned on
   *   - all input partitions implement [[HasPartitionKey]]
   *   - `keyGroupedPartitioning` is set
   *
   * The result, if defined, is a list of tuples where the first element is a partition value,
   * and the second element is a list of input partitions that share the same partition value.
   *
   * A non-empty result means each partition is clustered on a single key and therefore eligible
   * for further optimizations to eliminate shuffling in some operations such as join and aggregate.
   */
  def groupPartitions(
      inputPartitions: Seq[InputPartition]): Option[Seq[(InternalRow, Seq[InputPartition])]] = {
    if (!SQLConf.get.v2BucketingEnabled) return None
    keyGroupedPartitioning.flatMap { expressions =>
      val results = inputPartitions.takeWhile {
        case _: HasPartitionKey => true
        case _ => false
      }.map(p => (p.asInstanceOf[HasPartitionKey].partitionKey(), p))

      if (results.length != inputPartitions.length || inputPartitions.isEmpty) {
        // Not all of the `InputPartitions` implements `HasPartitionKey`, therefore skip here.
        None
      } else {
        val partKeyType = expressions.map(_.dataType)

        val groupedPartitions = results.groupBy(_._1).toSeq.map { case (key, s) =>
          (key, s.map(_._2))
        }

        // also sort the input partitions according to their partition key order. This ensures
        // a canonical order from both sides of a bucketed join, for example.
        val keyOrdering: Ordering[(InternalRow, Seq[InputPartition])] = {
          RowOrdering.createNaturalAscendingOrdering(partKeyType).on(_._1)
        }
        Some(groupedPartitions.sorted(keyOrdering))
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = {
    // when multiple partitions are grouped together, ordering inside partitions is not preserved
    val partitioningPreservesOrdering = groupedPartitions.forall(_.forall(_._2.length <= 1))
    ordering.filter(_ => partitioningPreservesOrdering).getOrElse(super.outputOrdering)
  }

  override def supportsColumnar: Boolean = {
    require(inputPartitions.forall(readerFactory.supportColumnarReads) ||
      !inputPartitions.exists(readerFactory.supportColumnarReads),
      "Cannot mix row-based and columnar input partitions.")

    inputPartitions.exists(readerFactory.supportColumnarReads)
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
    val driveSQLMetrics = scan.reportDriverMetrics().map(customTaskMetric => {
      val metric = metrics(customTaskMetric.name())
      metric.set(customTaskMetric.value())
      metric
    })

    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId,
      driveSQLMetrics)
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].map { b =>
      numOutputRows += b.numRows()
      b
    }
  }
}
