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
import org.apache.spark.sql.catalyst.expressions.{Ascending, Expression, ExpressionSet, SortOrder, TransformExpression}
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.catalyst.plans.physical.KeyedPartitioning
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.{ExplainUtils, LeafExecNode, SafeForKWayMerge}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.connector.SupportsMetadata
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

trait DataSourceV2ScanExecBase
  extends LeafExecNode
  with SafeForKWayMerge
  with SupportsCustomDriverMetrics {

  override lazy val customMetrics: Map[String, SQLMetric] =
    createCustomMetrics(scan.supportedCustomMetrics())

  override protected lazy val sparkMetrics: Map[String, SQLMetric] =
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  def scan: Scan

  def readerFactory: PartitionReaderFactory

  /** Optional partitioning expressions provided by the V2 data sources, through
   * `SupportsReportPartitioning` */
  def keyGroupedPartitioning: Option[Seq[Expression]]

  /** Optional ordering expressions provided by the V2 data sources, through
   * `SupportsReportOrdering` */
  def ordering: Option[Seq[SortOrder]]

  /** Must be stable for the instance, since `outputPartitioning` memoizes over it. */
  protected def inputPartitions: Seq[InputPartition]

  override def simpleString(maxFields: Int): String = {
    val result =
      s"$nodeName${truncatedString(output, "[", ", ", "]", maxFields)} ${scan.description()}"
    redact(result)
  }

  def partitions: Seq[Option[InputPartition]] = inputPartitions.map(Some)

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

  /**
   * The partitioning as the source reported it: one key per input partition, holding every
   * reported key position, with the partitions in the order those full keys sort into. It is built
   * from the raw, full-width `HasPartitionKey.partitionKey()` rows, so a consumer of those rows
   * (`filteredPartitions`) must take the keys, the key types and the order from here, not from the
   * possibly-projected `outputPartitioning`.
   *
   * A `lazy val` because this is the expensive half: it sorts every partition key, wraps each one
   * and runs a `distinct` over them, and both `outputPartitioning` and `filteredPartitions` ask
   * for it.
   */
  @transient protected lazy val reportedKeyedPartitioning: Option[KeyedPartitioning] = {
    keyGroupedPartitioning match {
      case Some(exprs) if conf.v2BucketingEnabled && KeyedPartitioning.supportsExpressions(exprs) &&
          inputPartitions.nonEmpty && inputPartitions.forall(_.isInstanceOf[HasPartitionKey]) =>
        // A data source reports its splits in its own order, and a keyed side and a side
        // re-shuffled onto it have to agree on the order or
        // `PartitioningCollection.fromPartitionings` refuses them. See `KeyedPartitioning.apply`
        // for why this is the ordering to sort with.
        val keys = inputPartitions.map(_.asInstanceOf[HasPartitionKey].partitionKey())
          .sorted(KeyedPartitioning.groupedKeyRowOrdering(exprs.map(_.dataType)))
        Some(KeyedPartitioning(exprs, keys))
      case _ => None
    }
  }

  // A `lazy val` because the planner asks a node for its partitioning many times, and each ask
  // would otherwise re-project the reported keys.
  @transient override lazy val outputPartitioning: physical.Partitioning =
    reportedKeyedPartitioning match {
      case Some(partitioning) =>
        // A partition key may reference a column that was pruned out of the scan output (see
        // V2ScanPartitioningAndOrdering). Project such unresolvable key positions away so the
        // reported partitioning only references output columns.
        val exprs = partitioning.expressions
        val resolvablePositions = exprs.indices.filter(i => exprs(i).references.subsetOf(outputSet))
        if (resolvablePositions.isEmpty) {
          super.outputPartitioning
        } else {
          partitioning.project(resolvablePositions)
        }
      case _ => super.outputPartitioning
    }

  /**
   * Returns the output ordering for this scan. When the source reports ordering via
   * `SupportsReportOrdering` and `V2ScanPartitioningAndOrdering` keeps it, that ordering may
   * reference columns pruned out of the scan output, while a consumer binds it against the
   * output, e.g. the k-way merge of `GroupPartitionsExec`. Ordering is prefix-based, so the
   * leading run of sort orders over the output is kept and the rest is dropped, except the sort
   * orders on a partition key when the output partitioning is a `KeyedPartitioning`: each
   * partition holds a single key, so those still hold. Otherwise, when the output partitioning
   * is a `KeyedPartitioning` and `spark.sql.sources.v2.bucketing.partitionKeyOrdering.enabled`
   * is on, each partition contains rows where the key expressions evaluate to a single constant
   * value, so the data is trivially sorted by those expressions within the partition.
   *
   * Either way, a sort order that holds a partition transform is dropped, even on a partition key.
   * In a reported ordering it also ends the leading run, like a sort order over a pruned column.
   * This loses nothing today, since no operator requires an ordering over a transform. The write
   * path sorts by the transform's function call instead. Dropping it is also a safe way to handle
   * a transform Spark cannot evaluate. Keeping it would only add comparisons nobody uses. A sort
   * order that Spark derives from a transform key is even constant within each partition. Revisit
   * this if an ordering over a transform becomes a real requirement.
   */
  override def outputOrdering: Seq[SortOrder] = {
    def holdsTransform(e: Expression): Boolean = e.exists(_.isInstanceOf[TransformExpression])
    (ordering, outputPartitioning) match {
      case (Some(o), p) =>
        val (prefix, rest) =
          o.span(order => order.references.subsetOf(outputSet) && !holdsTransform(order.child))
        p match {
          case k: KeyedPartitioning if rest.nonEmpty =>
            val keyExprs = ExpressionSet(k.expressions.filterNot(holdsTransform))
            prefix ++ rest.filter(order => keyExprs.contains(order.child))
          case _ => prefix
        }
      case (_, k: KeyedPartitioning) if conf.v2BucketingPartitionKeyOrderingEnabled =>
        k.expressions.filterNot(holdsTransform).map(SortOrder(_, Ascending))
      case _ => Seq.empty
    }
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

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].map { b =>
      numOutputRows += b.numRows()
      b
    }
  }
}
