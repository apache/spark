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

import scala.reflect.ClassTag

import org.apache.spark.rdd.{CoalescedRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ArrayImplicits._

/**
 * Packs the partitions of a V2 batch scan into fewer partitions, for a shuffle that reads the scan
 * through operators that keep each of its partitions apart (see [[PackV2ScanPartitions]]).
 *
 * The groups are decided when the node executes, from the input partitions that runtime filtering
 * left and the sizes they report, and its child's partitions are coalesced with
 * [[GroupedPartitionCoalescer]]. The operators below the node run as without it, with the scan's
 * partitioning and ordering. Besides the shuffle, only an AQE local read with one mapper per task
 * reads this node's layout, so it claims none.
 */
case class PackScanPartitionsExec(child: SparkPlan, packing: V2ScanPartitionPacking)
  extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  // The number of partitions is decided at execution.
  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  override def supportsColumnar: Boolean = child.supportsColumnar

  @transient override lazy val metrics: Map[String, SQLMetric] = Map(
    "numInputPartitions" -> SQLMetrics.createMetric(sparkContext, "number of input partitions"),
    "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions"),
    "maxPartitionsPerGroup" -> SQLMetrics.createMetric(sparkContext, "max partitions per group"))

  override protected def doExecute(): RDD[InternalRow] = pack(child.execute())

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = pack(child.executeColumnar())

  private def pack[T: ClassTag](rdd: RDD[T]): RDD[T] = {
    // The operators that PackV2ScanPartitions allows map each partition of their child, so these
    // are the scan's own partitions, which hold the inputs that runtime filtering left. Any other
    // RDD, such as one that a plugin's replacement for the scan returns, stays as it is.
    val partitions = rdd.partitions
    val slots = partitions.collect { case p: DataSourceRDDPartition => p.inputPartition }
    val groups = if (slots.length == partitions.length) {
      packing.packSlots(slots.toImmutableArraySeq, session.leafNodeDefaultParallelism)
    } else {
      None
    }
    sendDriverMetrics(partitions.length, groups)
    groups match {
      case Some(ranges) =>
        new CoalescedRDD(rdd, ranges.size, Some(new GroupedPartitionCoalescer(ranges)))
      case None => rdd
    }
  }

  private def sendDriverMetrics(numInputPartitions: Int, groups: Option[Seq[Range]]): Unit = {
    val values = Seq(
      "numInputPartitions" -> numInputPartitions,
      "numPartitions" -> groups.fold(numInputPartitions)(_.size),
      "maxPartitionsPerGroup" -> groups.fold(numInputPartitions.min(1))(_.iterator.map(_.size).max))
    values.foreach { case (name, value) => metrics(name).set(value) }
    SQLMetrics.postDriverMetricsUpdatedByValue(sparkContext,
      sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY),
      values.map { case (name, value) => metrics(name).id -> value.toLong })
  }

  override def simpleString(maxFields: Int): String = nodeName

  override protected def withNewChildInternal(newChild: SparkPlan): PackScanPartitionsExec =
    copy(child = newChild)
}
