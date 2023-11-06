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

package org.apache.spark.sql.execution.split

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.RoundRobinPartitioning
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.FileScanRDD
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}

case class SplitExchangeExec(maxExpandNum: Int, profitableSize: Long, child: SparkPlan)
    extends UnaryExecNode {

  /**
   * @return All metrics containing metrics of this SparkPlan.
   */
  override lazy val metrics: Map[String, SQLMetric] = ListMap(
    "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions"),
    "originalNumPartitions" -> SQLMetrics.createMetric(
      sparkContext,
      "original number of partitions"),
    "dataSize" -> SQLMetrics
      .createSizeMetric(sparkContext, "data size")) ++ readMetrics ++ writeMetrics

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)

  private lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)

  /**
   * Returns the name of this type of TreeNode.  Defaults to the class name.
   * Note that we remove the "Exec" suffix for physical operators here.
   */
  override def nodeName: String = "SplitExchange"

  override def output: Seq[Attribute] = child.output

  /**
   * The arguments that should be included in the arg string.  Defaults to the `productIterator`.
   */
  override protected def stringArgs: Iterator[Any] = super.stringArgs

  override protected def withNewChildInternal(newChild: SparkPlan): SplitExchangeExec =
    copy(child = newChild)

  /**
   * Produces the result of the query as an `RDD[InternalRow]`
   *
   * Overridden by concrete implementations of SparkPlan.
   */
  override protected def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute()
    val numPartitions = inputRDD.getNumPartitions
    metrics("numPartitions").set(numPartitions)
    metrics("originalNumPartitions").set(numPartitions)

    val expandPartNum = maxExpandNum min session.leafNodeDefaultParallelism

    val splitRDD = if (expandPartNum < (numPartitions << 1)) {
      inputRDD
    } else {
      val sourceSize = evalSourceSize(inputRDD).getOrElse(-1L)
      if (sourceSize < profitableSize) {
        inputRDD
      } else {
        metrics("numPartitions").set(expandPartNum)
        val serializer: Serializer =
          new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))
        val shuffleDependency =
          ShuffleExchangeExec.prepareShuffleDependency(
            inputRDD,
            child.output,
            RoundRobinPartitioning(expandPartNum),
            serializer,
            writeMetrics)
        new ShuffledRowRDD(shuffleDependency, readMetrics)
      }
    }

    // update metrics
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)

    splitRDD
  }

  @tailrec
  private def evalSourceSize[U: ClassTag](prev: RDD[U]): Option[Long] =
    prev match {
      case f: FileScanRDD => Some(f.filePartitions.map(_.files.map(_.length).sum).sum)
      case r if r.dependencies.isEmpty => None
      case o => evalSourceSize(o.firstParent)
    }

}
