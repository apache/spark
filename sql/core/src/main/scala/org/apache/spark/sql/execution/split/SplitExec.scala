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
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.FileScanRDD
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

case class SplitExec(maxExpandNum: Int, thresholdSize: Long, child: SparkPlan)
    extends UnaryExecNode {

  /**
   * @return All metrics containing metrics of this SparkPlan.
   */
  override lazy val metrics: Map[String, SQLMetric] = Map(
    "originPartNum" -> SQLMetrics.createMetric(sparkContext, "origin partition num"),
    "expandPartNum" -> SQLMetrics.createMetric(sparkContext, "expand partition num"))

  /**
   * Returns the name of this type of TreeNode.  Defaults to the class name.
   * Note that we remove the "Exec" suffix for physical operators here.
   */
  override def nodeName: String = "SplitSourcePartition"

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: SparkPlan): SplitExec =
    copy(child = newChild)

  /**
   * Produces the result of the query as an `RDD[InternalRow]`
   *
   * Overridden by concrete implementations of SparkPlan.
   */
  override protected def doExecute(): RDD[InternalRow] = {
    doSplit(child.execute())
  }

  private def doSplit[U: ClassTag](prev: RDD[U]): RDD[U] = {
    val prevPartNum = prev.getNumPartitions
    metrics("originPartNum").set(prevPartNum)
    // default: do nothing
    metrics("expandPartNum").set(prevPartNum)
    val sourceSize = evalSourceSize(prev)
    val after = sourceSize
      .map { size =>
        if (size < thresholdSize) {
          // If source size is tiny, split will not be profitable.
          prev
        } else {
          val expandPartNum = maxExpandNum min sparkContext.defaultParallelism
          if (expandPartNum < (prevPartNum << 1)) {
            // If expansion scale is tiny, split will also not be profitable.
            prev
          } else {
            metrics("expandPartNum").set(expandPartNum)
            // Maybe we could find better ways than `coalesce` to redistribute the partition data.
            prev.coalesce(expandPartNum, shuffle = true)
          }
        }
      }
      .getOrElse(prev)

    // update metrics
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
    after
  }

  @tailrec
  private def evalSourceSize[U: ClassTag](prev: RDD[U]): Option[Long] =
    prev match {
      case f: FileScanRDD => Some(f.partitionFilesTotalLength)
      case r if r.dependencies.isEmpty => None
      case other => evalSourceSize(other.firstParent)
    }

}
