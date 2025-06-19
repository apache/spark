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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.ArrayImplicits._


/**
 * Physical plan node for scanning data from a local collection.
 *
 * `Seq` may not be serializable and ideally we should not send `rows` and `unsafeRows`
 * to the executors. Thus marking them as transient.
 */
case class LocalTableScanExec(
    output: Seq[Attribute],
    @transient rows: Seq[InternalRow],
    @transient stream: Option[SparkDataStream])
  extends LeafExecNode
  with StreamSourceAwareSparkPlan
  with InputRDDCodegen {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  @transient private lazy val unsafeRows: Array[InternalRow] = {
    if (rows.isEmpty) {
      Array.empty
    } else {
      val proj = UnsafeProjection.create(output, output)
      rows.map(r => proj(r).copy()).toArray
    }
  }

  @transient private lazy val rdd: RDD[InternalRow] = {
    if (rows.isEmpty) {
      sparkContext.emptyRDD
    } else {
      val numSlices = math.min(
        unsafeRows.length, session.leafNodeDefaultParallelism)
      sparkContext.parallelize(unsafeRows.toImmutableArraySeq, numSlices)
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    rdd.map { r =>
      numOutputRows += 1
      r
    }
  }

  override protected def stringArgs: Iterator[Any] = {
    if (rows.isEmpty) {
      Iterator("<empty>", output)
    } else {
      Iterator(output)
    }
  }

  override def executeCollect(): Array[InternalRow] = {
    longMetric("numOutputRows").add(unsafeRows.length)
    sendDriverMetrics()
    unsafeRows
  }

  override def executeTake(limit: Int): Array[InternalRow] = {
    val taken = unsafeRows.take(limit)
    longMetric("numOutputRows").add(taken.length)
    sendDriverMetrics()
    taken
  }

  override def executeTail(limit: Int): Array[InternalRow] = {
    val taken: Seq[InternalRow] = unsafeRows.takeRight(limit).toImmutableArraySeq
    longMetric("numOutputRows").add(taken.size)
    sendDriverMetrics()
    taken.toArray
  }

  // Input is already UnsafeRows.
  override protected val createUnsafeProjection: Boolean = false

  override def inputRDD: RDD[InternalRow] = rdd

  override def getStream: Option[SparkDataStream] = stream

  private def sendDriverMetrics(): Unit = {
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
  }

  // Don't care about `stream` when canonicalizing.
  override protected def doCanonicalize(): SparkPlan = {
    super.doCanonicalize().asInstanceOf[LocalTableScanExec].copy(stream = None)
  }
}
