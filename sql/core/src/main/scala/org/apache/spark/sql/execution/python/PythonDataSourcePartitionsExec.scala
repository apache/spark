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

package org.apache.spark.sql.execution.python

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{InputRDDCodegen, LeafExecNode, SQLExecution}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.ArrayImplicits._

/**
 * A physical plan node for scanning data from a list of data source partition values.
 *
 * It creates a RDD with number of partitions equal to size of the partition value list and
 * each partition contains a single row with a serialized partition value.
 */
case class PythonDataSourcePartitionsExec(
    output: Seq[Attribute],
    partitions: Seq[Array[Byte]]) extends LeafExecNode with InputRDDCodegen {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  @transient private lazy val unsafeRows: Array[InternalRow] = {
    if (partitions.isEmpty) {
      Array.empty
    } else {
      val proj = UnsafeProjection.create(output, output)
      partitions.map(p => proj(InternalRow(p)).copy()).toArray
    }
  }

  @transient private lazy val rdd: RDD[InternalRow] = {
    val numPartitions = partitions.size
    if (numPartitions == 0) {
      sparkContext.emptyRDD
    } else {
      sparkContext.parallelize(unsafeRows.toImmutableArraySeq, numPartitions)
    }
  }

  override def inputRDD: RDD[InternalRow] = rdd

  override protected val createUnsafeProjection: Boolean = false

  protected override def doExecute(): RDD[InternalRow] = {
    longMetric("numOutputRows").add(partitions.size)
    sendDriverMetrics()
    rdd
  }

  override protected def stringArgs: Iterator[Any] = {
    if (partitions.isEmpty) {
      Iterator("<empty>", output)
    } else {
      Iterator(output)
    }
  }

  private def sendDriverMetrics(): Unit = {
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
  }
}
