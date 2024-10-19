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

package org.apache.spark.sql.execution.datasources.v2.python

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType


case class PythonInputPartition(index: Int, pickedPartition: Array[Byte]) extends InputPartition

class PythonPartitionReaderFactory(
    source: UserDefinedPythonDataSource,
    pickledReadFunc: Array[Byte],
    outputSchema: StructType,
    jobArtifactUUID: Option[String])
  extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new PartitionReader[InternalRow] {

      private[this] val metrics: Map[String, SQLMetric] = PythonCustomMetric.pythonMetrics

      private val outputIter = {
        val evaluatorFactory = source.createMapInBatchEvaluatorFactory(
          pickledReadFunc,
          "read_from_data_source",
          UserDefinedPythonDataSource.readInputSchema,
          outputSchema,
          metrics,
          jobArtifactUUID)

        val part = partition.asInstanceOf[PythonInputPartition]
        evaluatorFactory.createEvaluator().eval(
          part.index, Iterator.single(InternalRow(part.pickedPartition)))
      }

      override def next(): Boolean = outputIter.hasNext

      override def get(): InternalRow = outputIter.next()

      override def close(): Unit = {}

      override def currentMetricsValues(): Array[CustomTaskMetric] = {
        source.createPythonTaskMetrics(metrics.map { case (k, v) => k -> v.value})
      }
    }
  }
}
