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

import org.apache.spark.SparkEnv
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.PythonStreamBlockId


case class PythonStreamingInputPartition(
    index: Int,
    pickedPartition: Array[Byte],
    blockId: Option[PythonStreamBlockId]) extends InputPartition {
  def dropCache(): Unit = {
    blockId.foreach(SparkEnv.get.blockManager.master.removeBlock(_))
  }
}

class PythonStreamingPartitionReaderFactory(
    source: UserDefinedPythonDataSource,
    pickledReadFunc: Array[Byte],
    outputSchema: StructType,
    jobArtifactUUID: Option[String])
  extends PartitionReaderFactory with Logging {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val part = partition.asInstanceOf[PythonStreamingInputPartition]

    // Maybe read from cached block prefetched by SimpleStreamReader
    lazy val cachedBlock = if (part.blockId.isDefined) {
      val block = SparkEnv.get.blockManager.get[InternalRow](part.blockId.get)
        .map(_.data.asInstanceOf[Iterator[InternalRow]])
      if (block.isEmpty) {
        logWarning(log"Prefetched block ${MDC(LogKeys.BLOCK_ID, part.blockId)} " +
          log"for Python data source not found.")
      }
      block
    } else None

    new PartitionReader[InternalRow] {

      private[this] val metrics: Map[String, SQLMetric] = PythonCustomMetric.pythonMetrics

      private val outputIter = if (cachedBlock.isEmpty) {
        // Evaluate the python read UDF if the partition is not cached as block.
        val evaluatorFactory = source.createMapInBatchEvaluatorFactory(
          pickledReadFunc,
          "read_from_data_source",
          UserDefinedPythonDataSource.readInputSchema,
          outputSchema,
          metrics,
          jobArtifactUUID)

        evaluatorFactory.createEvaluator().eval(
          part.index, Iterator.single(InternalRow(part.pickedPartition)))
      } else cachedBlock.get

      override def next(): Boolean = outputIter.hasNext

      override def get(): InternalRow = outputIter.next()

      override def close(): Unit = {}

      override def currentMetricsValues(): Array[CustomTaskMetric] = {
        source.createPythonTaskMetrics(metrics.map { case (k, v) => k -> v.value })
      }
    }
  }
}
