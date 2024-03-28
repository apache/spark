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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.execution.python.PythonStreamingSourceRunner
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class PythonStreamingSourceOffset(json: String) extends Offset

case class PythonStreamingSourcePartition(partition: Array[Byte]) extends InputPartition

class PythonMicroBatchStream(
    ds: PythonDataSourceV2,
    shortName: String,
    outputSchema: StructType,
    options: CaseInsensitiveStringMap
  ) extends MicroBatchStream with Logging {
  private def createDataSourceFunc =
    ds.source.createPythonFunction(
      ds.getOrCreateDataSourceInPython(shortName, options, Some(outputSchema)).dataSource)

  private val runner: PythonStreamingSourceRunner =
    new PythonStreamingSourceRunner(createDataSourceFunc, outputSchema)
  runner.init()

  override def initialOffset(): Offset = PythonStreamingSourceOffset(runner.initialOffset())

  override def latestOffset(): Offset = PythonStreamingSourceOffset(runner.latestOffset())

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    runner.partitions(start.asInstanceOf[PythonStreamingSourceOffset].json,
      end.asInstanceOf[PythonStreamingSourceOffset].json).map(PythonStreamingSourcePartition(_))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    // TODO(SPARK-47107): fill in the implementation.
    null
  }

  override def commit(end: Offset): Unit = {
    runner.commit(end.asInstanceOf[PythonStreamingSourceOffset].json)
  }

  override def stop(): Unit = {
    runner.stop()
  }

  override def deserializeOffset(json: String): Offset = PythonStreamingSourceOffset(json)
}

