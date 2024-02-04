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

import org.apache.spark.JobArtifactSet
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.execution.python.PythonStreamingSourceRunner
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap


class PythonScan(
     ds: PythonDataSourceV2,
     shortName: String,
     outputSchema: StructType,
     options: CaseInsensitiveStringMap) extends Scan {

  override def toBatch: Batch = new PythonBatch(ds, shortName, outputSchema, options)

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = null

  override def description: String = "(Python)"

  override def readSchema(): StructType = outputSchema

  override def supportedCustomMetrics(): Array[CustomMetric] =
    ds.source.createPythonMetrics()
}

class PythonBatch(ds: PythonDataSourceV2, shortName: String,
                  outputSchema: StructType, options: CaseInsensitiveStringMap) extends Batch {
  private val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)

  private lazy val infoInPython: PythonDataSourceReadInfo = {
    ds.source.createReadInfoInPython(
      ds.getOrCreateDataSourceInPython(shortName, options, Some(outputSchema)),
      outputSchema)
  }

  override def planInputPartitions(): Array[InputPartition] =
    infoInPython.partitions.zipWithIndex.map(p => PythonInputPartition(p._2, p._1)).toArray

  override def createReaderFactory(): PartitionReaderFactory = {
    val readerFunc = infoInPython.func
    new PythonPartitionReaderFactory(
      ds.source, readerFunc, outputSchema, jobArtifactUUID)
  }
}

case class PythonStreamingSourceOffset(json: String) extends Offset

case class PythonStreamingSourcePartition(partition: Array[Byte]) extends InputPartition

class PythonMicroBatchStream(
    ds: PythonDataSourceV2,
    shortName: String,
    outputSchema: StructType,
    options: CaseInsensitiveStringMap) extends MicroBatchStream with Logging {
  private def createDataSourceFunc =
    ds.source.createPythonFunction(
      ds.getOrCreateDataSourceInPython(shortName, options, Some(outputSchema)).dataSource)

  val runner: PythonStreamingSourceRunner =
    new PythonStreamingSourceRunner(createDataSourceFunc, outputSchema, outputSchema)
  runner.init()

  override def initialOffset(): Offset = {
    // TODO: fill in the implementation.
    null
  }

  override def latestOffset(): Offset = PythonStreamingSourceOffset(runner.latestOffset())


  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    runner.partitions(start.asInstanceOf[PythonStreamingSourceOffset].json,
      end.asInstanceOf[PythonStreamingSourceOffset].json).map(PythonStreamingSourcePartition(_))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    // TODO: fill in the implementation.
    null
  }

  override def commit(end: Offset): Unit = {
    // TODO: fill in the implementation.
  }

  override def stop(): Unit = {
    runner.stop()
  }

  override def deserializeOffset(json: String): Offset = PythonStreamingSourceOffset(json)
}
