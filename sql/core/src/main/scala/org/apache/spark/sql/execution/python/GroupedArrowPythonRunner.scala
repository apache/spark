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

import java.io.DataOutputStream

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonWorker}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType



/**
 * Python UDF Runner for grouped udfs.
 */
class GroupedArrowPythonRunner(
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    schema: StructType,
    timeZoneId: String,
    largeVarTypes: Boolean,
    arrowMaxRecordsPerBatch: Int,
    conf: Map[String, String],
    pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String],
    profiler: Option[String])
  extends ArrowPythonRunner(
    funcs, evalType, argOffsets, schema, timeZoneId, largeVarTypes, conf, pythonMetrics,
    jobArtifactUUID, profiler
  ) {

  override protected def newWriter(
      env: SparkEnv,
      worker: PythonWorker,
      inputIterator: Iterator[Iterator[InternalRow]],
      partitionIndex: Int,
      context: TaskContext): Writer = {

    new Writer(env, worker, inputIterator, partitionIndex, context) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        handleMetadataBeforeExec(dataOut)
        writeUDF(dataOut)
      }

      override def writeNextInputToStream(dataOut: DataOutputStream): Boolean = {
        writer = new ArrowStreamWriter(root, null, dataOut)
        writer.start()

        writeNextInputToArrowStream(root, writer, dataOut, inputIterator)
      }
    }
  }

  override protected def writeNextInputToArrowStream(
      root: VectorSchemaRoot,
      writer: ArrowStreamWriter,
      dataOut: DataOutputStream,
      inputIterator: Iterator[Iterator[InternalRow]]): Boolean = {
    val batchWriter = new BaseStreamingArrowWriter(root, writer, arrowMaxRecordsPerBatch)

    if (inputIterator.hasNext) {
      val startData = dataOut.size()
      val nextBatch = inputIterator.next()

      while (nextBatch.hasNext) {
        val dataRow = nextBatch.next()
        batchWriter.writeRow(dataRow)
      }
      batchWriter.finalizeCurrentArrowBatch()
      val deltaData = dataOut.size() - startData
      pythonMetrics("pythonDataSent") += deltaData
      writer.end()
      true
    } else {
      close()
      false
    }
  }
}

