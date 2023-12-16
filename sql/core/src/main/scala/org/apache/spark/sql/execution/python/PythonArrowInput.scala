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
import org.apache.spark.api.python.{BasePythonRunner, PythonRDD, PythonWorker}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.Utils

/**
 * A trait that can be mixed-in with [[BasePythonRunner]]. It implements the logic from
 * JVM (an iterator of internal rows + additional data if required) to Python (Arrow).
 */
private[python] trait PythonArrowInput[IN] { self: BasePythonRunner[IN, _] =>
  protected val workerConf: Map[String, String]

  protected val schema: StructType

  protected val timeZoneId: String

  protected val errorOnDuplicatedFieldNames: Boolean

  protected val largeVarTypes: Boolean

  protected def pythonMetrics: Map[String, SQLMetric]

  protected def writeNextInputToArrowStream(
      root: VectorSchemaRoot,
      writer: ArrowStreamWriter,
      dataOut: DataOutputStream,
      inputIterator: Iterator[IN]): Boolean

  protected def writeUDF(dataOut: DataOutputStream): Unit

  protected def handleMetadataBeforeExec(stream: DataOutputStream): Unit = {
    // Write config for the worker as a number of key -> value pairs of strings
    stream.writeInt(workerConf.size)
    for ((k, v) <- workerConf) {
      PythonRDD.writeUTF(k, stream)
      PythonRDD.writeUTF(v, stream)
    }
  }
  private val arrowSchema = ArrowUtils.toArrowSchema(
    schema, timeZoneId, errorOnDuplicatedFieldNames, largeVarTypes)
  private val allocator =
    ArrowUtils.rootAllocator.newChildAllocator(s"stdout writer for $pythonExec", 0, Long.MaxValue)
  protected val root = VectorSchemaRoot.create(arrowSchema, allocator)
  protected var writer: ArrowStreamWriter = _

protected def close(): Unit = {
  Utils.tryWithSafeFinally {
    // end writes footer to the output stream and doesn't clean any resources.
    // It could throw exception if the output stream is closed, so it should be
    // in the try block.
    writer.end()
  } {
    root.close()
    allocator.close()
  }
}

  protected override def newWriter(
      env: SparkEnv,
      worker: PythonWorker,
      inputIterator: Iterator[IN],
      partitionIndex: Int,
      context: TaskContext): Writer = {
    new Writer(env, worker, inputIterator, partitionIndex, context) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        handleMetadataBeforeExec(dataOut)
        writeUDF(dataOut)
      }

      override def writeNextInputToStream(dataOut: DataOutputStream): Boolean = {

        if (writer == null) {
          writer = new ArrowStreamWriter(root, null, dataOut)
          writer.start()
        }

        assert(writer != null)
        writeNextInputToArrowStream(root, writer, dataOut, inputIterator)
      }
    }
  }
}

private[python] trait BasicPythonArrowInput extends PythonArrowInput[Iterator[InternalRow]] {
  self: BasePythonRunner[Iterator[InternalRow], _] =>
  private val arrowWriter: arrow.ArrowWriter = ArrowWriter.create(root)

  protected def writeNextInputToArrowStream(
      root: VectorSchemaRoot,
      writer: ArrowStreamWriter,
      dataOut: DataOutputStream,
      inputIterator: Iterator[Iterator[InternalRow]]): Boolean = {

    if (inputIterator.hasNext) {
      val startData = dataOut.size()
      val nextBatch = inputIterator.next()

      while (nextBatch.hasNext) {
        arrowWriter.write(nextBatch.next())
      }

      arrowWriter.finish()
      writer.writeBatch()
      arrowWriter.reset()
      val deltaData = dataOut.size() - startData
      pythonMetrics("pythonDataSent") += deltaData
      true
    } else {
      super[PythonArrowInput].close()
      false
    }
  }
}
