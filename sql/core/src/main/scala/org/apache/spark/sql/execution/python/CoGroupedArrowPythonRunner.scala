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
import java.net.Socket

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{BasePythonRunner, ChainedPythonFunctions, PythonRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

/**
 * Python UDF Runner for cogrouped udfs. It sends Arrow bathes from different DataFrames,
 * groups them in Python, and receive it back in JVM as batches of single DataFrame.
 */
class CoGroupedArrowPythonRunner(
                                       funcs: Seq[ChainedPythonFunctions],
                                       evalType: Int,
                                       argOffsets: Array[Array[Int]],
                                       schemas: List[StructType],
                                       timeZoneId: String,
                                       conf: Map[String, String],
                                       val pythonMetrics: Map[String, SQLMetric])
  extends BasePythonRunner[List[Iterator[InternalRow]], ColumnarBatch](funcs, evalType, argOffsets)
    with BasicPythonArrowOutput {

  override val simplifiedTraceback: Boolean = SQLConf.get.pysparkSimplifiedTraceback

  protected def newWriterThread(
                                 env: SparkEnv,
                                 worker: Socket,
                                 inputIterator: Iterator[List[Iterator[InternalRow]]],
                                 partitionIndex: Int,
                                 context: TaskContext): WriterThread = {
    new WriterThread(env, worker, inputIterator, partitionIndex, context) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        // Write config for the worker as a number of key -> value pairs of strings
        dataOut.writeInt(conf.size)
        for ((k, v) <- conf) {
          PythonRDD.writeUTF(k, dataOut)
          PythonRDD.writeUTF(v, dataOut)
        }

        PythonUDFRunner.writeUDFs(dataOut, funcs, argOffsets)
      }

      protected override def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        // For each we first send the number of dataframes in each group then send
        // first df, then send second df.  End of data is marked by sending 0.
        while (inputIterator.hasNext) {
          val startData = dataOut.size()
          val input = inputIterator.next()
          dataOut.writeInt(input.length)
          (input, schemas, Range.inclusive(1, input.length)).zipped.map({
            case (data: Iterator[InternalRow], schema: StructType, index) =>
              writeGroup(data, schema, dataOut, s"df_$index")
          })
          val deltaData = dataOut.size() - startData
          pythonMetrics("pythonDataSent") += deltaData
        }
        dataOut.writeInt(0)
      }

      private def writeGroup(
                              group: Iterator[InternalRow],
                              schema: StructType,
                              dataOut: DataOutputStream,
                              name: String): Unit = {
        val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
        val allocator = ArrowUtils.rootAllocator.newChildAllocator(
          s"stdout writer for $pythonExec ($name)",
          0,
          Long.MaxValue)
        val root = VectorSchemaRoot.create(arrowSchema, allocator)

        Utils.tryWithSafeFinally {
          val writer = new ArrowStreamWriter(root, null, dataOut)
          val arrowWriter = ArrowWriter.create(root)
          writer.start()

          while (group.hasNext) {
            arrowWriter.write(group.next())
          }
          arrowWriter.finish()
          writer.writeBatch()
          writer.end()
        } {
          root.close()
          allocator.close()
        }
      }
    }
  }
}
