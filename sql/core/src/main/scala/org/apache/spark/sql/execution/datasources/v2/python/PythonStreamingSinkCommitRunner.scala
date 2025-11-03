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

import java.io.{DataInputStream, DataOutputStream}

import net.razorvine.pickle.Pickler

import org.apache.spark.api.python.{PythonFunction, PythonWorkerUtils, SpecialLengths}
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.python.PythonPlannerRunner
import org.apache.spark.sql.types.StructType

/**
 * This class is a proxy to invoke commit or abort methods in Python DataSourceStreamWriter.
 * A runner spawns a python worker process. In the main function, set up communication
 * between JVM and python process through socket and create a DataSourceStreamWriter instance.
 * In an infinite loop, the python worker process receive write commit messages
 * from the socket, then commit or abort a microbatch.
 */
class PythonStreamingSinkCommitRunner(
    dataSourceCls: PythonFunction,
    schema: StructType,
    messages: Array[WriterCommitMessage],
    batchId: Long,
    overwrite: Boolean,
    abort: Boolean) extends PythonPlannerRunner[Unit](dataSourceCls) {
  override val workerModule: String = "pyspark.sql.worker.python_streaming_sink_runner"

  override protected def writeToPython(dataOut: DataOutputStream, pickler: Pickler): Unit = {
    // Send the user function to python process.
    PythonWorkerUtils.writePythonFunction(dataSourceCls, dataOut)
    // Send the output schema.
    PythonWorkerUtils.writeUTF(schema.json, dataOut)
    dataOut.writeBoolean(overwrite)

    // Send the commit messages.
    dataOut.writeInt(messages.length)
    messages.foreach { message =>
      // Commit messages can be null if there are task failures.
      if (message == null) {
        dataOut.writeInt(SpecialLengths.NULL)
      } else {
        PythonWorkerUtils.writeBytes(
          message.asInstanceOf[PythonWriterCommitMessage].pickledMessage, dataOut)
      }
    }
    dataOut.writeLong(batchId)
    // Send whether to invoke `abort` instead of `commit`.
    dataOut.writeBoolean(abort)
  }

  override protected def receiveFromPython(dataIn: DataInputStream): Unit = {
    // Receive any exceptions thrown in the Python worker.
    val code = dataIn.readInt()
    if (code == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val msg = PythonWorkerUtils.readUTF(dataIn)
      val action = if (abort) "abort" else "commit"
      throw QueryExecutionErrors.pythonStreamingDataSourceRuntimeError(action, msg)
    }
  }
}
