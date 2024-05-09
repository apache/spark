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

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream}

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkEnv
import org.apache.spark.api.python.{PythonFunction, PythonWorker, PythonWorkerFactory, PythonWorkerUtils, SpecialLengths}
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.PYTHON_EXEC
import org.apache.spark.internal.config.BUFFER_SIZE
import org.apache.spark.internal.config.Python.PYTHON_AUTH_SOCKET_TIMEOUT
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.StructType

/**
 * This class is a proxy to invoke commit or abort methods in Python DataSourceStreamWriter.
 * A runner spawns a python worker process. In the main function, set up communication
 * between JVM and python process through socket and create a DataSourceStreamWriter instance.
 * In an infinite loop, the python worker process receive write commit messages
 * from the socket, then commit or abort a microbatch.
 */
class PythonStreamingSinkCommitRunner(
    func: PythonFunction,
    schema: StructType,
    overwrite: Boolean) extends Logging {
  val workerModule: String = "pyspark.sql.worker.python_streaming_sink_runner"

  private val conf = SparkEnv.get.conf
  protected val bufferSize: Int = conf.get(BUFFER_SIZE)
  protected val authSocketTimeout = conf.get(PYTHON_AUTH_SOCKET_TIMEOUT)

  private val envVars: java.util.Map[String, String] = func.envVars
  private val pythonExec: String = func.pythonExec
  private var pythonWorker: Option[PythonWorker] = None
  private var pythonWorkerFactory: Option[PythonWorkerFactory] = None
  protected val pythonVer: String = func.pythonVer

  private var dataOut: DataOutputStream = null
  private var dataIn: DataInputStream = null

  /**
   * Initializes the Python worker for running the streaming sink committer.
   */
  def init(): Unit = {
    logInfo(log"Initializing Python runner pythonExec: ${MDC(PYTHON_EXEC, pythonExec)}")
    val env = SparkEnv.get

    val localdir = env.blockManager.diskBlockManager.localDirs.map(f => f.getPath()).mkString(",")
    envVars.put("SPARK_LOCAL_DIRS", localdir)

    envVars.put("SPARK_AUTH_SOCKET_TIMEOUT", authSocketTimeout.toString)
    envVars.put("SPARK_BUFFER_SIZE", bufferSize.toString)

    val workerFactory =
      new PythonWorkerFactory(pythonExec, workerModule, envVars.asScala.toMap, false)
    val (worker: PythonWorker, _) = workerFactory.createSimpleWorker(blockingMode = true)
    pythonWorker = Some(worker)
    pythonWorkerFactory = Some(workerFactory)

    val stream = new BufferedOutputStream(
      pythonWorker.get.channel.socket().getOutputStream, bufferSize)
    dataOut = new DataOutputStream(stream)

    PythonWorkerUtils.writePythonVersion(pythonVer, dataOut)

    val pythonIncludes = func.pythonIncludes.asScala.toSet
    PythonWorkerUtils.writeSparkFiles(Some("streaming_job"), pythonIncludes, dataOut)

    // Send the user function to python process
    PythonWorkerUtils.writePythonFunction(func, dataOut)

    PythonWorkerUtils.writeUTF(schema.json, dataOut)

    dataOut.writeBoolean(overwrite)

    dataOut.flush()

    dataIn = new DataInputStream(
      new BufferedInputStream(pythonWorker.get.channel.socket().getInputStream, bufferSize))

    val initStatus = dataIn.readInt()
    if (initStatus == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val msg = PythonWorkerUtils.readUTF(dataIn)
      throw QueryExecutionErrors.pythonStreamingDataSourceRuntimeError(
        action = "initialize streaming sink", msg)
    }
  }

  init()

  def commitOrAbort(
      messages: Array[WriterCommitMessage],
      batchId: Long,
      abort: Boolean): Unit = {
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
    dataOut.writeBoolean(abort)
    dataOut.flush()
    val status = dataIn.readInt()
    if (status == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val msg = PythonWorkerUtils.readUTF(dataIn)
      val action = if (abort) "abort" else "commit"
      throw QueryExecutionErrors.pythonStreamingDataSourceRuntimeError(action, msg)
    }
  }
}
