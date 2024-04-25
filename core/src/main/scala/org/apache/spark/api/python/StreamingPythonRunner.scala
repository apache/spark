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

package org.apache.spark.api.python

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream}

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkEnv, SparkPythonException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.BUFFER_SIZE
import org.apache.spark.internal.config.Python.PYTHON_AUTH_SOCKET_TIMEOUT


private[spark] object StreamingPythonRunner {
  def apply(
      func: PythonFunction,
      connectUrl: String,
      sessionId: String,
      workerModule: String
  ): StreamingPythonRunner = {
    new StreamingPythonRunner(func, connectUrl, sessionId, workerModule)
  }
}

private[spark] class StreamingPythonRunner(
    func: PythonFunction,
    connectUrl: String,
    sessionId: String,
    workerModule: String) extends Logging {
  private val conf = SparkEnv.get.conf
  protected val bufferSize: Int = conf.get(BUFFER_SIZE)
  protected val authSocketTimeout = conf.get(PYTHON_AUTH_SOCKET_TIMEOUT)

  private val envVars: java.util.Map[String, String] = func.envVars
  private val pythonExec: String = func.pythonExec
  private var pythonWorker: Option[PythonWorker] = None
  private var pythonWorkerFactory: Option[PythonWorkerFactory] = None
  protected val pythonVer: String = func.pythonVer

  /**
   * Initializes the Python worker for streaming functions. Sets up Spark Connect session
   * to be used with the functions.
   */
  def init(): (DataOutputStream, DataInputStream) = {
    logInfo(s"Initializing Python runner (session: $sessionId, pythonExec: $pythonExec)")
    val env = SparkEnv.get

    val localdir = env.blockManager.diskBlockManager.localDirs.map(f => f.getPath()).mkString(",")
    envVars.put("SPARK_LOCAL_DIRS", localdir)

    envVars.put("SPARK_AUTH_SOCKET_TIMEOUT", authSocketTimeout.toString)
    envVars.put("SPARK_BUFFER_SIZE", bufferSize.toString)
    envVars.put("SPARK_CONNECT_LOCAL_URL", connectUrl)

    val workerFactory =
      new PythonWorkerFactory(pythonExec, workerModule, envVars.asScala.toMap, false)
    val (worker: PythonWorker, _) = workerFactory.createSimpleWorker(blockingMode = true)
    pythonWorker = Some(worker)
    pythonWorkerFactory = Some(workerFactory)

    val stream = new BufferedOutputStream(
      pythonWorker.get.channel.socket().getOutputStream, bufferSize)
    val dataOut = new DataOutputStream(stream)

    PythonWorkerUtils.writePythonVersion(pythonVer, dataOut)

    // Send sessionId
    PythonRDD.writeUTF(sessionId, dataOut)

    // Send the user function to python process
    PythonWorkerUtils.writePythonFunction(func, dataOut)
    dataOut.flush()

    val dataIn = new DataInputStream(
      new BufferedInputStream(pythonWorker.get.channel.socket().getInputStream, bufferSize))

    val resFromPython = dataIn.readInt()
    if (resFromPython != 0) {
      val errMessage = PythonWorkerUtils.readUTF(dataIn)
      throw streamingPythonRunnerInitializationFailure(resFromPython, errMessage)
    }
    logInfo(s"Runner initialization succeeded (returned $resFromPython).")

    (dataOut, dataIn)
  }

  def streamingPythonRunnerInitializationFailure(resFromPython: Int, errMessage: String):
    StreamingPythonRunnerInitializationException = {
    new StreamingPythonRunnerInitializationException(resFromPython, errMessage)
  }

  class StreamingPythonRunnerInitializationException(resFromPython: Int, errMessage: String)
    extends SparkPythonException(
      errorClass = "STREAMING_PYTHON_RUNNER_INITIALIZATION_FAILURE",
      messageParameters = Map(
        "resFromPython" -> resFromPython.toString,
        "msg" -> errMessage))

  /**
   * Stops the Python worker.
   */
  def stop(): Unit = {
    logInfo(s"Stopping streaming runner for sessionId: $sessionId, module: $workerModule.")

    try {
      pythonWorkerFactory.foreach { factory =>
        pythonWorker.foreach { worker =>
          factory.stopWorker(worker)
          factory.stop()
        }
      }
    } catch {
      case e: Exception =>
        logError("Exception when trying to kill worker", e)
    }
  }

  /**
   * Returns whether the Python worker has been stopped.
   * @return Some(true) if the Python worker has been stopped.
   *         None if either the Python worker or the Python worker factory is not initialized.
   */
  def isWorkerStopped(): Option[Boolean] = {
    pythonWorkerFactory.flatMap { factory =>
      pythonWorker.map { worker =>
        factory.isWorkerStopped(worker)
      }
    }
  }
}
