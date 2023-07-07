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
import java.net.Socket

import scala.collection.JavaConverters._

import org.apache.spark.SparkEnv
import org.apache.spark.internal.config.BUFFER_SIZE
import org.apache.spark.internal.config.Python.{PYTHON_AUTH_SOCKET_TIMEOUT, PYTHON_USE_DAEMON}


private[spark] object StreamingPythonRunner {
  def apply(func: PythonFunction): StreamingPythonRunner = {
    new StreamingPythonRunner(func)
  }
}

private[spark] class StreamingPythonRunner(func: PythonFunction) {
  private val conf = SparkEnv.get.conf
  protected val bufferSize: Int = conf.get(BUFFER_SIZE)
  protected val authSocketTimeout = conf.get(PYTHON_AUTH_SOCKET_TIMEOUT)

  private val envVars: java.util.Map[String, String] = func.envVars
  private val pythonExec: String = func.pythonExec
  protected val pythonVer: String = func.pythonVer

  def init(sessionId: String): (DataOutputStream, DataInputStream) = {
    // scalastyle:off println
    println(s"##### init python runner for sessionId=$sessionId")
    println(s"##### init python runner pythonExec=$pythonExec")
    // scalastyle:on println

    val startTime = System.currentTimeMillis
    val env = SparkEnv.get

    val localdir = env.blockManager.diskBlockManager.localDirs.map(f => f.getPath()).mkString(",")
    envVars.put("SPARK_LOCAL_DIRS", localdir)

    envVars.put("SPARK_AUTH_SOCKET_TIMEOUT", authSocketTimeout.toString)
    envVars.put("SPARK_BUFFER_SIZE", bufferSize.toString)

    // For now, not use daemon
    conf.set(PYTHON_USE_DAEMON, false)

    // TODO: cache and reuse the pythonWorkerFactory
    val pythonWorkerFactory = new PythonWorkerFactory(pythonExec, envVars.asScala.toMap)
    val (worker: Socket, pid: Option[Int]) = pythonWorkerFactory.createStreamingWorker()

    val stream = new BufferedOutputStream(worker.getOutputStream, bufferSize)
    val dataOut = new DataOutputStream(stream)

    // TODO: verify python version

    // send sessionID
    PythonRDD.writeUTF(sessionId, dataOut)
    // scalastyle:off println
    println(s"##### sent sessionId to python")
    // scalastyle:on println

    // send the user function to python process
    val command = func.command
    dataOut.writeInt(command.length)
    dataOut.write(command.toArray)
    dataOut.flush()

    // scalastyle:off println
    println(s"##### sent func to python")
    // scalastyle:on println

    val dataIn = new DataInputStream(new BufferedInputStream(worker.getInputStream, bufferSize))
    // scalastyle:off println
    println(s"##### init dataIn")
    // scalastyle:on println

    val resFromPython = dataIn.readInt()
    // scalastyle:off println
    println(s"##### resFromPython = $resFromPython")
    // scalastyle:on println

    (dataOut, dataIn)
  }

}
