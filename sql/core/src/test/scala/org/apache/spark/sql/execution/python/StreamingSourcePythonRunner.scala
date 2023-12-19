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

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkEnv
import org.apache.spark.api.python.{PythonFunction, PythonWorker, PythonWorkerFactory, PythonWorkerUtils, SpecialLengths}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.BUFFER_SIZE
import org.apache.spark.internal.config.Python.{PYTHON_AUTH_SOCKET_TIMEOUT, PYTHON_USE_DAEMON}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType


class StreamingSourcePythonRunner(
    func: PythonFunction,
    inputSchema: StructType,
    outputSchema: StructType) extends Logging  {
  val workerModule = "pyspark.sql.worker.streaming_data_source_runner"

  private val conf = SparkEnv.get.conf
  protected val bufferSize: Int = conf.get(BUFFER_SIZE)
  protected val authSocketTimeout = conf.get(PYTHON_AUTH_SOCKET_TIMEOUT)

  private val envVars: java.util.Map[String, String] = func.envVars
  private val pythonExec: String = func.pythonExec
  private var pythonWorker: Option[PythonWorker] = None
  private var pythonWorkerFactory: Option[PythonWorkerFactory] = None
  protected val pythonVer: String = func.pythonVer

  /*
  EvaluatePython.registerPicklers()
  private val pickler = new Pickler(/* useMemo = */ true,
    /* valueCompare = */ false)
   */

  val partitions_func_id = 886
  val latestOffsets_func_id = 887
  val read_func_id = 888

  private var dataOut: DataOutputStream = null
  private var dataIn: DataInputStream = null

  /**
   * Initializes the Python worker for streaming functions. Sets up Spark Connect session
   * to be used with the functions.
   */
  def init(): Unit = {
    logInfo(s"Initializing Python runner pythonExec: $pythonExec")
    val env = SparkEnv.get

    val localdir = env.blockManager.diskBlockManager.localDirs.map(f => f.getPath()).mkString(",")
    envVars.put("SPARK_LOCAL_DIRS", localdir)

    envVars.put("SPARK_AUTH_SOCKET_TIMEOUT", authSocketTimeout.toString)
    envVars.put("SPARK_BUFFER_SIZE", bufferSize.toString)

    val prevConf = conf.get(PYTHON_USE_DAEMON)
    conf.set(PYTHON_USE_DAEMON, false)
    try {
      val workerFactory =
        new PythonWorkerFactory(pythonExec, workerModule, envVars.asScala.toMap)
      val (worker: PythonWorker, _) = workerFactory.createSimpleWorker(blockingMode = true)
      pythonWorker = Some(worker)
      pythonWorkerFactory = Some(workerFactory)
    } finally {
      conf.set(PYTHON_USE_DAEMON, prevConf)
    }

    val stream = new BufferedOutputStream(
      pythonWorker.get.channel.socket().getOutputStream, bufferSize)
    dataOut = new DataOutputStream(stream)

    PythonWorkerUtils.writePythonVersion(pythonVer, dataOut)

    val pythonIncludes = func.pythonIncludes.asScala.toSet
    PythonWorkerUtils.writeSparkFiles(Some("afd"), pythonIncludes, dataOut)

    // Send the user function to python process
    PythonWorkerUtils.writePythonFunction(func, dataOut)


    // Send input schema
    PythonWorkerUtils.writeUTF(inputSchema.json, dataOut)

    // Send output schema
    PythonWorkerUtils.writeUTF(outputSchema.json, dataOut)

    // Send configurations
    dataOut.writeInt(SQLConf.get.arrowMaxRecordsPerBatch)
    dataOut.flush()

    dataIn = new DataInputStream(
      new BufferedInputStream(pythonWorker.get.channel.socket().getInputStream, bufferSize))

    assert(dataIn.readInt() == 0)
  }

  def latestOffset(): String = {
    dataOut.writeInt(latestOffsets_func_id)
    dataOut.flush()
    PythonWorkerUtils.readUTF(dataIn)
  }

  def partitions(start: String, end: String): Int = {
    dataOut.writeInt(partitions_func_id)
    dataOut.writeUTF(start)
    dataOut.writeUTF(end)
    dataOut.flush()
    // Receive the list of partitions, if any.
    val pickledPartitions = ArrayBuffer.empty[Array[Byte]]
    val numPartitions = dataIn.readInt()

      /*
    for (_ <- 0 until numPartitions) {
      val pickledPartition: Array[Byte] = PythonWorkerUtils.readBytes(dataIn)
      pickledPartitions.append(pickledPartition)
    }
    pickledPartitions.toList
       */
  }

  def read(): Unit = {
    dataOut.writeInt(read_func_id)
    dataOut.flush()
    readIntOrThrow(read_func_id)
  }

  private def readIntOrThrow(expected: Int): Unit = {
    val res = dataIn.readInt()
    if (res != expected) {
      assert(res == SpecialLengths.PYTHON_EXCEPTION_THROWN)
      val error = PythonWorkerUtils.readUTF(dataIn)
      println(s"ppd $error")
      logInfo(s"Error (returned $error).")
      assert(false)
    }
  }

  def stop(): Unit = {
    logInfo(s"Stopping streaming runner for module: $workerModule.")

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
}
