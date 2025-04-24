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

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream, InputStream, IOException}
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.file.{Files => JavaFiles}
import java.util.HashMap
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._

import net.razorvine.pickle.Pickler

import org.apache.spark.{JobArtifactSet, SparkEnv, SparkException}
import org.apache.spark.api.python.{BasePythonRunner, PythonFunction, PythonWorker, PythonWorkerUtils, SpecialLengths}
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.internal.config.BUFFER_SIZE
import org.apache.spark.internal.config.Python._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.DirectByteBufferOutputStream

/**
 * A helper class to run Python functions in Spark driver.
 */
abstract class PythonPlannerRunner[T](func: PythonFunction) extends Logging {
  import BasePythonRunner._

  protected val workerModule: String

  protected def writeToPython(dataOut: DataOutputStream, pickler: Pickler): Unit

  protected def receiveFromPython(dataIn: DataInputStream): T

  def runInPython(useDaemon: Boolean = SparkEnv.get.conf.get(PYTHON_USE_DAEMON)): T = {
    val env = SparkEnv.get
    val bufferSize: Int = env.conf.get(BUFFER_SIZE)
    val authSocketTimeout = env.conf.get(PYTHON_AUTH_SOCKET_TIMEOUT)
    val reuseWorker = env.conf.get(PYTHON_WORKER_REUSE)
    val localdir = env.blockManager.diskBlockManager.localDirs.map(f => f.getPath()).mkString(",")
    val faultHandlerEnabled: Boolean = SQLConf.get.pythonUDFWorkerFaulthandlerEnabled
    val idleTimeoutSeconds: Long = SQLConf.get.pythonUDFWorkerIdleTimeoutSeconds
    val killOnIdleTimeout: Boolean = SQLConf.get.pythonUDFWorkerKillOnIdleTimeout
    val hideTraceback: Boolean = SQLConf.get.pysparkHideTraceback
    val simplifiedTraceback: Boolean = SQLConf.get.pysparkSimplifiedTraceback
    val workerMemoryMb = SQLConf.get.pythonPlannerExecMemory

    val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)

    val envVars = new HashMap[String, String](func.envVars)
    val pythonExec = func.pythonExec
    val pythonVer = func.pythonVer
    val pythonIncludes = func.pythonIncludes.asScala.toSet
    val broadcastVars = func.broadcastVars.asScala.toSeq
    val maybeAccumulator = Option(func.accumulator).map(_.copyAndReset())

    envVars.put("SPARK_LOCAL_DIRS", localdir)
    if (reuseWorker) {
      envVars.put("SPARK_REUSE_WORKER", "1")
    }
    if (hideTraceback) {
      envVars.put("SPARK_HIDE_TRACEBACK", "1")
    }
    if (simplifiedTraceback) {
      envVars.put("SPARK_SIMPLIFIED_TRACEBACK", "1")
    }
    workerMemoryMb.foreach { memoryMb =>
      envVars.put("PYSPARK_PLANNER_MEMORY_MB", memoryMb.toString)
    }
    envVars.put("SPARK_AUTH_SOCKET_TIMEOUT", authSocketTimeout.toString)
    envVars.put("SPARK_BUFFER_SIZE", bufferSize.toString)
    if (faultHandlerEnabled) {
      envVars.put("PYTHON_FAULTHANDLER_DIR", faultHandlerLogDir.toString)
    }

    envVars.put("SPARK_JOB_ARTIFACT_UUID", jobArtifactUUID.getOrElse("default"))

    EvaluatePython.registerPicklers()
    val pickler = new Pickler(/* useMemo = */ true,
      /* valueCompare = */ false)

    val (worker: PythonWorker, handle: Option[ProcessHandle]) =
      env.createPythonWorker(pythonExec, workerModule, envVars.asScala.toMap, useDaemon)
    val pid = handle.map(_.pid.toInt)
    var releasedOrClosed = false
    val bufferStream = new DirectByteBufferOutputStream()
    try {
      val dataOut = new DataOutputStream(new BufferedOutputStream(bufferStream, bufferSize))

      PythonWorkerUtils.writePythonVersion(pythonVer, dataOut)
      PythonWorkerUtils.writeSparkFiles(jobArtifactUUID, pythonIncludes, dataOut)
      PythonWorkerUtils.writeBroadcasts(broadcastVars, worker, env, dataOut)

      writeToPython(dataOut, pickler)

      dataOut.writeInt(SpecialLengths.END_OF_STREAM)
      dataOut.flush()

      val dataIn = new DataInputStream(new BufferedInputStream(
        new WorkerInputStream(
          worker, bufferStream.toByteBuffer, handle, idleTimeoutSeconds, killOnIdleTimeout),
        bufferSize))

      val res = receiveFromPython(dataIn)

      PythonWorkerUtils.receiveAccumulatorUpdates(maybeAccumulator, dataIn)
      Option(func.accumulator).foreach(_.merge(maybeAccumulator.get))

      dataIn.readInt() match {
        case SpecialLengths.END_OF_STREAM if reuseWorker =>
          env.releasePythonWorker(pythonExec, workerModule, envVars.asScala.toMap, worker)
        case _ =>
          env.destroyPythonWorker(pythonExec, workerModule, envVars.asScala.toMap, worker)
      }
      releasedOrClosed = true

      res
    } catch {
      case e: IOException if faultHandlerEnabled && pid.isDefined &&
        JavaFiles.exists(faultHandlerLogPath(pid.get)) =>
        val path = faultHandlerLogPath(pid.get)
        val error = String.join("\n", JavaFiles.readAllLines(path)) + "\n"
        JavaFiles.deleteIfExists(path)
        throw new SparkException(s"Python worker exited unexpectedly (crashed): $error", e)

      case e: IOException if !faultHandlerEnabled =>
        throw new SparkException(
          s"Python worker exited unexpectedly (crashed). " +
            "Consider setting 'spark.sql.execution.pyspark.udf.faulthandler.enabled' or" +
            s"'${PYTHON_WORKER_FAULTHANLDER_ENABLED.key}' configuration to 'true' for " +
            "the better Python traceback.", e)

      case e: IOException =>
        throw new SparkException("Python worker exited unexpectedly (crashed)", e)
    } finally {
      try {
        bufferStream.close()
      } finally {
        if (!releasedOrClosed) {
          // An error happened. Force to close the worker.
          env.destroyPythonWorker(pythonExec, workerModule, envVars.asScala.toMap, worker)
        }
      }
    }
  }

  /**
   * A wrapper of the non-blocking IO to write to/read from the worker.
   *
   * Since we use non-blocking IO to communicate with workers; see SPARK-44705,
   * a wrapper is needed to do IO with the worker.
   * This is a port and simplified version of `PythonRunner.ReaderInputStream`,
   * and only supports to write all at once and then read all.
   */
  private class WorkerInputStream(
      worker: PythonWorker,
      buffer: ByteBuffer,
      handle: Option[ProcessHandle],
      idleTimeoutSeconds: Long,
      killOnIdleTimeout: Boolean) extends InputStream {

    private[this] val temp = new Array[Byte](1)

    override def read(): Int = {
      val n = read(temp)
      if (n <= 0) {
        -1
      } else {
        // Signed byte to unsigned integer
        temp(0) & 0xff
      }
    }

    private[this] val idleTimeoutMillis: Long = TimeUnit.SECONDS.toMillis(idleTimeoutSeconds)
    private[this] var pythonWorkerKilled: Boolean = false

    override def read(b: Array[Byte], off: Int, len: Int): Int = {
      val buf = ByteBuffer.wrap(b, off, len)
      var n = 0
      while (n == 0) {
        val start = System.currentTimeMillis()
        val selected = worker.selector.select(idleTimeoutMillis)
        val end = System.currentTimeMillis()
        if (selected == 0
          // Avoid logging if no timeout or the selector doesn't wait for the idle timeout
          // as it can return 0 in some case.
          && idleTimeoutMillis > 0 && (end - start) >= idleTimeoutMillis) {
          if (pythonWorkerKilled) {
            logWarning(
              log"Waiting for Python planner worker process to terminate after idle timeout: " +
              pythonWorkerStatusMessageWithContext(handle, worker, buffer.hasRemaining))
          } else {
            logWarning(
              log"Idle timeout reached for Python planner worker (timeout: " +
              log"${MDC(LogKeys.PYTHON_WORKER_IDLE_TIMEOUT, idleTimeoutSeconds)} seconds). " +
              log"No data received from the worker process: " +
              pythonWorkerStatusMessageWithContext(handle, worker, buffer.hasRemaining))
            if (killOnIdleTimeout) {
              handle.foreach { handle =>
                if (handle.isAlive) {
                  logWarning(
                    log"Terminating Python planner worker process due to idle timeout (timeout: " +
                    log"${MDC(LogKeys.PYTHON_WORKER_IDLE_TIMEOUT, idleTimeoutSeconds)} seconds)")
                  pythonWorkerKilled = handle.destroy()
                }
              }
            }
          }
        }
        if (worker.selectionKey.isReadable) {
          n = worker.channel.read(buf)
        }
        if (worker.selectionKey.isWritable) {
          var acceptsInput = true
          while (acceptsInput && buffer.hasRemaining) {
            val n = worker.channel.write(buffer)
            acceptsInput = n > 0
          }
          if (!buffer.hasRemaining) {
            // We no longer have any data to write to the socket.
            worker.selectionKey.interestOps(SelectionKey.OP_READ)
          }
        }
      }
      n
    }
  }
}
