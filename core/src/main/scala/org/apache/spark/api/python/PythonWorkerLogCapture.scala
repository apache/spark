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

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkEnv
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.storage.{PythonWorkerLogBlockIdGenerator, PythonWorkerLogLine, RollingLogWriter}

/**
 * Manages Python UDF log capture and routing to per-worker log writers.
 *
 * This class handles the parsing of Python worker output streams and routes
 * log messages to appropriate rolling log writers based on worker PIDs.
 * Works for both daemon and non-daemon modes.
 */
private[python] class PythonWorkerLogCapture(
    sessionId: String,
    logMarker: String = "PYTHON_WORKER_LOGGING") extends Logging {

  // Map to track per-worker log writers: workerId(PID) -> (writer, sequenceId)
  private val workerLogWriters = new ConcurrentHashMap[String, (RollingLogWriter, AtomicLong)]()

  /**
   * Creates an InputStream wrapper that captures Python UDF logs from the given stream.
   *
   * @param inputStream The input stream to wrap (typically daemon stdout or worker stdout)
   * @return A wrapped InputStream that captures and routes log messages
   */
  def wrapInputStream(inputStream: InputStream): InputStream = {
    new CaptureWorkerLogsInputStream(inputStream)
  }

  /**
   * Removes and closes the log writer for a specific worker.
   *
   * @param workerId The worker ID (typically PID as string)
   */
  def removeAndCloseWorkerLogWriter(workerId: String): Unit = {
    Option(workerLogWriters.remove(workerId)).foreach { case (writer, _) =>
      try {
        writer.close()
      } catch {
        case e: Exception =>
          logWarning(
            log"Failed to close log writer for worker ${MDC(LogKeys.PYTHON_WORKER_ID, workerId)}",
            e)
      }
    }
  }

  /**
   * Closes all active worker log writers.
   */
  def closeAllWriters(): Unit = {
    workerLogWriters.asScala.foreach { case (workerId, (writer, _)) =>
      try {
        writer.close()
      } catch {
        case e: Exception =>
          logWarning(
            log"Failed to close log writer for worker ${MDC(LogKeys.PYTHON_WORKER_ID, workerId)}",
            e)
      }
    }
    workerLogWriters.clear()
  }

  /**
   * Gets or creates a log writer for the specified worker.
   *
   * @param workerId Unique identifier for the worker (typically PID)
   * @return Tuple of (RollingLogWriter, AtomicLong sequence counter)
   */
  private def getOrCreateLogWriter(workerId: String): (RollingLogWriter, AtomicLong) = {
    workerLogWriters.computeIfAbsent(workerId, _ => {
      val logWriter = SparkEnv.get.blockManager.getRollingLogWriter(
        new PythonWorkerLogBlockIdGenerator(sessionId, workerId)
      )
      (logWriter, new AtomicLong())
    })
  }

  /**
   * Processes a log line from a Python worker.
   *
   * @param line The complete line containing the log marker and JSON
   * @return The prefix (non-log content) that should be passed through
   */
  private def processLogLine(line: String): String = {
    val markerIndex = line.indexOf(s"$logMarker:")
    if (markerIndex >= 0) {
      val prefix = line.substring(0, markerIndex)
      val markerAndJson = line.substring(markerIndex)

      // Parse: "PYTHON_UDF_LOGGING:12345:{json}"
      val parts = markerAndJson.split(":", 3)
      if (parts.length >= 3) {
        val workerId = parts(1) // This is the PID from Python worker
        val json = parts(2)

        try {
          if (json.isEmpty) {
            removeAndCloseWorkerLogWriter(workerId)
          } else {
            val (writer, seqId) = getOrCreateLogWriter(workerId)
            writer.writeLog(
              PythonWorkerLogLine(System.currentTimeMillis(), seqId.getAndIncrement(), json)
            )
          }
        } catch {
          case e: Exception =>
            logWarning(
              log"Failed to write log for worker ${MDC(LogKeys.PYTHON_WORKER_ID, workerId)}", e)
        }
      }
      prefix
    } else {
      line + System.lineSeparator()
    }
  }

  /**
   * InputStream wrapper that captures and processes Python UDF logs.
   */
  private class CaptureWorkerLogsInputStream(in: InputStream) extends InputStream {

    private[this] val reader = new BufferedReader(
      new InputStreamReader(in, StandardCharsets.ISO_8859_1))
    private[this] val temp = new Array[Byte](1)
    private[this] var buffer = ByteBuffer.allocate(0)

    override def read(): Int = {
      val n = read(temp)
      if (n <= 0) {
        -1
      } else {
        // Signed byte to unsigned integer
        temp(0) & 0xff
      }
    }

    override def read(b: Array[Byte], off: Int, len: Int): Int = {
      if (buffer.hasRemaining) {
        val buf = ByteBuffer.wrap(b, off, len)
        val remaining = Math.min(buffer.remaining(), buf.remaining())
        buf.put(buf.position(), buffer, buffer.position(), remaining)
        buffer.position(buffer.position() + remaining)
        remaining
      } else {
        val line = reader.readLine()
        if (line == null) {
          closeAllWriters()
          -1
        } else {
          val processedContent = if (line.contains(s"$logMarker:")) {
            processLogLine(line)
          } else {
            line + System.lineSeparator()
          }

          buffer = ByteBuffer.wrap(processedContent.getBytes(StandardCharsets.ISO_8859_1))
          read(b, off, len)
        }
      }
    }

    override def close(): Unit = {
      try {
        reader.close()
      } finally {
        closeAllWriters()
      }
    }
  }
}
