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

package org.apache.spark.sql.connect.planner

import java.io.EOFException

import org.apache.spark.SparkException
import org.apache.spark.api.python.{PythonException, PythonWorkerUtils, SimplePythonFunction, SpecialLengths, StreamingPythonRunner}
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.FUNCTION_NAME
import org.apache.spark.sql.connect.service.{SessionHolder, SparkConnectService}
import org.apache.spark.sql.streaming.StreamingQueryListener

/**
 * A helper class for handling StreamingQueryListener related functionality in Spark Connect. Each
 * instance of this class starts a python process, inside which has the python handling logic.
 * When a new event is received, it is serialized to json, and passed to the python process.
 */
class PythonStreamingQueryListener(listener: SimplePythonFunction, sessionHolder: SessionHolder)
    extends StreamingQueryListener
    with Logging {

  private val port = SparkConnectService.localPort
  private val connectUrl = s"sc://localhost:$port/;user_id=${sessionHolder.userId}"
  // Scoped for testing
  private[connect] val runner = StreamingPythonRunner(
    listener,
    connectUrl,
    sessionHolder.sessionId,
    "pyspark.sql.connect.streaming.worker.listener_worker")

  val (dataOut, dataIn) = runner.init()

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    PythonWorkerUtils.writeUTF(event.json, dataOut)
    dataOut.writeInt(0)
    dataOut.flush()
    handlePythonWorkerError("onQueryStarted")
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    PythonWorkerUtils.writeUTF(event.json, dataOut)
    dataOut.writeInt(1)
    dataOut.flush()
    handlePythonWorkerError("onQueryProgress")
  }

  override def onQueryIdle(event: StreamingQueryListener.QueryIdleEvent): Unit = {
    PythonWorkerUtils.writeUTF(event.json, dataOut)
    dataOut.writeInt(2)
    dataOut.flush()
    handlePythonWorkerError("onQueryIdle")
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    PythonWorkerUtils.writeUTF(event.json, dataOut)
    dataOut.writeInt(3)
    dataOut.flush()
    handlePythonWorkerError("onQueryTerminated")
  }

  private[spark] def stopListenerProcess(): Unit = {
    runner.stop()
  }

  // TODO: Reuse the same method in StreamingForeachBatchHelper to avoid duplication.
  private def handlePythonWorkerError(functionName: String): Unit = {
    try {
      dataIn.readInt() match {
        case 0 =>
          logInfo(
            log"Streaming query listener function ${MDC(FUNCTION_NAME, functionName)} " +
              log"completed (ret: 0)")
        case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
          val msg = PythonWorkerUtils.readUTF(dataIn)
          throw new PythonException(
            s"Found error inside Streaming query listener Python " +
              s"process for function $functionName: $msg",
            null)
        case otherValue =>
          throw new IllegalStateException(
            s"Unexpected return value $otherValue from the " +
              s"Python worker.")
      }
    } catch {
      case eof: EOFException =>
        throw new SparkException("Python worker exited unexpectedly (crashed)", eof)
    }
  }
}
