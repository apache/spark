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

import org.apache.spark.api.python.{PythonRDD, SimplePythonFunction, StreamingPythonRunner}
import org.apache.spark.sql.connect.service.{SessionHolder, SparkConnectService}
import org.apache.spark.sql.streaming.StreamingQueryListener

/**
 * A helper class for handling StreamingQueryListener related functionality in Spark Connect. Each
 * instance of this class starts a python process, inside which has the python handling logic.
 * When a new event is received, it is serialized to json, and passed to the python process.
 */
class PythonStreamingQueryListener(
    listener: SimplePythonFunction,
    sessionHolder: SessionHolder,
    pythonExec: String)
    extends StreamingQueryListener {

  private val port = SparkConnectService.localPort
  private val connectUrl = s"sc://localhost:$port/;user_id=${sessionHolder.userId}"
  private val runner = StreamingPythonRunner(
    listener,
    connectUrl,
    sessionHolder.sessionId,
    "pyspark.sql.connect.streaming.worker.listener_worker")

  val (dataOut, _) = runner.init()

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    PythonRDD.writeUTF(event.json, dataOut)
    dataOut.writeInt(0)
    dataOut.flush()
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    PythonRDD.writeUTF(event.json, dataOut)
    dataOut.writeInt(1)
    dataOut.flush()
  }

  override def onQueryIdle(event: StreamingQueryListener.QueryIdleEvent): Unit = {
    PythonRDD.writeUTF(event.json, dataOut)
    dataOut.writeInt(2)
    dataOut.flush()
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    PythonRDD.writeUTF(event.json, dataOut)
    dataOut.writeInt(3)
    dataOut.flush()
  }

  private[spark] def stopListenerProcess(): Unit = {
    runner.stop()
  }
}
