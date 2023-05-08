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

import java.util.Base64

import scala.collection.JavaConverters._
import scala.io.Source

import org.apache.spark.api.python.PythonUtils
import org.apache.spark.connect.proto
import org.apache.spark.sql.streaming.StreamingQueryListener

class PythonStreamingQueryListener(
      listener: proto.AddStreamingQueryListenerCommand,
      sessionId: String,
      pythonExec: String) extends StreamingQueryListener {
    // Start a process to run foreachbatch python func
    // TODO: Reuse some functions from PythonRunner.scala
    // TODO: Handle process better: reuse process; release process; monitor process
  // TODO(wei) reuse process
//    val envVars = udf.func.envVars.asScala.toMap

    val pb = new ProcessBuilder()
    val pbEnv = pb.environment()
    val pythonPath = PythonUtils.mergePythonPaths(
      PythonUtils.sparkPythonPath,
      envVars.getOrElse("PYTHONPATH", ""),
      sys.env.getOrElse("PYTHONPATH", ""))
    pbEnv.put("PYTHONPATH", pythonPath)
//    pbEnv.putAll(envVars.asJava)

    pb.command(pythonExec)

    // Encode serialized func as string so that it can be passed into the process through
    // arguments
    val onQueryStartedBytes = listener.getOnQueryStarted.toByteArray.asJava
    val onQueryStartedStr = Base64.getEncoder().encodeToString(onQueryStartedBytes)

  // Output for debug for now.
  // TODO: redirect the output stream
  // TODO: handle error

  // TODO(Wei): serialize and deserialize events

    override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
      val id = event.id.toString
      val runId = event.runId.toString
      val name = event.name
      val ts = event.timestamp

      // pb.command(pythonExec, "-c", pythonScript, dfRefId, batchId.toString, forEachBatchStr)
      val pythonScript = s"""
      |print('###### Start running onQueryStarted ######')
      |from pyspark.sql import SparkSession
      |from pyspark.serializers import CloudPickleSerializer
      |from pyspark.sql.connect.streaming.listener import StreamingQueryListener
      |import sys
      |import base64
      |
      |startEvent = QueryStartedEvent('$id', '$runId', '$name', '$ts')
      |sessionId = '$sessionId'
      |sparkConnectSession = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
      |sparkConnectSession._client._session_id = sessionId
      |
      |bytes = base64.b64decode(onQueryStartedStr)
      |func = CloudPickleSerializer().loads(bytes)
      |# forEachBatchFunc = unpickledCode[0]
      |func(startEvent)
      |exit()
      """.stripMargin
      pb.command(pythonExec, "-c", pythonScript)
      val process = pb.start()
      // Output for debug for now.
      // TODO: redirect the output stream
      // TODO: handle error
      val is = process.getInputStream()
      val out = Source.fromInputStream(is).mkString
      println(s"##### Python out for query start event is: out=$out")

      val es = process.getErrorStream
      val errorOut = Source.fromInputStream(es).mkString
      println(s"##### Python error for query start event is: error=$errorOut")

      val exitCode = process.waitFor()
      println(s"##### End processing query start event exitCode=$exitCode")
    }

    override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {}

    override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
}