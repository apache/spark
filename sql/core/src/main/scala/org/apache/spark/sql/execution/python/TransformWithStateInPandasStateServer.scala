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
import java.net.ServerSocket

import com.google.protobuf.ByteString

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.{StatefulProcessorHandleImpl, StatefulProcessorHandleState}
import org.apache.spark.sql.execution.streaming.state.StateMessage.{HandleState, StateRequest}

class TransformWithStateInPandasStateServer(
    private val stateServerSocket: ServerSocket,
    private val statefulProcessorHandle: StatefulProcessorHandleImpl)
  extends Runnable
  with Logging{

  private var inputStream: DataInputStream = _
  private var outputStream: DataOutputStream = _

  def run(): Unit = {
    logWarning(s"Waiting for connection from Python worker")
    val listeningSocket = stateServerSocket.accept()
    logWarning(s"listening on socket - ${listeningSocket.getLocalAddress}")

    inputStream = new DataInputStream(
      new BufferedInputStream(listeningSocket.getInputStream))
    outputStream = new DataOutputStream(
      new BufferedOutputStream(listeningSocket.getOutputStream)
    )

    while (listeningSocket.isConnected &&
      statefulProcessorHandle.getHandleState != StatefulProcessorHandleState.CLOSED) {

      logWarning(s"reading the version")
      val version = inputStream.readInt()

      if (version != -1) {
        logWarning(s"version = ${version}")
        assert(version == 0)
        val messageLen = inputStream.readInt()
        logWarning(s"parsing a message of ${messageLen} bytes")

        val messageBytes = new Array[Byte](messageLen)
        inputStream.read(messageBytes)
        logWarning(s"read bytes = ${messageBytes.mkString("Array(", ", ", ")")}")

        val message = StateRequest.parseFrom(ByteString.copyFrom(messageBytes))

        logWarning(s"read message = $message")
        handleRequest(message)
        logWarning(s"flush output stream")

        outputStream.writeInt(0)
        outputStream.flush()
      }
    }

    logWarning(s"done from the state server thread")
  }

  private def handleRequest(message: StateRequest): Unit = {
    if (message.getMethodCase == StateRequest.MethodCase.STATEFULPROCESSORCALL) {
      val statefulProcessorHandleRequest = message.getStatefulProcessorCall
      val requestedState = statefulProcessorHandleRequest.getSetHandleState.getState
      requestedState match {
        case HandleState.CREATED =>
          logWarning(s"set handle state to Initialized")
          statefulProcessorHandle.setHandleState(StatefulProcessorHandleState.CREATED)
        case HandleState.INITIALIZED =>
          logWarning(s"set handle state to Initialized")
          statefulProcessorHandle.setHandleState(StatefulProcessorHandleState.INITIALIZED)
        case HandleState.CLOSED =>
          logWarning(s"set handle state to Initialized")
          statefulProcessorHandle.setHandleState(StatefulProcessorHandleState.CLOSED)
        case _ =>
      }
    }
  }
}
