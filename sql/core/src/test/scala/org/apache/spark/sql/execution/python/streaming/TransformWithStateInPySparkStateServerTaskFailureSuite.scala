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

package org.apache.spark.sql.execution.python.streaming

import java.io.{DataInputStream, DataOutputStream, IOException}
import java.net.SocketAddress
import java.nio.channels.{Channels, SocketChannel}
import java.util.UUID

import com.google.protobuf.ByteString

import org.apache.spark.{TaskContext, TaskKilledException}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.statefulprocessor.StatefulProcessorHandleImpl
import org.apache.spark.sql.execution.streaming.state.{RocksDBStateStoreProvider, StateVariableSuiteBase}
import org.apache.spark.sql.execution.streaming.state.StateMessage.{Get, ImplicitGroupingKeyRequest, SetImplicitKey, StateCallCommand, StatefulProcessorCall, StateRequest, StateResponse, StateVariableRequest, ValueStateCall}
import org.apache.spark.sql.streaming.TimeMode
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 * Drives a real state server, over a socket, against a real RocksDB store across a task failure:
 * a Python worker that is still running must not reach the store once the task has failed.
 */
class TransformWithStateInPySparkStateServerTaskFailureSuite extends StateVariableSuiteBase {

  private val groupingKeySchema = new StructType()

  private val getValueState = StateRequest.newBuilder().setStatefulProcessorCall(
    StatefulProcessorCall.newBuilder().setGetValueState(
      StateCallCommand.newBuilder()
        .setStateName("count")
        .setSchema(new StructType().add("value", IntegerType).json))).build()
  private val setImplicitKey = StateRequest.newBuilder().setImplicitGroupingKeyRequest(
    ImplicitGroupingKeyRequest.newBuilder().setSetImplicitKey(
      SetImplicitKey.newBuilder().setKey(
        // Pickled empty tuple, matching the empty groupingKeySchema.
        ByteString.copyFrom(Array(0x80.toByte, 0x03.toByte, ')'.toByte, '.'.toByte))))).build()
  private val getValue = StateRequest.newBuilder().setStateVariableRequest(
    StateVariableRequest.newBuilder().setValueStateCall(
      ValueStateCall.newBuilder().setStateName("count").setGet(Get.newBuilder()))).build()

  /** Starts the state server the way the executor-side Python runner does. */
  private class PythonRunner extends TransformWithStateInPySparkPythonRunnerUtils {
    def start(handle: StatefulProcessorHandleImpl, context: TaskContext): SocketAddress = {
      initStateServer()
      startStateServerForTask(
        new TransformWithStateInPySparkStateServer(
          stateServerSocket, handle, groupingKeySchema, 100),
        context)
      stateServerSocket.getLocalAddress
    }
  }

  /** Sends a request the way the Python worker does and reads back the response. */
  private def call(
      in: DataInputStream, out: DataOutputStream, request: StateRequest): StateResponse = {
    val bytes = request.toByteArray
    out.writeInt(0)
    out.writeInt(bytes.length)
    out.write(bytes)
    out.flush()
    val response = new Array[Byte](in.readInt())
    in.readFully(response)
    StateResponse.parseFrom(response)
  }

  /**
   * Opens a store and starts a state server, serves a state call, then fails the task the way a
   * stage abort kills it and sends one more call from the still-connected client. A kill
   * interrupts the task thread before its failure listeners run, which `interruptTaskThread`
   * reproduces. Returns, in order, the runner's server-stop log, the store's abort log and any
   * invalid-stamp warnings, plus whether the task thread was still interrupted after its failure
   * listeners ran.
   */
  private def failTaskWhileServing(interruptTaskThread: Boolean): (Seq[String], Boolean) = {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val taskContext = TaskContext.get()
      val handle = new StatefulProcessorHandleImpl(provider.getStore(0), UUID.randomUUID(),
        ExpressionEncoder(groupingKeySchema).resolveAndBind()
          .asInstanceOf[ExpressionEncoder[Any]],
        TimeMode.None())
      val killed = new TaskKilledException("Stage cancelled")
      val client = SocketChannel.open(new PythonRunner().start(handle, taskContext))
      try {
        val in = new DataInputStream(Channels.newInputStream(client))
        val out = new DataOutputStream(Channels.newOutputStream(client))
        assert(call(in, out, getValueState).getStatusCode === 0)
        assert(call(in, out, setImplicitKey).getStatusCode === 0)
        assert(call(in, out, getValue).getStatusCode === 0)

        val appender = new LogAppender("task failure")
        var stillInterrupted = false
        // The provider names its logger after its random store ids, so listen on the package.
        withLogAppender(appender, loggerNames = Seq(classOf[PythonRunner].getName,
            classOf[RocksDBStateStoreProvider].getPackage.getName)) {
          if (interruptTaskThread) {
            Thread.currentThread().interrupt()
          }
          taskContext.markTaskFailed(killed)
          stillInterrupted = Thread.interrupted()
          intercept[IOException] {
            call(in, out, getValue)
          }
        }
        val events = appender.loggingEvents.map(_.getMessage.getFormattedMessage).toSeq.collect {
          case message if message.startsWith("Stopped TransformWithState state server") ||
              message.startsWith("Invalid stamp") =>
            message
          // The provider wraps the message in its random store ids.
          case message if message.contains("Aborting 1 for ") =>
            "Aborting 1"
        }
        (events, stillInterrupted)
      } finally {
        Thread.interrupted()
        taskContext.markTaskCompleted(Some(killed))
        client.close()
      }
    }
  }

  // The race does not depend on how state rows are encoded, so each case runs once.
  Seq(false, true).foreach { interrupted =>
    testWithEncoding("unsaferow")("Python runner stops the state server before the store is " +
        s"aborted (task thread interrupted = $interrupted)") {
      assert(failTaskWhileServing(interrupted) === (Seq(
        "Stopped TransformWithState state server after the task failed", "Aborting 1"),
        interrupted))
    }
  }
}
