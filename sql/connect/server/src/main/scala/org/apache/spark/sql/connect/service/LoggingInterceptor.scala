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

package org.apache.spark.sql.connect.service

import scala.util.Random

import com.google.protobuf.Message
import com.google.protobuf.util.JsonFormat
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{DESCRIPTION, MESSAGE}

/**
 * A gRPC interceptor to log RPC requests and responses. It logs the protobufs as JSON. Useful for
 * local development. An ID is logged for each RPC so that requests and corresponding responses
 * can be exactly matched.
 */
class LoggingInterceptor extends ServerInterceptor with Logging {

  private val jsonPrinter = JsonFormat.printer().preservingProtoFieldNames()

  private def logProto[T](description: String, message: T): Unit = {
    message match {
      case m: Message =>
        logInfo(log"${MDC(DESCRIPTION, description)}:\n${MDC(MESSAGE, jsonPrinter.print(m))}")
      case other =>
        logInfo(
          log"${MDC(DESCRIPTION, description)}: " +
            log"(Unknown message type) ${MDC(MESSAGE, other)}")
    }
  }

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {

    val id = Random.nextInt(Int.MaxValue) // Assign a random id for this RPC.
    val desc = s"${call.getMethodDescriptor.getFullMethodName} (id $id)"

    val respLoggingCall = new SimpleForwardingServerCall[ReqT, RespT](call) {
      override def sendMessage(message: RespT): Unit = {
        logProto(s"Responding to RPC $desc", message)
        super.sendMessage(message)
      }
    }

    val listener = next.startCall(respLoggingCall, headers)

    new SimpleForwardingServerCallListener[ReqT](listener) {
      override def onMessage(message: ReqT): Unit = {
        logProto(s"Received RPC request $desc", message)
        super.onMessage(message)
      }
    }
  }
}
