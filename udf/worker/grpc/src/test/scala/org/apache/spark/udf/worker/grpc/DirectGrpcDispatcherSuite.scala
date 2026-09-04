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
package org.apache.spark.udf.worker.grpc

import io.grpc.ConnectivityState
// scalastyle:off funsuite
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.udf.worker.{DirectWorker, LocalTcpConnection,
  UDFProtoCommunicationPattern, UDFWorkerProperties, UDFWorkerSpecification,
  UnixDomainSocket, WorkerCapabilities, WorkerConnectionSpec}

class DirectGrpcDispatcherSuite extends AnyFunSuite {
// scalastyle:on funsuite

  private def directSpec(
      properties: UDFWorkerProperties,
      capabilities: Option[WorkerCapabilities] = None): UDFWorkerSpecification = {
    val builder = UDFWorkerSpecification.newBuilder()
      .setDirect(DirectWorker.newBuilder().setProperties(properties).build())
    capabilities.foreach(builder.setCapabilities)
    builder.build()
  }

  private def udsProperties: UDFWorkerProperties = UDFWorkerProperties.newBuilder()
    .setConnection(WorkerConnectionSpec.newBuilder()
      .setUnixDomainSocket(UnixDomainSocket.getDefaultInstance)
      .build())
    .build()

  test("requires a connection") {
    val error = intercept[IllegalArgumentException] {
      new DirectGrpcDispatcher(directSpec(UDFWorkerProperties.getDefaultInstance))
    }
    assert(error.getMessage.contains("connection must be set"))
  }

  test("requires Unix domain socket transport") {
    val properties = UDFWorkerProperties.newBuilder()
      .setConnection(WorkerConnectionSpec.newBuilder()
        .setTcp(LocalTcpConnection.getDefaultInstance)
        .build())
      .build()
    val error = intercept[IllegalArgumentException] {
      new DirectGrpcDispatcher(directSpec(properties))
    }
    assert(error.getMessage.contains("requires UNIX domain socket transport"))
  }

  test("requires capabilities") {
    val error = intercept[IllegalArgumentException] {
      new DirectGrpcDispatcher(directSpec(udsProperties))
    }
    assert(error.getMessage.contains("requires WorkerCapabilities"))
  }

  test("requires bidirectional streaming capability") {
    val capabilities = WorkerCapabilities.newBuilder()
      .addSupportedCommunicationPatterns(
        UDFProtoCommunicationPattern.UDF_PROTO_COMMUNICATION_PATTERN_UNSPECIFIED)
      .build()
    val error = intercept[IllegalArgumentException] {
      new DirectGrpcDispatcher(directSpec(udsProperties, Some(capabilities)))
    }
    assert(error.getMessage.contains("requires BIDIRECTIONAL_STREAMING"))
  }

  test("requests a backoff reset for every transient failure after endpoint creation") {
    assert((1 to 2).forall { _ =>
      DirectGrpcDispatcher.shouldResetConnectBackoff(
        ConnectivityState.TRANSIENT_FAILURE, endpointExists = true)
    })
    assert(!DirectGrpcDispatcher.shouldResetConnectBackoff(
      ConnectivityState.CONNECTING, endpointExists = true))
    assert(!DirectGrpcDispatcher.shouldResetConnectBackoff(
      ConnectivityState.TRANSIENT_FAILURE, endpointExists = false))
  }
}
