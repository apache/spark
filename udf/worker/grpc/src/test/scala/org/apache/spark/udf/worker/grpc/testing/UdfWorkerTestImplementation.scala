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
package org.apache.spark.udf.worker.grpc.testing

import com.google.protobuf.ByteString

import org.apache.spark.udf.worker.{DataRequest, DataResponse, ExecutionError, Init, UdfPayload,
  UDFWorkerDataFormat, UdfWorkerGrpc, UserError, WorkerError}

private[grpc] final case class SuccessfulExecution(
    init: Init,
    requests: Seq[DataRequest],
    expectedResponses: Seq[DataResponse])

private[grpc] final case class UserErrorExecution(
    init: Init,
    request: DataRequest,
    expectedError: ExecutionError)

private[grpc] final case class InitErrorExecution(
    init: Init,
    expectedError: ExecutionError)

/** Implementation-specific scenarios consumed by [[UdfWorkerProtocolSuite]]. */
private[grpc] trait UdfWorkerTestImplementation {
  def newService(): UdfWorkerGrpc.UdfWorkerImplBase
  def supportedProtocolVersion: Int
  def singleRequest: SuccessfulExecution
  def multipleRequests: SuccessfulExecution
  def noInput: SuccessfulExecution
  def userError: UserErrorExecution
  def initError: InitErrorExecution
}

/** Protocol scenarios for the minimal echo worker test implementation. */
private[grpc] object EchoWorkerTestImplementation extends UdfWorkerTestImplementation {
  import EchoWorkerService.{ErrorTrigger, InitErrorTrigger, SupportedVersion}

  override def newService(): UdfWorkerGrpc.UdfWorkerImplBase = new EchoWorkerService

  override val supportedProtocolVersion: Int = SupportedVersion

  override val singleRequest: SuccessfulExecution = SuccessfulExecution(
    init("single-request"),
    Seq(data("hello")),
    Seq(response("hello")))

  override val multipleRequests: SuccessfulExecution = SuccessfulExecution(
    init("multiple-requests"),
    Seq("batch-1", "batch-2", "batch-3").map(data),
    Seq("batch-1", "batch-2", "batch-3").map(response))

  override val noInput: SuccessfulExecution =
    SuccessfulExecution(init("no-input"), Seq.empty, Seq.empty)

  override val userError: UserErrorExecution = UserErrorExecution(
    init("user-error"),
    DataRequest.newBuilder().setData(ErrorTrigger).build(),
    ExecutionError.newBuilder()
      .setUser(UserError.newBuilder()
        .setMessage("simulated user-code error")
        .setErrorClass("SimulatedError")
        .build())
      .build())

  override val initError: InitErrorExecution = InitErrorExecution(
    init(InitErrorTrigger),
    ExecutionError.newBuilder()
      .setWorker(WorkerError.newBuilder().setMessage("simulated init failure").build())
      .build())

  private def init(payload: String): Init = init(ByteString.copyFromUtf8(payload))

  private def init(payload: ByteString): Init = Init.newBuilder()
    .setProtocolVersion(SupportedVersion)
    .setDataFormat(UDFWorkerDataFormat.ARROW)
    .setUdf(UdfPayload.newBuilder().setPayload(payload).setFormat("echo").build())
    .build()

  private def data(value: String): DataRequest =
    DataRequest.newBuilder().setData(ByteString.copyFromUtf8(value)).build()

  private def response(value: String): DataResponse =
    DataResponse.newBuilder().setData(ByteString.copyFromUtf8(value)).build()
}
