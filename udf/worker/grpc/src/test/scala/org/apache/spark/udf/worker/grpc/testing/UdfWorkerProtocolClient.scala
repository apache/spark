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

import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._

import io.grpc.stub.StreamObserver

import org.apache.spark.udf.worker.{Cancel, DataRequest, DataResponse, ExecutionError, Finish,
  Init, PayloadChunk, UdfControlRequest, UdfControlResponse, UdfRequest, UdfResponse,
  UdfWorkerGrpc}

/** Engine-side protocol driver shared by UDF worker protocol contract suites. */
private[grpc] final class UdfWorkerProtocolClient(stub: UdfWorkerGrpc.UdfWorkerStub) {
  private val results = new LinkedBlockingQueue[DataResponse]()
  private val done = new CountDownLatch(1)
  private val initResponse = new CountDownLatch(1)
  private val requestCompleted = new AtomicBoolean(false)
  private val requestLock = new Object

  @volatile var executionError: Option[ExecutionError] = None
  @volatile var streamError: Option[Throwable] = None

  private val responseObserver = new StreamObserver[UdfResponse] {
    override def onNext(response: UdfResponse): Unit = {
      response.getResponseCase match {
        case UdfResponse.ResponseCase.DATA =>
          results.add(response.getData)

        case UdfResponse.ResponseCase.CONTROL =>
          val control = response.getControl
          control.getControlCase match {
            case UdfControlResponse.ControlCase.INIT =>
              if (control.getInit.hasError) {
                executionError = Some(control.getInit.getError)
                sendCancel(Cancel.newBuilder().setReason("aborting after init error").build())
              }
              initResponse.countDown()

            case UdfControlResponse.ControlCase.ERROR =>
              executionError = Some(control.getError.getError)
              sendCancel(Cancel.newBuilder().setReason("aborting after execution error").build())

            case UdfControlResponse.ControlCase.FINISH |
                UdfControlResponse.ControlCase.CANCEL =>
              completeRequestStream()
              done.countDown()

            case unexpected =>
              throw new IllegalStateException(s"unexpected control response: $unexpected")
          }

        case unexpected =>
          throw new IllegalStateException(s"unexpected response type: $unexpected")
      }
    }

    override def onError(t: Throwable): Unit = {
      streamError = Some(t)
      completeRequestStream()
      initResponse.countDown()
      done.countDown()
    }

    override def onCompleted(): Unit = {
      initResponse.countDown()
      done.countDown()
    }
  }

  private val requestObserver: StreamObserver[UdfRequest] = stub.execute(responseObserver)

  def sendInit(init: Init): Unit = sendRaw(UdfRequest.newBuilder()
    .setControl(UdfControlRequest.newBuilder().setInit(init).build())
    .build())

  def sendPayloadChunk(chunk: PayloadChunk): Unit = sendRaw(UdfRequest.newBuilder()
    .setControl(UdfControlRequest.newBuilder().setPayload(chunk).build())
    .build())

  def sendData(data: DataRequest): Unit = {
    requireInitResponse()
    sendRaw(UdfRequest.newBuilder().setData(data).build())
  }

  def sendFinish(finish: Finish = Finish.getDefaultInstance): Unit = {
    requireInitResponse()
    if (!requestCompleted.get()) {
      sendRaw(UdfRequest.newBuilder()
        .setControl(UdfControlRequest.newBuilder().setFinish(finish).build())
        .build())
    }
  }

  def sendCancel(cancel: Cancel = Cancel.getDefaultInstance): Unit = {
    if (!requestCompleted.get()) {
      sendRaw(UdfRequest.newBuilder()
        .setControl(UdfControlRequest.newBuilder().setCancel(cancel).build())
        .build())
    }
  }

  /** Sends an unsequenced request for tests that intentionally violate ordering. */
  def sendRaw(request: UdfRequest): Unit = requestLock.synchronized {
    if (!requestCompleted.get()) requestObserver.onNext(request)
  }

  def awaitInitResponse(timeoutMs: Long = 5000L): Boolean =
    initResponse.await(timeoutMs, TimeUnit.MILLISECONDS)

  def awaitDone(timeoutMs: Long = 5000L): Boolean =
    done.await(timeoutMs, TimeUnit.MILLISECONDS)

  def drainResults(): Seq[DataResponse] = {
    val buffer = new java.util.ArrayList[DataResponse]()
    results.drainTo(buffer)
    buffer.asScala.toSeq
  }

  def completeRequestStream(): Unit = requestLock.synchronized {
    if (requestCompleted.compareAndSet(false, true)) {
      requestObserver.onCompleted()
    }
  }

  private def requireInitResponse(): Unit = {
    if (!awaitInitResponse()) {
      throw new IllegalStateException("InitResponse not received within timeout")
    }
  }
}
