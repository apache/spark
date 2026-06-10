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
import io.grpc.Status
import io.grpc.stub.StreamObserver

import org.apache.spark.udf.worker.{Cancel, CancelResponse, DataRequest, DataResponse,
  ErrorResponse, ExecutionError, FinishResponse, HeartbeatResponse, Init, InitResponse,
  PayloadChunk, ProtocolError, ShutdownResponse, UdfControlRequest, UdfControlResponse,
  UdfRequest, UdfResponse, UdfWorkerGrpc, UserError, WorkerError, WorkerRequest,
  WorkerResponse}

/**
 * Minimal echo worker implementing the UDF protocol.
 *
 * Used by:
 *  - `EchoProtocolSuite` (in-process), which exercises only the wire
 *    protocol via an in-process gRPC channel.
 *  - [[EchoGrpcWorkerMain]], which hosts this service over a UDS so the
 *    `DirectGrpcDispatcher` can spawn it as a separate
 *    worker process for integration tests.
 *
 * Behavior:
 *  - Each `DataRequest` is echoed back verbatim as a `DataResponse`.
 *  - A `DataRequest` whose payload is `"ERROR"` triggers an `ErrorResponse`
 *    carrying a `UserError`. The engine MUST follow with `Cancel`.
 *  - An `Init` whose assembled payload is `"INIT_ERROR"` triggers an
 *    `InitResponse` with a `WorkerError`. The engine MUST follow with `Cancel`.
 *  - Protocol version other than [[SupportedVersion]] triggers an init-time
 *    `ProtocolError`.
 *  - Chunked payloads accumulate inline + chunks before init finalises.
 */
object EchoWorkerService {
  /** Protocol version the echo worker advertises. */
  val SupportedVersion: Int = 1

  /** A `DataRequest` whose payload equals this triggers an `ErrorResponse`. */
  val ErrorTrigger: ByteString = ByteString.copyFromUtf8("ERROR")

  /** An `Init` whose assembled payload equals this triggers an init failure. */
  val InitErrorTrigger: ByteString = ByteString.copyFromUtf8("INIT_ERROR")
}

class EchoWorkerService extends UdfWorkerGrpc.UdfWorkerImplBase {
  import EchoWorkerService._

  override def execute(
      responseObserver: StreamObserver[UdfResponse]): StreamObserver[UdfRequest] =
    new ExecuteStreamHandler(responseObserver)

  override def manage(
      request: WorkerRequest,
      responseObserver: StreamObserver[WorkerResponse]): Unit = {
    request.getManageCase match {
      case WorkerRequest.ManageCase.HEARTBEAT =>
        responseObserver.onNext(WorkerResponse.newBuilder()
          .setHeartbeat(HeartbeatResponse.getDefaultInstance)
          .build())
        responseObserver.onCompleted()

      case WorkerRequest.ManageCase.SHUTDOWN =>
        responseObserver.onNext(WorkerResponse.newBuilder()
          .setShutdown(ShutdownResponse.newBuilder().setSessionsSettled(true).build())
          .build())
        responseObserver.onCompleted()

      case _ =>
        responseObserver.onError(
          Status.INVALID_ARGUMENT.withDescription("empty manage request")
            .asRuntimeException())
    }
  }

  // State machine matches the protocol comment in udf_message.proto:
  //   AwaitingInit -> AwaitingChunks? -> Data -> Draining -> Drained -> Done
  //                                        |
  //                                        +--> PostError -> Cancelling -> Cancelled -> Done
  private sealed trait WorkerState
  private case object AwaitingInit extends WorkerState
  private case class AwaitingChunks(accumulated: ByteString) extends WorkerState
  private case object Data extends WorkerState
  private case object PostError extends WorkerState
  private case object Draining extends WorkerState
  private case object Drained extends WorkerState
  private case object Cancelling extends WorkerState
  private case object Cancelled extends WorkerState
  private case object Done extends WorkerState

  private class ExecuteStreamHandler(
      responseObserver: StreamObserver[UdfResponse]) extends StreamObserver[UdfRequest] {

    @volatile private var state: WorkerState = AwaitingInit
    private val stateLock = new Object
    private val responseLock = new Object

    private def matchUpdateThen(
        transition: WorkerState => (WorkerState, () => Unit)): Unit = {
      val followUp = stateLock.synchronized {
        val (next, callback) = transition(state)
        state = next
        callback
      }
      followUp()
    }

    override def onNext(request: UdfRequest): Unit = request.getRequestCase match {
      case UdfRequest.RequestCase.CONTROL => handleControl(request.getControl)
      case UdfRequest.RequestCase.DATA => handleDataRequest(request.getData)
      case _ => closeWithProtocolError("empty request oneof")
    }

    private def handleControl(ctrl: UdfControlRequest): Unit = ctrl.getControlCase match {
      case UdfControlRequest.ControlCase.INIT => handleInit(ctrl.getInit)
      case UdfControlRequest.ControlCase.PAYLOAD => handleChunk(ctrl.getPayload)
      case UdfControlRequest.ControlCase.FINISH => handleFinish()
      case UdfControlRequest.ControlCase.CANCEL => handleCancel(ctrl.getCancel)
      case _ => closeWithProtocolError("empty control oneof")
    }

    private def handleInit(init: Init): Unit = matchUpdateThen {
      case AwaitingInit =>
        if (init.hasProtocolVersion && init.getProtocolVersion != SupportedVersion) {
          val err = ExecutionError.newBuilder()
            .setProtocol(ProtocolError.newBuilder()
              .setMessage(s"unsupported protocol version: ${init.getProtocolVersion}")
              .build())
            .build()
          (PostError, () => sendControl(UdfControlResponse.newBuilder()
            .setInit(InitResponse.newBuilder().setError(err).build())
            .build()))
        } else if (init.getIsChunkingPayload) {
          (AwaitingChunks(init.getUdf.getPayload), () => ())
        } else {
          val payload = init.getUdf.getPayload
          (AwaitingInit, () => finalizeInit(payload))
        }
      case other =>
        (other, () => closeWithProtocolError(s"Init received in state $other"))
    }

    private def handleChunk(chunk: PayloadChunk): Unit = matchUpdateThen {
      case AwaitingChunks(existing) =>
        val updated = existing.concat(chunk.getData)
        if (chunk.hasLast && chunk.getLast) {
          (AwaitingChunks(existing), () => finalizeInit(updated))
        } else {
          (AwaitingChunks(updated), () => ())
        }
      case other =>
        (other, () => closeWithProtocolError(s"PayloadChunk received in state $other"))
    }

    private def finalizeInit(payload: ByteString): Unit = {
      val initError: Option[ExecutionError] = if (payload == InitErrorTrigger) {
        Some(ExecutionError.newBuilder()
          .setWorker(WorkerError.newBuilder()
            .setMessage("simulated init failure")
            .build())
          .build())
      } else {
        None
      }
      matchUpdateThen {
        case AwaitingInit | AwaitingChunks(_) =>
          initError match {
            case Some(err) =>
              (PostError, () => sendControl(UdfControlResponse.newBuilder()
                .setInit(InitResponse.newBuilder().setError(err).build())
                .build()))
            case None =>
              (Data, () => sendInitResponse())
          }
        case other @ (Cancelling | Cancelled | Done) => (other, () => ())
        case other =>
          (other, () => closeWithProtocolError(s"finalizeInit invoked in state $other"))
      }
    }

    private def handleDataRequest(data: DataRequest): Unit = state match {
      case Data => processEcho(data)
      case _ => closeWithProtocolError(s"DataRequest received in state $state")
    }

    private def processEcho(data: DataRequest): Unit = {
      if (data.getData == ErrorTrigger) {
        val errEnvelope = UdfControlResponse.newBuilder()
          .setError(ErrorResponse.newBuilder()
            .setError(ExecutionError.newBuilder()
              .setUser(UserError.newBuilder()
                .setMessage("simulated user-code error")
                .setErrorClass("SimulatedError")
                .build())
              .build())
            .build())
          .build()
        matchUpdateThen {
          case Data => (PostError, () => sendControl(errEnvelope))
          case other @ (Cancelling | Cancelled | Done) => (other, () => ())
          case other =>
            (other, () => closeWithProtocolError(s"processEcho invoked in state $other"))
        }
      } else {
        responseLock.synchronized {
          responseObserver.onNext(UdfResponse.newBuilder()
            .setData(DataResponse.newBuilder().setData(data.getData).build())
            .build())
        }
      }
    }

    private def handleFinish(): Unit = matchUpdateThen {
      case Data => (Draining, () => onWorkComplete())
      case PostError => (PostError, () => ())
      case other =>
        (other, () => closeWithProtocolError(s"Finish received in state $other"))
    }

    private def handleCancel(@scala.annotation.unused cancel: Cancel): Unit = matchUpdateThen {
      case AwaitingInit | AwaitingChunks(_) | Data | PostError | Draining | Drained =>
        (Cancelling, () => onWorkComplete())
      case other @ (Cancelling | Cancelled | Done) => (other, () => ())
    }

    private def onWorkComplete(): Unit = matchUpdateThen {
      case Draining =>
        (Drained, () => {
          sendControl(UdfControlResponse.newBuilder()
            .setFinish(FinishResponse.newBuilder()
              .putMetrics("status", "ok")
              .build())
            .build())
          finalizeDone()
        })
      case Cancelling =>
        (Cancelled, () => {
          sendControl(UdfControlResponse.newBuilder()
            .setCancel(CancelResponse.getDefaultInstance)
            .build())
          finalizeDone()
        })
      case Done => (Done, () => ())
      case other =>
        (other, () => closeWithProtocolError(s"onWorkComplete invoked in state $other"))
    }

    /**
     * Transitions to `Done` and half-closes the response stream. Shared by the
     * FinishResponse, CancelResponse, and protocol-error paths, which all end
     * the conversation with `onCompleted()`.
     */
    private def finalizeDone(): Unit = matchUpdateThen { _ =>
      (Done, () => responseLock.synchronized { responseObserver.onCompleted() })
    }

    override def onError(t: Throwable): Unit = matchUpdateThen { _ =>
      (Done, () => ())
    }

    override def onCompleted(): Unit = state match {
      case Done => // normal: engine half-closed after session terminated
      case _ =>
        closeWithProtocolError(
          s"request stream closed by engine in unexpected state $state")
    }

    private def sendInitResponse(): Unit =
      sendControl(UdfControlResponse.newBuilder()
        .setInit(InitResponse.getDefaultInstance)
        .build())

    private def sendControl(ctrl: UdfControlResponse): Unit =
      responseLock.synchronized {
        responseObserver.onNext(UdfResponse.newBuilder().setControl(ctrl).build())
      }

    private def closeWithProtocolError(msg: String): Unit = {
      sendControl(UdfControlResponse.newBuilder()
        .setError(ErrorResponse.newBuilder()
          .setError(ExecutionError.newBuilder()
            .setProtocol(ProtocolError.newBuilder().setMessage(msg).build())
            .build())
          .build())
        .build())
      // The protocol requires a Cancel/Finish/Error envelope plus
      // onCompleted to fully close the stream. After ErrorResponse the
      // canonical terminator is CancelResponse.
      sendControl(UdfControlResponse.newBuilder()
        .setCancel(CancelResponse.getDefaultInstance)
        .build())
      finalizeDone()
    }
  }
}
