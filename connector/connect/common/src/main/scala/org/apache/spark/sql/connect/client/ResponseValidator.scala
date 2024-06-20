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
package org.apache.spark.sql.connect.client

import java.util.concurrent.atomic.AtomicBoolean

import com.google.protobuf.GeneratedMessageV3
import io.grpc.{Status, StatusRuntimeException}
import io.grpc.stub.StreamObserver

import org.apache.spark.internal.Logging

// This is common logic to be shared between different stub instances to keep the server-side
// session id and to validate responses as seen by the client.
class ResponseValidator extends Logging {

  // Server side session ID, used to detect if the server side session changed. This is set upon
  // receiving the first response from the server. This value is used only for executions that
  // do not use server-side streaming.
  private var serverSideSessionId: Option[String] = None

  // Indicates whether the client and the client information on the server correspond to each other
  // This flag being false means that the server has restarted and lost the client information, or
  // there is a logic error in the code; both cases, the user should establish a new connection to
  // the server. Access to the value has to be synchronized since it can be shared.
  private val isSessionActive: AtomicBoolean = new AtomicBoolean(true)

  // Returns the server side session ID, used to send it back to the server in the follow-up
  // requests so the server can validate it session id against the previous requests.
  def getServerSideSessionId: Option[String] = serverSideSessionId

  /**
   * Hijacks the stored server side session ID with the given suffix. Used for testing to make
   * sure that server is validating the session ID.
   */
  private[sql] def hijackServerSideSessionIdForTesting(suffix: String): Unit = {
    serverSideSessionId = Some(serverSideSessionId.getOrElse("") + suffix)
  }

  /**
   * Returns true if the session is valid on both the client and the server.
   */
  private[sql] def isSessionValid: Boolean = {
    // An active session is considered valid.
    isSessionActive.getAcquire
  }

  def verifyResponse[RespT <: GeneratedMessageV3](fn: => RespT): RespT = {
    val response =
      try {
        fn
      } catch {
        case e: StatusRuntimeException
            if e.getStatus.getCode == Status.Code.INTERNAL &&
              e.getMessage.contains("[INVALID_HANDLE.SESSION_CHANGED]") =>
          isSessionActive.setRelease(false)
          throw e
      }
    val field = response.getDescriptorForType.findFieldByName("server_side_session_id")
    // If the field does not exist, we ignore it. New / Old message might not contain it and this
    // behavior allows us to be compatible.
    if (field != null && response.hasField(field)) {
      val value = response.getField(field).asInstanceOf[String]
      // Ignore, if the value is unset.
      if (value != null && value.nonEmpty) {
        serverSideSessionId match {
          case Some(id) =>
            if (value != id) {
              isSessionActive.setRelease(false)
              throw new IllegalStateException(
                s"Server side session ID changed from $id to $value")
            }
          case _ =>
            synchronized {
              serverSideSessionId = Some(value)
            }
        }
      }
    } else {
      logDebug("Server side session ID field not found in response - Ignoring.")
    }
    response
  }

  /**
   * Wraps an existing iterator with another closeable iterator that verifies the response. This
   * is needed for server-side streaming calls that are converted to iterators.
   */
  def wrapIterator[T <: GeneratedMessageV3, V <: CloseableIterator[T]](
      inner: V): WrappedCloseableIterator[T] = {
    new WrappedCloseableIterator[T] {

      override def innerIterator: Iterator[T] = inner

      override def next(): T = {
        verifyResponse {
          innerIterator.next()
        }
      }
    }
  }

  /**
   * Wraps an existing stream observer with another stream observer that verifies the response.
   * This is necessary for client-side streaming calls.
   */
  def wrapStreamObserver[T <: GeneratedMessageV3](inner: StreamObserver[T]): StreamObserver[T] = {
    new StreamObserver[T] {
      private val innerObserver = inner
      override def onNext(value: T): Unit = {
        try {
          innerObserver.onNext(verifyResponse(value))
        } catch {
          case e: Exception =>
            onError(e)
        }
      }
      override def onError(t: Throwable): Unit = {
        innerObserver.onError(t)
      }
      override def onCompleted(): Unit = {
        innerObserver.onCompleted()
      }
    }
  }

}
