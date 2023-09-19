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

package org.apache.spark.sql.connect.utils

import java.util.UUID

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.google.protobuf.{Any => ProtoAny}
import com.google.rpc.{Code => RPCCode, ErrorInfo, Status => RPCStatus}
import io.grpc.Status
import io.grpc.protobuf.StatusProto
import io.grpc.stub.StreamObserver
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods

import org.apache.spark.{SparkEnv, SparkException, SparkThrowable}
import org.apache.spark.api.python.PythonException
import org.apache.spark.connect.proto.FetchErrorDetailsResponse
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.service.ExecuteEventsManager
import org.apache.spark.sql.connect.service.SparkConnectService
import org.apache.spark.sql.internal.SQLConf

private[connect] object ErrorUtils extends Logging {

  /**
   * Convert a throwable to an error chain based on cause.
   * @param t
   *   the throwable to be converted.
   * @return
   *   the error chain.
   */
  private def traverseCauses(t: Throwable): List[Throwable] = {
    if (t == null) {
      List.empty
    } else {
      t :: traverseCauses(t.getCause)
    }
  }

  private def allClasses(cl: Class[_]): Seq[Class[_]] = {
    val classes = ArrayBuffer.empty[Class[_]]
    if (cl != null && !cl.equals(classOf[java.lang.Object])) {
      classes.append(cl) // Includes itself.
    }

    @tailrec
    def appendSuperClasses(clazz: Class[_]): Unit = {
      if (clazz == null || clazz.equals(classOf[java.lang.Object])) return
      classes.append(clazz.getSuperclass)
      appendSuperClasses(clazz.getSuperclass)
    }

    appendSuperClasses(cl)
    classes.toSeq
  }

  private def serializeClasses(t: Throwable): String = {
    JsonMethods.compact(JsonMethods.render(allClasses(t.getClass).map(_.getName)))
  }

  // The maximum length of the error chain.
  private val MAX_ERROR_CHAIN_LENGTH = 5

  /**
   * Convert Throwable to a protobuf message FetchErrorDetailsResponse.
   * @param st
   *   the Throwable to be converted
   * @param serverStackTraceEnabled
   *   whether to return the server stack trace.
   * @param pySparkJVMStackTraceEnabled
   *   whether to return the server stack trace for Python client.
   * @param stackTraceInMessage
   *   whether to include the server stack trace in the message.
   * @return
   *   FetchErrorDetailsResponse
   */
  private[connect] def throwableToFetchErrorDetailsResponse(
      st: Throwable,
      serverStackTraceEnabled: Boolean = false,
      pySparkJVMStackTraceEnabled: Boolean = false,
      stackTraceInMessage: Boolean = false): FetchErrorDetailsResponse = {
    val errorChain = traverseCauses(st).take(MAX_ERROR_CHAIN_LENGTH).map { case error =>
      val builder = FetchErrorDetailsResponse.Error
        .newBuilder()
        .setMessage(error.getMessage)
        .addAllErrorTypeHierarchy(ErrorUtils.allClasses(error.getClass).map(_.getName).asJava)

      if (stackTraceInMessage) {
        if (serverStackTraceEnabled || pySparkJVMStackTraceEnabled) {
          builder.setMessage(
            s"${error.getMessage}\n\n" +
              s"JVM stacktrace:\n${ExceptionUtils.getStackTrace(error)}")
        }
      } else {
        if (serverStackTraceEnabled) {
          builder.addAllStackTrace(
            error.getStackTrace
              .map { stackTraceElement =>
                FetchErrorDetailsResponse.StackTraceElement
                  .newBuilder()
                  .setDeclaringClass(stackTraceElement.getClassName)
                  .setMethodName(stackTraceElement.getMethodName)
                  .setFileName(stackTraceElement.getFileName)
                  .setLineNumber(stackTraceElement.getLineNumber)
                  .build()
              }
              .toIterable
              .asJava)
        }
      }

      builder.build()
    }

    FetchErrorDetailsResponse
      .newBuilder()
      .addAllErrorChain(errorChain.asJava)
      .build()
  }

  private def buildStatusFromThrowable(
      st: Throwable,
      stackTraceEnabled: Boolean,
      enrichErrorEnabled: Boolean,
      userId: String,
      sessionId: String): RPCStatus = {
    val errorInfo = ErrorInfo
      .newBuilder()
      .setReason(st.getClass.getName)
      .setDomain("org.apache.spark")
      .putMetadata(
        "classes",
        JsonMethods.compact(JsonMethods.render(allClasses(st.getClass).map(_.getName))))

    if (enrichErrorEnabled) {
      // Generate a new unique key for this exception.
      val errorId = UUID.randomUUID().toString

      errorInfo.putMetadata("errorId", errorId)

      SparkConnectService
        .getOrCreateIsolatedSession(userId, sessionId)
        .errorIdToError
        .put(errorId, st)
    }

    lazy val stackTrace = Option(ExceptionUtils.getStackTrace(st))
    val withStackTrace = if (stackTraceEnabled && stackTrace.nonEmpty) {
      val maxSize = SparkEnv.get.conf.get(Connect.CONNECT_JVM_STACK_TRACE_MAX_SIZE)
      errorInfo.putMetadata("stackTrace", StringUtils.abbreviate(stackTrace.get, maxSize))
    } else {
      errorInfo
    }

    RPCStatus
      .newBuilder()
      .setCode(RPCCode.INTERNAL_VALUE)
      .addDetails(ProtoAny.pack(withStackTrace.build()))
      .setMessage(SparkConnectService.extractErrorMessage(st))
      .build()
  }

  private def isPythonExecutionException(se: SparkException): Boolean = {
    // See also pyspark.errors.exceptions.captured.convert_exception in PySpark.
    se.getCause != null && se.getCause
      .isInstanceOf[PythonException] && se.getCause.getStackTrace
      .exists(_.toString.contains("org.apache.spark.sql.execution.python"))
  }

  /**
   * Common exception handling function for RPC methods. Closes the stream after the error has
   * been sent.
   *
   * @param opType
   *   String value indicating the operation type (analysis, execution)
   * @param observer
   *   The GRPC response observer.
   * @tparam V
   * @return
   */
  def handleError[V](
      opType: String,
      observer: StreamObserver[V],
      userId: String,
      sessionId: String,
      events: Option[ExecuteEventsManager] = None,
      isInterrupted: Boolean = false): PartialFunction[Throwable, Unit] = {
    val session =
      SparkConnectService
        .getOrCreateIsolatedSession(userId, sessionId)
        .session
    val stackTraceEnabled = session.conf.get(SQLConf.PYSPARK_JVM_STACKTRACE_ENABLED)
    val enrichErrorEnabled = session.conf.get(Connect.CONNECT_ENRICH_ERROR_ENABLED)

    val partial: PartialFunction[Throwable, (Throwable, Throwable)] = {
      case se: SparkException if isPythonExecutionException(se) =>
        (
          se,
          StatusProto.toStatusRuntimeException(
            buildStatusFromThrowable(
              se.getCause,
              stackTraceEnabled,
              enrichErrorEnabled,
              userId,
              sessionId)))

      case e: Throwable if e.isInstanceOf[SparkThrowable] || NonFatal.apply(e) =>
        (
          e,
          StatusProto.toStatusRuntimeException(
            buildStatusFromThrowable(
              e,
              stackTraceEnabled,
              enrichErrorEnabled,
              userId,
              sessionId)))

      case e: Throwable =>
        (
          e,
          Status.UNKNOWN
            .withCause(e)
            .withDescription(StringUtils.abbreviate(e.getMessage, 2048))
            .asRuntimeException())
    }
    partial
      .andThen { case (original, wrapped) =>
        if (events.isDefined) {
          // Errors thrown inside execution are user query errors, return then as INFO.
          logInfo(
            s"Spark Connect error " +
              s"during: $opType. UserId: $userId. SessionId: $sessionId.",
            original)
        } else {
          // Other errors are server RPC errors, return them as ERROR.
          logError(
            s"Spark Connect RPC error " +
              s"during: $opType. UserId: $userId. SessionId: $sessionId.",
            original)
        }

        // If ExecuteEventsManager is present, this this is an execution error that needs to be
        // posted to it.
        events.foreach { executeEventsManager =>
          if (isInterrupted) {
            executeEventsManager.postCanceled()
          } else {
            executeEventsManager.postFailed(wrapped.getMessage)
          }
        }
        observer.onError(wrapped)
      }
  }
}
