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
import scala.collection.mutable
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

import org.apache.spark.{QueryContextType, SparkEnv, SparkException, SparkThrowable}
import org.apache.spark.api.python.PythonException
import org.apache.spark.connect.proto.FetchErrorDetailsResponse
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{OP_TYPE, SESSION_ID, USER_ID}
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.service.{ExecuteEventsManager, SessionHolder, SessionKey, SparkConnectService}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.ArrayImplicits._

private[connect] object ErrorUtils extends Logging {

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

  // The maximum length of the error chain.
  private[connect] val MAX_ERROR_CHAIN_LENGTH = 5

  /**
   * Convert Throwable to a protobuf message FetchErrorDetailsResponse.
   * @param st
   *   the Throwable to be converted
   * @param serverStackTraceEnabled
   *   whether to return the server stack trace.
   * @return
   *   FetchErrorDetailsResponse
   */
  private[connect] def throwableToFetchErrorDetailsResponse(
      st: Throwable,
      serverStackTraceEnabled: Boolean = false): FetchErrorDetailsResponse = {

    var currentError = st
    val buffer = mutable.Buffer.empty[FetchErrorDetailsResponse.Error]

    while (buffer.size < MAX_ERROR_CHAIN_LENGTH && currentError != null) {
      val builder = FetchErrorDetailsResponse.Error
        .newBuilder()
        .setMessage(currentError.getMessage)
        .addAllErrorTypeHierarchy(
          ErrorUtils.allClasses(currentError.getClass).map(_.getName).asJava)

      if (serverStackTraceEnabled) {
        builder.addAllStackTrace(
          currentError.getStackTrace
            .map { stackTraceElement =>
              val stackTraceBuilder = FetchErrorDetailsResponse.StackTraceElement
                .newBuilder()
                .setDeclaringClass(stackTraceElement.getClassName)
                .setMethodName(stackTraceElement.getMethodName)
                .setLineNumber(stackTraceElement.getLineNumber)

              if (stackTraceElement.getFileName != null) {
                stackTraceBuilder.setFileName(stackTraceElement.getFileName)
              }

              stackTraceBuilder.build()
            }
            .toImmutableArraySeq
            .asJava)
      }

      currentError match {
        case sparkThrowable: SparkThrowable =>
          val sparkThrowableBuilder = FetchErrorDetailsResponse.SparkThrowable
            .newBuilder()
          if (sparkThrowable.getErrorClass != null) {
            sparkThrowableBuilder.setErrorClass(sparkThrowable.getErrorClass)
          }
          for (queryCtx <- sparkThrowable.getQueryContext) {
            val builder = FetchErrorDetailsResponse.QueryContext
              .newBuilder()
            val context = if (queryCtx.contextType() == QueryContextType.SQL) {
              builder
                .setContextType(FetchErrorDetailsResponse.QueryContext.ContextType.SQL)
                .setObjectType(queryCtx.objectType())
                .setObjectName(queryCtx.objectName())
                .setStartIndex(queryCtx.startIndex())
                .setStopIndex(queryCtx.stopIndex())
                .setFragment(queryCtx.fragment())
                .setSummary(queryCtx.summary())
                .build()
            } else {
              builder
                .setContextType(FetchErrorDetailsResponse.QueryContext.ContextType.DATAFRAME)
                .setFragment(queryCtx.fragment())
                .setCallSite(queryCtx.callSite())
                .setSummary(queryCtx.summary())
                .build()
            }
            sparkThrowableBuilder.addQueryContexts(context)
          }
          if (sparkThrowable.getSqlState != null) {
            sparkThrowableBuilder.setSqlState(sparkThrowable.getSqlState)
          }
          sparkThrowableBuilder.putAllMessageParameters(sparkThrowable.getMessageParameters)
          builder.setSparkThrowable(sparkThrowableBuilder.build())
        case _ =>
      }

      val causeIdx = buffer.size + 1

      if (causeIdx < MAX_ERROR_CHAIN_LENGTH && currentError.getCause != null) {
        builder.setCauseIdx(causeIdx)
      }

      buffer.append(builder.build())

      currentError = currentError.getCause
    }

    FetchErrorDetailsResponse
      .newBuilder()
      .setRootErrorIdx(0)
      .addAllErrors(buffer.asJava)
      .build()
  }

  /**
   * This is a helper method that can be used by any GRPC handler to convert existing Throwables
   * into GRPC conform status objects.
   *
   * @param st
   * @param sessionHolderOpt
   * @return
   */
  private[connect] def buildStatusFromThrowable(
      st: Throwable,
      sessionHolderOpt: Option[SessionHolder]): RPCStatus = {
    val errorInfo = ErrorInfo
      .newBuilder()
      .setReason(st.getClass.getName)
      .setDomain("org.apache.spark")
      .putMetadata(
        "classes",
        JsonMethods.compact(JsonMethods.render(allClasses(st.getClass).map(_.getName))))

    val maxMetadataSize = SparkEnv.get.conf.get(Connect.CONNECT_GRPC_MAX_METADATA_SIZE)
    // Add the SQL State and Error Class to the response metadata of the ErrorInfoObject.
    st match {
      case e: SparkThrowable =>
        val state = e.getSqlState
        if (state != null && state.nonEmpty) {
          errorInfo.putMetadata("sqlState", state)
        }
        val errorClass = e.getErrorClass
        if (errorClass != null && errorClass.nonEmpty) {
          val messageParameters = JsonMethods.compact(
            JsonMethods.render(map2jvalue(e.getMessageParameters.asScala.toMap)))
          if (messageParameters.length <= maxMetadataSize) {
            errorInfo.putMetadata("errorClass", errorClass)
            errorInfo.putMetadata("messageParameters", messageParameters)
          }
        }
      case _ =>
    }

    val enrichErrorEnabled = sessionHolderOpt.exists(
      _.session.sessionState.conf.getConf(Connect.CONNECT_ENRICH_ERROR_ENABLED))
    if (enrichErrorEnabled) {
      // Generate a new unique key for this exception.
      val errorId = UUID.randomUUID().toString

      errorInfo.putMetadata("errorId", errorId)

      sessionHolderOpt.get.errorIdToError
        .put(errorId, st)
    }

    lazy val stackTrace = Option(ExceptionUtils.getStackTrace(st))
    val stackTraceEnabled = sessionHolderOpt.exists(
      _.session.sessionState.conf.getConf(SQLConf.PYSPARK_JVM_STACKTRACE_ENABLED))
    val withStackTrace =
      if (stackTraceEnabled && stackTrace.nonEmpty) {
        val maxSize = Math.min(
          SparkEnv.get.conf.get(Connect.CONNECT_JVM_STACK_TRACE_MAX_SIZE),
          maxMetadataSize)
        errorInfo.putMetadata("stackTrace", StringUtils.abbreviate(stackTrace.get, maxSize.toInt))
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
   * @param userId
   *   The user id.
   * @param sessionId
   *   The session id.
   * @param events
   *   The ExecuteEventsManager if present to report about failures.
   * @param isInterrupted
   *   Whether the error is caused by an interruption or during execution.
   * @param callback
   *   Optional callback to be called after the error has been sent that allows to caller to
   *   execute additional cleanup logic.
   * @tparam V
   * @return
   */
  def handleError[V](
      opType: String,
      observer: StreamObserver[V],
      userId: String,
      sessionId: String,
      events: Option[ExecuteEventsManager] = None,
      isInterrupted: Boolean = false,
      callback: Option[() => Unit] = None): PartialFunction[Throwable, Unit] = {

    // SessionHolder may not be present, e.g. if the session was already closed.
    // When SessionHolder is not present error details will not be available for FetchErrorDetails.
    val sessionHolderOpt =
      SparkConnectService.sessionManager.getIsolatedSessionIfPresent(
        SessionKey(userId, sessionId))

    val partial: PartialFunction[Throwable, (Throwable, Throwable)] = {
      case se: SparkException if isPythonExecutionException(se) =>
        (
          se,
          StatusProto.toStatusRuntimeException(
            buildStatusFromThrowable(se.getCause, sessionHolderOpt)))

      case e: Throwable if e.isInstanceOf[SparkThrowable] || NonFatal.apply(e) =>
        (e, StatusProto.toStatusRuntimeException(buildStatusFromThrowable(e, sessionHolderOpt)))

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
            log"Spark Connect error during: ${MDC(OP_TYPE, opType)}. " +
              log"UserId: ${MDC(USER_ID, userId)}. SessionId: ${MDC(SESSION_ID, sessionId)}.",
            original)
        } else {
          // Other errors are server RPC errors, return them as ERROR.
          logError(
            log"Spark Connect RPC error during: ${MDC(OP_TYPE, opType)}. " +
              log"UserId: ${MDC(USER_ID, userId)}. SessionId: ${MDC(SESSION_ID, sessionId)}.",
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
        callback.foreach(_.apply())
        observer.onError(wrapped)
      }
  }
}
