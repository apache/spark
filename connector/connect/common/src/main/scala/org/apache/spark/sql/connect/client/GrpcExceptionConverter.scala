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

import java.time.DateTimeException

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import com.google.rpc.ErrorInfo
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

import org.apache.spark.{SparkArithmeticException, SparkArrayIndexOutOfBoundsException, SparkDateTimeException, SparkException, SparkIllegalArgumentException, SparkNumberFormatException, SparkRuntimeException, SparkUnsupportedOperationException, SparkUpgradeException}
import org.apache.spark.connect.proto.{FetchErrorDetailsRequest, FetchErrorDetailsResponse, UserContext}
import org.apache.spark.connect.proto.SparkConnectServiceGrpc.SparkConnectServiceBlockingStub
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchDatabaseException, NoSuchTableException, TableAlreadyExistsException, TempTableAlreadyExistsException}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.streaming.StreamingQueryException

/**
 * GrpcExceptionConverter handles the conversion of StatusRuntimeExceptions into Spark exceptions.
 * It does so by utilizing the ErrorInfo defined in error_details.proto and making an additional
 * FetchErrorDetails RPC call to retrieve the full error message and optionally the server-side
 * stacktrace.
 *
 * If the FetchErrorDetails RPC call succeeds, the exceptions will be constructed based on the
 * response. If the RPC call fails, the exception will be constructed based on the ErrorInfo. If
 * the ErrorInfo is missing, the exception will be constructed based on the StatusRuntimeException
 * itself.
 */
private[client] class GrpcExceptionConverter(grpcStub: SparkConnectServiceBlockingStub)
    extends Logging {
  import GrpcExceptionConverter._

  def convert[T](sessionId: String, userContext: UserContext, clientType: String)(f: => T): T = {
    try {
      f
    } catch {
      case e: StatusRuntimeException =>
        throw toThrowable(e, sessionId, userContext, clientType)
    }
  }

  def convertIterator[T](
      sessionId: String,
      userContext: UserContext,
      clientType: String,
      iter: CloseableIterator[T]): CloseableIterator[T] = {
    new WrappedCloseableIterator[T] {

      override def innerIterator: Iterator[T] = iter

      override def hasNext: Boolean = {
        convert(sessionId, userContext, clientType) {
          iter.hasNext
        }
      }

      override def next(): T = {
        convert(sessionId, userContext, clientType) {
          iter.next()
        }
      }

      override def close(): Unit = {
        convert(sessionId, userContext, clientType) {
          iter.close()
        }
      }
    }
  }

  /**
   * Fetches enriched errors with full exception message and optionally stacktrace by issuing an
   * additional RPC call to fetch error details. The RPC call is best-effort at-most-once.
   */
  private def fetchEnrichedError(
      info: ErrorInfo,
      sessionId: String,
      userContext: UserContext,
      clientType: String): Option[Throwable] = {
    val errorId = info.getMetadataOrDefault("errorId", null)
    if (errorId == null) {
      logWarning("Unable to fetch enriched error since errorId is missing")
      return None
    }

    try {
      val errorDetailsResponse = grpcStub.fetchErrorDetails(
        FetchErrorDetailsRequest
          .newBuilder()
          .setSessionId(sessionId)
          .setErrorId(errorId)
          .setUserContext(userContext)
          .setClientType(clientType)
          .build())

      if (!errorDetailsResponse.hasRootErrorIdx) {
        logWarning("Unable to fetch enriched error since error is not found")
        return None
      }

      Some(
        errorsToThrowable(
          errorDetailsResponse.getRootErrorIdx,
          errorDetailsResponse.getErrorsList.asScala.toSeq))
    } catch {
      case e: StatusRuntimeException =>
        logWarning("Unable to fetch enriched error", e)
        None
    }
  }

  private def toThrowable(
      ex: StatusRuntimeException,
      sessionId: String,
      userContext: UserContext,
      clientType: String): Throwable = {
    val status = StatusProto.fromThrowable(ex)

    // Extract the ErrorInfo from the StatusProto, if present.
    val errorInfoOpt = status.getDetailsList.asScala
      .find(_.is(classOf[ErrorInfo]))
      .map(_.unpack(classOf[ErrorInfo]))

    if (errorInfoOpt.isDefined) {
      // If ErrorInfo is found, try to fetch enriched error details by an additional RPC.
      val enrichedErrorOpt =
        fetchEnrichedError(errorInfoOpt.get, sessionId, userContext, clientType)
      if (enrichedErrorOpt.isDefined) {
        return enrichedErrorOpt.get
      }

      // If fetching enriched error details fails, convert ErrorInfo to a Throwable.
      // Unlike enriched errors above, the message from status may be truncated,
      // and no cause exceptions or server-side stack traces will be reconstructed.
      return errorInfoToThrowable(errorInfoOpt.get, status.getMessage)
    }

    // If no ErrorInfo is found, create a SparkException based on the StatusRuntimeException.
    new SparkException(ex.toString, ex.getCause)
  }
}

private object GrpcExceptionConverter {

  private case class ErrorParams(
      message: String,
      cause: Option[Throwable],
      // errorClass will only be set if the error is enriched and SparkThrowable.
      errorClass: Option[String],
      // messageParameters will only be set if the error is enriched and SparkThrowable.
      messageParameters: Map[String, String])

  private def errorConstructor[T <: Throwable: ClassTag](
      throwableCtr: ErrorParams => T): (String, ErrorParams => Throwable) = {
    val className = implicitly[reflect.ClassTag[T]].runtimeClass.getName
    (className, throwableCtr)
  }

  private val errorFactory = Map(
    errorConstructor(params =>
      new StreamingQueryException(
        params.message,
        params.cause.orNull,
        params.errorClass.orNull,
        params.messageParameters)),
    errorConstructor(params =>
      new ParseException(
        None,
        params.message,
        Origin(),
        Origin(),
        errorClass = params.errorClass,
        messageParameters = params.messageParameters)),
    errorConstructor(params =>
      new AnalysisException(
        params.message,
        cause = params.cause,
        errorClass = params.errorClass,
        messageParameters = params.messageParameters)),
    errorConstructor(params => new NamespaceAlreadyExistsException(params.message)),
    errorConstructor(params => new TableAlreadyExistsException(params.message, params.cause)),
    errorConstructor(params => new TempTableAlreadyExistsException(params.message, params.cause)),
    errorConstructor(params => new NoSuchDatabaseException(params.message, params.cause)),
    errorConstructor(params => new NoSuchTableException(params.message, params.cause)),
    errorConstructor[NumberFormatException](params =>
      new SparkNumberFormatException(params.message)),
    errorConstructor[IllegalArgumentException](params =>
      new SparkIllegalArgumentException(params.message, params.cause)),
    errorConstructor[ArithmeticException](params => new SparkArithmeticException(params.message)),
    errorConstructor[UnsupportedOperationException](params =>
      new SparkUnsupportedOperationException(params.message)),
    errorConstructor[ArrayIndexOutOfBoundsException](params =>
      new SparkArrayIndexOutOfBoundsException(params.message)),
    errorConstructor[DateTimeException](params => new SparkDateTimeException(params.message)),
    errorConstructor(params => new SparkRuntimeException(params.message, params.cause)),
    errorConstructor(params => new SparkUpgradeException(params.message, params.cause)),
    errorConstructor(params =>
      new SparkException(
        message = params.message,
        cause = params.cause.orNull,
        errorClass = params.errorClass,
        messageParameters = params.messageParameters)))

  /**
   * errorsToThrowable reconstructs the exception based on a list of protobuf messages
   * FetchErrorDetailsResponse.Error with un-truncated error messages and server-side stacktrace
   * (if set).
   */
  private def errorsToThrowable(
      errorIdx: Int,
      errors: Seq[FetchErrorDetailsResponse.Error]): Throwable = {

    val error = errors(errorIdx)
    val classHierarchy = error.getErrorTypeHierarchyList.asScala

    val constructor =
      classHierarchy
        .flatMap(errorFactory.get)
        .headOption
        .getOrElse((params: ErrorParams) =>
          errorFactory
            .get(classOf[SparkException].getName)
            .get(params.copy(message = s"${classHierarchy.head}: ${params.message}")))

    val causeOpt =
      if (error.hasCauseIdx) Some(errorsToThrowable(error.getCauseIdx, errors)) else None

    val exception = constructor(
      ErrorParams(
        message = error.getMessage,
        cause = causeOpt,
        errorClass = if (error.hasSparkThrowable && error.getSparkThrowable.hasErrorClass) {
          Some(error.getSparkThrowable.getErrorClass)
        } else None,
        messageParameters = if (error.hasSparkThrowable) {
          error.getSparkThrowable.getMessageParametersMap.asScala.toMap
        } else Map.empty))

    if (!error.getStackTraceList.isEmpty) {
      exception.setStackTrace(error.getStackTraceList.asScala.toArray.map { stackTraceElement =>
        new StackTraceElement(
          stackTraceElement.getDeclaringClass,
          stackTraceElement.getMethodName,
          if (stackTraceElement.hasFileName) stackTraceElement.getFileName else null,
          stackTraceElement.getLineNumber)
      })
    }

    exception
  }

  /**
   * errorInfoToThrowable reconstructs the exception based on the error classes hierarchy and the
   * truncated error message.
   */
  private def errorInfoToThrowable(info: ErrorInfo, message: String): Throwable = {
    implicit val formats = DefaultFormats
    val classes =
      JsonMethods.parse(info.getMetadataOrDefault("classes", "[]")).extract[Array[String]]

    errorsToThrowable(
      0,
      Seq(
        FetchErrorDetailsResponse.Error
          .newBuilder()
          .setMessage(message)
          .addAllErrorTypeHierarchy(classes.toIterable.asJava)
          .build()))
  }
}
