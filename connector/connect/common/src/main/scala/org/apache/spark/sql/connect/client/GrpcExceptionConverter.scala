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
import io.grpc.{ManagedChannel, StatusRuntimeException}
import io.grpc.protobuf.StatusProto
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.JsonMethods

import org.apache.spark.{QueryContext, QueryContextType, SparkArithmeticException, SparkArrayIndexOutOfBoundsException, SparkDateTimeException, SparkException, SparkIllegalArgumentException, SparkNumberFormatException, SparkRuntimeException, SparkUnsupportedOperationException, SparkUpgradeException}
import org.apache.spark.connect.proto.{FetchErrorDetailsRequest, FetchErrorDetailsResponse, SparkConnectServiceGrpc, UserContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchDatabaseException, NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException, TempTableAlreadyExistsException}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.util.ArrayImplicits._

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
private[client] class GrpcExceptionConverter(channel: ManagedChannel) extends Logging {
  import GrpcExceptionConverter._

  val grpcStub = SparkConnectServiceGrpc.newBlockingStub(channel)

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

private[client] object GrpcExceptionConverter {

  private[client] case class ErrorParams(
      message: String,
      cause: Option[Throwable],
      // errorClass will only be set if the error is SparkThrowable.
      errorClass: Option[String],
      // messageParameters will only be set if the error is both enriched and SparkThrowable.
      messageParameters: Map[String, String],
      // queryContext will only be set if the error is both enriched and SparkThrowable.
      queryContext: Array[QueryContext])

  private def errorConstructor[T <: Throwable: ClassTag](
      throwableCtr: ErrorParams => T): (String, ErrorParams => Throwable) = {
    val className = implicitly[reflect.ClassTag[T]].runtimeClass.getName
    (className, throwableCtr)
  }

  private[client] val errorFactory = Map(
    errorConstructor(params =>
      new StreamingQueryException(
        params.message,
        params.cause.orNull,
        params.errorClass.orNull,
        params.messageParameters)),
    errorConstructor(params =>
      new ParseException(
        None,
        Origin(),
        Origin(),
        errorClass = params.errorClass.orNull,
        messageParameters = params.messageParameters,
        queryContext = params.queryContext)),
    errorConstructor(params =>
      new AnalysisException(
        errorClass = params.errorClass.getOrElse("_LEGACY_ERROR_TEMP_3100"),
        messageParameters = errorParamsToMessageParameters(params),
        cause = params.cause,
        context = params.queryContext)),
    errorConstructor(params =>
      new NamespaceAlreadyExistsException(params.errorClass.orNull, params.messageParameters)),
    errorConstructor(params =>
      new TableAlreadyExistsException(
        params.errorClass.orNull,
        params.messageParameters,
        params.cause)),
    errorConstructor(params =>
      new TempTableAlreadyExistsException(
        params.errorClass.orNull,
        params.messageParameters,
        params.cause)),
    errorConstructor(params =>
      new NoSuchDatabaseException(
        params.errorClass.orNull,
        params.messageParameters,
        params.cause)),
    errorConstructor(params =>
      new NoSuchNamespaceException(params.errorClass.orNull, params.messageParameters)),
    errorConstructor(params =>
      new NoSuchTableException(params.errorClass.orNull, params.messageParameters, params.cause)),
    errorConstructor[NumberFormatException](params =>
      new SparkNumberFormatException(
        errorClass = params.errorClass.getOrElse("_LEGACY_ERROR_TEMP_3104"),
        messageParameters = errorParamsToMessageParameters(params),
        params.queryContext)),
    errorConstructor[IllegalArgumentException](params =>
      new SparkIllegalArgumentException(
        errorClass = params.errorClass.getOrElse("_LEGACY_ERROR_TEMP_3105"),
        messageParameters = errorParamsToMessageParameters(params),
        params.queryContext,
        summary = "",
        cause = params.cause.orNull)),
    errorConstructor[ArithmeticException](params =>
      new SparkArithmeticException(
        errorClass = params.errorClass.getOrElse("_LEGACY_ERROR_TEMP_3106"),
        messageParameters = errorParamsToMessageParameters(params),
        params.queryContext)),
    errorConstructor[UnsupportedOperationException](params =>
      new SparkUnsupportedOperationException(
        errorClass = params.errorClass.getOrElse("_LEGACY_ERROR_TEMP_3107"),
        messageParameters = errorParamsToMessageParameters(params))),
    errorConstructor[ArrayIndexOutOfBoundsException](params =>
      new SparkArrayIndexOutOfBoundsException(
        errorClass = params.errorClass.getOrElse("_LEGACY_ERROR_TEMP_3108"),
        messageParameters = errorParamsToMessageParameters(params),
        params.queryContext)),
    errorConstructor[DateTimeException](params =>
      new SparkDateTimeException(
        errorClass = params.errorClass.getOrElse("_LEGACY_ERROR_TEMP_3109"),
        messageParameters = errorParamsToMessageParameters(params),
        params.queryContext)),
    errorConstructor(params =>
      new SparkRuntimeException(
        params.errorClass.orNull,
        params.messageParameters,
        params.cause.orNull,
        params.queryContext)),
    errorConstructor(params =>
      new SparkUpgradeException(
        params.errorClass.orNull,
        params.messageParameters,
        params.cause.orNull)),
    errorConstructor(params =>
      new SparkException(
        message = params.message,
        cause = params.cause.orNull,
        errorClass = params.errorClass,
        messageParameters = params.messageParameters,
        context = params.queryContext)))

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

    val errorClass = if (error.hasSparkThrowable && error.getSparkThrowable.hasErrorClass) {
      Some(error.getSparkThrowable.getErrorClass)
    } else None

    val messageParameters = if (error.hasSparkThrowable) {
      error.getSparkThrowable.getMessageParametersMap.asScala.toMap
    } else Map.empty[String, String]

    val queryContext = error.getSparkThrowable.getQueryContextsList.asScala.map { queryCtx =>
      new QueryContext {
        override def contextType(): QueryContextType = queryCtx.getContextType match {
          case FetchErrorDetailsResponse.QueryContext.ContextType.DATAFRAME =>
            QueryContextType.DataFrame
          case _ => QueryContextType.SQL
        }
        override def objectType(): String = queryCtx.getObjectType
        override def objectName(): String = queryCtx.getObjectName
        override def startIndex(): Int = queryCtx.getStartIndex
        override def stopIndex(): Int = queryCtx.getStopIndex
        override def fragment(): String = queryCtx.getFragment
        override def callSite(): String = queryCtx.getCallSite
        override def summary(): String = queryCtx.getSummary
      }
    }.toArray

    val exception = constructor(
      ErrorParams(
        message = error.getMessage,
        cause = causeOpt,
        errorClass = errorClass,
        messageParameters = messageParameters,
        queryContext = queryContext))

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
    implicit val formats: Formats = DefaultFormats
    val classes =
      JsonMethods.parse(info.getMetadataOrDefault("classes", "[]")).extract[Array[String]]
    val errorClass = info.getMetadataOrDefault("errorClass", null)
    val builder = FetchErrorDetailsResponse.Error
      .newBuilder()
      .setMessage(message)
      .addAllErrorTypeHierarchy(classes.toImmutableArraySeq.asJava)

    if (errorClass != null) {
      val messageParameters = JsonMethods
        .parse(info.getMetadataOrDefault("messageParameters", "{}"))
        .extract[Map[String, String]]
      builder.setSparkThrowable(
        FetchErrorDetailsResponse.SparkThrowable
          .newBuilder()
          .setErrorClass(errorClass)
          .putAllMessageParameters(messageParameters.asJava)
          .build())
    }

    errorsToThrowable(0, Seq(builder.build()))
  }

  /**
   * This method is used to convert error parameters to message parameters.
   *
   * @param params
   *   The error parameters to be converted.
   * @return
   *   A Map of message parameters. If the error class is defined in the params, it returns the
   *   message parameters from the params. If the error class is not defined, it returns a new Map
   *   with a single entry where the key is "message" and the value is the message from the
   *   params.
   */
  private def errorParamsToMessageParameters(params: ErrorParams): Map[String, String] =
    params.errorClass match {
      case Some(_) => params.messageParameters
      case None => Map("message" -> params.message)
    }
}
