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

import scala.collection.JavaConverters._
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

  def convert[T](sessionId: String, userContext: UserContext)(f: => T): T = {
    try {
      f
    } catch {
      case e: StatusRuntimeException =>
        throw toThrowable(e, sessionId, userContext)
    }
  }

  def convertIterator[T](
      sessionId: String,
      userContext: UserContext,
      iter: CloseableIterator[T]): CloseableIterator[T] = {
    new WrappedCloseableIterator[T] {

      override def innerIterator: Iterator[T] = iter

      override def hasNext: Boolean = {
        convert(sessionId, userContext) {
          iter.hasNext
        }
      }

      override def next(): T = {
        convert(sessionId, userContext) {
          iter.next()
        }
      }

      override def close(): Unit = {
        convert(sessionId, userContext) {
          iter.close()
        }
      }
    }
  }

  /**
   * fetchEnrichedError fetches enriched errors with full exception message and optionally
   * stacktrace by issuing an additional RPC call to fetch error details. The RPC call is
   * best-effort at-most-once.
   */
  private def fetchEnrichedError(
      info: ErrorInfo,
      sessionId: String,
      userContext: UserContext): Option[Throwable] = {
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
          .setUserContext(UserContext.newBuilder().setUserId(userContext.getUserId).build())
          .build())

      if (!errorDetailsResponse.hasRootErrorIdx) {
        logWarning("Unable to fetch enriched error since error is not found")
        return None
      }

      Some(
        errorsToThrowable(
          errorDetailsResponse.getRootErrorIdx,
          errorDetailsResponse.getErrorsList.asScala))
    } catch {
      case e: StatusRuntimeException =>
        logWarning("Unable to fetch enriched error", e)
        None
    }
  }

  private def toThrowable(
      ex: StatusRuntimeException,
      sessionId: String,
      userContext: UserContext): Throwable = {
    val status = StatusProto.fromThrowable(ex)

    val errorInfoOpt = status.getDetailsList.asScala
      .find(_.is(classOf[ErrorInfo]))
      .map(_.unpack(classOf[ErrorInfo]))

    if (errorInfoOpt.isDefined) {
      val enrichedErrorOpt = fetchEnrichedError(errorInfoOpt.get, sessionId, userContext)
      if (enrichedErrorOpt.isDefined) {
        return enrichedErrorOpt.get
      }

      return errorInfoToThrowable(errorInfoOpt.get, StatusProto.fromThrowable(ex).getMessage)
    }

    new SparkException(ex.toString, ex.getCause)
  }
}

private object GrpcExceptionConverter {

  private def errorConstructor[T <: Throwable: ClassTag](
      throwableCtr: (String, Option[Throwable]) => T)
      : (String, (String, Option[Throwable]) => Throwable) = {
    val className = implicitly[reflect.ClassTag[T]].runtimeClass.getName
    (className, throwableCtr)
  }

  private val errorFactory = Map(
    errorConstructor((message, _) => new ParseException(None, message, Origin(), Origin())),
    errorConstructor((message, cause) => new AnalysisException(message, cause = cause)),
    errorConstructor((message, _) => new NamespaceAlreadyExistsException(message)),
    errorConstructor((message, cause) => new TableAlreadyExistsException(message, cause)),
    errorConstructor((message, cause) => new TempTableAlreadyExistsException(message, cause)),
    errorConstructor((message, cause) => new NoSuchDatabaseException(message, cause)),
    errorConstructor((message, cause) => new NoSuchTableException(message, cause)),
    errorConstructor[NumberFormatException]((message, _) =>
      new SparkNumberFormatException(message)),
    errorConstructor[IllegalArgumentException]((message, cause) =>
      new SparkIllegalArgumentException(message, cause)),
    errorConstructor[ArithmeticException]((message, _) => new SparkArithmeticException(message)),
    errorConstructor[UnsupportedOperationException]((message, _) =>
      new SparkUnsupportedOperationException(message)),
    errorConstructor[ArrayIndexOutOfBoundsException]((message, _) =>
      new SparkArrayIndexOutOfBoundsException(message)),
    errorConstructor[DateTimeException]((message, _) => new SparkDateTimeException(message)),
    errorConstructor((message, cause) => new SparkRuntimeException(message, cause)),
    errorConstructor((message, cause) => new SparkUpgradeException(message, cause)),
    errorConstructor((message, cause) => new SparkException(message, cause.orNull)))

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
        .getOrElse((message: String, cause: Option[Throwable]) =>
          new SparkException(
            s"Exception received from Spark Connect on server ${classHierarchy.head}: ${message}",
            cause.orNull))

    val causeOpt =
      if (error.hasCauseIdx) Some(errorsToThrowable(error.getCauseIdx, errors)) else None

    val exception = constructor(error.getMessage, causeOpt)

    if (!error.getStackTraceList.isEmpty) {
      exception.setStackTrace(error.getStackTraceList.asScala.toArray.map { stackTraceElement =>
        new StackTraceElement(
          stackTraceElement.getDeclaringClass,
          stackTraceElement.getMethodName,
          stackTraceElement.getFileName,
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
