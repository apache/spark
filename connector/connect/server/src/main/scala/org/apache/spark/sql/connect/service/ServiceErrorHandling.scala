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
package org.apache.spark.sql.connect.service

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.google.protobuf.{Any => ProtoAny}
import com.google.rpc.{Code => RPCCode, ErrorInfo, Status => RPCStatus}
import io.grpc.Status
import io.grpc.protobuf.StatusProto
import io.grpc.stub.StreamObserver
import org.apache.commons.lang3.StringUtils
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.api.python.PythonException
import org.apache.spark.internal.Logging

/**
 * These are common functions used for error handling
 * in SparkConnect service and can be used by other custom
 * services.
 *
 */
trait ServiceErrorHandling extends Logging {

  private[spark] def allClasses(cl: Class[_]): Seq[Class[_]] = {
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

  private[spark] def buildStatusFromThrowable(st: Throwable): RPCStatus = {
    val message = StringUtils.abbreviate(st.getMessage, 2048)
    RPCStatus
      .newBuilder()
      .setCode(RPCCode.INTERNAL_VALUE)
      .addDetails(
        ProtoAny.pack(
          ErrorInfo
            .newBuilder()
            .setReason(st.getClass.getName)
            .setDomain("org.apache.spark")
            .putMetadata("classes", compact(render(allClasses(st.getClass).map(_.getName))))
            .build()))
      .setMessage(if (message != null) message else "")
      .build()
  }

  private[spark] def isPythonExecutionException(se: SparkException): Boolean = {
    // See also pyspark.errors.exceptions.captured.convert_exception in PySpark.
    se.getCause != null && se.getCause
      .isInstanceOf[PythonException] && se.getCause.getStackTrace
      .exists(_.toString.contains("org.apache.spark.sql.execution.python"))
  }

  /**
   * Common exception handling function for the Analysis and Execution methods. Closes the stream
   * after the error has been sent.
   *
   * @param opType
   * String value indicating the operation type (analysis, execution)
   * @param observer
   * The GRPC response observer.
   * @tparam V
   * @return
   */
  private[spark] def handleError[V](
                              opType: String,
                              observer: StreamObserver[V]): PartialFunction[Throwable, Unit] = {
    case se: SparkException if isPythonExecutionException(se) =>
      logError(s"Error during: $opType", se)
      observer.onError(
        StatusProto.toStatusRuntimeException(buildStatusFromThrowable(se.getCause)))

    case e: Throwable if e.isInstanceOf[SparkThrowable] || NonFatal.apply(e) =>
      logError(s"Error during: $opType", e)
      observer.onError(StatusProto.toStatusRuntimeException(buildStatusFromThrowable(e)))

    case e: Throwable =>
      logError(s"Error during: $opType", e)
      observer.onError(
        Status.UNKNOWN
          .withCause(e)
          .withDescription(StringUtils.abbreviate(e.getMessage, 2048))
          .asRuntimeException())
  }

}
