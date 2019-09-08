/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.server.cli

import java.sql.SQLException

import scala.collection.JavaConverters._

import org.apache.spark.service.cli.thrift.{TStatus, TStatusCode}

class SparkThriftServerSQLException(reason: String,
                                    sqlState: String,
                                    vendorCode: Int,
                                    cause: Throwable)
  extends SQLException(reason, sqlState, vendorCode, cause) {

  def this(reason: String, sqlState: String, cause: Throwable) = this(reason, sqlState, 0, cause)

  def this(reason: String, sqlState: String, vendorCode: Int) =
    this(reason, sqlState, vendorCode, null)

  def this(reason: String, cause: Throwable) = this(reason, null, 0, cause)

  def this(reason: String, sqlState: String) = this(reason, sqlState, vendorCode = 0)

  def this(reason: String) = this(reason, sqlState = null)

  def this(cause: Throwable) = this(cause.toString, cause)

  def this(status: TStatus) {
    // TODO: set correct vendorCode field
    this(status.getErrorMessage, status.getSqlState, status.getErrorCode)
    //    if (status.getInfoMessages != null) {
    //      initCause(toCause(status.getInfoMessages.asScala.toArray))
    //    }
  }

  /**
   * Converts current object to a [[TStatus]] object
   *
   * @return a { @link TStatus} object
   */
  def toTStatus: TStatus = {
    val tStatus = new TStatus(TStatusCode.ERROR_STATUS)
    tStatus.setSqlState(getSQLState)
    tStatus.setErrorCode(getErrorCode)
    tStatus.setErrorMessage(getMessage)
    tStatus.setInfoMessages(SparkThriftServerSQLException.toString(this).asJava)
    tStatus
  }

  //  def toCause(details: Array[String]): Throwable = {
  //    toStackTrace(details, null, 0)
  //  }

}

object SparkThriftServerSQLException {

  def toTStatus(e: Exception): TStatus = e match {
    case k: SparkThriftServerSQLException => k.toTStatus
    case _ =>
      val tStatus = new TStatus(TStatusCode.ERROR_STATUS)
      tStatus.setErrorMessage(e.getMessage)
      tStatus.setInfoMessages(toString(e).asJava)
      tStatus
  }


  def toString(cause: Throwable): List[String] = {
    toString(cause, null)
  }

  def toString(cause: Throwable, parent: Array[StackTraceElement]): List[String] = {
    val trace = cause.getStackTrace
    var m = trace.length - 1
    if (parent != null) {
      var n = parent.length - 1
      while (m >= 0 && n >= 0 && trace(m).equals(parent(n))) {
        m = m - 1
        n = n - 1
      }
    }

    enroll(cause, trace, m) ++
      Option(cause.getCause).map(toString(_, trace)).getOrElse(Nil)
  }

  private[this] def enroll(ex: Throwable,
                           trace: Array[StackTraceElement], max: Int): List[String] = {
    val builder = new StringBuilder
    builder.append('*').append(ex.getClass.getName).append(':')
    builder.append(ex.getMessage).append(':')
    builder.append(trace.length).append(':').append(max)
    List(builder.toString) ++ (0 to max).map { i =>
      builder.setLength(0)
      builder.append(trace(i).getClassName).append(":")
      builder.append(trace(i).getMethodName).append(":")
      builder.append(Option(trace(i).getFileName).getOrElse("")).append(':')
      builder.append(trace(i).getLineNumber)
      builder.toString
    }.toList
  }
}
