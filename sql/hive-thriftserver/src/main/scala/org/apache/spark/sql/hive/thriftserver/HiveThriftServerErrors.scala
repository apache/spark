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

package org.apache.spark.sql.hive.thriftserver

import java.io.IOException
import java.util.concurrent.RejectedExecutionException

import org.apache.hive.service.ServiceException
import org.apache.hive.service.cli.{HiveSQLException, OperationType}

import org.apache.spark.SparkThrowable

/**
 * Object for grouping error messages from (most) exceptions thrown during
 * hive execution with thrift server.
 */
object HiveThriftServerErrors {

  def taskExecutionRejectedError(rejected: RejectedExecutionException): Throwable = {
    new HiveSQLException("The background threadpool cannot accept" +
      " new task for execution, please retry the operation", rejected)
  }

  def runningQueryError(e: Throwable): Throwable = e match {
    case st: SparkThrowable =>
      val errorClassPrefix = Option(st.getErrorClass).map(e => s"[$e] ").getOrElse("")
      new HiveSQLException(
        s"Error running query: ${errorClassPrefix}${st.toString}", st.getSqlState, st)
    case _ => new HiveSQLException(s"Error running query: ${e.toString}", e)
  }

  def hiveOperatingError(operationType: OperationType, e: Throwable): Throwable = {
    new HiveSQLException(s"Error operating $operationType ${e.getMessage}", e)
  }

  def failedToOpenNewSessionError(e: Throwable): Throwable = {
    new HiveSQLException(s"Failed to open new session: $e", e)
  }

  def cannotLoginToKerberosError(e: Throwable): Throwable = {
    new ServiceException("Unable to login to kerberos with given principal/keytab", e)
  }

  def cannotLoginToSpnegoError(
      principal: String, keyTabFile: String, e: IOException): Throwable = {
    new ServiceException("Unable to login to spnego with given principal " +
      s"$principal and keytab $keyTabFile: $e", e)
  }

  def failedToStartServiceError(serviceName: String, e: Throwable): Throwable = {
    new ServiceException(s"Failed to Start $serviceName", e)
  }
}
