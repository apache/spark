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

package org.apache.spark.sql.hive.thriftserver.server

import java.util.{Map => JMap}
import scala.collection.mutable.Map

import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.{ExecuteStatementOperation, Operation, OperationManager}
import org.apache.hive.service.cli.session.HiveSession
import org.apache.spark.Logging
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.{SparkExecuteStatementOperation, ReflectionUtils}

/**
 * Executes queries using Spark SQL, and maintains a list of handles to active queries.
 */
private[thriftserver] class SparkSQLOperationManager()
  extends OperationManager with Logging {

  val handleToOperation = ReflectionUtils
    .getSuperField[JMap[OperationHandle, Operation]](this, "handleToOperation")

  val sessionToActivePool = Map[SessionHandle, String]()
  val sessionToContexts = Map[SessionHandle, HiveContext]()

  override def newExecuteStatementOperation(
      parentSession: HiveSession,
      statement: String,
      confOverlay: JMap[String, String],
      async: Boolean): ExecuteStatementOperation = synchronized {
    val hiveContext = sessionToContexts(parentSession.getSessionHandle)
    val runInBackground = async && hiveContext.hiveThriftServerAsync
    val operation = new SparkExecuteStatementOperation(parentSession, statement, confOverlay,
      runInBackground)(hiveContext, sessionToActivePool)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created Operation for $statement with session=$parentSession, " +
      s"runInBackground=$runInBackground")
    operation
  }
}
