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

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType
import org.apache.hive.service.cli.OperationState
import org.apache.hive.service.cli.operation.GetCatalogsOperation
import org.apache.hive.service.cli.session.HiveSession

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.sql.SparkSession

/**
 * Spark's own GetCatalogsOperation
 *
 * @param session SparkSession to use
 * @param parentSession a HiveSession from SessionManager
 */
private[hive] class SparkGetCatalogsOperation(
    val session: SparkSession,
    parentSession: HiveSession)
  extends GetCatalogsOperation(parentSession)
  with SparkOperation
  with Logging {

  override def runInternal(): Unit = withClassLoader { _ =>
    val logMsg = "Listing catalogs"
    logInfo(log"Listing catalogs with ${MDC(STATEMENT_ID, statementId)}")
    setState(OperationState.RUNNING)
    HiveThriftServer2.eventManager.onStatementStart(
      statementId,
      parentSession.getSessionHandle.getSessionId.toString,
      logMsg,
      statementId,
      parentSession.getUsername)

    try {
      if (isAuthV2Enabled) {
        authorizeMetaGets(HiveOperationType.GET_CATALOGS, null)
      }
      setState(OperationState.FINISHED)
    } catch onError()

    HiveThriftServer2.eventManager.onStatementFinish(statementId)
  }
}
