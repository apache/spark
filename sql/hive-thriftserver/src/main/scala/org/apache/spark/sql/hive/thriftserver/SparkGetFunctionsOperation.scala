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

import java.sql.DatabaseMetaData

import scala.jdk.CollectionConverters._

import org.apache.hadoop.hive.ql.security.authorization.plugin.{HiveOperationType, HivePrivilegeObjectUtils}
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.GetFunctionsOperation
import org.apache.hive.service.cli.operation.MetadataOperation.DEFAULT_HIVE_CATALOG
import org.apache.hive.service.cli.session.HiveSession

import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.SparkSession

/**
 * Spark's own GetFunctionsOperation
 *
 * @param session SparkSession to use
 * @param parentSession a HiveSession from SessionManager
 * @param catalogName catalog name. null if not applicable
 * @param schemaName database name, null or a concrete database name
 * @param functionName function name pattern
 */
private[hive] class SparkGetFunctionsOperation(
    val session: SparkSession,
    parentSession: HiveSession,
    catalogName: String,
    schemaName: String,
    functionName: String)
  extends GetFunctionsOperation(parentSession, catalogName, schemaName, functionName)
  with SparkOperation
  with Logging {

  override def runInternal(): Unit = {
    // Do not change cmdStr. It's used for Hive auditing and authorization.
    val cmdMDC = log"catalog : ${MDC(LogKeys.CATALOG_NAME, catalogName)}, " +
      log"schemaPattern : ${MDC(LogKeys.DATABASE_NAME, schemaName)}"
    val logMDC = log"Listing functions '" + cmdMDC +
      log", functionName : ${MDC(LogKeys.FUNCTION_NAME, functionName)}'"
    logInfo(logMDC + log" with ${MDC(LogKeys.STATEMENT_ID, statementId)}")
    setState(OperationState.RUNNING)
    // Always use the latest class loader provided by executionHive's state.
    val executionHiveClassLoader = session.sharedState.jarClassLoader
    Thread.currentThread().setContextClassLoader(executionHiveClassLoader)

    val catalog = session.sessionState.catalog
    // get databases for schema pattern
    val schemaPattern = convertSchemaPattern(schemaName)
    val matchingDbs = catalog.listDatabases(schemaPattern)
    val functionPattern = CLIServiceUtils.patternToRegex(functionName)

    if (isAuthV2Enabled) {
      // authorize this call on the schema objects
      val privObjs =
        HivePrivilegeObjectUtils.getHivePrivDbObjects(matchingDbs.asJava)
      authorizeMetaGets(HiveOperationType.GET_FUNCTIONS, privObjs, cmdMDC.message)
    }

    HiveThriftServer2.eventManager.onStatementStart(
      statementId,
      parentSession.getSessionHandle.getSessionId.toString,
      logMDC.message,
      statementId,
      parentSession.getUsername)

    try {
      matchingDbs.foreach { db =>
        catalog.listFunctions(db, functionPattern).foreach {
          case (funcIdentifier, _) =>
            val info = catalog.lookupFunctionInfo(funcIdentifier)
            val rowData = Array[AnyRef](
              DEFAULT_HIVE_CATALOG, // FUNCTION_CAT
              db, // FUNCTION_SCHEM
              funcIdentifier.funcName, // FUNCTION_NAME
              s"Usage: ${info.getUsage}\nExtended Usage:${info.getExtended}", // REMARKS
              DatabaseMetaData.functionResultUnknown.asInstanceOf[AnyRef], // FUNCTION_TYPE
              info.getClassName) // SPECIFIC_NAME
            rowSet.addRow(rowData);
        }
      }
      setState(OperationState.FINISHED)
    } catch onError()

    HiveThriftServer2.eventManager.onStatementFinish(statementId)
  }
}
