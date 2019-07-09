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

import java.util.UUID

import scala.collection.JavaConverters.seqAsJavaListConverter

import org.apache.hadoop.hive.ql.security.authorization.plugin.{HiveOperationType, HivePrivilegeObjectUtils}
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.GetFunctionsOperation
import org.apache.hive.service.cli.session.HiveSession
import org.apache.thrift.TException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.util.{Utils => SparkUtils}

/**
 * Spark's own GetTablesOperation
 *
 * @param sqlContext SQLContext to use
 * @param parentSession a HiveSession from SessionManager
 * @param catalogName catalog name. null if not applicable
 * @param schemaName database name, null or a concrete database name
 * @param functionName function name pattern
 */
private[hive] class SparkGetFunctionsOperation(
    sqlContext: SQLContext,
    parentSession: HiveSession,
    catalogName: String,
    schemaName: String,
    functionName: String)
  extends GetFunctionsOperation(parentSession, catalogName, schemaName, functionName)
    with Logging {

  override def runInternal(): Unit = {
    val statementId = UUID.randomUUID().toString
    // Do not change cmdStr. It's used for Hive auditing and authorization.
    val cmdStr = s"catalog : $catalogName, schemaPattern : $schemaName"
    val logMsg = s"Listing functions '$cmdStr'"
    logInfo(s"$logMsg with $statementId")
    setState(OperationState.RUNNING)
    // Always use the latest class loader provided by executionHive's state.
    val executionHiveClassLoader = sqlContext.sharedState.jarClassLoader
    Thread.currentThread().setContextClassLoader(executionHiveClassLoader)

    val catalog = sqlContext.sessionState.catalog

    if (isAuthV2Enabled) {
      // get databases for schema pattern
      val schemaPattern = convertSchemaPattern(schemaName)
      var matchingDbs: Seq[String] = null
      try {
        matchingDbs = catalog.listDatabases(schemaPattern)
      } catch {
        case e: TException =>
          setState(OperationState.ERROR)
          HiveThriftServer2.listener.onStatementError(
            statementId, e.getMessage, SparkUtils.exceptionString(e))
          throw new HiveSQLException(e)
      }
      // authorize this call on the schema objects
      val privObjs =
        HivePrivilegeObjectUtils.getHivePrivDbObjects(seqAsJavaListConverter(matchingDbs).asJava)
      authorizeMetaGets(HiveOperationType.GET_FUNCTIONS, privObjs, cmdStr)
    }

    HiveThriftServer2.listener.onStatementStart(
      statementId,
      parentSession.getSessionHandle.getSessionId.toString,
      logMsg,
      statementId,
      parentSession.getUsername)

    try {
      val functionPattern = CLIServiceUtils.patternToRegex(functionName)
      if ((null == catalogName || "".equals(catalogName))
        && (null == schemaName || "".equals(schemaName))) {
        catalog.listFunctions(catalog.getCurrentDatabase, functionPattern).foreach {
          case (functionIdentifier, _) =>
            val rowData = Array[AnyRef](
              null, // FUNCTION_CAT
              null, // FUNCTION_SCHEM
              functionIdentifier.funcName, // FUNCTION_NAME
              "", // REMARKS
              1.asInstanceOf[AnyRef], // FUNCTION_TYPE
              "")
            rowSet.addRow(rowData);
        }
      }
      setState(OperationState.FINISHED)
    } catch {
      case e: HiveSQLException =>
        setState(OperationState.ERROR)
        HiveThriftServer2.listener.onStatementError(
          statementId, e.getMessage, SparkUtils.exceptionString(e))
        throw e
    }
    HiveThriftServer2.listener.onStatementFinish(statementId)
  }
}
