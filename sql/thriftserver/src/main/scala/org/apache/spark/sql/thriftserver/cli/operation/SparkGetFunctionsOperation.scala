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

package org.apache.spark.sql.thriftserver.cli.operation

import java.sql.DatabaseMetaData
import java.util.UUID

import scala.collection.JavaConverters.seqAsJavaListConverter

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.hive.ql.security.authorization.plugin.{HiveOperationType, HivePrivilegeObjectUtils}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.thriftserver.SparkThriftServer2
import org.apache.spark.sql.thriftserver.cli._
import org.apache.spark.sql.thriftserver.cli.session.ThriftServerSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.util.{Utils => SparkUtils}

/**
 * Spark's own GetFunctionsOperation
 *
 * @param sqlContext    SQLContext to use
 * @param parentSession a HiveSession from SessionManager
 * @param catalogName   catalog name. null if not applicable
 * @param schemaName    database name, null or a concrete database name
 * @param functionName  function name pattern
 */
private[thriftserver] class SparkGetFunctionsOperation(
    sqlContext: SQLContext,
    parentSession: ThriftServerSession,
    catalogName: String,
    schemaName: String,
    functionName: String)
  extends SparkMetadataOperation(parentSession, GET_FUNCTIONS) with Logging {

  RESULT_SET_SCHEMA = new StructType()
    .add(StructField("FUNCTION_CAT", StringType))
    .add(StructField("FUNCTION_SCHEM", StringType))
    .add(StructField("FUNCTION_NAME", StringType))
    .add(StructField("REMARKS", StringType))
    .add(StructField("FUNCTION_TYPE", IntegerType))
    .add(StructField("SPECIFIC_NAME", StringType))

  private val rowSet: RowSet = RowSetFactory.create(RESULT_SET_SCHEMA, getProtocolVersion)

  override def close(): Unit = {
    super.close()
    SparkThriftServer2.listener.onOperationClosed(_statementId)
  }

  override def runInternal(): Unit = {
    _statementId = UUID.randomUUID().toString
    // Do not change cmdStr. It's used for Hive auditing and authorization.
    val cmdStr = s"catalog : $catalogName, schemaPattern : $schemaName"
    val logMsg = s"Listing functions '$cmdStr, functionName : $functionName'"
    logInfo(s"$logMsg with $statementId")
    setState(RUNNING)
    // Always use the latest class loader provided by executionHive's state.
    val executionHiveClassLoader = sqlContext.sharedState.jarClassLoader
    Thread.currentThread().setContextClassLoader(executionHiveClassLoader)

    val catalog = sqlContext.sessionState.catalog
    // get databases for schema pattern
    val schemaPattern = convertSchemaPattern(schemaName)
    val matchingDbs = catalog.listDatabases(schemaPattern)
    val functionPattern = CLIServiceUtils.patternToRegex(functionName)

    if (isAuthV2Enabled) {
      // authorize this call on the schema objects
      val privObjs =
        HivePrivilegeObjectUtils.getHivePrivDbObjects(seqAsJavaListConverter(matchingDbs).asJava)
      authorizeMetaGets(HiveOperationType.GET_FUNCTIONS, privObjs, cmdStr)
    }

    SparkThriftServer2.listener.onStatementStart(
      _statementId,
      parentSession.getSessionHandle.getSessionId.toString,
      logMsg,
      _statementId,
      parentSession.getUsername)

    try {
      matchingDbs.foreach { db =>
        catalog.listFunctions(db, functionPattern).foreach {
          case (funcIdentifier, _) =>
            val info = catalog.lookupFunctionInfo(funcIdentifier)
            val rowData = Row(
              DEFAULT_HIVE_CATALOG, // FUNCTION_CAT
              db, // FUNCTION_SCHEM
              funcIdentifier.funcName, // FUNCTION_NAME
              info.getUsage, // REMARKS
              DatabaseMetaData.functionResultUnknown.asInstanceOf[AnyRef], // FUNCTION_TYPE
              info.getClassName) // SPECIFIC_NAME
            rowSet.addRow(rowData);
        }
      }
      setState(FINISHED)
    } catch {
      case e: Throwable =>
        logError(s"Error executing get functions operation with $statementId", e)
        setState(ERROR)
        e match {
          case hiveException: SparkThriftServerSQLException =>
            SparkThriftServer2.listener.onStatementError(
              _statementId, hiveException.getMessage, SparkUtils.exceptionString(hiveException))
            throw hiveException
          case _ =>
            val root = ExceptionUtils.getRootCause(e)
            SparkThriftServer2.listener.onStatementError(
              _statementId, root.getMessage, SparkUtils.exceptionString(root))
            throw new SparkThriftServerSQLException("Error getting functions: " +
              root.toString, root)
        }
    }
    SparkThriftServer2.listener.onStatementFinish(_statementId)
  }

  override def getResultSetSchema: StructType = {
    assertState(FINISHED)
    RESULT_SET_SCHEMA
  }

  override def getNextRowSet(orientation: FetchOrientation, maxRows: Long): RowSet = {
    assertState(FINISHED)
    validateDefaultFetchOrientation(orientation)
    if (orientation == FetchOrientation.FETCH_FIRST) {
      rowSet.setStartOffset(0)
    }
    rowSet.extractSubset(maxRows.toInt)
  }
}
