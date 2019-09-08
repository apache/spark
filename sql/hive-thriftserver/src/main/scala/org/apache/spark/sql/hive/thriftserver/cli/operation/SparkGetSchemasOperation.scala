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

package org.apache.spark.sql.hive.thriftserver.cli.operation

import java.util.UUID
import java.util.regex.Pattern

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.apache.spark.sql.hive.thriftserver.cli._
import org.apache.spark.sql.hive.thriftserver.cli.session.ThriftSession
import org.apache.spark.sql.hive.thriftserver.cli.thrift.CLIServiceUtils
import org.apache.spark.sql.hive.thriftserver.server.cli.SparkThriftServerSQLException
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.util.{Utils => SparkUtils}

/**
 * Spark's own GetSchemasOperation
 *
 * @param sqlContext    SQLContext to use
 * @param parentSession a HiveSession from SessionManager
 * @param catalogName   catalog name. null if not applicable.
 * @param schemaName    database name, null or a concrete database name
 */
private[hive] class SparkGetSchemasOperation(sqlContext: SQLContext,
                                             parentSession: ThriftSession,
                                             catalogName: String,
                                             schemaName: String)
  extends SparkMetadataOperation(parentSession, GET_SCHEMAS) with Logging {

  private var statementId: String = _
  RESULT_SET_SCHEMA = new StructType()
    .add(StructField("TABLE_SCHEM", StringType))
    .add(StructField("TABLE_CATALOG", StringType))


  private val rowSet: RowSet = RowSetFactory.create(RESULT_SET_SCHEMA, getProtocolVersion)

  override def close(): Unit = {
    super.close()
    HiveThriftServer2.listener.onOperationClosed(statementId)
  }

  override def runInternal(): Unit = {
    statementId = UUID.randomUUID().toString
    // Do not change cmdStr. It's used for Hive auditing and authorization.
    val cmdStr = s"catalog : $catalogName, schemaPattern : $schemaName"
    val logMsg = s"Listing databases '$cmdStr'"
    logInfo(s"$logMsg with $statementId")
    setState(RUNNING)
    // Always use the latest class loader provided by executionHive's state.
    val executionHiveClassLoader = sqlContext.sharedState.jarClassLoader
    Thread.currentThread().setContextClassLoader(executionHiveClassLoader)

    if (isAuthV2Enabled) {
      authorizeMetaGets(HiveOperationType.GET_TABLES, null, cmdStr)
    }

    HiveThriftServer2.listener.onStatementStart(
      statementId,
      parentSession.getSessionHandle.getSessionId.toString,
      logMsg,
      statementId,
      parentSession.getUsername)

    try {
      val schemaPattern = convertSchemaPattern(schemaName)
      sqlContext.sessionState.catalog.listDatabases(schemaPattern).foreach { dbName =>
        rowSet.addRow(Row(dbName, DEFAULT_HIVE_CATALOG))
      }

      val globalTempViewDb = sqlContext.sessionState.catalog.globalTempViewManager.database
      val databasePattern = Pattern.compile(CLIServiceUtils.patternToRegex(schemaName))
      if (databasePattern.matcher(globalTempViewDb).matches()) {
        rowSet.addRow(Row(globalTempViewDb, DEFAULT_HIVE_CATALOG))
      }
      setState(FINISHED)
    } catch {
      case e: SparkThriftServerSQLException =>
        setState(ERROR)
        HiveThriftServer2.listener.onStatementError(
          statementId, e.getMessage, SparkUtils.exceptionString(e))
        throw e
    }
    HiveThriftServer2.listener.onStatementFinish(statementId)
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
