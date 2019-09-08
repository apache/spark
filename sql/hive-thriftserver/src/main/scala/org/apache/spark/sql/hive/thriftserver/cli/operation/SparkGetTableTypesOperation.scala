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

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.apache.spark.sql.hive.thriftserver.cli._
import org.apache.spark.sql.hive.thriftserver.cli.session.ThriftSession
import org.apache.spark.sql.hive.thriftserver.server.cli.SparkThriftServerSQLException
import org.apache.spark.sql.types._
import org.apache.spark.util.{Utils => SparkUtils}

/**
 * Spark's own GetTableTypesOperation
 *
 * @param sqlContext    SQLContext to use
 * @param parentSession a HiveSession from SessionManager
 */
private[hive] class SparkGetTableTypesOperation(sqlContext: SQLContext,
                                                parentSession: ThriftSession)
  extends SparkMetadataOperation(parentSession, GET_TABLE_TYPES)
  with SparkMetadataOperationUtils with Logging {

  private var statementId: String = _
  RESULT_SET_SCHEMA = new StructType()
    .add(StructField("TABLE_TYPE", StringType))

  private var tableTypeMapping: TableTypeMapping = {
    val tableMappingStr =
      parentSession.getHiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_TABLE_TYPE_MAPPING)
    TableTypeMappingFactory.getTableTypeMapping(tableMappingStr)
  }

  private val rowSet: RowSet = RowSetFactory.create(RESULT_SET_SCHEMA, getProtocolVersion)

  override def close(): Unit = {
    super.close()
    HiveThriftServer2.listener.onOperationClosed(statementId)
  }

  override def runInternal(): Unit = {
    statementId = UUID.randomUUID().toString
    val logMsg = "Listing table types"
    logInfo(s"$logMsg with $statementId")
    setState(RUNNING)
    // Always use the latest class loader provided by executionHive's state.
    val executionHiveClassLoader = sqlContext.sharedState.jarClassLoader
    Thread.currentThread().setContextClassLoader(executionHiveClassLoader)

    if (isAuthV2Enabled) {
      authorizeMetaGets(HiveOperationType.GET_TABLETYPES, null)
    }

    HiveThriftServer2.listener.onStatementStart(
      statementId,
      parentSession.getSessionHandle.getSessionId.toString,
      logMsg,
      statementId,
      parentSession.getUsername)

    try {
      val tableTypes = CatalogTableType.tableTypes.map(tableTypeString).toSet
      tableTypes.foreach { tableType =>
        rowSet.addRow(Row(tableType))
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
