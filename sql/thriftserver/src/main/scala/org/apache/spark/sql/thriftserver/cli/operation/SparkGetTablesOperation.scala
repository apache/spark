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

import java.util.{List => JList, UUID}
import java.util.regex.Pattern

import scala.collection.JavaConverters._

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.hive.ql.security.authorization.plugin.{HiveOperationType, HivePrivilegeObjectUtils}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.catalog.CatalogTableType._
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.thriftserver.cli._
import org.apache.spark.sql.thriftserver.cli.session.ThriftServerSession
import org.apache.spark.sql.thriftserver.server.SparkThriftServer
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.util.{Utils => SparkUtils}

/**
 * Spark's own GetTablesOperation
 *
 * @param sqlContext SQLContext to use
 * @param parentSession a HiveSession from SessionManager
 * @param catalogName catalog name. null if not applicable
 * @param schemaName database name, null or a concrete database name
 * @param tableName table name pattern
 * @param tableTypes list of allowed table types, e.g. "TABLE", "VIEW"
 */
private[thriftserver] class SparkGetTablesOperation(
    sqlContext: SQLContext,
    parentSession: ThriftServerSession,
    catalogName: String,
    schemaName: String,
    tableName: String,
    tableTypes: JList[String])
  extends SparkMetadataOperation(parentSession, OperationType.GET_TABLES)
  with Logging {

  if (HiveUtils.isHive23) {
    RESULT_SET_SCHEMA = new TableSchema()
      .addStringColumn("TABLE_CAT",
        "Catalog name. NULL if not applicable.")
      .addStringColumn("TABLE_SCHEM",
        "Schema name.")
      .addStringColumn("TABLE_NAME",
        "Table name.")
      .addStringColumn("TABLE_TYPE",
        "The table type, e.g. \"TABLE\", \"VIEW\", etc.")
      .addStringColumn("REMARKS",
        "Comments about the table.")
      .addStringColumn("TYPE_CAT",
        "The types catalog.")
      .addStringColumn("TYPE_SCHEM",
        "The types schema.")
      .addStringColumn("TYPE_NAME",
        "Type name.")
      .addStringColumn("SELF_REFERENCING_COL_NAME",
        "Name of the designated \"identifier\" column of a typed table.")
      .addStringColumn("REF_GENERATION",
        "Specifies how values in SELF_REFERENCING_COL_NAME are created.")
  } else {
    RESULT_SET_SCHEMA = new TableSchema()
      .addStringColumn("TABLE_CAT",
        "Catalog name. NULL if not applicable.")
      .addStringColumn("TABLE_SCHEM",
        "Schema name.")
      .addStringColumn("TABLE_NAME",
        "Table name.")
      .addStringColumn("TABLE_TYPE",
        "The table type, e.g. \"TABLE\", \"VIEW\", etc.")
      .addStringColumn("REMARKS",
        "Comments about the table.")
  }

  private val rowSet: RowSet = RowSetFactory.create(RESULT_SET_SCHEMA, getProtocolVersion, false)

  override def close(): Unit = {
    super.close()
    SparkThriftServer.listener.onOperationClosed(statementId)
  }

  override def runInternal(): Unit = {
    statementId = UUID.randomUUID().toString
    // Do not change cmdStr. It's used for Hive auditing and authorization.
    val cmdStr = s"catalog : $catalogName, schemaPattern : $schemaName"
    val tableTypesStr = if (tableTypes == null) {
      "null"
    } else {
      tableTypes.asScala.mkString(",")
    }
    val logMsg = s"Listing tables '$cmdStr, tableTypes : $tableTypesStr, tableName : $tableName'"
    logInfo(s"$logMsg with $statementId")
    setState(OperationState.RUNNING)
    // Always use the latest class loader provided by executionHive's state.
    val executionHiveClassLoader = sqlContext.sharedState.jarClassLoader
    Thread.currentThread().setContextClassLoader(executionHiveClassLoader)

    val catalog = sqlContext.sessionState.catalog
    val schemaPattern = convertSchemaPattern(schemaName)
    val tablePattern = convertIdentifierPattern(tableName, true)
    val matchingDbs = catalog.listDatabases(schemaPattern)

    if (isAuthV2Enabled) {
      val privObjs =
        HivePrivilegeObjectUtils.getHivePrivDbObjects(seqAsJavaListConverter(matchingDbs).asJava)
      authorizeMetaGets(HiveOperationType.GET_TABLES, privObjs, cmdStr)
    }

    SparkThriftServer.listener.onStatementStart(
      statementId,
      parentSession.getSessionHandle.getSessionId.toString,
      logMsg,
      statementId,
      parentSession.getUserName)

    try {
      // Tables and views
      matchingDbs.foreach { dbName =>
        val tables = catalog.listTables(dbName, tablePattern, includeLocalTempViews = false)
        catalog.getTablesByName(tables).foreach { table =>
          val tableType = tableTypeString(table.tableType)
          if (tableTypes == null || tableTypes.isEmpty || tableTypes.contains(tableType)) {
            addToRowSet(table.database, table.identifier.table, tableType, table.comment)
          }
        }
      }

      // Temporary views and global temporary views
      if (tableTypes == null || tableTypes.isEmpty || tableTypes.contains(VIEW.name)) {
        val globalTempViewDb = catalog.globalTempViewManager.database
        val databasePattern = Pattern.compile(CLIServiceUtils.patternToRegex(schemaName))
        val tempViews = if (databasePattern.matcher(globalTempViewDb).matches()) {
          catalog.listTables(globalTempViewDb, tablePattern, includeLocalTempViews = true)
        } else {
          catalog.listLocalTempViews(tablePattern)
        }
        tempViews.foreach { view =>
          addToRowSet(view.database.orNull, view.table, VIEW.name, None)
        }
      }
      setState(OperationState.FINISHED)
    } catch {
      case e: Throwable =>
        logError(s"Error executing get tables operation with $statementId", e)
        setState(OperationState.ERROR)
        e match {
          case hiveException: SparkThriftServerSQLException =>
            SparkThriftServer.listener.onStatementError(
              statementId, hiveException.getMessage, SparkUtils.exceptionString(hiveException))
            throw hiveException
          case _ =>
            val root = ExceptionUtils.getRootCause(e)
            SparkThriftServer.listener.onStatementError(
              statementId, root.getMessage, SparkUtils.exceptionString(root))
            throw new SparkThriftServerSQLException("Error getting tables: " + root.toString, root)
        }
    }
    SparkThriftServer.listener.onStatementFinish(statementId)
  }

  private def addToRowSet(
      dbName: String,
      tableName: String,
      tableType: String,
      comment: Option[String]): Unit = {
    val rowData = Array[AnyRef](
      "",
      dbName,
      tableName,
      tableType,
      comment.getOrElse(""))
    // Since HIVE-7575(Hive 2.0.0), adds 5 additional columns to the ResultSet of GetTables.
    if (HiveUtils.isHive23) {
      rowSet.addRow(rowData ++ Array(null, null, null, null, null))
    } else {
      rowSet.addRow(rowData)
    }
  }

  override def getResultSetSchema: TableSchema = {
    assertState(OperationState.FINISHED)
    RESULT_SET_SCHEMA
  }


  override def getNextRowSet(orientation: FetchOrientation, maxRows: Long): RowSet = {
    assertState(OperationState.FINISHED)
    validateDefaultFetchOrientation(orientation)
    if (orientation == FetchOrientation.FETCH_FIRST) {
      rowSet.setStartOffset(0)
    }
    rowSet.extractSubset(maxRows.toInt)
  }
}
