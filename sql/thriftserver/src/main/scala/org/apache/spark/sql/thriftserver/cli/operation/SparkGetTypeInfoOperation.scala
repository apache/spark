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

import java.util.UUID

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.thriftserver.cli._
import org.apache.spark.sql.thriftserver.cli.session.ThriftServerSession
import org.apache.spark.sql.thriftserver.server.SparkThriftServer
import org.apache.spark.util.{Utils => SparkUtils}

/**
 * Spark's own GetTypeInfoOperation
 *
 * @param sqlContext    SQLContext to use
 * @param parentSession a HiveSession from SessionManager
 */
private[thriftserver] class SparkGetTypeInfoOperation(
    sqlContext: SQLContext,
    parentSession: ThriftServerSession)
  extends SparkMetadataOperation(parentSession, OperationType.GET_TYPE_INFO) with Logging {

  RESULT_SET_SCHEMA = new TableSchema()
    .addPrimitiveColumn("TYPE_NAME", Type.STRING_TYPE,
      "Type name")
    .addPrimitiveColumn("DATA_TYPE", Type.INT_TYPE,
      "SQL data type from java.sql.Types")
    .addPrimitiveColumn("PRECISION", Type.INT_TYPE,
      "Maximum precision")
    .addPrimitiveColumn("LITERAL_PREFIX", Type.STRING_TYPE,
      "Prefix used to quote a literal (may be null)")
    .addPrimitiveColumn("LITERAL_SUFFIX", Type.STRING_TYPE,
      "Suffix used to quote a literal (may be null)")
    .addPrimitiveColumn("CREATE_PARAMS", Type.STRING_TYPE,
      "Parameters used in creating the type (may be null)")
    .addPrimitiveColumn("NULLABLE", Type.SMALLINT_TYPE,
      "Can you use NULL for this type")
    .addPrimitiveColumn("CASE_SENSITIVE", Type.BOOLEAN_TYPE,
      "Is it case sensitive")
    .addPrimitiveColumn("SEARCHABLE", Type.SMALLINT_TYPE,
      "Can you use \"WHERE\" based on this type")
    .addPrimitiveColumn("UNSIGNED_ATTRIBUTE", Type.BOOLEAN_TYPE,
      "Is it unsigned")
    .addPrimitiveColumn("FIXED_PREC_SCALE", Type.BOOLEAN_TYPE,
      "Can it be a money value")
    .addPrimitiveColumn("AUTO_INCREMENT", Type.BOOLEAN_TYPE,
      "Can it be used for an auto-increment value")
    .addPrimitiveColumn("LOCAL_TYPE_NAME", Type.STRING_TYPE,
      "Localized version of type name (may be null)")
    .addPrimitiveColumn("MINIMUM_SCALE", Type.SMALLINT_TYPE,
      "Minimum scale supported")
    .addPrimitiveColumn("MAXIMUM_SCALE", Type.SMALLINT_TYPE,
      "Maximum scale supported")
    .addPrimitiveColumn("SQL_DATA_TYPE", Type.INT_TYPE,
      "Unused")
    .addPrimitiveColumn("SQL_DATETIME_SUB", Type.INT_TYPE,
      "Unused")
    .addPrimitiveColumn("NUM_PREC_RADIX", Type.INT_TYPE,
      "Usually 2 or 10");



  private val rowSet: RowSet = RowSetFactory.create(RESULT_SET_SCHEMA, getProtocolVersion, false)

  override def close(): Unit = {
    super.close()
    SparkThriftServer.listener.onOperationClosed(statementId)
  }

  override def runInternal(): Unit = {
    statementId = UUID.randomUUID().toString
    val logMsg = "Listing type info"
    logInfo(s"$logMsg with $statementId")
    setState(OperationState.RUNNING)
    // Always use the latest class loader provided by executionHive's state.
    val executionHiveClassLoader = sqlContext.sharedState.jarClassLoader
    Thread.currentThread().setContextClassLoader(executionHiveClassLoader)

    if (isAuthV2Enabled) {
      authorizeMetaGets(HiveOperationType.GET_TYPEINFO, null)
    }

    SparkThriftServer.listener.onStatementStart(
      statementId,
      parentSession.getSessionHandle.getSessionId.toString,
      logMsg,
      statementId,
      parentSession.getUserName)

    try {
      Type.values().foreach(typeInfo => {
        val rowData = Array[AnyRef](
          typeInfo.getName, // TYPE_NAME
          typeInfo.toJavaSQLType.asInstanceOf[AnyRef], // DATA_TYPE
          typeInfo.getMaxPrecision, // PRECISION
          typeInfo.getLiteralPrefix, // LITERAL_PREFIX
          typeInfo.getLiteralSuffix, // LITERAL_SUFFIX
          typeInfo.getCreateParams, // CREATE_PARAMS
          typeInfo.getNullable, // NULLABLE
          typeInfo.isCaseSensitive, // CASE_SENSITIVE
          typeInfo.getSearchable, // SEARCHABLE
          typeInfo.isUnsignedAttribute, // UNSIGNED_ATTRIBUTE
          typeInfo.isFixedPrecScale, // FIXED_PREC_SCALE
          typeInfo.isAutoIncrement, // AUTO_INCREMENT
          typeInfo.getLocalizedName, // LOCAL_TYPE_NAME
          typeInfo.getMinimumScale, // MINIMUM_SCALE
          typeInfo.getMaximumScale, // MAXIMUM_SCALE
          null, // SQL_DATA_TYPE, unused
          null, // SQL_DATETIME_SUB, unused
          typeInfo.getNumPrecRadix // NUM_PREC_RADIX
        )
        rowSet.addRow(rowData)
      })
      setState(OperationState.FINISHED)
    } catch {
      case e: Throwable =>
        logError(s"Error executing get type info with $statementId", e)
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
            throw new SparkThriftServerSQLException("Error getting type info: " +
              root.toString, root)
        }
    }
    SparkThriftServer.listener.onStatementFinish(statementId)
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
