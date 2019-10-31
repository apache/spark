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
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.thriftserver.SparkThriftServer2
import org.apache.spark.sql.thriftserver.cli._
import org.apache.spark.sql.thriftserver.cli.session.ThriftServerSession
import org.apache.spark.sql.types._
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
  extends SparkMetadataOperation(parentSession, GET_TYPE_INFO) with Logging {

  RESULT_SET_SCHEMA = new StructType()
    .add(StructField("TYPE_NAME", StringType))
    .add(StructField("DATA_TYPE", IntegerType))
    .add(StructField("PRECISION", IntegerType))
    .add(StructField("LITERAL_PREFIX", StringType))
    .add(StructField("LITERAL_SUFFIX", StringType))
    .add(StructField("CREATE_PARAMS", StringType))
    .add(StructField("NULLABLE", ShortType))
    .add(StructField("CASE_SENSITIVE", BooleanType))
    .add(StructField("SEARCHABLE", ShortType))
    .add(StructField("UNSIGNED_ATTRIBUTE", BooleanType))
    .add(StructField("FIXED_PREC_SCALE", BooleanType))
    .add(StructField("AUTO_INCREMENT", BooleanType))
    .add(StructField("LOCAL_TYPE_NAME", StringType))
    .add(StructField("MINIMUM_SCALE", ShortType))
    .add(StructField("MAXIMUM_SCALE", ShortType))
    .add(StructField("SQL_DATA_TYPE", IntegerType))
    .add(StructField("SQL_DATETIME_SUB", IntegerType))
    .add(StructField("NUM_PREC_RADIX", IntegerType))


  private val rowSet: RowSet = RowSetFactory.create(RESULT_SET_SCHEMA, getProtocolVersion)

  override def close(): Unit = {
    super.close()
    SparkThriftServer2.listener.onOperationClosed(statementId)
  }

  override def runInternal(): Unit = {
    setStatementId(UUID.randomUUID().toString)
    val logMsg = "Listing type info"
    logInfo(s"$logMsg with $statementId")
    setState(RUNNING)
    // Always use the latest class loader provided by executionHive's state.
    val executionHiveClassLoader = sqlContext.sharedState.jarClassLoader
    Thread.currentThread().setContextClassLoader(executionHiveClassLoader)

    if (isAuthV2Enabled) {
      authorizeMetaGets(HiveOperationType.GET_TYPEINFO, null)
    }

    SparkThriftServer2.listener.onStatementStart(
      statementId,
      parentSession.getSessionHandle.getSessionId.toString,
      logMsg,
      statementId,
      parentSession.getUsername)

    try {
      Type.values().foreach(typeInfo => {
        val rowData = Row(
          typeInfo.getName, // TYPE_NAME
          typeInfo.toJavaSQLType, // DATA_TYPE
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
      setState(FINISHED)
    } catch {
      case e: Throwable =>
        logError(s"Error executing get type info with $statementId", e)
        setState(ERROR)
        e match {
          case hiveException: SparkThriftServerSQLException =>
            SparkThriftServer2.listener.onStatementError(
              statementId, hiveException.getMessage, SparkUtils.exceptionString(hiveException))
            throw hiveException
          case _ =>
            val root = ExceptionUtils.getRootCause(e)
            SparkThriftServer2.listener.onStatementError(
              statementId, root.getMessage, SparkUtils.exceptionString(root))
            throw new SparkThriftServerSQLException("Error getting type info: " +
              root.toString, root)
        }
    }
    SparkThriftServer2.listener.onStatementFinish(statementId)
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
