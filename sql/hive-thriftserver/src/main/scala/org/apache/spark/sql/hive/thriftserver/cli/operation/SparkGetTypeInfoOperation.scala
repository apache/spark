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

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SQLContext}
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
private[hive] class SparkGetTypeInfoOperation(sqlContext: SQLContext,
                                              parentSession: ThriftSession)
  extends SparkMetadataOperation(parentSession, GET_TYPE_INFO)
  with SparkMetadataOperationUtils with Logging {

  private var statementId: String = _
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
    HiveThriftServer2.listener.onOperationClosed(statementId)
  }

  override def runInternal(): Unit = {
    statementId = UUID.randomUUID().toString
    val logMsg = "Listing types"
    logInfo(s"$logMsg with $statementId")
    setState(RUNNING)
    // Always use the latest class loader provided by executionHive's state.
    val executionHiveClassLoader = sqlContext.sharedState.jarClassLoader
    Thread.currentThread().setContextClassLoader(executionHiveClassLoader)

    if (isAuthV2Enabled) {
      authorizeMetaGets(HiveOperationType.GET_TYPEINFO, null)
    }

    HiveThriftServer2.listener.onStatementStart(
      statementId,
      parentSession.getSessionHandle.getSessionId.toString,
      logMsg,
      statementId,
      parentSession.getUsername)

    try {
      Type.values.foreach(typeInfo => {
        val rowData = Row(
          typeInfo.getName, // TYPE_NAME
          typeInfo.toJavaSQLType, // DATA_TYPE
          typeInfo.getMaxPrecision.getOrElse(null), // PRECISION
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
