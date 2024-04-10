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

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType
import org.apache.hadoop.hive.serde2.thrift.Type
import org.apache.hadoop.hive.serde2.thrift.Type._
import org.apache.hive.service.cli.OperationState
import org.apache.hive.service.cli.operation.GetTypeInfoOperation
import org.apache.hive.service.cli.session.HiveSession

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey._
import org.apache.spark.sql.SQLContext

/**
 * Spark's own GetTypeInfoOperation
 *
 * @param sqlContext    SQLContext to use
 * @param parentSession a HiveSession from SessionManager
 */
private[hive] class SparkGetTypeInfoOperation(
    val sqlContext: SQLContext,
    parentSession: HiveSession)
  extends GetTypeInfoOperation(parentSession)
  with SparkOperation
  with Logging {

  override def runInternal(): Unit = {
    statementId = UUID.randomUUID().toString
    val logMsg = "Listing type info"
    logInfo(log"Listing type info with ${MDC(STATEMENT_ID, statementId)}")
    setState(OperationState.RUNNING)
    // Always use the latest class loader provided by executionHive's state.
    val executionHiveClassLoader = sqlContext.sharedState.jarClassLoader
    Thread.currentThread().setContextClassLoader(executionHiveClassLoader)

    if (isAuthV2Enabled) {
      authorizeMetaGets(HiveOperationType.GET_TYPEINFO, null)
    }

    HiveThriftServer2.eventManager.onStatementStart(
      statementId,
      parentSession.getSessionHandle.getSessionId.toString,
      logMsg,
      statementId,
      parentSession.getUsername)

    try {
      SparkGetTypeInfoUtil.supportedType.foreach(typeInfo => {
        val rowData = Array[AnyRef](
          typeInfo.getName, // TYPE_NAME
          typeInfo.toJavaSQLType.asInstanceOf[AnyRef], // DATA_TYPE
          typeInfo.getMaxPrecision.asInstanceOf[AnyRef], // PRECISION
          typeInfo.getLiteralPrefix, // LITERAL_PREFIX
          typeInfo.getLiteralSuffix, // LITERAL_SUFFIX
          typeInfo.getCreateParams, // CREATE_PARAMS
          typeInfo.getNullable.asInstanceOf[AnyRef], // NULLABLE
          typeInfo.isCaseSensitive.asInstanceOf[AnyRef], // CASE_SENSITIVE
          typeInfo.getSearchable.asInstanceOf[AnyRef], // SEARCHABLE
          typeInfo.isUnsignedAttribute.asInstanceOf[AnyRef], // UNSIGNED_ATTRIBUTE
          typeInfo.isFixedPrecScale.asInstanceOf[AnyRef], // FIXED_PREC_SCALE
          typeInfo.isAutoIncrement.asInstanceOf[AnyRef], // AUTO_INCREMENT
          typeInfo.getLocalizedName, // LOCAL_TYPE_NAME
          typeInfo.getMinimumScale.asInstanceOf[AnyRef], // MINIMUM_SCALE
          typeInfo.getMaximumScale.asInstanceOf[AnyRef], // MAXIMUM_SCALE
          null, // SQL_DATA_TYPE, unused
          null, // SQL_DATETIME_SUB, unused
          typeInfo.getNumPrecRadix // NUM_PREC_RADIX
        )
        rowSet.addRow(rowData)
      })
      setState(OperationState.FINISHED)
    } catch onError()

    HiveThriftServer2.eventManager.onStatementFinish(statementId)
  }
}

private[hive] object SparkGetTypeInfoUtil {
  val supportedType: Seq[Type] = {
    Seq(NULL_TYPE, BOOLEAN_TYPE, STRING_TYPE, BINARY_TYPE,
      TINYINT_TYPE, SMALLINT_TYPE, INT_TYPE, BIGINT_TYPE,
      FLOAT_TYPE, DOUBLE_TYPE, DECIMAL_TYPE,
      DATE_TYPE, TIMESTAMP_TYPE,
      ARRAY_TYPE, MAP_TYPE, STRUCT_TYPE, CHAR_TYPE, VARCHAR_TYPE,
      INTERVAL_YEAR_MONTH_TYPE, INTERVAL_DAY_TIME_TYPE)
  }
}
