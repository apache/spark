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

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType
import org.apache.hive.service.cli.{HiveSQLException, OperationState}
import org.apache.hive.service.cli.operation.GetTypeInfoOperation
import org.apache.hive.service.cli.session.HiveSession

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.util.{Utils => SparkUtils}

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
    logInfo(s"$logMsg with $statementId")
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
      ThriftserverShimUtils.supportedType().foreach(typeInfo => {
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
    } catch {
      case e: Throwable =>
        logError(s"Error executing get type info with $statementId", e)
        setState(OperationState.ERROR)
        e match {
          case hiveException: HiveSQLException =>
            HiveThriftServer2.eventManager.onStatementError(
              statementId, hiveException.getMessage, SparkUtils.exceptionString(hiveException))
            throw hiveException
          case _ =>
            val root = ExceptionUtils.getRootCause(e)
            HiveThriftServer2.eventManager.onStatementError(
              statementId, root.getMessage, SparkUtils.exceptionString(root))
            throw new HiveSQLException("Error getting type info: " + root.toString, root)
        }
    }
    HiveThriftServer2.eventManager.onStatementFinish(statementId)
  }
}
