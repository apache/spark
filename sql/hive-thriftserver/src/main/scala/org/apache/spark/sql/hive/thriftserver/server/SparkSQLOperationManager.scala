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

package org.apache.spark.sql.hive.thriftserver.server

import java.util.{List => JList, Map => JMap}
import java.util.concurrent.ConcurrentHashMap

import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.{ExecuteStatementOperation, GetSchemasOperation, MetadataOperation, Operation, OperationManager}
import org.apache.hive.service.cli.session.HiveSession

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.thriftserver.{ReflectionUtils, SparkExecuteStatementOperation, SparkGetSchemasOperation, SparkGetTablesOperation}
import org.apache.spark.sql.internal.SQLConf

/**
 * Executes queries using Spark SQL, and maintains a list of handles to active queries.
 */
private[thriftserver] class SparkSQLOperationManager()
  extends OperationManager with Logging {

  val handleToOperation = ReflectionUtils
    .getSuperField[JMap[OperationHandle, Operation]](this, "handleToOperation")

  val sessionToActivePool = new ConcurrentHashMap[SessionHandle, String]()
  val sessionToContexts = new ConcurrentHashMap[SessionHandle, SQLContext]()

  override def newExecuteStatementOperation(
      parentSession: HiveSession,
      statement: String,
      confOverlay: JMap[String, String],
      async: Boolean): ExecuteStatementOperation = synchronized {
    val sqlContext = sessionToContexts.get(parentSession.getSessionHandle)
    require(sqlContext != null, s"Session handle: ${parentSession.getSessionHandle} has not been" +
      s" initialized or had already closed.")
    val conf = sqlContext.sessionState.conf
    val hiveSessionState = parentSession.getSessionState
    setConfMap(conf, hiveSessionState.getOverriddenConfigurations)
    setConfMap(conf, hiveSessionState.getHiveVariables)
    val runInBackground = async && conf.getConf(HiveUtils.HIVE_THRIFT_SERVER_ASYNC)
    val operation = new SparkExecuteStatementOperation(parentSession, statement, confOverlay,
      runInBackground)(sqlContext, sessionToActivePool)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created Operation for $statement with session=$parentSession, " +
      s"runInBackground=$runInBackground")
    operation
  }

  override def newGetSchemasOperation(
      parentSession: HiveSession,
      catalogName: String,
      schemaName: String): GetSchemasOperation = synchronized {
    val sqlContext = sessionToContexts.get(parentSession.getSessionHandle)
    require(sqlContext != null, s"Session handle: ${parentSession.getSessionHandle} has not been" +
      " initialized or had already closed.")
    val operation = new SparkGetSchemasOperation(sqlContext, parentSession, catalogName, schemaName)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created GetSchemasOperation with session=$parentSession.")
    operation
  }

  override def newGetTablesOperation(
      parentSession: HiveSession,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: JList[String]): MetadataOperation = synchronized {
    val sqlContext = sessionToContexts.get(parentSession.getSessionHandle)
    require(sqlContext != null, s"Session handle: ${parentSession.getSessionHandle} has not been" +
      " initialized or had already closed.")
    val operation = new SparkGetTablesOperation(sqlContext, parentSession,
      catalogName, schemaName, tableName, tableTypes)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created GetTablesOperation with session=$parentSession.")
    operation
  }

  def setConfMap(conf: SQLConf, confMap: java.util.Map[String, String]): Unit = {
    val iterator = confMap.entrySet().iterator()
    while (iterator.hasNext) {
      val kv = iterator.next()
      conf.setConfString(kv.getKey, kv.getValue)
    }
  }
}
