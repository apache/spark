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
import org.apache.hive.service.cli.operation._
import org.apache.hive.service.cli.session.HiveSession

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.thriftserver._

/**
 * Executes queries using Spark SQL, and maintains a list of handles to active queries.
 */
private[thriftserver] class SparkSQLOperationManager()
  extends OperationManager with Logging {

  val handleToOperation = ReflectionUtils
    .getSuperField[JMap[OperationHandle, Operation]](this, "handleToOperation")

  val sessionToContexts = new ConcurrentHashMap[SessionHandle, SparkSession]()

  override def newExecuteStatementOperation(
      parentSession: HiveSession,
      statement: String,
      confOverlay: JMap[String, String],
      async: Boolean,
      queryTimeout: Long): ExecuteStatementOperation = synchronized {
    val sparkSession = sessionToContexts.get(parentSession.getSessionHandle)
    require(sparkSession != null, s"Session handle: ${parentSession.getSessionHandle} " +
      s"has not been initialized or had already closed.")
    val conf = sparkSession.sessionState.conf
    val runInBackground = async && conf.getConf(HiveUtils.HIVE_THRIFT_SERVER_ASYNC)
    val operation = new SparkExecuteStatementOperation(
      sparkSession, parentSession, statement, confOverlay, runInBackground, queryTimeout)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created Operation for $statement with session=$parentSession, " +
      s"runInBackground=$runInBackground")
    operation
  }

  override def newGetCatalogsOperation(
      parentSession: HiveSession): GetCatalogsOperation = synchronized {
    val session = sessionToContexts.get(parentSession.getSessionHandle)
    require(session != null, s"Session handle: ${parentSession.getSessionHandle} has not been" +
      " initialized or had already closed.")
    val operation = new SparkGetCatalogsOperation(session, parentSession)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created GetCatalogsOperation with session=$parentSession.")
    operation
  }

  override def newGetSchemasOperation(
      parentSession: HiveSession,
      catalogName: String,
      schemaName: String): GetSchemasOperation = synchronized {
    val session = sessionToContexts.get(parentSession.getSessionHandle)
    require(session != null, s"Session handle: ${parentSession.getSessionHandle} has not been" +
      " initialized or had already closed.")
    val operation = new SparkGetSchemasOperation(session, parentSession, catalogName, schemaName)
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
    val session = sessionToContexts.get(parentSession.getSessionHandle)
    require(session != null, s"Session handle: ${parentSession.getSessionHandle} has not been" +
      " initialized or had already closed.")
    val operation = new SparkGetTablesOperation(session, parentSession,
      catalogName, schemaName, tableName, tableTypes)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created GetTablesOperation with session=$parentSession.")
    operation
  }

  override def newGetColumnsOperation(
      parentSession: HiveSession,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): GetColumnsOperation = synchronized {
    val session = sessionToContexts.get(parentSession.getSessionHandle)
    require(session != null, s"Session handle: ${parentSession.getSessionHandle} has not been" +
      " initialized or had already closed.")
    val operation = new SparkGetColumnsOperation(session, parentSession,
      catalogName, schemaName, tableName, columnName)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created GetColumnsOperation with session=$parentSession.")
    operation
  }

  override def newGetTableTypesOperation(
      parentSession: HiveSession): GetTableTypesOperation = synchronized {
    val session = sessionToContexts.get(parentSession.getSessionHandle)
    require(session != null, s"Session handle: ${parentSession.getSessionHandle} has not been" +
      " initialized or had already closed.")
    val operation = new SparkGetTableTypesOperation(session, parentSession)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created GetTableTypesOperation with session=$parentSession.")
    operation
  }

  override def newGetFunctionsOperation(
      parentSession: HiveSession,
      catalogName: String,
      schemaName: String,
      functionName: String): GetFunctionsOperation = synchronized {
    val session = sessionToContexts.get(parentSession.getSessionHandle)
    require(session != null, s"Session handle: ${parentSession.getSessionHandle} has not been" +
      " initialized or had already closed.")
    val operation = new SparkGetFunctionsOperation(session, parentSession,
      catalogName, schemaName, functionName)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created GetFunctionsOperation with session=$parentSession.")
    operation
  }

  override def newGetTypeInfoOperation(
       parentSession: HiveSession): GetTypeInfoOperation = synchronized {
    val session = sessionToContexts.get(parentSession.getSessionHandle)
    require(session != null, s"Session handle: ${parentSession.getSessionHandle} has not been" +
      " initialized or had already closed.")
    val operation = new SparkGetTypeInfoOperation(session, parentSession)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created GetTypeInfoOperation with session=$parentSession.")
    operation
  }
}
