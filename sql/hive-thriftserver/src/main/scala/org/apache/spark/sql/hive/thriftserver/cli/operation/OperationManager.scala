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

import java.sql.SQLException
import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.hive.ql.session.OperationLog
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.thriftserver.cli._
import org.apache.spark.sql.hive.thriftserver.service.AbstractService
import org.apache.spark.sql.types.StructType
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

private[hive] class OperationManager private(name: String)
  extends AbstractService(name) with Logging {


  def this() = this(classOf[OperationManager].getSimpleName)

  private[this] lazy val logSchema: StructType = new StructType().add("operation_log", "string")
  private[this] val handleToOperation = new ConcurrentHashMap[OperationHandle, Operation]
  private[this] val userToOperationLog = new ConcurrentHashMap[String, OperationLog]()

  override def init(conf: SparkConf): Unit = synchronized {
    if (conf.get(LOGGING_OPERATION_ENABLED.key).toBoolean) {
      initOperationLogCapture(conf.get(LOGGING_OPERATION_LEVEL.key))
    } else {
      debug("Operation level logging is turned off")
    }
    super.init(conf)
  }

  private[this] def initOperationLogCapture(loggingMode: String): Unit = {
    // Register another Appender (with the same layout) that talks to us.
    val ap = new LogDivertAppender(this, OperationLog.getLoggingLevel(loggingMode))
    Logger.getRootLogger.addAppender(ap)
  }

  def getOperationLogByThread: OperationLog = OperationLog.getCurrentOperationLog

  private[this] def getOperationLogByName: OperationLog = {
    if (!userToOperationLog.isEmpty) {
      userToOperationLog.get(KyuubiSparkUtil.getCurrentUserName)
    } else {
      null
    }
  }

  def getOperationLog: OperationLog = {
    Option(getOperationLogByThread).getOrElse(getOperationLogByName)
  }

  def setOperationLog(user: String, log: OperationLog): Unit = {
    OperationLog.setCurrentOperationLog(log)
    userToOperationLog.put(Option(user).getOrElse(KyuubiSparkUtil.getCurrentUserName), log)
  }

  def unregisterOperationLog(user: String): Unit = {
    OperationLog.removeCurrentOperationLog()
    userToOperationLog.remove(user)
  }

  def newExecuteStatementOperation(parentSession: KyuubiSession,
                                   statement: String,
                                   runAsync: Boolean): SparkExecuteStatementOperation = synchronized {
    val operation = new KyuubiExecuteStatementOperation(parentSession, statement, runAsync)
    addOperation(operation)
    operation
  }

  def newGetCatalogsOperation(session: KyuubiSession): SparkGetCatalogsOperation = {
    val operation = new SparkGetCatalogsOperation(session)
    addOperation(operation)
    operation
  }


  def newGetColumnsOperation(session: KyuubiSession,
                             catalogName: String,
                             schemaName: String,
                             tableName: String,
                             columnName: String): KyuubiGetColumnsOperation = {
    val operation = new SparkGetColumnsOperation(session, catalogName, schemaName, tableName, columnName)
    addOperation(operation)
    operation
  }

  def newGetSchemasOperation(session: KyuubiSession, catalogName: String, schemaName: String): KyuubiGetSchemasOperation = {
    val operation = new SparkGetSchemasOperation(session, catalogName, schemaName)
    addOperation(operation)
    operation
  }

  def newGetTablesOperation(session: KyuubiSession,
                            catalogName: String,
                            schemaName: String,
                            tableName: String,
                            tableTypes: Seq[String]): SparkGetTablesOperation = {
    val operation = new SparkGetTablesOperation(session, catalogName, schemaName, tableName, tableTypes)
    addOperation(operation)
    operation
  }

  def newGetTableTypesOperation(session: KyuubiSession): SparkGetTableTypesOperation = {
    val operation = new SparkGetTableTypesOperation(session)
    addOperation(operation)
    operation
  }


  def newGetFunctionsOperation(session: KyuubiSession,
                               catalogName: String,
                               schemaName: String,
                               functionName: String): KyuubiGetFunctionsOperation = {
    val operation = new SparkGetFunctionsOperation(session, catalogName, schemaName, functionName)
    addOperation(operation)
    operation
  }


  def newGetTypeInfoOperation(session: KyuubiSession): KyuubiGetTypeInfoOperation = {
    val operation = new SparkGetTypeInfoOperation(session)
    addOperation(operation)
    operation
  }

  def getOperation(operationHandle: OperationHandle): Operation = {
    val operation = getOperationInternal(operationHandle)
    if (operation == null) {
      throw new SparkThriftServerSQLException("Invalid OperationHandle " + operationHandle)
    }
    operation
  }

  private[this] def getOperationInternal(operationHandle: OperationHandle) =
    handleToOperation.get(operationHandle)

  private[this] def addOperation(operation: Operation): Unit = {
    handleToOperation.put(operation.getHandle, operation)
  }

  private[this] def removeOperation(opHandle: OperationHandle) =
    handleToOperation.remove(opHandle)

  private def removeTimedOutOperation(operationHandle: OperationHandle): Option[Operation] = synchronized {
    Some(handleToOperation.get(operationHandle))
      .filter(_.isTimedOut(System.currentTimeMillis()))
      .map(_ => handleToOperation.remove(operationHandle))
  }

  def cancelOperation(opHandle: OperationHandle): Unit = {
    val operation = getOperation(opHandle)
    val opState = operation.getStatus.getState
    if ((opState eq CANCELED)
      || (opState eq CLOSED)
      || (opState eq FINISHED)
      || (opState eq ERROR)
      || (opState eq UNKNOWN)) {
      // Cancel should be a no-op in either cases
      logInfo(opHandle + ": Operation is already aborted in state - " + opState)
    }
    else {
      logInfo(opHandle + ": Attempting to cancel from state - " + opState)
      operation.cancel()
    }
  }

  @throws[SparkThriftServerSQLException]
  def closeOperation(opHandle: OperationHandle): Unit = {
    val operation = removeOperation(opHandle)
    if (operation == null)
      throw new SparkThriftServerSQLException("Operation does not exist!")
    operation.close()
  }

  @throws[SparkThriftServerSQLException]
  def getOperationNextRowSet(opHandle: OperationHandle,
                             orientation: FetchOrientation,
                             maxRows: Long): RowSet =
    getOperation(opHandle).getNextRowSet(orientation, maxRows)

  @throws[SparkThriftServerSQLException]
  def getOperationLogRowSet(opHandle: OperationHandle,
                            orientation: FetchOrientation,
                            maxRows: Long): RowSet = {
    // get the OperationLog object from the operation
    val opLog: OperationLog = getOperation(opHandle).getOperationLog
    if (opLog == null) {
      throw new SparkThriftServerSQLException(
        "Couldn't find log associated with operation handle: " + opHandle)
    }
    try {
      // convert logs to RowBasedSet
      val logs = opLog.readOperationLog(isFetchFirst(orientation), maxRows).asScala.map(Row(_))
      RowSetFactory.create(logSchema, logs, getOperation(opHandle).getProtocolVersion)
    } catch {
      case e: SQLException =>
        throw new SparkThriftServerSQLException(e.getMessage, e.getCause)
    }
  }

  def getResultSetSchema(opHandle: OperationHandle): StructType = {
    getOperation(opHandle).getResultSetSchema
  }

  private[this] def isFetchFirst(fetchOrientation: FetchOrientation): Boolean = {
    fetchOrientation == FetchOrientation.FETCH_FIRST
  }

  def removeExpiredOperations(handles: Seq[OperationHandle]): Seq[Operation] = {
    handles.flatMap(removeTimedOutOperation).map { op =>
      logWarning("Operation " + op.getHandle + " is timed-out and will be closed")
      op
    }
  }

  def isHavingOperationStillRun(): Boolean = {
    handleToOperation.size() == 0 || handleToOperation.isEmpty
  }
}
