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

import java.security.PrivilegedExceptionAction
import java.sql.{Date, Timestamp}
import java.util.concurrent.RejectedExecutionException
import java.util.{Map => JMap, UUID}

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, Map => SMap}
import scala.util.control.NonFatal

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hive.service.cli._
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.shims.Utils
import org.apache.hive.service.cli.operation.ExecuteStatementOperation
import org.apache.hive.service.cli.session.HiveSession

import org.apache.spark.Logging
import org.apache.spark.sql.execution.SetCommand
import org.apache.spark.sql.hive.{HiveContext, HiveMetastoreTypes}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row => SparkRow, SQLConf}


private[hive] class SparkExecuteStatementOperation(
    parentSession: HiveSession,
    statement: String,
    confOverlay: JMap[String, String],
    runInBackground: Boolean = true)
    (hiveContext: HiveContext, sessionToActivePool: SMap[SessionHandle, String])
  extends ExecuteStatementOperation(parentSession, statement, confOverlay, runInBackground)
  with Logging {

  private var result: DataFrame = _
  private var iter: Iterator[SparkRow] = _
  private var dataTypes: Array[DataType] = _
  private var statementId: String = _

  def close(): Unit = {
    // RDDs will be cleaned automatically upon garbage collection.
    hiveContext.sparkContext.clearJobGroup()
    logDebug(s"CLOSING $statementId")
    cleanup(OperationState.CLOSED)
  }

  def addNonNullColumnValue(from: SparkRow, to: ArrayBuffer[Any], ordinal: Int) {
    dataTypes(ordinal) match {
      case StringType =>
        to += from.getString(ordinal)
      case IntegerType =>
        to += from.getInt(ordinal)
      case BooleanType =>
        to += from.getBoolean(ordinal)
      case DoubleType =>
        to += from.getDouble(ordinal)
      case FloatType =>
        to += from.getFloat(ordinal)
      case DecimalType() =>
        to += from.getDecimal(ordinal)
      case LongType =>
        to += from.getLong(ordinal)
      case ByteType =>
        to += from.getByte(ordinal)
      case ShortType =>
        to += from.getShort(ordinal)
      case DateType =>
        to += from.getAs[Date](ordinal)
      case TimestampType =>
        to +=  from.getAs[Timestamp](ordinal)
      case BinaryType | _: ArrayType | _: StructType | _: MapType =>
        val hiveString = HiveContext.toHiveString((from.get(ordinal), dataTypes(ordinal)))
        to += hiveString
    }
  }

  def getNextRowSet(order: FetchOrientation, maxRowsL: Long): RowSet = {
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)
    val resultRowSet: RowSet = RowSetFactory.create(getResultSetSchema, getProtocolVersion)
    if (!iter.hasNext) {
      resultRowSet
    } else {
      // maxRowsL here typically maps to java.sql.Statement.getFetchSize, which is an int
      val maxRows = maxRowsL.toInt
      var curRow = 0
      while (curRow < maxRows && iter.hasNext) {
        val sparkRow = iter.next()
        val row = ArrayBuffer[Any]()
        var curCol = 0
        while (curCol < sparkRow.length) {
          if (sparkRow.isNullAt(curCol)) {
            row += null
          } else {
            addNonNullColumnValue(sparkRow, row, curCol)
          }
          curCol += 1
        }
        resultRowSet.addRow(row.toArray.asInstanceOf[Array[Object]])
        curRow += 1
      }
      resultRowSet
    }
  }

  def getResultSetSchema: TableSchema = {
    if (result == null || result.queryExecution.analyzed.output.size == 0) {
      new TableSchema(new FieldSchema("Result", "string", "") :: Nil)
    } else {
      logInfo(s"Result Schema: ${result.queryExecution.analyzed.output}")
      val schema = result.queryExecution.analyzed.output.map { attr =>
        new FieldSchema(attr.name, HiveMetastoreTypes.toMetastoreType(attr.dataType), "")
      }
      new TableSchema(schema)
    }
  }

  override def run(): Unit = {
    setState(OperationState.PENDING)
    setHasResultSet(true) // avoid no resultset for async run

    if (!runInBackground) {
      runInternal()
    } else {
      val parentSessionState = SessionState.get()
      val hiveConf = getConfigForOperation()
      val sparkServiceUGI = Utils.getUGI()
      val sessionHive = getCurrentHive()
      val currentSqlSession = hiveContext.currentSession

      // Runnable impl to call runInternal asynchronously,
      // from a different thread
      val backgroundOperation = new Runnable() {

        override def run(): Unit = {
          val doAsAction = new PrivilegedExceptionAction[Object]() {
            override def run(): Object = {

              // User information is part of the metastore client member in Hive
              hiveContext.setSession(currentSqlSession)
              // Always use the latest class loader provided by executionHive's state.
              val executionHiveClassLoader =
                hiveContext.executionHive.state.getConf.getClassLoader
              sessionHive.getConf.setClassLoader(executionHiveClassLoader)
              parentSessionState.getConf.setClassLoader(executionHiveClassLoader)

              Hive.set(sessionHive)
              SessionState.setCurrentSessionState(parentSessionState)
              try {
                runInternal()
              } catch {
                case e: HiveSQLException =>
                  setOperationException(e)
                  log.error("Error running hive query: ", e)
              }
              return null
            }
          }

          try {
            sparkServiceUGI.doAs(doAsAction)
          } catch {
            case e: Exception =>
              setOperationException(new HiveSQLException(e))
              logError("Error running hive query as user : " +
                sparkServiceUGI.getShortUserName(), e)
          }
        }
      }
      try {
        // This submit blocks if no background threads are available to run this operation
        val backgroundHandle =
          getParentSession().getSessionManager().submitBackgroundOperation(backgroundOperation)
        setBackgroundHandle(backgroundHandle)
      } catch {
        case rejected: RejectedExecutionException =>
          setState(OperationState.ERROR)
          throw new HiveSQLException("The background threadpool cannot accept" +
            " new task for execution, please retry the operation", rejected)
        case NonFatal(e) =>
          logError(s"Error executing query in background", e)
          setState(OperationState.ERROR)
          throw e
      }
    }
  }

  override def runInternal(): Unit = {
    statementId = UUID.randomUUID().toString
    logInfo(s"Running query '$statement' with $statementId")
    setState(OperationState.RUNNING)
    HiveThriftServer2.listener.onStatementStart(
      statementId,
      parentSession.getSessionHandle.getSessionId.toString,
      statement,
      statementId,
      parentSession.getUsername)
    hiveContext.sparkContext.setJobGroup(statementId, statement)
    sessionToActivePool.get(parentSession.getSessionHandle).foreach { pool =>
      hiveContext.sparkContext.setLocalProperty("spark.scheduler.pool", pool)
    }
    try {
      result = hiveContext.sql(statement)
      logDebug(result.queryExecution.toString())
      result.queryExecution.logical match {
        case SetCommand(Some((SQLConf.THRIFTSERVER_POOL.key, Some(value)))) =>
          sessionToActivePool(parentSession.getSessionHandle) = value
          logInfo(s"Setting spark.scheduler.pool=$value for future statements in this session.")
        case _ =>
      }
      HiveThriftServer2.listener.onStatementParsed(statementId, result.queryExecution.toString())
      iter = {
        val useIncrementalCollect =
          hiveContext.getConf("spark.sql.thriftServer.incrementalCollect", "false").toBoolean
        if (useIncrementalCollect) {
          result.rdd.toLocalIterator
        } else {
          result.collect().iterator
        }
      }
      dataTypes = result.queryExecution.analyzed.output.map(_.dataType).toArray
    } catch {
      case e: HiveSQLException =>
        if (getStatus().getState() == OperationState.CANCELED) {
          return
        } else {
          setState(OperationState.ERROR);
          throw e
        }
      // Actually do need to catch Throwable as some failures don't inherit from Exception and
      // HiveServer will silently swallow them.
      case e: Throwable =>
        val currentState = getStatus().getState()
        logError(s"Error executing query, currentState $currentState, ", e)
        setState(OperationState.ERROR)
        HiveThriftServer2.listener.onStatementError(
          statementId, e.getMessage, e.getStackTraceString)
        throw new HiveSQLException(e.toString)
    }
    setState(OperationState.FINISHED)
    HiveThriftServer2.listener.onStatementFinish(statementId)
  }

  override def cancel(): Unit = {
    logInfo(s"Cancel '$statement' with $statementId")
    if (statementId != null) {
      hiveContext.sparkContext.cancelJobGroup(statementId)
    }
    cleanup(OperationState.CANCELED)
  }

  private def cleanup(state: OperationState) {
    setState(state)
    if (runInBackground) {
      val backgroundHandle = getBackgroundHandle()
      if (backgroundHandle != null) {
        backgroundHandle.cancel(true)
      }
    }
  }

  /**
   * If there are query specific settings to overlay, then create a copy of config
   * There are two cases we need to clone the session config that's being passed to hive driver
   * 1. Async query -
   *    If the client changes a config setting, that shouldn't reflect in the execution
   *    already underway
   * 2. confOverlay -
   *    The query specific settings should only be applied to the query config and not session
   * @return new configuration
   * @throws HiveSQLException
   */
  private def getConfigForOperation(): HiveConf = {
    var sqlOperationConf = getParentSession().getHiveConf()
    if (!getConfOverlay().isEmpty() || runInBackground) {
      // clone the partent session config for this query
      sqlOperationConf = new HiveConf(sqlOperationConf)

      // apply overlay query specific settings, if any
      getConfOverlay().foreach { case (k, v) =>
        try {
          sqlOperationConf.verifyAndSet(k, v)
        } catch {
          case e: IllegalArgumentException =>
            throw new HiveSQLException("Error applying statement specific settings", e)
        }
      }
    }
    return sqlOperationConf
  }

  private def getCurrentHive(): Hive = {
    try {
      return Hive.get()
    } catch {
      case e: HiveException =>
        throw new HiveSQLException("Failed to get current Hive object", e);
    }
  }
}
