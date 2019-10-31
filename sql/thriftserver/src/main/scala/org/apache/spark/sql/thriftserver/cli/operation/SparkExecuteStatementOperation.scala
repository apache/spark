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

import java.security.PrivilegedExceptionAction
import java.util.{Map => JMap, UUID}
import java.util.concurrent.RejectedExecutionException

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.hive.ql.session.OperationLog
import org.apache.hadoop.hive.shims.Utils

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row => SparkRow, SQLContext}
import org.apache.spark.sql.execution.command.SetCommand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.thriftserver.SparkThriftServer2
import org.apache.spark.sql.thriftserver.cli._
import org.apache.spark.sql.thriftserver.cli.session.ThriftServerSession
import org.apache.spark.sql.types._
import org.apache.spark.util.{Utils => SparkUtils}

private[thriftserver] class SparkExecuteStatementOperation(
    parentSession: ThriftServerSession,
    statement: String,
    confOverlay: JMap[String, String],
    runInBackground: Boolean = true)(sqlContext: SQLContext,
                                     sessionToActivePool: JMap[SessionHandle, String])
  extends Operation(parentSession, OperationType.EXECUTE_STATEMENT, runInBackground)
  with Logging {

  private var result: DataFrame = _

  // We cache the returned rows to get iterators again in case the user wants to use FETCH_FIRST.
  // This is only used when `spark.sql.thriftServer.incrementalCollect` is set to `false`.
  // In case of `true`, this will be `None` and FETCH_FIRST will trigger re-execution.
  private var resultList: Option[Array[SparkRow]] = _
  private var previousFetchEndOffset: Long = 0
  private var previousFetchStartOffset: Long = 0
  private var iter: Iterator[SparkRow] = _
  private var dataTypes: Array[DataType] = _

  def registerCurrentOperationLog(): Unit = {
    if (_isOperationLogEnabled) {
      if (_operationLog == null) {
        logWarning("Failed to get current OperationLog object of Operation: "
          + getHandle.getHandleIdentifier)
        _isOperationLogEnabled = false
      } else {
        OperationLog.setCurrentOperationLog(_operationLog)
      }
    }
  }

  override def close(): Unit = {
    // RDDs will be cleaned automatically upon garbage collection.
    logInfo(s"Close statement with $statementId")
    cleanup(OperationState.CLOSED)
    SparkThriftServer2.listener.onOperationClosed(statementId)
  }

  def getNextRowSet(order: FetchOrientation, maxRowsL: Long): RowSet = withSchedulerPool {
    logInfo(s"Received getNextRowSet request order=${order} and maxRowsL=${maxRowsL} " +
      s"with ${statementId}")
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)
    val resultRowSet: RowSet =
      RowSetFactory.create(getResultSetSchema, getProtocolVersion)

    // Reset iter when FETCH_FIRST or FETCH_PRIOR
    if ((order.equals(FetchOrientation.FETCH_FIRST) ||
      order.equals(FetchOrientation.FETCH_PRIOR)) && previousFetchEndOffset != 0) {
      // Reset the iterator to the beginning of the query.
      iter = if (sqlContext.getConf(SQLConf.THRIFTSERVER_INCREMENTAL_COLLECT.key).toBoolean) {
        resultList = None
        result.toLocalIterator.asScala
      } else {
        if (resultList.isEmpty) {
          resultList = Some(result.collect())
        }
        resultList.get.iterator
      }
    }

    var resultOffset = {
      if (order.equals(FetchOrientation.FETCH_FIRST)) {
        logInfo(s"FETCH_FIRST request with $statementId. Resetting to resultOffset=0")
        0
      } else if (order.equals(FetchOrientation.FETCH_PRIOR)) {
        // TODO: FETCH_PRIOR should be handled more efficiently than rewinding to beginning and
        // reiterating.
        val targetOffset = math.max(previousFetchStartOffset - maxRowsL, 0)
        logInfo(s"FETCH_PRIOR request with $statementId. Resetting to resultOffset=$targetOffset")
        var off = 0
        while (off < targetOffset && iter.hasNext) {
          iter.next()
          off += 1
        }
        off
      } else { // FETCH_NEXT
        previousFetchEndOffset
      }
    }

    resultRowSet.setStartOffset(resultOffset)
    previousFetchStartOffset = resultOffset
    if (!iter.hasNext) {
      resultRowSet
    } else {
      // maxRowsL here typically maps to java.sql.Statement.getFetchSize, which is an int
      val maxRows = maxRowsL.toInt
      var curRow = 0
      while (curRow < maxRows && iter.hasNext) {
        val sparkRow = iter.next()
        resultRowSet.addRow(sparkRow)
        curRow += 1
        resultOffset += 1
      }
      previousFetchEndOffset = resultOffset
      logInfo(s"Returning result set with ${curRow} rows from offsets " +
        s"[$previousFetchStartOffset, $previousFetchEndOffset) with $statementId")
      resultRowSet
    }
  }

  def getResultSetSchema: StructType = {
    assertState(OperationState.FINISHED)
    if (result == null || result.schema.isEmpty) {
      new StructType().add("Result", "string")
    } else {
      result.schema
    }
  }

  override def runInternal(): Unit = {
    setState(OperationState.PENDING)
    setStatementId(UUID.randomUUID().toString)
    logInfo(s"Submitting query '$statement' with $statementId")
    SparkThriftServer2.listener.onStatementStart(
      statementId,
      parentSession.getSessionHandle.getSessionId.toString,
      statement,
      statementId,
      parentSession.getUsername)
    setHasResultSet(true) // avoid no resultset for async run

    if (!runInBackground) {
      execute()
    } else {
      val sparkServiceUGI = Utils.getUGI()

      // Runnable impl to call runInternal asynchronously,
      // from a different thread
      val backgroundOperation = new Runnable() {

        override def run(): Unit = {
          val doAsAction = new PrivilegedExceptionAction[Unit]() {
            override def run(): Unit = {
              registerCurrentOperationLog()
              try {
                execute()
              } catch {
                case e: SparkThriftServerSQLException =>
                  setOperationException(e)
                  logError("Error running hive query: ", e)
              }
            }
          }

          try {
            sparkServiceUGI.doAs(doAsAction)
          } catch {
            case e: Exception =>
              setOperationException(new SparkThriftServerSQLException(e))
              logError("Error running hive query as user : " +
                sparkServiceUGI.getShortUserName(), e)
          }
        }
      }
      try {
        // This submit blocks if no background threads are available to run this operation
        val backgroundHandle =
          parentSession.getSessionManager.submitBackgroundOperation(backgroundOperation)
        setBackgroundHandle(backgroundHandle)
      } catch {
        case rejected: RejectedExecutionException =>
          logError("Error submitting query in background, query rejected", rejected)
          setState(OperationState.ERROR)
          SparkThriftServer2.listener.onStatementError(
            statementId, rejected.getMessage, SparkUtils.exceptionString(rejected))
          throw new SparkThriftServerSQLException("The background threadpool cannot accept" +
            " new task for execution, please retry the operation", rejected)
        case NonFatal(e) =>
          logError(s"Error executing query in background", e)
          setState(OperationState.ERROR)
          SparkThriftServer2.listener.onStatementError(
            statementId, e.getMessage, SparkUtils.exceptionString(e))
          throw new SparkThriftServerSQLException(e)
      }
    }
  }

  private def execute(): Unit = withSchedulerPool {
    try {
      synchronized {
        if (getStatus.getState.isTerminal) {
          logInfo(s"Query with $statementId in terminal state before it started running")
          return
        } else {
          logInfo(s"Running query with $statementId")
          setState(OperationState.RUNNING)
        }
      }
      // Always use the latest class loader provided by executionHive's state.
      val executionHiveClassLoader = sqlContext.sharedState.jarClassLoader
      Thread.currentThread().setContextClassLoader(executionHiveClassLoader)

      sqlContext.sparkContext.setJobGroup(statementId, statement)
      result = sqlContext.sql(statement)
      logDebug(result.queryExecution.toString())
      result.queryExecution.logical match {
        case SetCommand(Some((SQLConf.THRIFTSERVER_POOL.key, Some(value)))) =>
          sessionToActivePool.put(parentSession.getSessionHandle, value)
          logInfo(s"Setting ${SparkContext.SPARK_SCHEDULER_POOL}=$value for future statements " +
            "in this session.")
        case _ =>
      }
      SparkThriftServer2.listener.onStatementParsed(statementId, result.queryExecution.toString())
      iter = {
        if (sqlContext.getConf(SQLConf.THRIFTSERVER_INCREMENTAL_COLLECT.key).toBoolean) {
          resultList = None
          result.toLocalIterator.asScala
        } else {
          resultList = Some(result.collect())
          resultList.get.iterator
        }
      }
      dataTypes = result.queryExecution.analyzed.output.map(_.dataType).toArray
    } catch {
      // Actually do need to catch Throwable as some failures don't inherit from Exception and
      // HiveServer will silently swallow them.
      case e: Throwable =>
        // When cancel() or close() is called very quickly after the query is started,
        // then they may both call cleanup() before Spark Jobs are started. But before background
        // task interrupted, it may have start some spark job, so we need to cancel again to
        // make sure job was cancelled when background thread was interrupted
        if (statementId != null) {
          sqlContext.sparkContext.cancelJobGroup(statementId)
        }
        val currentState = getStatus.getState
        if (currentState.isTerminal) {
          // This may happen if the execution was cancelled, and then closed from another thread.
          logWarning(s"Ignore exception in terminal state with $statementId: $e")
        } else {
          logError(s"Error executing query with $statementId, currentState $currentState, ", e)
          setState(OperationState.ERROR)
          e match {
            case hiveException: SparkThriftServerSQLException =>
              SparkThriftServer2.listener.onStatementError(
                statementId, hiveException.getMessage, SparkUtils.exceptionString(hiveException))
              throw hiveException
            case _ =>
              val root = ExceptionUtils.getRootCause(e)
              SparkThriftServer2.listener.onStatementError(
                statementId, root.getMessage, SparkUtils.exceptionString(root))
              throw new SparkThriftServerSQLException("Error running query: " + root.toString, root)
          }
        }
    } finally {
      synchronized {
        if (!getStatus.getState.isTerminal) {
          setState(OperationState.FINISHED)
          SparkThriftServer2.listener.onStatementFinish(statementId)
        }
      }
      sqlContext.sparkContext.clearJobGroup()
    }
  }

  override def cancel(): Unit = {
    synchronized {
      if (!getStatus.getState.isTerminal) {
        logInfo(s"Cancel query with $statementId")
        cleanup(OperationState.CANCELED)
        SparkThriftServer2.listener.onStatementCanceled(statementId)
      }
    }
  }

  private def cleanup(state: OperationState): Unit = {
    setState(state)
    if (runInBackground) {
      val backgroundHandle = getBackgroundHandle
      if (backgroundHandle != null) {
        backgroundHandle.cancel(true)
      }
    }
    if (statementId != null) {
      sqlContext.sparkContext.cancelJobGroup(statementId)
    }
  }

  private def withSchedulerPool[T](body: => T): T = {
    val pool = sessionToActivePool.get(parentSession.getSessionHandle)
    if (pool != null) {
      sqlContext.sparkContext.setLocalProperty(SparkContext.SPARK_SCHEDULER_POOL, pool)
    }
    try {
      body
    } finally {
      if (pool != null) {
        sqlContext.sparkContext.setLocalProperty(SparkContext.SPARK_SCHEDULER_POOL, null)
      }
    }
  }
}
