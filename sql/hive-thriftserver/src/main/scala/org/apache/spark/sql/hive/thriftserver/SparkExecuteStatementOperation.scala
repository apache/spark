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
import java.util.{Arrays, Map => JMap}
import java.util.concurrent.{Executors, RejectedExecutionException, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.shims.Utils
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.ExecuteStatementOperation
import org.apache.hive.service.cli.session.HiveSession

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row => SparkRow, SQLContext}
import org.apache.spark.sql.execution.HiveResult.{getTimeFormatters, toHiveString, TimeFormatters}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.VariableSubstitution
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.{Utils => SparkUtils}

private[hive] class SparkExecuteStatementOperation(
    val sqlContext: SQLContext,
    parentSession: HiveSession,
    statement: String,
    confOverlay: JMap[String, String],
    runInBackground: Boolean = true,
    queryTimeout: Long)
  extends ExecuteStatementOperation(parentSession, statement, confOverlay, runInBackground)
  with SparkOperation
  with Logging {

  // If a timeout value `queryTimeout` is specified by users and it is smaller than
  // a global timeout value, we use the user-specified value.
  // This code follows the Hive timeout behaviour (See #29933 for details).
  private val timeout = {
    val globalTimeout = sqlContext.conf.getConf(SQLConf.THRIFTSERVER_QUERY_TIMEOUT)
    if (globalTimeout > 0 && (queryTimeout <= 0 || globalTimeout < queryTimeout)) {
      globalTimeout
    } else {
      queryTimeout
    }
  }

  private val forceCancel = sqlContext.conf.getConf(SQLConf.THRIFTSERVER_FORCE_CANCEL)

  private val substitutorStatement = SQLConf.withExistingConf(sqlContext.conf) {
    new VariableSubstitution().substitute(statement)
  }

  private var result: DataFrame = _

  private var iter: FetchIterator[SparkRow] = _
  private var dataTypes: Array[DataType] = _

  private lazy val resultSchema: TableSchema = {
    if (result == null || result.schema.isEmpty) {
      new TableSchema(Arrays.asList(new FieldSchema("Result", "string", "")))
    } else {
      logInfo(s"Result Schema: ${result.schema}")
      SparkExecuteStatementOperation.getTableSchema(result.schema)
    }
  }

  def addNonNullColumnValue(
      from: SparkRow,
      to: ArrayBuffer[Any],
      ordinal: Int,
      timeFormatters: TimeFormatters): Unit = {
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
      case BinaryType =>
        to += from.getAs[Array[Byte]](ordinal)
      // SPARK-31859, SPARK-31861: Date and Timestamp need to be turned to String here to:
      // - respect spark.sql.session.timeZone
      // - work with spark.sql.datetime.java8API.enabled
      // These types have always been sent over the wire as string, converted later.
      case _: DateType | _: TimestampType =>
        to += toHiveString((from.get(ordinal), dataTypes(ordinal)), false, timeFormatters)
      case CalendarIntervalType =>
        to += toHiveString(
          (from.getAs[CalendarInterval](ordinal), CalendarIntervalType),
          false,
          timeFormatters)
      case _: ArrayType | _: StructType | _: MapType | _: UserDefinedType[_] |
          _: AnsiIntervalType | _: TimestampNTZType =>
        to += toHiveString((from.get(ordinal), dataTypes(ordinal)), false, timeFormatters)
    }
  }

  def getNextRowSet(order: FetchOrientation, maxRowsL: Long): RowSet = withLocalProperties {
    try {
      sqlContext.sparkContext.setJobGroup(statementId, substitutorStatement, forceCancel)
      getNextRowSetInternal(order, maxRowsL)
    } finally {
      sqlContext.sparkContext.clearJobGroup()
    }
  }

  private def getNextRowSetInternal(
      order: FetchOrientation,
      maxRowsL: Long): RowSet = withLocalProperties {
    log.info(s"Received getNextRowSet request order=${order} and maxRowsL=${maxRowsL} " +
      s"with ${statementId}")
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)
    val resultRowSet: RowSet = RowSetFactory.create(getResultSetSchema, getProtocolVersion, false)

    if (order.equals(FetchOrientation.FETCH_FIRST)) {
      iter.fetchAbsolute(0)
    } else if (order.equals(FetchOrientation.FETCH_PRIOR)) {
      iter.fetchPrior(maxRowsL)
    } else {
      iter.fetchNext()
    }
    resultRowSet.setStartOffset(iter.getPosition)
    if (!iter.hasNext) {
      resultRowSet
    } else {
      val timeFormatters = getTimeFormatters
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
            addNonNullColumnValue(sparkRow, row, curCol, timeFormatters)
          }
          curCol += 1
        }
        resultRowSet.addRow(row.toArray.asInstanceOf[Array[Object]])
        curRow += 1
      }
      log.info(s"Returning result set with ${curRow} rows from offsets " +
        s"[${iter.getFetchStart}, ${iter.getPosition}) with $statementId")
      resultRowSet
    }
  }

  def getResultSetSchema: TableSchema = resultSchema

  override def runInternal(): Unit = {
    setState(OperationState.PENDING)
    logInfo(s"Submitting query '$statement' with $statementId")
    HiveThriftServer2.eventManager.onStatementStart(
      statementId,
      parentSession.getSessionHandle.getSessionId.toString,
      statement,
      statementId,
      parentSession.getUsername)
    setHasResultSet(true) // avoid no resultset for async run

    if (timeout > 0) {
      val timeoutExecutor = Executors.newSingleThreadScheduledExecutor()
      timeoutExecutor.schedule(new Runnable {
        override def run(): Unit = {
          try {
            timeoutCancel()
          } catch {
            case NonFatal(e) =>
              setOperationException(new HiveSQLException(e))
              logError(s"Error cancelling the query after timeout: $timeout seconds")
          } finally {
            timeoutExecutor.shutdown()
          }
        }
      }, timeout, TimeUnit.SECONDS)
    }

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
                withLocalProperties {
                  execute()
                }
              } catch {
                case e: HiveSQLException => setOperationException(e)
              }
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
          parentSession.getSessionManager().submitBackgroundOperation(backgroundOperation)
        setBackgroundHandle(backgroundHandle)
      } catch {
        case rejected: RejectedExecutionException =>
          logError("Error submitting query in background, query rejected", rejected)
          setState(OperationState.ERROR)
          HiveThriftServer2.eventManager.onStatementError(
            statementId, rejected.getMessage, SparkUtils.exceptionString(rejected))
          throw HiveThriftServerErrors.taskExecutionRejectedError(rejected)
        case NonFatal(e) =>
          logError(s"Error executing query in background", e)
          setState(OperationState.ERROR)
          HiveThriftServer2.eventManager.onStatementError(
            statementId, e.getMessage, SparkUtils.exceptionString(e))
          throw new HiveSQLException(e)
      }
    }
  }

  private def execute(): Unit = {
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

      // Always set the session state classloader to `executionHiveClassLoader` even for sync mode
      if (!runInBackground) {
        parentSession.getSessionState.getConf.setClassLoader(executionHiveClassLoader)
      }

      sqlContext.sparkContext.setJobGroup(statementId, substitutorStatement, forceCancel)
      result = sqlContext.sql(statement)
      logDebug(result.queryExecution.toString())
      HiveThriftServer2.eventManager.onStatementParsed(statementId,
        result.queryExecution.toString())
      iter = if (sqlContext.getConf(SQLConf.THRIFTSERVER_INCREMENTAL_COLLECT.key).toBoolean) {
        new IterableFetchIterator[SparkRow](new Iterable[SparkRow] {
          override def iterator: Iterator[SparkRow] = result.toLocalIterator.asScala
        })
      } else {
        new ArrayFetchIterator[SparkRow](result.collect())
      }
      dataTypes = result.schema.fields.map(_.dataType)
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
        val currentState = getStatus().getState()
        if (currentState.isTerminal) {
          // This may happen if the execution was cancelled, and then closed from another thread.
          logWarning(s"Ignore exception in terminal state with $statementId: $e")
        } else {
          logError(s"Error executing query with $statementId, currentState $currentState, ", e)
          setState(OperationState.ERROR)
          HiveThriftServer2.eventManager.onStatementError(
            statementId, e.getMessage, SparkUtils.exceptionString(e))
          e match {
            case _: HiveSQLException => throw e
            case _ => throw HiveThriftServerErrors.runningQueryError(e)
          }
        }
    } finally {
      synchronized {
        if (!getStatus.getState.isTerminal) {
          setState(OperationState.FINISHED)
          HiveThriftServer2.eventManager.onStatementFinish(statementId)
        }
      }
      sqlContext.sparkContext.clearJobGroup()
    }
  }

  def timeoutCancel(): Unit = {
    synchronized {
      if (!getStatus.getState.isTerminal) {
        logInfo(s"Query with $statementId timed out after $timeout seconds")
        setState(OperationState.TIMEDOUT)
        cleanup()
        HiveThriftServer2.eventManager.onStatementTimeout(statementId)
      }
    }
  }

  override def cancel(): Unit = {
    synchronized {
      if (!getStatus.getState.isTerminal) {
        logInfo(s"Cancel query with $statementId")
        setState(OperationState.CANCELED)
        cleanup()
        HiveThriftServer2.eventManager.onStatementCanceled(statementId)
      }
    }
  }

  override protected def cleanup(): Unit = {
    if (runInBackground) {
      val backgroundHandle = getBackgroundHandle()
      if (backgroundHandle != null) {
        backgroundHandle.cancel(true)
      }
    }
    // RDDs will be cleaned automatically upon garbage collection.
    if (statementId != null) {
      sqlContext.sparkContext.cancelJobGroup(statementId)
    }
  }
}

object SparkExecuteStatementOperation {
  def getTableSchema(structType: StructType): TableSchema = {
    val schema = structType.map { field =>
      val attrTypeString = field.dataType match {
        case CalendarIntervalType => StringType.catalogString
        case _: YearMonthIntervalType => "interval_year_month"
        case _: DayTimeIntervalType => "interval_day_time"
        case _: TimestampNTZType => "timestamp"
        case other => other.catalogString
      }
      new FieldSchema(field.name, attrTypeString, field.getComment.getOrElse(""))
    }
    new TableSchema(schema.asJava)
  }
}
