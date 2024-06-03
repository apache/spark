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
import java.util.{Collections, Map => JMap}
import java.util.concurrent.{Executors, RejectedExecutionException, TimeUnit}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.hive.shims.Utils
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.ExecuteStatementOperation
import org.apache.hive.service.cli.session.HiveSession
import org.apache.hive.service.rpc.thrift.{TCLIServiceConstants, TColumnDesc, TPrimitiveTypeEntry, TRowSet, TTableSchema, TTypeDesc, TTypeEntry, TTypeId, TTypeQualifiers, TTypeQualifierValue}

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MILLIS_PER_SECOND
import org.apache.spark.sql.internal.{SQLConf, VariableSubstitution}
import org.apache.spark.sql.types._
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

  val session = sqlContext.sparkSession

  // If a timeout value `queryTimeout` is specified by users and it is smaller than
  // a global timeout value, we use the user-specified value.
  // This code follows the Hive timeout behaviour (See #29933 for details).
  private val timeout = {
    val globalTimeout = session.sessionState.conf.getConf(SQLConf.THRIFTSERVER_QUERY_TIMEOUT)
    if (globalTimeout > 0 && (queryTimeout <= 0 || globalTimeout < queryTimeout)) {
      globalTimeout
    } else {
      queryTimeout
    }
  }

  private val forceCancel = session.sessionState.conf.getConf(SQLConf.THRIFTSERVER_FORCE_CANCEL)

  private val redactedStatement = {
    val substitutorStatement = SQLConf.withExistingConf(session.sessionState.conf) {
      new VariableSubstitution().substitute(statement)
    }
    SparkUtils.redact(session.sessionState.conf.stringRedactionPattern, substitutorStatement)
  }

  private var result: DataFrame = _

  private var iter: FetchIterator[Row] = _
  private var dataTypes: Array[DataType] = _

  private lazy val resultSchema: TTableSchema = {
    if (result == null || result.schema.isEmpty) {
      val sparkType = new StructType().add("Result", "string")
      SparkExecuteStatementOperation.toTTableSchema(sparkType)
    } else {
      logInfo(s"Result Schema: ${result.schema.sql}")
      SparkExecuteStatementOperation.toTTableSchema(result.schema)
    }
  }

  def getNextRowSet(order: FetchOrientation, maxRowsL: Long): TRowSet = withLocalProperties {
    try {
      sqlContext.sparkContext.setJobGroup(statementId, redactedStatement, forceCancel)
      getNextRowSetInternal(order, maxRowsL)
    } finally {
      sqlContext.sparkContext.clearJobGroup()
    }
  }

  private def getNextRowSetInternal(
      order: FetchOrientation,
      maxRowsL: Long): TRowSet = withLocalProperties {
    log.debug(s"Received getNextRowSet request order=${order} and maxRowsL=${maxRowsL} " +
      s"with ${statementId}")
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)

    if (order.equals(FetchOrientation.FETCH_FIRST)) {
      iter.fetchAbsolute(0)
    } else if (order.equals(FetchOrientation.FETCH_PRIOR)) {
      iter.fetchPrior(maxRowsL)
    } else {
      iter.fetchNext()
    }
    val maxRows = maxRowsL.toInt
    val offset = iter.getPosition
    val rows = iter.take(maxRows).toList
    log.debug(s"Returning result set with ${rows.length} rows from offsets " +
      s"[${iter.getFetchStart}, ${iter.getPosition}) with $statementId")
    RowSetUtils.toTRowSet(offset, rows, dataTypes, getProtocolVersion)
  }

  def getResultSetSchema: TTableSchema = resultSchema

  override def runInternal(): Unit = {
    setState(OperationState.PENDING)
    logInfo(
      log"Submitting query '${MDC(REDACTED_STATEMENT, redactedStatement)}' with " +
        log"${MDC(STATEMENT_ID, statementId)}")
    HiveThriftServer2.eventManager.onStatementStart(
      statementId,
      parentSession.getSessionHandle.getSessionId.toString,
      redactedStatement,
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
              val timeout_ms = timeout * MILLIS_PER_SECOND
              logError(
                log"Error cancelling the query after timeout: ${MDC(TIMEOUT, timeout_ms)} ms")
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
              logError(log"Error running hive query as user : " +
                log"${MDC(USER_NAME, sparkServiceUGI.getShortUserName())}", e)
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
          logError("Error executing query in background", e)
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
          logInfo(
            log"Query with ${MDC(STATEMENT_ID, statementId)} in terminal state " +
              log"before it started running")
          return
        } else {
          logInfo(log"Running query with ${MDC(STATEMENT_ID, statementId)}")
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

      sqlContext.sparkContext.setJobGroup(statementId, redactedStatement, forceCancel)
      result = sqlContext.sql(statement)
      logDebug(result.queryExecution.toString())
      HiveThriftServer2.eventManager.onStatementParsed(statementId,
        result.queryExecution.toString())
      iter = if (sqlContext.getConf(SQLConf.THRIFTSERVER_INCREMENTAL_COLLECT.key).toBoolean) {
        new IterableFetchIterator[Row](new Iterable[Row] {
          override def iterator: Iterator[Row] = result.toLocalIterator().asScala
        })
      } else {
        new ArrayFetchIterator[Row](result.collect())
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
          logWarning(
            log"Ignore exception in terminal state with ${MDC(STATEMENT_ID, statementId)}", e
          )
        } else {
          logError(log"Error executing query with ${MDC(STATEMENT_ID, statementId)}, " +
            log"currentState ${MDC(HIVE_OPERATION_STATE, currentState)}, ", e)
          setState(OperationState.ERROR)
          HiveThriftServer2.eventManager.onStatementError(
            statementId, e.getMessage, SparkUtils.exceptionString(e))
          e match {
            case _: HiveSQLException => throw e
            case _ => throw HiveThriftServerErrors.runningQueryError(
              e, sqlContext.sparkSession.sessionState.conf.errorMessageFormat)
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
        logInfo(
          log"Query with ${MDC(STATEMENT_ID, statementId)} timed out after " +
            log"${MDC(TIMEOUT, timeout)} seconds")
        setState(OperationState.TIMEDOUT)
        cleanup()
        HiveThriftServer2.eventManager.onStatementTimeout(statementId)
      }
    }
  }

  override def cancel(): Unit = {
    synchronized {
      if (!getStatus.getState.isTerminal) {
        logInfo(log"Cancel query with ${MDC(STATEMENT_ID, statementId)}")
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

  def toTTypeId(typ: DataType): TTypeId = typ match {
    case NullType => TTypeId.NULL_TYPE
    case BooleanType => TTypeId.BOOLEAN_TYPE
    case ByteType => TTypeId.TINYINT_TYPE
    case ShortType => TTypeId.SMALLINT_TYPE
    case IntegerType => TTypeId.INT_TYPE
    case LongType => TTypeId.BIGINT_TYPE
    case FloatType => TTypeId.FLOAT_TYPE
    case DoubleType => TTypeId.DOUBLE_TYPE
    case StringType => TTypeId.STRING_TYPE
    case _: DecimalType => TTypeId.DECIMAL_TYPE
    case DateType => TTypeId.DATE_TYPE
    // TODO: Shall use TIMESTAMPLOCALTZ_TYPE, keep AS-IS now for
    // unnecessary behavior change
    case TimestampType => TTypeId.TIMESTAMP_TYPE
    case TimestampNTZType => TTypeId.TIMESTAMP_TYPE
    case BinaryType => TTypeId.BINARY_TYPE
    case CalendarIntervalType => TTypeId.STRING_TYPE
    case _: DayTimeIntervalType => TTypeId.INTERVAL_DAY_TIME_TYPE
    case _: YearMonthIntervalType => TTypeId.INTERVAL_YEAR_MONTH_TYPE
    case _: ArrayType => TTypeId.ARRAY_TYPE
    case _: MapType => TTypeId.MAP_TYPE
    case _: StructType => TTypeId.STRUCT_TYPE
    case _: CharType => TTypeId.CHAR_TYPE
    case _: VarcharType => TTypeId.VARCHAR_TYPE
    case other =>
      throw new IllegalArgumentException(s"Unrecognized type name: ${other.catalogString}")
  }

  private def toTTypeQualifiers(typ: DataType): TTypeQualifiers = {
    val ret = new TTypeQualifiers()
    val qualifiers = typ match {
      case d: DecimalType =>
        Map(
          TCLIServiceConstants.PRECISION -> TTypeQualifierValue.i32Value(d.precision),
          TCLIServiceConstants.SCALE -> TTypeQualifierValue.i32Value(d.scale)).asJava
      case _: VarcharType | _: CharType =>
        Map(TCLIServiceConstants.CHARACTER_MAXIMUM_LENGTH ->
          TTypeQualifierValue.i32Value(typ.defaultSize)).asJava
      case _ => Collections.emptyMap[String, TTypeQualifierValue]()
    }
    ret.setQualifiers(qualifiers)
    ret
  }

  private def toTTypeDesc(typ: DataType): TTypeDesc = {
    val typeEntry = new TPrimitiveTypeEntry(toTTypeId(typ))
    typeEntry.setTypeQualifiers(toTTypeQualifiers(typ))
    val tTypeDesc = new TTypeDesc()
    tTypeDesc.addToTypes(TTypeEntry.primitiveEntry(typeEntry))
    tTypeDesc
  }

  private def toTColumnDesc(field: StructField, pos: Int): TColumnDesc = {
    val tColumnDesc = new TColumnDesc()
    tColumnDesc.setColumnName(field.name)
    tColumnDesc.setTypeDesc(toTTypeDesc(field.dataType))
    tColumnDesc.setComment(field.getComment().getOrElse(""))
    tColumnDesc.setPosition(pos)
    tColumnDesc
  }

  def toTTableSchema(schema: StructType): TTableSchema = {
    val tTableSchema = new TTableSchema()
    CharVarcharUtils.getRawSchema(schema).zipWithIndex.foreach { case (f, i) =>
      tTableSchema.addToColumns(toTColumnDesc(f, i))
    }
    tTableSchema
  }
}
