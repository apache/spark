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
import java.util.{Arrays, Map => JMap, UUID}
import java.util.concurrent.RejectedExecutionException

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
import org.apache.spark.sql.execution.command.SetCommand
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.{Utils => SparkUtils}

private[hive] class SparkExecuteStatementOperation(
    parentSession: HiveSession,
    statement: String,
    confOverlay: JMap[String, String],
    runInBackground: Boolean = true)
    (sqlContext: SQLContext, sessionToActivePool: JMap[SessionHandle, String])
  extends ExecuteStatementOperation(parentSession, statement, confOverlay, runInBackground)
  with Logging {

  private var result: DataFrame = _

  // We cache the returned rows to get iterators again in case the user wants to use FETCH_FIRST.
  // This is only used when `spark.sql.thriftServer.incrementalCollect` is set to `false`.
  // In case of `true`, this will be `None` and FETCH_FIRST will trigger re-execution.
  private var resultList: Option[Array[SparkRow]] = _

  private var iter: Iterator[SparkRow] = _
  private var dataTypes: Array[DataType] = _
  private var statementId: String = _

  private lazy val resultSchema: TableSchema = {
    if (result == null || result.schema.isEmpty) {
      new TableSchema(Arrays.asList(new FieldSchema("Result", "string", "")))
    } else {
      logInfo(s"Result Schema: ${result.schema}")
      SparkExecuteStatementOperation.getTableSchema(result.schema)
    }
  }

  def close(): Unit = {
    // RDDs will be cleaned automatically upon garbage collection.
    logDebug(s"CLOSING $statementId")
    cleanup(OperationState.CLOSED)
    sqlContext.sparkContext.clearJobGroup()
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
        to += from.getAs[Timestamp](ordinal)
      case BinaryType =>
        to += from.getAs[Array[Byte]](ordinal)
      case _: ArrayType | _: StructType | _: MapType | _: UserDefinedType[_] =>
        val hiveString = HiveUtils.toHiveString((from.get(ordinal), dataTypes(ordinal)))
        to += hiveString
    }
  }

  def getNextRowSet(order: FetchOrientation, maxRowsL: Long): RowSet = {
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)
    val resultRowSet: RowSet = RowSetFactory.create(getResultSetSchema, getProtocolVersion)

    // Reset iter to header when fetching start from first row
    if (order.equals(FetchOrientation.FETCH_FIRST)) {
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

  def getResultSetSchema: TableSchema = resultSchema

  override def runInternal(): Unit = {
    setState(OperationState.PENDING)
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
                case e: HiveSQLException =>
                  setOperationException(e)
                  log.error("Error running hive query: ", e)
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

  private def execute(): Unit = {
    statementId = UUID.randomUUID().toString
    logInfo(s"Running query '$statement' with $statementId")
    setState(OperationState.RUNNING)
    // Always use the latest class loader provided by executionHive's state.
    val executionHiveClassLoader = sqlContext.sharedState.jarClassLoader
    Thread.currentThread().setContextClassLoader(executionHiveClassLoader)

    HiveThriftServer2.listener.onStatementStart(
      statementId,
      parentSession.getSessionHandle.getSessionId.toString,
      statement,
      statementId,
      parentSession.getUsername)
    sqlContext.sparkContext.setJobGroup(statementId, statement)
    val pool = sessionToActivePool.get(parentSession.getSessionHandle)
    if (pool != null) {
      sqlContext.sparkContext.setLocalProperty("spark.scheduler.pool", pool)
    }
    try {
      result = sqlContext.sql(statement)
      logDebug(result.queryExecution.toString())
      result.queryExecution.logical match {
        case SetCommand(Some((SQLConf.THRIFTSERVER_POOL.key, Some(value)))) =>
          sessionToActivePool.put(parentSession.getSessionHandle, value)
          logInfo(s"Setting spark.scheduler.pool=$value for future statements in this session.")
        case _ =>
      }
      HiveThriftServer2.listener.onStatementParsed(statementId, result.queryExecution.toString())
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
      case e: HiveSQLException =>
        if (getStatus().getState() == OperationState.CANCELED) {
          return
        } else {
          setState(OperationState.ERROR)
          HiveThriftServer2.listener.onStatementError(
            statementId, e.getMessage, SparkUtils.exceptionString(e))
          throw e
        }
      // Actually do need to catch Throwable as some failures don't inherit from Exception and
      // HiveServer will silently swallow them.
      case e: Throwable =>
        val currentState = getStatus().getState()
        logError(s"Error executing query, currentState $currentState, ", e)
        setState(OperationState.ERROR)
        HiveThriftServer2.listener.onStatementError(
          statementId, e.getMessage, SparkUtils.exceptionString(e))
        throw new HiveSQLException(e.toString)
    }
    setState(OperationState.FINISHED)
    HiveThriftServer2.listener.onStatementFinish(statementId)
  }

  override def cancel(): Unit = {
    logInfo(s"Cancel '$statement' with $statementId")
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
    if (statementId != null) {
      sqlContext.sparkContext.cancelJobGroup(statementId)
    }
  }
}

object SparkExecuteStatementOperation {
  def getTableSchema(structType: StructType): TableSchema = {
    val schema = structType.map { field =>
      val attrTypeString = if (field.dataType == NullType) "void" else field.dataType.catalogString
      new FieldSchema(field.name, attrTypeString, field.getComment.getOrElse(""))
    }
    new TableSchema(schema.asJava)
  }
}
