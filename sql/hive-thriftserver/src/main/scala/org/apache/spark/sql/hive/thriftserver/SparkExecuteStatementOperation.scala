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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

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

private[hive] class SparkExecuteStatementOperation(
    val sqlContext: SQLContext,
    parentSession: HiveSession,
    statement: String,
    confOverlay: JMap[String, String],
    runInBackground: Boolean = true)
  extends ExecuteStatementOperation(parentSession, statement, confOverlay, runInBackground)
  with SparkOperation
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
      case _: ArrayType | _: StructType | _: MapType | _: UserDefinedType[_] =>
        to += toHiveString((from.get(ordinal), dataTypes(ordinal)), false, timeFormatters)
    }
  }

  def getNextRowSet(order: FetchOrientation, maxRowsL: Long): RowSet = withLocalProperties {
    logInfo(s"Received getNextRowSet request order=${order} and maxRowsL=${maxRowsL} " +
      s"with ${statementId}")
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)
    val resultRowSet: RowSet =
      ThriftserverShimUtils.resultRowSet(getResultSetSchema, getProtocolVersion)

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
        resultOffset += 1
      }
      previousFetchEndOffset = resultOffset
      logInfo(s"Returning result set with ${curRow} rows from offsets " +
        s"[$previousFetchStartOffset, $previousFetchEndOffset) with $statementId")
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
                case e: HiveSQLException =>
                  setOperationException(e)
                  logError(s"Error executing query with $statementId,", e)
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
      } catch onError()
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

      val substitutorStatement = new VariableSubstitution(sqlContext.conf).substitute(statement)
      sqlContext.sparkContext.setJobGroup(statementId, substitutorStatement)
      result = sqlContext.sql(statement)
      logDebug(result.queryExecution.toString())
      HiveThriftServer2.eventManager.onStatementParsed(statementId,
        result.queryExecution.toString())
      iter = {
        if (sqlContext.getConf(SQLConf.THRIFTSERVER_INCREMENTAL_COLLECT.key).toBoolean) {
          resultList = None
          result.toLocalIterator.asScala
        } else {
          resultList = Some(result.collect())
          resultList.get.iterator
        }
      }
      dataTypes = result.schema.fields.map(_.dataType)
    } catch {
      onError(needCancel = true)
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
    sqlContext.sparkContext.cancelJobGroup(statementId)
  }
}

object SparkExecuteStatementOperation {
  def getTableSchema(structType: StructType): TableSchema = {
    val schema = structType.map { field =>
      val attrTypeString = field.dataType match {
        case NullType => "void"
        case CalendarIntervalType => StringType.catalogString
        case other => other.catalogString
      }
      new FieldSchema(field.name, attrTypeString, field.getComment.getOrElse(""))
    }
    new TableSchema(schema.asJava)
  }
}
