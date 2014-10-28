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
import java.sql.Timestamp
import java.util.concurrent.Future
import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, Map => SMap}
import scala.math._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.ExecuteStatementOperation
import org.apache.hive.service.cli.session.HiveSession

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.{Row => SparkRow, SchemaRDD}
import org.apache.spark.sql.hive.{HiveContext, HiveMetastoreTypes}
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._

/**
 * A compatibility layer for interacting with Hive version 0.12.0.
 */
private[thriftserver] object HiveThriftServerShim {
  val version = "0.13.1"

  def setServerUserName(sparkServiceUGI: UserGroupInformation, sparkCliService:SparkSQLCLIService) = {
    setSuperField(sparkCliService, "serviceUGI", sparkServiceUGI)
  }
}

private[hive] class SparkSQLDriver(val _context: HiveContext = SparkSQLEnv.hiveContext)
  extends AbstractSparkSQLDriver(_context) {
  override def getResults(res: JList[_]): Boolean = {
    if (hiveResponse == null) {
      false
    } else {
      res.asInstanceOf[JArrayList[String]].addAll(hiveResponse)
      hiveResponse = null
      true
    }
  }
}

private[hive] class SparkExecuteStatementOperation(
    parentSession: HiveSession,
    statement: String,
    confOverlay: JMap[String, String],
    runInBackground: Boolean = true)(
    hiveContext: HiveContext,
    sessionToActivePool: SMap[HiveSession, String]) extends ExecuteStatementOperation(
  parentSession, statement, confOverlay, runInBackground) with Logging {

  private var result: SchemaRDD = _
  private var iter: Iterator[SparkRow] = _
  private var dataTypes: Array[DataType] = _

  private def runInternal(cmd: String) = {
    try {
      result = hiveContext.sql(cmd)
      logDebug(result.queryExecution.toString())
      val groupId = round(random * 1000000).toString
      hiveContext.sparkContext.setJobGroup(groupId, statement)
      iter = {
        val resultRdd = result.queryExecution.toRdd
        val useIncrementalCollect =
          hiveContext.getConf("spark.sql.thriftServer.incrementalCollect", "false").toBoolean
        if (useIncrementalCollect) {
          resultRdd.toLocalIterator
        } else {
          resultRdd.collect().iterator
        }
      }
      dataTypes = result.queryExecution.analyzed.output.map(_.dataType).toArray
    } catch {
      // Actually do need to catch Throwable as some failures don't inherit from Exception and
      // HiveServer will silently swallow them.
      case e: Throwable =>
        logError("Error executing query:",e)
        throw new HiveSQLException(e.toString)
    }
  }

  def close(): Unit = {
    // RDDs will be cleaned automatically upon garbage collection.
    logDebug("CLOSING")
  }

  def addNonNullColumnValue(from: SparkRow, to: ArrayBuffer[Any],  ordinal: Int) {
    dataTypes(ordinal) match {
      case StringType =>
        to += from.get(ordinal).asInstanceOf[String]
      case IntegerType =>
        to += from.getInt(ordinal)
      case BooleanType =>
        to += from.getBoolean(ordinal)
      case DoubleType =>
        to += from.getDouble(ordinal)
      case FloatType =>
        to += from.getFloat(ordinal)
      case DecimalType =>
        to += from.get(ordinal).asInstanceOf[BigDecimal].bigDecimal
      case LongType =>
        to += from.getLong(ordinal)
      case ByteType =>
        to += from.getByte(ordinal)
      case ShortType =>
        to += from.getShort(ordinal)
      case TimestampType =>
        to +=  from.get(ordinal).asInstanceOf[Timestamp]
      case BinaryType =>
        to += from.get(ordinal).asInstanceOf[String]
      case _: ArrayType =>
        to += from.get(ordinal).asInstanceOf[String]
      case _: StructType =>
        to += from.get(ordinal).asInstanceOf[String]
      case _: MapType =>
        to += from.get(ordinal).asInstanceOf[String]
    }
  }

  def getNextRowSet(order: FetchOrientation, maxRowsL: Long): RowSet = {
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)
    val reultRowSet: RowSet = RowSetFactory.create(getResultSetSchema, getProtocolVersion)
    if (!iter.hasNext) {
      reultRowSet
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
        reultRowSet.addRow(row.toArray.asInstanceOf[Array[Object]])
        curRow += 1
      }
      reultRowSet
    }
  }

  def getResultSetSchema: TableSchema = {
    logInfo(s"Result Schema: ${result.queryExecution.analyzed.output}")
    if (result.queryExecution.analyzed.output.size == 0) {
      new TableSchema(new FieldSchema("Result", "string", "") :: Nil)
    } else {
      val schema = result.queryExecution.analyzed.output.map { attr =>
        new FieldSchema(attr.name, HiveMetastoreTypes.toMetastoreType(attr.dataType), "")
      }
      new TableSchema(schema)
    }
  }

  private def getConfigForOperation: HiveConf = {
    var sqlOperationConf: HiveConf = getParentSession.getHiveConf
    if (!getConfOverlay.isEmpty || shouldRunAsync) {
      sqlOperationConf = new HiveConf(sqlOperationConf)
      import scala.collection.JavaConversions._
      for (confEntry <- getConfOverlay.entrySet) {
        try {
          sqlOperationConf.verifyAndSet(confEntry.getKey, confEntry.getValue)
        }
        catch {
          case e: IllegalArgumentException => {
            throw new HiveSQLException("Error applying statement specific settings", e)
          }
        }
      }
    }
    return sqlOperationConf
  }

  def run(): Unit = {
    logInfo(s"Running query '$statement'")
    val opConfig: HiveConf = getConfigForOperation
    setState(OperationState.RUNNING)
    setHasResultSet(true)

    if (!shouldRunAsync) {
      runInternal(statement)
      setState(OperationState.FINISHED)
    } else {
      val parentSessionState = SessionState.get
      val sessionHive: Hive = Hive.get
      val currentUGI: UserGroupInformation = ShimLoader.getHadoopShims.getUGIForConf(opConfig)

      val backgroundOperation: Runnable = new Runnable {
        def run {
          val doAsAction: PrivilegedExceptionAction[AnyRef] =
            new PrivilegedExceptionAction[AnyRef] {
              def run: AnyRef = {
                Hive.set(sessionHive)
                SessionState.setCurrentSessionState(parentSessionState)
                try {
                  runInternal(statement)
                }
                catch {
                  case e: HiveSQLException => {
                    setOperationException(e)
                    logError("Error running hive query: ", e)
                  }
                }
                return null
              }
            }
          try {
            ShimLoader.getHadoopShims.doAs(currentUGI, doAsAction)
          }
          catch {
            case e: Exception => {
              setOperationException(new HiveSQLException(e))
              logError("Error running hive query as user : " + currentUGI.getShortUserName, e)
            }
          }
          setState(OperationState.FINISHED)
        }
      }

      try {
        val backgroundHandle: Future[_] = getParentSession.getSessionManager.
          submitBackgroundOperation(backgroundOperation)
        setBackgroundHandle(backgroundHandle)
      } catch {
        // Actually do need to catch Throwable as some failures don't inherit from Exception and
        // HiveServer will silently swallow them.
        case e: Throwable =>
          logError("Error executing query:",e)
          throw new HiveSQLException(e.toString)
      }
    }
  }
}
