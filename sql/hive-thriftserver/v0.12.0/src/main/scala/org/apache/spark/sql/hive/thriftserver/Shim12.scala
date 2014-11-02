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

import java.sql.Timestamp
import java.util.{ArrayList => JArrayList, Map => JMap}

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, Map => SMap}
import scala.math._

import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.ExecuteStatementOperation
import org.apache.hive.service.cli.session.HiveSession

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.plans.logical.SetCommand
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.{Row => SparkRow, SQLConf, SchemaRDD}
import org.apache.spark.sql.hive.{HiveMetastoreTypes, HiveContext}
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._

/**
 * A compatibility layer for interacting with Hive version 0.12.0.
 */
private[thriftserver] object HiveThriftServerShim {
  val version = "0.12.0"

  def setServerUserName(sparkServiceUGI: UserGroupInformation, sparkCliService:SparkSQLCLIService) = {
    val serverUserName = ShimLoader.getHadoopShims.getShortUserName(sparkServiceUGI)
    setSuperField(sparkCliService, "serverUserName", serverUserName)
  }
}

private[hive] class SparkSQLDriver(val _context: HiveContext = SparkSQLEnv.hiveContext)
  extends AbstractSparkSQLDriver(_context) {
  override def getResults(res: JArrayList[String]): Boolean = {
    if (hiveResponse == null) {
      false
    } else {
      res.addAll(hiveResponse)
      hiveResponse = null
      true
    }
  }
}

private[hive] class SparkExecuteStatementOperation(
    parentSession: HiveSession,
    statement: String,
    confOverlay: JMap[String, String])(
    hiveContext: HiveContext,
    sessionToActivePool: SMap[HiveSession, String]) extends ExecuteStatementOperation(
  parentSession, statement, confOverlay) with Logging {
  private var result: SchemaRDD = _
  private var iter: Iterator[SparkRow] = _
  private var dataTypes: Array[DataType] = _

  def close(): Unit = {
    // RDDs will be cleaned automatically upon garbage collection.
    logDebug("CLOSING")
  }

  def getNextRowSet(order: FetchOrientation, maxRowsL: Long): RowSet = {
    if (!iter.hasNext) {
      new RowSet()
    } else {
      // maxRowsL here typically maps to java.sql.Statement.getFetchSize, which is an int
      val maxRows = maxRowsL.toInt
      var curRow = 0
      var rowSet = new ArrayBuffer[Row](maxRows.min(1024))

      while (curRow < maxRows && iter.hasNext) {
        val sparkRow = iter.next()
        val row = new Row()
        var curCol = 0

        while (curCol < sparkRow.length) {
          if (sparkRow.isNullAt(curCol)) {
            addNullColumnValue(sparkRow, row, curCol)
          } else {
            addNonNullColumnValue(sparkRow, row, curCol)
          }
          curCol += 1
        }
        rowSet += row
        curRow += 1
      }
      new RowSet(rowSet, 0)
    }
  }

  def addNonNullColumnValue(from: SparkRow, to: Row, ordinal: Int) {
    dataTypes(ordinal) match {
      case StringType =>
        to.addString(from(ordinal).asInstanceOf[String])
      case IntegerType =>
        to.addColumnValue(ColumnValue.intValue(from.getInt(ordinal)))
      case BooleanType =>
        to.addColumnValue(ColumnValue.booleanValue(from.getBoolean(ordinal)))
      case DoubleType =>
        to.addColumnValue(ColumnValue.doubleValue(from.getDouble(ordinal)))
      case FloatType =>
        to.addColumnValue(ColumnValue.floatValue(from.getFloat(ordinal)))
      case DecimalType() =>
        val hiveDecimal = from.get(ordinal).asInstanceOf[BigDecimal].bigDecimal
        to.addColumnValue(ColumnValue.stringValue(new HiveDecimal(hiveDecimal)))
      case LongType =>
        to.addColumnValue(ColumnValue.longValue(from.getLong(ordinal)))
      case ByteType =>
        to.addColumnValue(ColumnValue.byteValue(from.getByte(ordinal)))
      case ShortType =>
        to.addColumnValue(ColumnValue.shortValue(from.getShort(ordinal)))
      case TimestampType =>
        to.addColumnValue(
          ColumnValue.timestampValue(from.get(ordinal).asInstanceOf[Timestamp]))
      case BinaryType | _: ArrayType | _: StructType | _: MapType =>
        val hiveString = result
          .queryExecution
          .asInstanceOf[HiveContext#QueryExecution]
          .toHiveString((from.get(ordinal), dataTypes(ordinal)))
        to.addColumnValue(ColumnValue.stringValue(hiveString))
    }
  }

  def addNullColumnValue(from: SparkRow, to: Row, ordinal: Int) {
    dataTypes(ordinal) match {
      case StringType =>
        to.addString(null)
      case IntegerType =>
        to.addColumnValue(ColumnValue.intValue(null))
      case BooleanType =>
        to.addColumnValue(ColumnValue.booleanValue(null))
      case DoubleType =>
        to.addColumnValue(ColumnValue.doubleValue(null))
      case FloatType =>
        to.addColumnValue(ColumnValue.floatValue(null))
      case DecimalType() =>
        to.addColumnValue(ColumnValue.stringValue(null: HiveDecimal))
      case LongType =>
        to.addColumnValue(ColumnValue.longValue(null))
      case ByteType =>
        to.addColumnValue(ColumnValue.byteValue(null))
      case ShortType =>
        to.addColumnValue(ColumnValue.shortValue(null))
      case TimestampType =>
        to.addColumnValue(ColumnValue.timestampValue(null))
      case BinaryType | _: ArrayType | _: StructType | _: MapType =>
        to.addColumnValue(ColumnValue.stringValue(null: String))
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

  def run(): Unit = {
    logInfo(s"Running query '$statement'")
    setState(OperationState.RUNNING)
    try {
      result = hiveContext.sql(statement)
      logDebug(result.queryExecution.toString())
      result.queryExecution.logical match {
        case SetCommand(Some((SQLConf.THRIFTSERVER_POOL, Some(value)))) =>
          sessionToActivePool(parentSession) = value
          logInfo(s"Setting spark.scheduler.pool=$value for future statements in this session.")
        case _ =>
      }

      val groupId = round(random * 1000000).toString
      hiveContext.sparkContext.setJobGroup(groupId, statement)
      sessionToActivePool.get(parentSession).foreach { pool =>
        hiveContext.sparkContext.setLocalProperty("spark.scheduler.pool", pool)
      }
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
      setHasResultSet(true)
    } catch {
      // Actually do need to catch Throwable as some failures don't inherit from Exception and
      // HiveServer will silently swallow them.
      case e: Throwable =>
        logError("Error executing query:",e)
        throw new HiveSQLException(e.toString)
    }
    setState(OperationState.FINISHED)
  }
}
