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

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.math.{random, round}

import java.sql.Timestamp
import java.util.{Map => JMap}

import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.{ExecuteStatementOperation, Operation, OperationManager}
import org.apache.hive.service.cli.session.HiveSession

import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils
import org.apache.spark.sql.hive.{HiveContext, HiveMetastoreTypes}
import org.apache.spark.sql.{Logging, SchemaRDD, Row => SparkRow}

/**
 * Executes queries using Spark SQL, and maintains a list of handles to active queries.
 */
class SparkSQLOperationManager(hiveContext: HiveContext) extends OperationManager with Logging {
  val handleToOperation = ReflectionUtils
    .getSuperField[JMap[OperationHandle, Operation]](this, "handleToOperation")

  override def newExecuteStatementOperation(
      parentSession: HiveSession,
      statement: String,
      confOverlay: JMap[String, String],
      async: Boolean): ExecuteStatementOperation = synchronized {

    val operation = new ExecuteStatementOperation(parentSession, statement, confOverlay) {
      private var result: SchemaRDD = _
      private var iter: Iterator[SparkRow] = _
      private var dataTypes: Array[DataType] = _

      def close(): Unit = {
        // RDDs will be cleaned automatically upon garbage collection.
        logger.debug("CLOSING")
      }

      def getNextRowSet(order: FetchOrientation, maxRowsL: Long): RowSet = {
        if (!iter.hasNext) {
          new RowSet()
        } else {
          val maxRows = maxRowsL.toInt // Do you really want a row batch larger than Int Max? No.
          var curRow = 0
          var rowSet = new ArrayBuffer[Row](maxRows)

          while (curRow < maxRows && iter.hasNext) {
            val sparkRow = iter.next()
            val row = new Row()
            var curCol = 0

            while (curCol < sparkRow.length) {
              dataTypes(curCol) match {
                case StringType =>
                  row.addString(sparkRow(curCol).asInstanceOf[String])
                case IntegerType =>
                  row.addColumnValue(ColumnValue.intValue(sparkRow.getInt(curCol)))
                case BooleanType =>
                  row.addColumnValue(ColumnValue.booleanValue(sparkRow.getBoolean(curCol)))
                case DoubleType =>
                  row.addColumnValue(ColumnValue.doubleValue(sparkRow.getDouble(curCol)))
                case FloatType =>
                  row.addColumnValue(ColumnValue.floatValue(sparkRow.getFloat(curCol)))
                case DecimalType =>
                  val hiveDecimal = sparkRow.get(curCol).asInstanceOf[BigDecimal].bigDecimal
                  row.addColumnValue(ColumnValue.stringValue(new HiveDecimal(hiveDecimal)))
                case LongType =>
                  row.addColumnValue(ColumnValue.longValue(sparkRow.getLong(curCol)))
                case ByteType =>
                  row.addColumnValue(ColumnValue.byteValue(sparkRow.getByte(curCol)))
                case ShortType =>
                  row.addColumnValue(ColumnValue.intValue(sparkRow.getShort(curCol)))
                case TimestampType =>
                  row.addColumnValue(
                    ColumnValue.timestampValue(sparkRow.get(curCol).asInstanceOf[Timestamp]))
                case BinaryType | _: ArrayType | _: StructType | _: MapType =>
                  val hiveString = result
                    .queryExecution
                    .asInstanceOf[HiveContext#QueryExecution]
                    .toHiveString((sparkRow.get(curCol), dataTypes(curCol)))
                  row.addColumnValue(ColumnValue.stringValue(hiveString))
              }
              curCol += 1
            }
            rowSet += row
            curRow += 1
          }
          new RowSet(rowSet, 0)
        }
      }

      def getResultSetSchema: TableSchema = {
        logger.warn(s"Result Schema: ${result.queryExecution.analyzed.output}")
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
        logger.info(s"Running query '$statement'")
        setState(OperationState.RUNNING)
        try {
          result = hiveContext.hql(statement)
          logger.debug(result.queryExecution.toString())
          val groupId = round(random * 1000000).toString
          hiveContext.sparkContext.setJobGroup(groupId, statement)
          iter = result.queryExecution.toRdd.toLocalIterator
          dataTypes = result.queryExecution.analyzed.output.map(_.dataType).toArray
          setHasResultSet(true)
        } catch {
          // Actually do need to catch Throwable as some failures don't inherit from Exception and
          // HiveServer will silently swallow them.
          case e: Throwable =>
            logger.error("Error executing query:",e)
            throw new HiveSQLException(e.toString)
        }
        setState(OperationState.FINISHED)
      }
    }

   handleToOperation.put(operation.getHandle, operation)
   operation
  }
}
