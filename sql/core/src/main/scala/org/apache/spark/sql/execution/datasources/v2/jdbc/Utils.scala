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

package org.apache.spark.sql.execution.datasources.v2.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite, JdbcUtils}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.{getCommonJDBCType, getJdbcType}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType}

/* Misc utils

 */
object Utils extends Logging{
  def logSchema(prefix: String, schema: Option[StructType]) : Unit = {
    schema match {
      case Some(i) =>
        val schemaString = i.printTreeString()
        logInfo(s"***dsv2-flows*** $prefix schema exists" )
        logInfo(s"***dsv2-flows*** $prefix schemaInDB schema is $schemaString")
      case None =>
        logInfo(s"***dsv2-flows*** $prefix schemaInDB schema is None" )
    }
  }

  def createTable(structType: StructType): Unit = {
    /* Create table per passed schema. Raise exception on any failure.
     */
  }

  def strictSchemaCheck(schemaInSpark: StructType, dbTableSchema: StructType) : Boolean = {
    // TODO : Raise exception if fwPassedSchema is not same as schemaInDB.
    if (schemaInSpark == dbTableSchema) {
      logInfo(s"***dsv2-flows*** strictSchemaCheck passed" )
      true
    } else {
      logInfo(s"***dsv2-flows*** schema check failed" )
      throw new AnalysisException(
        s"Schema does not match with that with the database table")
    }
  }
  def colList(cols: StructType): String = {
    // TODO: column names in quotes
    cols match {
      case null =>
        logInfo(s"***dsv2-flows*** prunedCols is NULL")
        logInfo(s"***dsv2-flows*** returning * ")
        "*"
      case _ =>
        logInfo(s"***dsv2-flows*** prunedCols is NULL")
        val colArr = cols.names
        colArr.length match {
          case 0 =>
            logInfo(s"***dsv2-flows*** returning *")
            "*"
          case _ => colArr.mkString(",")
        }
    }
  }

  def filterList(filters: Array[Filter]) : String = {
    // TODO: Support for filters
    ""
  }

  def createSelectStmt(prunedCols: StructType,
                       filters: Array[Filter],
                       table: String): String = {
    val cols = colList(prunedCols)
    val filtersToAdd = filterList(filters)
    val selectStmt = s"SELECT $cols FROM $table $filtersToAdd"
    logInfo(s"***dsv2-flows*** selectStmt is $selectStmt")
    selectStmt
  }

  def executeSelect(options: JDBCOptions, prunedCols: StructType,
                    filters: Array[Filter], schema : StructType): Iterator[InternalRow] = {
    val stmtString = createSelectStmt(prunedCols, filters, options.tableOrQuery)
    logInfo(s"***dsv2-flows*** executeStmt is $stmtString")
    // TODO : Why we do need the inputMetric here.
    val inputMetrics = TaskContext.get.taskMetrics().inputMetrics
    val conn = JdbcUtils.createConnectionFactory(options)()
    val stmt = conn.prepareStatement(stmtString,
      ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    stmt.setFetchSize(options.fetchSize)
    stmt.setQueryTimeout(options.queryTimeout)
    val rs = stmt.executeQuery()
    JdbcUtils.resultSetToSparkInternalRows(rs, schema, inputMetrics)
  }
}
