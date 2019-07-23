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

import java.io.IOException

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.PartitionReader
import org.apache.spark.sql.types.{StructType}

/*
 * Provides basic read implementation.
 * TODO : multi executor paritition scenario
 * TODO : Optimal JDBC connection parameters usage
 */
class DBPartitionReader(options: JDBCOptions, schema : StructType,
                        filters: Array[Filter], prunedCols: StructType)
  extends PartitionReader[InternalRow] with Logging {
  var retrievedRows = 0
  val sqlSelectStmtWithFilters = s"SELECT $prunedCols from ${options.tableOrQuery} $filters"
  val sqlSelectStmt = s"SELECT * from ${options.tableOrQuery}"
  val tc = TaskContext.get
  val inputMetrics = tc.taskMetrics().inputMetrics
  val conn = JdbcUtils.createConnectionFactory(options)()
  val stmt = conn.prepareStatement(sqlSelectStmt)
  val rs = stmt.executeQuery()
  val itrRowIterator = JdbcUtils.resultSetToSparkInternalRows(rs, schema, inputMetrics)

  logInfo("***dsv2-flows*** DBPartitionReader created")
  logInfo(s"***dsv2-flows*** DBPartitionReader SQL stmt $sqlSelectStmt")
  logInfo(s"***dsv2-flows*** DBPartitionReader SQLWithFilters stmt is $sqlSelectStmtWithFilters")

  @throws[IOException]
  def next(): Boolean = {
    logInfo("***dsv2-flows*** next() called")
    itrRowIterator.hasNext
  }

  def get: InternalRow = {
    logInfo("***dsv2-flows*** get() called for row ")
    retrievedRows = retrievedRows + 1
    itrRowIterator.next()
  }

  @throws[IOException]
  override def close(): Unit = {
    logInfo(s"***dsv2-flows*** close called. number of rows retrieved is $retrievedRows")
  }
}
