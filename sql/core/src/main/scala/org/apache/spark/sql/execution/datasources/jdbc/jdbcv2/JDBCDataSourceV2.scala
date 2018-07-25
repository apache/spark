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

package org.apache.spark.sql.execution.datasources.jdbc.jdbcv2

import java.sql.{Connection, ResultSet}
import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils._
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types.StructType

class JDBCDataSourceV2 extends DataSourceV2 with ReadSupport with DataSourceRegister {

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    val jDBCOptV2 = new JDBCOptionsV2(options)
    new JDBCDataSourceReader(jDBCOptV2)
  }

  override def shortName(): String = "jdbcv2"
}

class JDBCDataSourceReader(options: JDBCOptionsV2)
  extends DataSourceReader with SupportsPushDownFilters with SupportsPushDownRequiredColumns {
  val fullschema = JDBCRDD.resolveTable(options)
  var requiredSchema = fullschema
  val schema = readSchema()
  var pushedFiltersArray: Array[Filter] = Array.empty

  override def readSchema(): StructType = {
    requiredSchema
  }

  override def planInputPartitions(): util.List[InputPartition[Row]] = {
    if (options.partitionColumn.isDefined) {
      val partitionInfo = JDBCPartitioningInfo(
        options.partitionColumn.get, options.lowerBound,
        options.upperBound, options.numPartitions)
      val parts = JDBCRelation.columnPartition(partitionInfo.asInstanceOf[JDBCPartitioningInfo])

      parts.map { p =>
        new JDBCInputPartition(requiredSchema, options,
          p.asInstanceOf[JDBCPartition], pushedFiltersArray): InputPartition[Row]
      }.toList.asJava
    }
    else {
      List(new JDBCInputPartition(requiredSchema, options, (JDBCPartition(null, 0)),
        pushedFiltersArray)
        : InputPartition[Row]).asJava
    }
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val postScanfilters =
      filters.filter(JDBCRDD.compileFilter(_, JdbcDialects.get(options.url)).isEmpty)
    pushedFiltersArray = filters diff postScanfilters
    postScanfilters
  }

  override def pushedFilters(): Array[Filter] = {
    pushedFiltersArray
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }
}

class JDBCInputPartition(
    requiredSchema: StructType,
    options: JDBCOptionsV2,
    jDBCPartition: JDBCPartition,
    pushedFiltersArray: Array[Filter]
) extends InputPartition[Row] {
  override def createPartitionReader(): InputPartitionReader[Row] =
    new JDBCInputPartitionReader(requiredSchema, options, jDBCPartition, pushedFiltersArray)
}

class JDBCInputPartitionReader(
    requiredSchema: StructType,
    options: JDBCOptionsV2,
    jDBCPartition: JDBCPartition,
    pushedFiltersArray: Array[Filter]
) extends InputPartitionReader[Row] {

  private val columnList: String = {
    val sb = new StringBuilder()
    requiredSchema.fieldNames.foreach(x => sb.append(",").append(x))
    if (sb.isEmpty) "1" else sb.substring(1)
  }

  private val filterWhereClause: String =
    pushedFiltersArray
      .flatMap(JDBCRDD.compileFilter(_, JdbcDialects.get(options.url)))
      .map(p => s"($p)").mkString(" AND ")

  private def getWhereClause(part: JDBCPartition): String = {
    if (part.whereClause != null && filterWhereClause.length > 0) {
      "WHERE " + s"($filterWhereClause)" + " AND " + s"(${part.whereClause})"
    } else if (part.whereClause != null) {
      "WHERE " + part.whereClause
    } else if (filterWhereClause.length > 0) {
      "WHERE " + filterWhereClause
    } else {
      ""
    }
  }

  val myWhereClause = getWhereClause(jDBCPartition)
  val conn: Connection = JdbcUtils.createConnectionFactory(options)()
  val sqlText = s"SELECT $columnList FROM ${options.table} $myWhereClause"
  val stmt = conn.prepareStatement(sqlText,
    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
  stmt.setFetchSize(options.fetchSize)
  stmt.setQueryTimeout(options.queryTimeout)
  val rs = stmt.executeQuery()
  val rowIterator = resultSetToRows(rs: ResultSet, requiredSchema: StructType)

  override def next(): Boolean = rowIterator.hasNext

  override def get(): Row = {
    rowIterator.next
  }

  override def close(): Unit = rs.close()
}
