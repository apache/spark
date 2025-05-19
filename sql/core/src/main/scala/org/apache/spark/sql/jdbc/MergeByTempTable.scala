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

package org.apache.spark.sql.jdbc

import java.sql.Statement
import java.util.UUID

import org.apache.spark.sql.types.StructField


trait MergeByTempTable {
  self: JdbcDialect =>

  def createTempTableName(): String =
    "temp" + UUID.randomUUID().toString.replaceAll("-", "")

  def getCreatePrimaryIndex(tableName: String, columns: Array[String]): String = {
    val indexColumns = columns.map(quoteIdentifier).mkString(", ")
    s"ALTER TABLE $tableName ADD PRIMARY KEY ($indexColumns)"
  }

  def createPrimaryIndex(
      stmt: Statement,
      tableName: String,
      indexColumns: Array[String]): Unit = {
    val sql = getCreatePrimaryIndex(tableName, indexColumns)
    stmt.executeUpdate(sql)
  }

  /**
   * Returns a SQL query that merges `sourceTableName` into `destinationTableName`
   * w.r.t. to the `keyColumns`.
   *
   * Table names `destinationTableName` and `sourceTableName`, as well as columns in `columns`
   * are expected to be quoted by `JdbcDialect.quoteIdentifier`.
   *
   * @param sourceTableName
   * @param destinationTableName
   * @param columns
   * @param keyColumns
   * @return sql query
   */
  def getMergeQuery(
      sourceTableName: String,
      destinationTableName: String,
      columns: Array[StructField],
      keyColumns: Array[String]): String = {
    val indexColumns = keyColumns.map(quoteIdentifier)
    val mergeCondition = indexColumns.map(k => s"dst.$k = src.$k").mkString(" AND ")
    val updateClause = columns.filterNot(col => keyColumns.contains(col.name))
      .map(col => quoteIdentifier(col.name))
      .map(col => s"$col = src.$col")
      .mkString(", ")
    val quotedColumns = columns.map(col => quoteIdentifier(col.name))
    val insertColumns = quotedColumns.mkString(", ")
    val insertValues = quotedColumns.map(k => s"src.$k").mkString(", ")

    s"""
       |MERGE INTO $destinationTableName AS dst
       |     USING $sourceTableName AS src
       |        ON ($mergeCondition)
       |  WHEN MATCHED THEN
       |    UPDATE SET $updateClause
       |  WHEN NOT MATCHED THEN
       |    INSERT ($insertColumns) VALUES ($insertValues);
       |""".stripMargin
  }

  /**
   * Merges table `sourceTableName` into `destinationTableName` w.r.t. to the `keyColumns`.
   *
   * Table names `destinationTableName` and `sourceTableName`, as well as columns in `columns`
   * are expected to be quoted by `JdbcDialect.quoteIdentifier`.
   *
   * @param stmt
   * @param sourceTableName
   * @param destinationTableName
   * @param columns
   * @param keyColumns
   */
  def merge(
      stmt: Statement,
      sourceTableName: String,
      destinationTableName: String,
      columns: Array[StructField],
      keyColumns: Array[String]): Int = {
    val sql = getMergeQuery(sourceTableName, destinationTableName, columns, keyColumns)
    stmt.executeUpdate(sql)
  }

}
