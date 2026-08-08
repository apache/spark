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

import java.sql.{Connection, SQLException}

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.connector.expressions.Expression
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types.{DataType, MetadataBuilder}

/**
 * AggregatedDialect can unify multiple dialects into one virtual Dialect.
 * Dialects are tried in order, and the first dialect that does not return a
 * neutral element will win.
 *
 * @param dialects List of dialects.
 */
private class AggregatedDialect(dialects: List[JdbcDialect])
  extends JdbcDialect with NoLegacyJDBCError {

  require(dialects.nonEmpty)

  override def canHandle(url : String): Boolean =
    dialects.map(_.canHandle(url)).reduce(_ && _)

  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    dialects.flatMap(_.getCatalystType(sqlType, typeName, size, md)).headOption
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = {
    dialects.flatMap(_.getJDBCType(dt)).headOption
  }

  override def quoteIdentifier(colName: String): String = {
    dialects.head.quoteIdentifier(colName)
  }

  override def getTableExistsQuery(table: String): String = {
    dialects.head.getTableExistsQuery(table)
  }

  override def getSchemaQuery(table: String): String = {
    dialects.head.getSchemaQuery(table)
  }

  override def isSyntaxErrorBestEffort(exception: SQLException): Boolean = {
    dialects.head.isSyntaxErrorBestEffort(exception)
  }

  override def isCascadingTruncateTable(): Option[Boolean] = {
    // If any dialect claims cascading truncate, this dialect is also cascading truncate.
    // Otherwise, if any dialect has unknown cascading truncate, this dialect is also unknown.
    dialects.flatMap(_.isCascadingTruncateTable()).reduceOption(_ || _) match {
      case Some(true) => Some(true)
      case _ if dialects.exists(_.isCascadingTruncateTable().isEmpty) => None
      case _ => Some(false)
    }
  }

  /**
   * The SQL query used to truncate a table.
   * @param table The table to truncate.
   * @param cascade Whether or not to cascade the truncation. Default value is the
   *                value of isCascadingTruncateTable()
   * @return The SQL query to use for truncating a table
   */
  override def getTruncateQuery(
      table: String,
      cascade: Option[Boolean] = isCascadingTruncateTable()): String = {
    dialects.head.getTruncateQuery(table, cascade)
  }

  override def getFetchSize(options: JDBCOptions): Int = {
    dialects.head.getFetchSize(options)
  }

  override def beforeFetch(connection: Connection, options: JDBCOptions): Unit = {
    dialects.head.beforeFetch(connection, options)
  }

  override def compileValue(value: Any): Any = dialects.head.compileValue(value)

  override def compileExpression(expr: Expression): Option[String] = {
    dialects.flatMap(_.compileExpression(expr)).headOption
  }

  override def isSupportedFunction(funcName: String): Boolean = {
    dialects.exists(_.isSupportedFunction(funcName))
  }

  override def getJdbcSQLQueryBuilder(options: JDBCOptions): JdbcSQLQueryBuilder = {
    dialects.head.getJdbcSQLQueryBuilder(options)
  }

  override def supportsLimit: Boolean = dialects.head.supportsLimit

  override def supportsOffset: Boolean = dialects.head.supportsOffset

  override def supportsHint: Boolean = dialects.head.supportsHint

  override def supportsJoin: Boolean = dialects.head.supportsJoin

  override def getLimitClause(limit: Integer): String = dialects.head.getLimitClause(limit)

  override def getOffsetClause(offset: Integer): String = dialects.head.getOffsetClause(offset)

  override def classifyException(
      e: Throwable,
      condition: String,
      messageParameters: Map[String, String],
      description: String,
      isRuntime: Boolean): Throwable with SparkThrowable = {
    dialects.head.classifyException(e, condition, messageParameters, description, isRuntime)
  }

  override def renameTable(oldTable: String, newTable: String): String = {
    dialects.head.renameTable(oldTable, newTable)
  }

  override def functions: Seq[(String, UnboundFunction)] = dialects.head.functions

  override def createConnectionFactory(options: JDBCOptions): Int => Connection = {
    dialects.head.createConnectionFactory(options)
  }
}
