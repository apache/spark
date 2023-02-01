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

import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCPartition}
import org.apache.spark.sql.execution.datasources.v2.TableSampleInfo

/**
 * The builder to generate jdbc sql query.
 *
 * @since 3.4.0
 */
class JdbcSQLQueryBuilder(dialect: JdbcDialect, options: JDBCOptions) {

  /**
   * `columns`, but as a String suitable for injection into a SQL query.
   */
  protected var columnList: String = "1"

  /**
   * A WHERE clause representing both `filters`, if any, and the current partition.
   */
  protected var whereClause: String = ""

  /**
   * A GROUP BY clause representing pushed-down grouping columns.
   */
  protected var groupByClause: String = ""

  /**
   * A ORDER BY clause representing pushed-down sort of top n.
   */
  protected var orderByClause: String = ""

  /**
   * A LIMIT clause representing pushed-down limit.
   */
  protected var limitClause: String = ""

  /**
   * A OFFSET clause representing pushed-down offset.
   */
  protected var offsetClause: String = ""

  /**
   * A table sample clause representing pushed-down table sample.
   */
  protected var tableSampleClause: String = ""

  def withColumns(columns: Array[String]): JdbcSQLQueryBuilder = {
    if (columns.nonEmpty) {
      columnList = columns.mkString(",")
    }
    this
  }

  def withPredicates(predicates: Array[Predicate], part: JDBCPartition): JdbcSQLQueryBuilder = {
    // `filters`, but as a WHERE clause suitable for injection into a SQL query.
    val filterWhereClause: String = {
      predicates.flatMap(dialect.compileExpression(_)).map(p => s"($p)").mkString(" AND ")
    }

    // A WHERE clause representing both `filters`, if any, and the current partition.
    whereClause = if (part.whereClause != null && filterWhereClause.length > 0) {
      "WHERE " + s"($filterWhereClause)" + " AND " + s"(${part.whereClause})"
    } else if (part.whereClause != null) {
      "WHERE " + part.whereClause
    } else if (filterWhereClause.length > 0) {
      "WHERE " + filterWhereClause
    } else {
      ""
    }

    this
  }

  def withGroupByColumns(groupByColumns: Option[Array[String]]): JdbcSQLQueryBuilder = {
    if (groupByColumns.nonEmpty && groupByColumns.get.nonEmpty) {
      // The GROUP BY columns should already be quoted by the caller side.
      groupByClause = s"GROUP BY ${groupByColumns.get.mkString(", ")}"
    }

    this
  }

  def withSortOrders(sortOrders: Array[String]): JdbcSQLQueryBuilder = {
    if (sortOrders.nonEmpty) {
      orderByClause = s" ORDER BY ${sortOrders.mkString(", ")}"
    }

    this
  }

  def withLimit(limit: Int): JdbcSQLQueryBuilder = {
    limitClause = dialect.getLimitClause(limit)

    this
  }

  def withOffset(offset: Int): JdbcSQLQueryBuilder = {
    offsetClause = dialect.getOffsetClause(offset)

    this
  }

  def withTableSample(sample: Option[TableSampleInfo]): JdbcSQLQueryBuilder = {
    if (sample.nonEmpty) {
      tableSampleClause = dialect.getTableSample(sample.get)
    }

    this
  }

  def build(): String = {
    options.prepareQuery +
      s"SELECT $columnList FROM ${options.tableOrQuery} $tableSampleClause" +
      s" $whereClause $groupByClause $orderByClause $limitClause $offsetClause"
  }
}
