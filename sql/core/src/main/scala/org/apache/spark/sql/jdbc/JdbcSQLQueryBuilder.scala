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
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCPartition}
import org.apache.spark.sql.execution.datasources.v2.TableSampleInfo

/**
 * The builder to build a single SELECT query.
 *
 * Note: All the `withXXX` methods will be invoked at most once. The invocation order does not
 * matter, as all these clauses follow the natural SQL order: sample the table first, then filter,
 * then group by, then sort, then offset, then limit.
 *
 * @since 3.5.0
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
   * A LIMIT value representing pushed-down limit.
   */
  protected var limit: Int = -1

  /**
   * A OFFSET value representing pushed-down offset.
   */
  protected var offset: Int = -1

  /**
   * A table sample clause representing pushed-down table sample.
   */
  protected var tableSampleClause: String = ""

  /**
   * A hint clause representing query hints.
   */
  protected val hintClause: String = {
    if (options.hint == "" || dialect.supportsHint) {
      options.hint
    } else {
      throw QueryExecutionErrors.hintUnsupportedForJdbcDialectError(
        dialect.getClass.getSimpleName)
    }
  }

  /**
   * The columns names that following dialect's SQL syntax.
   * e.g. The column name is the raw name or quoted name.
   */
  def withColumns(columns: Array[String]): JdbcSQLQueryBuilder = {
    if (columns.nonEmpty) {
      columnList = columns.mkString(",")
    }
    this
  }

  /**
   * Constructs the WHERE clause that following dialect's SQL syntax.
   */
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

  /**
   * Constructs the GROUP BY clause that following dialect's SQL syntax.
   */
  def withGroupByColumns(groupByColumns: Array[String]): JdbcSQLQueryBuilder = {
    if (groupByColumns.nonEmpty) {
      // The GROUP BY columns should already be quoted by the caller side.
      groupByClause = s"GROUP BY ${groupByColumns.mkString(", ")}"
    }

    this
  }

  /**
   * Constructs the ORDER BY clause that following dialect's SQL syntax.
   */
  def withSortOrders(sortOrders: Array[String]): JdbcSQLQueryBuilder = {
    if (sortOrders.nonEmpty) {
      orderByClause = s" ORDER BY ${sortOrders.mkString(", ")}"
    }

    this
  }

  /**
   * Saves the limit value used to construct LIMIT clause.
   */
  def withLimit(limit: Int): JdbcSQLQueryBuilder = {
    this.limit = limit

    this
  }

  /**
   * Saves the offset value used to construct OFFSET clause.
   */
  def withOffset(offset: Int): JdbcSQLQueryBuilder = {
    this.offset = offset

    this
  }

  /**
   * Constructs the table sample clause that following dialect's SQL syntax.
   */
  def withTableSample(sample: TableSampleInfo): JdbcSQLQueryBuilder = {
    tableSampleClause = dialect.getTableSample(sample)

    this
  }

  /**
   * Build the final SQL query that following dialect's SQL syntax.
   */
  def build(): String = {
    // Constructs the LIMIT clause that following dialect's SQL syntax.
    val limitClause = dialect.getLimitClause(limit)
    // Constructs the OFFSET clause that following dialect's SQL syntax.
    val offsetClause = dialect.getOffsetClause(offset)

    options.prepareQuery +
      s"SELECT $hintClause$columnList FROM ${options.tableOrQuery} $tableSampleClause" +
      s" $whereClause $groupByClause $orderByClause $limitClause $offsetClause"
  }
}
