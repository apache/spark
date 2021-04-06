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
package org.apache.spark.sql.execution.datasources.v2.pushdown.sql

import java.util.Locale

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, NamedExpression}
import org.apache.spark.sql.connector.read.sqlpushdown.SQLStatement
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.Filter

/**
 * Builds a pushed `SELECT` query with optional WHERE and GROUP BY clauses.
 *
 * @param relation Table name, join clause or subquery for the FROM clause.
 * @param projects List of fields for projection as strings.
 * @param filters List of filters for the WHERE clause (can be empty).
 * @param groupBy List if expressions for the GROUP BY clause (can be empty).
 */
case class SingleSQLStatement(
    relation: String,
    projects: Option[Seq[String]],
    filters: Option[Seq[sources.Filter]],
    groupBy: Option[Seq[String]],
    url: Option[String] = None) extends SQLStatement {

  /**
   * `columns`, but as a String suitable for injection into a SQL query.
   *
   * The optimizer sometimes does not report any fields (since no specific is required by
   * the query (usually a nested select), thus we add the group by clauses as fields
   */
  lazy val columnList: String = projects
    .map(_.mkString(", "))
    .getOrElse(groupBy.map(_.mkString(", ")).getOrElse("1"))

  /**
   * `filters`, but as a WHERE clause suitable for injection into a SQL query.
   */
  lazy val filterWhereClause: String =
    filters.getOrElse(Seq.empty)
      .flatMap(JDBCRDD.compileFilter(_, JdbcDialects.get(url.getOrElse("Unknown URL"))))
      .map(p => s"($p)").mkString(" AND ")

  lazy val groupByStr: String = groupBy.map(g => s" GROUP BY ${g.mkString(", ")}").getOrElse("")

  /**
   * A WHERE clause representing both `filters`, if any, and the current partition.
   */
  private def getWhereClause(extraFilter: String): String = {
    if (extraFilter != null && filterWhereClause.nonEmpty) {
      "WHERE " + s"($filterWhereClause)" + " AND " + s"($extraFilter)"
    } else if (extraFilter != null) {
      "WHERE " + extraFilter
    } else if (filterWhereClause.nonEmpty) {
      "WHERE " + filterWhereClause
    } else {
      ""
    }
  }

  def toSQL(extraFilter: String = null): String = {
    val myWhereClause = getWhereClause(extraFilter)
    s"SELECT $columnList FROM $relation $myWhereClause $groupByStr"
  }
}

object SingleSQLStatement {
  def apply(
      projects: Array[String],
      filters: Array[Filter],
      jdbcOptions: JDBCOptions): SingleSQLStatement = {
    val url = jdbcOptions.url
    val dialect = JdbcDialects.get(url)
    val newProjects: Option[Seq[String]] = if (projects.isEmpty) {
      None
    } else {
      Some(projects.map(colName => dialect.quoteIdentifier(colName)))
    }

    SingleSQLStatement(
      relation = jdbcOptions.tableOrQuery,
      projects = newProjects,
      filters = if (filters.isEmpty) None else Some(filters),
      groupBy = None,
      url = Some(url)
    )
  }
}

/**
 * It's `ScanBuilder`'s duty to translate [[SingleCatalystStatement]] into [[SingleSQLStatement]]
 */
case class SingleCatalystStatement(
  relation: DataSourceV2Relation,
  projects: Seq[NamedExpression],
  filters: Seq[sources.Filter],
  groupBy: Seq[NamedExpression]) extends SQLStatement {
}

object SingleCatalystStatement {
  // TODO: get from configuration
  val isCaseSensitive = false

  private def getColumnName(columnName: String, caseSensitive: Boolean): String = {
    if (caseSensitive) {
      columnName
    } else {
      columnName.toUpperCase(Locale.ROOT)
    }
  }

  private def verifyPushedColumn(
       nameExpr: NamedExpression,
       colSet: Set[String]): Unit = {
    nameExpr
      .collect{ case att@AttributeReference(_, _, _, _) => att }
      .foreach{ attr =>
        if (!colSet.contains(getColumnName(attr.name, isCaseSensitive))) {
          // TODO: report exception
          throw new UnsupportedOperationException
        }
      }
  }

  def of(
      relation: DataSourceV2Relation,
      projects: Seq[NamedExpression],
      filters: Seq[sources.Filter],
      groupBy: Seq[NamedExpression]
  ): SingleCatalystStatement = {
    val schemaSet = relation.schema
      .map(s => getColumnName(s.name, isCaseSensitive))
      .toSet
    projects.foreach(verifyPushedColumn(_, schemaSet))
    groupBy.foreach(verifyPushedColumn(_, schemaSet))
    SingleCatalystStatement(relation, projects, filters, groupBy)
  }
}
