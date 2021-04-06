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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.sqlpushdown.{SQLStatement, SupportsSQLPushDown}
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD, JDBCRelation}
import org.apache.spark.sql.execution.datasources.v2.pushdown.sql.{SingleCatalystStatement, SingleSQLStatement, SQLBuilder}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

case class JDBCScanBuilder(
    session: SparkSession,
    schema: StructType,
    jdbcOptions: JDBCOptions)
  extends ScanBuilder with SupportsSQLPushDown {

  private val isCaseSensitive = session.sessionState.conf.caseSensitiveAnalysis

  private var pushedFilter = Array.empty[Filter]

  private var statement: SingleSQLStatement = _

  private var prunedSchema = schema

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    if (jdbcOptions.pushDownPredicate) {
      val dialect = JdbcDialects.get(jdbcOptions.url)
      val (pushed, unSupported) = filters.partition(JDBCRDD.compileFilter(_, dialect).isDefined)
      this.pushedFilter = pushed
      unSupported
    } else {
      filters
    }
  }

  override def pushedFilters(): Array[Filter] = pushedFilter

  override def pruneColumns(requiredSchema: StructType): Unit = {
    // JDBC doesn't support nested column pruning.
    // TODO (SPARK-32593): JDBC support nested column and nested column pruning.
    val requiredCols = requiredSchema.fields.map(PartitioningUtils.getColName(_, isCaseSensitive))
      .toSet
    val fields = schema.fields.filter { field =>
      val colName = PartitioningUtils.getColName(field, isCaseSensitive)
      requiredCols.contains(colName)
    }
    prunedSchema = StructType(fields)
  }

  override def build(): Scan = {
    val resolver = session.sessionState.conf.resolver
    val timeZoneId = session.sessionState.conf.sessionLocalTimeZone
    val parts = JDBCRelation.columnPartition(schema, resolver, timeZoneId, jdbcOptions)
    val relationSchema = if (statement != null) {
      prunedSchema
    } else {
      schema
    }
    JDBCScan(JDBCRelation(relationSchema, parts, jdbcOptions)(session),
      prunedSchema, pushedFilter, statement)
  }

  private def toSQLStatement(catalystStatement: SingleCatalystStatement): SingleSQLStatement = {
    val projects = catalystStatement.projects
    val filters = catalystStatement.filters
    val groupBy = catalystStatement.groupBy
    SingleSQLStatement (
      relation = jdbcOptions.tableOrQuery,
      projects = Some(projects.map(SQLBuilder.expressionToSql(_))),
      filters = if (filters.isEmpty) None else Some(filters),
      groupBy = if (groupBy.isEmpty) None else Some(groupBy.map(SQLBuilder.expressionToSql(_))),
      url = Some(jdbcOptions.url)
    )
  }

  override def isMultiplePartitionExecution: Boolean = true

 override def pushStatement(push: SQLStatement, outputSchema: StructType): Array[Filter] = {
   statement = toSQLStatement(push.asInstanceOf[SingleCatalystStatement])
   if (outputSchema != null) {
     prunedSchema = outputSchema
   }
   statement.filters.map(f => pushFilters(f.toArray)).getOrElse(Array.empty)
 }

  override def pushedStatement(): SQLStatement = statement
}
