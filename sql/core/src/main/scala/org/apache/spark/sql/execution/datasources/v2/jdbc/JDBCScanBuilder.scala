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
import org.apache.spark.sql.connector.expressions.{Aggregation, Count, CountOne, FieldReference, Max, Min, Sum}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownAggregates, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD, JDBCRelation}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{LongType, StructField, StructType}

case class JDBCScanBuilder(
    session: SparkSession,
    schema: StructType,
    jdbcOptions: JDBCOptions)
  extends ScanBuilder with SupportsPushDownFilters with SupportsPushDownRequiredColumns
    with SupportsPushDownAggregates{

  private val isCaseSensitive = session.sessionState.conf.caseSensitiveAnalysis

  private var pushedFilter = Array.empty[Filter]

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

  private var pushedAggregations = Option.empty[Aggregation]

  private var pushedAggregateColumn: Array[String] = Array()

  private def getStructFieldForCol(col: FieldReference): StructField =
    schema.fields(schema.fieldNames.toList.indexOf(col.fieldNames.head))

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    if (!jdbcOptions.pushDownAggregate) return false

    val dialect = JdbcDialects.get(jdbcOptions.url)
    val compiledAgg = JDBCRDD.compileAggregates(aggregation.getAggregateExpressions, dialect)
    // if any of the aggregates is not supported by the data source, not push down
    if (compiledAgg.length != aggregation.getAggregateExpressions.size) return false

    var pushedSchema = new StructType()
    aggregation.getGroupByColumns.foreach { col =>
      val structField = getStructFieldForCol(col)
      pushedSchema = pushedSchema.add(StructField(structField.name, structField.dataType))
      pushedAggregateColumn = pushedAggregateColumn :+ dialect.quoteIdentifier(structField.name)
    }

    // The column names here are already quoted and can be used to build sql string directly.
    // e.g. "DEPT","NAME",MAX("SALARY"),MIN("BONUS") =>
    // SELECT "DEPT","NAME",MAX("SALARY"),MIN("BONUS") FROM "test"."employee"
    //   GROUP BY "DEPT", "NAME"
    pushedAggregateColumn = pushedAggregateColumn ++ compiledAgg

    aggregation.getAggregateExpressions.foreach {
      case Max(col) =>
        val structField = getStructFieldForCol(col)
        pushedSchema = pushedSchema.add(structField.copy("max(" + structField.name + ")"))
      case Min(col) =>
        val structField = getStructFieldForCol(col)
        pushedSchema = pushedSchema.add(structField.copy("min(" + structField.name + ")"))
      case Count(col, isDistinct) =>
        val distinct = if (isDistinct) "DISTINCT " else ""
        val structField = getStructFieldForCol(col)
        pushedSchema =
          pushedSchema.add(StructField(s"count($distinct" + structField.name + ")", LongType))
      case CountOne() =>
          pushedSchema = pushedSchema.add(StructField("count(*)", LongType))
      case Sum(col, dataType, isDistinct) =>
        val distinct = if (isDistinct) "DISTINCT " else ""
        val structField = getStructFieldForCol(col)
        pushedSchema =
          pushedSchema.add(StructField(s"sum($distinct" + structField.name + ")", dataType))
      case _ => return false
    }
    this.pushedAggregations = Some(aggregation)
    prunedSchema = pushedSchema
    true
  }

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

    // in prunedSchema, the schema is either pruned in pushAggregation (if aggregates are
    // pushed down), or pruned in pruneColumns (in regular column pruning). These
    // two are mutual exclusive.
    // For aggregate push down case, we want to pass down the quoted column lists such as
    // "DEPT","NAME",MAX("SALARY"),MIN("BONUS"), instead of getting column names from
    // prunedSchema and quote them (will become "MAX(SALARY)", "MIN(BONUS)" and can't
    // be used in sql string.
    JDBCScan(JDBCRelation(schema, parts, jdbcOptions)(session), prunedSchema, pushedFilter,
      pushedAggregations, pushedAggregateColumn)
  }
}
