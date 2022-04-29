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

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.expressions.{FieldReference, SortOrder}
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownAggregates, SupportsPushDownLimit, SupportsPushDownRequiredColumns, SupportsPushDownTableSample, SupportsPushDownTopN, SupportsPushDownV2Filters}
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD, JDBCRelation}
import org.apache.spark.sql.execution.datasources.v2.TableSampleInfo
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType

case class JDBCScanBuilder(
    session: SparkSession,
    schema: StructType,
    jdbcOptions: JDBCOptions)
  extends ScanBuilder
    with SupportsPushDownV2Filters
    with SupportsPushDownRequiredColumns
    with SupportsPushDownAggregates
    with SupportsPushDownLimit
    with SupportsPushDownTableSample
    with SupportsPushDownTopN
    with Logging {

  private val isCaseSensitive = session.sessionState.conf.caseSensitiveAnalysis

  private var pushedPredicate = Array.empty[Predicate]

  private var finalSchema = schema

  private var tableSample: Option[TableSampleInfo] = None

  private var pushedLimit = 0

  private var sortOrders: Array[String] = Array.empty[String]

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    if (jdbcOptions.pushDownPredicate) {
      val dialect = JdbcDialects.get(jdbcOptions.url)
      val (pushed, unSupported) = predicates.partition(dialect.compileExpression(_).isDefined)
      this.pushedPredicate = pushed
      unSupported
    } else {
      predicates
    }
  }

  override def pushedPredicates(): Array[Predicate] = pushedPredicate

  private var pushedAggregateList: Array[String] = Array()

  private var pushedGroupBys: Option[Array[String]] = None

  override def supportCompletePushDown(aggregation: Aggregation): Boolean = {
    lazy val fieldNames = aggregation.groupByExpressions()(0) match {
      case field: FieldReference => field.fieldNames
      case _ => Array.empty[String]
    }
    jdbcOptions.numPartitions.map(_ == 1).getOrElse(true) ||
      (aggregation.groupByExpressions().length == 1 && fieldNames.length == 1 &&
        jdbcOptions.partitionColumn.exists(fieldNames(0).equalsIgnoreCase(_)))
  }

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    if (!jdbcOptions.pushDownAggregate) return false

    val dialect = JdbcDialects.get(jdbcOptions.url)
    val compiledAggs = aggregation.aggregateExpressions.flatMap(dialect.compileAggregate)
    if (compiledAggs.length != aggregation.aggregateExpressions.length) return false

    val compiledGroupBys = aggregation.groupByExpressions.flatMap(dialect.compileExpression)
    if (compiledGroupBys.length != aggregation.groupByExpressions.length) return false

    // The column names here are already quoted and can be used to build sql string directly.
    // e.g. "DEPT","NAME",MAX("SALARY"),MIN("BONUS") =>
    // SELECT "DEPT","NAME",MAX("SALARY"),MIN("BONUS") FROM "test"."employee"
    //   GROUP BY "DEPT", "NAME"
    val selectList = compiledGroupBys ++ compiledAggs
    val groupByClause = if (compiledGroupBys.isEmpty) {
      ""
    } else {
      "GROUP BY " + compiledGroupBys.mkString(",")
    }

    val aggQuery = s"SELECT ${selectList.mkString(",")} FROM ${jdbcOptions.tableOrQuery} " +
      s"WHERE 1=0 $groupByClause"
    try {
      finalSchema = JDBCRDD.getQueryOutputSchema(aggQuery, jdbcOptions, dialect)
      pushedAggregateList = selectList
      pushedGroupBys = Some(compiledGroupBys)
      true
    } catch {
      case NonFatal(e) =>
        logError("Failed to push down aggregation to JDBC", e)
        false
    }
  }

  override def pushTableSample(
      lowerBound: Double,
      upperBound: Double,
      withReplacement: Boolean,
      seed: Long): Boolean = {
    if (jdbcOptions.pushDownTableSample &&
      JdbcDialects.get(jdbcOptions.url).supportsTableSample) {
      this.tableSample = Some(TableSampleInfo(lowerBound, upperBound, withReplacement, seed))
      return true
    }
    false
  }

  override def pushLimit(limit: Int): Boolean = {
    if (jdbcOptions.pushDownLimit) {
      pushedLimit = limit
      return true
    }
    false
  }

  override def pushTopN(orders: Array[SortOrder], limit: Int): Boolean = {
    if (jdbcOptions.pushDownLimit) {
      val dialect = JdbcDialects.get(jdbcOptions.url)
      val compiledOrders = orders.flatMap { order =>
        dialect.compileExpression(order.expression())
          .map(sortKey => s"$sortKey ${order.direction()} ${order.nullOrdering()}")
      }
      if (orders.length != compiledOrders.length) return false
      pushedLimit = limit
      sortOrders = compiledOrders
      return true
    }
    false
  }

  override def isPartiallyPushed(): Boolean = jdbcOptions.numPartitions.map(_ > 1).getOrElse(false)

  override def pruneColumns(requiredSchema: StructType): Unit = {
    // JDBC doesn't support nested column pruning.
    // TODO (SPARK-32593): JDBC support nested column and nested column pruning.
    val requiredCols = requiredSchema.fields.map(PartitioningUtils.getColName(_, isCaseSensitive))
      .toSet
    val fields = schema.fields.filter { field =>
      val colName = PartitioningUtils.getColName(field, isCaseSensitive)
      requiredCols.contains(colName)
    }
    finalSchema = StructType(fields)
  }

  override def build(): Scan = {
    val resolver = session.sessionState.conf.resolver
    val timeZoneId = session.sessionState.conf.sessionLocalTimeZone
    val parts = JDBCRelation.columnPartition(schema, resolver, timeZoneId, jdbcOptions)

    // the `finalSchema` is either pruned in pushAggregation (if aggregates are
    // pushed down), or pruned in pruneColumns (in regular column pruning). These
    // two are mutual exclusive.
    // For aggregate push down case, we want to pass down the quoted column lists such as
    // "DEPT","NAME",MAX("SALARY"),MIN("BONUS"), instead of getting column names from
    // prunedSchema and quote them (will become "MAX(SALARY)", "MIN(BONUS)" and can't
    // be used in sql string.
    JDBCScan(JDBCRelation(schema, parts, jdbcOptions)(session), finalSchema, pushedPredicate,
      pushedAggregateList, pushedGroupBys, tableSample, pushedLimit, sortOrders)
  }
}
