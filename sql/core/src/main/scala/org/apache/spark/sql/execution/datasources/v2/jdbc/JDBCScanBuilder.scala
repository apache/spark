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
import org.apache.spark.sql.connector.join.JoinType
import org.apache.spark.sql.connector.read.{ScanBuilder, SupportsPushDownAggregates, SupportsPushDownJoin, SupportsPushDownLimit, SupportsPushDownOffset, SupportsPushDownRequiredColumns, SupportsPushDownTableSample, SupportsPushDownTopN, SupportsPushDownV2Filters}
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCPartition, JDBCRDD, JDBCRelation}
import org.apache.spark.sql.execution.datasources.v2.TableSampleInfo
import org.apache.spark.sql.jdbc.{JdbcDialects, JdbcSQLQueryBuilder}
import org.apache.spark.sql.types.StructType

case class JDBCScanBuilder(
    session: SparkSession,
    schema: StructType,
    var jdbcOptions: JDBCOptions)
  extends ScanBuilder
    with SupportsPushDownV2Filters
    with SupportsPushDownRequiredColumns
    with SupportsPushDownAggregates
    with SupportsPushDownLimit
    with SupportsPushDownOffset
    with SupportsPushDownTableSample
    with SupportsPushDownTopN
    with SupportsPushDownJoin
    with Logging {

  private val dialect = JdbcDialects.get(jdbcOptions.url)

  private val isCaseSensitive = session.sessionState.conf.caseSensitiveAnalysis

  private var pushedPredicate = Array.empty[Predicate]

  private var finalSchema = schema

  private var tableSample: Option[TableSampleInfo] = None

  private var pushedLimit = 0

  private var pushedOffset = 0

  private var sortOrders: Array[String] = Array.empty[String]

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    if (jdbcOptions.pushDownPredicate) {
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

    val compiledAggs = aggregation.aggregateExpressions.flatMap(dialect.compileExpression)
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

    val aggQuery = jdbcOptions.prepareQuery +
      s"SELECT ${selectList.mkString(",")} FROM ${jdbcOptions.tableOrQuery} " +
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

  // TODO: currently we check that all the options are same (besides dbtable and query options).
  // That is too strict, so in the future we should relax this check by asserting only specific
  // options are some (e.g. host, port, username, password, database...).
  // Also, we need to check if join is done on 2 tables from 2 different databases within same
  // host. These shouldn't be allowed.
  override def isOtherSideCompatibleForJoin(other: SupportsPushDownJoin): Boolean = {
    if (!jdbcOptions.pushDownJoin ||
        !dialect.supportsJoin ||
        !other.isInstanceOf[JDBCScanBuilder]) {
      return false
    }

    val filteredJDBCOptions = jdbcOptions.parameters -
      JDBCOptions.JDBC_TABLE_NAME -
      JDBCOptions.JDBC_QUERY_STRING

    val otherSideFilteredJDBCOptions = other.asInstanceOf[JDBCScanBuilder].jdbcOptions.parameters -
      JDBCOptions.JDBC_TABLE_NAME -
      JDBCOptions.JDBC_QUERY_STRING

    filteredJDBCOptions == otherSideFilteredJDBCOptions
  };

  /**
   * Helper method to calculate StructType based on the SupportsPushDownJoin.ColumnWithAlias and
   * the given schema.
   *
   * If ColumnWithAlias object has defined alias, new field with new name being equal to alias
   * should be returned. Otherwise, original field is returned.
   */
  private def calculateJoinOutputSchema(
      columnsWithAliases: Array[SupportsPushDownJoin.ColumnWithAlias],
      schema: StructType): StructType = {
    var newSchema = StructType(Seq())
    columnsWithAliases.foreach { columnWithAlias =>
      val colName = columnWithAlias.colName()
      val alias = columnWithAlias.alias()
      val field = schema(colName)

      if (alias == null) {
        newSchema = newSchema.add(field)
      } else {
        newSchema = newSchema.add(alias, field.dataType, field.nullable, field.metadata)
      }
    }

    newSchema
  }

  override def pushDownJoin(
      other: SupportsPushDownJoin,
      joinType: JoinType,
      leftSideRequiredColumnsWithAliases: Array[SupportsPushDownJoin.ColumnWithAlias],
      rightSideRequiredColumnsWithAliases: Array[SupportsPushDownJoin.ColumnWithAlias],
      condition: Predicate ): Boolean = {
    if (!jdbcOptions.pushDownJoin || !dialect.supportsJoin) {
      return false
    }

    val joinTypeStringOption = joinType match {
      case JoinType.INNER_JOIN => Some("INNER JOIN")
      case _ => None
    }
    if (!joinTypeStringOption.isDefined) {
      return false
    }

    val compiledCondition = dialect.compileExpression(condition)
    if (!compiledCondition.isDefined) {
      return false
    }

    val otherJdbcScanBuilder = other.asInstanceOf[JDBCScanBuilder]

    // requiredSchema will become the finalSchema of this JDBCScanBuilder
    var requiredSchema = StructType(Seq())
    requiredSchema = calculateJoinOutputSchema(leftSideRequiredColumnsWithAliases, finalSchema)
    requiredSchema = requiredSchema.merge(
      calculateJoinOutputSchema(
        rightSideRequiredColumnsWithAliases,
        otherJdbcScanBuilder.finalSchema
      )
    )

    val joinOutputColumns = requiredSchema.fields.map(f => dialect.quoteIdentifier(f.name))
    val conditionString = compiledCondition.get

    // Get left side and right side of join sql query builders and recursively build them when
    // crafting join sql query.
    val leftSideJdbcSQLBuilder = getJoinPushdownJdbcSQLBuilder(leftSideRequiredColumnsWithAliases)
    val otherSideJdbcSQLBuilder = otherJdbcScanBuilder
      .getJoinPushdownJdbcSQLBuilder(rightSideRequiredColumnsWithAliases)

    val joinQuery = dialect
      .getJdbcSQLQueryBuilder(jdbcOptions)
      .withJoin(
        leftSideJdbcSQLBuilder,
        otherSideJdbcSQLBuilder,
        JoinPushdownAliasGenerator.getSubqueryQualifier,
        JoinPushdownAliasGenerator.getSubqueryQualifier,
        joinOutputColumns,
        joinTypeStringOption.get,
        conditionString
      )
      .build()

    val newJdbcOptionsMap = jdbcOptions.parameters.originalMap +
      (JDBCOptions.JDBC_QUERY_STRING -> joinQuery) - JDBCOptions.JDBC_TABLE_NAME

    jdbcOptions = new JDBCOptions(newJdbcOptionsMap)
    finalSchema = requiredSchema

    // We need to reset the pushedPredicate because it has already been consumed in previously
    // crafted SQL query.
    pushedPredicate = Array.empty[Predicate]
    // Table sample is pushed down already as well, so we need to reset it to None to not push it
    // down again when join pushdown is triggered again on this JDBCScanBuilder.
    tableSample = None

    true
  }

  def getJoinPushdownJdbcSQLBuilder(
      columnsWithAliases: Array[SupportsPushDownJoin.ColumnWithAlias]): JdbcSQLQueryBuilder = {
    val quotedColumns = columnsWithAliases.map(col => dialect.quoteIdentifier(col.colName()))
    val quotedAliases = columnsWithAliases
      .map(col => Option(col.alias()).map(dialect.quoteIdentifier))

    // Only filters can be pushed down before join pushdown, so we need to craft SQL query
    // that contains filters as well.
    // Joins on top of samples are not supported so we don't need to provide tableSample here.
    dialect
      .getJdbcSQLQueryBuilder(jdbcOptions)
      .withPredicates(pushedPredicate, JDBCPartition(whereClause = null, idx = 1))
      .withAliasedColumns(quotedColumns, quotedAliases)
  }

  override def pushTableSample(
      lowerBound: Double,
      upperBound: Double,
      withReplacement: Boolean,
      seed: Long): Boolean = {
    if (jdbcOptions.pushDownTableSample && dialect.supportsTableSample) {
      this.tableSample = Some(TableSampleInfo(lowerBound, upperBound, withReplacement, seed))
      return true
    }
    false
  }

  override def pushLimit(limit: Int): Boolean = {
    if (jdbcOptions.pushDownLimit && dialect.supportsLimit) {
      pushedLimit = limit
      return true
    }
    false
  }

  override def pushOffset(offset: Int): Boolean = {
    if (jdbcOptions.pushDownOffset && !isPartiallyPushed && dialect.supportsOffset) {
      // Spark pushes down LIMIT first, then OFFSET. In SQL statements, OFFSET is applied before
      // LIMIT. Here we need to adjust the LIMIT value to match SQL statements.
      if (pushedLimit > 0) {
        pushedLimit = pushedLimit - offset
      }
      pushedOffset = offset
      return true
    }
    false
  }

  override def pushTopN(orders: Array[SortOrder], limit: Int): Boolean = {
    if (jdbcOptions.pushDownLimit) {
      val compiledOrders = orders.flatMap(dialect.compileExpression(_))
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

  override def build(): JDBCScan = {
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
      pushedAggregateList, pushedGroupBys, tableSample, pushedLimit, sortOrders, pushedOffset)
  }

}

object JoinPushdownAliasGenerator {
  private val subQueryId = new java.util.concurrent.atomic.AtomicLong()

  def getSubqueryQualifier: String = {
    "join_subquery_" + subQueryId.getAndIncrement()
  }
}
