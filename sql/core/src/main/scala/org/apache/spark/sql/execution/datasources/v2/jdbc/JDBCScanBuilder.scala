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

import java.util.Optional

import scala.jdk.OptionConverters._
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.expressions.{FieldReference, SortOrder}
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.join.{JoinColumn, JoinType}
import org.apache.spark.sql.connector.read.{ScanBuilder, SupportsPushDownAggregates, SupportsPushDownJoin, SupportsPushDownLimit, SupportsPushDownOffset, SupportsPushDownRequiredColumns, SupportsPushDownTableSample, SupportsPushDownTopN, SupportsPushDownV2Filters}
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD, JDBCRelation}
import org.apache.spark.sql.execution.datasources.v2.TableSampleInfo
import org.apache.spark.sql.jdbc.JdbcDialects
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

  override def isRightSideCompatibleForJoin(other: SupportsPushDownJoin): Boolean = {
    other.isInstanceOf[JDBCScanBuilder] &&
      jdbcOptions.url == other.asInstanceOf[JDBCScanBuilder].jdbcOptions.url
  };

  override def pushJoin(
    other: SupportsPushDownJoin,
    joinType: JoinType,
    condition: Optional[Predicate],
    leftRequiredSchema: StructType,
    rightRequiredSchema: StructType
  ): Boolean = {
    if (!jdbcOptions.pushDownJoin || !dialect.supportsJoin) return false

    val leftNodeSQLQuery = buildSQLQuery()
    val rightNodeSQLQuery = other.asInstanceOf[JDBCScanBuilder].buildSQLQuery()

    val leftSideQualifier = JoinOutputAliasIterator.get
    val rightSideQualifier = JoinOutputAliasIterator.get

    val leftProjections: Seq[JoinColumn] = leftRequiredSchema.fields.map { e =>
      new JoinColumn(Array(leftSideQualifier), e.name, true)
    }.toSeq
    val rightProjections: Seq[JoinColumn] = rightRequiredSchema.fields.map { e =>
      new JoinColumn(Array(rightSideQualifier), e.name, false)
    }.toSeq

    var aliasedLeftSchema = StructType(Seq())
    var aliasedRightSchema = StructType(Seq())
    val outputAliasPrefix = JoinOutputAliasIterator.get

    val aliasedOutput = (leftProjections ++ rightProjections)
      .zipWithIndex
      .map { case (proj, i) =>
        val name = s"${outputAliasPrefix}_col_$i"
        val output = FieldReference(name)
        if (i < leftProjections.length) {
          val field = leftRequiredSchema.fields(i)
          aliasedLeftSchema =
            aliasedLeftSchema.add(name, field.dataType, field.nullable, field.metadata)
        } else {
          val field = rightRequiredSchema.fields(i - leftRequiredSchema.fields.length)
          aliasedRightSchema =
            aliasedRightSchema.add(name, field.dataType, field.nullable, field.metadata)
        }

        s"""${dialect.compileExpression(proj).get} AS ${dialect.compileExpression(output).get}"""
      }.mkString(",")

    val compiledJoinType = dialect.compileJoinType(joinType)
    if (!compiledJoinType.isDefined) return false

    val conditionString = condition.toScala match {
      case Some(cond) =>
        qualifyCondition(cond, leftSideQualifier, rightSideQualifier)
        s"ON ${dialect.compileExpression(cond).get}"
      case _ => ""
    }

    val subqueryASKeyword = if (dialect.needsASKeywordForJoinSubquery) {
      " AS "
    } else {
      ""
    }

    val compiledLeftSideQualifier =
      dialect.compileExpression(FieldReference(leftSideQualifier)).get
    val compiledRightSideQualifier =
      dialect.compileExpression(FieldReference(rightSideQualifier)).get

    val joinQuery =
      s"""
         |SELECT $aliasedOutput FROM
         |($leftNodeSQLQuery)$subqueryASKeyword$compiledLeftSideQualifier
         |${compiledJoinType.get}
         |($rightNodeSQLQuery)$subqueryASKeyword$compiledRightSideQualifier
         |$conditionString
         |""".stripMargin

    val newMap = jdbcOptions.parameters.originalMap +
      (JDBCOptions.JDBC_QUERY_STRING -> joinQuery) - (JDBCOptions.JDBC_TABLE_NAME)

    jdbcOptions = new JDBCOptions(newMap)
    jdbcOptions.containsJoinInQuery = true

    // We can merge schemas since there are no fields with duplicate names
    finalSchema = aliasedLeftSchema.merge(aliasedRightSchema)
    pushedPredicate = Array.empty[Predicate]
    pushedAggregateList = Array()
    pushedGroupBys = None
    tableSample = None
    pushedLimit = 0
    sortOrders = Array.empty[String]
    pushedOffset = 0

    true
  }

  def buildSQLQuery(): String = {
    build()
      .toV1TableScan(session.sqlContext).asInstanceOf[JDBCV1RelationFromV2Scan]
      .buildScan().asInstanceOf[JDBCRDD]
      .getExternalEngineQuery
  }

  // Fully qualify the condition. For example:
  // DEPT=SALARY turns into leftSideQualifier.DEPT = rightSideQualifier=SALARY
  def qualifyCondition(condition: Predicate, leftSideQualifier: String, rightSideQualifier: String)
  : Unit = {
    condition.references()
      .filter(_.isInstanceOf[JoinColumn])
      .foreach { e =>
        val qualifier = if (e.asInstanceOf[JoinColumn].isInLeftSideOfJoin) {
          leftSideQualifier
        } else {
          rightSideQualifier
        }

        e.asInstanceOf[JoinColumn].qualifier = Array(qualifier)
      }
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

object JoinOutputAliasIterator {
  private var curId = new java.util.concurrent.atomic.AtomicLong()

  def get: String = {
    "subquery_" + curId.getAndIncrement()
  }

  def reset(): Unit = {
    curId = new java.util.concurrent.atomic.AtomicLong()
  }
}
