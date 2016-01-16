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

package org.apache.spark.sql.hive

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.optimizer.ProjectCollapsing
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 * A builder class used to convert a resolved logical plan into a SQL query string.  Note that this
 * all resolved logical plan are convertible.  They either don't have corresponding SQL
 * representations (e.g. logical plans that operate on local Scala collections), or are simply not
 * supported by this builder (yet).
 */
class SQLBuilder(logicalPlan: LogicalPlan, sqlContext: SQLContext) extends Logging {
  def this(df: DataFrame) = this(df.queryExecution.analyzed, df.sqlContext)

  def toSQL: Option[String] = {
    val canonicalizedPlan = Canonicalizer.execute(logicalPlan)
    val maybeSQL = try {
      toSQL(canonicalizedPlan)
    } catch { case cause: UnsupportedOperationException =>
      logInfo(s"Failed to build SQL query string because: ${cause.getMessage}")
      None
    }

    if (maybeSQL.isDefined) {
      logDebug(
        s"""Built SQL query string successfully from given logical plan:
           |
           |# Original logical plan:
           |${logicalPlan.treeString}
           |# Canonicalized logical plan:
           |${canonicalizedPlan.treeString}
           |# Built SQL query string:
           |${maybeSQL.get}
         """.stripMargin)
    } else {
      logDebug(
        s"""Failed to build SQL query string from given logical plan:
           |
           |# Original logical plan:
           |${logicalPlan.treeString}
           |# Canonicalized logical plan:
           |${canonicalizedPlan.treeString}
         """.stripMargin)
    }

    maybeSQL
  }

  private def projectToSQL(
      projectList: Seq[NamedExpression],
      child: LogicalPlan,
      isDistinct: Boolean): Option[String] = {
    for {
      childSQL <- toSQL(child)
      listSQL = projectList.map(_.sql).mkString(", ")
      maybeFrom = child match {
        case OneRowRelation => " "
        case _ => " FROM "
      }
      distinct = if (isDistinct) " DISTINCT " else " "
    } yield s"SELECT$distinct$listSQL$maybeFrom$childSQL"
  }

  private def aggregateToSQL(
      groupingExprs: Seq[Expression],
      aggExprs: Seq[Expression],
      child: LogicalPlan): Option[String] = {
    val aggSQL = aggExprs.map(_.sql).mkString(", ")
    val groupingSQL = groupingExprs.map(_.sql).mkString(", ")
    val maybeGroupBy = if (groupingSQL.isEmpty) "" else " GROUP BY "
    val maybeFrom = child match {
      case OneRowRelation => " "
      case _ => " FROM "
    }

    toSQL(child).map { childSQL =>
      s"SELECT $aggSQL$maybeFrom$childSQL$maybeGroupBy$groupingSQL"
    }
  }

  private def toSQL(node: LogicalPlan): Option[String] = node match {
    case Distinct(Project(list, child)) =>
      projectToSQL(list, child, isDistinct = true)

    case Project(list, child) =>
      projectToSQL(list, child, isDistinct = false)

    case Aggregate(groupingExprs, aggExprs, child) =>
      aggregateToSQL(groupingExprs, aggExprs, child)

    case Limit(limit, child) =>
      for {
        childSQL <- toSQL(child)
        limitSQL = limit.sql
      } yield s"$childSQL LIMIT $limitSQL"

    case Filter(condition, child) =>
      for {
        childSQL <- toSQL(child)
        whereOrHaving = child match {
          case _: Aggregate => "HAVING"
          case _ => "WHERE"
        }
        conditionSQL = condition.sql
      } yield s"$childSQL $whereOrHaving $conditionSQL"

    case Union(left, right) =>
      for {
        leftSQL <- toSQL(left)
        rightSQL <- toSQL(right)
      } yield s"$leftSQL UNION ALL $rightSQL"

    // Persisted data source relation
    case Subquery(alias, LogicalRelation(_, _, Some(TableIdentifier(table, Some(database))))) =>
      Some(s"`$database`.`$table`")

    case Subquery(alias, child) =>
      toSQL(child).map(childSQL => s"($childSQL) AS $alias")

    case Join(left, right, joinType, condition) =>
      for {
        leftSQL <- toSQL(left)
        rightSQL <- toSQL(right)
        joinTypeSQL = joinType.sql
        conditionSQL = condition.map(" ON " + _.sql).getOrElse("")
      } yield s"$leftSQL $joinTypeSQL JOIN $rightSQL$conditionSQL"

    case MetastoreRelation(database, table, alias) =>
      val aliasSQL = alias.map(a => s" AS `$a`").getOrElse("")
      Some(s"`$database`.`$table`$aliasSQL")

    case Sort(orders, _, RepartitionByExpression(partitionExprs, child, _))
        if orders.map(_.child) == partitionExprs =>
      for {
        childSQL <- toSQL(child)
        partitionExprsSQL = partitionExprs.map(_.sql).mkString(", ")
      } yield s"$childSQL CLUSTER BY $partitionExprsSQL"

    case Sort(orders, global, child) =>
      for {
        childSQL <- toSQL(child)
        ordersSQL = orders.map { case SortOrder(e, dir) => s"${e.sql} ${dir.sql}" }.mkString(", ")
        orderOrSort = if (global) "ORDER" else "SORT"
      } yield s"$childSQL $orderOrSort BY $ordersSQL"

    case RepartitionByExpression(partitionExprs, child, _) =>
      for {
        childSQL <- toSQL(child)
        partitionExprsSQL = partitionExprs.map(_.sql).mkString(", ")
      } yield s"$childSQL DISTRIBUTE BY $partitionExprsSQL"

    case OneRowRelation =>
      Some("")

    case _ => None
  }

  object Canonicalizer extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] = Seq(
      Batch("Canonicalizer", FixedPoint(100),
        // The `WidenSetOperationTypes` analysis rule may introduce extra `Project`s over
        // `Aggregate`s to perform type casting.  This rule merges these `Project`s into
        // `Aggregate`s.
        ProjectCollapsing,

        // Used to handle other auxiliary `Project`s added by analyzer (e.g.
        // `ResolveAggregateFunctions` rule)
        RecoverScopingInfo
      )
    )

    object RecoverScopingInfo extends Rule[LogicalPlan] {
      override def apply(tree: LogicalPlan): LogicalPlan = tree transform {
        // This branch handles aggregate functions within HAVING clauses.  For example:
        //
        //   SELECT key FROM src GROUP BY key HAVING max(value) > "val_255"
        //
        // This kind of query results in query plans of the following form because of analysis rule
        // `ResolveAggregateFunctions`:
        //
        //   Project ...
        //    +- Filter ...
        //        +- Aggregate ...
        //            +- MetastoreRelation default, src, None
        case plan @ Project(_, Filter(_, _: Aggregate)) =>
          wrapChildWithSubquery(plan)

        case plan @ Project(_,
          _: Subquery | _: Filter | _: Join | _: MetastoreRelation | OneRowRelation | _: Limit
        ) => plan

        case plan: Project =>
          wrapChildWithSubquery(plan)
      }

      def wrapChildWithSubquery(project: Project): Project = project match {
        case Project(projectList, child) =>
          val alias = SQLBuilder.newSubqueryName
          val childAttributes = child.outputSet
          val aliasedProjectList = projectList.map(_.transform {
            case a: Attribute if childAttributes.contains(a) =>
              a.withQualifiers(alias :: Nil)
          }.asInstanceOf[NamedExpression])

          Project(aliasedProjectList, Subquery(alias, child))
      }
    }
  }
}

object SQLBuilder {
  private val nextSubqueryId = new AtomicLong(0)

  private def newSubqueryName: String = s"gen_subquery_${nextSubqueryId.getAndIncrement()}"
}
