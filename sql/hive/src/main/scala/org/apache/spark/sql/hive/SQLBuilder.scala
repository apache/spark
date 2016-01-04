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
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.{NamedExpression, Attribute, SortOrder}
import org.apache.spark.sql.catalyst.optimizer.ProjectCollapsing
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.catalyst.util.sequenceOption

class SQLBuilder(logicalPlan: LogicalPlan, sqlContext: SQLContext) extends Logging {
  def toSQL: Option[String] = {
    val canonicalizedPlan = Canonicalizer.execute(logicalPlan)

    logDebug(
      s"""Building SQL query string from given logical plan:
         |
         |# Original logical plan:
         |${logicalPlan.treeString}
         |# Canonicalized logical plan:
         |${canonicalizedPlan.treeString}
       """.stripMargin)

    toSQL(canonicalizedPlan)
  }

  private def toSQL(node: LogicalPlan): Option[String] = node match {
    case plan @ Project(list, child) =>
      for {
        listSQL <- sequenceOption(list.map(_.sql))
        childSQL <- toSQL(child)
        from = child match {
          case OneRowRelation => ""
          case _ => " FROM "
        }
      } yield s"SELECT ${listSQL.mkString(", ")}$from$childSQL"

    case plan @ Aggregate(groupingExpressions, aggregateExpressions, child) =>
      for {
        aggregateSQL <- sequenceOption(aggregateExpressions.map(_.sql))
        groupingSQL <- sequenceOption(groupingExpressions.map(_.sql))
        maybeGroupBy = if (groupingSQL.isEmpty) "" else " GROUP BY "
        maybeFrom = child match {
          case OneRowRelation => ""
          case _ => " FROM "
        }
        childSQL <- toSQL(child).map(maybeFrom + _)
      } yield {
        s"SELECT ${aggregateSQL.mkString(", ")}$childSQL$maybeGroupBy${groupingSQL.mkString(", ")}"
      }

    case plan @ Limit(limit, child) =>
      for {
        limitSQL <- limit.sql
        childSQL <- toSQL(child)
      } yield s"$childSQL LIMIT $limitSQL"

    case plan @ Filter(condition, child) =>
      for {
        conditionSQL <- condition.sql
        childSQL <- toSQL(child)
        whereOrHaving = child match {
          case _: Aggregate => "HAVING"
          case _ => "WHERE"
        }
      } yield s"$childSQL $whereOrHaving $conditionSQL"

    case plan @ Union(left, right) =>
      for {
        leftSQL <- toSQL(left)
        rightSQL <- toSQL(right)
      } yield s"$leftSQL UNION ALL $rightSQL"

    case plan @ Subquery(alias, child) =>
      toSQL(child).map(childSQL => s"($childSQL) AS $alias")

    case plan @ Join(left, right, joinType, condition) =>
      for {
        leftSQL <- toSQL(left)
        rightSQL <- toSQL(right)
        joinTypeSQL = joinType.sql
        conditionSQL = condition.flatMap(_.sql).map(" ON " + _).getOrElse("")
      } yield s"$leftSQL $joinTypeSQL JOIN $rightSQL$conditionSQL"

    case plan @ MetastoreRelation(database, table, alias) =>
      val aliasSQL = alias.map(a => s" AS `$a`").getOrElse("")
      Some(s"`$database`.`$table`$aliasSQL")

    case plan @ Sort(orders, global, child) =>
      for {
        childSQL <- toSQL(child)
        ordersSQL <- sequenceOption(orders.map { case SortOrder(e, dir) =>
          e.sql.map(sql => s"$sql ${dir.sql}")
        })
        orderOrSort = if (global) "ORDER" else "SORT"
      } yield s"$childSQL $orderOrSort BY ${ordersSQL.mkString(", ")}"

    case OneRowRelation =>
      Some("")

    case _ => None
  }

  object Canonicalizer extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] = Seq(
      Batch("Normalizer", FixedPoint(100),
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

  private def newSubqueryName: String = s"subquery_${nextSubqueryId.getAndIncrement()}__"
}
