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

import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.sequenceOption

class SQLBuilder(logicalPlan: LogicalPlan, sqlContext: SQLContext) extends Logging {
  def toSQL: Option[String] = toSQL(logicalPlan)

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
      val aliasSQL = alias.map(a => s" `$a`").getOrElse("")
      Some(s"`$database`.`$table`$aliasSQL")

    case plan @ Sort(orders, global, child) =>
      for {
        childSQL <- toSQL(child)
        ordersSQL <- sequenceOption(orders.map { case SortOrder(e, dir) =>
          e.sql.map(sql => s"$sql ${dir.sql}")
        })
        orderOrSort = if (global) "ORDER" else "SORT"
      } yield s"$childSQL $orderOrSort BY ${ordersSQL.mkString(", ")}"

    case _ => None
  }
}
