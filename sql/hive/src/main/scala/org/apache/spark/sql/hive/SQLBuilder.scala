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

import scala.util.control.NonFatal

import org.apache.spark.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.optimizer.CollapseProject
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

  def toSQL: String = {
    val canonicalizedPlan = Canonicalizer.execute(logicalPlan)
    try {
      val generatedSQL = toSQL(canonicalizedPlan)
      logDebug(
        s"""Built SQL query string successfully from given logical plan:
           |
           |# Original logical plan:
           |${logicalPlan.treeString}
           |# Canonicalized logical plan:
           |${canonicalizedPlan.treeString}
           |# Generated SQL:
           |$generatedSQL
         """.stripMargin)
      generatedSQL
    } catch { case NonFatal(e) =>
      logDebug(
        s"""Failed to build SQL query string from given logical plan:
           |
           |# Original logical plan:
           |${logicalPlan.treeString}
           |# Canonicalized logical plan:
           |${canonicalizedPlan.treeString}
         """.stripMargin)
      throw e
    }
  }

  private def toSQL(node: LogicalPlan): String = node match {
    case Distinct(p: Project) =>
      projectToSQL(p, isDistinct = true)

    case p: Project =>
      projectToSQL(p, isDistinct = false)

    case p: Aggregate =>
      aggregateToSQL(p)

    case Limit(limitExpr, child) =>
      s"${toSQL(child)} LIMIT ${limitExpr.sql}"

    case p: Filter =>
      val whereOrHaving = p.child match {
        case _: Aggregate => "HAVING"
        case _ => "WHERE"
      }
      build(toSQL(p.child), whereOrHaving, p.condition.sql)

    case p: Union if p.children.length > 1 =>
      val childrenSql = p.children.map(toSQL(_))
      childrenSql.mkString(" UNION ALL ")

    case p: Subquery =>
      p.child match {
        // Persisted data source relation
        case LogicalRelation(_, _, Some(TableIdentifier(table, Some(database)))) =>
          s"`$database`.`$table`"
        // Parentheses is not used for persisted data source relations
        // e.g., select x.c1 from (t1) as x inner join (t1) as y on x.c1 = y.c1
        case Subquery(_, _: LogicalRelation | _: MetastoreRelation) =>
          build(toSQL(p.child), "AS", p.alias)
        case _ =>
          build("(" + toSQL(p.child) + ")", "AS", p.alias)
      }

    case p: Join =>
      build(
        toSQL(p.left),
        p.joinType.sql,
        "JOIN",
        toSQL(p.right),
        p.condition.map(" ON " + _.sql).getOrElse(""))

    case p: MetastoreRelation =>
      build(
        s"`${p.databaseName}`.`${p.tableName}`",
        p.alias.map(a => s" AS `$a`").getOrElse("")
      )

    case Sort(orders, _, RepartitionByExpression(partitionExprs, child, _))
        if orders.map(_.child) == partitionExprs =>
      build(toSQL(child), "CLUSTER BY", partitionExprs.map(_.sql).mkString(", "))

    case p: Sort =>
      build(
        toSQL(p.child),
        if (p.global) "ORDER BY" else "SORT BY",
        p.order.map { case SortOrder(e, dir) => s"${e.sql} ${dir.sql}" }.mkString(", ")
      )

    case p: RepartitionByExpression =>
      build(
        toSQL(p.child),
        "DISTRIBUTE BY",
        p.partitionExpressions.map(_.sql).mkString(", ")
      )

    case OneRowRelation =>
      ""

    case _ =>
      throw new UnsupportedOperationException(s"unsupported plan $node")
  }

  /**
   * Turns a bunch of string segments into a single string and separate each segment by a space.
   * The segments are trimmed so only a single space appears in the separation.
   * For example, `build("a", " b ", " c")` becomes "a b c".
   */
  private def build(segments: String*): String = segments.map(_.trim).mkString(" ")

  private def projectToSQL(plan: Project, isDistinct: Boolean): String = {
    build(
      "SELECT",
      if (isDistinct) "DISTINCT" else "",
      plan.projectList.map(_.sql).mkString(", "),
      if (plan.child == OneRowRelation) "" else "FROM",
      toSQL(plan.child)
    )
  }

  private def aggregateToSQL(plan: Aggregate): String = {
    val groupingSQL = plan.groupingExpressions.map(_.sql).mkString(", ")
    build(
      "SELECT",
      plan.aggregateExpressions.map(_.sql).mkString(", "),
      if (plan.child == OneRowRelation) "" else "FROM",
      toSQL(plan.child),
      if (groupingSQL.isEmpty) "" else "GROUP BY",
      groupingSQL
    )
  }

  object Canonicalizer extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] = Seq(
      Batch("Canonicalizer", FixedPoint(100),
        // The `WidenSetOperationTypes` analysis rule may introduce extra `Project`s over
        // `Aggregate`s to perform type casting.  This rule merges these `Project`s into
        // `Aggregate`s.
        CollapseProject,

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
          _: Subquery
            | _: Filter
            | _: Join
            | _: MetastoreRelation
            | OneRowRelation
            | _: LocalLimit
            | _: GlobalLimit
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
