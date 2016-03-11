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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.CollapseProject
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{ByteType, DataType, IntegerType, NullType}

/**
 * A place holder for generated SQL for subquery expression.
 */
case class SubqueryHolder(query: String) extends LeafExpression with Unevaluable {
  override def dataType: DataType = NullType
  override def nullable: Boolean = true
  override def sql: String = s"($query)"
}

/**
 * A builder class used to convert a resolved logical plan into a SQL query string.  Note that not
 * all resolved logical plan are convertible.  They either don't have corresponding SQL
 * representations (e.g. logical plans that operate on local Scala collections), or are simply not
 * supported by this builder (yet).
 */
class SQLBuilder(logicalPlan: LogicalPlan, sqlContext: SQLContext) extends Logging {
  require(logicalPlan.resolved, "SQLBuilder only supports resolved logical query plans")

  def this(df: DataFrame) = this(df.queryExecution.analyzed, df.sqlContext)

  def toSQL: String = {
    val canonicalizedPlan = Canonicalizer.execute(logicalPlan)
    try {
      val replaced = canonicalizedPlan.transformAllExpressions {
        case e: SubqueryExpression =>
          SubqueryHolder(new SQLBuilder(e.query, sqlContext).toSQL)
        case e: NonSQLExpression =>
          throw new UnsupportedOperationException(
            s"Expression $e doesn't have a SQL representation"
          )
        case e => e
      }

      val generatedSQL = toSQL(replaced)
      logDebug(
        s"""Built SQL query string successfully from given logical plan:
           |
           |# Original logical plan:
           |${logicalPlan.treeString}
           |# Canonicalized logical plan:
           |${replaced.treeString}
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

    case a @ Aggregate(_, _, e @ Expand(_, _, p: Project)) if isGroupingSet(a, e, p) =>
      groupingSetToSQL(a, e, p)

    case p: Aggregate =>
      aggregateToSQL(p)

    case w: Window =>
      windowToSQL(w)

    case Limit(limitExpr, child) =>
      s"${toSQL(child)} LIMIT ${limitExpr.sql}"

    case p: Sample if p.isTableSample =>
      val fraction = math.min(100, math.max(0, (p.upperBound - p.lowerBound) * 100))
      p.child match {
        case m: MetastoreRelation =>
          val aliasName = m.alias.getOrElse("")
          build(
            s"`${m.databaseName}`.`${m.tableName}`",
            "TABLESAMPLE(" + fraction + " PERCENT)",
            aliasName)
        case s: SubqueryAlias =>
          val aliasName = if (s.child.isInstanceOf[SubqueryAlias]) s.alias else ""
          val plan = if (s.child.isInstanceOf[SubqueryAlias]) s.child else s
          build(toSQL(plan), "TABLESAMPLE(" + fraction + " PERCENT)", aliasName)
        case _ =>
          build(toSQL(p.child), "TABLESAMPLE(" + fraction + " PERCENT)")
      }

    case Filter(condition, child) =>
      val whereOrHaving = child match {
        case _: Aggregate => "HAVING"
        case _ => "WHERE"
      }
      build(toSQL(child), whereOrHaving, condition.sql)

    case p @ Distinct(u: Union) if u.children.length > 1 =>
      val childrenSql = u.children.map(c => s"(${toSQL(c)})")
      childrenSql.mkString(" UNION DISTINCT ")

    case p: Union if p.children.length > 1 =>
      val childrenSql = p.children.map(c => s"(${toSQL(c)})")
      childrenSql.mkString(" UNION ALL ")

    case p: Intersect =>
      build("(" + toSQL(p.left), ") INTERSECT (", toSQL(p.right) + ")")

    case p: Except =>
      build("(" + toSQL(p.left), ") EXCEPT (", toSQL(p.right) + ")")

    case p: SubqueryAlias =>
      p.child match {
        // Persisted data source relation
        case LogicalRelation(_, _, Some(TableIdentifier(table, Some(database)))) =>
          s"${quoteIdentifier(database)}.${quoteIdentifier(table)}"
        // Parentheses is not used for persisted data source relations
        // e.g., select x.c1 from (t1) as x inner join (t1) as y on x.c1 = y.c1
        case SubqueryAlias(_, _: LogicalRelation | _: MetastoreRelation) =>
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
        s"${quoteIdentifier(p.databaseName)}.${quoteIdentifier(p.tableName)}",
        p.alias.map(a => s" AS ${quoteIdentifier(a)}").getOrElse("")
      )

    case Sort(orders, _, RepartitionByExpression(partitionExprs, child, _))
        if orders.map(_.child) == partitionExprs =>
      build(toSQL(child), "CLUSTER BY", partitionExprs.map(_.sql).mkString(", "))

    case p: Sort =>
      build(
        toSQL(p.child),
        if (p.global) "ORDER BY" else "SORT BY",
        p.order.map(_.sql).mkString(", ")
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
  private def build(segments: String*): String =
    segments.map(_.trim).filter(_.nonEmpty).mkString(" ")

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

  private def sameOutput(output1: Seq[Attribute], output2: Seq[Attribute]): Boolean =
    output1.size == output2.size &&
      output1.zip(output2).forall(pair => pair._1.semanticEquals(pair._2))

  private def isGroupingSet(a: Aggregate, e: Expand, p: Project): Boolean = {
    assert(a.child == e && e.child == p)
    a.groupingExpressions.forall(_.isInstanceOf[Attribute]) &&
      sameOutput(e.output, p.child.output ++ a.groupingExpressions.map(_.asInstanceOf[Attribute]))
  }

  private def groupingSetToSQL(
      agg: Aggregate,
      expand: Expand,
      project: Project): String = {
    assert(agg.groupingExpressions.length > 1)

    // The last column of Expand is always grouping ID
    val gid = expand.output.last

    val numOriginalOutput = project.child.output.length
    // Assumption: Aggregate's groupingExpressions is composed of
    // 1) the attributes of aliased group by expressions
    // 2) gid, which is always the last one
    val groupByAttributes = agg.groupingExpressions.dropRight(1).map(_.asInstanceOf[Attribute])
    // Assumption: Project's projectList is composed of
    // 1) the original output (Project's child.output),
    // 2) the aliased group by expressions.
    val groupByExprs = project.projectList.drop(numOriginalOutput).map(_.asInstanceOf[Alias].child)
    val groupingSQL = groupByExprs.map(_.sql).mkString(", ")

    // a map from group by attributes to the original group by expressions.
    val groupByAttrMap = AttributeMap(groupByAttributes.zip(groupByExprs))

    val groupingSet: Seq[Seq[Expression]] = expand.projections.map { project =>
      // Assumption: expand.projections is composed of
      // 1) the original output (Project's child.output),
      // 2) group by attributes(or null literal)
      // 3) gid, which is always the last one in each project in Expand
      project.drop(numOriginalOutput).dropRight(1).collect {
        case attr: Attribute if groupByAttrMap.contains(attr) => groupByAttrMap(attr)
      }
    }
    val groupingSetSQL = "GROUPING SETS(" +
      groupingSet.map(e => s"(${e.map(_.sql).mkString(", ")})").mkString(", ") + ")"

    val aggExprs = agg.aggregateExpressions.map { case expr =>
      expr.transformDown {
        // grouping_id() is converted to VirtualColumn.groupingIdName by Analyzer. Revert it back.
        case ar: AttributeReference if ar == gid => GroupingID(Nil)
        case ar: AttributeReference if groupByAttrMap.contains(ar) => groupByAttrMap(ar)
        case a @ Cast(BitwiseAnd(
            ShiftRight(ar: AttributeReference, Literal(value: Any, IntegerType)),
            Literal(1, IntegerType)), ByteType) if ar == gid =>
          // for converting an expression to its original SQL format grouping(col)
          val idx = groupByExprs.length - 1 - value.asInstanceOf[Int]
          groupByExprs.lift(idx).map(Grouping).getOrElse(a)
      }
    }

    build(
      "SELECT",
      aggExprs.map(_.sql).mkString(", "),
      if (agg.child == OneRowRelation) "" else "FROM",
      toSQL(project.child),
      "GROUP BY",
      groupingSQL,
      groupingSetSQL
    )
  }

  private def windowToSQL(w: Window): String = {
    build(
      "SELECT",
      (w.child.output ++ w.windowExpressions).map(_.sql).mkString(", "),
      if (w.child == OneRowRelation) "" else "FROM",
      toSQL(w.child)
    )
  }

  object Canonicalizer extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] = Seq(
      Batch("Collapse Project", FixedPoint(100),
        // The `WidenSetOperationTypes` analysis rule may introduce extra `Project`s over
        // `Aggregate`s to perform type casting.  This rule merges these `Project`s into
        // `Aggregate`s.
        CollapseProject),
      Batch("Recover Scoping Info", Once,
        // Used to handle other auxiliary `Project`s added by analyzer (e.g.
        // `ResolveAggregateFunctions` rule)
        AddSubquery,
        // Previous rule will add extra sub-queries, this rule is used to re-propagate and update
        // the qualifiers bottom up, e.g.:
        //
        // Sort
        //   ordering = t1.a
        //   Project
        //     projectList = [t1.a, t1.b]
        //     Subquery gen_subquery
        //       child ...
        //
        // will be transformed to:
        //
        // Sort
        //   ordering = gen_subquery.a
        //   Project
        //     projectList = [gen_subquery.a, gen_subquery.b]
        //     Subquery gen_subquery
        //       child ...
        UpdateQualifiers
      )
    )

    object AddSubquery extends Rule[LogicalPlan] {
      override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
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
        case plan @ Project(_, Filter(_, _: Aggregate)) => wrapChildWithSubquery(plan)

        case w @ Window(_, _, _, Filter(_, _: Aggregate)) => wrapChildWithSubquery(w)

        case plan @ Project(_,
          _: SubqueryAlias
            | _: Filter
            | _: Join
            | _: MetastoreRelation
            | OneRowRelation
            | _: LocalLimit
            | _: GlobalLimit
            | _: Sample
        ) => plan

        case plan: Project => wrapChildWithSubquery(plan)

        // We will generate "SELECT ... FROM ..." for Window operator, so its child operator should
        // be able to put in the FROM clause, or we wrap it with a subquery.
        case w @ Window(_, _, _,
          _: SubqueryAlias
            | _: Filter
            | _: Join
            | _: MetastoreRelation
            | OneRowRelation
            | _: LocalLimit
            | _: GlobalLimit
            | _: Sample
        ) => w

        case w: Window => wrapChildWithSubquery(w)
      }

      private def wrapChildWithSubquery(plan: UnaryNode): LogicalPlan = {
        val newChild = SubqueryAlias(SQLBuilder.newSubqueryName, plan.child)
        plan.withNewChildren(Seq(newChild))
      }
    }

    object UpdateQualifiers extends Rule[LogicalPlan] {
      override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
        case plan =>
          val inputAttributes = plan.children.flatMap(_.output)
          plan transformExpressions {
            case a: AttributeReference if !plan.producedAttributes.contains(a) =>
              val qualifier = inputAttributes.find(_ semanticEquals a).map(_.qualifiers)
              a.withQualifiers(qualifier.getOrElse(Nil))
          }
      }
    }
  }
}

object SQLBuilder {
  private val nextSubqueryId = new AtomicLong(0)

  private def newSubqueryName: String = s"gen_subquery_${nextSubqueryId.getAndIncrement()}"
}
