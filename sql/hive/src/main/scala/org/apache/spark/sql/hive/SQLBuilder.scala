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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.CollapseProject
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.execution.HiveScriptIOSchema
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
    val outputNames = logicalPlan.output.map(_.name)
    val qualifiers = logicalPlan.output.flatMap(_.qualifiers).distinct

    // Keep the qualifier information by using it as sub-query name, if there is only one qualifier
    // present.
    val finalName = if (qualifiers.length == 1) {
      qualifiers.head
    } else {
      SQLBuilder.newSubqueryName
    }

    // Canonicalizer will remove all naming information, we should add it back by adding an extra
    // Project and alias the outputs.
    val aliasedOutput = canonicalizedPlan.output.zip(outputNames).map {
      case (attr, name) => Alias(attr.withQualifiers(Nil), name)()
    }
    val finalPlan = Project(aliasedOutput, SubqueryAlias(finalName, canonicalizedPlan))

    try {
      val replaced = finalPlan.transformAllExpressions {
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

    case g: Generate =>
      generateToSQL(g)

    case Limit(limitExpr, child) =>
      s"${toSQL(child)} LIMIT ${limitExpr.sql}"

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

    case p: SubqueryAlias => build("(" + toSQL(p.child) + ")", "AS", p.alias)

    case p: Join =>
      build(
        toSQL(p.left),
        p.joinType.sql,
        "JOIN",
        toSQL(p.right),
        p.condition.map(" ON " + _.sql).getOrElse(""))

    case SQLTable(database, table, _, sample) =>
      val qualifiedName = s"${quoteIdentifier(database)}.${quoteIdentifier(table)}"
      sample.map { case (lowerBound, upperBound) =>
        val fraction = math.min(100, math.max(0, (upperBound - lowerBound) * 100))
        qualifiedName + " TABLESAMPLE(" + fraction + " PERCENT)"
      }.getOrElse(qualifiedName)

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

    case p: ScriptTransformation =>
      scriptTransformationToSQL(p)

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

  private def scriptTransformationToSQL(plan: ScriptTransformation): String = {
    val ioSchema = plan.ioschema.asInstanceOf[HiveScriptIOSchema]
    val inputRowFormatSQL = ioSchema.inputRowFormatSQL.getOrElse(
      throw new UnsupportedOperationException(
        s"unsupported row format ${ioSchema.inputRowFormat}"))
    val outputRowFormatSQL = ioSchema.outputRowFormatSQL.getOrElse(
      throw new UnsupportedOperationException(
        s"unsupported row format ${ioSchema.outputRowFormat}"))

    val outputSchema = plan.output.map { attr =>
      s"${attr.sql} ${attr.dataType.simpleString}"
    }.mkString(", ")

    build(
      "SELECT TRANSFORM",
      "(" + plan.input.map(_.sql).mkString(", ") + ")",
      inputRowFormatSQL,
      s"USING \'${plan.script}\'",
      "AS (" + outputSchema + ")",
      outputRowFormatSQL,
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

  private def generateToSQL(g: Generate): String = {
    val columnAliases = g.generatorOutput.map(_.sql).mkString(",")

    val childSQL = if (g.child == OneRowRelation) {
      // This only happens when we put UDTF in project list and there is no FROM clause. Because we
      // always generate LATERAL VIEW for `Generate`, here we use a trick to put a dummy sub-query
      // after FROM clause, so that we can generate a valid LATERAL VIEW SQL string.
      // For example, if the original SQL is: "SELECT EXPLODE(ARRAY(1, 2))", we will convert in to
      // LATERAL VIEW format, and generate:
      // SELECT col FROM (SELECT 1) sub-q0 LATERAL VIEW EXPLODE(ARRAY(1, 2)) sub_q1 AS col
      s"(SELECT 1) ${SQLBuilder.newSubqueryName}"
    } else {
      toSQL(g.child)
    }

    // The final SQL string for Generate contains 7 parts:
    //   1. the SQL of child, can be a table or sub-query
    //   2. the LATERAL VIEW keyword
    //   3. an optional OUTER keyword
    //   4. the SQL of generator, e.g. EXPLODE(array_col)
    //   5. the table alias for output columns of generator.
    //   6. the AS keyword
    //   7. the column alias, can be more than one, e.g. AS key, value
    // An concrete example: "tbl LATERAL VIEW EXPLODE(map_col) sub_q AS key, value", and the builder
    // will put it in FROM clause later.
    build(
      childSQL,
      "LATERAL VIEW",
      if (g.outer) "OUTER" else "",
      g.generator.sql,
      SQLBuilder.newSubqueryName,
      "AS",
      columnAliases
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

    val aggExprs = agg.aggregateExpressions.map { case aggExpr =>
      val originalAggExpr = aggExpr.transformDown {
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

      originalAggExpr match {
        // Ancestor operators may reference the output of this grouping set, and we use exprId to
        // generate a unique name for each attribute, so we should make sure the transformed
        // aggregate expression won't change the output, i.e. exprId and alias name should remain
        // the same.
        case ne: NamedExpression if ne.exprId == aggExpr.exprId => ne
        case e => Alias(e, normalizedName(aggExpr))(exprId = aggExpr.exprId)
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

  private def normalizedName(n: NamedExpression): String = "gen_attr_" + n.exprId.id

  object Canonicalizer extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] = Seq(
      Batch("Collapse Project", FixedPoint(100),
        // The `WidenSetOperationTypes` analysis rule may introduce extra `Project`s over
        // `Aggregate`s to perform type casting.  This rule merges these `Project`s into
        // `Aggregate`s.
        CollapseProject),
      Batch("Recover Scoping Info", Once,
        // Remove all sub queries, as we will insert new ones when it's necessary.
        EliminateSubqueryAliases,
        // A logical plan is allowed to have same-name outputs with different qualifiers(e.g. the
        // `Join` operator). However, this kind of plan can't be put under a sub query as we will
        // erase and assign a new qualifier to all outputs and make it impossible to distinguish
        // same-name outputs. This rule renames all attributes, to guarantee different
        // attributes(with different exprId) always have different names. It also removes all
        // qualifiers, as attributes have unique names now and we don't need qualifiers to resolve
        // ambiguity.
        NormalizedAttribute,
        // Finds the table relations and wrap them with `SQLTable`s.  If there are any `Sample`
        // operators on top of a table relation, merge the sample information into `SQLTable` of
        // that table relation, as we can only convert table sample to standard SQL string.
        ResolveSQLTable,
        // Insert sub queries on top of operators that need to appear after FROM clause.
        AddSubquery
      )
    )

    object NormalizedAttribute extends Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
        case a: AttributeReference =>
          AttributeReference(normalizedName(a), a.dataType)(exprId = a.exprId, qualifiers = Nil)
        case a: Alias =>
          Alias(a.child, normalizedName(a))(exprId = a.exprId, qualifiers = Nil)
      }
    }

    object ResolveSQLTable extends Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
        case Sample(lowerBound, upperBound, _, _, ExtractSQLTable(table)) =>
          aliasColumns(table.withSample(lowerBound, upperBound))
        case ExtractSQLTable(table) =>
          aliasColumns(table)
      }

      /**
       * Aliases the table columns to the generated attribute names, as we use exprId to generate
       * unique name for each attribute when normalize attributes, and we can't reference table
       * columns with their real names.
       */
      private def aliasColumns(table: SQLTable): LogicalPlan = {
        val aliasedOutput = table.output.map { attr =>
          Alias(attr, normalizedName(attr))(exprId = attr.exprId)
        }
        addSubquery(Project(aliasedOutput, table))
      }
    }

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
        case p @ Project(_, f @ Filter(_, _: Aggregate)) => p.copy(child = addSubquery(f))

        case w @ Window(_, _, _, f @ Filter(_, _: Aggregate)) => w.copy(child = addSubquery(f))

        case p: Project => p.copy(child = addSubqueryIfNeeded(p.child))

        // We will generate "SELECT ... FROM ..." for Window operator, so its child operator should
        // be able to put in the FROM clause, or we wrap it with a subquery.
        case w: Window => w.copy(child = addSubqueryIfNeeded(w.child))

        case j: Join => j.copy(
          left = addSubqueryIfNeeded(j.left),
          right = addSubqueryIfNeeded(j.right))

        // A special case for Generate. When we put UDTF in project list, followed by WHERE, e.g.
        // SELECT EXPLODE(arr) FROM tbl WHERE id > 1, the Filter operator will be under Generate
        // operator and we need to add a sub-query between them, as it's not allowed to have a WHERE
        // before LATERAL VIEW, e.g. "... FROM tbl WHERE id > 2 EXPLODE(arr) ..." is illegal.
        case g @ Generate(_, _, _, _, _, f: Filter) =>
          // Add an extra `Project` to make sure we can generate legal SQL string for sub-query,
          // for example, Subquery -> Filter -> Table will generate "(tbl WHERE ...) AS name", which
          // misses the SELECT part.
          val proj = Project(f.output, f)
          g.copy(child = addSubquery(proj))
      }
    }

    private def addSubquery(plan: LogicalPlan): SubqueryAlias = {
      SubqueryAlias(SQLBuilder.newSubqueryName, plan)
    }

    private def addSubqueryIfNeeded(plan: LogicalPlan): LogicalPlan = plan match {
      case _: SubqueryAlias => plan
      case _: Filter => plan
      case _: Join => plan
      case _: LocalLimit => plan
      case _: GlobalLimit => plan
      case _: SQLTable => plan
      case _: Generate => plan
      case _: OneRowRelation => plan
      case _ => addSubquery(plan)
    }
  }

  case class SQLTable(
      database: String,
      table: String,
      output: Seq[Attribute],
      sample: Option[(Double, Double)] = None) extends LeafNode {
    def withSample(lowerBound: Double, upperBound: Double): SQLTable =
      this.copy(sample = Some(lowerBound -> upperBound))
  }

  object ExtractSQLTable {
    def unapply(plan: LogicalPlan): Option[SQLTable] = plan match {
      case l @ LogicalRelation(_, _, Some(TableIdentifier(table, Some(database)))) =>
        Some(SQLTable(database, table, l.output.map(_.withQualifiers(Nil))))

      case m: MetastoreRelation =>
        Some(SQLTable(m.databaseName, m.tableName, m.output.map(_.withQualifiers(Nil))))

      case _ => None
    }
  }
}

object SQLBuilder {
  private val nextSubqueryId = new AtomicLong(0)

  private def newSubqueryName: String = s"gen_subquery_${nextSubqueryId.getAndIncrement()}"
}
