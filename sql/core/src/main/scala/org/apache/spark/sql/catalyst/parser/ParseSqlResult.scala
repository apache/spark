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

package org.apache.spark.sql.catalyst.parser

import scala.collection.mutable

import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, parse => parseJson, render}

import org.apache.spark.{ErrorMessageFormat, SparkThrowable, SparkThrowableHelper}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin, SQLQueryContext}
import org.apache.spark.sql.exceptions.SqlScriptingException
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.command.{CreateViewCommand, DescribeQueryCommand, ExplainCommand}
import org.apache.spark.sql.execution.datasources.CreateTempViewUsing

/**
 * Parses a SQL statement string and returns a compact JSON description of the
 * unresolved plan (parse-only; no catalog resolution).
 *
 * Uses a stock [[SparkSqlParser]] (ThreadLocal) so statement coverage matches
 * the default production parser (EXPLAIN / SET / ADD JAR / temp views / etc.).
 * Session-specific [[org.apache.spark.sql.SparkSessionExtensions]] parser
 * wrappers are intentionally not applied: `parse_sql` must evaluate on
 * executors without a session, so only the stock parser is available under
 * distributed eval.
 *
 * On success the JSON always includes `parse_success`, the statement
 * identifier/code (ISO/IEC 9075-2:2023 Table 39), and omits unused optional
 * fields (`table_references`, `function_references`, `select_list`,
 * `parameter_markers`) when empty. On parse failure it returns
 * `parse_success: false` with source location and a nested STANDARD-format
 * error object, and does not throw. Only [[ParseException]] /
 * [[SqlScriptingException]] are converted to JSON; unexpected / internal
 * failures propagate so the function fails.
 */
object ParseSqlResult {

  private val parser: ThreadLocal[SparkSqlParser] =
    ThreadLocal.withInitial(() => new SparkSqlParser())

  /** Parse `sql` and render the JSON result string. */
  def fromSql(sql: String): String = {
    try {
      // Do not inherit the outer query's origin from the parse_sql expression.
      // Errors and parsed nodes must refer to the SQL string passed to this function.
      val origin = if (sql.nonEmpty) {
        Origin(startIndex = Some(0), stopIndex = Some(sql.length - 1), sqlText = Some(sql))
      } else {
        Origin(sqlText = Some(sql))
      }
      CurrentOrigin.withOrigin(origin) {
        val plan = parser.get().parsePlan(sql)
        fromPlan(plan)
      }
    } catch {
      // User-facing parse / scripting failures become JSON; everything else fails.
      case e: ParseException =>
        errorJson(e)
      case e: SqlScriptingException =>
        errorJson(e)
    }
  }

  /** Build success JSON from an already-parsed unresolved plan. */
  def fromPlan(plan: LogicalPlan): String = {
    val classification = SqlStatementCodes.classify(plan)
    val fields = mutable.ListBuffer.empty[JField]
    fields += "parse_success" -> JBool(true)
    fields += "statement_identifier" -> JString(classification.statementIdentifier)
    fields += "statement_code" -> JInt(classification.statementCode)
    // Omit unused collections / markers so consumers can treat absence as empty.
    val refs = collectPlanReferences(plan)
    if (refs.tables.nonEmpty) {
      fields += "table_references" -> JArray(refs.tables.map(partsToJArray).toList)
    }
    if (refs.functions.nonEmpty) {
      fields += "function_references" -> JArray(refs.functions.map(partsToJArray).toList)
    }
    val selectList = collectSelectList(plan)
    if (selectList.nonEmpty) {
      fields += "select_list" -> JArray(selectList.toList)
    }
    refs.parameterMarkers.foreach(markers => fields += "parameter_markers" -> markers)
    compact(render(JObject(fields.toList)))
  }

  private def errorJson(e: SparkThrowable with Throwable): String = {
    val errorObj = parseJson(
      SparkThrowableHelper.getMessage(e, ErrorMessageFormat.STANDARD)).asInstanceOf[JObject]
    val origin = e match {
      case p: ParseException => Some(p.start)
      case s: SqlScriptingException => Some(s.origin)
      case _ => None
    }
    val locationFields = origin.toSeq.flatMap(originFields)
    val contextFields = if (errorObj.obj.exists(_._1 == "queryContext")) {
      Nil
    } else {
      origin.toSeq.flatMap(queryContextField)
    }
    compact(render(JObject(
      "parse_success" -> JBool(false),
      "error" -> JObject(errorObj.obj ++ contextFields ++ locationFields)
    )))
  }

  private def queryContextField(origin: Origin): Option[JField] = origin.context match {
    case context: SQLQueryContext if context.isValid =>
      Some("queryContext" -> JArray(List(JObject(
        "objectType" -> JString(context.objectType),
        "objectName" -> JString(context.objectName),
        "startIndex" -> JInt(context.startIndex + 1),
        "stopIndex" -> JInt(context.stopIndex + 1),
        "fragment" -> JString(context.fragment)
      ))))
    case _ => None
  }

  private def originFields(origin: Origin): Seq[JField] = Seq(
    origin.line.map(line => "line" -> JInt(line)),
    origin.startPosition.map(position => "position" -> JInt(position))).flatten

  private def partsToJArray(parts: Seq[String]): JArray =
    JArray(parts.map(JString).toList)

  /**
   * Walk expressions in all product fields, including wrappers such as column
   * definitions that [[LogicalPlan.expressions]] does not descend into.
   */
  private def foreachExpressionDeep(plan: LogicalPlan)(f: Expression => Unit): Unit = {
    def visit(value: Any): Unit = value match {
      case e: Expression => f(e)
      case _: LogicalPlan =>
      case values: Iterable[_] => values.foreach(visit)
      case value: Product => value.productIterator.foreach(visit)
      case _ =>
    }
    plan.productIterator.foreach(visit)
  }

  /** CTE aliases visible at a given point of the walk, normalized for lookup. */
  private type CteScope = Set[String]

  private def normalizeCteName(name: String): String =
    name.toLowerCase(java.util.Locale.ROOT)

  /**
   * Deep plan walk that hands each node the CTE aliases in scope there, and
   * covers tree slots that standard `collect` / `collectWithSubqueries` miss:
   *   - [[UnresolvedWith]] CTE definitions (`innerChildren`, not `children`)
   *   - [[InsertIntoStatement]].table (non-child plan slot)
   *   - [[SingleStatement]].parsedPlan (`children` exposes only the wrapped
   *     plan's children, so the wrapped plan is walked in their place)
   *   - [[CompoundBody]].handlers (not in `children`)
   *   - [[SimpleCaseStatement]].elseBody (not in `children`)
   * Expression subqueries are walked in the scope that encloses them.
   */
  private def foreachPlanDeep(plan: LogicalPlan)(f: (LogicalPlan, CteScope) => Unit): Unit =
    visitPlan(plan, Set.empty)(f)

  private def visitPlan(plan: LogicalPlan, scope: CteScope)(
      f: (LogicalPlan, CteScope) => Unit): Unit = {
    f(plan, scope)
    plan match {
      case s: SingleStatement =>
        // `children` forwards the wrapped plan's children, so walking the
        // wrapped plan here visits every node exactly once.
        visitPlan(s.parsedPlan, scope)(f)
      case _ =>
        visitNonChildSlots(plan, scope)(f)
        // A WITH body sees every alias the clause defines.
        val childScope = plan match {
          case w: UnresolvedWith => scope ++ w.cteRelations.map(r => normalizeCteName(r._1))
          case _ => scope
        }
        plan.subqueries.foreach(sq => visitPlan(sq, childScope)(f))
        plan.children.foreach(child => visitPlan(child, childScope)(f))
    }
  }

  /** Visit plan slots that are not exposed via `children` / subqueries. */
  private def visitNonChildSlots(plan: LogicalPlan, scope: CteScope)(
      f: (LogicalPlan, CteScope) => Unit): Unit = {
    plan match {
      case w: UnresolvedWith =>
        // A CTE definition sees the aliases defined before it, plus its own
        // name when the clause is RECURSIVE. Later aliases are not in scope,
        // so a definition naming one refers to the real table.
        var definitionScope = scope
        w.cteRelations.foreach { case (name, ctePlan, _) =>
          val normalized = normalizeCteName(name)
          val bodyScope =
            if (w.allowRecursion) definitionScope + normalized else definitionScope
          visitPlan(ctePlan, bodyScope)(f)
          definitionScope += normalized
        }
      case InsertIntoStatement(table, _, _, _, _, _, _, _, _) =>
        visitPlan(table, scope)(f)
      case c: CompoundBody =>
        c.handlers.foreach(h => visitPlan(h, scope)(f))
      case s: SimpleCaseStatement =>
        s.elseBody.foreach(b => visitPlan(b, scope)(f))
      case ExplainCommand(logicalPlan, _) =>
        visitPlan(logicalPlan, scope)(f)
      case DescribeQueryCommand(_, queryPlan) =>
        visitPlan(queryPlan, scope)(f)
      case _ =>
    }
  }

  /** Multipart name from a table/view-shaped plan node, if any. */
  private def tableOrViewParts(plan: LogicalPlan): Option[Seq[String]] = plan match {
    case u: UnresolvedRelation => Some(u.multipartIdentifier)
    case u: UnresolvedTable => Some(u.multipartIdentifier)
    case u: UnresolvedView => Some(u.multipartIdentifier)
    case u: UnresolvedTableOrView => Some(u.multipartIdentifier)
    case u: UnresolvedIdentifier => Some(u.nameParts)
    case _ => None
  }

  private def tableIdentifierParts(id: org.apache.spark.sql.catalyst.TableIdentifier): Seq[String] =
    id.catalog.toSeq ++ id.database.toSeq :+ id.table

  private final case class PlanReferences(
      tables: Seq[Seq[String]],
      functions: Seq[Seq[String]],
      parameterMarkers: Option[JObject])

  /**
   * Collect multipart table/view identifiers, function names, and parameter
   * markers for lineage in a single deep walk. A single-part name is dropped
   * only when a CTE alias in scope at that node shadows it, so an inner CTE
   * does not hide a same-named real table outside it; tables referenced inside
   * CTE bodies are still included. Correlation aliases and function / variable
   * identifiers are not collected as tables. Deduplicates while preserving
   * first-seen order.
   */
  private def collectPlanReferences(plan: LogicalPlan): PlanReferences = {
    val tables = mutable.LinkedHashSet.empty[Seq[String]]
    val functions = mutable.LinkedHashSet.empty[Seq[String]]
    val namedParams = mutable.LinkedHashSet.empty[String]
    var unnamedCount = 0

    // CTE aliases are always single-part in the grammar, and so are references
    // to them, so shadowing only applies to single-part names.
    def isCteName(parts: Seq[String], scope: CteScope): Boolean = parts match {
      case Seq(name) => scope.contains(normalizeCteName(name))
      case _ => false
    }

    def addTable(parts: Seq[String], scope: CteScope): Unit = {
      if (parts.nonEmpty && !isCteName(parts, scope)) tables += parts
    }

    def addFunction(parts: Seq[String]): Unit = {
      if (parts.nonEmpty) functions += parts
    }

    def visitExpr(e: Expression): Unit = e.foreach {
      case f: UnresolvedFunction => addFunction(f.nameParts)
      case n: NamedParameter => namedParams += n.name
      case _: PosParameter => unnamedCount += 1
      case _ =>
    }

    foreachPlanDeep(plan) { (p, scope) =>
      foreachExpressionDeep(p)(visitExpr)
      def add(parts: Seq[String]): Unit = addTable(parts, scope)
      p match {
        case u: UnresolvedRelation => add(u.multipartIdentifier)
        case u: UnresolvedTable => add(u.multipartIdentifier)
        case u: UnresolvedView => add(u.multipartIdentifier)
        case u: UnresolvedTableOrView => add(u.multipartIdentifier)
        // Table/view DDL targets only -- not CreateFunction / CreateVariable names.
        case c: CreateView => tableOrViewParts(c.child).foreach(add)
        case c: CreateViewCommand => add(tableIdentifierParts(c.name))
        case c: CreateTempViewUsing => add(tableIdentifierParts(c.tableIdent))
        case c: CreateTable => tableOrViewParts(c.name).foreach(add)
        case c: CreateTableAsSelect => tableOrViewParts(c.name).foreach(add)
        case c: ReplaceTable => tableOrViewParts(c.name).foreach(add)
        case c: ReplaceTableAsSelect => tableOrViewParts(c.name).foreach(add)
        case c: DropTable => tableOrViewParts(c.child).foreach(add)
        case c: DropView => tableOrViewParts(c.child).foreach(add)
        case c: TruncateTable => tableOrViewParts(c.table).foreach(add)
        case c: TruncatePartition => tableOrViewParts(c.table).foreach(add)
        case c: CacheTable =>
          if (c.multipartIdentifier.nonEmpty) add(c.multipartIdentifier)
          else tableOrViewParts(c.table).foreach(add)
        case c: UncacheTable => tableOrViewParts(c.table).foreach(add)
        case c: RefreshTable => tableOrViewParts(c.child).foreach(add)
        case c: CommentOnTable => tableOrViewParts(c.table).foreach(add)
        case u: UnresolvedTableValuedFunction => addFunction(u.name)
        case _ =>
      }
    }

    val markers =
      if (namedParams.isEmpty && unnamedCount == 0) {
        None
      } else {
        val markerFields = mutable.ListBuffer.empty[JField]
        if (namedParams.nonEmpty) {
          markerFields += "named" -> JArray(namedParams.toList.map(JString))
        }
        if (unnamedCount > 0) markerFields += "unnamed_count" -> JInt(unnamedCount)
        Some(JObject(markerFields.toList))
      }
    PlanReferences(tables.toSeq, functions.toSeq, markers)
  }

  /**
   * Collect the primary select list as `{name}` objects (multipart name
   * parts only). Empty for non-query statements without a projected query body.
   */
  private def collectSelectList(plan: LogicalPlan): Seq[JObject] = {
    val query = primaryQueryPlan(plan)
    val named: Seq[NamedExpression] = query match {
      case p: Project => p.projectList
      case a: Aggregate => a.aggregateExpressions
      case _ => Nil
    }
    named.map(selectListItem)
  }

  private def primaryQueryPlan(plan: LogicalPlan): LogicalPlan = plan match {
    case UnresolvedWith(child, _, _) => primaryQueryPlan(child)
    case InsertIntoStatement(_, _, _, query, _, _, _, _, _) =>
      primaryQueryPlan(query)
    case c: CreateTableAsSelect => primaryQueryPlan(c.query)
    case r: ReplaceTableAsSelect => primaryQueryPlan(r.query)
    case c: CreateView => primaryQueryPlan(c.query)
    case c: CreateViewCommand => primaryQueryPlan(c.plan)
    case c: CacheTableAsSelect => primaryQueryPlan(c.plan)
    case ExplainCommand(logicalPlan, _) => primaryQueryPlan(logicalPlan)
    case DescribeQueryCommand(_, queryPlan) => primaryQueryPlan(queryPlan)
    case SubqueryAlias(_, child) => primaryQueryPlan(child)
    case Sort(_, _, child, _) => primaryQueryPlan(child)
    case Filter(_, child) => primaryQueryPlan(child)
    case UnresolvedHaving(_, child) => primaryQueryPlan(child)
    case UnresolvedQualify(_, child) => primaryQueryPlan(child)
    case Distinct(child) => primaryQueryPlan(child)
    case GlobalLimit(_, child) => primaryQueryPlan(child)
    case LocalLimit(_, child) => primaryQueryPlan(child)
    case Offset(_, child) => primaryQueryPlan(child)
    case Repartition(_, _, child) => primaryQueryPlan(child)
    case RepartitionByExpression(_, child, _, _) => primaryQueryPlan(child)
    case Sample(_, _, _, _, child, _) => primaryQueryPlan(child)
    case other => other
  }

  private def selectListItem(ne: NamedExpression): JObject = ne match {
    case Alias(_, name) =>
      JObject("name" -> partsToJArray(Seq(name)))
    case _: UnresolvedAlias =>
      JObject("name" -> partsToJArray(Nil))
    case s: UnresolvedStar =>
      val name = s.target.map(_ :+ "*").getOrElse(Seq("*"))
      JObject("name" -> partsToJArray(name))
    case a: UnresolvedAttribute =>
      JObject("name" -> partsToJArray(a.nameParts))
    case other =>
      JObject("name" -> partsToJArray(Seq(other.name)))
  }
}
