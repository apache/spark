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

import java.util.Locale

import scala.collection.mutable

import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, parse => parseJson, render}

import org.apache.spark.{ErrorMessageFormat, SparkThrowable, SparkThrowableHelper}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, SqlScriptingContextManager, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin, SQLQueryContext}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.classic.{SparkSession => ClassicSparkSession}
import org.apache.spark.sql.connector.catalog.{CatalogManager, Identifier}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.exceptions.SqlScriptingException
import org.apache.spark.sql.execution.{CommandExecutionMode, SparkSqlParser}
import org.apache.spark.sql.execution.command.{
  CreateDataSourceTableAsSelectCommand,
  CreateDataSourceTableCommand,
  CreateTableCommand,
  CreateViewCommand,
  DataWritingCommand,
  DescribeQueryCommand,
  DescribeRelationJsonCommand,
  DropTableCommand,
  DropTempViewCommand,
  ExplainCommand,
  LeafRunnableCommand
}
import org.apache.spark.sql.execution.datasources.{
  CreateTempViewUsing, HadoopFsRelation, InsertIntoDataSourceCommand, LogicalRelation
}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf

/**
 * Mode for [[ParseSqlResult.fromSql]]. `PARSE` is unresolved. `ANALYZE` types
 * the select list and fully qualifies resolved relations and routines.
 */
object ParseSqlMode {
  val Parse = "PARSE"
  val Analyze = "ANALYZE"

  def parse(value: String): Option[String] = value.trim.toUpperCase(Locale.ROOT) match {
    case Parse => Some(Parse)
    case Analyze => Some(Analyze)
    case _ => None
  }
}

/**
 * Parses a SQL batch string and returns a compact JSON array describing its
 * statements. `PARSE` mode is unresolved (no catalog resolution). `ANALYZE`
 * mode also types selected columns and fully qualifies relation / routine
 * names using the active session catalog. ANALYZE inherits the invoker's
 * current catalog, schema, and SQL path and does not change them.
 *
 * Uses a stock [[SparkSqlParser]] (ThreadLocal) so statement coverage matches
 * the default production parser (EXPLAIN / SET / ADD JAR / temp views / etc.).
 * Session-specific [[org.apache.spark.sql.SparkSessionExtensions]] parser
 * wrappers are intentionally not applied: `parse_sql` must evaluate on
 * executors without a session, so only the stock parser is available under
 * distributed eval.
 *
 * Every statement object includes its 1-based character `start` in the
 * original batch and its character `length`, excluding surrounding
 * whitespace and the terminating semicolon. Characters are Unicode code
 * points (Spark SQL `length()` / UTF-8 strings): a supplementary-plane
 * emoji is one character. Java `String` offsets are UTF-16 (emoji = 2);
 * ANTLR `CodePointCharStream` offsets are already code points (emoji = 1).
 * The splitter maps ANTLR indexes to UTF-16 for `substring`; JSON converts
 * those spans back to code points. On success it also includes
 * `parse_success`, the statement
 * identifier/code (ISO/IEC 9075-2:2023 Table 39), and omits unused optional
 * fields (`target_table_references`, `source_table_references`,
 * `function_references`, `select_list`, `parameter_markers`) when empty. On
 * parse failure the statement object contains `parse_success: false` with
 * source location and a nested STANDARD-format error object, and parsing
 * continues with later statements. Nested `queryContext` is the STANDARD
 * renderer output for that error. View and EXECUTE IMMEDIATE bodies keep
 * their own text; `start` is relative to the original batch. An empty or
 * closed-comment-only batch produces an empty array. PARSE converts
 * [[ParseException]] / [[SqlScriptingException]] to JSON.
 * ANALYZE also reports non-internal analysis errors as JSON;
 * unexpected / internal failures propagate so the function fails.
 */
object ParseSqlResult {

  private val parser: ThreadLocal[SparkSqlParser] =
    ThreadLocal.withInitial(() => new SparkSqlParser())

  /** Parse `sql` as a batch and render the JSON result array. */
  def fromSql(
      sql: String,
      mode: String = ParseSqlMode.Parse): String = {
    val split = parser.get().splitStatementsWithPositions(sql)
    val statements = split.completeStatements ++ split.partialStatement
    val spans = statementSpans(sql, statements)
    val json = if (mode == ParseSqlMode.Analyze) {
      analyzeBatch(statements, spans)
    } else {
      JArray(statements.zip(spans).map { case (segment, span) =>
        parsePlanOrError(segment, span) match {
          case ParseFailed(errorObj) => errorObj
          case Parsed(_, plan) =>
            successJson(plan, analyzed = None, analyzeError = None, None, span)
        }
      }.toList)
    }
    compact(render(json))
  }

  private sealed trait ParsedStatement
  private case class Parsed(segment: PositionedSqlStatement, plan: LogicalPlan)
    extends ParsedStatement
  private case class ParseFailed(errorObj: JObject) extends ParsedStatement

  /**
   * Parse every statement, then analyze each against the invoker session.
   * Statements are not executed, so one does not see the effect of an earlier
   * one. Analysis failures stay JSON (`analyze_success` false) per statement.
   */
  private def analyzeBatch(
      statements: Seq[PositionedSqlStatement],
      spans: Seq[StatementSpan]): JArray = {
    val parsed = statements.zip(spans).map { case (segment, span) =>
      parsePlanOrError(segment, span)
    }
    val session = ClassicSparkSession.getActiveSession
      .getOrElse(throw QueryExecutionErrors.parseSqlAnalyzeRequiresSession())
    // The batch is analyzed on its own: a SQL script that calls parse_sql does
    // not lend it its locals, and CALL is not run.
    SqlScriptingContextManager.create(null).runWith {
      InvokeProcedures.withoutInvocation {
        JArray(parsed.zip(spans).map {
          case (ParseFailed(errorObj), _) => errorObj
          case (Parsed(_, plan), span) =>
            val (analyzed, error) = analyze(session, plan)
            successJson(plan, analyzed, error, Some(session), span)
        }.toList)
      }
    }
  }

  /** INTERNAL_ERROR is a Spark bug, not an analysis result: it fails the call. */
  private def isInternalError(e: SparkThrowable): Boolean =
    Option(e.getCondition).exists(_.startsWith("INTERNAL_ERROR"))

  /** Character span of a statement object in the original batch. */
  private case class StatementSpan(start: Int, length: Int)

  /**
   * Code-point `start` for each statement in one forward pass over `batch`.
   * Statements are in source order, so prefix counts are not repeated.
   */
  private def statementSpans(
      batch: String,
      statements: Seq[PositionedSqlStatement]): Seq[StatementSpan] = {
    var lastUtf16 = 0
    var lastCp = 0
    statements.map { segment =>
      val utf16 = math.max(0, math.min(segment.start, batch.length))
      if (utf16 >= lastUtf16) {
        lastCp += batch.codePointCount(lastUtf16, utf16)
        lastUtf16 = utf16
      } else {
        lastCp = batch.codePointCount(0, utf16)
        lastUtf16 = utf16
      }
      StatementSpan(lastCp + 1, charLength(segment))
    }
  }

  /** Parsed plan, or parse-failure JSON for this statement. */
  private def parsePlanOrError(
      segment: PositionedSqlStatement,
      span: StatementSpan): ParsedStatement = {
    val sql = segment.statement
    try {
      // Do not inherit the outer query's origin from the parse_sql expression.
      // Errors and parsed nodes refer to the individual statement.
      val origin = if (sql.nonEmpty) {
        Origin(
          startIndex = Some(0),
          stopIndex = Some(sql.codePointCount(0, sql.length) - 1),
          sqlText = Some(sql))
      } else {
        Origin(sqlText = Some(sql))
      }
      CurrentOrigin.withOrigin(origin) {
        Parsed(segment, parser.get().parsePlan(sql))
      }
    } catch {
      case e: ParseException =>
        ParseFailed(errorJson(e, span))
      case e: SqlScriptingException =>
        ParseFailed(errorJson(e, span))
    }
  }

  /** Analyze `plan` without executing it. Scripts never reach QueryExecution. */
  private def analyze(
      session: ClassicSparkSession,
      plan: LogicalPlan): (Option[LogicalPlan], Option[SparkThrowable with Throwable]) = {
    plan match {
      case _: CompoundBody =>
        (None, Some(QueryExecutionErrors.parseSqlAnalyzeUnsupportedScript()))
      case _ =>
        try {
          val toAnalyze = plan match {
            case s: SupervisingCommand =>
              s.withTransformedSupervisedPlan { inner =>
                session.sessionState.executePlan(inner, CommandExecutionMode.SKIP).analyzed
              }
            case other => other
          }
          val analyzed =
            session.sessionState.executePlan(toAnalyze, CommandExecutionMode.SKIP).analyzed
          (Some(analyzed), None)
        } catch {
          case e: SparkThrowable with Throwable if !isInternalError(e) => (None, Some(e))
        }
    }
  }

  /** Build success JSON from an already-parsed unresolved plan. */
  private def successJson(
      plan: LogicalPlan,
      analyzed: Option[LogicalPlan],
      analyzeError: Option[SparkThrowable with Throwable],
      session: Option[ClassicSparkSession],
      span: StatementSpan): JObject = {
    val classification = SqlStatementCodes.classify(plan)
    val fields = mutable.ListBuffer.empty[JField]
    fields += "start" -> JInt(span.start)
    fields += "length" -> JInt(span.length)
    fields += "parse_success" -> JBool(true)
    fields += "statement_identifier" -> JString(classification.statementIdentifier)
    fields += "statement_code" -> JInt(classification.statementCode)
    if (session.nonEmpty) {
      fields += "analyze_success" -> JBool(analyzed.isDefined)
    }
    // Omit unused collections / markers so consumers can treat absence as empty.
    // Functions and parameter markers always come from the parse tree. Relation
    // identities come from the analyzed plan when analysis succeeded.
    val parseRefs = collectPlanReferences(plan)
    val tableRefs = analyzed.map(collectPlanReferences).getOrElse(parseRefs)
    val targetTables = session.map { s =>
      if (analyzed.isDefined) {
        uniqueIdentities(tableRefs.targetTables, s)
      } else {
        uniqueIdentities(parseRefs.targetTables.map(qualifyTarget(_, plan, s)), s)
      }
    }.getOrElse(parseRefs.targetTables)
    val sourceTables = session.map { s =>
      if (analyzed.isDefined) {
        uniqueIdentities(tableRefs.sourceTables, s)
      } else {
        uniqueIdentities(parseRefs.sourceTables.map(qualifyWithCurrentNamespace(_, s)), s)
      }
    }.getOrElse(parseRefs.sourceTables)
    val functions = session.map { s =>
      uniqueIdentities(parseRefs.functions.map(qualifyRoutine(_, s)), s)
    }.getOrElse(parseRefs.functions)
    if (targetTables.nonEmpty) {
      fields += "target_table_references" ->
        JArray(targetTables.map(partsToJArray).toList)
    }
    if (sourceTables.nonEmpty) {
      fields += "source_table_references" ->
        JArray(sourceTables.map(partsToJArray).toList)
    }
    if (functions.nonEmpty) {
      fields += "function_references" -> JArray(functions.map(partsToJArray).toList)
    }
    val selectList = analyzed.map(collectTypedSelectList(_, plan))
      .getOrElse(collectSelectList(plan))
    if (selectList.nonEmpty) {
      fields += "select_list" -> JArray(selectList.toList)
    }
    parseRefs.parameterMarkers.foreach(markers => fields += "parameter_markers" -> markers)
    analyzeError.foreach { e =>
      fields += "error" -> errorObject(e)
    }
    JObject(fields.toList)
  }

  /** Unicode code-point length of the trimmed statement text. */
  private def charLength(segment: PositionedSqlStatement): Int = {
    val text = segment.statement
    if (text.isEmpty) 0 else text.codePointCount(0, text.length)
  }

  /** STANDARD JSON of `e`. Nested `queryContext` is left to the renderer. */
  private def errorObject(e: SparkThrowable with Throwable): JObject = {
    parseJson(
      SparkThrowableHelper.getMessage(e, ErrorMessageFormat.STANDARD)).asInstanceOf[JObject]
  }

  private def errorJson(e: SparkThrowable with Throwable, span: StatementSpan): JObject = {
    val errorObj = errorObject(e)
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
    JObject(
      "start" -> JInt(span.start),
      "length" -> JInt(span.length),
      "parse_success" -> JBool(false),
      "error" -> JObject(errorObj.obj ++ contextFields ++ locationFields)
    )
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

  /** Whether a command's single `child` names the statement target table/view. */
  private def isTargetTableChild(parent: LogicalPlan): Boolean = parent match {
    case _: DeleteFromTable | _: DeleteFromTableWithFilters | _: UpdateTable |
         _: CreateTable | _: ReplaceTable | _: DropTable | _: DropView |
         _: TruncateTable | _: TruncatePartition | _: AlterTableCommand |
         _: RenameTable | _: SetViewProperties | _: RefreshTable |
         _: UncacheTable | _: CommentOnTable | _: CreateIndex =>
      true
    case _ => false
  }

  /** CTE aliases visible at a given point of the walk, normalized for lookup. */
  private type CteScope = Set[String]

  /** Whether a table/view reference names the statement target or a read source. */
  private sealed trait TableRefRole
  private object TableRefRole {
    case object Target extends TableRefRole
    case object Source extends TableRefRole
  }

  private def normalizeCteName(name: String): String =
    name.toLowerCase(java.util.Locale.ROOT)

  private def childScope(parent: LogicalPlan, scope: CteScope): CteScope = parent match {
    case w: UnresolvedWith => scope ++ w.cteRelations.map(r => normalizeCteName(r.name))
    case _ => scope
  }

  /**
   * Deep plan walk that hands each node the CTE aliases in scope there and the
   * table-reference role for relation nodes at that position, and covers tree
   * slots that standard `collect` / `collectWithSubqueries` miss.
   */
  private def foreachPlanDeep(
      plan: LogicalPlan)(f: (LogicalPlan, CteScope, TableRefRole) => Unit): Unit =
    visitPlan(plan, Set.empty, TableRefRole.Source)(f)

  private def visitPlan(
      plan: LogicalPlan,
      scope: CteScope,
      role: TableRefRole)(f: (LogicalPlan, CteScope, TableRefRole) => Unit): Unit = {
    f(plan, scope, role)
    plan match {
      case s: SingleStatement =>
        visitPlan(s.parsedPlan, scope, role)(f)
      case _ =>
        visitNonChildSlots(plan, scope, role)(f)
        visitChildPlans(plan, scope, role)(f)
    }
  }

  private def visitChildPlans(
      parent: LogicalPlan,
      scope: CteScope,
      role: TableRefRole)(f: (LogicalPlan, CteScope, TableRefRole) => Unit): Unit = {
    val nextScope = childScope(parent, scope)
    parent match {
      case m: MergeIntoTable =>
        visitPlan(m.targetTable, scope, TableRefRole.Target)(f)
        visitPlan(m.sourceTable, scope, TableRefRole.Source)(f)
      case i: UnresolvedInsert =>
        visitPlan(i.table, scope, TableRefRole.Target)(f)
        visitPlan(i.query, nextScope, TableRefRole.Source)(f)
      case i: InsertIntoStatement =>
        visitPlan(i.query, nextScope, TableRefRole.Source)(f)
      case c: CreateTableAsSelect =>
        visitPlan(c.name, scope, TableRefRole.Target)(f)
        visitPlan(c.query, nextScope, TableRefRole.Source)(f)
      case r: ReplaceTableAsSelect =>
        visitPlan(r.name, scope, TableRefRole.Target)(f)
        visitPlan(r.query, nextScope, TableRefRole.Source)(f)
      case cv: CreateView =>
        visitPlan(cv.child, scope, TableRefRole.Target)(f)
        visitPlan(cv.query, nextScope, TableRefRole.Source)(f)
      case cts: CacheTableAsSelect =>
        visitPlan(cts.plan, nextScope, TableRefRole.Source)(f)
      case _: CacheTable =>
        // Name is taken from multipartIdentifier or the non-child `table` slot.
      case _: View =>
        // The view identity is the view; do not collect tables from its body.
      case w: V2WriteCommand =>
        visitPlan(w.table, scope, TableRefRole.Target)(f)
        visitPlan(w.query, nextScope, TableRefRole.Source)(f)
      case w: WithCTE =>
        w.cteDefs.foreach(d => visitPlan(d, nextScope, TableRefRole.Source)(f))
        visitPlan(w.plan, nextScope, role)(f)
      case SubqueryAlias(_, child) =>
        visitPlan(child, nextScope, role)(f)
      case _ =>
        parent.subqueries.foreach(sq => visitPlan(sq, nextScope, TableRefRole.Source)(f))
        parent.children.foreach { child =>
          val childRole =
            if (isTargetTableChild(parent)) TableRefRole.Target else role
          visitPlan(child, nextScope, childRole)(f)
        }
    }
  }

  /** Visit plan slots that are not exposed via `children` / subqueries. */
  private def visitNonChildSlots(
      plan: LogicalPlan,
      scope: CteScope,
      role: TableRefRole)(f: (LogicalPlan, CteScope, TableRefRole) => Unit): Unit = {
    plan match {
      case w: UnresolvedWith =>
        // A CTE definition sees the aliases defined before it, plus its own
        // name when the clause is RECURSIVE. Later aliases are not in scope,
        // so a definition naming one refers to the real table.
        var definitionScope = scope
        w.cteRelations.foreach { cteRelation =>
          val normalized = normalizeCteName(cteRelation.name)
          val bodyScope =
            if (w.allowRecursion) definitionScope + normalized else definitionScope
          visitPlan(cteRelation.plan, bodyScope, TableRefRole.Source)(f)
          definitionScope += normalized
        }
      // An INSERT target is never resolved against a CTE, so no alias shadows it.
      case i: InsertIntoStatement =>
        visitPlan(i.table, Set.empty, TableRefRole.Target)(f)
      case c: CacheTable if c.multipartIdentifier.isEmpty =>
        visitPlan(c.table, scope, TableRefRole.Target)(f)
      case c: CompoundBody =>
        c.handlers.foreach(h => visitPlan(h, scope, TableRefRole.Source)(f))
      case s: SimpleCaseStatement =>
        s.elseBody.foreach(b => visitPlan(b, scope, TableRefRole.Source)(f))
      case ExplainCommand(logicalPlan, _) =>
        visitPlan(logicalPlan, scope, TableRefRole.Source)(f)
      case DescribeQueryCommand(_, queryPlan) =>
        visitPlan(queryPlan, scope, TableRefRole.Source)(f)
      case _ =>
    }
  }

  private def tableIdentifierParts(id: TableIdentifier): Seq[String] =
    id.catalog.toSeq ++ id.database.toSeq :+ id.table

  private def persistedTableParts(id: TableIdentifier): Seq[String] = {
    val parts = tableIdentifierParts(id)
    if (id.catalog.isDefined) {
      parts
    } else {
      CatalogManager.SESSION_CATALOG_NAME +: parts
    }
  }

  private final case class PlanReferences(
      targetTables: Seq[Seq[String]],
      sourceTables: Seq[Seq[String]],
      functions: Seq[Seq[String]],
      parameterMarkers: Option[JObject])

  /**
   * Collect multipart table/view identifiers, function names, and parameter
   * markers for lineage in a single deep walk. Target references name the
   * table/view a DML or DDL statement writes to or alters; source references
   * name tables read in FROM clauses and query bodies. A single-part name is
   * dropped only when a CTE alias in scope at that node shadows it. Function /
   * variable identifiers are not collected as tables. Deduplicates while
   * preserving first-seen order within each category.
   */
  private def collectPlanReferences(plan: LogicalPlan): PlanReferences = {
    val targetTables = mutable.LinkedHashSet.empty[Seq[String]]
    val sourceTables = mutable.LinkedHashSet.empty[Seq[String]]
    val functions = mutable.LinkedHashSet.empty[Seq[String]]
    val namedParams = mutable.LinkedHashSet.empty[String]
    var unnamedCount = 0

    def isCteName(parts: Seq[String], scope: CteScope): Boolean = parts match {
      case Seq(name) => scope.contains(normalizeCteName(name))
      case _ => false
    }

    def addTable(parts: Seq[String], scope: CteScope, role: TableRefRole): Unit = {
      if (parts.nonEmpty && !isCteName(parts, scope)) {
        role match {
          case TableRefRole.Target => targetTables += parts
          case TableRefRole.Source => sourceTables += parts
        }
      }
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

    def addTarget(parts: Seq[String]): Unit = {
      if (parts.nonEmpty) targetTables += parts
    }

    foreachPlanDeep(plan) { (p, scope, role) =>
      foreachExpressionDeep(p)(visitExpr)
      def add(parts: Seq[String]): Unit = addTable(parts, scope, role)
      p match {
        case u: UnresolvedRelation => add(u.multipartIdentifier)
        case u: UnresolvedTable => add(u.multipartIdentifier)
        case u: UnresolvedView => add(u.multipartIdentifier)
        case u: UnresolvedTableOrView => add(u.multipartIdentifier)
        case u: UnresolvedIdentifier if role == TableRefRole.Target => add(u.nameParts)
        case c: CreateViewCommand =>
          addTarget(c.viewType match {
            case LocalTempView => sessionIdent(c.name.table)
            case GlobalTempView =>
              Seq(
                CatalogManager.SESSION_CATALOG_NAME,
                SQLConf.get.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE),
                c.name.table)
            case _ => tableIdentifierParts(c.name)
          })
        case c: CreateTempViewUsing => addTarget(sessionIdent(c.tableIdent.table))
        case c: CreateDataSourceTableCommand =>
          addTarget(persistedTableParts(c.table.identifier))
        case c: CreateTableCommand =>
          addTarget(persistedTableParts(c.table.identifier))
        case c: DropTableCommand =>
          addTarget(
            if (c.isView && c.tableName.database.isEmpty) {
              sessionIdent(c.tableName.table)
            } else {
              persistedTableParts(c.tableName)
            })
        case c: DropTempViewCommand =>
          addTarget(tempViewParts(c.ident))
        case c: CacheTable if c.multipartIdentifier.nonEmpty =>
          addTarget(c.multipartIdentifier)
        case u: UnresolvedTableValuedFunction => addFunction(u.name)
        case _: CTERelationRef =>
        case r: DataSourceV2Relation =>
          v2RelationParts(r).foreach(add)
        case r: LogicalRelation =>
          logicalRelationParts(r).foreach(add)
        case h: HiveTableRelation =>
          add(tableIdentifierParts(h.tableMeta.identifier))
        case v: View =>
          add(viewParts(v))
        case r: ResolvedTable =>
          add(catalogIdentParts(r.catalog.name(), r.identifier))
        case r: ResolvedPersistentView =>
          add(catalogIdentParts(r.catalog.name(), r.identifier))
        case r: ResolvedTempView =>
          add(tempViewParts(r.identifier))
        case r: ResolvedIdentifier if role == TableRefRole.Target =>
          add(catalogIdentParts(r.catalog.name(), r.identifier))
        case _: Range =>
          addFunction(FunctionRegistry.builtinFunctionIdentifier("range").nameParts)
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
    PlanReferences(targetTables.toSeq, sourceTables.toSeq, functions.toSeq, markers)
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

  /** Select-list items with SQL types from an analyzed query's output. */
  private def collectTypedSelectList(
      analyzed: LogicalPlan,
      parsed: LogicalPlan): Seq[JObject] = {
    val query = primaryQueryPlan(analyzed)
    if (query.resolved && query.output.nonEmpty) {
      query.output.map { attr =>
        JObject(
          "name" -> partsToJArray(Seq(attr.name)),
          "type" -> DescribeRelationJsonCommand.jsonType(
            CharVarcharUtils.getRawType(attr.metadata).getOrElse(attr.dataType)))
      }
    } else {
      collectSelectList(parsed)
    }
  }

  private def uniqueIdentities(
      ids: Seq[Seq[String]],
      session: ClassicSparkSession): Seq[Seq[String]] = {
    val sensitive = session.sessionState.conf.caseSensitiveAnalysis
    val seen = mutable.LinkedHashSet.empty[Seq[String]]
    ids.filter { parts =>
      val key = if (sensitive) parts else parts.map(_.toLowerCase(Locale.ROOT))
      seen.add(key)
    }
  }

  private def sessionIdent(name: String): Seq[String] =
    Seq(CatalogManager.SYSTEM_CATALOG_NAME, CatalogManager.SESSION_NAMESPACE, name)

  private def catalogIdentParts(catalog: String, ident: Identifier): Seq[String] =
    Seq(catalog) ++ ident.namespace() :+ ident.name()

  private def v2RelationParts(r: DataSourceV2Relation): Option[Seq[String]] =
    (r.catalog, r.identifier) match {
      case (Some(catalog), Some(ident)) => Some(catalogIdentParts(catalog.name(), ident))
      case _ => None
    }

  private def logicalRelationParts(r: LogicalRelation): Option[Seq[String]] =
    r.catalogTable.map { t =>
      val id = t.identifier
      if (id.catalog.isEmpty && id.database.isDefined) {
        CatalogManager.SESSION_CATALOG_NAME +: tableIdentifierParts(id)
      } else {
        tableIdentifierParts(id)
      }
    }.orElse {
      r.relation match {
        case fs: HadoopFsRelation =>
          fs.location.rootPaths.headOption.map(p => Seq(p.toString))
        case _ => None
      }
    }

  private def viewParts(v: View): Seq[String] = {
    val id = v.desc.identifier
    if (v.isTempView) {
      val globalTemp = SQLConf.get.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE)
      if (id.database.exists(_.equalsIgnoreCase(globalTemp))) {
        Seq(id.catalog.getOrElse(CatalogManager.SESSION_CATALOG_NAME)) ++
          id.database.toSeq :+ id.table
      } else {
        sessionIdent(id.table)
      }
    } else {
      tableIdentifierParts(id)
    }
  }

  private def tempViewParts(ident: Identifier): Seq[String] = {
    val ns = ident.namespace()
    val globalTemp = SQLConf.get.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE)
    if (ns.length == 1 && ns.head.equalsIgnoreCase(globalTemp)) {
      Seq(CatalogManager.SESSION_CATALOG_NAME, ns.head, ident.name())
    } else if (ns.isEmpty) {
      sessionIdent(ident.name())
    } else {
      catalogIdentParts(CatalogManager.SESSION_CATALOG_NAME, ident)
    }
  }

  private def isTempViewCreate(plan: LogicalPlan): Boolean = plan match {
    case c: CreateViewCommand =>
      c.viewType == LocalTempView || c.viewType == GlobalTempView
    case _: CreateTempViewUsing => true
    case _ => false
  }

  private def qualifyTarget(
      parts: Seq[String],
      plan: LogicalPlan,
      session: ClassicSparkSession): Seq[String] = {
    if (isTempViewCreate(plan)) {
      sessionIdent(parts.last)
    } else {
      qualifyWithCurrentNamespace(parts, session)
    }
  }

  private def qualifyRoutine(
      parts: Seq[String],
      session: ClassicSparkSession): Seq[String] = {
    val catalog = session.sessionState.catalog
    val tempFirst =
      session.sessionState.conf.getConf(SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER) == "first"
    def isTemp(name: String): Boolean =
      catalog.isTemporaryFunction(FunctionIdentifier(name))
    def isBuiltin(name: String): Boolean = {
      val id = FunctionIdentifier(name)
      FunctionRegistry.builtin.functionExists(id) ||
        TableFunctionRegistry.builtin.functionExists(id)
    }
    def isSessionBuiltin(name: String): Boolean = {
      val id = FunctionIdentifier(name)
      !isTemp(name) && !isBuiltin(name) &&
        (session.sessionState.functionRegistry.functionExists(id) ||
          session.sessionState.tableFunctionRegistry.functionExists(id))
    }
    def builtinIdent(name: String): Seq[String] =
      FunctionRegistry.builtinFunctionIdentifier(name).nameParts
    parts match {
      case Seq(name) if tempFirst && isTemp(name) =>
        sessionIdent(name)
      case Seq(name) if isBuiltin(name) || isSessionBuiltin(name) =>
        builtinIdent(name)
      case Seq(name) if isTemp(name) =>
        sessionIdent(name)
      case Seq(ns, name) if isSystemNamespace(ns) =>
        Seq(CatalogManager.SYSTEM_CATALOG_NAME, ns.toLowerCase(Locale.ROOT), name)
      case Seq(cat, ns, name)
          if cat.equalsIgnoreCase(CatalogManager.SYSTEM_CATALOG_NAME) &&
            isSystemNamespace(ns) =>
        Seq(CatalogManager.SYSTEM_CATALOG_NAME, ns.toLowerCase(Locale.ROOT), name)
      case other =>
        qualifyWithCurrentNamespace(other, session)
    }
  }

  private def qualifyWithCurrentNamespace(
      parts: Seq[String],
      session: ClassicSparkSession): Seq[String] = {
    val mgr = session.sessionState.catalogManager
    val catalogName = mgr.currentCatalog.name()
    val namespace = mgr.currentNamespace.toSeq
    parts match {
      case Seq(name) => Seq(catalogName) ++ namespace :+ name
      case other if mgr.isCatalogRegistered(other.head) => other
      case other => catalogName +: other
    }
  }

  private def isSystemNamespace(ns: String): Boolean =
    ns.equalsIgnoreCase(CatalogManager.BUILTIN_NAMESPACE) ||
      ns.equalsIgnoreCase(CatalogManager.SESSION_NAMESPACE)

  private def primaryQueryPlan(plan: LogicalPlan): LogicalPlan = plan match {
    case UnresolvedWith(child, _, _) => primaryQueryPlan(child)
    case WithCTE(child, _) => primaryQueryPlan(child)
    case UnresolvedInsert(_, _, _, query, _, _, _, _, _) =>
      primaryQueryPlan(query)
    case i: InsertIntoStatement => primaryQueryPlan(i.query)
    case d: DataWritingCommand => primaryQueryPlan(d.query)
    case w: V2WriteCommand => primaryQueryPlan(w.query)
    case c: CreateDataSourceTableAsSelectCommand => primaryQueryPlan(c.query)
    case i: InsertIntoDataSourceCommand => primaryQueryPlan(i.query)
    case c: CreateTableAsSelect => primaryQueryPlan(c.query)
    case r: ReplaceTableAsSelect => primaryQueryPlan(r.query)
    case c: CreateView => primaryQueryPlan(c.query)
    case c: CreateViewCommand => primaryQueryPlan(c.plan)
    case c: CacheTableAsSelect => primaryQueryPlan(c.plan)
    case ExplainCommand(logicalPlan, _) => primaryQueryPlan(logicalPlan)
    case DescribeQueryCommand(_, queryPlan) => primaryQueryPlan(queryPlan)
    case cmd: LeafRunnableCommand =>
      cmd.innerChildren.collectFirst { case p: LogicalPlan => primaryQueryPlan(p) }
        .getOrElse(cmd)
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
