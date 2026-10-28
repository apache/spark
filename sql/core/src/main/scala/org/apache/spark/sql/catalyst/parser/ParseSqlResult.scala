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
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin, SQLQueryContext}
import org.apache.spark.sql.catalyst.trees.TreePattern.PARAMETER
import org.apache.spark.sql.classic.{SparkSession => ClassicSparkSession}
import org.apache.spark.sql.connector.catalog.{CatalogManager, PathElement}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.exceptions.SqlScriptingException
import org.apache.spark.sql.execution.{CommandExecutionMode, SparkSqlParser}
import org.apache.spark.sql.execution.command.{
  CreateViewCommand,
  DescribeQueryCommand,
  DescribeRelationJsonCommand,
  ExplainCommand
}
import org.apache.spark.sql.execution.datasources.CreateTempViewUsing
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}

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
 * Optional ANALYZE-mode catalog context for [[ParseSqlResult.fromSql]].
 * `None` on a field means inherit the invoker session value.
 */
case class ParseSqlAnalyzeContext(
    catalog: Option[String] = None,
    schema: Option[String] = None,
    path: Option[String] = None,
    variables: Seq[(String, String)] = Nil,
    relations: Seq[(String, String)] = Nil,
    parameterMarkers: Seq[(String, String)] = Nil) {
  def isEmpty: Boolean = {
    catalog.isEmpty && schema.isEmpty && path.isEmpty &&
      variables.isEmpty && relations.isEmpty && parameterMarkers.isEmpty
  }
}

object ParseSqlAnalyzeContext {
  val empty: ParseSqlAnalyzeContext = ParseSqlAnalyzeContext()
}

/**
 * Parses a SQL batch string and returns a compact JSON array describing its
 * statements. `PARSE` mode is unresolved (no catalog resolution). `ANALYZE`
 * mode also types selected columns and fully qualifies relation / routine
 * names using the active session catalog. ANALYZE inherits the invoker's
 * current catalog, schema, and SQL path unless [[ParseSqlAnalyzeContext]]
 * overrides them.
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
 * continues with later statements. Nested `queryContext` indexes are 1-based
 * Unicode code points relative to the individual statement, while `start` is
 * relative to the original batch. A SQL script object also carries a
 * `statements` array for its nested statements and control-flow conditions,
 * with batch-relative spans and error indexes relative to the script. An
 * empty or closed-comment-only batch produces an empty array. Only
 * [[ParseException]] / [[SqlScriptingException]] are converted to JSON;
 * unexpected / internal failures propagate so the function fails.
 */
object ParseSqlResult {

  private val parser: ThreadLocal[SparkSqlParser] =
    ThreadLocal.withInitial(() => new SparkSqlParser())

  /** Parse `sql` as a batch and render the JSON result array. */
  def fromSql(
      sql: String,
      mode: String = ParseSqlMode.Parse,
      context: ParseSqlAnalyzeContext = ParseSqlAnalyzeContext.empty): String = {
    val split = parser.get().splitStatementsWithPositions(sql)
    val statements = split.completeStatements ++ split.partialStatement
    val json = if (mode == ParseSqlMode.Analyze) {
      analyzeBatch(sql, statements, context)
    } else {
      JArray(statements.map { segment =>
        parsePlanOrError(sql, segment) match {
          case ParseFailed(errorObj) => errorObj
          case Parsed(seg, script: CompoundBody) =>
            val span = segmentSpan(sql, seg)
            val nested = mutable.ListBuffer.empty[JObject]
            walkScript(script, Nil, { (plan, origin, _) =>
              nested += successJson(plan, None, None, None, nestedSpan(span, origin))
              None
            })
            scriptJson(script, span, nested.toSeq, session = None)
          case Parsed(seg, plan) =>
            successJson(plan, analyzed = None, analyzeError = None, None, segmentSpan(sql, seg))
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
   * Parse every statement, then analyze with optional context overrides.
   * Statements are analyzed in order without being executed: each sees the
   * variables, tables, and views declared or created by earlier statements
   * (see [[AnalyzeState]]). SQL scripts are walked statement by statement in
   * lexical scope order (see [[walkScript]]).
   * Analysis / context failures stay JSON (`analyze_success` false) with
   * whatever parse and catalog qualification is already known.
   */
  private def analyzeBatch(
      batch: String,
      statements: Seq[PositionedSqlStatement],
      context: ParseSqlAnalyzeContext): JArray = {
    val parsed = statements.map(parsePlanOrError(batch, _))
    val session = ClassicSparkSession.getActiveSession
      .orElse(ClassicSparkSession.getDefaultSession)
      .getOrElse(throw QueryExecutionErrors.parseSqlAnalyzeRequiresSession())
    try {
      withAnalyzeContext(session, context) { env =>
        val state = new AnalyzeState(env)
        JArray(parsed.map {
          case ParseFailed(errorObj) => errorObj
          case Parsed(segment, script: CompoundBody) =>
            val span = segmentSpan(batch, segment)
            val nested = mutable.ListBuffer.empty[JObject]
            walkScript(script, Nil, { (plan, origin, scopes) =>
              val (json, analyzed) = state.statement(plan, nestedSpan(span, origin), scopes)
              nested += json
              analyzed
            })
            scriptJson(script, span, nested.toSeq, Some(session))
          case Parsed(segment, plan) =>
            state.statement(plan, segmentSpan(batch, segment), Nil)._1
        }.toList)
      }
    } catch {
      case e: SparkThrowable with Throwable =>
        JArray(parsed.map {
          case ParseFailed(errorObj) => errorObj
          case Parsed(segment, plan) =>
            successJson(plan, None, Some(e), Some(session), segmentSpan(batch, segment))
        }.toList)
    }
  }

  /** Character span of a statement object and the text its error indexes refer to. */
  private case class StatementSpan(start: Int, length: Int, contextText: String)

  private def segmentSpan(batch: String, segment: PositionedSqlStatement): StatementSpan =
    StatementSpan(charStart(batch, segment), charLength(segment), segment.statement)

  /**
   * Span of a statement nested in a SQL script. Parser origins are ANTLR
   * code-point offsets into the enclosing top-level statement, so nested error
   * indexes stay relative to that statement.
   */
  private def nestedSpan(parent: StatementSpan, origin: Origin): StatementSpan =
    (origin.startIndex, origin.stopIndex) match {
      case (Some(from), Some(to)) if to >= from =>
        StatementSpan(parent.start + from, to - from + 1, parent.contextText)
      case _ => parent
    }

  /** A lexical SQL script scope whose locals and cursors are known so far. */
  private final class LocalScope(label: String) {
    private val declared = mutable.ListBuffer.empty[(String, DataType)]
    private val cursors = mutable.ListBuffer.empty[(String, String)]
    def declare(name: String, dataType: DataType): Unit = declared += name -> dataType
    def declareCursor(name: String, queryText: String): Unit = cursors += name -> queryText
    def snapshot: SuppliedLocalScope =
      SuppliedLocalScope(label, declared.toList, cursors.toList)
  }

  /**
   * Visits one statement of a SQL script with the local scopes in effect
   * (innermost first) and returns its analyzed plan, if any.
   */
  private type ScriptStatementVisitor =
    (LogicalPlan, Origin, List[LocalScope]) => Option[LogicalPlan]

  /**
   * Walk a SQL script in source order without executing it. Every leaf
   * statement and control-flow condition is visited once, whether or not the
   * interpreter would reach it. A BEGIN ... END block opens a local scope
   * (named by its label); a FOR loop opens a scope named by its loop variable
   * that declares one local per query column, as the interpreter does.
   * Exception handler bodies are visited after the statements of their block.
   */
  private def walkScript(
      node: CompoundPlanStatement,
      scopes: List[LocalScope],
      visit: ScriptStatementVisitor): Unit = {
    def walk(child: CompoundPlanStatement, in: List[LocalScope]): Unit =
      walkScript(child, in, visit)
    def condition(expr: Expression): LogicalPlan =
      Project(Seq(Alias(expr, "condition")()), OneRowRelation())
    node match {
      case s: SingleStatement =>
        visit(s.parsedPlan, s.origin, scopes)
      case body: CompoundBody =>
        val inner = if (body.isScope) new LocalScope(body.label.getOrElse("")) :: scopes else scopes
        body.collection.foreach(walk(_, inner))
        body.handlers.foreach(walk(_, inner))
      case handler: ExceptionHandler if handler.body.isScope =>
        walk(handler.body, scopes)
      case handler: ExceptionHandler =>
        // A single-statement handler body carries the origin of the whole
        // DECLARE ... HANDLER; its parsed plan has the statement's own span.
        handler.body.collection.foreach {
          case s: SingleStatement =>
            val planOrigin = s.parsedPlan.origin
            val origin =
              if (planOrigin.startIndex.isDefined && planOrigin.stopIndex.isDefined) {
                planOrigin
              } else {
                s.origin
              }
            visit(s.parsedPlan, origin, scopes)
          case other => walk(other, scopes)
        }
      case i: IfElseStatement =>
        i.conditions.zip(i.conditionalBodies).foreach { case (c, b) =>
          walk(c, scopes)
          walk(b, scopes)
        }
        i.elseBody.foreach(walk(_, scopes))
      case c: SearchedCaseStatement =>
        c.conditions.zip(c.conditionalBodies).foreach { case (cond, b) =>
          walk(cond, scopes)
          walk(b, scopes)
        }
        c.elseBody.foreach(walk(_, scopes))
      case c: SimpleCaseStatement =>
        visit(condition(c.caseVariableExpression), c.caseVariableExpression.origin, scopes)
        c.conditionExpressions.zip(c.conditionalBodies).foreach { case (e, b) =>
          visit(condition(e), e.origin, scopes)
          walk(b, scopes)
        }
        c.elseBody.foreach(walk(_, scopes))
      case w: WhileStatement =>
        walk(w.condition, scopes)
        walk(w.body, scopes)
      case r: RepeatStatement =>
        walk(r.body, scopes)
        walk(r.condition, scopes)
      case l: LoopStatement =>
        walk(l.body, scopes)
      case f: ForStatement =>
        val analyzed = visit(f.query.parsedPlan, f.query.origin, scopes)
        val loopScope = new LocalScope(f.variableName.getOrElse(""))
        analyzed.foreach(_.output.foreach(a => loopScope.declare(a.name, a.dataType)))
        walk(f.body, loopScope :: scopes)
      case _: LeaveStatement | _: IterateStatement | _: ErrorCondition =>
    }
  }

  /**
   * Analysis state carried across the statements of one ANALYZE batch. It
   * starts from the caller-supplied variables and relations and records the
   * effect of each analyzed statement on later ones, without executing
   * anything:
   *  - DECLARE in a SQL script adds a local to the innermost scope; outside a
   *    script it adds a session variable. DROP TEMPORARY VARIABLE removes it.
   *  - DECLARE CURSOR in a SQL script adds a cursor to the innermost scope.
   *    OPEN / FETCH / CLOSE resolve it by name (and label) like the interpreter.
   *  - CREATE [OR REPLACE] TABLE / VIEW / TEMP VIEW (column list, AS query,
   *    LIKE, or a temp view USING with a schema) adds a schema-only relation of
   *    that identity. DROP TABLE / VIEW removes a relation known this way.
   * Objects dropped that exist in the catalog stay visible, and statements that
   * change a schema in place (for example ALTER TABLE) are not tracked.
   */
  private final class AnalyzeState(env: AnalyzeEnv) {
    private val session = env.session
    private def mgr: CatalogManager = session.sessionState.catalogManager
    private def resolver: (String, String) => Boolean = session.sessionState.conf.resolver

    private var variables: List[(Seq[String], DataType)] = env.variables.map {
      case (name, dataType) => SuppliedVariableRegistry.canonicalize(name) -> dataType
    }.toList
    private var relations: List[(Seq[String], StructType)] = env.relations.map {
      case (name, schema) => SuppliedRelationRegistry.canonicalize(mgr, name) -> schema
    }.toList

    /**
     * Analyze one statement with the known objects and local `scopes`
     * (innermost first), render its JSON, then record its effect.
     */
    def statement(
        plan: LogicalPlan,
        span: StatementSpan,
        scopes: List[LocalScope]): (JObject, Option[LogicalPlan]) = {
      SuppliedVariableRegistry.create(variables).runWith {
        SuppliedRelationRegistry.create(mgr, relations).runWith {
          SuppliedLocalScopeRegistry.create(scopes.reverse.map(_.snapshot)).runWith {
            val (analyzed, error, cursorQuery) = analyzeStatement(plan)
            val json = successJson(
              plan, analyzed, error, Some(session), span, cursorQuery)
            record(plan, analyzed, scopes)
            (json, analyzed)
          }
        }
      }
    }

    private def analyze(
        plan: LogicalPlan): (Option[LogicalPlan], Option[SparkThrowable with Throwable]) = {
      try {
        val toAnalyze = bindSuppliedParameterMarkers(plan, env.parameterMarkers)
        val analyzed =
          session.sessionState.executePlan(toAnalyze, CommandExecutionMode.SKIP).analyzed
        (Some(analyzed), None)
      } catch {
        case e: SparkThrowable with Throwable => (None, Some(e))
      }
    }

    /**
     * Analyze `plan`. For `DECLARE CURSOR`, also parse and analyze the cursor
     * query with the same overlays so a later FETCH can see its tables and
     * a failed query is reported on the DECLARE.
     */
    private def analyzeStatement(plan: LogicalPlan): (
        Option[LogicalPlan],
        Option[SparkThrowable with Throwable],
        Option[LogicalPlan]) = plan match {
      case d: DeclareCursor =>
        val (analyzed, error) = analyze(d)
        if (error.isDefined) {
          (analyzed, error, None)
        } else {
          val (query, queryError) = analyzeCursorQuery(d.queryText)
          if (queryError.isDefined) (None, queryError, None) else (analyzed, None, query)
        }
      case _ =>
        val (analyzed, error) = analyze(plan)
        (analyzed, error, None)
    }

    private def analyzeCursorQuery(
        queryText: String): (Option[LogicalPlan], Option[SparkThrowable with Throwable]) = {
      try {
        analyze(parser.get().parsePlan(queryText))
      } catch {
        case e: SparkThrowable with Throwable => (None, Some(e))
      }
    }

    private def record(
        plan: LogicalPlan,
        analyzed: Option[LogicalPlan],
        scopes: List[LocalScope]): Unit = plan match {
      case c: CreateVariable =>
        variableType(c, analyzed).foreach { dataType =>
          c.names.foreach {
            case UnresolvedIdentifier(nameParts, _) =>
              scopes match {
                case scope :: _ => scope.declare(nameParts.last, dataType)
                case Nil => declareVariable(nameParts, dataType)
              }
            case _ =>
          }
        }
      case DropVariable(UnresolvedIdentifier(nameParts, _), _) if scopes.isEmpty =>
        val identity = SuppliedVariableRegistry.canonicalize(nameParts)
        variables = variables.filterNot { case (name, _) => sameName(name, identity) }
      case c: CreateTable =>
        nameOf(c.name).filterNot(n => c.ignoreIfExists && relationExists(n)).foreach {
          createRelation(_, StructType(c.columns.map(col =>
            StructField(col.name, col.dataType, col.nullable))))
        }
      case r: ReplaceTable =>
        nameOf(r.name).foreach {
          createRelation(_, StructType(r.columns.map(col =>
            StructField(col.name, col.dataType, col.nullable))))
        }
      case c: CreateTableAsSelect =>
        nameOf(c.name).filterNot(n => c.ignoreIfExists && relationExists(n)).foreach { name =>
          querySchema(c.query).foreach(createRelation(name, _))
        }
      case r: ReplaceTableAsSelect =>
        nameOf(r.name).foreach(name => querySchema(r.query).foreach(createRelation(name, _)))
      case c: CreateTableLike =>
        for {
          name <- nameOf(c.name) if !(c.ifNotExists && relationExists(name))
          source <- nameOf(c.source)
          schema <- querySchema(UnresolvedRelation(source))
        } createRelation(name, schema)
      case v: CreateView =>
        nameOf(v.child).filterNot(n => v.allowExisting && relationExists(n)).foreach { name =>
          querySchema(v.query).foreach(s =>
            createRelation(name, withColumnNames(s, v.userSpecifiedColumns)))
        }
      case v: CreateViewCommand =>
        val name = viewIdentity(v.name, v.viewType)
        if (!(v.allowExisting && relationExists(name))) {
          querySchema(v.plan).foreach(s =>
            createRelation(name, withColumnNames(s, v.userSpecifiedColumns)))
        }
      case v: CreateTempViewUsing =>
        val viewType = if (v.global) GlobalTempView else LocalTempView
        v.userSpecifiedSchema.foreach(createRelation(viewIdentity(v.tableIdent, viewType), _))
      case d: DropTable => nameOf(d.child).foreach(dropRelation)
      case d: DropView => nameOf(d.child).foreach(dropRelation)
      case d: DeclareCursor =>
        scopes.headOption.foreach(_.declareCursor(d.cursorName, d.queryText))
      case _ =>
    }

    /** The declared type, else the type of the analyzed DEFAULT expression. */
    private def variableType(
        c: CreateVariable,
        analyzed: Option[LogicalPlan]): Option[DataType] = c.defaultExpr.child match {
      case cast: Cast => Some(cast.dataType)
      case Literal(null, dataType) => Some(dataType)
      case _ =>
        analyzed.flatMap(_.collectFirst {
          case v: CreateVariable if v.defaultExpr.child.resolved => v.defaultExpr.child.dataType
        })
    }

    private def declareVariable(nameParts: Seq[String], dataType: DataType): Unit = {
      val identity = SuppliedVariableRegistry.canonicalize(nameParts)
      variables = (identity -> dataType) ::
        variables.filterNot { case (name, _) => sameName(name, identity) }
    }

    private def nameOf(plan: LogicalPlan): Option[Seq[String]] = plan match {
      case UnresolvedIdentifier(nameParts, _) => Some(nameParts)
      case u: UnresolvedTableOrView => Some(u.multipartIdentifier)
      case u: UnresolvedTable => Some(u.multipartIdentifier)
      case u: UnresolvedView => Some(u.multipartIdentifier)
      case u: UnresolvedRelation => Some(u.multipartIdentifier)
      case _ => None
    }

    private def viewIdentity(name: TableIdentifier, viewType: ViewType): Seq[String] =
      viewType match {
        case LocalTempView => SuppliedRelationRegistry.sessionIdentity(name.table)
        case GlobalTempView => Seq(session.sessionState.catalog.globalTempDatabase, name.table)
        case _ => tableIdentifierParts(name)
      }

    private def withColumnNames(
        schema: StructType,
        columns: Seq[(String, Option[String])]): StructType = {
      if (columns.nonEmpty && columns.length == schema.length) {
        StructType(schema.fields.zip(columns).map { case (f, (name, _)) => f.copy(name = name) })
      } else {
        schema
      }
    }

    private def querySchema(query: LogicalPlan): Option[StructType] =
      analyze(query)._1.map(_.schema)

    private def relationExists(nameParts: Seq[String]): Boolean =
      querySchema(UnresolvedRelation(nameParts)).isDefined

    private def createRelation(nameParts: Seq[String], schema: StructType): Unit = {
      if (schema.nonEmpty) {
        val identity = SuppliedRelationRegistry.canonicalize(mgr, nameParts)
        relations = (identity -> schema) ::
          relations.filterNot { case (name, _) => sameName(name, identity) }
      }
    }

    /** A one-part name drops the temp view of that name first, as DROP does. */
    private def dropRelation(nameParts: Seq[String]): Unit = {
      val sessionIdentity = SuppliedRelationRegistry.sessionIdentity(nameParts.last)
      val identity =
        if (nameParts.length == 1 && relations.exists(r => sameName(r._1, sessionIdentity))) {
          sessionIdentity
        } else {
          SuppliedRelationRegistry.canonicalize(mgr, nameParts)
        }
      relations = relations.filterNot { case (name, _) => sameName(name, identity) }
    }

    private def sameName(left: Seq[String], right: Seq[String]): Boolean =
      left.length == right.length && left.zip(right).forall { case (a, b) => resolver(a, b) }
  }

  /**
   * JSON for a SQL script: the script's own statement object plus a
   * `statements` array with one object per nested statement and control-flow
   * condition, ordered by `start`. In ANALYZE mode the script's
   * `analyze_success` is true only when every nested statement analyzed, and
   * its table / routine references are the union of the qualified nested ones.
   */
  private def scriptJson(
      script: CompoundBody,
      span: StatementSpan,
      nested: Seq[JObject],
      session: Option[ClassicSparkSession]): JObject = {
    val ordered = nested.zipWithIndex.sortBy { case (obj, index) =>
      val start = obj \ "start" match {
        case JInt(n) => n.toInt
        case _ => 0
      }
      (start, index)
    }.map(_._1)
    val base = successJson(script, None, None, None, span)
    val refKeys = Seq("target_table_references", "source_table_references", "function_references")
    val fields = session match {
      case None => base.obj
      case Some(_) =>
        val analyzeSuccess =
          "analyze_success" -> JBool(ordered.forall(o => (o \ "analyze_success") == JBool(true)))
        val nestedRefs = refKeys.flatMap { key =>
          val refs = ordered.flatMap(o => o \ key match {
            case JArray(values) => values
            case _ => Nil
          }).distinct
          if (refs.isEmpty) None else Some(key -> JArray(refs.toList))
        }
        val kept = base.obj.filterNot(f => refKeys.contains(f._1))
        val (head, tail) = kept.splitAt(kept.indexWhere(_._1 == "statement_code") + 1)
        head ++ (analyzeSuccess +: nestedRefs) ++ tail
    }
    JObject(fields :+ ("statements" -> JArray(ordered.toList)))
  }

  /** Parsed plan, or parse-failure JSON for this statement. */
  private def parsePlanOrError(
      batch: String,
      segment: PositionedSqlStatement): ParsedStatement = {
    val sql = segment.statement
    try {
      // Do not inherit the outer query's origin from the parse_sql expression.
      // Errors and parsed nodes refer to the individual statement.
      val origin = if (sql.nonEmpty) {
        Origin(startIndex = Some(0), stopIndex = Some(sql.length - 1), sqlText = Some(sql))
      } else {
        Origin(sqlText = Some(sql))
      }
      CurrentOrigin.withOrigin(origin) {
        Parsed(segment, parser.get().parsePlan(sql))
      }
    } catch {
      case e: ParseException =>
        ParseFailed(errorJson(e, segmentSpan(batch, segment)))
      case e: SqlScriptingException =>
        ParseFailed(errorJson(e, segmentSpan(batch, segment)))
    }
  }

  /** Session and caller-supplied objects for one ANALYZE batch. */
  private case class AnalyzeEnv(
      session: ClassicSparkSession,
      parameterMarkers: Seq[(String, DataType)],
      variables: Seq[(Seq[String], DataType)],
      relations: Seq[(Seq[String], StructType)])

  /**
   * Run `f` with optional catalog / schema / SQL path overrides, then restore
   * the invoker session. `None` on a field keeps the current value. Supplied
   * variables and relations are validated here and overlaid per statement.
   */
  private def withAnalyzeContext[T](
      session: ClassicSparkSession,
      context: ParseSqlAnalyzeContext)(f: AnalyzeEnv => T): T = {
    val parsedVars = context.variables.map(parseKnownVariable)
    val parsedRels = context.relations.map(parseKnownRelation)
    val parsedMarkers = context.parameterMarkers.map(parseKnownParameterMarker)
    val env = AnalyzeEnv(session, parsedMarkers, parsedVars, parsedRels)
    val needsSessionOverride =
      context.catalog.nonEmpty || context.schema.nonEmpty || context.path.nonEmpty
    if (!needsSessionOverride) {
      f(env)
    } else {
      val mgr = session.sessionState.catalogManager
      val conf = session.sessionState.conf
      val prevCatalog = mgr.currentCatalog.name()
      val prevNamespace = mgr.currentNamespace.clone()
      val prevPath = mgr.storedSessionPathEntries
      val prevPathEnabled = conf.getConf(SQLConf.PATH_ENABLED)
      try {
        context.catalog.foreach { catalog =>
          if (catalog.trim.isEmpty) {
            throw QueryExecutionErrors.invalidParseSqlContext("current_catalog", catalog)
          }
          mgr.setCurrentCatalog(catalog)
        }
        context.schema.foreach { schema =>
          val parts = try {
            parser.get().parseMultipartIdentifier(schema)
          } catch {
            case _: ParseException =>
              throw QueryExecutionErrors.invalidParseSqlContext("current_schema", schema)
          }
          if (parts.isEmpty) {
            throw QueryExecutionErrors.invalidParseSqlContext("current_schema", schema)
          }
          mgr.setCurrentNamespace(parts.toArray)
        }
        context.path.foreach { path =>
          if (!prevPathEnabled) {
            conf.setConf(SQLConf.PATH_ENABLED, true)
          }
          applyPathOverride(mgr, conf, path)
        }
        f(env)
      } finally {
        mgr.setCurrentCatalog(prevCatalog)
        mgr.setCurrentNamespace(prevNamespace)
        prevPath match {
          case Some(entries) => mgr.setSessionPath(entries)
          case None => mgr.clearSessionPath()
        }
        if (conf.getConf(SQLConf.PATH_ENABLED) != prevPathEnabled) {
          conf.setConf(SQLConf.PATH_ENABLED, prevPathEnabled)
        }
      }
    }
  }

  private def applyPathOverride(
      mgr: CatalogManager,
      conf: SQLConf,
      path: String): Unit = {
    if (path.trim.isEmpty) {
      mgr.clearSessionPath()
    } else {
      val elements = try {
        parser.get().parsePathElements(path)
      } catch {
        case _: ParseException =>
          throw QueryExecutionErrors.invalidParseSqlContext("current_path", path)
      }
      val expanded = PathElement.validateNoStaticDuplicates(
        PathElement.expand(elements, conf, mgr),
        conf.caseSensitiveAnalysis)
      if (expanded.isEmpty) {
        mgr.clearSessionPath()
      } else {
        mgr.setSessionPath(expanded)
      }
    }
  }

  private def parseKnownVariable(
      spec: (String, String)): (Seq[String], DataType) = {
    val (name, typeDdl) = spec
    val parts = parseKnownName(name, "variables")
    val dataType = try {
      DataType.fromDDL(typeDdl)
    } catch {
      case _: ParseException | _: AnalysisException =>
        throw QueryExecutionErrors.invalidParseSqlContext("variables", typeDdl)
    }
    (parts, dataType)
  }

  private def parseKnownRelation(
      spec: (String, String)): (Seq[String], StructType) = {
    val (name, schemaDdl) = spec
    val parts = parseKnownName(name, "relations")
    val dataType = try {
      DataType.fromDDL(schemaDdl)
    } catch {
      case _: ParseException | _: AnalysisException =>
        throw QueryExecutionErrors.invalidParseSqlContext("relations", schemaDdl)
    }
    dataType match {
      case schema: StructType if schema.nonEmpty => (parts, schema)
      case _ =>
        throw QueryExecutionErrors.invalidParseSqlContext("relations", schemaDdl)
    }
  }

  private def parseKnownName(name: String, parameter: String): Seq[String] = {
    val parts = try {
      parser.get().parseMultipartIdentifier(name)
    } catch {
      case _: ParseException =>
        throw QueryExecutionErrors.invalidParseSqlContext(parameter, name)
    }
    if (parts.isEmpty) {
      throw QueryExecutionErrors.invalidParseSqlContext(parameter, name)
    }
    parts
  }

  private def parseKnownParameterMarker(
      spec: (String, String)): (String, DataType) = {
    val (name, typeDdl) = spec
    val dataType = try {
      DataType.fromDDL(typeDdl)
    } catch {
      case _: ParseException | _: AnalysisException =>
        throw QueryExecutionErrors.invalidParseSqlContext("parameter_markers", typeDdl)
    }
    val trimmed = name.trim
    if (trimmed == "?") {
      ("?", dataType)
    } else {
      val parts = parseKnownName(trimmed, "parameter_markers")
      if (parts.length != 1) {
        throw QueryExecutionErrors.invalidParseSqlContext("parameter_markers", name)
      }
      (parts.head, dataType)
    }
  }

  /**
   * Bind caller-supplied marker types using the same [[BindParameters]] rule as
   * `sql(..., args)`, then analyze the bound plan.
   */
  private def bindSuppliedParameterMarkers(
      plan: LogicalPlan,
      markers: Seq[(String, DataType)]): LogicalPlan = {
    if (markers.isEmpty || !plan.containsPattern(PARAMETER)) {
      plan
    } else {
      val namedArgs = markers.collect {
        case (name, dataType) if name != "?" =>
          name -> Literal.create(null, dataType)
      }
      val posArgs = markers.collect {
        case ("?", dataType) => Literal.create(null, dataType)
      }
      var usesNamed = false
      var usesPos = false
      plan.foreach { p =>
        p.expressions.foreach { e =>
          e.foreach {
            case _: NamedParameter => usesNamed = true
            case _: PosParameter => usesPos = true
            case _ =>
          }
        }
      }
      val wrapped = if (usesNamed && namedArgs.nonEmpty) {
        NameParameterizedQuery(plan, namedArgs.toMap)
      } else if (usesPos && posArgs.nonEmpty) {
        PosParameterizedQuery(plan, posArgs)
      } else {
        plan
      }
      BindParameters(wrapped)
    }
  }

  /** Build success JSON from an already-parsed unresolved plan. */
  private def successJson(
      plan: LogicalPlan,
      analyzed: Option[LogicalPlan],
      analyzeError: Option[SparkThrowable with Throwable],
      session: Option[ClassicSparkSession],
      span: StatementSpan,
      analyzedQuery: Option[LogicalPlan] = None): JObject = {
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
    val refs = collectPlanReferences(plan)
    // Qualify from the ANALYZE session even when analysis failed: names and
    // builtins are known from parse plus current catalog / schema / path.
    val targetTables = session.map(s => refs.targetTables.map(qualifyRelation(_, s)))
      .getOrElse(refs.targetTables)
    val sourceTables = session.map(s => refs.sourceTables.map(qualifyRelation(_, s)))
      .getOrElse(refs.sourceTables)
    val functions = session.map(s => refs.functions.map(qualifyRoutine(_, s)))
      .getOrElse(refs.functions)
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
    val selectList = analyzedQuery.orElse(analyzed).map(collectTypedSelectList)
      .getOrElse(collectSelectList(plan))
    if (selectList.nonEmpty) {
      fields += "select_list" -> JArray(selectList.toList)
    }
    refs.parameterMarkers.foreach(markers => fields += "parameter_markers" -> markers)
    analyzeError.foreach { e =>
      val errorObj = parseJson(
        SparkThrowableHelper.getMessage(e, ErrorMessageFormat.STANDARD)).asInstanceOf[JObject]
      fields += "error" -> rewriteErrorCharIndexes(errorObj, span.contextText)
    }
    JObject(fields.toList)
  }

  /** 1-based Unicode code-point offset of `segment` in `batch`. */
  private def charStart(batch: String, segment: PositionedSqlStatement): Int = {
    val utf16 = math.max(0, math.min(segment.start, batch.length))
    batch.codePointCount(0, utf16) + 1
  }

  /** Unicode code-point length of the trimmed statement text. */
  private def charLength(segment: PositionedSqlStatement): Int = {
    val text = segment.statement
    if (text.isEmpty) 0 else text.codePointCount(0, text.length)
  }

  /**
   * `queryContext` indexes in STANDARD errors are not one unit: some Origins
   * store Java `String` (UTF-16) offsets, some store ANTLR code-point offsets.
   * Emit Unicode code points so a supplementary character is length 1, matching
   * statement `start`/`length`. Leave values that already slice the fragment
   * as code points.
   */
  private def rewriteErrorCharIndexes(errorObj: JObject, statement: String): JObject = {
    JObject(errorObj.obj.map {
      case ("queryContext", JArray(contexts)) =>
        "queryContext" -> JArray(contexts.map {
          case ctx: JObject => rewriteQueryContextIndexes(ctx, statement)
          case other => other
        }.toList)
      case other => other
    })
  }

  private def rewriteQueryContextIndexes(ctx: JObject, statement: String): JObject = {
    val start = ctx.obj.collectFirst { case ("startIndex", JInt(n)) => n.toInt }
    val stop = ctx.obj.collectFirst { case ("stopIndex", JInt(n)) => n.toInt }
    val fragment = ctx.obj.collectFirst { case ("fragment", JString(s)) => s }
    (start, stop, fragment) match {
      case (Some(from), Some(to), Some(frag))
          if shouldConvertUtf16Indexes(statement, from, to, frag) =>
        JObject(ctx.obj.map {
          case ("startIndex", _) =>
            "startIndex" -> JInt(utf16OneBasedStartToChar(statement, from))
          case ("stopIndex", _) =>
            "stopIndex" -> JInt(utf16OneBasedStopToChar(statement, to))
          case other => other
        })
      case _ => ctx
    }
  }

  private def shouldConvertUtf16Indexes(
      text: String,
      start: Int,
      stop: Int,
      fragment: String): Boolean = {
    utf16Slice(text, start, stop).contains(fragment) &&
      !charSlice(text, start, stop).contains(fragment)
  }

  private def utf16Slice(text: String, start: Int, stop: Int): Option[String] = {
    val begin = math.max(0, start - 1)
    val end = math.min(text.length, math.max(begin, stop))
    if (begin <= end && end <= text.length) Some(text.substring(begin, end)) else None
  }

  private def charSlice(text: String, start: Int, stop: Int): Option[String] = {
    try {
      val len = stop - start + 1
      if (start < 1 || len < 0) {
        None
      } else {
        val begin = text.offsetByCodePoints(0, start - 1)
        val end = text.offsetByCodePoints(begin, len)
        Some(text.substring(begin, end))
      }
    } catch {
      case _: IndexOutOfBoundsException => None
    }
  }

  private def utf16OneBasedStartToChar(text: String, start: Int): Int = {
    var utf16 = math.max(0, math.min(start - 1, text.length))
    if (utf16 > 0 && utf16 < text.length &&
        Character.isLowSurrogate(text.charAt(utf16)) &&
        Character.isHighSurrogate(text.charAt(utf16 - 1))) {
      utf16 -= 1
    }
    text.codePointCount(0, utf16) + 1
  }

  private def utf16OneBasedStopToChar(text: String, stop: Int): Int = {
    var end = math.max(0, math.min(stop, text.length))
    if (end > 0 && end < text.length &&
        Character.isHighSurrogate(text.charAt(end - 1)) &&
        Character.isLowSurrogate(text.charAt(end))) {
      end += 1
    }
    if (end == 0) 1 else text.codePointCount(0, end)
  }

  private def errorJson(e: SparkThrowable with Throwable, span: StatementSpan): JObject = {
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
    JObject(
      "start" -> JInt(span.start),
      "length" -> JInt(span.length),
      "parse_success" -> JBool(false),
      "error" -> rewriteErrorCharIndexes(
        JObject(errorObj.obj ++ contextFields ++ locationFields),
        span.contextText)
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
      case i: InsertIntoSuppliedRelation =>
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
      case d: DeclareCursor =>
        parseQueryPlan(d.queryText).foreach(
          q => visitPlan(q, scope, TableRefRole.Source)(f))
      case _ =>
    }
  }

  private def tableIdentifierParts(id: TableIdentifier): Seq[String] =
    id.catalog.toSeq ++ id.database.toSeq :+ id.table

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
        case c: CreateViewCommand => addTarget(tableIdentifierParts(c.name))
        case c: CreateTempViewUsing => addTarget(tableIdentifierParts(c.tableIdent))
        case c: CacheTable if c.multipartIdentifier.nonEmpty =>
          addTarget(c.multipartIdentifier)
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
  private def collectTypedSelectList(plan: LogicalPlan): Seq[JObject] = {
    val query = primaryQueryPlan(plan)
    if (query.resolved && query.output.nonEmpty) {
      query.output.map { attr =>
        JObject(
          "name" -> partsToJArray(Seq(attr.name)),
          "type" -> DescribeRelationJsonCommand.jsonType(attr.dataType))
      }
    } else {
      collectSelectList(plan)
    }
  }

  private def qualifyRelation(
      parts: Seq[String],
      session: ClassicSparkSession): Seq[String] = {
    val catalog = session.sessionState.catalog
    val resolver = session.sessionState.conf.resolver
    val mgr = session.sessionState.catalogManager
    val sessionIdent = SuppliedRelationRegistry.sessionIdentity(parts.last)
    val overlaySession =
      SuppliedRelationRegistry.lookup(sessionIdent, resolver).isDefined
    parts match {
      case Seq(name) if catalog.getRawTempView(name).isDefined =>
        sessionIdent
      case Seq(name) if overlaySession && mgr.isSystemSessionOnPath =>
        sessionIdent
      case _ if CatalogManager.isSessionQualifiedViewName(parts) &&
          (catalog.isTempView(parts) || overlaySession) =>
        sessionIdent
      case _ if catalog.isTempView(parts) =>
        parts match {
          case Seq(db, name) => Seq(db, name)
          case other => other
        }
      case _ =>
        qualifyWithCurrentNamespace(parts, session)
    }
  }

  private def qualifyRoutine(
      parts: Seq[String],
      session: ClassicSparkSession): Seq[String] = {
    val catalog = session.sessionState.catalog
    parts match {
      case Seq(name) if catalog.isTemporaryFunction(FunctionIdentifier(name)) =>
        Seq(
          CatalogManager.SYSTEM_CATALOG_NAME,
          CatalogManager.SESSION_NAMESPACE,
          name)
      case Seq(name) if FunctionRegistry.builtin.functionExists(FunctionIdentifier(name)) =>
        FunctionRegistry.builtinFunctionIdentifier(name).nameParts
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
      case Seq(ns, name) => Seq(catalogName, ns, name)
      case other => other
    }
  }

  /** Parse a cursor query for lineage; failures leave the DECLARE without refs. */
  private def parseQueryPlan(sqlText: String): Option[LogicalPlan] = {
    try {
      Some(parser.get().parsePlan(sqlText))
    } catch {
      case _: ParseException | _: SqlScriptingException => None
    }
  }

  private def primaryQueryPlan(plan: LogicalPlan): LogicalPlan = plan match {
    case UnresolvedWith(child, _, _) => primaryQueryPlan(child)
    case WithCTE(child, _) => primaryQueryPlan(child)
    case UnresolvedInsert(_, _, _, query, _, _, _, _, _) =>
      primaryQueryPlan(query)
    case i: InsertIntoSuppliedRelation => primaryQueryPlan(i.query)
    case i: InsertIntoStatement => primaryQueryPlan(i.query)
    case c: CreateTableAsSelect => primaryQueryPlan(c.query)
    case r: ReplaceTableAsSelect => primaryQueryPlan(r.query)
    case c: CreateView => primaryQueryPlan(c.query)
    case c: CreateViewCommand => primaryQueryPlan(c.plan)
    case c: CacheTableAsSelect => primaryQueryPlan(c.plan)
    case ExplainCommand(logicalPlan, _) => primaryQueryPlan(logicalPlan)
    case DescribeQueryCommand(_, queryPlan) => primaryQueryPlan(queryPlan)
    case d: DeclareCursor =>
      parseQueryPlan(d.queryText).map(primaryQueryPlan).getOrElse(d)
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
