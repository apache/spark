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
import scala.util.Try

import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, parse => parseJson, render}

import org.apache.spark.{
  ErrorMessageFormat, SparkRuntimeException, SparkThrowable, SparkThrowableHelper
}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, SqlScriptingContextManager, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.{HiveTableRelation, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin, SQLQueryContext}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.classic.{SparkSession => ClassicSparkSession}
import org.apache.spark.sql.connector.catalog.{
  AnalysisCatalogManager, CatalogManager, CatalogV2Util, FunctionCatalog, Identifier,
  PathElement, SupportsNamespaces
}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.exceptions.SqlScriptingException
import org.apache.spark.sql.execution.{CommandExecutionMode, QueryExecution, SparkSqlParser}
import org.apache.spark.sql.execution.command.{
  AlterViewAsCommand,
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
  CreateTempViewUsing, DataSource, InsertIntoDataSourceCommand,
  InsertIntoHadoopFsRelationCommand, LogicalRelation
}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.metricview.logical.ResolvedMetricView
import org.apache.spark.sql.types.{DataType, StructType}

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
 * Optional ANALYZE-only context for [[ParseSqlResult.fromSql]]. `None` inherits the
 * invoker session. A value applies to one call and never changes the session.
 */
case class ParseSqlAnalyzeContext(
    catalog: Option[String] = None,
    schema: Option[String] = None,
    path: Option[String] = None,
    variables: Seq[(String, String)] = Nil,
    relations: Seq[(String, String)] = Nil,
    parameterMarkers: Seq[(String, String)] = Nil)

object ParseSqlAnalyzeContext {
  val empty: ParseSqlAnalyzeContext = ParseSqlAnalyzeContext()
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
 * Nested error locations are statement-relative Unicode code points.
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
    val spans = statementSpans(sql, statements)
    val json = if (mode == ParseSqlMode.Analyze) {
      analyzeBatch(statements, spans, context)
    } else {
      JArray(statements.zip(spans).map { case (segment, span) =>
        parsePlanOrError(segment, span) match {
          case ParseFailed(errorObj) => errorObj
          case Parsed(plan) =>
            successJson(plan, analyzed = None, analyzeError = None, None, span)
        }
      }.toList)
    }
    compact(render(json))
  }

  private sealed trait ParsedStatement
  private case class Parsed(plan: LogicalPlan) extends ParsedStatement
  private case class ParseFailed(errorObj: JObject) extends ParsedStatement

  /**
   * Parse every statement, then analyze each against the invoker session.
   * Statements are not executed, so one does not see the effect of an earlier
   * one. Analysis failures stay JSON (`analyze_success` false) per statement.
   */
  private def analyzeBatch(
      statements: Seq[PositionedSqlStatement],
      spans: Seq[StatementSpan],
      context: ParseSqlAnalyzeContext): JArray = {
    val parsed = statements.zip(spans).map { case (segment, span) =>
      parsePlanOrError(segment, span)
    }
    val session = ClassicSparkSession.getActiveSession
      .getOrElse(throw QueryExecutionErrors.parseSqlAnalyzeRequiresSession())
    val (analyzerOpt, callConf) =
      if (context.catalog.isEmpty && context.schema.isEmpty && context.path.isEmpty) {
        (None, None)
      } else {
        val (analyzer, conf) = contextAnalyzer(session, context)
        (Some(analyzer), conf)
      }
    // Overlay names are canonicalized, and reported names qualified, by the catalog manager
    // the batch analyzes with.
    val analysisMgr = analyzerOpt.map(_.catalogManager)
      .getOrElse(session.sessionState.catalogManager)
    val scope = AnalysisScope(session, analysisMgr)
    // A bad overlay spec is reported on every statement, like an analysis error.
    val overlays = try {
      val variables = context.variables.map(parseKnownVariable)
      val relations = context.relations.map { spec =>
        val (name, schema) = parseKnownRelation(spec)
        SuppliedRelationRegistry.canonicalize(analysisMgr, name) -> schema
      }
      val markers = context.parameterMarkers.map(parseKnownParameterMarker)
      scala.Right((variables, relations, markers))
    } catch {
      case e: SparkRuntimeException => scala.Left(e)
    }
    // The batch is analyzed on its own: a SQL script that calls parse_sql does
    // not lend it its locals, and CALL is not run.
    SqlScriptingContextManager.create(null).runWith {
      InvokeProcedures.withoutInvocation {
        JArray(parsed.zip(spans).map {
          case (ParseFailed(errorObj), _) => errorObj
          case (Parsed(plan), span) =>
            overlays match {
              case scala.Left(e) =>
                inCallConf(callConf) { successJson(plan, None, Some(e), Some(scope), span) }
              case scala.Right((variables, relations, markers)) =>
                // Caller-supplied overlays eclipse the real objects for this statement only.
                val (analyzed, error) = SuppliedVariableRegistry.create(variables).runWith {
                  SuppliedRelationRegistry.create(relations).runWith {
                    inCallConf(callConf) {
                      analyze(session, analyzerOpt, plan, markers)
                    }
                  }
                }
                // Names are qualified under the conf the statement was analyzed with.
                inCallConf(callConf) { successJson(plan, analyzed, error, Some(scope), span) }
            }
        }.toList)
      }
    }
  }

  /** Analyze `plan`, including nested EXPLAIN / DESCRIBE QUERY payloads. */
  private def analyzePlan(
      session: ClassicSparkSession,
      analyzerOpt: Option[Analyzer],
      plan: LogicalPlan): LogicalPlan = {
    // Checked here, before QueryExecution, because QueryExecution executes a script (also
    // when named parameter markers wrap it) as soon as analysis starts. A script under a
    // supervising command is refused by the recursive call below.
    if (QueryExecution.isUnresolvedPlanSqlScript(plan)) {
      throw QueryExecutionErrors.parseSqlAnalyzeUnsupportedScript()
    }
    val toAnalyze = plan match {
      case s: SupervisingCommand =>
        s.withTransformedSupervisedPlan(inner => analyzePlan(session, analyzerOpt, inner))
      case other => other
    }
    new QueryExecution(
      session,
      toAnalyze,
      mode = CommandExecutionMode.SKIP,
      analyzerOpt = analyzerOpt).analyzed
  }

  /**
   * Run `f` under the call's conf when the call gives its own SQL path. The copy enables the
   * path feature for this call only; the invoker's conf is not changed.
   */
  private def inCallConf[T](conf: Option[SQLConf])(f: => T): T = conf match {
    case Some(c) => SQLConf.withExistingConf(c)(f)
    case None => f
  }

  /**
   * The analyzer for one ANALYZE call with `current_catalog` / `current_schema` /
   * `current_path` overrides, and the conf to run it under when the call gives a path.
   * The session is not changed: the call gets its own catalog manager, v1 catalog and conf.
   */
  private def contextAnalyzer(
      session: ClassicSparkSession,
      context: ParseSqlAnalyzeContext): (Analyzer, Option[SQLConf]) = {
    val state = session.sessionState
    val mgr = state.catalogManager
    val invokerCatalog = mgr.currentCatalog.name()
    val target = context.catalog.map(checkedContextValue("current_catalog", _))
      .map(mgr.catalog)
      .getOrElse(mgr.currentCatalog)
    val catalogName = target.name()
    val isSession = CatalogV2Util.isSessionCatalog(target)
    val switched = catalogName != invokerCatalog
    val namespace: Array[String] = context.schema match {
      case Some(schema) => checkedNamespace(schema, isSession)
      case None if switched =>
        if (isSession) Array(state.conf.defaultDatabase) else target.defaultNamespace()
      case None => mgr.currentNamespace
    }
    target match {
      case ns: SupportsNamespaces if !isSession && !ns.namespaceExists(namespace) =>
        throw QueryCompilationErrors.noSuchNamespaceError(catalogName +: namespace)
      case _ =>
    }
    val v1 = perCallSessionCatalog(session)
    if (isSession) v1.setCurrentDatabase(namespace.head)
    val callConf = context.path.map { _ =>
      val copy = state.conf.clone()
      copy.setConf(SQLConf.PATH_ENABLED, true)
      copy
    }
    // PATH in the call's path means the invoker's path, as in SET PATH. CURRENT_SCHEMA
    // stays a marker that resolves against the call's schema.
    val pathOverride = (context.path, callConf) match {
      case (Some(path), Some(conf)) => callPathOverride(path, conf, mgr)
      case _ => None
    }
    val analyzer = state.analyzer.withCatalogManager(
      new AnalysisCatalogManager(mgr, catalogName, namespace, v1, pathOverride))
    (analyzer, callConf)
  }

  /**
   * The SQL path of one call. A blank path clears it to the conf default, as `SET PATH`
   * does. A path that expands to nothing does the same.
   */
  private def callPathOverride(
      path: String,
      conf: SQLConf,
      mgr: CatalogManager): Option[Option[Seq[CatalogManager.SessionPathEntry]]] = {
    if (path.trim.isEmpty) {
      Some(None)
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
      if (expanded.isEmpty) Some(None) else Some(Some(expanded))
    }
  }

  private def checkedContextValue(parameter: String, value: String): String = {
    if (value.trim.isEmpty) {
      throw QueryExecutionErrors.invalidParseSqlContext(parameter, value)
    }
    value
  }

  /** A `current_schema` value as a namespace. The session catalog takes one part. */
  private def checkedNamespace(schema: String, isSession: Boolean): Array[String] = {
    val parts = try {
      parser.get().parseMultipartIdentifier(checkedContextValue("current_schema", schema))
    } catch {
      case _: ParseException =>
        throw QueryExecutionErrors.invalidParseSqlContext("current_schema", schema)
    }
    if (parts.isEmpty || (isSession && parts.length != 1)) {
      throw QueryExecutionErrors.invalidParseSqlContext("current_schema", schema)
    }
    parts.toArray
  }

  /** A v1 catalog copy with the invoker's temp views and current database. */
  private def perCallSessionCatalog(session: ClassicSparkSession): SessionCatalog = {
    val state = session.sessionState
    val catalog = new SessionCatalog(
      () => session.sharedState.externalCatalog,
      () => session.sharedState.globalTempViewManager,
      state.functionRegistry,
      state.tableFunctionRegistry,
      state.newHadoopConf(),
      state.sqlParser,
      state.resourceLoader,
      state.catalog.functionExpressionBuilder)
    state.catalog.copyStateTo(catalog)
    catalog
  }

  /** INTERNAL_ERROR / SQLSTATE XX are Spark bugs, not analysis results. */
  private def isInternalError(e: SparkThrowable): Boolean =
    e.isInternalError || Option(e.getSqlState).exists(_.startsWith("XX"))

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
          stopIndex = Some(span.length - 1),
          sqlText = Some(sql))
      } else {
        Origin(sqlText = Some(sql))
      }
      CurrentOrigin.withOrigin(origin) {
        Parsed(parser.get().parsePlan(sql))
      }
    } catch {
      case e: ParseException =>
        ParseFailed(errorJson(e, span))
      case e: SqlScriptingException =>
        ParseFailed(errorJson(e, span))
    }
  }

  /**
   * Analyze `plan` without executing it. Scripts never reach QueryExecution, which runs
   * a script when analysis starts; [[analyzePlan]] refuses them before that.
   */
  private def analyze(
      session: ClassicSparkSession,
      analyzerOpt: Option[Analyzer],
      plan: LogicalPlan,
      markers: Seq[(String, DataType)])
      : (Option[LogicalPlan], Option[SparkThrowable with Throwable]) = {
    try {
      // Refuse a script before any binding wraps it, as [[analyzePlan]] does after.
      if (QueryExecution.isUnresolvedPlanSqlScript(plan)) {
        throw QueryExecutionErrors.parseSqlAnalyzeUnsupportedScript()
      }
      val toAnalyze = bindSuppliedParameterMarkers(plan, markers)
      (Some(analyzePlan(session, analyzerOpt, toAnalyze)), None)
    } catch {
      case e: SparkThrowable with Throwable if !isInternalError(e) => (None, Some(e))
    }
  }

  private def parseKnownVariable(spec: (String, String)): (Seq[String], DataType) = {
    val (name, typeDdl) = spec
    val parts = parseKnownName(name, "variables")
    // A variable name has at most three parts, as in system.session.v.
    if (parts.length > 3) {
      throw QueryExecutionErrors.invalidParseSqlContext("variables", name)
    }
    val dataType = try {
      DataType.fromDDL(typeDdl)
    } catch {
      case _: ParseException | _: AnalysisException =>
        throw QueryExecutionErrors.invalidParseSqlContext("variables", typeDdl)
    }
    (parts, dataType)
  }

  private def parseKnownRelation(spec: (String, String)): (Seq[String], StructType) = {
    val (name, schemaDdl) = spec
    val parts = parseKnownName(name, "relations")
    val dataType = try {
      DataType.fromDDL(schemaDdl)
    } catch {
      case _: ParseException | _: AnalysisException =>
        throw QueryExecutionErrors.invalidParseSqlContext("relations", schemaDdl)
    }
    dataType match {
      case schema: StructType if schema.nonEmpty =>
        (parts, CharVarcharUtils.replaceCharVarcharWithStringInSchema(schema))
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

  private def parseKnownParameterMarker(spec: (String, String)): (String, DataType) = {
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
   * Bind caller-supplied marker types with the [[BindParameters]] rule that
   * `sql(..., args)` uses, so the plan is typed the way a real query with those
   * arguments would be. Markers the plan does not use leave it unchanged.
   */
  private def bindSuppliedParameterMarkers(
      plan: LogicalPlan,
      markers: Seq[(String, DataType)]): LogicalPlan = {
    val used = if (markers.isEmpty) None else collectPlanReferences(plan).parameterMarkers
    if (used.isEmpty) {
      plan
    } else {
      // The bound values are built during constant folding of parse_sql, so they would
      // otherwise inherit the outer call's origin. Build them without one, so an error on a
      // marker does not report the outer query text as its context.
      def nullValue(dataType: DataType): Literal =
        CurrentOrigin.withOrigin(Origin()) { Literal.create(null, dataType) }
      val namedArgs = markers.collect {
        case (name, dataType) if name != "?" => name -> nullValue(dataType)
      }
      val posArgs = markers.collect {
        case ("?", dataType) => nullValue(dataType)
      }
      val usesNamed = used.exists(m => (m \ "named") != JNothing)
      val usesPos = used.exists(m => (m \ "unnamed_count") != JNothing)
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

  /** The session, and the catalog manager one ANALYZE call resolves names with. */
  private case class AnalysisScope(session: ClassicSparkSession, catalogManager: CatalogManager) {
    def catalog: SessionCatalog = catalogManager.v1SessionCatalog
  }

  /** Build success JSON from an already-parsed unresolved plan. */
  private def successJson(
      plan: LogicalPlan,
      analyzed: Option[LogicalPlan],
      analyzeError: Option[SparkThrowable with Throwable],
      scope: Option[AnalysisScope],
      span: StatementSpan): JObject = {
    val classification = SqlStatementCodes.classify(plan)
    val fields = mutable.ListBuffer.empty[JField]
    fields += "start" -> JInt(span.start)
    fields += "length" -> JInt(span.length)
    fields += "parse_success" -> JBool(true)
    fields += "statement_identifier" -> JString(classification.statementIdentifier)
    fields += "statement_code" -> JInt(classification.statementCode)
    if (scope.nonEmpty) {
      fields += "analyze_success" -> JBool(analyzed.isDefined)
    }
    // Omit unused collections / markers so consumers can treat absence as empty.
    // Parse names decide which references exist and in which role. ANALYZE
    // qualifies those names from the analyzed plan when it can match them.
    val parseRefs = collectPlanReferences(plan)
    val analyzedRefs = analyzed.map(collectPlanReferences)
    val (targetTables, sourceTables, functions) = try {
      scope.map { s =>
        val analyzedTargets = analyzedRefs.map(_.targetTables).getOrElse(Nil)
        val analyzedSources = analyzedRefs.map(_.sourceTables).getOrElse(Nil)
        val analysisSucceeded = analyzed.isDefined
        (
          uniqueIdentities(
            qualifyParseIdentities(
              parseRefs.targetTables, analyzedTargets, analysisSucceeded, s,
              qualifyTarget(_, plan, s)),
            s),
          uniqueIdentities(
            qualifyParseIdentities(
              parseRefs.sourceTables, analyzedSources, analysisSucceeded, s,
              qualifySource(_, s)),
            s),
          uniqueIdentities(parseRefs.functions.map(qualifyRoutine(_, s)), s)
        )
      }.getOrElse(
        (parseRefs.targetTables, parseRefs.sourceTables, parseRefs.functions))
    } catch {
      case e: SparkThrowable with Throwable if !isInternalError(e) =>
        (parseRefs.targetTables, parseRefs.sourceTables, parseRefs.functions)
    }
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
    // The query of a row-level rewrite or insert-only MERGE. It re-reads the write target, so
    // the role marks the re-read for the walk. Subqueries in the query are Source again.
    case object RewriteRead extends TableRefRole
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
      case _: View | _: ResolvedMetricView =>
        // The named object is the reference; do not collect tables from its body.
      case w: V2WriteCommand =>
        visitPlan(w.table, scope, TableRefRole.Target)(f)
        val queryRole = w match {
          case _: ReplaceData | _: WriteDelta | _: InsertOnlyMerge => TableRefRole.RewriteRead
          case _ => TableRefRole.Source
        }
        visitPlan(w.query, nextScope, queryRole)(f)
      case a: AlterViewAs =>
        visitPlan(a.child, scope, TableRefRole.Target)(f)
        visitPlan(a.query, nextScope, TableRefRole.Source)(f)
      case w: WithCTE =>
        w.cteDefs.foreach(d => visitPlan(d, nextScope, TableRefRole.Source)(f))
        visitPlan(w.plan, nextScope, role)(f)
      case SubqueryAlias(_, child) =>
        visitPlan(child, nextScope, role)(f)
      case _: org.apache.spark.sql.catalyst.analysis.SQLFunctionNode =>
        // The body of a SQL table function is the routine's own text, not this statement's.
      case _ =>
        statementSubqueries(parent).foreach(sq => visitPlan(sq, nextScope, TableRefRole.Source)(f))
        parent.children.foreach { child =>
          val childRole =
            if (isTargetTableChild(parent)) TableRefRole.Target else role
          visitPlan(child, nextScope, childRole)(f)
        }
    }
  }

  /**
   * Subquery plans of `plan` that belong to the statement. Subqueries inside the body of a SQL
   * scalar function belong to the routine, so only that function's arguments are kept.
   */
  private def statementSubqueries(plan: LogicalPlan): Seq[LogicalPlan] = {
    def visit(e: Expression): Seq[LogicalPlan] = e match {
      case p: PlanExpression[_] => p.plan match {
        case sub: LogicalPlan => Seq(sub)
        case _ => Nil
      }
      case s: SQLScalarFunction => s.inputs.flatMap(visit)
      case other => other.children.flatMap(visit)
    }
    plan.expressions.flatMap(visit)
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
      case c: CacheTable if c.multipartIdentifier.isEmpty || c.isAnalyzed =>
        visitPlan(c.table, scope, TableRefRole.Target)(f)
      // The query of a v1 CTAS, CREATE VIEW, ALTER VIEW or INSERT is not a child after analysis.
      case c: CreateDataSourceTableAsSelectCommand =>
        visitPlan(c.query, scope, TableRefRole.Source)(f)
      case c: CreateViewCommand =>
        visitPlan(c.plan, scope, TableRefRole.Source)(f)
      case c: AlterViewAsCommand =>
        visitPlan(c.query, scope, TableRefRole.Source)(f)
      case i: InsertIntoHadoopFsRelationCommand =>
        visitPlan(i.query, scope, TableRefRole.Source)(f)
      case i: InsertIntoDataSourceCommand =>
        visitPlan(i.query, scope, TableRefRole.Source)(f)
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
    // Targets of row-level rewrites. The rewritten query reads the target again in a
    // RewriteRead position, so that read is not a table the statement names. A subquery in the
    // same statement that reads the target is still a source.
    val rewriteTargets = mutable.HashSet.empty[Seq[String]]
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
          case TableRefRole.Source | TableRefRole.RewriteRead => sourceTables += parts
        }
      }
    }

    def addFunction(parts: Seq[String]): Unit = {
      if (parts.nonEmpty) functions += parts
    }

    def visitExpr(e: Expression): Unit = e.foreach {
      case f: UnresolvedFunction => addFunction(f.nameParts)
      // A routed JSON constructor is a function call too (a routine may own it).
      case j: UnresolvedJsonConstructor if j.routeThroughResolution =>
        addFunction(Seq(j.prettyName))
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
        case c: CreateViewCommand => addTarget(Seq(c.name.table))
        case c: CreateTempViewUsing => addTarget(Seq(c.tableIdent.table))
        case c: CreateDataSourceTableCommand =>
          addTarget(persistedTableParts(c.table.identifier))
        case c: CreateDataSourceTableAsSelectCommand =>
          addTarget(persistedTableParts(c.table.identifier))
        case c: CreateTableCommand =>
          addTarget(persistedTableParts(c.table.identifier))
        case c: DropTableCommand =>
          addTarget(tableIdentifierParts(c.tableName))
        case c: DropTempViewCommand =>
          addTarget(tempViewParts(c.ident))
        case c: CacheTable if c.multipartIdentifier.nonEmpty && !c.isAnalyzed =>
          addTarget(c.multipartIdentifier)
        case i: InsertIntoHadoopFsRelationCommand =>
          i.catalogTable.foreach(t => addTarget(persistedTableParts(t.identifier)))
        case i: InsertIntoDataSourceCommand =>
          i.logicalRelation.catalogTable.foreach(t => addTarget(persistedTableParts(t.identifier)))
        case u: UnresolvedTableValuedFunction => addFunction(u.name)
        case w: ReplaceData => rewriteTargets ++= rewriteTargetParts(w.table)
        case w: WriteDelta => rewriteTargets ++= rewriteTargetParts(w.table)
        case w: InsertOnlyMerge => rewriteTargets ++= rewriteTargetParts(w.table)
        case r: DataSourceV2Relation =>
          v2RelationParts(r)
            .filterNot(parts => role == TableRefRole.RewriteRead && rewriteTargets.contains(parts))
            .foreach(add)
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
        case l: LocalRelation =>
          // A caller-supplied relation (relations overlay) is named by its canonical identity.
          l.getTagValue(SuppliedRelationRegistry.IDENTITY_TAG).foreach(add)
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

  /**
   * Select-list items with SQL types from the analyzed query output, so `*`
   * is expanded and names match analysis. INSERT reports the write projection
   * (target columns), not the user's SELECT aliases.
   */
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
      scope: AnalysisScope): Seq[Seq[String]] = {
    val sensitive = scope.session.sessionState.conf.caseSensitiveAnalysis
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

  private def rewriteTargetParts(table: NamedRelation): Option[Seq[String]] = table match {
    case d: DataSourceV2Relation => v2RelationParts(d)
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
    val parts = ident.namespace().toSeq :+ ident.name()
    val globalTemp = SQLConf.get.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE)
    val ns = ident.namespace()
    if (ns.length == 1 && ns.head.equalsIgnoreCase(globalTemp)) {
      Seq(CatalogManager.SESSION_CATALOG_NAME, ns.head, ident.name())
    } else if (ns.isEmpty || CatalogManager.isSessionQualifiedViewName(parts)) {
      sessionIdent(ident.name())
    } else {
      catalogIdentParts(CatalogManager.SESSION_CATALOG_NAME, ident)
    }
  }

  private def globalTempIdent(name: String): Seq[String] =
    Seq(
      CatalogManager.SESSION_CATALOG_NAME,
      SQLConf.get.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE),
      name)

  /**
   * Match each parse name to an analyzed identity at most once. Longer parse
   * names claim first so `spark_catalog.db.t` is not stolen by a later bare
   * `t`. Equal-length matches count: a 3-part parse name is already complete.
   */
  private def qualifyParseIdentities(
      parseIds: Seq[Seq[String]],
      analyzedIds: Seq[Seq[String]],
      analysisSucceeded: Boolean,
      scope: AnalysisScope,
      fallback: Seq[String] => Seq[String]): Seq[Seq[String]] = {
    val remaining = analyzedIds.to(mutable.ArrayBuffer)
    val assigned = Array.fill[Option[Seq[String]]](parseIds.length)(None)
    parseIds.zipWithIndex.sortBy { case (parts, _) => -parts.length }.foreach {
      case (parts, i) =>
        matchingIdentity(parts, remaining.toSeq) match {
          case Some(matched) =>
            val matchedKey = matched.map(_.toLowerCase(Locale.ROOT))
            val idx = remaining.indexWhere { analyzed =>
              analyzed.map(_.toLowerCase(Locale.ROOT)) == matchedKey
            }
            if (idx >= 0) {
              remaining.remove(idx)
            }
            assigned(i) = Some(matched)
          case None =>
            val ident =
              if (isPathReference(parts) ||
                  (analysisSucceeded && isFileReference(parts, scope))) {
                parts
              } else {
                fallback(parts)
              }
            assigned(i) = Some(ident)
        }
    }
    assigned.map(_.get).toSeq
  }

  private def matchingIdentity(
      parseParts: Seq[String],
      analyzedIds: Seq[Seq[String]]): Option[Seq[String]] = {
    val parsed = parseParts.map(_.toLowerCase(Locale.ROOT))
    analyzedIds.find { analyzed =>
      val ident = analyzed.map(_.toLowerCase(Locale.ROOT))
      ident.endsWith(parsed) &&
        (ident.length > parsed.length ||
          ident.length == parsed.length && parsed.length >= 2)
    }
  }

  /** Absolute or URI-like path written in the statement text. */
  private def isPathReference(parts: Seq[String]): Boolean = parts match {
    case Seq(_, path) => path.contains("/") || path.contains(":")
    case Seq(path) => path.contains("/") || path.startsWith("file:")
    case _ => false
  }

  /**
   * A `format`.`path` reference that analysis resolved to a file relation without a catalog
   * table. With `spark.sql.runSQLOnFiles` (default on) and the session catalog current, a
   * two-part name whose first part is a data source and not a session database is read as a
   * path, relative or not. The analyzed plan carries no identity for it, so the name is
   * reported as written.
   */
  private def isFileReference(parts: Seq[String], scope: AnalysisScope): Boolean = {
    val mgr = scope.catalogManager
    parts match {
      case Seq(format, _)
          if mgr.currentCatalog.name().equalsIgnoreCase(CatalogManager.SESSION_CATALOG_NAME) =>
        val isDataSource =
          try {
            DataSource.lookupDataSource(format, scope.session.sessionState.conf)
            true
          } catch {
            case _: ClassNotFoundException => false
          }
        isDataSource && !scope.catalog.databaseExists(format)
      case _ => false
    }
  }

  private def qualifyTarget(
      parts: Seq[String],
      plan: LogicalPlan,
      scope: AnalysisScope): Seq[String] = plan match {
    case ExplainCommand(inner, _) =>
      qualifyTarget(parts, inner, scope)
    case c: CreateViewCommand if c.viewType == LocalTempView =>
      sessionIdent(parts.last)
    case c: CreateViewCommand if c.viewType == GlobalTempView =>
      globalTempIdent(parts.last)
    case c: CreateTempViewUsing if c.global =>
      globalTempIdent(parts.last)
    case _: CreateTempViewUsing =>
      sessionIdent(parts.last)
    case _: CreateTableAsSelect | _: ReplaceTableAsSelect | _: CreateTable |
         _: ReplaceTable | _: CreateView =>
      // Persistent DDL never names a temp view, even if one shares the name.
      qualifyWithCurrentNamespace(parts, scope)
    case _ =>
      qualifySource(parts, scope)
  }

  private def qualifySource(
      parts: Seq[String],
      scope: AnalysisScope): Seq[String] = {
    if (CatalogManager.isSessionQualifiedViewName(parts)) {
      // `session.v` and `system.session.v` name a temp view even when it does not exist.
      sessionIdent(parts.last)
    } else if (scope.catalog.isTempView(parts)) {
      val globalTemp = SQLConf.get.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE)
      val last = parts.last
      if (parts.exists(_.equalsIgnoreCase(globalTemp))) {
        globalTempIdent(last)
      } else {
        sessionIdent(last)
      }
    } else {
      qualifyWithCurrentNamespace(parts, scope)
    }
  }

  private def qualifyRoutine(
      parts: Seq[String],
      scope: AnalysisScope): Seq[String] = {
    val catalog = scope.catalog
    def isTemp(name: String): Boolean =
      catalog.isTemporaryFunction(FunctionIdentifier(name))
    def isPersistentIn(entry: Seq[String], name: String): Boolean = entry match {
      case Seq(cat, db) if cat.equalsIgnoreCase(CatalogManager.SESSION_CATALOG_NAME) =>
        Try(catalog.isPersistentFunction(FunctionIdentifier(name, Some(db)))).getOrElse(false)
      case cat +: ns if ns.nonEmpty && !cat.equalsIgnoreCase(CatalogManager.SESSION_CATALOG_NAME) =>
        Try(scope.catalogManager.catalog(cat)).toOption.exists {
          case fc: FunctionCatalog =>
            Try(fc.functionExists(Identifier.of(ns.toArray, name))).getOrElse(false)
          case _ => false
        }
      case _ => false
    }
    def isBuiltin(name: String): Boolean = {
      val id = FunctionIdentifier(name)
      FunctionRegistry.builtin.functionExists(id) ||
        TableFunctionRegistry.builtin.functionExists(id)
    }
    def isSessionBuiltin(name: String): Boolean = {
      val id = FunctionRegistry.builtinFunctionIdentifier(name)
      !isTemp(name) && !isBuiltin(name) &&
        (scope.session.sessionState.functionRegistry.functionExists(id) ||
          scope.session.sessionState.tableFunctionRegistry.functionExists(id))
    }
    def builtinIdent(name: String): Seq[String] =
      FunctionRegistry.builtinFunctionIdentifier(name).nameParts
    def isSystemEntry(entry: Seq[String], ns: String): Boolean = entry match {
      case Seq(cat, n) => cat.equalsIgnoreCase(CatalogManager.SYSTEM_CATALOG_NAME) &&
        n.equalsIgnoreCase(ns)
      case _ => false
    }
    // An unqualified name takes the first entry of the statement's path that has it, as in
    // FunctionResolution. Without SET PATH the path follows the session function order.
    def onPath(name: String): Option[Seq[String]] =
      scope.catalogManager.resolutionPathEntriesForAnalysis(None, Nil).iterator.collectFirst {
        case e if isSystemEntry(e, CatalogManager.BUILTIN_NAMESPACE) &&
            (isBuiltin(name) || isSessionBuiltin(name)) => builtinIdent(name)
        case e if isSystemEntry(e, CatalogManager.SESSION_NAMESPACE) && isTemp(name) =>
          sessionIdent(name)
        case e if isPersistentIn(e, name) => e :+ name
      }
    parts match {
      case Seq(name) =>
        onPath(name).getOrElse(qualifyWithCurrentNamespace(parts, scope))
      case Seq(ns, name) if isSystemNamespace(ns) =>
        Seq(CatalogManager.SYSTEM_CATALOG_NAME, ns.toLowerCase(Locale.ROOT), name)
      case Seq(cat, ns, name)
          if cat.equalsIgnoreCase(CatalogManager.SYSTEM_CATALOG_NAME) &&
            isSystemNamespace(ns) =>
        Seq(CatalogManager.SYSTEM_CATALOG_NAME, ns.toLowerCase(Locale.ROOT), name)
      case other =>
        qualifyWithCurrentNamespace(other, scope)
    }
  }

  /**
   * Qualify `parts` using the current catalog/namespace without loading a
   * catalog plugin. `isCatalogRegistered` instantiates the plugin.
   */
  private def qualifyWithCurrentNamespace(
      parts: Seq[String],
      scope: AnalysisScope): Seq[String] = {
    val mgr = scope.catalogManager
    val catalogName = mgr.currentCatalog.name()
    val namespace = mgr.currentNamespace.toSeq
    parts match {
      case Seq(name) => Seq(catalogName) ++ namespace :+ name
      case other if isKnownCatalogName(other.head, scope) => other
      case other => catalogName +: other
    }
  }

  private def isKnownCatalogName(
      name: String,
      scope: AnalysisScope): Boolean = {
    val mgr = scope.catalogManager
    // The session catalog name matches case-insensitively, as CatalogManager.catalog does.
    // Other catalogs are stored and loaded by their exact name.
    name.equalsIgnoreCase(CatalogManager.SESSION_CATALOG_NAME) ||
      name == mgr.currentCatalog.name() ||
      scope.session.sessionState.conf.contains("spark.sql.catalog." + name)
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
    // Row-level rewrites add internal columns; leave them out of select_list.
    case _: ReplaceData | _: WriteDelta => plan
    case w: V2WriteCommand => primaryQueryPlan(w.query)
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
