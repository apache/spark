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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{
  ExpressionBuilder, FunctionRegistry, FunctionRegistryBase, TypeCheckResult
}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.parser.{
  ParseSqlAnalyzeContext, ParseSqlMode, ParseSqlResult
}
import org.apache.spark.sql.catalyst.plans.logical.{FunctionSignature, InputParameter}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{
  AbstractDataType, ArrayType, DataType, StringType, StructField, StructType
}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Parses a SQL batch string and returns a compact JSON array describing its
 * statements (source position, identifier/code, lineage references,
 * select-list names, parameters). `PARSE` mode is unresolved. `ANALYZE`
 * mode also types the select list and fully qualifies resolved relations
 * and routines. A statement that does not parse is represented by a
 * STANDARD-format error object at its position in the array.
 *
 * Behind [[SQLConf.PARSE_SQL_ENABLED]] while the JSON contract is still
 * evolving. Designed for batch evaluation over DataFrames of SQL text.
 * User-facing parse / analyze errors become JSON; unexpected internal
 * failures propagate. `ANALYZE` needs an active Spark session (catalog)
 * and inherits the invoker's current catalog, schema, and SQL path.
 * Optional named `current_catalog`, `current_schema`, and `current_path`
 * override that context for the duration of analysis. Optional named
 * `variables`, `relations`, and `parameter_markers` inject known session /
 * procedure variables, non-persisted relations, and typed parameter markers
 * so ANALYZE can resolve them.
 */
// scalastyle:off line.size.limit
// scalastyle:off nonascii
@ExpressionDescription(
  usage = """_FUNC_(sqlStmt[, mode[, current_catalog[, current_schema[, current_path[, variables[, relations[, parameter_markers]]]]]]]) -
    Splits `sqlStmt` into SQL statements, parses each with the stock Spark SQL parser, and
    returns a JSON array describing them so callers can inspect SQL text (lineage, statement
    kind, select-list types) without running the statements. Each object has 1-based
    character `start` and character `length` (Unicode code points; an emoji is one
    character), parse success, Table 39 statement identifier/code, target and source
    table references, select-list column names, and parameter markers. Every parameter
    may be passed by name (`sqlStmt => ...`, `mode => ...`).
    `mode` is `PARSE` (default) or `ANALYZE` (case-insensitive). `PARSE` is unresolved and
    does not need a catalog. `ANALYZE` needs an active session; it types selected columns,
    fully qualifies resolved relations and routines, and reports analysis failures as JSON
    without aborting the batch. `ANALYZE` inherits the invoker's current catalog, schema,
    and SQL path. Named `current_catalog`, `current_schema`, and `current_path` override
    that context for analysis only; omitted or NULL keeps the invoker value, which is
    restored after the call. Named `variables` is an array of structs (`name`, `type`) for
    session or procedure variables not in the persisted schema. Named `relations` is an
    array of structs (`name`, `schema`) whose `schema` is a DDL column list, parsed like
    `from_json`. Named `parameter_markers` is an array of structs (`name`, `type`) whose
    `name` is `?` for positional markers or a simple identifier for named markers (`:p`).
    Omitted or NULL injects nothing. `PARSE` ignores the catalog, overlay, and marker
    arguments. Statement length excludes surrounding whitespace and the terminating
    semicolon. Session parser extensions are not applied. The JSON contract may still
    change while spark.sql.function.parseSql.enabled remains off by default. On syntax /
    parse error returns JSON for that statement with `parse_success` false, source
    location, and a nested STANDARD error object instead of throwing or stopping later
    statements. Nested error locations are statement-relative Unicode code
    points. Unexpected internal failures still fail the outer query. An empty
    or closed-comment-only batch returns
    `[]`. NULL `sqlStmt` or `mode` returns NULL.""",
  arguments = """
    Arguments:
      * sqlStmt - A SQL batch string to split and parse.
        An expression that evaluates to a string. NULL returns NULL.
      * mode - Optional `PARSE` or `ANALYZE` (case-insensitive). Defaults to `PARSE`.
        An expression that evaluates to a string. NULL returns NULL. Any other value
        fails the outer query with INVALID_PARAMETER_VALUE.PARSE_SQL_MODE.
      * current_catalog - Optional catalog name used as current catalog during `ANALYZE`.
        NULL or omitted inherits the invoker session. Ignored in `PARSE`. The invoker
        catalog, schema, and path are restored after analysis.
      * current_schema - Optional schema / namespace used as current schema during `ANALYZE`.
        NULL or omitted inherits the invoker session. Ignored in `PARSE`.
      * current_path - Optional SQL PATH used during `ANALYZE` (SET PATH grammar, or the
        string returned by `current_path()`). NULL or omitted inherits the invoker session.
        Ignored in `PARSE`.
      * variables - Optional ARRAY<STRUCT<name STRING, type STRING>> of known variables
        for `ANALYZE` (for example `system.session.var` or `proc.parm1`). `type` is a DDL
        data type string as accepted by `from_json`. NULL or omitted injects none.
        Ignored in `PARSE`. Session-shaped names (`v`, `session.v`, `system.session.v`)
        resolve as `system.session.<name>`. Other names keep their identity. Looked up
        after real SQL-script locals; a supplied session variable shadows a persisted
        one without mutating it. Unqualified session names still follow the SQL path.
      * relations - Optional ARRAY<STRUCT<name STRING, schema STRING>> of known relations
        (tables or views) for `ANALYZE`. `name` is a relation identity and may be
        qualified (`catalog.schema.t`, `system.session.v`). `schema` is a DDL column list
        (`a INT, b STRING`) as accepted by `from_json`. NULL or omitted injects none.
        Ignored in `PARSE`. Session-shaped names canonicalize to `system.session.<name>`.
        Other names follow ordinary relation qualification (one-part: analysis catalog
        and schema). During ANALYZE the overlay eclipses the real object of that
        identity: temp view, persistent table, or view; query source or DML target.
        PATH still selects the identity of an unqualified SQL name.
      * parameter_markers - Optional ARRAY<STRUCT<name STRING, type STRING>> of known
        parameter markers for `ANALYZE`. `name` is `?` for each positional marker in
        left-to-right order, or a simple identifier for `:name`. `type` is a DDL data
        type string as accepted by `from_json`. NULL or omitted injects none.
        Ignored in `PARSE`, which still reports marker names and `unnamed_count`
        without binding types. Bound as typed NULLs so functions can type-check;
        values are not executed. Named and unnamed markers cannot be mixed in one
        statement.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('SELECT 1;SELECT 2');
       [{"start":1,"length":8,"parse_success":true,"statement_identifier":"SELECT","statement_code":21,"select_list":[{"name":[]}]},{"start":10,"length":8,"parse_success":true,"statement_identifier":"SELECT","statement_code":21,"select_list":[{"name":[]}]}]
      > SELECT get_json_object(_FUNC_('SELECT ''😀'';SELECT 2'), '$[1].start');
       12
      > SELECT get_json_object(_FUNC_('SELEC'), '$[0].error.errorClass');
       PARSE_SYNTAX_ERROR
      > SELECT get_json_object(_FUNC_('SELECT 1 AS a', 'ANALYZE'), '$[0].select_list[0].type.name');
       int
      > SELECT get_json_object(_FUNC_('SELECT a FROM missing_t', 'ANALYZE'), '$[0].analyze_success');
       false
      > SELECT get_json_object(_FUNC_('SELECT a FROM t', 'ANALYZE', relations => array(named_struct('name', 't', 'schema', 'a INT'))), '$[0].select_list[0].type.name');
       int
      > SELECT get_json_object(_FUNC_('SELECT system.session.v AS x', 'ANALYZE', variables => array(named_struct('name', 'v', 'type', 'INT'))), '$[0].select_list[0].type.name');
       int
      > SELECT get_json_object(_FUNC_('SELECT abs(?) AS x', 'ANALYZE', parameter_markers => array(named_struct('name', '?', 'type', 'INT'))), '$[0].select_list[0].type.name');
       int
      > SELECT get_json_object(_FUNC_('SELECT proc.parm1 AS x', 'ANALYZE', variables => array(named_struct('name', 'proc.parm1', 'type', 'STRING'))), '$[0].select_list[0].type.name');
       string
      > SELECT get_json_object(_FUNC_('SELECT :p AS x', 'ANALYZE', parameter_markers => array(named_struct('name', 'p', 'type', 'STRING'))), '$[0].select_list[0].type.name');
       string
  """,
  note = """
    Requires spark.sql.function.parseSql.enabled=true. The result type is STRING JSON.
    ANALYZE needs an active session. Overlays exist only for that call: Spark does not
    register temp views, mutate session variables, or install a SQL-script context.
    A NULL array element or empty name/type/schema fails the outer query with
    INVALID_PARAMETER_VALUE.PARSE_SQL_CONTEXT. Invalid DDL types and names are recorded
    as JSON (`analyze_success` false) and do not fail the outer query. Each JSON object
    has `start` (1-based character offset in the batch), `length`, and
    `parse_success`. Parse success also has `statement_identifier` /
    `statement_code` (ISO/IEC 9075-2:2023 Table 39). ANALYZE adds
    `analyze_success`. Optional fields are omitted when empty:
    `target_table_references`, `source_table_references`, `function_references` (arrays
    of name parts; ANALYZE qualifies persistent names, local temp views and temp
    functions as `system.session`, builtins as `system.builtin`; CTE aliases in
    scope are not listed as tables), `select_list` (`{name}` parts; ANALYZE adds
    `type` as a DESCRIBE TABLE EXTENDED AS JSON type object), `parameter_markers` (`named`
    and/or `unnamed_count`), and `error` (STANDARD format; nested `queryContext`
    `startIndex`/`stopIndex` are 1-based Unicode code points, statement-relative).
    Later statements are still processed after a per-statement
    parse or analysis failure.
  """,
  group = "misc_funcs",
  since = "4.4.0")
// scalastyle:on nonascii
// scalastyle:on line.size.limit
case class ParseSql(
    sqlStmt: Expression,
    mode: Expression,
    currentCatalog: Expression,
    currentSchema: Expression,
    currentPath: Expression,
    variables: Expression,
    relations: Expression,
    parameterMarkers: Expression)
  extends Expression
  with ImplicitCastInputTypes
  with CodegenFallback {

  def this(
      sqlStmt: Expression,
      mode: Expression,
      currentCatalog: Expression,
      currentSchema: Expression,
      currentPath: Expression) =
    this(
      sqlStmt,
      mode,
      currentCatalog,
      currentSchema,
      currentPath,
      ParseSql.nullVariables,
      ParseSql.nullRelations,
      ParseSql.nullParameterMarkers)

  def this(
      sqlStmt: Expression,
      mode: Expression,
      currentCatalog: Expression,
      currentSchema: Expression) =
    this(
      sqlStmt,
      mode,
      currentCatalog,
      currentSchema,
      Literal.create(null, StringType),
      ParseSql.nullVariables,
      ParseSql.nullRelations,
      ParseSql.nullParameterMarkers)

  def this(sqlStmt: Expression, mode: Expression, currentCatalog: Expression) =
    this(
      sqlStmt,
      mode,
      currentCatalog,
      Literal.create(null, StringType),
      Literal.create(null, StringType),
      ParseSql.nullVariables,
      ParseSql.nullRelations,
      ParseSql.nullParameterMarkers)

  def this(sqlStmt: Expression, mode: Expression) =
    this(
      sqlStmt,
      mode,
      Literal.create(null, StringType),
      Literal.create(null, StringType),
      Literal.create(null, StringType),
      ParseSql.nullVariables,
      ParseSql.nullRelations,
      ParseSql.nullParameterMarkers)

  def this(sqlStmt: Expression) = this(sqlStmt, Literal("PARSE"))

  override def prettyName: String = "parse_sql"

  override def children: Seq[Expression] =
    Seq(
      sqlStmt, mode, currentCatalog, currentSchema, currentPath,
      variables, relations, parameterMarkers)

  override def nullable: Boolean = sqlStmt.nullable || mode.nullable

  // Foldable so constant SQL is evaluated on the driver, where ANALYZE can see the session.
  override def foldable: Boolean = children.forall(_.foldable)

  override def nullIntolerant: Boolean = false

  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] =
    Seq.fill(5)(StringTypeWithCollation(supportsTrimCollation = true)) ++
      Seq(ParseSql.variablesType, ParseSql.relationsType, ParseSql.parameterMarkersType)

  // ANALYZE reads the session catalog, so results are not context-independent.
  override def contextIndependentFoldable: Boolean = false

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!SQLConf.get.parseSqlEnabled) {
      throw new AnalysisException(
        errorClass = "FEATURE_NOT_ENABLED",
        messageParameters = Map(
          "featureName" -> "parse_sql",
          "configKey" -> SQLConf.PARSE_SQL_ENABLED.key,
          "configValue" -> "true"))
    }
    val typeCheck = super.checkInputDataTypes()
    if (!typeCheck.isSuccess) {
      typeCheck
    } else {
      if (mode.foldable) {
        val value = mode.eval()
        if (value != null) {
          val modeStr = value.asInstanceOf[UTF8String].toString
          if (ParseSqlMode.parse(modeStr).isEmpty) {
            throw QueryCompilationErrors.invalidParseSqlMode(mode)
          }
        }
      }
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def eval(input: InternalRow): Any = {
    val sqlInput = sqlStmt.eval(input)
    if (sqlInput == null) {
      return null
    }
    val modeInput = mode.eval(input)
    if (modeInput == null) {
      return null
    }
    val sqlText = sqlInput.asInstanceOf[UTF8String].toString
    val modeStr = modeInput.asInstanceOf[UTF8String].toString
    val parsedMode = ParseSqlMode.parse(modeStr).getOrElse {
      throw QueryExecutionErrors.invalidParseSqlMode(modeStr)
    }
    val context = if (parsedMode == ParseSqlMode.Analyze) {
      ParseSqlAnalyzeContext(
        catalog = utf8Option(currentCatalog.eval(input)),
        schema = utf8Option(currentSchema.eval(input)),
        path = utf8Option(currentPath.eval(input)),
        variables = ParseSql.decodeNameSpecArray(
          variables.eval(input), "variables", "type"),
        relations = ParseSql.decodeNameSpecArray(
          relations.eval(input), "relations", "schema"),
        parameterMarkers = ParseSql.decodeNameSpecArray(
          parameterMarkers.eval(input), "parameter_markers", "type"))
    } else {
      ParseSqlAnalyzeContext.empty
    }
    UTF8String.fromString(ParseSqlResult.fromSql(sqlText, parsedMode, context))
  }

  private def utf8Option(value: Any): Option[String] =
    Option(value).map(_.asInstanceOf[UTF8String].toString)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): ParseSql =
    copy(
      sqlStmt = newChildren(0),
      mode = newChildren(1),
      currentCatalog = newChildren(2),
      currentSchema = newChildren(3),
      currentPath = newChildren(4),
      variables = newChildren(5),
      relations = newChildren(6),
      parameterMarkers = newChildren(7))
}

object ParseSql extends ExpressionBuilder {
  val variableEntryType: StructType = StructType(Seq(
    StructField("name", StringType, nullable = false),
    StructField("type", StringType, nullable = false)))

  val relationEntryType: StructType = StructType(Seq(
    StructField("name", StringType, nullable = false),
    StructField("schema", StringType, nullable = false)))

  val variablesType: DataType = ArrayType(variableEntryType, containsNull = false)

  val relationsType: DataType = ArrayType(relationEntryType, containsNull = false)

  val parameterMarkersType: DataType = variablesType

  def nullVariables: Expression = Literal.create(null, variablesType)

  def nullRelations: Expression = Literal.create(null, relationsType)

  def nullParameterMarkers: Expression = Literal.create(null, parameterMarkersType)

  def apply(sqlStmt: Expression): ParseSql = new ParseSql(sqlStmt)

  def apply(sqlStmt: Expression, mode: Expression): ParseSql = new ParseSql(sqlStmt, mode)

  /** Decode ARRAY<STRUCT<name, spec>> pairs. NULL means omit. */
  private[expressions] def decodeNameSpecArray(
      value: Any,
      parameter: String,
      specField: String): Seq[(String, String)] = value match {
    case null => Nil
    case arr: ArrayData =>
      val n = arr.numElements()
      (0 until n).map { i =>
        if (arr.isNullAt(i)) {
          throw QueryExecutionErrors.invalidParseSqlContext(parameter, "NULL")
        }
        val row = arr.getStruct(i, 2)
        def field(ord: Int, fieldName: String): String = {
          if (row.isNullAt(ord)) {
            throw QueryExecutionErrors.invalidParseSqlContext(
              parameter, s"NULL $fieldName")
          }
          val text = row.getUTF8String(ord).toString
          if (text.trim.isEmpty) {
            throw QueryExecutionErrors.invalidParseSqlContext(parameter, text)
          }
          text
        }
        (field(0, "name"), field(1, specField))
      }
    case other =>
      throw QueryExecutionErrors.invalidParseSqlContext(parameter, String.valueOf(other))
  }

  // All eight parameters are named-invocation eligible, including required `sqlStmt`.
  override def functionSignature: Option[FunctionSignature] = Some(FunctionSignature(Seq(
    InputParameter("sqlStmt"),
    InputParameter("mode", Some(Literal("PARSE"))),
    InputParameter("current_catalog", Some(Literal.create(null, StringType))),
    InputParameter("current_schema", Some(Literal.create(null, StringType))),
    InputParameter("current_path", Some(Literal.create(null, StringType))),
    InputParameter("variables", Some(nullVariables)),
    InputParameter("relations", Some(nullRelations)),
    InputParameter("parameter_markers", Some(nullParameterMarkers)))))

  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    assert(expressions.size == 8)
    ParseSql(
      expressions(0),
      expressions(1),
      expressions(2),
      expressions(3),
      expressions(4),
      expressions(5),
      expressions(6),
      expressions(7))
  }

  /** Register the builtin with a session function registry and the global builtin set. */
  def register(registry: FunctionRegistry): Unit = {
    val info = FunctionRegistryBase.expressionInfo[ParseSql]("parse_sql", Some("4.4.0"))
    val builder = (expressions: Seq[Expression]) => {
      val rearranged = FunctionRegistry.rearrangeExpressions("parse_sql", this, expressions)
      build("parse_sql", rearranged)
    }
    // Keep the session registry in sync for the first session (cloned before this runs).
    registry.registerFunction(
      FunctionRegistry.builtinFunctionIdentifier("parse_sql"),
      info,
      builder)
    // Also publish into FunctionRegistry.builtin / functionSet so SHOW USER/SYSTEM
    // FUNCTIONS classify parse_sql as SYSTEM rather than a user/temp function.
    FunctionRegistry.registerExtraBuiltin("parse_sql", info, builder)
  }
}
