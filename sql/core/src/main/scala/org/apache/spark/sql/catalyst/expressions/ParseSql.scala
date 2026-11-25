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

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{
  ExpressionBuilder, FunctionRegistry, FunctionRegistryBase, TypeCheckResult
}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.parser.{ParseSqlAnalyzeContext, ParseSqlMode, ParseSqlResult}
import org.apache.spark.sql.catalyst.plans.logical.{FunctionSignature, InputParameter}
import org.apache.spark.sql.catalyst.trees.TreePattern.{PARSE_SQL, TreePattern}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase, QueryExecutionErrors}
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
 */
// scalastyle:off line.size.limit
// scalastyle:off nonascii
@ExpressionDescription(
  usage = """_FUNC_(sqlStmt[, mode[, current_catalog[, current_schema[, current_path[, variables[, relations[, parameter_markers]]]]]]]) - Splits `sqlStmt` into SQL statements, parses each with the stock Spark SQL parser, and
    returns a JSON array describing them so callers can inspect SQL text (lineage, statement
    kind, select-list types) without running the statements. Each object has 1-based
    character `start` and character `length` (Unicode code points; a single-code-point
    character, including a BMP glyph or one supplementary emoji, counts as one),
    parse success, Table 39 statement identifier/code, target and source
    table references, select-list column names, and parameter markers. Every parameter
    may be passed by name (`sqlStmt => ...`, `mode => ...`).
    `mode` is `PARSE` (default) or `ANALYZE` (case-insensitive). `PARSE` is unresolved and
    does not need a catalog. `ANALYZE` needs an active session; it types selected columns,
    fully qualifies resolved relations and routines, and reports analysis failures as JSON
    without aborting the batch. Each statement is analyzed against the session as it is,
    without being executed, so a DECLARE, CREATE, USE, or SET does not affect later
    statements. `CALL` is analyzed but the procedure is not run. A SQL script calling
    `parse_sql` does not lend it its locals or cursors. `ANALYZE` uses the invoker's current
    catalog, schema, and SQL path unless `current_catalog`, `current_schema`, or
    `current_path` gives one for the call; the session is not changed. The body of an
    `EXECUTE IMMEDIATE` is analyzed with the call's catalog, schema, and path. It sees the
    call's session variables and `relations`, whose unqualified names are qualified with the
    call's catalog and schema, but not its supplied locals such as a procedure parameter.
    Analysis may still run time-travel subqueries, infer file schemas, and call Python UDTF
    `analyze`.
    Statement length excludes surrounding
    whitespace and the terminating semicolon. Session parser extensions are not applied.
    The JSON contract may still change while spark.sql.function.parseSql.enabled remains
    off by default. On syntax / parse error returns JSON for that statement with
    `parse_success` false, source location, and a nested STANDARD error object instead of
    throwing or stopping later statements. Nested `queryContext` is the STANDARD
    renderer output (view and EXECUTE IMMEDIATE bodies keep their own text).
    Unexpected internal failures still fail the outer query. An
    empty or closed-comment-only batch returns `[]`. NULL `sqlStmt` or `mode` returns
    NULL.""",
  arguments = """
    Arguments:
      * sqlStmt - A SQL batch string to split and parse.
        An expression that evaluates to a string. NULL returns NULL.
      * mode - Optional `PARSE` or `ANALYZE` (case-insensitive). Defaults to `PARSE`.
        Must be a constant string. NULL returns NULL. Any other value fails the outer
        query with INVALID_PARAMETER_VALUE.PARSE_SQL_MODE.
      * current_catalog - Optional catalog for `ANALYZE` of this call only. Must be a
        constant string. NULL or omitted keeps the invoker's catalog. An unknown catalog
        fails the outer query.
      * current_schema - Optional schema for `ANALYZE` of this call only. Must be a constant
        string. NULL or omitted keeps the invoker's schema (or the catalog default when
        `current_catalog` names another catalog). A schema that does not exist fails the
        outer query.
      * current_path - Optional SQL path for `ANALYZE` of this call only, as in `SET PATH`.
        Must be a constant string. NULL or omitted keeps the invoker's path. A blank path
        uses the conf default path. The path feature is enabled for this call only.
      * variables - Optional constant array of `named_struct('name', ..., 'type', ...)`
        entries. Each name is a variable that `ANALYZE` treats as declared with the DDL
        type: a session variable (`v`, `session.v`, or `system.session.v`) or a qualified
        local such as a procedure parameter (`proc.p`). The variable is not created, and
        the body of a view or SQL function still reads the real variable.
      * relations - Optional constant array of `named_struct('name', ..., 'schema', ...)`
        entries. Each name is a relation that `ANALYZE` treats as existing with the DDL
        schema; nothing is created. Query sources and the targets of INSERT, UPDATE,
        DELETE, and MERGE resolve to the entry. DDL and other commands that name a table
        act on the catalog's own objects, and the body of a view or SQL function still
        reads the real relation. A metric view's definition is expanded into the statement,
        so it does see the entry.
      * parameter_markers - Optional constant array of `named_struct('name', ..., 'type', ...)`
        entries. The name is `?` for a positional marker or the marker name for a named
        marker. A marker without a type leaves the statement unresolved.
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
  """,
  note = """
    Requires spark.sql.function.parseSql.enabled=true. The result type is STRING JSON.
    ANALYZE needs an active session, and every argument must be constant for the query
    (DATATYPE_MISMATCH.NON_FOLDABLE_INPUT otherwise); a DataFrame column of statements
    can use PARSE only. Each JSON object has `start` (1-based character
    offset in the batch), `length`, and `parse_success`. Parse success also has
    `statement_identifier` / `statement_code` (ISO/IEC 9075-2:2023 Table 39). ANALYZE
    adds `analyze_success`. Optional fields are omitted when empty:
    `target_table_references`, `source_table_references`, `function_references` (arrays
    of name parts; ANALYZE qualifies persistent names to catalog.schema.object, local
    temp views and temp functions as `system.session`, builtins as `system.builtin`;
    CTE aliases in scope are not listed as tables), `select_list` (`{name}` parts;
    ANALYZE lists the output columns, with `*` expanded, and adds `type` as a
    DESCRIBE TABLE EXTENDED AS JSON type object), `parameter_markers` (`named` and/or
    `unnamed_count`), and `error` (STANDARD format). Nested error locations are
    statement-relative Unicode code points. Later statements are still processed
    after a per-statement parse or analysis failure. ANALYZE of a SQL script reports
    `analyze_success` false without executing the script. Path-based relations keep
    `[format, path]` as written. Analysis may still run time-travel subqueries,
    infer file schemas, and call Python UDTF `analyze`.
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
  with CodegenFallback
  with QueryErrorsBase {

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
      ParseSql.nullPath,
      ParseSql.nullVariables,
      ParseSql.nullRelations,
      ParseSql.nullParameterMarkers)

  def this(sqlStmt: Expression, mode: Expression) =
    this(sqlStmt, mode, Literal.create(null, StringType), Literal.create(null, StringType))

  def this(sqlStmt: Expression) = this(sqlStmt, Literal("PARSE"))

  override def prettyName: String = "parse_sql"

  override def children: Seq[Expression] =
    Seq(
      sqlStmt, mode, currentCatalog, currentSchema, currentPath,
      variables, relations, parameterMarkers)

  // Omit trailing NULL context arguments, so the generated column name for a call that
  // gives no context is unchanged. The defaults are NULL literals, possibly implicitly cast,
  // and the name is taken after pretty-printing, which shows a NULL literal as NULL.
  override def sql: String = {
    def isDefault(e: Expression): Boolean = e match {
      case Literal(null, _) => true
      case c: Cast => isDefault(c.child)
      case _ => e.sql == "NULL"
    }
    val shown = children.take(2) ++ children.drop(2).reverse.dropWhile(isDefault).reverse
    s"$prettyName(${shown.map(_.sql).mkString(", ")})"
  }

  override def nullable: Boolean = sqlStmt.nullable || mode.nullable

  // Foldable lets ConstantFolding fold constant PARSE calls. Constant ANALYZE calls are
  // evaluated on the driver by EvaluateParseSql, which needs the session.
  override def foldable: Boolean = children.forall(_.foldable)

  override val nodePatterns: Seq[TreePattern] = Seq(PARSE_SQL)

  // Every context argument defaults to a NULL literal, so null propagation would fold every
  // call that omits one to NULL. eval returns NULL itself for a NULL statement or mode.
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
      if (!mode.foldable) {
        DataTypeMismatch(
          errorSubClass = "NON_FOLDABLE_INPUT",
          messageParameters = Map(
            "inputName" -> toSQLId("mode"),
            "inputType" -> toSQLType(mode.dataType),
            "inputExpr" -> toSQLExpr(mode)))
      } else {
        val parsedMode = Option(mode.eval()).map { value =>
          val modeStr = value.asInstanceOf[UTF8String].toString
          ParseSqlMode.parse(modeStr).getOrElse {
            throw QueryCompilationErrors.invalidParseSqlMode(mode)
          }
        }
        // ANALYZE needs the session, which only the driver has, so every argument must
        // fold for the call to run there. PARSE may take a column of statement text.
        val nonFoldable = ParseSql.parameterNames.zip(children).find(c => !constantArg(c._2))
        (parsedMode, nonFoldable) match {
          case (Some(ParseSqlMode.Analyze), Some((name, child))) =>
            DataTypeMismatch(
              errorSubClass = "NON_FOLDABLE_INPUT",
              messageParameters = Map(
                "inputName" -> toSQLId(name),
                "inputType" -> toSQLType(child.dataType),
                "inputExpr" -> toSQLExpr(child)))
          case _ => TypeCheckResult.TypeCheckSuccess
        }
      }
    }
  }

  /**
   * True if `e` folds once the optimizer has replaced the current_* functions with literals,
   * which happens before [[org.apache.spark.sql.catalyst.optimizer.EvaluateParseSql]] runs.
   */
  private def constantArg(e: Expression): Boolean = e.transformUp {
    case c @ (_: CurrentDatabase | _: CurrentCatalog | _: CurrentPath | _: CurrentUser) =>
      Literal.create(null, c.dataType)
  }.foldable

  /**
   * The normalized mode, or `None` when the mode is NULL. `mode` is foldable (checked in
   * [[checkInputDataTypes]]), so it is evaluated once rather than per row.
   */
  @transient private lazy val constantMode: Option[String] =
    Option(mode.eval()).map { value =>
      ParseSqlMode.parse(value.asInstanceOf[UTF8String].toString).getOrElse {
        throw SparkException.internalError(
          s"parse_sql mode passed analysis but is not PARSE or ANALYZE: $value")
      }
    }

  /**
   * Whether this is a constant ANALYZE call. Such a call needs the driver session, so
   * [[org.apache.spark.sql.catalyst.optimizer.EvaluateParseSql]] folds only these.
   */
  private[sql] def isConstantAnalyze: Boolean =
    children.forall(_.foldable) && constantMode.contains(ParseSqlMode.Analyze)

  override def eval(input: InternalRow): Any = {
    val sqlInput = sqlStmt.eval(input)
    if (sqlInput == null) {
      return null
    }
    constantMode match {
      case None => null
      case Some(parsedMode) =>
        val sqlText = sqlInput.asInstanceOf[UTF8String].toString
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

  def nullPath: Expression = Literal.create(null, StringType)

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
          throw QueryExecutionErrors.invalidParseSqlContextNull(parameter, "NULL")
        }
        val row = arr.getStruct(i, 2)
        def field(ord: Int, fieldName: String): String = {
          if (row.isNullAt(ord)) {
            throw QueryExecutionErrors.invalidParseSqlContextNull(
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

  /** Argument names in order, as they appear in error messages. */
  val parameterNames: Seq[String] = Seq(
    "sqlStmt", "mode", "current_catalog", "current_schema", "current_path",
    "variables", "relations", "parameter_markers")

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
