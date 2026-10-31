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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.parser.{ParseSqlMode, ParseSqlResult}
import org.apache.spark.sql.catalyst.plans.logical.{FunctionSignature, InputParameter}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{AbstractDataType, DataType, StringType}
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
  usage = """_FUNC_(sqlStmt[, mode]) -
    Splits `sqlStmt` into SQL statements, parses each with the stock Spark SQL parser, and
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
    `parse_sql` does not lend it its locals or cursors. `ANALYZE` inherits the invoker's
    current catalog, schema, and SQL path and does not change them.
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
        An expression that evaluates to a string. NULL returns NULL. Any other value
        fails the outer query with INVALID_PARAMETER_VALUE.PARSE_SQL_MODE.
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
    ANALYZE needs an active session, and `sqlStmt` and `mode` must be constant for the
    query (DATATYPE_MISMATCH.NON_FOLDABLE_INPUT otherwise); a DataFrame column of
    statements can use PARSE only. Each JSON object has `start` (1-based character
    offset in the batch), `length`, and `parse_success`. Parse success also has
    `statement_identifier` / `statement_code` (ISO/IEC 9075-2:2023 Table 39). ANALYZE
    adds `analyze_success`. Optional fields are omitted when empty:
    `target_table_references`, `source_table_references`, `function_references` (arrays
    of name parts; ANALYZE qualifies persistent names to catalog.schema.object, local
    temp views and temp functions as `system.session`, builtins as `system.builtin`;
    CTE aliases in scope are not listed as tables), `select_list` (`{name}` parts;
    ANALYZE lists the output columns, with `*` expanded, and adds `type` as a
    DESCRIBE TABLE EXTENDED AS JSON type object), `parameter_markers` (`named` and/or
    `unnamed_count`), and `error` (STANDARD format). Later statements are still
    processed after a per-statement parse or analysis failure. ANALYZE of a SQL
    script reports `analyze_success` false without executing the script.
  """,
  group = "misc_funcs",
  since = "4.4.0")
// scalastyle:on nonascii
// scalastyle:on line.size.limit
case class ParseSql(
    sqlStmt: Expression,
    mode: Expression)
  extends Expression
  with ImplicitCastInputTypes
  with CodegenFallback
  with QueryErrorsBase {

  def this(sqlStmt: Expression) = this(sqlStmt, Literal("PARSE"))

  override def prettyName: String = "parse_sql"

  override def children: Seq[Expression] = Seq(sqlStmt, mode)

  override def nullable: Boolean = sqlStmt.nullable || mode.nullable

  // Foldable so constant SQL is evaluated on the driver, where ANALYZE can see the session.
  override def foldable: Boolean = children.forall(_.foldable)

  override def nullIntolerant: Boolean = false

  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] =
    Seq.fill(2)(StringTypeWithCollation(supportsTrimCollation = true))

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
        // ANALYZE needs the session, which only the driver has, so sqlStmt
        // must fold. PARSE may take a column of statement text.
        if (parsedMode.contains(ParseSqlMode.Analyze) && !sqlStmt.foldable) {
          DataTypeMismatch(
            errorSubClass = "NON_FOLDABLE_INPUT",
            messageParameters = Map(
              "inputName" -> toSQLId("sqlStmt"),
              "inputType" -> toSQLType(sqlStmt.dataType),
              "inputExpr" -> toSQLExpr(sqlStmt)))
        } else {
          TypeCheckResult.TypeCheckSuccess
        }
      }
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
    UTF8String.fromString(ParseSqlResult.fromSql(sqlText, parsedMode))
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): ParseSql =
    copy(sqlStmt = newChildren(0), mode = newChildren(1))
}

object ParseSql extends ExpressionBuilder {
  def apply(sqlStmt: Expression): ParseSql = new ParseSql(sqlStmt)

  def apply(sqlStmt: Expression, mode: Expression): ParseSql = new ParseSql(sqlStmt, mode)

  /** Parameter names in argument order. */
  val parameterNames: Seq[String] = Seq("sqlStmt", "mode")

  override def functionSignature: Option[FunctionSignature] = Some(FunctionSignature(Seq(
    InputParameter("sqlStmt"),
    InputParameter("mode", Some(Literal("PARSE"))))))

  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    assert(expressions.size == 2)
    ParseSql(expressions(0), expressions(1))
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
