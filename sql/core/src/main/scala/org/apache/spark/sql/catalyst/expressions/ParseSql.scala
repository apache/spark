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
 * `variables` and `relations` inject known session/procedure variables
 * and non-persisted relations so ANALYZE can resolve them.
 */
// scalastyle:off line.size.limit
// scalastyle:off nonascii
@ExpressionDescription(
  usage = """_FUNC_(sqlStmt[, mode[, current_catalog[, current_schema[, current_path[, variables[, relations]]]]]]) -
    Splits `sqlStmt` into SQL statements, parses each with the stock Spark SQL parser, and
    returns a JSON array describing them (1-based UTF-16 code-unit `start` and UTF-16
    code-unit `length`, parse success, Table 39 statement identifier/code, target and source
    table references for lineage, select-list column names, and parameter markers).
    `mode` is `PARSE` (default) or `ANALYZE` (case-insensitive). `PARSE` is unresolved.
    `ANALYZE` also types selected columns and fully qualifies resolved relations and
    routines, and reports analysis failures as JSON without aborting the batch.
    `ANALYZE` inherits the invoker's current catalog, schema, and SQL path. Named
    `current_catalog`, `current_schema`, and `current_path` override that context for
    analysis only; omitted or NULL keeps the invoker value. Named `variables` is an array of
    structs (`name`, `type`) for session or procedure variables not in the persisted schema.
    Named `relations` is an array of structs (`name`, `schema`) whose `schema` is a DDL
    column list, parsed like `from_json`. Omitted or NULL injects nothing.
    Statement length excludes surrounding whitespace and the terminating semicolon.
    Session parser extensions are not applied.
    Requires spark.sql.function.parseSql.enabled=true. On syntax / parse error returns JSON
    for that statement with `parse_success` false, source location, and a nested STANDARD
    error object instead of throwing or stopping the remaining statements. Nested error
    locations are statement-relative. An empty or closed-comment-only batch returns `[]`.""",
  arguments = """
    Arguments:
      * sqlStmt - A SQL batch string to split and parse.
        An expression that evaluates to a string.
      * mode - Optional `PARSE` or `ANALYZE` (case-insensitive). Defaults to `PARSE`.
        An expression that evaluates to a string.
      * current_catalog - Optional catalog name used as current catalog during `ANALYZE`.
        NULL or omitted inherits the invoker session. Named argument only after `mode`.
      * current_schema - Optional schema / namespace used as current schema during `ANALYZE`.
        NULL or omitted inherits the invoker session.
      * current_path - Optional SQL PATH used during `ANALYZE` (SET PATH grammar, or the
        string returned by `current_path()`). NULL or omitted inherits the invoker session.
      * variables - Optional ARRAY<STRUCT<name STRING, type STRING>> of known variables
        for `ANALYZE` (for example `system.session.var` or `proc.parm1`). `type` is a DDL
        data type string as accepted by `from_json`. NULL or omitted injects none.
      * relations - Optional ARRAY<STRUCT<name STRING, schema STRING>> of known relations
        for `ANALYZE`. `name` may be qualified (`catalog.schema.t`); it is not forced into
        the session namespace. `schema` is a DDL column list (`a INT, b STRING`) as
        accepted by `from_json`. NULL or omitted injects none.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('SELECT 1;SELECT 2');
       [{"start":1,"length":8,"parse_success":true,"statement_identifier":"SELECT","statement_code":21,"select_list":[{"name":[]}]},{"start":10,"length":8,"parse_success":true,"statement_identifier":"SELECT","statement_code":21,"select_list":[{"name":[]}]}]
      > SELECT get_json_object(_FUNC_('SELECT ''😀'';SELECT 2'), '$[1].start');
       13
      > SELECT get_json_object(_FUNC_('SELEC'), '$[0].error.errorClass');
       PARSE_SYNTAX_ERROR
      > SELECT get_json_object(_FUNC_('SELECT 1 AS a', 'ANALYZE'), '$[0].select_list[0].type');
       INT
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
    relations: Expression)
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
      ParseSql.nullRelations)

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
      ParseSql.nullRelations)

  def this(sqlStmt: Expression, mode: Expression, currentCatalog: Expression) =
    this(
      sqlStmt,
      mode,
      currentCatalog,
      Literal.create(null, StringType),
      Literal.create(null, StringType),
      ParseSql.nullVariables,
      ParseSql.nullRelations)

  def this(sqlStmt: Expression, mode: Expression) =
    this(
      sqlStmt,
      mode,
      Literal.create(null, StringType),
      Literal.create(null, StringType),
      Literal.create(null, StringType),
      ParseSql.nullVariables,
      ParseSql.nullRelations)

  def this(sqlStmt: Expression) = this(sqlStmt, Literal("PARSE"))

  override def prettyName: String = "parse_sql"

  override def children: Seq[Expression] =
    Seq(sqlStmt, mode, currentCatalog, currentSchema, currentPath, variables, relations)

  override def nullable: Boolean = sqlStmt.nullable || mode.nullable

  // Foldable so constant SQL is evaluated on the driver, where ANALYZE can see the session.
  override def foldable: Boolean = children.forall(_.foldable)

  override def nullIntolerant: Boolean = false

  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] =
    Seq.fill(5)(StringTypeWithCollation(supportsTrimCollation = true)) ++
      Seq(ParseSql.variablesType, ParseSql.relationsType)

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
          relations.eval(input), "relations", "schema"))
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
      relations = newChildren(6))
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

  def nullVariables: Expression = Literal.create(null, variablesType)

  def nullRelations: Expression = Literal.create(null, relationsType)

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

  override def functionSignature: Option[FunctionSignature] = Some(FunctionSignature(Seq(
    InputParameter("sqlStmt"),
    InputParameter("mode", Some(Literal("PARSE"))),
    InputParameter("current_catalog", Some(Literal.create(null, StringType))),
    InputParameter("current_schema", Some(Literal.create(null, StringType))),
    InputParameter("current_path", Some(Literal.create(null, StringType))),
    InputParameter("variables", Some(nullVariables)),
    InputParameter("relations", Some(nullRelations)))))

  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    assert(expressions.size == 7)
    ParseSql(
      expressions(0),
      expressions(1),
      expressions(2),
      expressions(3),
      expressions(4),
      expressions(5),
      expressions(6))
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
