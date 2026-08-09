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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.json.{GetJsonObjectEvaluator, JsonExpressionUtils,
  JsonPathParser, JsonPathResult, JsonTableEvaluator, JsonTablePathTrie, JsonToStructsEvaluator,
  JsonTupleEvaluator, MultiGetJsonObjectEvaluator, PathInstruction, SchemaOfJsonEvaluator,
  StructsToJsonEvaluator}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.catalyst.json._
import org.apache.spark.sql.catalyst.trees.TreePattern.{GET_JSON_OBJECT, JSON_TO_STRUCT,
  RUNTIME_REPLACEABLE, TreePattern}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Extracts json object from a json string based on json path specified, and returns json string
 * of the extracted json object. It will return null if the input json string is invalid.
 */
@ExpressionDescription(
  usage = "_FUNC_(json_txt, path) - Extracts a json object from `path`.",
  arguments = """
    Arguments:
      * json_txt - The JSON text to extract from.
        An expression that evaluates to a string.
      * path - The path identifying the JSON object to extract.
        An expression that evaluates to a string.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('{"a":"b"}', '$.a');
       b
      > SELECT _FUNC_('[{"a":"b"},{"a":"c"}]', '$[0].a');
       b
      > SELECT _FUNC_('[{"a":"b"},{"a":"c"}]', '$[*].a');
       ["b","c"]
  """,
  group = "json_funcs",
  since = "1.5.0")
case class GetJsonObject(json: Expression, path: Expression)
  extends BinaryExpression
  with ExpectsInputTypes
  with DefaultStringProducingExpression {

  override def left: Expression = json
  override def right: Expression = path
  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      StringTypeWithCollation(supportsTrimCollation = true),
      StringTypeWithCollation(supportsTrimCollation = true))
  override def nullable: Boolean = true
  override def prettyName: String = "get_json_object"

  final override val nodePatterns: Seq[TreePattern] = Seq(GET_JSON_OBJECT)

  @transient
  private lazy val evaluator = if (path.foldable) {
    new GetJsonObjectEvaluator(path.eval().asInstanceOf[UTF8String])
  } else {
    new GetJsonObjectEvaluator()
  }
  override def stateful: Boolean = true

  override def eval(input: InternalRow): Any = {
    evaluator.setJson(json.eval(input).asInstanceOf[UTF8String])
    if (!path.foldable) {
      evaluator.setPath(path.eval(input).asInstanceOf[UTF8String])
    }
    evaluator.evaluate()
  }

  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evaluatorClass = classOf[GetJsonObjectEvaluator].getName
    val initEvaluator = path.foldable match {
      case true if path.eval() != null =>
        val cachedPath = path.eval().asInstanceOf[UTF8String]
        val refCachedPath = ctx.addReferenceObj("cachedPath", cachedPath)
        s"new $evaluatorClass($refCachedPath)"
      case _ => s"new $evaluatorClass()"
    }
    val evaluator = ctx.addMutableState(evaluatorClass, "evaluator",
      v => s"""$v = $initEvaluator;""", forceInline = true)

    val jsonEval = json.genCode(ctx)
    val pathEval = path.genCode(ctx)

    val setJson =
      s"""
         |if (${jsonEval.isNull}) {
         |  $evaluator.setJson(null);
         |} else {
         |  $evaluator.setJson(${jsonEval.value});
         |}
         |""".stripMargin
    val setPath = if (!path.foldable) {
      s"""
         |if (${pathEval.isNull}) {
         |  $evaluator.setPath(null);
         |} else {
         |  $evaluator.setPath(${pathEval.value});
         |}
         |""".stripMargin
    } else {
      ""
    }

    val resultType = CodeGenerator.boxedType(dataType)
    val resultTerm = ctx.freshName("result")
    ev.copy(code =
      code"""
         |${jsonEval.code}
         |${pathEval.code}
         |$setJson
         |$setPath
         |$resultType $resultTerm = ($resultType) $evaluator.evaluate();
         |boolean ${ev.isNull} = $resultTerm == null;
         |${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
         |if (!${ev.isNull}) {
         |  ${ev.value} = $resultTerm;
         |}
         |""".stripMargin
    )
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): GetJsonObject =
    copy(json = newLeft, path = newRight)
}

object GetJsonObject {
  import PathInstruction._

  private[sql] sealed trait SimpleJsonPathSegment
  private[sql] case class NamedPathSegment(name: String) extends SimpleJsonPathSegment
  private[sql] case class IndexedPathSegment(index: Long) extends SimpleJsonPathSegment

  private[sql] def simplePath(path: UTF8String): Option[Seq[SimpleJsonPathSegment]] = {
    try {
      Option(path).flatMap(value => JsonPathParser.parse(value.toString)).flatMap { instructions =>
        val segments = instructions.grouped(2).map {
          case List(Key, Named(fieldName)) => Some(NamedPathSegment(fieldName))
          case List(Subscript, Index(index)) if index >= 0 => Some(IndexedPathSegment(index))
          case _ => None
        }.toSeq
        if (segments.nonEmpty && segments.forall(_.isDefined)) Some(segments.flatten) else None
      }
    } catch {
      // Numeric subscripts are parsed as Long and can overflow before the parser returns None.
      case _: NumberFormatException => None
    }
  }

}

/**
 * Extracts multiple simple object-key and array-index paths from a JSON string in one parse. This
 * is an internal expression used to share sibling [[GetJsonObject]] expressions; unsupported and
 * prefix-conflicting JSON paths remain as independent GetJsonObject expressions.
 */
case class MultiGetJsonObject(
    json: Expression,
    fallbackPaths: Seq[String])
  extends UnaryExpression
  with ExpectsInputTypes {

  // OptimizeCsvJsonExprs caps shared path depth to keep evaluator recursion stack-safe.
  require(fallbackPaths.nonEmpty)

  override def child: Expression = json

  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeWithCollation(supportsTrimCollation = true))

  override lazy val dataType: DataType = StructType(fallbackPaths.indices.map { index =>
    StructField(s"_$index", StringType, nullable = true)
  })

  override def nullable: Boolean = true

  // This internal unary expression always returns null when its JSON child is null.
  override def nullIntolerant: Boolean = true

  override def prettyName: String = "multi_get_json_object"

  final override val nodePatterns: Seq[TreePattern] = Seq(GET_JSON_OBJECT)

  @transient
  private lazy val simplePaths = fallbackPaths.map { path =>
    GetJsonObject.simplePath(UTF8String.fromString(path)).getOrElse {
      throw new IllegalArgumentException(s"Unsupported shared JSON path: $path")
    }
  }

  override def stateful: Boolean = true

  @transient
  private lazy val evaluator = MultiGetJsonObjectEvaluator(
    fallbackPaths.map(UTF8String.fromString),
    simplePaths)

  override def eval(input: InternalRow): Any = {
    evaluator.evaluate(json.eval(input).asInstanceOf[UTF8String])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val refEvaluator = ctx.addReferenceObj("evaluator", evaluator)
    val jsonEval = json.genCode(ctx)
    val resultType = CodeGenerator.javaType(dataType)
    ev.copy(code = code"""
       |${jsonEval.code}
       |boolean ${ev.isNull} = ${jsonEval.isNull};
       |$resultType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
       |if (!${ev.isNull}) {
       |  ${ev.value} = ($resultType) $refEvaluator.evaluate(${jsonEval.value});
       |  ${ev.isNull} = ${ev.value} == null;
       |}
       |""".stripMargin)
  }

  override protected def withNewChildInternal(newChild: Expression): MultiGetJsonObject =
    copy(json = newChild)
}

// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = "_FUNC_(jsonStr, p1, p2, ..., pn) - Returns a tuple like the function get_json_object, but it takes multiple names. All the input parameters and output column types are string.",
  arguments = """
    Arguments:
      * jsonStr - A JSON string to extract fields from.
      * pN - The field names to extract. Each name yields one output column with
          the corresponding field value.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('{"a":1, "b":2}', 'a', 'b');
       1	2
  """,
  group = "json_funcs",
  since = "1.6.0")
// scalastyle:on line.size.limit line.contains.tab
case class JsonTuple(children: Seq[Expression])
  extends Generator
  with QueryErrorsBase {

  override def nullable: Boolean = {
    // A row is always returned.
    false
  }

  // The json body is the first child.
  @transient private lazy val jsonExpr: Expression = children.head

  // The fields to query are the remaining children.
  @transient private lazy val fieldExpressions: Seq[Expression] = children.tail

  // Eagerly evaluate any foldable the field names.
  @transient private lazy val foldableFieldNames: Array[Option[String]] = {
    fieldExpressions.map {
      case expr if expr.foldable => Option(expr.eval()).map(_.asInstanceOf[UTF8String].toString)
      case _ => null
    }.toArray
  }

  override def elementSchema: StructType = StructType(fieldExpressions.zipWithIndex.map {
    case (_, idx) => StructField(s"c$idx", children.head.dataType, nullable = true)
  })

  override def prettyName: String = "json_tuple"

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length < 2) {
      throw QueryCompilationErrors.wrongNumArgsError(
        toSQLId(prettyName), Seq("> 1"), children.length
      )
    } else if (
      children.forall(
        child => StringTypeWithCollation(supportsTrimCollation = true)
          .acceptsType(child.dataType))) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      DataTypeMismatch(
        errorSubClass = "NON_STRING_TYPE",
        messageParameters = Map("funcName" -> toSQLId(prettyName)))
    }
  }

  @transient
  private lazy val evaluator: JsonTupleEvaluator = JsonTupleEvaluator(foldableFieldNames)
  override def stateful: Boolean = true

  override def eval(input: InternalRow): IterableOnce[InternalRow] = {
    val json = jsonExpr.eval(input).asInstanceOf[UTF8String]
    val filedNames = fieldExpressions.map(_.eval(input).asInstanceOf[UTF8String]).toArray
    evaluator.evaluate(json, filedNames)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val refEvaluator = ctx.addReferenceObj("evaluator", evaluator)
    val jsonEval = jsonExpr.genCode(ctx)
    val filedNamesTerm = ctx.freshName("fieldNames")
    val fieldNamesEval = fieldExpressions.map(_.genCode(ctx))
    val wrapperClass = classOf[IterableOnce[_]].getName
    val setFieldNames = fieldNamesEval.zipWithIndex.map {
      case (fieldNameEval, idx) =>
        s"""
           |if (${fieldNameEval.isNull}) {
           |  $filedNamesTerm[$idx] = null;
           |} else {
           |  $filedNamesTerm[$idx] = ${fieldNameEval.value};
           |}
           |""".stripMargin
    }
    ev.copy(code =
      code"""
         |UTF8String[] $filedNamesTerm = new UTF8String[${fieldExpressions.length}];
         |${jsonEval.code}
         |${fieldNamesEval.map(_.code).mkString("\n")}
         |${setFieldNames.mkString("\n")}
         |boolean ${ev.isNull} = false;
         |$wrapperClass<InternalRow> ${ev.value} =
         |  $refEvaluator.evaluate(${jsonEval.value}, $filedNamesTerm);
         |""".stripMargin)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): JsonTuple =
    copy(children = newChildren)
}

/**
 * The kind of a single `JSON_TABLE` column.
 */
sealed trait JsonTableColumnKind
object JsonTableColumnKind {
  /** A `FOR ORDINALITY` column: a 1-based sequential row counter. */
  case object Ordinality extends JsonTableColumnKind
  /** A regular value column: extracts the value at `path` and casts it to `dataType`. */
  case object Value extends JsonTableColumnKind
  /** An `EXISTS` column: true when `path` matches, cast to `dataType`. */
  case object Exists extends JsonTableColumnKind
}

/**
 * A single column definition of a `JSON_TABLE` invocation.
 *
 * @param name     the output column name
 * @param dataType the declared Spark type of the column (LongType for ORDINALITY columns)
 * @param path     the SQL/JSON path relative to a row item; None for ORDINALITY columns
 * @param kind     the column kind (ordinality / value / exists)
 */
case class JsonTableColumn(
    name: String,
    dataType: DataType,
    path: Option[String],
    kind: JsonTableColumnKind)

/**
 * Behavior when the JSON input is malformed.
 */
sealed trait JsonTableErrorMode
object JsonTableErrorMode {
  /** Produce no rows on malformed input (the SQL-standard default). */
  case object NullOnError extends JsonTableErrorMode
  /** Raise an error on malformed input. */
  case object ErrorOnError extends JsonTableErrorMode
}

// scalastyle:off line.size.limit
/**
 * The SQL:2016 `JSON_TABLE` table-valued function. Shreds a JSON document into a relational table:
 * the `rowPath` selects a sequence of row items and each [[JsonTableColumn]] projects a value out
 * of each item. Implemented as a [[Generator]] so it plugs into the existing, well-tested
 * [[org.apache.spark.sql.catalyst.plans.logical.Generate]] operator; no new execution operator is
 * introduced.
 *
 * Only the flat (non-`NESTED PATH`) subset of the standard is supported. Row-source and value
 * extraction use the token-aware [[JsonTableEvaluator]], which (unlike `get_json_object`)
 * distinguishes a missing path from a JSON `null` value; type coercion reuses [[Cast]].
 *
 * {{{
 *   SELECT t.* FROM json_table(
 *     '{"items":[{"id":1,"n":"a"},{"id":2,"n":"b"}]}',
 *     '$.items[*]'
 *     COLUMNS (seq FOR ORDINALITY, id INT PATH '$.id', name STRING PATH '$.n')
 *   ) AS t;
 * }}}
 */
// scalastyle:on line.size.limit
case class JsonTable(
    child: Expression,
    rowPath: String,
    columns: Seq[JsonTableColumn],
    errorMode: JsonTableErrorMode,
    timeZoneId: Option[String] = None,
    // Captured at plan-construction time so column casts do not change behavior if the session's
    // ANSI mode is flipped between building the plan and executing it (matching `Cast`, which
    // fixes its eval mode when the expression is constructed).
    ansiEnabled: Boolean = SQLConf.get.ansiEnabled)
  extends UnaryExpression
  with Generator
  with TimeZoneAwareExpression
  with CodegenFallback
  with ImplicitCastInputTypes
  with QueryErrorsBase {

  // Declared via ImplicitCastInputTypes so the analyzer coerces the JSON input to STRING. In
  // particular an untyped SQL NULL (NullType) is cast to STRING rather than rejected, so
  // `JSON_TABLE(NULL, ...)` reaches the runtime and applies the NULL ON ERROR behavior.
  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeWithCollation(supportsTrimCollation = true))

  // ORDINALITY columns always hold a non-null counter; value/EXISTS columns may be null.
  override def elementSchema: StructType =
    StructType(columns.map { c =>
      val nullable = c.kind != JsonTableColumnKind.Ordinality
      StructField(c.name, c.dataType, nullable = nullable)
    })

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def checkInputDataTypes(): TypeCheckResult = {
    // First the standard input-type check (STRING for the JSON input, with NULL coerced).
    val inputCheck = super.checkInputDataTypes()
    if (inputCheck.isFailure) {
      inputCheck
    } else {
      // Validate the row path and every column path. A path is valid here iff it parses and is
      // free of wildcards -- except the row path may end in a single `[*]`, which is stripped into
      // `containerInstructions`, so the row path is checked on that already-stripped list.
      val rowPathValid = JsonPathParser.parse(rowPath).isDefined &&
        !containerInstructions.contains(PathInstruction.Wildcard)
      val invalid: Option[(String, String)] = if (!rowPathValid) {
        Some(("row path", rowPath))
      } else {
        columns.iterator.collect { case c if c.path.isDefined => (c.name, c.path.get) }
          .collectFirst {
            // Valid column path: parses and is wildcard-free, i.e. hasWildcard == Some(false).
            case (name, path) if !JsonPathParser.hasWildcard(path).contains(false) =>
              (s"column '$name'", path)
          }
      }
      invalid match {
        case Some((location, path)) =>
          DataTypeMismatch(
            errorSubClass = "INVALID_JSON_TABLE_PATH",
            messageParameters = Map("location" -> location, "path" -> toSQLValue(path)))
        case None =>
          // Every value/EXISTS column is produced by casting from a source type (StringType for
          // value columns, BooleanType for EXISTS columns) to the declared column type. Reject a
          // non-castable declared type (e.g. a value column declared STRUCT/ARRAY/MAP) here rather
          // than failing at runtime. Ordinality columns are always LongType and need no check.
          // The castability rules differ between ANSI and non-ANSI mode (e.g. BOOLEAN -> TIMESTAMP
          // is allowed by non-ANSI casts but not ANSI casts), so this must use the same eval mode
          // as the actual per-column `Cast` built in `columnCasts`.
          def sourceType(c: JsonTableColumn): Option[DataType] = c.kind match {
            case JsonTableColumnKind.Value => Some(StringType)
            case JsonTableColumnKind.Exists => Some(BooleanType)
            case JsonTableColumnKind.Ordinality => None
          }
          def castable(src: DataType, target: DataType): Boolean =
            if (ansiEnabled) Cast.canAnsiCast(src, target) else Cast.canCast(src, target)
          columns.iterator.flatMap(c => sourceType(c).map((c, _)))
            .collectFirst { case (c, src) if !castable(src, c.dataType) => (c, src) } match {
            case Some((c, srcType)) =>
              DataTypeMismatch(
                errorSubClass = "CAST_WITHOUT_SUGGESTION",
                messageParameters = Map(
                  "srcType" -> toSQLType(srcType),
                  "targetType" -> toSQLType(c.dataType)))
            case None =>
              TypeCheckResult.TypeCheckSuccess
          }
      }
    }
  }

  // The row path is `containerRowPath` plus an optional trailing `[*]`. Splitting on the parsed
  // instruction list (rather than the raw string) is whitespace-insensitive and unambiguous.
  // `checkInputDataTypes` guarantees the path parses and is wildcard-free at this point.
  @transient private lazy val (containerInstructions, explodeRoot)
      : (Seq[PathInstruction], Boolean) = {
    val parsed = JsonPathParser.parse(rowPath).getOrElse(Nil)
    parsed match {
      case init :+ PathInstruction.Subscript :+ PathInstruction.Wildcard =>
        (init, true)
      case other =>
        (other, false)
    }
  }

  @transient private lazy val rowEvaluator: JsonTableEvaluator =
    JsonTableEvaluator(containerInstructions, explodeRoot)

  // Parsed instruction list per column (empty for ordinality columns, which have no path).
  @transient private lazy val columnPaths: Array[Seq[PathInstruction]] =
    columns.map(c => c.path.flatMap(JsonPathParser.parse).getOrElse(Nil)).toArray

  // Column kinds snapshotted into an array, like `columnPaths` and `columnCasts`: `columns` is a
  // `List` (the parser builds it with `.map(...).toSeq`), so `columns(i)` is O(i) and indexing it
  // in the per-row projection loop would make `projectRow` O(n^2) in the column count.
  @transient private lazy val columnKinds: Array[JsonTableColumnKind] = columns.map(_.kind).toArray

  // Prefix trie over the column paths, built once so every row's value/EXISTS columns are resolved
  // in a single traversal of the item rather than one re-parse per column. Ordinality columns have
  // no path and are excluded (their empty path must not be confused with a root path `$`, which is
  // an included column reading the whole item).
  @transient private lazy val columnTrie: JsonTablePathTrie = {
    val include = columnKinds.map(_ != JsonTableColumnKind.Ordinality)
    rowEvaluator.buildPathTrie(columnPaths, include)
  }

  // One reusable Cast per non-ordinality column, evaluated against a single-slot mutable input
  // row. Building the Cast once (over a BoundReference) avoids allocating an expression tree per
  // row/column on the hot path. The source type is BooleanType for EXISTS, StringType otherwise.
  @transient private lazy val columnCasts: Array[Expression] = {
    val evalMode = EvalMode.fromBoolean(ansiEnabled)
    columns.map { c =>
      c.kind match {
        case JsonTableColumnKind.Ordinality => null
        case JsonTableColumnKind.Exists =>
          Cast(BoundReference(0, BooleanType, nullable = false), c.dataType, timeZoneId, evalMode)
        case JsonTableColumnKind.Value =>
          Cast(BoundReference(0, StringType, nullable = true), c.dataType, timeZoneId, evalMode)
      }
    }.toArray
  }

  // Reusable single-slot input row for the per-column casts above.
  @transient private lazy val castInput: GenericInternalRow = new GenericInternalRow(1)

  private def castColumn(i: Int, value: Any): Any = {
    castInput.update(0, value)
    columnCasts(i).eval(castInput)
  }

  private def projectRow(item: UTF8String, ordinal: Long): InternalRow = {
    val numColumns = columnKinds.length
    // Resolve every value/EXISTS column in a single traversal of the item; ordinality slots are
    // not in the trie and come back as Missing (filled below).
    val resolved = rowEvaluator.navigateColumns(item, columnTrie, numColumns)
    val values = new Array[Any](numColumns)
    var i = 0
    while (i < numColumns) {
      values(i) = columnKinds(i) match {
        case JsonTableColumnKind.Ordinality =>
          ordinal
        case JsonTableColumnKind.Exists =>
          // Present (including an explicit JSON null) counts as existing; only Missing is false.
          val exists = resolved(i) != JsonPathResult.Missing
          castColumn(i, exists)
        case JsonTableColumnKind.Value =>
          resolved(i) match {
            // `raw` is a re-parseable JSON fragment; unquote a scalar string so the column gets
            // its content (e.g. `"hi"` -> `hi`), then cast to the declared type.
            case JsonPathResult.Found(raw) => castColumn(i, rowEvaluator.unquotedString(raw))
            // A missing path and an explicit JSON null both yield SQL NULL for a value column.
            case _ => null
          }
      }
      i += 1
    }
    new GenericInternalRow(values)
  }

  override def eval(input: InternalRow): IterableOnce[InternalRow] = {
    val json = child.eval(input).asInstanceOf[UTF8String]
    rowEvaluator.evaluate(json) match {
      case Some(items) =>
        // A manual Long counter for FOR ORDINALITY: `zipWithIndex` is Int-based and would wrap
        // past Int.MaxValue for a very large array, whereas ordinality is a BIGINT.
        var ordinal = 0L
        items.map { item =>
          ordinal += 1L
          projectRow(item, ordinal)
        }
      case None =>
        // `errorMode` governs the row-source JSON only (null / malformed input, or `[*]` over a
        // non-array). Per-column value extraction follows normal `Cast` semantics: a bad cast is
        // NULL in non-ANSI mode and raises in ANSI mode, independent of ON ERROR.
        errorMode match {
          case JsonTableErrorMode.NullOnError => Iterator.empty
          case JsonTableErrorMode.ErrorOnError =>
            throw QueryExecutionErrors.malformedRecordsDetectedInRecordParsingError(
              if (json == null) "null" else json.toString,
              SparkException.internalError("JSON_TABLE encountered malformed JSON input."))
        }
    }
  }

  override def prettyName: String = "json_table"

  // The default `Expression.sql` renders only children, i.e. `json_table(<json_expr>)`, dropping
  // the row path, columns, and ON ERROR mode. Render the full `JSON_TABLE(...)` syntax so
  // analysis/type-check diagnostics (e.g. INVALID_JSON_TABLE_PATH) point at the whole invocation.
  override def sql: String = {
    val columnsSQL = columns.map { c =>
      val pathSQL = c.path.map(p => s" PATH '$p'").getOrElse("")
      c.kind match {
        case JsonTableColumnKind.Ordinality => s"${c.name} FOR ORDINALITY"
        case JsonTableColumnKind.Exists => s"${c.name} ${c.dataType.sql} EXISTS$pathSQL"
        case JsonTableColumnKind.Value => s"${c.name} ${c.dataType.sql}$pathSQL"
      }
    }.mkString(", ")
    val errorSQL = errorMode match {
      case JsonTableErrorMode.NullOnError => "NULL ON ERROR"
      case JsonTableErrorMode.ErrorOnError => "ERROR ON ERROR"
    }
    s"JSON_TABLE(${child.sql}, '$rowPath' COLUMNS ($columnsSQL) $errorSQL)"
  }

  override protected def withNewChildInternal(newChild: Expression): JsonTable =
    copy(child = newChild)
}

/**
 * Converts an json input string to a [[StructType]], [[ArrayType]] or [[MapType]]
 * with the specified schema.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(jsonStr, schema[, options]) - Returns a struct value with the given `jsonStr` and `schema`.",
  arguments = """
    Arguments:
      * jsonStr - A JSON string to parse.
      * schema - The schema to use when parsing the JSON string, given as a DDL
          formatted string or a schema expression.
      * options - Optional. A map of string key-value pairs that control how the
          JSON is parsed. By default no options are set.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('{"a":1, "b":0.8}', 'a INT, b DOUBLE');
       {"a":1,"b":0.8}
      > SELECT _FUNC_('{"time":"26/08/2015"}', 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy'));
       {"time":2015-08-26 00:00:00}
      > SELECT _FUNC_('{"teacher": "Alice", "student": [{"name": "Bob", "rank": 1}, {"name": "Charlie", "rank": 2}]}', 'STRUCT<teacher: STRING, student: ARRAY<STRUCT<name: STRING, rank: INT>>>');
       {"teacher":"Alice","student":[{"name":"Bob","rank":1},{"name":"Charlie","rank":2}]}
  """,
  group = "json_funcs",
  since = "2.2.0")
// scalastyle:on line.size.limit
case class JsonToStructs(
    schema: DataType,
    options: Map[String, String],
    child: Expression,
    timeZoneId: Option[String] = None,
    variantAllowDuplicateKeys: Boolean = SQLConf.get.getConf(SQLConf.VARIANT_ALLOW_DUPLICATE_KEYS))
  extends UnaryExpression
  with TimeZoneAwareExpression
  with CodegenFallback
  with ExpectsInputTypes
  with QueryErrorsBase {

  // The JSON input data might be missing certain fields. We force the nullability
  // of the user-provided schema to avoid data corruptions. In particular, the parquet-mr encoder
  // can generate incorrect files if values are missing in columns declared as non-nullable.
  private val nullableSchema: DataType = schema.asNullable

  override def nullable: Boolean = true

  final override def nodePatternsInternal(): Seq[TreePattern] = Seq(JSON_TO_STRUCT)

  override def nullIntolerant: Boolean = true

  // Used in `FunctionRegistry`
  def this(child: Expression, schema: Expression, options: Map[String, String]) =
    this(
      schema = ExprUtils.evalTypeExpr(schema),
      options = options,
      child = child,
      timeZoneId = None)

  def this(child: Expression, schema: Expression) = this(child, schema, Map.empty[String, String])

  def this(child: Expression, schema: Expression, options: Expression) =
    this(
      schema = ExprUtils.evalTypeExpr(schema),
      options = ExprUtils.convertToMapData(options),
      child = child,
      timeZoneId = None)

  override def checkInputDataTypes(): TypeCheckResult = {
    // `from_json` parses each input string as one JSON document, so the embedded array
    // splitting can never apply.
    if (CaseInsensitiveMap(options).contains(JSONOptions.EXPLODE_EMBEDDED_ARRAY)) {
      throw QueryCompilationErrors.explodeEmbeddedArrayUnsupportedUsage(
        "the from_json function")
    }
    nullableSchema match {
      case _: StructType | _: ArrayType | _: MapType | _: VariantType =>
        val checkResult = ExprUtils.checkJsonSchema(nullableSchema)
        if (checkResult.isFailure) checkResult else super.checkInputDataTypes()
      case _ =>
        DataTypeMismatch(
          errorSubClass = "INVALID_JSON_SCHEMA",
          messageParameters = Map("schema" -> toSQLType(nullableSchema)))
    }
  }

  override def dataType: DataType = nullableSchema

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  @transient
  private lazy val nameOfCorruptRecord = SQLConf.get.getConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD)

  @transient
  private lazy val evaluator = new JsonToStructsEvaluator(
    options, nullableSchema, nameOfCorruptRecord, timeZoneId, variantAllowDuplicateKeys)
  override def stateful: Boolean = true

  override def nullSafeEval(json: Any): Any = evaluator.evaluate(json.asInstanceOf[UTF8String])

  override def inputTypes: Seq[AbstractDataType] =
    StringTypeWithCollation(supportsTrimCollation = true) :: Nil

  override def sql: String = schema match {
    case _: MapType => "entries"
    case _ => super.sql
  }

  override def prettyName: String = "from_json"

  override protected def withNewChildInternal(newChild: Expression): JsonToStructs =
    copy(child = newChild)
}

object JsonToStructs {
  def unapply(
      j: JsonToStructs): Option[(DataType, Map[String, String], Expression, Option[String])] =
    Some((j.schema, j.options, j.child, j.timeZoneId))
}

/**
 * Converts a [[StructType]], [[ArrayType]] or [[MapType]] to a JSON output string.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr[, options]) - Returns a JSON string with a given struct value",
  arguments = """
    Arguments:
      * expr - The struct value to convert to a JSON string.
        An expression that evaluates to a struct, array, map, or variant.
      * options - Options controlling how the JSON string is produced.
        An expression that evaluates to a map. Must be a constant.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(named_struct('a', 1, 'b', 2));
       {"a":1,"b":2}
      > SELECT _FUNC_(named_struct('time', to_timestamp('2015-08-26', 'yyyy-MM-dd')), map('timestampFormat', 'dd/MM/yyyy'));
       {"time":"26/08/2015"}
      > SELECT _FUNC_(array(named_struct('a', 1, 'b', 2)));
       [{"a":1,"b":2}]
      > SELECT _FUNC_(map('a', named_struct('b', 1)));
       {"a":{"b":1}}
      > SELECT _FUNC_(map(named_struct('a', 1),named_struct('b', 2)));
       {"[1]":{"b":2}}
      > SELECT _FUNC_(map('a', 1));
       {"a":1}
      > SELECT _FUNC_(array(map('a', 1)));
       [{"a":1}]
      > SELECT _FUNC_(named_struct('b', 1, 'a', 2), map('sortKeys', 'true'));
       {"a":2,"b":1}
  """,
  group = "json_funcs",
  since = "2.2.0")
// scalastyle:on line.size.limit
case class StructsToJson(
    options: Map[String, String],
    child: Expression,
    timeZoneId: Option[String] = None)
  extends UnaryExpression
  with RuntimeReplaceable
  with ExpectsInputTypes
  with TimeZoneAwareExpression
  with DefaultStringProducingExpression
  with QueryErrorsBase {

  override def nullable: Boolean = true

  override def nodePatternsInternal(): Seq[TreePattern] = Seq(RUNTIME_REPLACEABLE)

  def this(options: Map[String, String], child: Expression) = this(options, child, None)

  // Used in `FunctionRegistry`
  def this(child: Expression) = this(Map.empty, child, None)
  def this(child: Expression, options: Expression) =
    this(
      options = ExprUtils.convertToMapData(options),
      child = child,
      timeZoneId = None)

  @transient
  private lazy val inputSchema = child.dataType

  override def checkInputDataTypes(): TypeCheckResult = inputSchema match {
    case dt @ (_: StructType | _: MapType | _: ArrayType | _: VariantType) =>
      JacksonUtils.verifyType(prettyName, dt)
    case _ =>
      DataTypeMismatch(
        errorSubClass = "INVALID_JSON_SCHEMA",
        messageParameters = Map("schema" -> toSQLType(child.dataType)))
  }

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def inputTypes: Seq[AbstractDataType] = TypeCollection(ArrayType, StructType) :: Nil

  override def prettyName: String = "to_json"

  override protected def withNewChildInternal(newChild: Expression): StructsToJson =
    copy(child = newChild)

  @transient
  private lazy val evaluator = StructsToJsonEvaluator(options, inputSchema, timeZoneId)

  override def replacement: Expression = Invoke(
    Literal.create(evaluator, ObjectType(classOf[StructsToJsonEvaluator])),
    "evaluate",
    dataType,
    Seq(child),
    Seq(child.dataType)
  )
}

/**
 * A function infers schema of JSON string.
 */
@ExpressionDescription(
  usage = "_FUNC_(json[, options]) - Returns schema in the DDL format of JSON string.",
  arguments = """
    Arguments:
      * json - A JSON string whose schema is inferred.
      * options - Optional. A map of string key-value pairs that control how the
          JSON is parsed. By default no options are set.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('[{"col":0}]');
       ARRAY<STRUCT<col: BIGINT>>
      > SELECT _FUNC_('[{"col":01}]', map('allowNumericLeadingZeros', 'true'));
       ARRAY<STRUCT<col: BIGINT>>
  """,
  group = "json_funcs",
  since = "2.4.0")
case class SchemaOfJson(
    child: Expression,
    options: Map[String, String])
  extends UnaryExpression
  with RuntimeReplaceable
  with DefaultStringProducingExpression
  with QueryErrorsBase {

  def this(child: Expression) = this(child, Map.empty[String, String])

  def this(child: Expression, options: Expression) = this(
      child = child,
      options = ExprUtils.convertToMapData(options))

  override def nullable: Boolean = false

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!child.foldable) {
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> toSQLId("json"),
          "inputType" -> toSQLType(child.dataType),
          "inputExpr" -> toSQLExpr(child)))
    } else if (child.eval() == null) {
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_NULL",
        messageParameters = Map("exprName" -> "json"))
    } else if (child.dataType != StringType) {
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "inputSql" -> toSQLExpr(child),
          "inputType" -> toSQLType(child.dataType),
          "requiredType" -> toSQLType(StringType)))
    } else {
      super.checkInputDataTypes()
    }
  }

  @transient
  private lazy val evaluator: SchemaOfJsonEvaluator = SchemaOfJsonEvaluator(options)

  override def replacement: Expression = Invoke(
    Literal.create(evaluator, ObjectType(classOf[SchemaOfJsonEvaluator])),
    "evaluate",
    dataType,
    Seq(child),
    Seq(child.dataType),
    returnNullable = false)

  override def prettyName: String = "schema_of_json"

  override protected def withNewChildInternal(newChild: Expression): SchemaOfJson =
    copy(child = newChild)
}

/**
 * A function that returns the number of elements in the outermost JSON array.
 */
@ExpressionDescription(
  usage = "_FUNC_(jsonArray) - Returns the number of elements in the outermost JSON array.",
  arguments = """
    Arguments:
      * jsonArray - A JSON array. `NULL` is returned in case of any other valid JSON string,
          `NULL` or an invalid JSON.
        An expression that evaluates to a string.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('[1,2,3,4]');
        4
      > SELECT _FUNC_('[1,2,3,{"f1":1,"f2":[5,6]},4]');
        5
      > SELECT _FUNC_('[1,2');
        NULL
  """,
  group = "json_funcs",
  since = "3.1.0"
)
case class LengthOfJsonArray(child: Expression)
  extends UnaryExpression
  with ExpectsInputTypes
  with RuntimeReplaceable {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeWithCollation(supportsTrimCollation = true))
  override def dataType: DataType = IntegerType
  override def nullable: Boolean = true
  override def prettyName: String = "json_array_length"

  override protected def withNewChildInternal(newChild: Expression): LengthOfJsonArray =
    copy(child = newChild)

  override def replacement: Expression = StaticInvoke(
    classOf[JsonExpressionUtils],
    dataType,
    "lengthOfJsonArray",
    Seq(child),
    inputTypes
  )
}

/**
 * A function which returns all the keys of the outermost JSON object.
 */
@ExpressionDescription(
  usage = "_FUNC_(json_object) - Returns all the keys of the outermost JSON object as an array.",
  arguments = """
    Arguments:
      * json_object - A JSON object. If a valid JSON object is given, all the keys of the outermost
          object will be returned as an array. If it is any other valid JSON string, an invalid JSON
          string or an empty string, the function returns null.
        An expression that evaluates to a string.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('{}');
        []
      > SELECT _FUNC_('{"key": "value"}');
        ["key"]
      > SELECT _FUNC_('{"f1":"abc","f2":{"f3":"a", "f4":"b"}}');
        ["f1","f2"]
  """,
  group = "json_funcs",
  since = "3.1.0"
)
case class JsonObjectKeys(child: Expression)
  extends UnaryExpression
  with ExpectsInputTypes
  with RuntimeReplaceable
  with DefaultStringProducingExpression {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeWithCollation(supportsTrimCollation = true))
  override def dataType: DataType = ArrayType(super.dataType)
  override def nullable: Boolean = true
  override def prettyName: String = "json_object_keys"

  override def replacement: Expression = StaticInvoke(
    classOf[JsonExpressionUtils],
    dataType,
    "jsonObjectKeys",
    Seq(child),
    inputTypes
  )

  override protected def withNewChildInternal(newChild: Expression): JsonObjectKeys =
    copy(child = newChild)
}

/**
 * A function which returns the type of the outermost JSON value as a string.
 */
@ExpressionDescription(
  usage = "_FUNC_(json) - Returns the type of the outermost JSON value, or null if invalid.",
  arguments = """
    Arguments:
      * json - A JSON string. Returns the type of the outermost value ('object', 'array',
          'string', 'number', 'boolean', 'null'), or null for an invalid or empty string.
        An expression that evaluates to a string.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('{"a": 1}');
        object
      > SELECT _FUNC_('[1, 2, 3]');
        array
      > SELECT _FUNC_('123');
        number
  """,
  group = "json_funcs",
  since = "4.4.0"
)
case class JsonTypeof(child: Expression)
  extends UnaryExpression
  with ExpectsInputTypes
  with RuntimeReplaceable
  with DefaultStringProducingExpression {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeWithCollation(supportsTrimCollation = true))
  override def nullable: Boolean = true
  override def prettyName: String = "json_typeof"

  override def replacement: Expression = StaticInvoke(
    classOf[JsonExpressionUtils],
    dataType,
    "jsonTypeof",
    Seq(child),
    inputTypes
  )

  override protected def withNewChildInternal(newChild: Expression): JsonTypeof =
    copy(child = newChild)
}
