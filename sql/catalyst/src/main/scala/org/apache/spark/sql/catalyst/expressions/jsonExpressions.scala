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

import java.io._

import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.combinator.RegexParsers

import com.fasterxml.jackson.core._
import com.fasterxml.jackson.core.json.JsonReadFeature

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.json._
import org.apache.spark.sql.catalyst.trees.TreePattern.{JSON_TO_STRUCT, TreePattern}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

private[this] sealed trait PathInstruction
private[this] object PathInstruction {
  private[expressions] case object Subscript extends PathInstruction
  private[expressions] case object Wildcard extends PathInstruction
  private[expressions] case object Key extends PathInstruction
  private[expressions] case class Index(index: Long) extends PathInstruction
  private[expressions] case class Named(name: String) extends PathInstruction
}

private[this] sealed trait WriteStyle
private[this] object WriteStyle {
  private[expressions] case object RawStyle extends WriteStyle
  private[expressions] case object QuotedStyle extends WriteStyle
  private[expressions] case object FlattenStyle extends WriteStyle
}

private[this] object JsonPathParser extends RegexParsers {
  import PathInstruction._

  def root: Parser[Char] = '$'

  def long: Parser[Long] = "\\d+".r ^? {
    case x => x.toLong
  }

  // parse `[*]` and `[123]` subscripts
  def subscript: Parser[List[PathInstruction]] =
    for {
      operand <- '[' ~> ('*' ^^^ Wildcard | long ^^ Index) <~ ']'
    } yield {
      Subscript :: operand :: Nil
    }

  // parse `.name` or `['name']` child expressions
  def named: Parser[List[PathInstruction]] =
    for {
      name <- '.' ~> "[^\\.\\[]+".r | "['" ~> "[^\\'\\?]+".r <~ "']"
    } yield {
      Key :: Named(name) :: Nil
    }

  // child wildcards: `..`, `.*` or `['*']`
  def wildcard: Parser[List[PathInstruction]] =
    (".*" | "['*']") ^^^ List(Wildcard)

  def node: Parser[List[PathInstruction]] =
    wildcard |
      named |
      subscript

  val expression: Parser[List[PathInstruction]] = {
    phrase(root ~> rep(node) ^^ (x => x.flatten))
  }

  def parse(str: String): Option[List[PathInstruction]] = {
    this.parseAll(expression, str) match {
      case Success(result, _) =>
        Some(result)

      case _ =>
        None
    }
  }
}

private[this] object SharedFactory {
  val jsonFactory = new JsonFactoryBuilder()
    // The two options below enabled for Hive compatibility
    .enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS)
    .enable(JsonReadFeature.ALLOW_SINGLE_QUOTES)
    .build()
}

/**
 * Extracts json object from a json string based on json path specified, and returns json string
 * of the extracted json object. It will return null if the input json string is invalid.
 */
@ExpressionDescription(
  usage = "_FUNC_(json_txt, path) - Extracts a json object from `path`.",
  examples = """
    Examples:
      > SELECT _FUNC_('{"a":"b"}', '$.a');
       b
  """,
  group = "json_funcs",
  since = "1.5.0")
case class GetJsonObject(json: Expression, path: Expression)
  extends BinaryExpression with ExpectsInputTypes with CodegenFallback {

  import com.fasterxml.jackson.core.JsonToken._

  import PathInstruction._
  import SharedFactory._
  import WriteStyle._

  override def left: Expression = json
  override def right: Expression = path
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)
  override def dataType: DataType = StringType
  override def nullable: Boolean = true
  override def prettyName: String = "get_json_object"

  @transient private lazy val parsedPath = parsePath(path.eval().asInstanceOf[UTF8String])

  override def eval(input: InternalRow): Any = {
    val jsonStr = json.eval(input).asInstanceOf[UTF8String]
    if (jsonStr == null) {
      return null
    }

    val parsed = if (path.foldable) {
      parsedPath
    } else {
      parsePath(path.eval(input).asInstanceOf[UTF8String])
    }

    if (parsed.isDefined) {
      try {
        /* We know the bytes are UTF-8 encoded. Pass a Reader to avoid having Jackson
          detect character encoding which could fail for some malformed strings */
        Utils.tryWithResource(CreateJacksonParser.utf8String(jsonFactory, jsonStr)) { parser =>
          val output = new ByteArrayOutputStream()
          val matched = Utils.tryWithResource(
            jsonFactory.createGenerator(output, JsonEncoding.UTF8)) { generator =>
            parser.nextToken()
            evaluatePath(parser, generator, RawStyle, parsed.get)
          }
          if (matched) {
            UTF8String.fromBytes(output.toByteArray)
          } else {
            null
          }
        }
      } catch {
        case _: JsonProcessingException => null
      }
    } else {
      null
    }
  }

  private def parsePath(path: UTF8String): Option[List[PathInstruction]] = {
    if (path != null) {
      JsonPathParser.parse(path.toString)
    } else {
      None
    }
  }

  // advance to the desired array index, assumes to start at the START_ARRAY token
  private def arrayIndex(p: JsonParser, f: () => Boolean): Long => Boolean = {
    case _ if p.getCurrentToken == END_ARRAY =>
      // terminate, nothing has been written
      false

    case 0 =>
      // we've reached the desired index
      val dirty = f()

      while (p.nextToken() != END_ARRAY) {
        // advance the token stream to the end of the array
        p.skipChildren()
      }

      dirty

    case i if i > 0 =>
      // skip this token and evaluate the next
      p.skipChildren()
      p.nextToken()
      arrayIndex(p, f)(i - 1)
  }

  /**
   * Evaluate a list of JsonPath instructions, returning a bool that indicates if any leaf nodes
   * have been written to the generator
   */
  private def evaluatePath(
      p: JsonParser,
      g: JsonGenerator,
      style: WriteStyle,
      path: List[PathInstruction]): Boolean = {
    (p.getCurrentToken, path) match {
      case (VALUE_STRING, Nil) if style == RawStyle =>
        // there is no array wildcard or slice parent, emit this string without quotes
        if (p.hasTextCharacters) {
          g.writeRaw(p.getTextCharacters, p.getTextOffset, p.getTextLength)
        } else {
          g.writeRaw(p.getText)
        }
        true

      case (START_ARRAY, Nil) if style == FlattenStyle =>
        // flatten this array into the parent
        var dirty = false
        while (p.nextToken() != END_ARRAY) {
          dirty |= evaluatePath(p, g, style, Nil)
        }
        dirty

      case (_, Nil) =>
        // general case: just copy the child tree verbatim
        g.copyCurrentStructure(p)
        true

      case (START_OBJECT, Key :: xs) =>
        var dirty = false
        while (p.nextToken() != END_OBJECT) {
          if (dirty) {
            // once a match has been found we can skip other fields
            p.skipChildren()
          } else {
            dirty = evaluatePath(p, g, style, xs)
          }
        }
        dirty

      case (START_ARRAY, Subscript :: Wildcard :: Subscript :: Wildcard :: xs) =>
        // special handling for the non-structure preserving double wildcard behavior in Hive
        var dirty = false
        g.writeStartArray()
        while (p.nextToken() != END_ARRAY) {
          dirty |= evaluatePath(p, g, FlattenStyle, xs)
        }
        g.writeEndArray()
        dirty

      case (START_ARRAY, Subscript :: Wildcard :: xs) if style != QuotedStyle =>
        // retain Flatten, otherwise use Quoted... cannot use Raw within an array
        val nextStyle = style match {
          case RawStyle => QuotedStyle
          case FlattenStyle => FlattenStyle
          case QuotedStyle => throw new IllegalStateException()
        }

        // temporarily buffer child matches, the emitted json will need to be
        // modified slightly if there is only a single element written
        val buffer = new StringWriter()

        var dirty = 0
        Utils.tryWithResource(jsonFactory.createGenerator(buffer)) { flattenGenerator =>
          flattenGenerator.writeStartArray()

          while (p.nextToken() != END_ARRAY) {
            // track the number of array elements and only emit an outer array if
            // we've written more than one element, this matches Hive's behavior
            dirty += (if (evaluatePath(p, flattenGenerator, nextStyle, xs)) 1 else 0)
          }
          flattenGenerator.writeEndArray()
        }

        val buf = buffer.getBuffer
        if (dirty > 1) {
          g.writeRawValue(buf.toString)
        } else if (dirty == 1) {
          // remove outer array tokens
          g.writeRawValue(buf.substring(1, buf.length()-1))
        } // else do not write anything

        dirty > 0

      case (START_ARRAY, Subscript :: Wildcard :: xs) =>
        var dirty = false
        g.writeStartArray()
        while (p.nextToken() != END_ARRAY) {
          // wildcards can have multiple matches, continually update the dirty count
          dirty |= evaluatePath(p, g, QuotedStyle, xs)
        }
        g.writeEndArray()

        dirty

      case (START_ARRAY, Subscript :: Index(idx) :: (xs@Subscript :: Wildcard :: _)) =>
        p.nextToken()
        // we're going to have 1 or more results, switch to QuotedStyle
        arrayIndex(p, () => evaluatePath(p, g, QuotedStyle, xs))(idx)

      case (START_ARRAY, Subscript :: Index(idx) :: xs) =>
        p.nextToken()
        arrayIndex(p, () => evaluatePath(p, g, style, xs))(idx)

      case (FIELD_NAME, Named(name) :: xs) if p.getCurrentName == name =>
        // exact field match
        if (p.nextToken() != JsonToken.VALUE_NULL) {
          evaluatePath(p, g, style, xs)
        } else {
          false
        }

      case (FIELD_NAME, Wildcard :: xs) =>
        // wildcard field match
        p.nextToken()
        evaluatePath(p, g, style, xs)

      case _ =>
        p.skipChildren()
        false
    }
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): GetJsonObject =
    copy(json = newLeft, path = newRight)
}

// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = "_FUNC_(jsonStr, p1, p2, ..., pn) - Returns a tuple like the function get_json_object, but it takes multiple names. All the input parameters and output column types are string.",
  examples = """
    Examples:
      > SELECT _FUNC_('{"a":1, "b":2}', 'a', 'b');
       1	2
  """,
  group = "json_funcs",
  since = "1.6.0")
// scalastyle:on line.size.limit line.contains.tab
case class JsonTuple(children: Seq[Expression])
  extends Generator with CodegenFallback {

  import SharedFactory._

  override def nullable: Boolean = {
    // a row is always returned
    false
  }

  // if processing fails this shared value will be returned
  @transient private lazy val nullRow: Seq[InternalRow] =
    new GenericInternalRow(Array.ofDim[Any](fieldExpressions.length)) :: Nil

  // the json body is the first child
  @transient private lazy val jsonExpr: Expression = children.head

  // the fields to query are the remaining children
  @transient private lazy val fieldExpressions: Seq[Expression] = children.tail

  // eagerly evaluate any foldable the field names
  @transient private lazy val foldableFieldNames: IndexedSeq[Option[String]] = {
    fieldExpressions.map {
      case expr if expr.foldable => Option(expr.eval()).map(_.asInstanceOf[UTF8String].toString)
      case _ => null
    }.toIndexedSeq
  }

  // and count the number of foldable fields, we'll use this later to optimize evaluation
  @transient private lazy val constantFields: Int = foldableFieldNames.count(_ != null)

  override def elementSchema: StructType = StructType(fieldExpressions.zipWithIndex.map {
    case (_, idx) => StructField(s"c$idx", StringType, nullable = true)
  })

  override def prettyName: String = "json_tuple"

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length < 2) {
      TypeCheckResult.TypeCheckFailure(s"$prettyName requires at least two arguments")
    } else if (children.forall(child => StringType.acceptsType(child.dataType))) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(s"$prettyName requires that all arguments are strings")
    }
  }

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    val json = jsonExpr.eval(input).asInstanceOf[UTF8String]
    if (json == null) {
      return nullRow
    }

    try {
      /* We know the bytes are UTF-8 encoded. Pass a Reader to avoid having Jackson
      detect character encoding which could fail for some malformed strings */
      Utils.tryWithResource(CreateJacksonParser.utf8String(jsonFactory, json)) { parser =>
        parseRow(parser, input)
      }
    } catch {
      case _: JsonProcessingException =>
        nullRow
    }
  }

  private def parseRow(parser: JsonParser, input: InternalRow): Seq[InternalRow] = {
    // only objects are supported
    if (parser.nextToken() != JsonToken.START_OBJECT) {
      return nullRow
    }

    // evaluate the field names as String rather than UTF8String to
    // optimize lookups from the json token, which is also a String
    val fieldNames = if (constantFields == fieldExpressions.length) {
      // typically the user will provide the field names as foldable expressions
      // so we can use the cached copy
      foldableFieldNames.map(_.orNull)
    } else if (constantFields == 0) {
      // none are foldable so all field names need to be evaluated from the input row
      fieldExpressions.map(_.eval(input).asInstanceOf[UTF8String].toString)
    } else {
      // if there is a mix of constant and non-constant expressions
      // prefer the cached copy when available
      foldableFieldNames.zip(fieldExpressions).map {
        case (null, expr) => expr.eval(input).asInstanceOf[UTF8String].toString
        case (fieldName, _) => fieldName.orNull
      }
    }

    val row = Array.ofDim[Any](fieldNames.length)

    // start reading through the token stream, looking for any requested field names
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      if (parser.getCurrentToken == JsonToken.FIELD_NAME) {
        // check to see if this field is desired in the output
        val jsonField = parser.getCurrentName
        var idx = fieldNames.indexOf(jsonField)
        if (idx >= 0) {
          // it is, copy the child tree to the correct location in the output row
          val output = new ByteArrayOutputStream()

          // write the output directly to UTF8 encoded byte array
          if (parser.nextToken() != JsonToken.VALUE_NULL) {
            Utils.tryWithResource(jsonFactory.createGenerator(output, JsonEncoding.UTF8)) {
              generator => copyCurrentStructure(generator, parser)
            }

            val jsonValue = UTF8String.fromBytes(output.toByteArray)

            // SPARK-21804: json_tuple returns null values within repeated columns
            // except the first one; so that we need to check the remaining fields.
            do {
              row(idx) = jsonValue
              idx = fieldNames.indexOf(jsonField, idx + 1)
            } while (idx >= 0)
          }
        }
      }

      // always skip children, it's cheap enough to do even if copyCurrentStructure was called
      parser.skipChildren()
    }

    new GenericInternalRow(row) :: Nil
  }

  private def copyCurrentStructure(generator: JsonGenerator, parser: JsonParser): Unit = {
    parser.getCurrentToken match {
      // if the user requests a string field it needs to be returned without enclosing
      // quotes which is accomplished via JsonGenerator.writeRaw instead of JsonGenerator.write
      case JsonToken.VALUE_STRING if parser.hasTextCharacters =>
        // slight optimization to avoid allocating a String instance, though the characters
        // still have to be decoded... Jackson doesn't have a way to access the raw bytes
        generator.writeRaw(parser.getTextCharacters, parser.getTextOffset, parser.getTextLength)

      case JsonToken.VALUE_STRING =>
        // the normal String case, pass it through to the output without enclosing quotes
        generator.writeRaw(parser.getText)

      case JsonToken.VALUE_NULL =>
        // a special case that needs to be handled outside of this method.
        // if a requested field is null, the result must be null. the easiest
        // way to achieve this is just by ignoring null tokens entirely
        throw QueryExecutionErrors.copyNullFieldNotAllowedError

      case _ =>
        // handle other types including objects, arrays, booleans and numbers
        generator.copyCurrentStructure(parser)
    }
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): JsonTuple =
    copy(children = newChildren)
}

/**
 * Converts an json input string to a [[StructType]], [[ArrayType]] or [[MapType]]
 * with the specified schema.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(jsonStr, schema[, options]) - Returns a struct value with the given `jsonStr` and `schema`.",
  examples = """
    Examples:
      > SELECT _FUNC_('{"a":1, "b":0.8}', 'a INT, b DOUBLE');
       {"a":1,"b":0.8}
      > SELECT _FUNC_('{"time":"26/08/2015"}', 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy'));
       {"time":2015-08-26 00:00:00}
  """,
  group = "json_funcs",
  since = "2.2.0")
// scalastyle:on line.size.limit
case class JsonToStructs(
    schema: DataType,
    options: Map[String, String],
    child: Expression,
    timeZoneId: Option[String] = None)
  extends UnaryExpression with TimeZoneAwareExpression with CodegenFallback with ExpectsInputTypes
    with NullIntolerant {

  // The JSON input data might be missing certain fields. We force the nullability
  // of the user-provided schema to avoid data corruptions. In particular, the parquet-mr encoder
  // can generate incorrect files if values are missing in columns declared as non-nullable.
  val nullableSchema = schema.asNullable

  override def nullable: Boolean = true

  final override def nodePatternsInternal(): Seq[TreePattern] = Seq(JSON_TO_STRUCT)

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

  override def checkInputDataTypes(): TypeCheckResult = nullableSchema match {
    case _: StructType | _: ArrayType | _: MapType =>
      ExprUtils.checkJsonSchema(nullableSchema).map { e =>
        TypeCheckResult.TypeCheckFailure(e.getMessage)
      } getOrElse {
        super.checkInputDataTypes()
      }
    case _ => TypeCheckResult.TypeCheckFailure(
      s"Input schema ${nullableSchema.catalogString} must be a struct, an array or a map.")
  }

  // This converts parsed rows to the desired output by the given schema.
  @transient
  lazy val converter = nullableSchema match {
    case _: StructType =>
      (rows: Iterator[InternalRow]) => if (rows.hasNext) rows.next() else null
    case _: ArrayType =>
      (rows: Iterator[InternalRow]) => if (rows.hasNext) rows.next().getArray(0) else null
    case _: MapType =>
      (rows: Iterator[InternalRow]) => if (rows.hasNext) rows.next().getMap(0) else null
  }

  val nameOfCorruptRecord = SQLConf.get.getConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD)
  @transient lazy val parser = {
    val parsedOptions = new JSONOptions(options, timeZoneId.get, nameOfCorruptRecord)
    val mode = parsedOptions.parseMode
    if (mode != PermissiveMode && mode != FailFastMode) {
      throw QueryCompilationErrors.parseModeUnsupportedError("from_json", mode)
    }
    val (parserSchema, actualSchema) = nullableSchema match {
      case s: StructType =>
        ExprUtils.verifyColumnNameOfCorruptRecord(s, parsedOptions.columnNameOfCorruptRecord)
        (s, StructType(s.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord)))
      case other =>
        (StructType(StructField("value", other) :: Nil), other)
    }

    val rawParser = new JacksonParser(actualSchema, parsedOptions, allowArrayAsStructs = false)
    val createParser = CreateJacksonParser.utf8String _

    new FailureSafeParser[UTF8String](
      input => rawParser.parse(input, createParser, identity[UTF8String]),
      mode,
      parserSchema,
      parsedOptions.columnNameOfCorruptRecord)
  }

  override def dataType: DataType = nullableSchema

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def nullSafeEval(json: Any): Any = {
    converter(parser.parse(json.asInstanceOf[UTF8String]))
  }

  override def inputTypes: Seq[AbstractDataType] = StringType :: Nil

  override def sql: String = schema match {
    case _: MapType => "entries"
    case _ => super.sql
  }

  override def prettyName: String = "from_json"

  override protected def withNewChildInternal(newChild: Expression): JsonToStructs =
    copy(child = newChild)
}

/**
 * Converts a [[StructType]], [[ArrayType]] or [[MapType]] to a JSON output string.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr[, options]) - Returns a JSON string with a given struct value",
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
      > SELECT _FUNC_(array((map('a', 1))));
       [{"a":1}]
  """,
  group = "json_funcs",
  since = "2.2.0")
// scalastyle:on line.size.limit
case class StructsToJson(
    options: Map[String, String],
    child: Expression,
    timeZoneId: Option[String] = None)
  extends UnaryExpression with TimeZoneAwareExpression with CodegenFallback
    with ExpectsInputTypes with NullIntolerant {
  override def nullable: Boolean = true

  def this(options: Map[String, String], child: Expression) = this(options, child, None)

  // Used in `FunctionRegistry`
  def this(child: Expression) = this(Map.empty, child, None)
  def this(child: Expression, options: Expression) =
    this(
      options = ExprUtils.convertToMapData(options),
      child = child,
      timeZoneId = None)

  @transient
  lazy val writer = new CharArrayWriter()

  @transient
  lazy val gen = new JacksonGenerator(
    inputSchema, writer, new JSONOptions(options, timeZoneId.get))

  @transient
  lazy val inputSchema = child.dataType

  // This converts rows to the JSON output according to the given schema.
  @transient
  lazy val converter: Any => UTF8String = {
    def getAndReset(): UTF8String = {
      gen.flush()
      val json = writer.toString
      writer.reset()
      UTF8String.fromString(json)
    }

    inputSchema match {
      case _: StructType =>
        (row: Any) =>
          gen.write(row.asInstanceOf[InternalRow])
          getAndReset()
      case _: ArrayType =>
        (arr: Any) =>
          gen.write(arr.asInstanceOf[ArrayData])
          getAndReset()
      case _: MapType =>
        (map: Any) =>
          gen.write(map.asInstanceOf[MapData])
          getAndReset()
    }
  }

  override def dataType: DataType = StringType

  override def checkInputDataTypes(): TypeCheckResult = inputSchema match {
    case struct: StructType =>
      try {
        JacksonUtils.verifySchema(struct)
        TypeCheckResult.TypeCheckSuccess
      } catch {
        case e: UnsupportedOperationException =>
          TypeCheckResult.TypeCheckFailure(e.getMessage)
      }
    case map: MapType =>
      try {
        JacksonUtils.verifyType(prettyName, map)
        TypeCheckResult.TypeCheckSuccess
      } catch {
        case e: UnsupportedOperationException =>
          TypeCheckResult.TypeCheckFailure(e.getMessage)
      }
    case array: ArrayType =>
      try {
        JacksonUtils.verifyType(prettyName, array)
        TypeCheckResult.TypeCheckSuccess
      } catch {
        case e: UnsupportedOperationException =>
          TypeCheckResult.TypeCheckFailure(e.getMessage)
      }
    case _ => TypeCheckResult.TypeCheckFailure(
      s"Input type ${child.dataType.catalogString} must be a struct, array of structs or " +
          "a map or array of map.")
  }

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def nullSafeEval(value: Any): Any = converter(value)

  override def inputTypes: Seq[AbstractDataType] = TypeCollection(ArrayType, StructType) :: Nil

  override def prettyName: String = "to_json"

  override protected def withNewChildInternal(newChild: Expression): StructsToJson =
    copy(child = newChild)
}

/**
 * A function infers schema of JSON string.
 */
@ExpressionDescription(
  usage = "_FUNC_(json[, options]) - Returns schema in the DDL format of JSON string.",
  examples = """
    Examples:
      > SELECT _FUNC_('[{"col":0}]');
       ARRAY<STRUCT<`col`: BIGINT>>
      > SELECT _FUNC_('[{"col":01}]', map('allowNumericLeadingZeros', 'true'));
       ARRAY<STRUCT<`col`: BIGINT>>
  """,
  group = "json_funcs",
  since = "2.4.0")
case class SchemaOfJson(
    child: Expression,
    options: Map[String, String])
  extends UnaryExpression with CodegenFallback {

  def this(child: Expression) = this(child, Map.empty[String, String])

  def this(child: Expression, options: Expression) = this(
      child = child,
      options = ExprUtils.convertToMapData(options))

  override def dataType: DataType = StringType

  override def nullable: Boolean = false

  @transient
  private lazy val jsonOptions = new JSONOptions(options, "UTC")

  @transient
  private lazy val jsonFactory = jsonOptions.buildJsonFactory()

  @transient
  private lazy val jsonInferSchema = new JsonInferSchema(jsonOptions)

  @transient
  private lazy val json = child.eval().asInstanceOf[UTF8String]

  override def checkInputDataTypes(): TypeCheckResult = {
    if (child.foldable && json != null) {
      super.checkInputDataTypes()
    } else {
      TypeCheckResult.TypeCheckFailure(
        "The input json should be a foldable string expression and not null; " +
        s"however, got ${child.sql}.")
    }
  }

  override def eval(v: InternalRow): Any = {
    val dt = Utils.tryWithResource(CreateJacksonParser.utf8String(jsonFactory, json)) { parser =>
      parser.nextToken()
      // To match with schema inference from JSON datasource.
      jsonInferSchema.inferField(parser) match {
        case st: StructType =>
          jsonInferSchema.canonicalizeType(st, jsonOptions).getOrElse(StructType(Nil))
        case at: ArrayType if at.elementType.isInstanceOf[StructType] =>
          jsonInferSchema
            .canonicalizeType(at.elementType, jsonOptions)
            .map(ArrayType(_, containsNull = at.containsNull))
            .getOrElse(ArrayType(StructType(Nil), containsNull = at.containsNull))
        case other: DataType =>
          jsonInferSchema.canonicalizeType(other, jsonOptions).getOrElse(StringType)
      }
    }

    UTF8String.fromString(dt.sql)
  }

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
case class LengthOfJsonArray(child: Expression) extends UnaryExpression
  with CodegenFallback with ExpectsInputTypes {

  override def inputTypes: Seq[DataType] = Seq(StringType)
  override def dataType: DataType = IntegerType
  override def nullable: Boolean = true
  override def prettyName: String = "json_array_length"

  override def eval(input: InternalRow): Any = {
    val json = child.eval(input).asInstanceOf[UTF8String]
    // return null for null input
    if (json == null) {
      return null
    }

    try {
      Utils.tryWithResource(CreateJacksonParser.utf8String(SharedFactory.jsonFactory, json)) {
        parser => {
          // return null if null array is encountered.
          if (parser.nextToken() == null) {
            return null
          }
          // Parse the array to compute its length.
          parseCounter(parser, input)
        }
      }
    } catch {
      case _: JsonProcessingException | _: IOException => null
    }
  }

  private def parseCounter(parser: JsonParser, input: InternalRow): Any = {
    var length = 0
    // Only JSON array are supported for this function.
    if (parser.currentToken != JsonToken.START_ARRAY) {
      return null
    }
    // Keep traversing until the end of JSON array
    while(parser.nextToken() != JsonToken.END_ARRAY) {
      length += 1
      // skip all the child of inner object or array
      parser.skipChildren()
    }
    length
  }

  override protected def withNewChildInternal(newChild: Expression): LengthOfJsonArray =
    copy(child = newChild)
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
case class JsonObjectKeys(child: Expression) extends UnaryExpression with CodegenFallback
  with ExpectsInputTypes {

  override def inputTypes: Seq[DataType] = Seq(StringType)
  override def dataType: DataType = ArrayType(StringType)
  override def nullable: Boolean = true
  override def prettyName: String = "json_object_keys"

  override def eval(input: InternalRow): Any = {
    val json = child.eval(input).asInstanceOf[UTF8String]
    // return null for `NULL` input
    if(json == null) {
      return null
    }

    try {
      Utils.tryWithResource(CreateJacksonParser.utf8String(SharedFactory.jsonFactory, json)) {
        parser => {
          // return null if an empty string or any other valid JSON string is encountered
          if (parser.nextToken() == null || parser.currentToken() != JsonToken.START_OBJECT) {
            return null
          }
          // Parse the JSON string to get all the keys of outermost JSON object
          getJsonKeys(parser, input)
        }
      }
    } catch {
      case _: JsonProcessingException | _: IOException => null
    }
  }

  private def getJsonKeys(parser: JsonParser, input: InternalRow): GenericArrayData = {
    var arrayBufferOfKeys = ArrayBuffer.empty[UTF8String]

    // traverse until the end of input and ensure it returns valid key
    while(parser.nextValue() != null && parser.currentName() != null) {
      // add current fieldName to the ArrayBuffer
      arrayBufferOfKeys += UTF8String.fromString(parser.getCurrentName)

      // skip all the children of inner object or array
      parser.skipChildren()
    }
    new GenericArrayData(arrayBufferOfKeys.toArray)
  }

  override protected def withNewChildInternal(newChild: Expression): JsonObjectKeys =
    copy(child = newChild)
}
