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
package org.apache.spark.sql.catalyst.expressions.json

import java.io.{ByteArrayOutputStream, CharArrayWriter, StringWriter}

import scala.collection.mutable
import scala.util.parsing.combinator.RegexParsers

import com.fasterxml.jackson.core._
import com.fasterxml.jackson.core.json.JsonReadFeature

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExprUtils, GenericInternalRow, GetJsonObject}
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JacksonGenerator, JacksonParser, JsonInferSchema, JSONOptions}
import org.apache.spark.sql.catalyst.util.{ArrayData, FailFastMode, FailureSafeParser, MapData, PermissiveMode}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StringType, StructField, StructType, VariantType}
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}
import org.apache.spark.util.Utils

sealed trait PathInstruction
object PathInstruction {
  private[expressions] case object Subscript extends PathInstruction
  private[expressions] case object Wildcard extends PathInstruction
  private[expressions] case object Key extends PathInstruction
  private[expressions] case class Index(index: Long) extends PathInstruction
  case class Named(name: String) extends PathInstruction
}

private[this] sealed trait WriteStyle
private[this] object WriteStyle {
  private[expressions] case object RawStyle extends WriteStyle
  private[expressions] case object QuotedStyle extends WriteStyle
  private[expressions] case object FlattenStyle extends WriteStyle
}

object JsonPathParser extends RegexParsers {
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
      name <- '.' ~> "[^\\.\\[]+".r | "['" ~> "[^\\']+".r <~ "']"
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
  val jsonFactory: JsonFactory = new JsonFactoryBuilder()
    // The two options below enabled for Hive compatibility
    .enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS)
    .enable(JsonReadFeature.ALLOW_SINGLE_QUOTES)
    .build()
}

case class JsonToStructsEvaluator(
    options: Map[String, String],
    nullableSchema: DataType,
    nameOfCorruptRecord: String,
    timeZoneId: Option[String],
    variantAllowDuplicateKeys: Boolean) {

  // This converts parsed rows to the desired output by the given schema.
  @transient
  private lazy val converter = nullableSchema match {
    case _: StructType =>
      (rows: Iterator[InternalRow]) => if (rows.hasNext) rows.next() else null
    case _: ArrayType =>
      (rows: Iterator[InternalRow]) => if (rows.hasNext) rows.next().getArray(0) else null
    case _: MapType =>
      (rows: Iterator[InternalRow]) => if (rows.hasNext) rows.next().getMap(0) else null
    case _: VariantType =>
      (rows: Iterator[InternalRow]) => if (rows.hasNext) rows.next().getVariant(0) else null
  }

  @transient
  private lazy val parser = {
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
        (StructType(Array(StructField("value", other))), other)
    }

    val rawParser = new JacksonParser(actualSchema, parsedOptions, allowArrayAsStructs = false)
    val createParser = CreateJacksonParser.utf8String _

    new FailureSafeParser[UTF8String](
      input => rawParser.parse(input, createParser, identity[UTF8String]),
      mode,
      parserSchema,
      parsedOptions.columnNameOfCorruptRecord)
  }

  final def evaluate(json: UTF8String): Any = {
    if (json == null) return null
    converter(parser.parse(json))
  }
}

case class StructsToJsonEvaluator(
    options: Map[String, String],
    inputSchema: DataType,
    timeZoneId: Option[String]) {

  @transient
  private lazy val writer = new CharArrayWriter()

  @transient
  private lazy val gen = new JacksonGenerator(
    inputSchema, writer, new JSONOptions(options, timeZoneId.get))

  // This converts rows to the JSON output according to the given schema.
  @transient
  private lazy val converter: Any => UTF8String = {
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
      case _: VariantType =>
        (v: Any) =>
          gen.write(v.asInstanceOf[VariantVal])
          getAndReset()
    }
  }

  final def evaluate(value: Any): Any = {
    converter(value)
  }
}

case class SchemaOfJsonEvaluator(options: Map[String, String]) {
  @transient
  private lazy val jsonOptions = new JSONOptions(options, "UTC")

  @transient
  private lazy val jsonFactory = jsonOptions.buildJsonFactory()

  @transient
  private lazy val jsonInferSchema = new JsonInferSchema(jsonOptions)

  final def evaluate(json: UTF8String): Any = {
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
          jsonInferSchema.canonicalizeType(other, jsonOptions).getOrElse(
            StringType)
      }
    }

    UTF8String.fromString(dt.sql)
  }
}

/**
 * The expression `JsonTuple` will utilize it to support codegen.
 */
case class JsonTupleEvaluator(foldableFieldNames: Array[Option[String]]) {

  import SharedFactory._

  // If processing fails this shared value will be returned.
  @transient private lazy val nullRow: Seq[InternalRow] =
    new GenericInternalRow(Array.ofDim[Any](foldableFieldNames.length)) :: Nil

  // And count the number of foldable fields, we'll use this later to optimize evaluation.
  @transient private lazy val constantFields: Int = foldableFieldNames.count(_ != null)

  private def getFieldNameStrings(fields: Array[UTF8String]): Array[String] = {
    // Evaluate the field names as String rather than UTF8String to
    // optimize lookups from the json token, which is also a String.
    if (constantFields == fields.length) {
      // Typically the user will provide the field names as foldable expressions
      // so we can use the cached copy.
      foldableFieldNames.map(_.orNull)
    } else if (constantFields == 0) {
      // None are foldable so all field names need to be evaluated from the input row.
      fields.map { f => if (f != null) f.toString else null }
    } else {
      // If there is a mix of constant and non-constant expressions
      // prefer the cached copy when available.
      foldableFieldNames.zip(fields).map {
        case (null, f) => if (f != null) f.toString else null
        case (fieldName, _) => fieldName.orNull
      }
    }
  }

  private def parseRow(parser: JsonParser, fieldNames: Array[String]): Seq[InternalRow] = {
    // Only objects are supported.
    if (parser.nextToken() != JsonToken.START_OBJECT) return nullRow

    val row = Array.ofDim[Any](fieldNames.length)

    // Start reading through the token stream, looking for any requested field names.
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      if (parser.getCurrentToken == JsonToken.FIELD_NAME) {
        // Check to see if this field is desired in the output.
        val jsonField = parser.currentName
        var idx = fieldNames.indexOf(jsonField)
        if (idx >= 0) {
          // It is, copy the child tree to the correct location in the output row.
          val output = new ByteArrayOutputStream()

          // Write the output directly to UTF8 encoded byte array.
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

      // Always skip children, it's cheap enough to do even if copyCurrentStructure was called.
      parser.skipChildren()
    }
    new GenericInternalRow(row) :: Nil
  }

  private def copyCurrentStructure(generator: JsonGenerator, parser: JsonParser): Unit = {
    parser.getCurrentToken match {
      // If the user requests a string field it needs to be returned without enclosing
      // quotes which is accomplished via JsonGenerator.writeRaw instead of JsonGenerator.write.
      case JsonToken.VALUE_STRING if parser.hasTextCharacters =>
        // Slight optimization to avoid allocating a String instance, though the characters
        // still have to be decoded... Jackson doesn't have a way to access the raw bytes.
        generator.writeRaw(parser.getTextCharacters, parser.getTextOffset, parser.getTextLength)

      case JsonToken.VALUE_STRING =>
        // The normal String case, pass it through to the output without enclosing quotes.
        generator.writeRaw(parser.getText)

      case JsonToken.VALUE_NULL =>
        // A special case that needs to be handled outside of this method.
        // If a requested field is null, the result must be null. The easiest
        // way to achieve this is just by ignoring null tokens entirely.
        throw SparkException.internalError("Do not attempt to copy a null field.")

      case _ =>
        // Handle other types including objects, arrays, booleans and numbers.
        generator.copyCurrentStructure(parser)
    }
  }

  final def evaluate(json: UTF8String, fieldNames: Array[UTF8String]): IterableOnce[InternalRow] = {
    if (json == null) return nullRow
    try {
      /* We know the bytes are UTF-8 encoded. Pass a Reader to avoid having Jackson
      detect character encoding which could fail for some malformed strings. */
      Utils.tryWithResource(CreateJacksonParser.utf8String(jsonFactory, json)) { parser =>
        parseRow(parser, getFieldNameStrings(fieldNames))
      }
    } catch {
      case _: JsonProcessingException => nullRow
    }
  }
}

/**
 * The expression `GetJsonObject` will utilize it to support codegen.
 */
case class GetJsonObjectEvaluator(cachedPath: UTF8String) {
  import com.fasterxml.jackson.core.JsonToken._
  import PathInstruction._
  import SharedFactory._
  import WriteStyle._

  def this() = this(null)

  @transient
  private lazy val parsedPath: Option[List[PathInstruction]] = parsePath(cachedPath)

  @transient
  private var jsonStr: UTF8String = _

  @transient
  private var pathStr: UTF8String = _

  @transient
  private lazy val outputBuffer = new ByteArrayOutputStream()

  def setJson(arg: UTF8String): Unit = {
    jsonStr = arg
  }

  def setPath(arg: UTF8String): Unit = {
    pathStr = arg
  }

  def evaluate(): Any = {
    if (jsonStr == null) return null

    val parsed = if (cachedPath != null) {
      parsedPath
    } else {
      parsePath(pathStr)
    }

    if (parsed.isDefined) {
      try {
        /* We know the bytes are UTF-8 encoded. Pass a Reader to avoid having Jackson
          detect character encoding which could fail for some malformed strings */
        Utils.tryWithResource(CreateJacksonParser.utf8String(jsonFactory, jsonStr)) { parser =>
          outputBuffer.reset()
          val matched = Utils.tryWithResource(
            jsonFactory.createGenerator(outputBuffer, JsonEncoding.UTF8)) { generator =>
            parser.nextToken()
            evaluatePath(parser, generator, RawStyle, parsed.get)
          }
          if (matched) {
            UTF8String.fromBytes(outputBuffer.toByteArray)
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
          case QuotedStyle => throw SparkException.internalError("Unexpected the quoted style.")
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
          g.writeRawValue(buf.substring(1, buf.length() - 1))
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

      case (FIELD_NAME, Named(name) :: xs) if p.currentName == name =>
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
}

/**
 * Evaluates multiple simple object-key and array-index JSON paths in one parse.
 */
case class MultiGetJsonObjectEvaluator(
    fallbackPaths: Seq[UTF8String],
    simplePaths: Seq[Seq[GetJsonObject.SimpleJsonPathSegment]]) {
  import SharedFactory._

  require(fallbackPaths.nonEmpty && simplePaths.length == fallbackPaths.length)

  @transient
  private lazy val useTopLevelFastPath: Boolean =
    simplePaths.forall {
      case Seq(_: GetJsonObject.NamedPathSegment) => true
      case _ => false
    } && simplePaths.distinct.length == simplePaths.length

  @transient
  private lazy val topLevelFieldToOrdinal: Map[String, Int] =
    simplePaths.zipWithIndex.map { case (path, ordinal) =>
      path.head.asInstanceOf[GetJsonObject.NamedPathSegment].name -> ordinal
    }.toMap

  @transient
  private lazy val pathTrie: MultiGetJsonObjectEvaluator.PathTrieNode =
    MultiGetJsonObjectEvaluator.buildPathTrie(simplePaths)

  @transient
  private lazy val nullRow: InternalRow =
    new GenericInternalRow(Array.ofDim[Any](fallbackPaths.length))

  @transient
  private lazy val fallbackEvaluators: Seq[GetJsonObjectEvaluator] =
    fallbackPaths.map(new GetJsonObjectEvaluator(_))

  @transient
  private lazy val outputBuffer = new ByteArrayOutputStream()

  private def fallback(json: UTF8String): InternalRow = {
    new GenericInternalRow(fallbackEvaluators.map { evaluator =>
      evaluator.setJson(json)
      evaluator.evaluate()
    }.toArray)
  }

  def evaluate(json: UTF8String): InternalRow = {
    if (json == null) return null

    val values = Array.ofDim[Any](fallbackPaths.length)
    val matched = Array.ofDim[Boolean](fallbackPaths.length)

    try {
      val validRoot = Utils.tryWithResource(
        CreateJacksonParser.utf8String(jsonFactory, json)) { parser =>
        parser.nextToken() match {
          case JsonToken.START_OBJECT if pathTrie.namedChildren.isEmpty =>
            false
          case JsonToken.START_OBJECT if useTopLevelFastPath =>
            extractTopLevelObject(parser, values, matched)
          case JsonToken.START_OBJECT =>
            extractObject(parser, pathTrie, values, matched)
          case JsonToken.START_ARRAY if pathTrie.indexedChildren.isEmpty =>
            false
          case JsonToken.START_ARRAY =>
            extractArray(parser, pathTrie, values, matched)
          case _ =>
            false
        }
      }
      if (validRoot) {
        new GenericInternalRow(values)
      } else {
        nullRow
      }
    } catch {
      // Every eligible legacy extraction scans through its root container's closing token, so a
      // syntax failure makes every sibling null without needing per-path reparsing.
      case _: JsonParseException => nullRow
      // A parser-side rendering failure, such as a string-length constraint violation, can leave
      // the shared token stream unusable. Reparse each path with the legacy evaluator so one bad
      // selected value cannot erase independent sibling results.
      case _: JsonProcessingException => fallback(json)
    }
  }

  private def extractTopLevelObject(
      parser: JsonParser,
      values: Array[Any],
      matched: Array[Boolean]): Boolean = {
    var token = parser.nextToken()
    while (token != null && token != JsonToken.END_OBJECT) {
      if (token == JsonToken.FIELD_NAME) {
        val ordinal = topLevelFieldToOrdinal.get(parser.currentName).filter(!matched(_))
        val valueToken = parser.nextToken()
        if (ordinal.nonEmpty && valueToken != JsonToken.VALUE_NULL) {
          val index = ordinal.get
          matched(index) = true
          copyCurrentStructure(parser).foreach(value => values(index) = value)
        } else {
          parser.skipChildren()
        }
      } else {
        parser.skipChildren()
      }
      token = parser.nextToken()
    }
    token == JsonToken.END_OBJECT
  }

  private def extractObject(
      parser: JsonParser,
      node: MultiGetJsonObjectEvaluator.PathTrieNode,
      values: Array[Any],
      matched: Array[Boolean]): Boolean = {
    var valid = true
    var token = parser.nextToken()
    while (valid && token != null && token != JsonToken.END_OBJECT) {
      if (token == JsonToken.FIELD_NAME) {
        val child = node.namedChildren.get(parser.currentName).filter(_.hasUnmatched(matched))
        val valueToken = parser.nextToken()
        if (child.nonEmpty && valueToken != JsonToken.VALUE_NULL) {
          valid = extractValue(parser, child.get, values, matched)
        } else {
          parser.skipChildren()
        }
      } else {
        parser.skipChildren()
      }
      if (valid) {
        token = parser.nextToken()
      }
    }
    valid && token == JsonToken.END_OBJECT
  }

  private def extractArray(
      parser: JsonParser,
      node: MultiGetJsonObjectEvaluator.PathTrieNode,
      values: Array[Any],
      matched: Array[Boolean]): Boolean = {
    var valid = true
    var index = 0L
    var token = parser.nextToken()
    while (valid && token != null && token != JsonToken.END_ARRAY) {
      val child = node.indexedChildren.get(index).filter(_.hasUnmatched(matched))
      if (child.nonEmpty) {
        valid = extractValue(parser, child.get, values, matched)
      } else {
        parser.skipChildren()
      }
      if (valid) {
        token = parser.nextToken()
        index += 1
      }
    }
    valid && token == JsonToken.END_ARRAY
  }

  private def extractValue(
      parser: JsonParser,
      node: MultiGetJsonObjectEvaluator.PathTrieNode,
      values: Array[Any],
      matched: Array[Boolean]): Boolean = {
    // Optimizer-generated paths are deduplicated. Multiple ordinals defensively support
    // directly constructed internal expressions with duplicate paths.
    if (node.terminalOrdinals.nonEmpty) {
      node.terminalOrdinals.foreach { ordinal => matched(ordinal) = true }
      val value = copyCurrentStructure(parser)
      value.foreach { result =>
        node.terminalOrdinals.foreach { ordinal => values(ordinal) = result }
      }
      true
    } else if (parser.currentToken == JsonToken.START_OBJECT) {
      extractObject(parser, node, values, matched)
    } else if (parser.currentToken == JsonToken.START_ARRAY) {
      extractArray(parser, node, values, matched)
    } else {
      parser.skipChildren()
      true
    }
  }

  private def copyCurrentStructure(parser: JsonParser): Option[UTF8String] = {
    outputBuffer.reset()
    var renderingFailed = false

    def render(write: => Unit): Unit = {
      if (!renderingFailed) {
        try {
          write
        } catch {
          // A generator-side failure does not invalidate the parser's token stream. Keep
          // consuming that value so other requested fields remain independent.
          case _: JsonGenerationException => renderingFailed = true
        }
      }
    }

    def copyValue(generator: JsonGenerator, rawString: Boolean): Unit = {
      if (parser.currentToken == JsonToken.VALUE_STRING && rawString) {
        render {
          if (parser.hasTextCharacters) {
            generator.writeRaw(
              parser.getTextCharacters,
              parser.getTextOffset,
              parser.getTextLength)
          } else {
            generator.writeRaw(parser.getText)
          }
        }
      } else {
        // Keep this traversal iterative so a value near the configured nesting limit does not
        // consume one JVM frame per level.
        var depth = 0
        var done = false
        while (!done && parser.currentToken != null) {
          parser.currentToken match {
            case JsonToken.START_OBJECT =>
              render(generator.writeStartObject())
              depth += 1
            case JsonToken.START_ARRAY =>
              render(generator.writeStartArray())
              depth += 1
            case JsonToken.END_OBJECT =>
              render(generator.writeEndObject())
              depth -= 1
            case JsonToken.END_ARRAY =>
              render(generator.writeEndArray())
              depth -= 1
            case _ =>
              render(generator.copyCurrentEvent(parser))
          }
          done = depth == 0
          if (!done) {
            parser.nextToken()
          }
        }
      }
    }

    try {
      Utils.tryWithResource(
        jsonFactory.createGenerator(outputBuffer, JsonEncoding.UTF8)) { generator =>
        copyValue(generator, rawString = true)
      }
    } catch {
      case _: JsonGenerationException => renderingFailed = true
    }

    if (renderingFailed) None else Some(UTF8String.fromBytes(outputBuffer.toByteArray))
  }
}

object MultiGetJsonObjectEvaluator {
  private final class MutablePathTrieNode {
    val terminalOrdinals: mutable.ArrayBuffer[Int] = mutable.ArrayBuffer.empty
    val namedChildren: mutable.LinkedHashMap[String, MutablePathTrieNode] =
      mutable.LinkedHashMap.empty
    val indexedChildren: mutable.LinkedHashMap[Long, MutablePathTrieNode] =
      mutable.LinkedHashMap.empty

    def freeze(): PathTrieNode = {
      require(
        terminalOrdinals.isEmpty || (namedChildren.isEmpty && indexedChildren.isEmpty),
        "Shared JSON paths must not be prefixes of one another")
      val frozenNamedChildren = namedChildren.iterator.map { case (name, child) =>
        name -> child.freeze()
      }.toMap
      val frozenIndexedChildren = indexedChildren.iterator.map { case (index, child) =>
        index -> child.freeze()
      }.toMap
      val ordinals = (terminalOrdinals.iterator ++
        frozenNamedChildren.valuesIterator.flatMap(_.descendantOrdinals.iterator) ++
        frozenIndexedChildren.valuesIterator.flatMap(_.descendantOrdinals.iterator)).toArray
      PathTrieNode(
        terminalOrdinals.toArray, frozenNamedChildren, frozenIndexedChildren, ordinals)
    }
  }

  private case class PathTrieNode(
      terminalOrdinals: Array[Int],
      namedChildren: Map[String, PathTrieNode],
      indexedChildren: Map[Long, PathTrieNode],
      descendantOrdinals: Array[Int]) {
    def hasUnmatched(matched: Array[Boolean]): Boolean = {
      descendantOrdinals.exists(index => !matched(index))
    }
  }

  private def buildPathTrie(
      paths: Seq[Seq[GetJsonObject.SimpleJsonPathSegment]]): PathTrieNode = {
    val root = new MutablePathTrieNode
    paths.zipWithIndex.foreach { case (path, ordinal) =>
      var node = root
      path.foreach {
        case GetJsonObject.NamedPathSegment(fieldName) =>
          node = node.namedChildren.getOrElseUpdate(fieldName, new MutablePathTrieNode)
        case GetJsonObject.IndexedPathSegment(index) =>
          node = node.indexedChildren.getOrElseUpdate(index, new MutablePathTrieNode)
      }
      node.terminalOrdinals += ordinal
    }
    root.freeze()
  }
}
