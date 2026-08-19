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

import org.apache.spark.{SparkException, TaskContext}
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

  // Guard the conversion so an oversized index (e.g. `[999999999999999999999999]`) makes the path
  // fail to parse rather than throwing NumberFormatException out of the parser.
  def long: Parser[Long] = "\\d+".r ^? { case x if x.toLongOption.isDefined => x.toLong }

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

  /**
   * Returns `Some(true)` if the path parses and contains a wildcard, `Some(false)` if it parses
   * without a wildcard, and `None` if it does not parse. Used by `JSON_TABLE` to validate that
   * column and (container) row paths are simple, wildcard-free paths.
   */
  def hasWildcard(str: String): Option[Boolean] = parse(str).map(_.contains(Wildcard))
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
 * The three-state result of navigating a JSON path for `JSON_TABLE`. `get_json_object` collapses
 * "the path is absent" and "the value is JSON null" into a single `null`, which is wrong for
 * `JSON_TABLE`: `EXISTS` must treat a present-but-null value as existing, and a value column must
 * distinguish SQL `NULL` from the literal string `"null"`. This ADT keeps the two cases distinct.
 */
sealed trait JsonPathResult
object JsonPathResult {
  /** The path did not match (the key/index is absent). */
  case object Missing extends JsonPathResult
  /** The path matched a JSON `null` literal. */
  case object NullValue extends JsonPathResult
  /** The path matched a value; `raw` is its verbatim JSON text (including quoted strings). */
  case class Found(raw: UTF8String) extends JsonPathResult
}

/**
 * The result of a single-value [[JsonTableEvaluator.lookup]] for `JSON_VALUE`. Unlike
 * [[JsonPathResult]] -- whose `Found` carries verbatim JSON text for `JSON_TABLE` to unquote later
 * -- a `Scalar` here already holds the value's cast-ready text (a string's unquoted/unescaped
 * content, a number's or boolean's source text), and an object/array match is reported as
 * `NonScalar` without being serialized at all, since `JSON_VALUE` routes non-scalars to ON ERROR.
 */
sealed trait JsonValueLookup
object JsonValueLookup {
  /** The path did not match (routes to ON EMPTY). */
  case object Missing extends JsonValueLookup
  /** The path matched a JSON `null` literal (yields SQL NULL). */
  case object NullValue extends JsonValueLookup
  /** The path matched an object or array, i.e. not a scalar (routes to ON ERROR). */
  case object NonScalar extends JsonValueLookup
  /** The path matched a scalar; `text` is its cast-ready value (strings already unquoted). */
  case class Scalar(text: UTF8String) extends JsonValueLookup
}

/**
 * A prefix trie over the (wildcard-free) column paths of a single `JSON_TABLE` invocation, built
 * once via [[JsonTableEvaluator.buildPathTrie]] and reused for every row. It lets
 * [[JsonTableEvaluator.navigateAll]] resolve all columns in a single traversal of a row item
 * instead of re-parsing the item once per column.
 *
 * Each node groups the paths that share a common prefix: `named`/`indexed` hold the object-key and
 * array-index steps to child nodes, and `terminals` lists the result-slot indices of the columns
 * whose path ends exactly at this node.
 */
private[expressions] final class JsonTablePathTrie {
  // Result-slot indices of columns whose path terminates at this node.
  var terminals: List[Int] = Nil
  // Object-key children, keyed by field name.
  val named: mutable.HashMap[String, JsonTablePathTrie] = mutable.HashMap.empty
  // Array-index children, keyed by index.
  val indexed: mutable.HashMap[Long, JsonTablePathTrie] = mutable.HashMap.empty

  def hasChildren: Boolean = named.nonEmpty || indexed.nonEmpty

  /** True if no column path was inserted (e.g. an ordinality-only table): nothing to resolve. */
  def isEmpty: Boolean = terminals.isEmpty && !hasChildren
}

/**
 * The result of positioning a parser at a JSON path for the `JSON_TABLE` row source (see
 * `positionAt`). Like [[JsonPathResult]] it distinguishes a missing path from a JSON `null`, but
 * `AtValue` leaves the parser on the matched value's first token (rather than serializing it) so
 * the row source can be streamed.
 */
sealed trait PositionResult
object PositionResult {
  /** The path did not match. */
  case object Missing extends PositionResult
  /** The path matched a JSON `null` literal. */
  case object NullValue extends PositionResult
  /** The path matched a value; the parser is positioned at its first token. */
  case object AtValue extends PositionResult
}

/**
 * Token-aware navigation of a `containerPath` shared by the SQL/JSON functions. Three entry
 * points, each with its own path constraints -- so `containerPath` is NOT wildcard-free in general:
 *
 *   - [[evaluate]] -- `JSON_TABLE` row source: given the input, a wildcard-free container path, and
 *     whether the row path ended in `[*]`, produces the per-row JSON documents that the
 *     [[org.apache.spark.sql.catalyst.expressions.JsonTable]] generator projects into columns via
 *     [[navigateColumns]]. `$.items[*]` (containerPath `$.items`, `explodeRoot` = true) explodes an
 *     array into rows; `$` or `$.x` (`explodeRoot` = false) yields exactly one row.
 *   - [[lookup]] -- `JSON_VALUE` single-scalar extraction, over a wildcard-free path
 *     (`explodeRoot` = false).
 *   - [[pathExists]] -- `JSON_EXISTS` existence test. Here `containerPath` MAY contain wildcards
 *     (`[*]`, `.*`, `['*']`) and is evaluated in SQL/JSON *lax* mode (auto-wrap/unwrap, see
 *     [[anyMatch]]); `explodeRoot` is unused, so construct with `explodeRoot` = false.
 *
 * Unlike `get_json_object`, navigation here is token-aware and distinguishes missing keys from
 * JSON `null` values (see [[JsonPathResult]] / [[PositionResult]]).
 *
 * Every entry point requires the input to be exactly one well-formed JSON value (no trailing
 * garbage, not empty); anything else is treated as malformed, so the caller applies the ON ERROR
 * behavior consistently in both modes.
 */
case class JsonTableEvaluator(containerPath: Seq[PathInstruction], explodeRoot: Boolean) {
  import PathInstruction._
  import SharedFactory._

  /**
   * Returns the per-row JSON documents selected by the row path as an iterator, or `None` if the
   * JSON is null or malformed, or if `[*]` was applied to a non-array (the caller maps `None` to
   * the configured ON ERROR behavior). A well-formed input whose row path matches nothing returns
   * `Some(empty iterator)`.
   *
   * The input is first scanned once to validate it is a single well-formed JSON value (so trailing
   * garbage is rejected consistently in both ON ERROR modes -- this pass is O(n) tokens and does
   * not materialize values). For the array (`[*]`) case the elements are then serialized one at a
   * time from a second parser, so the whole expanded payload is never held in memory at once.
   */
  final def evaluate(json: UTF8String): Option[Iterator[UTF8String]] = {
    if (json == null || !isSingleWellFormedValue(json)) return None
    // The parser is positioned at the matched value and, for the array case, handed to a lazy
    // iterator that reads elements directly from it -- the container is never serialized whole.
    // Ownership of `parser` transfers to that iterator (which closes it on exhaustion); in every
    // other branch we close it before returning.
    val parser = CreateJacksonParser.utf8String(jsonFactory, json)
    var transferred = false
    try {
      parser.nextToken()
      positionAt(parser, containerPath) match {
        case PositionResult.Missing =>
          // Well-formed JSON, but the row path matched nothing: no rows.
          Some(Iterator.empty)
        case PositionResult.NullValue =>
          // The container is JSON null. `[*]` over a non-array is an error; otherwise one row.
          if (explodeRoot) None else Some(Iterator.single(UTF8String.fromString("null")))
        case PositionResult.AtValue =>
          if (explodeRoot) {
            // `[*]` requires an array; a non-array match is an error.
            if (parser.currentToken != JsonToken.START_ARRAY) {
              None
            } else {
              val it = arrayElementIterator(parser) // owns and eventually closes `parser`
              transferred = true
              Some(it)
            }
          } else {
            Some(Iterator.single(serializeCurrentValue(parser)))
          }
      }
    } catch {
      case _: JsonProcessingException => None
    } finally {
      if (!transferred) parser.close()
    }
  }

  /**
   * Resolves `containerPath` against a single JSON value for `JSON_VALUE`, preserving the missing /
   * JSON-null / found distinction that [[evaluate]] collapses. Returns:
   *
   *   - `None` if the input is not a single well-formed JSON value (malformed / trailing garbage /
   *     empty);
   *   - `Some(Missing)` if the path matches nothing;
   *   - `Some(NullValue)` if the path matches an explicit JSON `null`;
   *   - `Some(NonScalar)` if the path matches an object or array;
   *   - `Some(Scalar(text))` if the path matches a scalar, where `text` is its cast-ready value --
   *     a string's unquoted/unescaped content, a number's or boolean's source text (see
   *     [[scalarAt]]).
   *
   * A `null` input is the caller's responsibility. `explodeRoot` is ignored: this is a single-value
   * lookup, so construct the evaluator with `explodeRoot = false`.
   *
   * A single parser both navigates the path and validates well-formedness: after the matched value
   * is captured (or the path is found missing), [[drainToRootEnd]] consumes the rest of the root
   * value and rejects any trailing content, so a valid prefix followed by garbage (or a second
   * root value) is rejected exactly as a fully malformed document is. This avoids the extra
   * O(document size) validation pass a separate `isSingleWellFormedValue` scan would add per row.
   */
  final def lookup(json: UTF8String): Option[JsonValueLookup] = {
    Utils.tryWithResource(CreateJacksonParser.utf8String(jsonFactory, json)) { parser =>
      try {
        if (parser.nextToken() == null) {
          None // empty or whitespace-only
        } else {
          val result = positionAt(parser, containerPath) match {
            case PositionResult.Missing => JsonValueLookup.Missing
            case PositionResult.NullValue => JsonValueLookup.NullValue
            case PositionResult.AtValue => scalarAt(parser)
          }
          // Reject a valid prefix trailed by extra content, keeping malformed-input semantics
          // identical to the array row source (which validates the whole document up front).
          if (drainToRootEnd(parser)) Some(result) else None
        }
      } catch {
        case _: JsonProcessingException => None
      }
    }
  }

  /**
   * Classifies the value the parser is positioned at (the `AtValue` case of [[positionAt]]) for a
   * `JSON_VALUE` [[lookup]], avoiding the serialize-then-reparse round trip that
   * [[serializeCurrentValue]] followed by [[unquotedString]] would incur:
   *
   *   - an object or array is consumed with `skipChildren` -- so [[drainToRootEnd]] can still walk
   *     back out and validate the document -- and reported as `NonScalar`, never serialized, since
   *     `JSON_VALUE` routes non-scalars to ON ERROR and so never needs the value;
   *   - a scalar's cast-ready text is read straight from the parser via `getText`, which returns a
   *     string's unquoted, unescaped content and a number's or boolean's verbatim source characters
   *     (so a high-precision fraction reaches a DECIMAL/STRING cast intact, as with
   *     `copyCurrentStructureExact`).
   *
   * A JSON `null` never reaches here -- [[positionAt]] reports it as `NullValue`.
   */
  private def scalarAt(parser: JsonParser): JsonValueLookup = parser.currentToken match {
    case JsonToken.START_OBJECT | JsonToken.START_ARRAY =>
      parser.skipChildren()
      JsonValueLookup.NonScalar
    case _ =>
      JsonValueLookup.Scalar(UTF8String.fromString(parser.getText))
  }

  /**
   * Finishes consuming the root JSON value the `parser` is partway through and verifies nothing
   * follows it, returning false if the input is truncated or has trailing content. Callers navigate
   * to (and serialize) a matched value with the same parser, which can leave it positioned inside
   * the enclosing containers; this walks back out to the root and confirms the document held
   * exactly one well-formed value.
   */
  private def drainToRootEnd(parser: JsonParser): Boolean = {
    // Walk out of any still-open containers, consuming the remainder of the root value.
    while (!parser.getParsingContext.inRoot) {
      if (parser.nextToken() == null) return false // truncated mid-value
    }
    // At the root now: exactly one value was present iff nothing remains.
    parser.nextToken() == null
  }

  /**
   * Tests whether `containerPath` matches at least one item in a single JSON document, evaluated in
   * SQL/JSON *lax* mode (see [[anyMatch]]): wildcards are supported and a structural mismatch is a
   * non-match rather than an error. Returns `Some(true)` if the path matches (including a match
   * whose value is an explicit JSON `null`), `Some(false)` if it matches nothing, and `None` if the
   * input is not a single well-formed JSON value (malformed / trailing garbage / empty). Does not
   * serialize the matched value. A `null` input is the caller's responsibility; `explodeRoot` is
   * ignored (construct the evaluator with `explodeRoot = false`).
   *
   * A single parser both navigates the path and validates well-formedness: after the match
   * decision, [[drainToRootEnd]] consumes the rest of the root value and rejects any trailing
   * content, so a valid prefix followed by garbage (or a second root value) is malformed exactly as
   * a fully bad document is. This avoids the extra O(document size) pass a separate
   * `isSingleWellFormedValue` scan would add per row.
   */
  final def pathExists(json: UTF8String): Option[Boolean] = {
    Utils.tryWithResource(CreateJacksonParser.utf8String(jsonFactory, json)) { parser =>
      try {
        if (parser.nextToken() == null) {
          None // empty or whitespace-only
        } else {
          val exists = anyMatch(parser, containerPath)
          if (drainToRootEnd(parser)) Some(exists) else None
        }
      } catch {
        case _: JsonProcessingException => None
      }
    }
  }

  /**
   * Returns whether `path` matches at least one item within the JSON value at the parser's current
   * token, evaluated in SQL/JSON *lax* mode (the `JSON_EXISTS` default, matching Oracle and
   * PostgreSQL):
   *   - wildcards are supported: `[*]` (any array element) and `.*` / `['*']` (any object member);
   *   - arrays are auto-unwrapped, i.e. a member/index/wildcard step applied to an array is applied
   *     to each element (so `$.a.b` matches when `a` is an array of objects each having `b`);
   *   - a non-array is auto-wrapped as a single-element array, so `[*]` / `[0]` match the value;
   *   - a structural mismatch (e.g. a member step on a scalar) is a non-match, never an error.
   *
   * Invariant: the parser enters positioned on the first token of the current value and leaves
   * positioned on that value's last token -- the value is always fully consumed, even after a match
   * is found -- so wildcard branches compose and [[drainToRootEnd]] can validate trailing content.
   */
  private def anyMatch(parser: JsonParser, path: Seq[PathInstruction]): Boolean = {
    path match {
      case Nil =>
        // End of path: the current value is present (including a JSON null) -> a match. Consume it.
        parser.skipChildren()
        true

      case Key :: Named(name) :: rest =>
        parser.currentToken match {
          case JsonToken.START_OBJECT =>
            // First-match semantics for a named key: only the first member with this name is
            // followed, so a duplicate key later in the object is ignored. This matches the
            // first-match `positionAt` / `navigateAll` used by `JSON_VALUE` / `JSON_TABLE`, so a
            // path resolves consistently across the JSON functions. The whole object is still
            // drained (subsequent members skipped) to keep the parser-position invariant.
            var found = false
            var matched = false
            var token = parser.nextToken()
            while (token != null && token != JsonToken.END_OBJECT) {
              val matches = !matched && parser.currentName == name
              parser.nextToken() // move onto the value; each branch consumes it
              if (matches) {
                matched = true
                if (anyMatch(parser, rest)) found = true
              } else {
                parser.skipChildren()
              }
              token = parser.nextToken()
            }
            found
          case JsonToken.START_ARRAY =>
            forEachElement(parser)(anyMatch(parser, path)) // lax auto-unwrap: apply to each element
          case _ =>
            false // member accessor on a scalar: no match (the scalar is already fully consumed)
        }

      case Subscript :: Index(index) :: rest =>
        parser.currentToken match {
          case JsonToken.START_ARRAY =>
            var found = false
            var i = 0L
            var token = parser.nextToken()
            while (token != null && token != JsonToken.END_ARRAY) {
              if (i == index) {
                if (anyMatch(parser, rest)) found = true
              } else {
                parser.skipChildren()
              }
              i += 1
              token = parser.nextToken()
            }
            found
          case _ =>
            // lax auto-wrap: a non-array is a single-element array; [0] matches, [i>0] does not.
            if (index == 0) {
              anyMatch(parser, rest)
            } else {
              parser.skipChildren() // consume the wrapped value to keep the invariant
              false
            }
        }

      case Subscript :: Wildcard :: rest =>
        parser.currentToken match {
          case JsonToken.START_ARRAY =>
            forEachElement(parser)(anyMatch(parser, rest))
          case _ =>
            anyMatch(parser, rest) // lax auto-wrap: a non-array is a single-element array
        }

      case Wildcard :: rest =>
        parser.currentToken match {
          case JsonToken.START_OBJECT =>
            var found = false
            var token = parser.nextToken()
            while (token != null && token != JsonToken.END_OBJECT) {
              parser.nextToken() // move onto the member value
              if (found) parser.skipChildren() else if (anyMatch(parser, rest)) found = true
              token = parser.nextToken()
            }
            found
          case JsonToken.START_ARRAY =>
            forEachElement(parser)(anyMatch(parser, path)) // lax auto-unwrap: apply to each element
          case _ =>
            false // member wildcard on a scalar: no members
        }

      case _ =>
        // Unreachable: JsonPathParser only produces the instruction pairs handled above.
        parser.skipChildren()
        false
    }
  }

  /**
   * Iterates the array the parser is positioned on (its current token is `START_ARRAY`), evaluating
   * `matchElement` once per element with the parser positioned on that element's first token; each
   * call must fully consume its element. Returns whether any element matched, and always drains the
   * whole array, leaving the parser on the closing `END_ARRAY`. Once a match is found the remaining
   * elements are skipped, not matched (existence short-circuits, but the array is still drained).
   */
  private def forEachElement(parser: JsonParser)(matchElement: => Boolean): Boolean = {
    var found = false
    var token = parser.nextToken()
    while (token != null && token != JsonToken.END_ARRAY) {
      if (found) parser.skipChildren() else if (matchElement) found = true
      token = parser.nextToken()
    }
    found
  }

  /**
   * Navigates `path` and leaves the parser positioned at the first token of the matched value
   * (returning `AtValue`), or returns `Missing`/`NullValue`. Unlike the column projection traversal
   * ([[navigateColumns]]), this does not serialize the value or finish consuming the enclosing
   * containers -- the caller either streams from the current position (array row source) or
   * serializes the single matched value.
   */
  private def positionAt(parser: JsonParser, path: Seq[PathInstruction]): PositionResult = {
    path match {
      case Nil =>
        if (parser.currentToken == JsonToken.VALUE_NULL) PositionResult.NullValue
        else PositionResult.AtValue

      case Key :: Named(name) :: rest =>
        if (parser.currentToken != JsonToken.START_OBJECT) {
          skipRest(parser)
          PositionResult.Missing
        } else {
          var token = parser.nextToken()
          while (token != null && token != JsonToken.END_OBJECT) {
            if (parser.currentName == name) {
              parser.nextToken() // move onto the value; stop here (first match wins)
              return positionAt(parser, rest)
            }
            parser.nextToken()
            parser.skipChildren()
            token = parser.nextToken()
          }
          PositionResult.Missing
        }

      case Subscript :: Index(index) :: rest =>
        if (parser.currentToken != JsonToken.START_ARRAY) {
          skipRest(parser)
          PositionResult.Missing
        } else {
          var i = 0L
          var token = parser.nextToken()
          while (token != null && token != JsonToken.END_ARRAY) {
            if (i == index) {
              return positionAt(parser, rest)
            }
            parser.skipChildren()
            i += 1
            token = parser.nextToken()
          }
          PositionResult.Missing
        }

      case _ =>
        // Should not happen: JSON_TABLE paths are validated to be simple and wildcard-free.
        skipRest(parser)
        PositionResult.Missing
    }
  }

  /**
   * Returns true if the input is exactly one well-formed JSON value with no trailing content, so a
   * valid prefix followed by garbage, or an empty document, is treated as malformed (consistently
   * in both ON ERROR modes).
   */
  private def isSingleWellFormedValue(json: UTF8String): Boolean = {
    try {
      Utils.tryWithResource(CreateJacksonParser.utf8String(jsonFactory, json)) { parser =>
        if (parser.nextToken() == null) {
          false // empty or whitespace-only
        } else {
          parser.skipChildren() // consume the first value in full
          parser.nextToken() == null // nothing must remain after it
        }
      }
    } catch {
      case _: JsonProcessingException => false
    }
  }

  /**
   * Resolves every column of `trie` against the value at the parser's current token in a single
   * traversal, writing each matched terminal's [[JsonPathResult]] into `out` at its slot index.
   * Only the simple wildcard-free instruction set produced for `JSON_TABLE` paths is modeled by the
   * trie (`Key`/`Named` object steps and `Subscript`/`Index` array steps).
   *
   * Slots left untouched keep their initial `Missing`. A matched value is stored as its raw JSON
   * text (`Found.raw`), i.e. strings keep their enclosing quotes so the fragment stays
   * re-parseable; value columns unquote scalar strings afterwards via [[JsonTable]]'s extraction.
   */
  private def navigateAll(
      parser: JsonParser,
      trie: JsonTablePathTrie,
      out: Array[JsonPathResult]): Unit = {
    val isNull = parser.currentToken == JsonToken.VALUE_NULL

    if (!trie.hasChildren) {
      // Leaf node: every column terminates here, so just record the current value (or null) and
      // consume it. This is the common case for disjoint column paths.
      if (trie.terminals.nonEmpty) {
        val result = if (isNull) JsonPathResult.NullValue
          else JsonPathResult.Found(serializeCurrentValue(parser))
        trie.terminals.foreach(out(_) = result)
      } else {
        skipRest(parser)
      }
    } else if (trie.terminals.nonEmpty && !isNull) {
      // A column path both ends here and extends deeper (e.g. `$.a` alongside `$.a.b`). Serialize
      // the value once for the terminals, then re-parse that fragment to resolve the deeper
      // columns -- this rare prefix overlap is the only place *within a single traversal* that a
      // value is parsed more than once, and even then the descendant columns are still resolved in
      // a single sub-traversal. (Separately, an array row item is serialized by
      // `arrayElementIterator` and parsed again here, once per row.)
      val raw = serializeCurrentValue(parser)
      val result = JsonPathResult.Found(raw)
      trie.terminals.foreach(out(_) = result)
      Utils.tryWithResource(CreateJacksonParser.utf8String(jsonFactory, raw)) { sub =>
        sub.nextToken()
        descendInto(sub, trie, out)
      }
    } else {
      // Descend for the deeper columns. Any terminal ending at this node resolves to `NullValue`:
      // the earlier `!isNull` branch already handled non-null terminals, so a terminal reaching
      // here means the value is a JSON null. Descendant-only slots that do not match stay `Missing`
      // (a null has no children, so `descendInto` skips it and leaves them untouched).
      if (trie.terminals.nonEmpty) trie.terminals.foreach(out(_) = JsonPathResult.NullValue)
      descendInto(parser, trie, out)
    }
  }

  /**
   * Descends into the object or array at the parser's current token, dispatching each matching
   * field/element to the corresponding child trie node via [[navigateAll]] and skipping the rest.
   * A scalar (or JSON null) has no children, so the whole value is skipped and the deeper columns
   * are left as `Missing`.
   */
  private def descendInto(
      parser: JsonParser,
      trie: JsonTablePathTrie,
      out: Array[JsonPathResult]): Unit = {
    parser.currentToken match {
      case JsonToken.START_OBJECT if trie.named.isEmpty =>
        // No object-key columns descend here (only array-index paths): skip the whole object.
        skipRest(parser)

      case JsonToken.START_OBJECT =>
        // First match wins for duplicate keys: once a trie key has been dispatched, later fields
        // with the same name are skipped.
        val consumed = mutable.HashSet.empty[String]
        var token = parser.nextToken()
        while (token != null && token != JsonToken.END_OBJECT) {
          val name = parser.currentName
          val child = trie.named.get(name)
          parser.nextToken() // move onto the field value
          if (child.isDefined && consumed.add(name)) {
            navigateAll(parser, child.get, out)
          } else {
            parser.skipChildren()
          }
          token = parser.nextToken()
        }

      case JsonToken.START_ARRAY if trie.indexed.isEmpty =>
        // No array-index columns descend here (only object-key paths): skip the whole array.
        skipRest(parser)

      case JsonToken.START_ARRAY =>
        var i = 0L
        var token = parser.nextToken()
        while (token != null && token != JsonToken.END_ARRAY) {
          val child = trie.indexed.get(i)
          if (child.isDefined) {
            navigateAll(parser, child.get, out)
          } else {
            parser.skipChildren()
          }
          i += 1
          token = parser.nextToken()
        }

      case _ =>
        // A scalar where some columns expected to descend: those stay Missing.
        skipRest(parser)
    }
  }

  /** Skips the remainder of the value at the parser's current token. */
  private def skipRest(parser: JsonParser): Unit = parser.skipChildren()

  /**
   * Serializes the value at the parser's current token to its raw JSON text. Strings keep their
   * enclosing quotes, so the result is always a re-parseable JSON fragment (this matters because a
   * matched value may be re-parsed as a row item). Value columns unquote scalar strings afterwards
   * via [[JsonTableEvaluator.unquotedString]].
   */
  private def serializeCurrentValue(parser: JsonParser): UTF8String = {
    val output = new ByteArrayOutputStream()
    Utils.tryWithResource(jsonFactory.createGenerator(output, JsonEncoding.UTF8)) {
      // `copyCurrentStructureExact` preserves floating-point tokens byte-for-byte; the plain
      // `copyCurrentStructure` may round them for textual formats, which would corrupt a
      // high-precision fraction before JSON_TABLE casts the reserialized text to DECIMAL/STRING.
      generator => generator.copyCurrentStructureExact(parser)
    }
    UTF8String.fromBytes(output.toByteArray)
  }

  // The array parser currently owned by an outstanding `arrayElementIterator`, or null. Since
  // `GenerateExec` evaluates rows sequentially and fully drains each row's iterator before the
  // next `eval`, at most one such parser is open at a time per task. Tracked so a single
  // task-completion listener (registered once below) can close it on early termination.
  @transient private var openArrayParser: JsonParser = _
  @transient private var completionListenerRegistered = false

  /**
   * Streams the elements of the array the `parser` is currently positioned at (`START_ARRAY`),
   * serializing one element at a time straight from the source parser -- the enclosing array is
   * never materialized as a whole. The iterator owns `parser`: it closes it on exhaustion (the
   * fast path). To also close it when the consumer stops early (e.g. a downstream `LIMIT`, or a
   * per-column cast failure) -- the `Generator` API has no close hook -- a *single* task-completion
   * listener is registered per evaluator (i.e. per task) that closes whichever parser is currently
   * open, rather than one listener per input row, so processing many JSON rows does not accumulate
   * an unbounded listener list. `Generate` can thus emit rows for a large array without holding the
   * full expanded payload in memory.
   */
  private def arrayElementIterator(parser: JsonParser): Iterator[UTF8String] = {
    openArrayParser = parser
    if (!completionListenerRegistered) {
      Option(TaskContext.get()).foreach { tc =>
        tc.addTaskCompletionListener[Unit] { _ =>
          val p = openArrayParser
          if (p != null && !p.isClosed) p.close()
        }
        completionListenerRegistered = true
      }
    }
    new Iterator[UTF8String] {
      private var nextToken = parser.nextToken()

      override def hasNext: Boolean = {
        val more = nextToken != null && nextToken != JsonToken.END_ARRAY
        if (!more) close()
        more
      }

      override def next(): UTF8String = {
        val element = serializeCurrentValue(parser)
        nextToken = parser.nextToken()
        element
      }

      // Close the parser and drop the evaluator's reference to it so the listener does not retain
      // it (and does not double-close) after this iterator is exhausted.
      private def close(): Unit = {
        if (!parser.isClosed) parser.close()
        if (openArrayParser eq parser) openArrayParser = null
      }
    }
  }

  /**
   * If `raw` is a JSON string literal (e.g. `"hi"`), returns its unquoted, unescaped value;
   * otherwise (numbers, booleans, objects, arrays) returns the raw JSON text unchanged. Used to
   * give a value column the string's content rather than its quoted JSON form.
   *
   * `raw` is a Jackson-serialized fragment with no leading whitespace, so only a fragment whose
   * first byte is `"` can be a string literal. Non-string values (the common case) are returned
   * without constructing a parser at all.
   */
  def unquotedString(raw: UTF8String): UTF8String = {
    if (raw.numBytes() == 0 || raw.getByte(0) != '"') return raw
    try {
      Utils.tryWithResource(CreateJacksonParser.utf8String(jsonFactory, raw)) { parser =>
        if (parser.nextToken() == JsonToken.VALUE_STRING) {
          UTF8String.fromString(parser.getText)
        } else {
          raw
        }
      }
    } catch {
      case _: JsonProcessingException => raw
    }
  }

  /**
   * Builds the prefix trie that lets [[navigateColumns]] resolve every column path in one pass. The
   * `paths` are indexed by result slot; a slot whose `include` is false (an ordinality column,
   * which has no JSON path) contributes nothing to the trie -- note this is distinct from a root
   * path `$`, which is an *empty but included* path that must resolve to the whole item. Call once
   * per `JSON_TABLE` invocation and reuse the result for every row.
   */
  def buildPathTrie(
      paths: Array[Seq[PathInstruction]],
      include: Array[Boolean]): JsonTablePathTrie = {
    val root = new JsonTablePathTrie
    var slot = 0
    while (slot < paths.length) {
      if (include(slot)) insertPath(root, paths(slot), slot)
      slot += 1
    }
    root
  }

  private def insertPath(root: JsonTablePathTrie, path: Seq[PathInstruction], slot: Int): Unit = {
    var node = root
    var rest = path
    var valid = true
    while (rest.nonEmpty && valid) {
      rest match {
        case Key :: Named(name) :: tail =>
          node = node.named.getOrElseUpdate(name, new JsonTablePathTrie)
          rest = tail
        case Subscript :: Index(index) :: tail =>
          node = node.indexed.getOrElseUpdate(index, new JsonTablePathTrie)
          rest = tail
        case _ =>
          // Should not happen: JSON_TABLE column paths are validated to be simple and
          // wildcard-free. Drop the slot rather than mis-resolve it (it will read as Missing).
          valid = false
      }
    }
    if (valid) node.terminals ::= slot
  }

  /**
   * Resolves every column path (as built by [[buildPathTrie]]) within a single row item in one
   * traversal, returning the per-slot results. Slots for ordinality columns (empty paths) are not
   * in the trie and stay `Missing`; the caller fills them directly. Used to extract value and
   * EXISTS columns with correct missing-vs-null semantics.
   */
  def navigateColumns(item: UTF8String, trie: JsonTablePathTrie, numColumns: Int)
      : Array[JsonPathResult] = {
    val out = Array.fill[JsonPathResult](numColumns)(JsonPathResult.Missing)
    // No path columns (e.g. an ordinality-only table): skip parsing the item entirely.
    if (trie.isEmpty) return out
    try {
      Utils.tryWithResource(CreateJacksonParser.utf8String(jsonFactory, item)) { parser =>
        parser.nextToken()
        navigateAll(parser, trie, out)
      }
    } catch {
      // A malformed item leaves already-resolved slots in place and the rest as Missing.
      case _: JsonProcessingException =>
    }
    out
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
