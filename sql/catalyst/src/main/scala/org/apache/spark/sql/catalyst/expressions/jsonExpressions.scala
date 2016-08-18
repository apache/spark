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

import java.io.{ByteArrayOutputStream, StringWriter}

import scala.util.parsing.combinator.RegexParsers

import com.fasterxml.jackson.core._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
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
      name <- '.' ~> "[^\\.\\[]+".r | "[\\'" ~> "[^\\'\\?]+" <~ "\\']"
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

      case NoSuccess(msg, next) =>
        None
    }
  }
}

private[this] object SharedFactory {
  val jsonFactory = new JsonFactory()

  // Enabled for Hive compatibility
  jsonFactory.enable(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS)
}

/**
 * Extracts json object from a json string based on json path specified, and returns json string
 * of the extracted json object. It will return null if the input json string is invalid.
 */
@ExpressionDescription(
  usage = "_FUNC_(json_txt, path) - Extract a json object from path")
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
        Utils.tryWithResource(jsonFactory.createParser(jsonStr.getBytes)) { parser =>
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
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(jsonStr, p1, p2, ..., pn) - like get_json_object, but it takes multiple names and return a tuple. All the input parameters and output column types are string.")
// scalastyle:on line.size.limit
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
  @transient private lazy val foldableFieldNames: IndexedSeq[String] = {
    fieldExpressions.map {
      case expr if expr.foldable => expr.eval().asInstanceOf[UTF8String].toString
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
      Utils.tryWithResource(jsonFactory.createParser(json.getBytes)) {
        parser => parseRow(parser, input)
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
      foldableFieldNames
    } else if (constantFields == 0) {
      // none are foldable so all field names need to be evaluated from the input row
      fieldExpressions.map(_.eval(input).asInstanceOf[UTF8String].toString)
    } else {
      // if there is a mix of constant and non-constant expressions
      // prefer the cached copy when available
      foldableFieldNames.zip(fieldExpressions).map {
        case (null, expr) => expr.eval(input).asInstanceOf[UTF8String].toString
        case (fieldName, _) => fieldName
      }
    }

    val row = Array.ofDim[Any](fieldNames.length)

    // start reading through the token stream, looking for any requested field names
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      if (parser.getCurrentToken == JsonToken.FIELD_NAME) {
        // check to see if this field is desired in the output
        val idx = fieldNames.indexOf(parser.getCurrentName)
        if (idx >= 0) {
          // it is, copy the child tree to the correct location in the output row
          val output = new ByteArrayOutputStream()

          // write the output directly to UTF8 encoded byte array
          if (parser.nextToken() != JsonToken.VALUE_NULL) {
            Utils.tryWithResource(jsonFactory.createGenerator(output, JsonEncoding.UTF8)) {
              generator => copyCurrentStructure(generator, parser)
            }

            row(idx) = UTF8String.fromBytes(output.toByteArray)
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
        throw new IllegalStateException("Do not attempt to copy a null field")

      case _ =>
        // handle other types including objects, arrays, booleans and numbers
        generator.copyCurrentStructure(parser)
    }
  }
}

