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

package org.apache.spark.sql.catalyst.json

import java.io.{ByteArrayOutputStream, CharConversionException}
import java.nio.charset.MalformedInputException

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import com.fasterxml.jackson.core._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.util.DateTimeUtils.TimestampParser
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * Constructs a parser for a given schema that translates a json string to an [[InternalRow]].
 */
class JacksonParser(
    schema: DataType,
    val options: JSONOptions) extends Logging {

  import JacksonUtils._
  import com.fasterxml.jackson.core.JsonToken._

  // A `ValueConverter` is responsible for converting a value from `JsonParser`
  // to a value in a field for `InternalRow`.
  private type ValueConverter = JsonParser => AnyRef

  // `ValueConverter`s for the root schema for all fields in the schema
  private val rootConverter = makeRootConverter(schema)

  private val factory = new JsonFactory()
  options.setJacksonOptions(factory)

  @transient private lazy val timestampParser = new TimestampParser(options.timestampFormat)

  /**
   * Create a converter which converts the JSON documents held by the `JsonParser`
   * to a value according to a desired schema. This is a wrapper for the method
   * `makeConverter()` to handle a row wrapped with an array.
   */
  private def makeRootConverter(dt: DataType): JsonParser => Seq[InternalRow] = {
    dt match {
      case st: StructType => makeStructRootConverter(st)
      case mt: MapType => makeMapRootConverter(mt)
      case at: ArrayType => makeArrayRootConverter(at)
    }
  }

  private def makeStructRootConverter(st: StructType): JsonParser => Seq[InternalRow] = {
    val elementConverter = makeConverter(st)
    val fieldConverters = st.map(_.dataType).map(makeConverter).toArray
    (parser: JsonParser) => parseJsonToken[Seq[InternalRow]](parser, st) {
      case START_OBJECT => convertObject(parser, st, fieldConverters) :: Nil
        // SPARK-3308: support reading top level JSON arrays and take every element
        // in such an array as a row
        //
        // For example, we support, the JSON data as below:
        //
        // [{"a":"str_a_1"}]
        // [{"a":"str_a_2"}, {"b":"str_b_3"}]
        //
        // resulting in:
        //
        // List([str_a_1,null])
        // List([str_a_2,null], [null,str_b_3])
        //
      case START_ARRAY =>
        val array = convertArray(parser, elementConverter)
        // Here, as we support reading top level JSON arrays and take every element
        // in such an array as a row, this case is possible.
        if (array.numElements() == 0) {
          Nil
        } else {
          array.toArray[InternalRow](schema).toSeq
        }
    }
  }

  private def makeMapRootConverter(mt: MapType): JsonParser => Seq[InternalRow] = {
    val fieldConverter = makeConverter(mt.valueType)
    (parser: JsonParser) => parseJsonToken[Seq[InternalRow]](parser, mt) {
      case START_OBJECT => Seq(InternalRow(convertMap(parser, fieldConverter)))
    }
  }

  private def makeArrayRootConverter(at: ArrayType): JsonParser => Seq[InternalRow] = {
    val elemConverter = makeConverter(at.elementType)
    (parser: JsonParser) => parseJsonToken[Seq[InternalRow]](parser, at) {
      case START_ARRAY => Seq(InternalRow(convertArray(parser, elemConverter)))
      case START_OBJECT if at.elementType.isInstanceOf[StructType] =>
        // This handles the case when an input JSON object is a structure but
        // the specified schema is an array of structures. In that case, the input JSON is
        // considered as an array of only one element of struct type.
        // This behavior was introduced by changes for SPARK-19595.
        //
        // For example, if the specified schema is ArrayType(new StructType().add("i", IntegerType))
        // and JSON input as below:
        //
        // [{"i": 1}, {"i": 2}]
        // [{"i": 3}]
        // {"i": 4}
        //
        // The last row is considered as an array with one element, and result of conversion:
        //
        // Seq(Row(1), Row(2))
        // Seq(Row(3))
        // Seq(Row(4))
        //
        val st = at.elementType.asInstanceOf[StructType]
        val fieldConverters = st.map(_.dataType).map(makeConverter).toArray
        Seq(InternalRow(new GenericArrayData(Seq(convertObject(parser, st, fieldConverters)))))
    }
  }

  /**
   * Create a converter which converts the JSON documents held by the `JsonParser`
   * to a value according to a desired schema.
   */
  def makeConverter(dataType: DataType): ValueConverter = dataType match {
    case BooleanType =>
      (parser: JsonParser) => parseJsonToken[java.lang.Boolean](parser, dataType) {
        case VALUE_TRUE => true
        case VALUE_FALSE => false
      }

    case ByteType =>
      (parser: JsonParser) => parseJsonToken[java.lang.Byte](parser, dataType) {
        case VALUE_NUMBER_INT => parser.getByteValue
      }

    case ShortType =>
      (parser: JsonParser) => parseJsonToken[java.lang.Short](parser, dataType) {
        case VALUE_NUMBER_INT => parser.getShortValue
      }

    case IntegerType =>
      (parser: JsonParser) => parseJsonToken[java.lang.Integer](parser, dataType) {
        case VALUE_NUMBER_INT => parser.getIntValue
      }

    case LongType =>
      (parser: JsonParser) => parseJsonToken[java.lang.Long](parser, dataType) {
        case VALUE_NUMBER_INT => parser.getLongValue
      }

    case FloatType =>
      (parser: JsonParser) => parseJsonToken[java.lang.Float](parser, dataType) {
        case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT =>
          parser.getFloatValue

        case VALUE_STRING =>
          // Special case handling for NaN and Infinity.
          parser.getText match {
            case "NaN" => Float.NaN
            case "Infinity" => Float.PositiveInfinity
            case "-Infinity" => Float.NegativeInfinity
            case other => throw new RuntimeException(
              s"Cannot parse $other as ${FloatType.catalogString}.")
          }
      }

    case DoubleType =>
      (parser: JsonParser) => parseJsonToken[java.lang.Double](parser, dataType) {
        case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT =>
          parser.getDoubleValue

        case VALUE_STRING =>
          // Special case handling for NaN and Infinity.
          parser.getText match {
            case "NaN" => Double.NaN
            case "Infinity" => Double.PositiveInfinity
            case "-Infinity" => Double.NegativeInfinity
            case other =>
              throw new RuntimeException(s"Cannot parse $other as ${DoubleType.catalogString}.")
          }
      }

    case StringType =>
      (parser: JsonParser) => parseJsonToken[UTF8String](parser, dataType) {
        case VALUE_STRING =>
          UTF8String.fromString(parser.getText)

        case _ =>
          // Note that it always tries to convert the data as string without the case of failure.
          val writer = new ByteArrayOutputStream()
          Utils.tryWithResource(factory.createGenerator(writer, JsonEncoding.UTF8)) {
            generator => generator.copyCurrentStructure(parser)
          }
          UTF8String.fromBytes(writer.toByteArray)
      }

    case TimestampType =>
      (parser: JsonParser) => parseJsonToken[java.lang.Long](parser, dataType) {
        case VALUE_STRING =>
          val stringValue = parser.getText
          Long.box {
            Try(timestampParser.parse(stringValue)).getOrElse {
              // If it fails to parse, then tries the way used in 2.0 and 1.x for backwards
              // compatibility.
              DateTimeUtils.stringToTime(stringValue).getTime * 1000L
            }
          }

        case VALUE_NUMBER_INT =>
          parser.getLongValue * 1000000L
      }

    case DateType =>
      (parser: JsonParser) => parseJsonToken[java.lang.Integer](parser, dataType) {
        case VALUE_STRING =>
          val stringValue = parser.getText
          // This one will lose microseconds parts.
          // See https://issues.apache.org/jira/browse/SPARK-10681.x
          Int.box {
            Try(DateTimeUtils.millisToDays(options.dateFormat.parse(stringValue).getTime))
              .orElse {
                // If it fails to parse, then tries the way used in 2.0 and 1.x for backwards
                // compatibility.
                Try(DateTimeUtils.millisToDays(DateTimeUtils.stringToTime(stringValue).getTime))
              }
              .getOrElse {
                // In Spark 1.5.0, we store the data as number of days since epoch in string.
                // So, we just convert it to Int.
                stringValue.toInt
              }
          }
      }

    case BinaryType =>
      (parser: JsonParser) => parseJsonToken[Array[Byte]](parser, dataType) {
        case VALUE_STRING => parser.getBinaryValue
      }

    case dt: DecimalType =>
      (parser: JsonParser) => parseJsonToken[Decimal](parser, dataType) {
        case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT) =>
          Decimal(parser.getDecimalValue, dt.precision, dt.scale)
      }

    case st: StructType =>
      val fieldConverters = st.map(_.dataType).map(makeConverter).toArray
      (parser: JsonParser) => parseJsonToken[InternalRow](parser, dataType) {
        case START_OBJECT => convertObject(parser, st, fieldConverters)
      }

    case at: ArrayType =>
      val elementConverter = makeConverter(at.elementType)
      (parser: JsonParser) => parseJsonToken[ArrayData](parser, dataType) {
        case START_ARRAY => convertArray(parser, elementConverter)
      }

    case mt: MapType =>
      val valueConverter = makeConverter(mt.valueType)
      (parser: JsonParser) => parseJsonToken[MapData](parser, dataType) {
        case START_OBJECT => convertMap(parser, valueConverter)
      }

    case udt: UserDefinedType[_] =>
      makeConverter(udt.sqlType)

    case _ =>
      (parser: JsonParser) =>
        // Here, we pass empty `PartialFunction` so that this case can be
        // handled as a failed conversion. It will throw an exception as
        // long as the value is not null.
        parseJsonToken[AnyRef](parser, dataType)(PartialFunction.empty[JsonToken, AnyRef])
  }

  /**
   * This method skips `FIELD_NAME`s at the beginning, and handles nulls ahead before trying
   * to parse the JSON token using given function `f`. If the `f` failed to parse and convert the
   * token, call `failedConversion` to handle the token.
   */
  private def parseJsonToken[R >: Null](
      parser: JsonParser,
      dataType: DataType)(f: PartialFunction[JsonToken, R]): R = {
    parser.getCurrentToken match {
      case FIELD_NAME =>
        // There are useless FIELD_NAMEs between START_OBJECT and END_OBJECT tokens
        parser.nextToken()
        parseJsonToken[R](parser, dataType)(f)

      case null | VALUE_NULL => null

      case other => f.applyOrElse(other, failedConversion(parser, dataType))
    }
  }

  /**
   * This function throws an exception for failed conversion, but returns null for empty string,
   * to guard the non string types.
   */
  private def failedConversion[R >: Null](
      parser: JsonParser,
      dataType: DataType): PartialFunction[JsonToken, R] = {
    case VALUE_STRING if parser.getTextLength < 1 =>
      // If conversion is failed, this produces `null` rather than throwing exception.
      // This will protect the mismatch of types.
      null

    case token =>
      // We cannot parse this token based on the given data type. So, we throw a
      // RuntimeException and this exception will be caught by `parse` method.
      throw new RuntimeException(
        s"Failed to parse a value for data type ${dataType.catalogString} (current token: $token).")
  }

  /**
   * Parse an object from the token stream into a new Row representing the schema.
   * Fields in the json that are not defined in the requested schema will be dropped.
   */
  private def convertObject(
      parser: JsonParser,
      schema: StructType,
      fieldConverters: Array[ValueConverter]): InternalRow = {
    val row = new GenericInternalRow(schema.length)
    while (nextUntil(parser, JsonToken.END_OBJECT)) {
      schema.getFieldIndex(parser.getCurrentName) match {
        case Some(index) =>
          row.update(index, fieldConverters(index).apply(parser))

        case None =>
          parser.skipChildren()
      }
    }

    row
  }

  /**
   * Parse an object as a Map, preserving all fields.
   */
  private def convertMap(
      parser: JsonParser,
      fieldConverter: ValueConverter): MapData = {
    val keys = ArrayBuffer.empty[UTF8String]
    val values = ArrayBuffer.empty[Any]
    while (nextUntil(parser, JsonToken.END_OBJECT)) {
      keys += UTF8String.fromString(parser.getCurrentName)
      values += fieldConverter.apply(parser)
    }

    ArrayBasedMapData(keys.toArray, values.toArray)
  }

  /**
   * Parse an object as a Array.
   */
  private def convertArray(
      parser: JsonParser,
      fieldConverter: ValueConverter): ArrayData = {
    val values = ArrayBuffer.empty[Any]
    while (nextUntil(parser, JsonToken.END_ARRAY)) {
      values += fieldConverter.apply(parser)
    }

    new GenericArrayData(values.toArray)
  }

  /**
   * Parse the JSON input to the set of [[InternalRow]]s.
   *
   * @param recordLiteral an optional function that will be used to generate
   *   the corrupt record text instead of record.toString
   */
  def parse[T](
      record: T,
      createParser: (JsonFactory, T) => JsonParser,
      recordLiteral: T => UTF8String): Seq[InternalRow] = {
    try {
      Utils.tryWithResource(createParser(factory, record)) { parser =>
        // a null first token is equivalent to testing for input.trim.isEmpty
        // but it works on any token stream and not just strings
        parser.nextToken() match {
          case null => Nil
          case _ => rootConverter.apply(parser) match {
            case null => throw new RuntimeException("Root converter returned null")
            case rows => rows
          }
        }
      }
    } catch {
      case e @ (_: RuntimeException | _: JsonProcessingException | _: MalformedInputException) =>
        // JSON parser currently doesn't support partial results for corrupted records.
        // For such records, all fields other than the field configured by
        // `columnNameOfCorruptRecord` are set to `null`.
        throw BadRecordException(() => recordLiteral(record), () => None, e)
      case e: CharConversionException if options.encoding.isEmpty =>
        val msg =
          """JSON parser cannot handle a character in its input.
            |Specifying encoding as an input option explicitly might help to resolve the issue.
            |""".stripMargin + e.getMessage
        val wrappedCharException = new CharConversionException(msg)
        wrappedCharException.initCause(e)
        throw BadRecordException(() => recordLiteral(record), () => None, wrappedCharException)
    }
  }
}
