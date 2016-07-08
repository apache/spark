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

package org.apache.spark.sql.execution.datasources.json

import java.io.ByteArrayOutputStream

import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.core._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.datasources.json.JacksonUtils.nextUntil
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

private[json] class SparkSQLJsonProcessingException(msg: String) extends RuntimeException(msg)

private[sql] class JacksonParser(schema: StructType, options: JSONOptions) extends Logging {
  import com.fasterxml.jackson.core.JsonToken._

  // A `ValueConverter` is responsible for converting a value from `JsonParser`
  // to a value in a field for `InternalRow`.
  private type ValueConverter = (JsonParser) => Any

  // `ValueConverter`s for the root schema for all fields in the schema
  private val rootConverter: ValueConverter = makeRootConverter(schema)

  private val factory = new JsonFactory()
  options.setJacksonOptions(factory)


  private def failedConversion(
      parser: JsonParser,
      dataType: DataType,
      token: JsonToken): Any = token match {
    case null | VALUE_NULL => null
    case _ if parser.getTextLength < 1 => null
    case _ =>
    // We cannot parse this token based on the given data type. So, we throw a
    // SparkSQLJsonProcessingException and this exception will be caught by
    // parseJson method.
    throw new SparkSQLJsonProcessingException(
      s"Failed to parse a value for data type $dataType (current token: $token).")
  }

  private def failedRecord(record: String): Seq[InternalRow] = {
    // create a row even if no corrupt record column is present
    if (options.failFast) {
      throw new RuntimeException(s"Malformed line in FAILFAST mode: $record")
    }
    if (options.dropMalformed) {
      logWarning(s"Dropping malformed line: $record")
      Nil
    } else {
      val row = new GenericMutableRow(schema.length)
      for (corruptIndex <- schema.getFieldIndex(options.columnNameOfCorruptRecord)) {
        require(schema(corruptIndex).dataType == StringType)
        row.update(corruptIndex, UTF8String.fromString(record))
      }
      Seq(row)
    }
  }

  /**
   * Parse the current token (and related children) according to a desired schema
   * This is a wrapper for the method `convertField()` to handle a row wrapped
   * with an array.
   */
  def makeRootConverter(dataType: DataType): ValueConverter = dataType match {
    case st: StructType =>
      // SPARK-3308: support reading top level JSON arrays and take every element
      // in such an array as a row
      val elementConverter = makeConverter(st)
      val fieldConverters = st.map(_.dataType).map(makeConverter)
      (parser: JsonParser) => parser.getCurrentToken match {
        case START_ARRAY => convertArray(parser, elementConverter)
        case START_OBJECT => convertObject(parser, st, fieldConverters)
        case token => failedConversion(parser, st, token)
      }

    case ArrayType(st: StructType, _) =>
      // the business end of SPARK-3308:
      // when an object is found but an array is requested just wrap it in a list
      val elementConverter = makeConverter(st)
      val fieldConverters = st.map(_.dataType).map(makeConverter)
      (parser: JsonParser) => parser.getCurrentToken match {
        case START_OBJECT => convertObject(parser, st, fieldConverters)
        case START_ARRAY => convertArray(parser, elementConverter)
        case token => failedConversion(parser, st, token)
      }

    case _ => makeConverter(dataType)
  }

  private def makeConverter(dataType: DataType): ValueConverter = dataType match {
    case dt: BooleanType =>
      (parser: JsonParser) => skipFieldNameTokenIfExists(parser) {
        parser.getCurrentToken match {
          case VALUE_TRUE => true
          case VALUE_FALSE => false
          case token => failedConversion(parser, dt, token)
        }
      }

    case dt: ByteType =>
      (parser: JsonParser) => skipFieldNameTokenIfExists(parser) {
        parser.getCurrentToken match {
          case VALUE_NUMBER_INT => parser.getByteValue
          case token => failedConversion(parser, dt, token)
        }
      }

    case dt: ShortType =>
      (parser: JsonParser) => skipFieldNameTokenIfExists(parser) {
        parser.getCurrentToken match {
          case VALUE_NUMBER_INT => parser.getShortValue
          case token => failedConversion(parser, dt, token)
        }
      }

    case dt: IntegerType =>
      (parser: JsonParser) => skipFieldNameTokenIfExists(parser) {
        parser.getCurrentToken match {
          case VALUE_NUMBER_INT => parser.getIntValue
          case token => failedConversion(parser, dt, token)
        }
      }

    case dt: LongType =>
      (parser: JsonParser) => skipFieldNameTokenIfExists(parser) {
        parser.getCurrentToken match {
          case VALUE_NUMBER_INT => parser.getLongValue
          case token => failedConversion(parser, dt, token)
        }
      }

    case dt: FloatType =>
      (parser: JsonParser) => skipFieldNameTokenIfExists(parser) {
        parser.getCurrentToken match {
          case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT =>
            parser.getFloatValue

          case VALUE_STRING =>
            // Special case handling for NaN and Infinity.
            val value = parser.getText
            val lowerCaseValue = value.toLowerCase
            if (lowerCaseValue.equals("nan") ||
              lowerCaseValue.equals("infinity") ||
              lowerCaseValue.equals("-infinity") ||
              lowerCaseValue.equals("inf") ||
              lowerCaseValue.equals("-inf")) {
              value.toFloat
            } else {
              throw new SparkSQLJsonProcessingException(s"Cannot parse $value as FloatType.")
            }

          case token => failedConversion(parser, dt, token)
        }
      }

    case dt: DoubleType =>
      (parser: JsonParser) => skipFieldNameTokenIfExists(parser) {
        parser.getCurrentToken match {
          case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT =>
            parser.getDoubleValue

          case VALUE_STRING =>
            // Special case handling for NaN and Infinity.
            val value = parser.getText
            val lowerCaseValue = value.toLowerCase
            if (lowerCaseValue.equals("nan") ||
              lowerCaseValue.equals("infinity") ||
              lowerCaseValue.equals("-infinity") ||
              lowerCaseValue.equals("inf") ||
              lowerCaseValue.equals("-inf")) {
              value.toDouble
            } else {
              throw new SparkSQLJsonProcessingException(s"Cannot parse $value as DoubleType.")
            }

          case token => failedConversion(parser, dt, token)
        }
      }

    case StringType =>
      (parser: JsonParser) => skipFieldNameTokenIfExists(parser) {
        parser.getCurrentToken match {
          case VALUE_STRING =>
            UTF8String.fromString(parser.getText)

          case token if token != VALUE_NULL =>
            val writer = new ByteArrayOutputStream()
            Utils.tryWithResource(factory.createGenerator(writer, JsonEncoding.UTF8)) {
              generator => generator.copyCurrentStructure(parser)
            }
            UTF8String.fromBytes(writer.toByteArray)

          case token => failedConversion(parser, dataType, token)
        }
      }

    case dt: TimestampType =>
      (parser: JsonParser) => skipFieldNameTokenIfExists(parser) {
        parser.getCurrentToken match {
          case VALUE_STRING =>
            // This one will lose microseconds parts.
            // See https://issues.apache.org/jira/browse/SPARK-10681.
            DateTimeUtils.stringToTime(parser.getText).getTime * 1000L

          case VALUE_NUMBER_INT =>
            parser.getLongValue * 1000000L

          case token => failedConversion(parser, dt, token)
        }
      }

    case dt: DateType =>
      (parser: JsonParser) => skipFieldNameTokenIfExists(parser) {
        parser.getCurrentToken match {
          case VALUE_STRING =>
            val stringValue = parser.getText
            if (stringValue.contains("-")) {
              // The format of this string will probably be "yyyy-mm-dd".
              DateTimeUtils.millisToDays(DateTimeUtils.stringToTime(parser.getText).getTime)
            } else {
              // In Spark 1.5.0, we store the data as number of days since epoch in string.
              // So, we just convert it to Int.
              stringValue.toInt
            }

          case token => failedConversion(parser, dt, token)
        }
      }

    case dt: BinaryType =>
      (parser: JsonParser) => skipFieldNameTokenIfExists(parser) {
        parser.getCurrentToken match {
          case VALUE_STRING => parser.getBinaryValue
          case token => failedConversion(parser, dt, token)
        }
      }

    case dt: DecimalType =>
      (parser: JsonParser) => skipFieldNameTokenIfExists(parser) {
        parser.getCurrentToken match {
          case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT) =>
            Decimal(parser.getDecimalValue, dt.precision, dt.scale)

          case token => failedConversion(parser, dt, token)
        }
      }

    case st: StructType =>
      val fieldConverters = st.map(_.dataType).map(makeConverter)
      (parser: JsonParser) => skipFieldNameTokenIfExists(parser) {
        parser.getCurrentToken match {
          case START_OBJECT => convertObject(parser, st, fieldConverters)
          case token => failedConversion(parser, st, token)
        }
      }

    case at: ArrayType =>
      val elementConverter = makeConverter(at.elementType)
      (parser: JsonParser) => skipFieldNameTokenIfExists(parser) {
        parser.getCurrentToken match {
          case START_ARRAY => convertArray(parser, elementConverter)
          case token => failedConversion(parser, at, token)
        }
      }

    case mt: MapType =>
      val valueConverter = makeConverter(mt.valueType)
      (parser: JsonParser) => skipFieldNameTokenIfExists(parser) {
        parser.getCurrentToken match {
          case START_OBJECT => convertMap(parser, valueConverter)
          case token => failedConversion(parser, mt, token)
        }
      }

    case t: UserDefinedType[_] =>
      makeConverter(t.sqlType)

    case dt: DataType =>
      (parser: JsonParser) =>
        failedConversion(parser, dt, parser.getCurrentToken)
  }

  private def skipFieldNameTokenIfExists(parser: JsonParser)(f: => Any): Any = {
    parser.getCurrentToken match {
      case FIELD_NAME =>
        parser.nextToken
        f
      case _ =>
        f
    }
  }

  /**
   * Parse an object from the token stream into a new Row representing the schema.
   *
   * Fields in the json that are not defined in the requested schema will be dropped.
   */
  private def convertObject(
      parser: JsonParser,
      currentSchema: StructType,
      fieldConverters: Seq[ValueConverter]): InternalRow = {
    val row = new GenericMutableRow(currentSchema.length)
    while (nextUntil(parser, JsonToken.END_OBJECT)) {
      currentSchema.getFieldIndex(parser.getCurrentName) match {
        case Some(index) =>
          row.update(index, fieldConverters(index).apply(parser))

        case None =>
          parser.skipChildren()
      }
    }

    row
  }

  /**
   * Parse an object as a Map, preserving all fields
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

  private def convertArray(
      parser: JsonParser,
      fieldConverter: ValueConverter): ArrayData = {
    val values = ArrayBuffer.empty[Any]
    while (nextUntil(parser, JsonToken.END_ARRAY)) {
      values += fieldConverter.apply(parser)
    }

    new GenericArrayData(values.toArray)
  }

  def parse(input: String): Seq[InternalRow] = {
    if (input.trim.isEmpty) {
      Nil
    } else {
      try {
        Utils.tryWithResource(factory.createParser(input)) { parser =>
          parser.nextToken()
          rootConverter.apply(parser) match {
            case null => failedRecord(input)
            case row: InternalRow => row :: Nil
            case array: ArrayData =>
              if (array.numElements() == 0) {
                Nil
              } else {
                array.toArray[InternalRow](schema)
              }
            case _ =>
              failedRecord(input)
          }
        }
      } catch {
        case _: JsonProcessingException =>
          failedRecord(input)
        case _: SparkSQLJsonProcessingException =>
          failedRecord(input)
      }
    }
  }
}
