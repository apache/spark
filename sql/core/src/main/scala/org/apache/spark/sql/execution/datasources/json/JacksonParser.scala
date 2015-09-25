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

import com.fasterxml.jackson.core._

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.json.JacksonUtils.nextUntil
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

private[sql] object JacksonParser {
  def apply(
      json: RDD[String],
      schema: StructType,
      columnNameOfCorruptRecords: String): RDD[InternalRow] = {
    parseJson(json, schema, columnNameOfCorruptRecords)
  }

  /**
   * Parse the current token (and related children) according to a desired schema
   */
  private[sql] def convertField(
      factory: JsonFactory,
      parser: JsonParser,
      schema: DataType): Any = {
    import com.fasterxml.jackson.core.JsonToken._
    (parser.getCurrentToken, schema) match {
      case (null | VALUE_NULL, _) =>
        null

      case (FIELD_NAME, _) =>
        parser.nextToken()
        convertField(factory, parser, schema)

      case (VALUE_STRING, StringType) =>
        UTF8String.fromString(parser.getText)

      case (VALUE_STRING, _) if parser.getTextLength < 1 =>
        // guard the non string type
        null

      case (VALUE_STRING, BinaryType) =>
        parser.getBinaryValue

      case (VALUE_STRING, DateType) =>
        val stringValue = parser.getText
        if (stringValue.contains("-")) {
          // The format of this string will probably be "yyyy-mm-dd".
          DateTimeUtils.millisToDays(DateTimeUtils.stringToTime(parser.getText).getTime)
        } else {
          // In Spark 1.5.0, we store the data as number of days since epoch in string.
          // So, we just convert it to Int.
          stringValue.toInt
        }

      case (VALUE_STRING, TimestampType) =>
        // This one will lose microseconds parts.
        // See https://issues.apache.org/jira/browse/SPARK-10681.
        DateTimeUtils.stringToTime(parser.getText).getTime * 1000L

      case (VALUE_NUMBER_INT, TimestampType) =>
        parser.getLongValue * 1000L

      case (_, StringType) =>
        val writer = new ByteArrayOutputStream()
        val generator = factory.createGenerator(writer, JsonEncoding.UTF8)
        generator.copyCurrentStructure(parser)
        generator.close()
        UTF8String.fromBytes(writer.toByteArray)

      case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT, FloatType) =>
        parser.getFloatValue

      case (VALUE_STRING, FloatType) =>
        // Special case handling for NaN and Infinity.
        val value = parser.getText
        val lowerCaseValue = value.toLowerCase()
        if (lowerCaseValue.equals("nan") ||
          lowerCaseValue.equals("infinity") ||
          lowerCaseValue.equals("-infinity") ||
          lowerCaseValue.equals("inf") ||
          lowerCaseValue.equals("-inf")) {
          value.toFloat
        } else {
          sys.error(s"Cannot parse $value as FloatType.")
        }

      case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT, DoubleType) =>
        parser.getDoubleValue

      case (VALUE_STRING, DoubleType) =>
        // Special case handling for NaN and Infinity.
        val value = parser.getText
        val lowerCaseValue = value.toLowerCase()
        if (lowerCaseValue.equals("nan") ||
          lowerCaseValue.equals("infinity") ||
          lowerCaseValue.equals("-infinity") ||
          lowerCaseValue.equals("inf") ||
          lowerCaseValue.equals("-inf")) {
          value.toDouble
        } else {
          sys.error(s"Cannot parse $value as DoubleType.")
        }

      case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT, dt: DecimalType) =>
        Decimal(parser.getDecimalValue, dt.precision, dt.scale)

      case (VALUE_NUMBER_INT, ByteType) =>
        parser.getByteValue

      case (VALUE_NUMBER_INT, ShortType) =>
        parser.getShortValue

      case (VALUE_NUMBER_INT, IntegerType) =>
        parser.getIntValue

      case (VALUE_NUMBER_INT, LongType) =>
        parser.getLongValue

      case (VALUE_TRUE, BooleanType) =>
        true

      case (VALUE_FALSE, BooleanType) =>
        false

      case (START_OBJECT, st: StructType) =>
        convertObject(factory, parser, st)

      case (START_ARRAY, st: StructType) =>
        // SPARK-3308: support reading top level JSON arrays and take every element
        // in such an array as a row
        convertArray(factory, parser, st)

      case (START_ARRAY, ArrayType(st, _)) =>
        convertArray(factory, parser, st)

      case (START_OBJECT, ArrayType(st, _)) =>
        // the business end of SPARK-3308:
        // when an object is found but an array is requested just wrap it in a list
        convertField(factory, parser, st) :: Nil

      case (START_OBJECT, MapType(StringType, kt, _)) =>
        convertMap(factory, parser, kt)

      case (_, udt: UserDefinedType[_]) =>
        convertField(factory, parser, udt.sqlType)

      case (token, dataType) =>
        sys.error(s"Failed to parse a value for data type $dataType (current token: $token).")
    }
  }

  /**
   * Parse an object from the token stream into a new Row representing the schema.
   *
   * Fields in the json that are not defined in the requested schema will be dropped.
   */
  private def convertObject(
      factory: JsonFactory,
      parser: JsonParser,
      schema: StructType): InternalRow = {
    val row = new GenericMutableRow(schema.length)
    while (nextUntil(parser, JsonToken.END_OBJECT)) {
      schema.getFieldIndex(parser.getCurrentName) match {
        case Some(index) =>
          row.update(index, convertField(factory, parser, schema(index).dataType))

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
      factory: JsonFactory,
      parser: JsonParser,
      valueType: DataType): MapData = {
    val keys = ArrayBuffer.empty[UTF8String]
    val values = ArrayBuffer.empty[Any]
    while (nextUntil(parser, JsonToken.END_OBJECT)) {
      keys += UTF8String.fromString(parser.getCurrentName)
      values += convertField(factory, parser, valueType)
    }
    ArrayBasedMapData(keys.toArray, values.toArray)
  }

  private def convertArray(
      factory: JsonFactory,
      parser: JsonParser,
      elementType: DataType): ArrayData = {
    val values = ArrayBuffer.empty[Any]
    while (nextUntil(parser, JsonToken.END_ARRAY)) {
      values += convertField(factory, parser, elementType)
    }

    new GenericArrayData(values.toArray)
  }

  private def parseJson(
      json: RDD[String],
      schema: StructType,
      columnNameOfCorruptRecords: String): RDD[InternalRow] = {

    def failedRecord(record: String): Seq[InternalRow] = {
      // create a row even if no corrupt record column is present
      val row = new GenericMutableRow(schema.length)
      for (corruptIndex <- schema.getFieldIndex(columnNameOfCorruptRecords)) {
        require(schema(corruptIndex).dataType == StringType)
        row.update(corruptIndex, UTF8String.fromString(record))
      }

      Seq(row)
    }

    json.mapPartitions { iter =>
      val factory = new JsonFactory()

      iter.flatMap { record =>
        try {
          val parser = factory.createParser(record)
          parser.nextToken()

          convertField(factory, parser, schema) match {
            case null => failedRecord(record)
            case row: InternalRow => row :: Nil
            case array: ArrayData =>
              if (array.numElements() == 0) {
                Nil
              } else {
                array.toArray[InternalRow](schema)
              }
            case _ =>
              sys.error(
                s"Failed to parse record $record. Please make sure that each line of the file " +
                  "(or each string in the RDD) is a valid JSON object or an array of JSON objects.")
          }
        } catch {
          case _: JsonProcessingException =>
            failedRecord(record)
        }
      }
    }
  }
}
