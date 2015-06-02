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

package org.apache.spark.sql.json

import java.io.ByteArrayOutputStream
import java.sql.Timestamp

import scala.collection.Map

import com.fasterxml.jackson.core._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateUtils
import org.apache.spark.sql.json.JacksonUtils.nextUntil
import org.apache.spark.sql.types._

private[sql] object JacksonParser {
  def apply(
      json: RDD[String],
      schema: StructType,
      columnNameOfCorruptRecords: String): RDD[Row] = {
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
        UTF8String(parser.getText)

      case (VALUE_STRING, _) if parser.getTextLength < 1 =>
        // guard the non string type
        null

      case (VALUE_STRING, DateType) =>
        DateUtils.millisToDays(DateUtils.stringToTime(parser.getText).getTime)

      case (VALUE_STRING, TimestampType) =>
        new Timestamp(DateUtils.stringToTime(parser.getText).getTime)

      case (VALUE_NUMBER_INT, TimestampType) =>
        new Timestamp(parser.getLongValue)

      case (_, StringType) =>
        val writer = new ByteArrayOutputStream()
        val generator = factory.createGenerator(writer, JsonEncoding.UTF8)
        generator.copyCurrentStructure(parser)
        generator.close()
        UTF8String(writer.toByteArray)

      case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT, FloatType) =>
        parser.getFloatValue

      case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT, DoubleType) =>
        parser.getDoubleValue

      case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT, DecimalType()) =>
        // TODO: add fixed precision and scale handling
        Decimal(parser.getDecimalValue)

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

      case (START_ARRAY, ArrayType(st, _)) =>
        convertList(factory, parser, st)

      case (START_OBJECT, ArrayType(st, _)) =>
        // the business end of SPARK-3308:
        // when an object is found but an array is requested just wrap it in a list
        convertField(factory, parser, st) :: Nil

      case (START_OBJECT, MapType(StringType, kt, _)) =>
        convertMap(factory, parser, kt)

      case (_, udt: UserDefinedType[_]) =>
        udt.deserialize(convertField(factory, parser, udt.sqlType))
    }
  }

  /**
   * Parse an object from the token stream into a new Row representing the schema.
   *
   * Fields in the json that are not defined in the requested schema will be dropped.
   */
  private def convertObject(factory: JsonFactory, parser: JsonParser, schema: StructType): Row = {
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
      valueType: DataType): Map[UTF8String, Any] = {
    val builder = Map.newBuilder[UTF8String, Any]
    while (nextUntil(parser, JsonToken.END_OBJECT)) {
      builder += UTF8String(parser.getCurrentName) -> convertField(factory, parser, valueType)
    }

    builder.result()
  }

  private def convertList(
      factory: JsonFactory,
      parser: JsonParser,
      schema: DataType): Seq[Any] = {
    val builder = Seq.newBuilder[Any]
    while (nextUntil(parser, JsonToken.END_ARRAY)) {
      builder += convertField(factory, parser, schema)
    }

    builder.result()
  }

  private def parseJson(
      json: RDD[String],
      schema: StructType,
      columnNameOfCorruptRecords: String): RDD[Row] = {

    def failedRecord(record: String): Seq[Row] = {
      // create a row even if no corrupt record column is present
      val row = new GenericMutableRow(schema.length)
      for (corruptIndex <- schema.getFieldIndex(columnNameOfCorruptRecords)) {
        require(schema(corruptIndex).dataType == StringType)
        row.update(corruptIndex, UTF8String(record))
      }

      Seq(row)
    }

    json.mapPartitions { iter =>
      val factory = new JsonFactory()

      iter.flatMap { record =>
        try {
          val parser = factory.createParser(record)
          parser.nextToken()

          // to support both object and arrays (see SPARK-3308) we'll start
          // by converting the StructType schema to an ArrayType and let
          // convertField wrap an object into a single value array when necessary.
          convertField(factory, parser, ArrayType(schema)) match {
            case null => failedRecord(record)
            case list: Seq[Row @unchecked] => list
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
