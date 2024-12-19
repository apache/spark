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

import java.io.{ByteArrayOutputStream, CharArrayWriter}

import com.fasterxml.jackson.core._

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExprUtils, GenericInternalRow, SharedFactory}
import org.apache.spark.sql.catalyst.expressions.variant.VariantExpressionEvalUtils
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JacksonGenerator, JacksonParser, JsonInferSchema, JSONOptions}
import org.apache.spark.sql.catalyst.util.{ArrayData, FailFastMode, FailureSafeParser, MapData, PermissiveMode}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType, VariantType}
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}
import org.apache.spark.util.Utils

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
    nullableSchema match {
      case _: VariantType =>
        VariantExpressionEvalUtils.parseJson(json,
          allowDuplicateKeys = variantAllowDuplicateKeys)
      case _ =>
        converter(parser.parse(json))
    }
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
            SQLConf.get.defaultStringType)
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
