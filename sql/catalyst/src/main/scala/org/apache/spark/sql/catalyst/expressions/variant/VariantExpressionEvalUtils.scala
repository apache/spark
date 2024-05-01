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

package org.apache.spark.sql.catalyst.expressions.variant

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, BadRecordException, MapData}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.types.variant.{Variant, VariantBuilder, VariantSizeLimitException, VariantUtil}
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

/**
 * A utility class for constructing variant expressions.
 */
object VariantExpressionEvalUtils {

  def parseJson(input: UTF8String, failOnError: Boolean = true): VariantVal = {
    def parseJsonFailure(exception: Throwable): VariantVal = {
      if (failOnError) {
        throw exception
      } else {
        null
      }
    }
    try {
      val v = VariantBuilder.parseJson(input.toString)
      new VariantVal(v.getValue, v.getMetadata)
    } catch {
      case _: VariantSizeLimitException =>
        parseJsonFailure(QueryExecutionErrors
          .variantSizeLimitError(VariantUtil.SIZE_LIMIT, "parse_json"))
      case NonFatal(e) =>
        parseJsonFailure(QueryExecutionErrors.malformedRecordsDetectedInRecordParsingError(
          input.toString, BadRecordException(() => input, cause = e)))
    }
  }

  def isVariantNull(input: VariantVal): Boolean = {
    if (input == null) {
      // This is a SQL NULL, not a Variant NULL
      false
    } else {
      val variantValue = input.getValue
      if (variantValue.isEmpty) {
        throw QueryExecutionErrors.malformedVariant()
      } else {
        // Variant NULL is denoted by basic_type == 0 and val_header == 0
        variantValue(0) == 0
      }
    }
  }

  /** Cast a Spark value from `dataType` into the variant type. */
  def castToVariant(input: Any, dataType: DataType): VariantVal = {
    val builder = new VariantBuilder
    buildVariant(builder, input, dataType)
    val v = builder.result()
    new VariantVal(v.getValue, v.getMetadata)
  }

  private def buildVariant(builder: VariantBuilder, input: Any, dataType: DataType): Unit = {
    if (input == null) {
      builder.appendNull()
      return
    }
    dataType match {
      case BooleanType => builder.appendBoolean(input.asInstanceOf[Boolean])
      case ByteType => builder.appendLong(input.asInstanceOf[Byte])
      case ShortType => builder.appendLong(input.asInstanceOf[Short])
      case IntegerType => builder.appendLong(input.asInstanceOf[Int])
      case LongType => builder.appendLong(input.asInstanceOf[Long])
      case FloatType => builder.appendFloat(input.asInstanceOf[Float])
      case DoubleType => builder.appendDouble(input.asInstanceOf[Double])
      case _: DecimalType => builder.appendDecimal(input.asInstanceOf[Decimal].toJavaBigDecimal)
      case _: StringType => builder.appendString(input.asInstanceOf[UTF8String].toString)
      case BinaryType => builder.appendBinary(input.asInstanceOf[Array[Byte]])
      case DateType => builder.appendDate(input.asInstanceOf[Int])
      case TimestampType => builder.appendTimestamp(input.asInstanceOf[Long])
      case TimestampNTZType => builder.appendTimestampNtz(input.asInstanceOf[Long])
      case VariantType =>
        val v = input.asInstanceOf[VariantVal]
        builder.appendVariant(new Variant(v.getValue, v.getMetadata))
      case ArrayType(elementType, _) =>
        val data = input.asInstanceOf[ArrayData]
        val start = builder.getWritePos
        val offsets = new java.util.ArrayList[java.lang.Integer](data.numElements())
        for (i <- 0 until data.numElements()) {
          offsets.add(builder.getWritePos - start)
          buildVariant(builder, data.get(i, elementType), elementType)
        }
        builder.finishWritingArray(start, offsets)
      case MapType(StringType, valueType, _) =>
        val data = input.asInstanceOf[MapData]
        val keys = data.keyArray()
        val values = data.valueArray()
        val start = builder.getWritePos
        val fields = new java.util.ArrayList[VariantBuilder.FieldEntry](data.numElements())
        for (i <- 0 until data.numElements()) {
          val key = keys.getUTF8String(i).toString
          val id = builder.addKey(key)
          fields.add(new VariantBuilder.FieldEntry(key, id, builder.getWritePos - start))
          buildVariant(builder, values.get(i, valueType), valueType)
        }
        builder.finishWritingObject(start, fields)
      case StructType(structFields) =>
        val data = input.asInstanceOf[InternalRow]
        val start = builder.getWritePos
        val fields = new java.util.ArrayList[VariantBuilder.FieldEntry](structFields.length)
        for (i <- 0 until structFields.length) {
          val key = structFields(i).name
          val id = builder.addKey(key)
          fields.add(new VariantBuilder.FieldEntry(key, id, builder.getWritePos - start))
          buildVariant(builder, data.get(i, structFields(i).dataType), structFields(i).dataType)
        }
        builder.finishWritingObject(start, fields)
    }
  }
}
