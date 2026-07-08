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

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.types.variant.{Variant, VariantBuilder, VariantPathTypeMismatchException, VariantSizeLimitException, VariantUtil}
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

/**
 * A utility class for constructing variant expressions.
 */
object VariantExpressionEvalUtils {

  def parseJson(
      input: UTF8String,
      allowDuplicateKeys: Boolean = false,
      failOnError: Boolean = true,
      validateUnicodeInJsonParsing: Boolean = true): VariantVal = {
    def parseJsonFailure(exception: Throwable): VariantVal = {
      if (failOnError) {
        throw exception
      } else {
        null
      }
    }
    try {
      val v = VariantBuilder.parseJson(
        input.toString, allowDuplicateKeys, validateUnicodeInJsonParsing)
      new VariantVal(v.getValue, v.getMetadata)
    } catch {
      case _: VariantSizeLimitException =>
        parseJsonFailure(QueryExecutionErrors
          .variantSizeLimitError(VariantUtil.SIZE_LIMIT, "parse_json"))
      case NonFatal(e) =>
        parseJsonFailure(QueryExecutionErrors.malformedRecordsDetectedInRecordParsingError(
          input.toString, e))
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

  def isValidVariant(input: VariantVal): Boolean =
    VariantUtil.isValidVariant(input.getValue, input.getMetadata)

  /**
   * Parse a JSONPath for a variant manipulation function. Throws `INVALID_VARIANT_PATH` on a
   * malformed path or on the empty (root `$`) path.
   */
  def parseVariantPath(pathValue: String, functionName: String): Array[VariantPathSegment] = {
    val parsed = VariantPathParser.parse(pathValue).getOrElse {
      throw QueryExecutionErrors.invalidVariantPath(pathValue, functionName)
    }
    if (parsed.isEmpty) {
      throw QueryExecutionErrors.invalidVariantPath(pathValue, functionName)
    }
    parsed
  }

  /** Render a parsed path prefix back to a JSONPath string for error messages. */
  private def renderVariantPath(segments: Array[VariantBuilder.PathSegment]): String = {
    val sb = new StringBuilder("$")
    segments.foreach {
      case o: VariantBuilder.ObjectKeySegment =>
        val key = o.key
        // Dot notation only parses keys with no `.` or `[` (and at least one char); anything else
        // must use bracket notation so the rendered path round-trips to the same segments.
        if (key.nonEmpty && !key.contains('.') && !key.contains('[')) {
          sb.append('.').append(key)
        } else if (!key.contains('\'')) {
          sb.append("['").append(key).append("']")
        } else {
          sb.append("[\"").append(key).append("\"]")
        }
      case a: VariantBuilder.ArrayIndexSegment => sb.append('[').append(a.index).append(']')
    }
    sb.toString
  }

  def toJavaSegments(
      segments: Array[VariantPathSegment]): Array[VariantBuilder.PathSegment] = {
    segments.map {
      case ObjectExtraction(key) => new VariantBuilder.ObjectKeySegment(key)
      case ArrayExtraction(index) => new VariantBuilder.ArrayIndexSegment(index)
    }
  }

  def deleteAtPath(
      input: VariantVal,
      javaSegments: Array[VariantBuilder.PathSegment]): VariantVal = {
    val v = new Variant(input.getValue, input.getMetadata)
    val out = VariantBuilder.deleteAtPath(v, javaSegments)
    new VariantVal(out.getValue, out.getMetadata)
  }

  def deleteAtPath(input: VariantVal, path: UTF8String): VariantVal =
    deleteAtPath(input, toJavaSegments(parseVariantPath(path.toString, "variant_delete")))

  /**
   * Insert `value` into `input` at `javaSegments`. `path` is the source string used in error
   * messages. The cast and insert share one try, so any size overflow maps to `VARIANT_SIZE_LIMIT`
   * and a type mismatch maps to `VARIANT_PATH_TYPE_MISMATCH`. When `failOnError` is false (the
   * `try_variant_insert` mode), a duplicate key or path type mismatch returns null instead of
   * throwing; a size overflow (and a malformed path, rejected earlier during parsing) is still
   * raised.
   */
  def insertAtPath(
      input: VariantVal,
      javaSegments: Array[VariantBuilder.PathSegment],
      path: String,
      value: Any,
      valueDataType: DataType,
      functionName: String,
      failOnError: Boolean): VariantVal = {
    val v = new Variant(input.getValue, input.getMetadata)
    try {
      val valVal = castToVariant(value, valueDataType)
      val valVariant = new Variant(valVal.getValue, valVal.getMetadata)
      val out = VariantBuilder.insertAtPath(v, javaSegments, valVariant)
      new VariantVal(out.getValue, out.getMetadata)
    } catch {
      case _: VariantPathTypeMismatchException if !failOnError => null
      case e: VariantPathTypeMismatchException =>
        throw QueryExecutionErrors.variantPathTypeMismatch(
          path, renderVariantPath(javaSegments.take(e.depth)), functionName)
      case _: VariantSizeLimitException =>
        throw QueryExecutionErrors.variantSizeLimitError(VariantUtil.SIZE_LIMIT, functionName)
      case e: SparkRuntimeException if !failOnError && e.getCondition == "VARIANT_DUPLICATE_KEY" =>
        null
    }
  }

  def insertAtPath(
      input: VariantVal,
      path: UTF8String,
      value: Any,
      valueDataType: DataType,
      functionName: String,
      failOnError: Boolean): VariantVal = {
    val pathStr = path.toString
    val javaSegments = toJavaSegments(parseVariantPath(pathStr, functionName))
    insertAtPath(input, javaSegments, pathStr, value, valueDataType, functionName, failOnError)
  }

  /** Cast a Spark value from `dataType` into the variant type. */
  def castToVariant(input: Any, dataType: DataType): VariantVal = {
    // Enforce strict check because it is illegal for input struct/map/variant to contain duplicate
    // keys.
    val builder = new VariantBuilder(false)
    buildVariant(builder, input, dataType)
    val v = builder.result()
    new VariantVal(v.getValue, v.getMetadata)
  }

  /** Returns `true` if a data type is or has a child variant type. */
  def typeContainsVariant(dt: DataType): Boolean = dt match {
    case _: VariantType => true
    case st: StructType => st.fields.exists(f => typeContainsVariant(f.dataType))
    case at: ArrayType => typeContainsVariant(at.elementType)
    // Variants cannot be map keys.
    case mt: MapType => typeContainsVariant(mt.valueType)
    case _ => false
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
          val element = if (data.isNullAt(i)) null else data.get(i, elementType)
          buildVariant(builder, element, elementType)
        }
        builder.finishWritingArray(start, offsets)
      case MapType(_: StringType, valueType, _) =>
        val data = input.asInstanceOf[MapData]
        val keys = data.keyArray()
        val values = data.valueArray()
        val start = builder.getWritePos
        val fields = new java.util.ArrayList[VariantBuilder.FieldEntry](data.numElements())
        for (i <- 0 until data.numElements()) {
          val key = keys.getUTF8String(i).toString
          val id = builder.addKey(key)
          fields.add(new VariantBuilder.FieldEntry(key, id, builder.getWritePos - start))
          val value = if (values.isNullAt(i)) null else values.get(i, valueType)
          buildVariant(builder, value, valueType)
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
          val value = if (data.isNullAt(i)) null else data.get(i, structFields(i).dataType)
          buildVariant(builder, value, structFields(i).dataType)
        }
        builder.finishWritingObject(start, fields)
    }
  }
}
