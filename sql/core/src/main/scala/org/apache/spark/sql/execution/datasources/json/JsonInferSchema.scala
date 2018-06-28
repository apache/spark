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

import java.util.Comparator

import com.fasterxml.jackson.core._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.catalyst.json.JacksonUtils.nextUntil
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.catalyst.util.{DropMalformedMode, FailFastMode, ParseMode, PermissiveMode}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

private[sql] object JsonInferSchema {

  /**
   * Infer the type of a collection of json records in three stages:
   *   1. Infer the type of each record
   *   2. Merge types by choosing the lowest type necessary to cover equal keys
   *   3. Replace any remaining null fields with string, the top type
   */
  def infer[T](
      json: RDD[T],
      configOptions: JSONOptions,
      createParser: (JsonFactory, T) => JsonParser): StructType = {
    val parseMode = configOptions.parseMode
    val columnNameOfCorruptRecord = configOptions.columnNameOfCorruptRecord

    // perform schema inference on each row and merge afterwards
    val rootType = json.mapPartitions { iter =>
      val factory = new JsonFactory()
      configOptions.setJacksonOptions(factory)
      iter.flatMap { row =>
        try {
          Utils.tryWithResource(createParser(factory, row)) { parser =>
            parser.nextToken()
            Some(inferField(parser, configOptions))
          }
        } catch {
          case  e @ (_: RuntimeException | _: JsonProcessingException) => parseMode match {
            case PermissiveMode =>
              Some(StructType(Seq(StructField(columnNameOfCorruptRecord, StringType))))
            case DropMalformedMode =>
              None
            case FailFastMode =>
              throw e
          }
        }
      }
    }.fold(StructType(Nil))(
      compatibleRootType(columnNameOfCorruptRecord, parseMode))

    canonicalizeType(rootType) match {
      case Some(st: StructType) => st
      case _ =>
        // canonicalizeType erases all empty structs, including the only one we want to keep
        StructType(Nil)
    }
  }

  private[this] val structFieldComparator = new Comparator[StructField] {
    override def compare(o1: StructField, o2: StructField): Int = {
      o1.name.compareTo(o2.name)
    }
  }

  private def isSorted(arr: Array[StructField]): Boolean = {
    var i: Int = 0
    while (i < arr.length - 1) {
      if (structFieldComparator.compare(arr(i), arr(i + 1)) > 0) {
        return false
      }
      i += 1
    }
    true
  }

  /**
   * Infer the type of a json document from the parser's token stream
   */
  private def inferField(parser: JsonParser, configOptions: JSONOptions): DataType = {
    import com.fasterxml.jackson.core.JsonToken._
    parser.getCurrentToken match {
      case null | VALUE_NULL => NullType

      case FIELD_NAME =>
        parser.nextToken()
        inferField(parser, configOptions)

      case VALUE_STRING if parser.getTextLength < 1 =>
        // Zero length strings and nulls have special handling to deal
        // with JSON generators that do not distinguish between the two.
        // To accurately infer types for empty strings that are really
        // meant to represent nulls we assume that the two are isomorphic
        // but will defer treating null fields as strings until all the
        // record fields' types have been combined.
        NullType

      case VALUE_STRING => StringType
      case START_OBJECT =>
        val builder = Array.newBuilder[StructField]
        while (nextUntil(parser, END_OBJECT)) {
          builder += StructField(
            parser.getCurrentName,
            inferField(parser, configOptions),
            nullable = true)
        }
        val fields: Array[StructField] = builder.result()
        // Note: other code relies on this sorting for correctness, so don't remove it!
        java.util.Arrays.sort(fields, structFieldComparator)
        StructType(fields)

      case START_ARRAY =>
        // If this JSON array is empty, we use NullType as a placeholder.
        // If this array is not empty in other JSON objects, we can resolve
        // the type as we pass through all JSON objects.
        var elementType: DataType = NullType
        while (nextUntil(parser, END_ARRAY)) {
          elementType = compatibleType(
            elementType, inferField(parser, configOptions))
        }

        ArrayType(elementType)

      case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT) if configOptions.primitivesAsString => StringType

      case (VALUE_TRUE | VALUE_FALSE) if configOptions.primitivesAsString => StringType

      case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT =>
        import JsonParser.NumberType._
        parser.getNumberType match {
          // For Integer values, use LongType by default.
          case INT | LONG => LongType
          // Since we do not have a data type backed by BigInteger,
          // when we see a Java BigInteger, we use DecimalType.
          case BIG_INTEGER | BIG_DECIMAL =>
            val v = parser.getDecimalValue
            if (Math.max(v.precision(), v.scale()) <= DecimalType.MAX_PRECISION) {
              DecimalType(Math.max(v.precision(), v.scale()), v.scale())
            } else {
              DoubleType
            }
          case FLOAT | DOUBLE if configOptions.prefersDecimal =>
            val v = parser.getDecimalValue
            if (Math.max(v.precision(), v.scale()) <= DecimalType.MAX_PRECISION) {
              DecimalType(Math.max(v.precision(), v.scale()), v.scale())
            } else {
              DoubleType
            }
          case FLOAT | DOUBLE =>
            DoubleType
        }

      case VALUE_TRUE | VALUE_FALSE => BooleanType
    }
  }

  /**
   * Convert NullType to StringType and remove StructTypes with no fields
   */
  private def canonicalizeType(tpe: DataType): Option[DataType] = tpe match {
    case at @ ArrayType(elementType, _) =>
      for {
        canonicalType <- canonicalizeType(elementType)
      } yield {
        at.copy(canonicalType)
      }

    case StructType(fields) =>
      val canonicalFields: Array[StructField] = for {
        field <- fields
        if field.name.length > 0
        canonicalType <- canonicalizeType(field.dataType)
      } yield {
        field.copy(dataType = canonicalType)
      }

      if (canonicalFields.length > 0) {
        Some(StructType(canonicalFields))
      } else {
        // per SPARK-8093: empty structs should be deleted
        None
      }

    case NullType => Some(StringType)
    case other => Some(other)
  }

  private def withCorruptField(
      struct: StructType,
      other: DataType,
      columnNameOfCorruptRecords: String,
      parseMode: ParseMode) = parseMode match {
    case PermissiveMode =>
      // If we see any other data type at the root level, we get records that cannot be
      // parsed. So, we use the struct as the data type and add the corrupt field to the schema.
      if (!struct.fieldNames.contains(columnNameOfCorruptRecords)) {
        // If this given struct does not have a column used for corrupt records,
        // add this field.
        val newFields: Array[StructField] =
          StructField(columnNameOfCorruptRecords, StringType, nullable = true) +: struct.fields
        // Note: other code relies on this sorting for correctness, so don't remove it!
        java.util.Arrays.sort(newFields, structFieldComparator)
        StructType(newFields)
      } else {
        // Otherwise, just return this struct.
        struct
      }

    case DropMalformedMode =>
      // If corrupt record handling is disabled we retain the valid schema and discard the other.
      struct

    case FailFastMode =>
      // If `other` is not struct type, consider it as malformed one and throws an exception.
      throw new RuntimeException("Failed to infer a common schema. Struct types are expected" +
        s" but ${other.catalogString} was found.")
  }

  /**
   * Remove top-level ArrayType wrappers and merge the remaining schemas
   */
  private def compatibleRootType(
      columnNameOfCorruptRecords: String,
      parseMode: ParseMode): (DataType, DataType) => DataType = {
    // Since we support array of json objects at the top level,
    // we need to check the element type and find the root level data type.
    case (ArrayType(ty1, _), ty2) =>
      compatibleRootType(columnNameOfCorruptRecords, parseMode)(ty1, ty2)
    case (ty1, ArrayType(ty2, _)) =>
      compatibleRootType(columnNameOfCorruptRecords, parseMode)(ty1, ty2)
    // Discard null/empty documents
    case (struct: StructType, NullType) => struct
    case (NullType, struct: StructType) => struct
    case (struct: StructType, o) if !o.isInstanceOf[StructType] =>
      withCorruptField(struct, o, columnNameOfCorruptRecords, parseMode)
    case (o, struct: StructType) if !o.isInstanceOf[StructType] =>
      withCorruptField(struct, o, columnNameOfCorruptRecords, parseMode)
    // If we get anything else, we call compatibleType.
    // Usually, when we reach here, ty1 and ty2 are two StructTypes.
    case (ty1, ty2) => compatibleType(ty1, ty2)
  }

  private[this] val emptyStructFieldArray = Array.empty[StructField]

  /**
   * Returns the most general data type for two given data types.
   */
  def compatibleType(t1: DataType, t2: DataType): DataType = {
    TypeCoercion.findTightestCommonType(t1, t2).getOrElse {
      // t1 or t2 is a StructType, ArrayType, or an unexpected type.
      (t1, t2) match {
        // Double support larger range than fixed decimal, DecimalType.Maximum should be enough
        // in most case, also have better precision.
        case (DoubleType, _: DecimalType) | (_: DecimalType, DoubleType) =>
          DoubleType

        case (t1: DecimalType, t2: DecimalType) =>
          val scale = math.max(t1.scale, t2.scale)
          val range = math.max(t1.precision - t1.scale, t2.precision - t2.scale)
          if (range + scale > 38) {
            // DecimalType can't support precision > 38
            DoubleType
          } else {
            DecimalType(range + scale, scale)
          }

        case (StructType(fields1), StructType(fields2)) =>
          // Both fields1 and fields2 should be sorted by name, since inferField performs sorting.
          // Therefore, we can take advantage of the fact that we're merging sorted lists and skip
          // building a hash map or performing additional sorting.
          assert(isSorted(fields1), s"StructType's fields were not sorted: ${fields1.toSeq}")
          assert(isSorted(fields2), s"StructType's fields were not sorted: ${fields2.toSeq}")

          val newFields = new java.util.ArrayList[StructField]()

          var f1Idx = 0
          var f2Idx = 0

          while (f1Idx < fields1.length && f2Idx < fields2.length) {
            val f1Name = fields1(f1Idx).name
            val f2Name = fields2(f2Idx).name
            val comp = f1Name.compareTo(f2Name)
            if (comp == 0) {
              val dataType = compatibleType(fields1(f1Idx).dataType, fields2(f2Idx).dataType)
              newFields.add(StructField(f1Name, dataType, nullable = true))
              f1Idx += 1
              f2Idx += 1
            } else if (comp < 0) { // f1Name < f2Name
              newFields.add(fields1(f1Idx))
              f1Idx += 1
            } else { // f1Name > f2Name
              newFields.add(fields2(f2Idx))
              f2Idx += 1
            }
          }
          while (f1Idx < fields1.length) {
            newFields.add(fields1(f1Idx))
            f1Idx += 1
          }
          while (f2Idx < fields2.length) {
            newFields.add(fields2(f2Idx))
            f2Idx += 1
          }
          StructType(newFields.toArray(emptyStructFieldArray))

        case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
          ArrayType(compatibleType(elementType1, elementType2), containsNull1 || containsNull2)

        // The case that given `DecimalType` is capable of given `IntegralType` is handled in
        // `findTightestCommonType`. Both cases below will be executed only when the given
        // `DecimalType` is not capable of the given `IntegralType`.
        case (t1: IntegralType, t2: DecimalType) =>
          compatibleType(DecimalType.forType(t1), t2)
        case (t1: DecimalType, t2: IntegralType) =>
          compatibleType(t1, DecimalType.forType(t2))

        // strings and every string is a Json object.
        case (_, _) => StringType
      }
    }
  }
}
