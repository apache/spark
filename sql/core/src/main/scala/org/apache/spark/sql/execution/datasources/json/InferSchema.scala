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

import com.fasterxml.jackson.core._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion
import org.apache.spark.sql.execution.datasources.json.JacksonUtils.nextUntil
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


private[json] object InferSchema {

  /**
   * Infer the type of a collection of json records in three stages:
   *   1. Infer the type of each record
   *   2. Merge types by choosing the lowest type necessary to cover equal keys
   *   3. Replace any remaining null fields with string, the top type
   */
  def infer(
      json: RDD[String],
      columnNameOfCorruptRecords: String,
      configOptions: JSONOptions): StructType = {
    require(configOptions.samplingRatio > 0,
      s"samplingRatio (${configOptions.samplingRatio}) should be greater than 0")
    val schemaData = if (configOptions.samplingRatio > 0.99) {
      json
    } else {
      json.sample(withReplacement = false, configOptions.samplingRatio, 1)
    }

    // perform schema inference on each row and merge afterwards
    val rootType = schemaData.mapPartitions { iter =>
      val factory = new JsonFactory()
      configOptions.setJacksonOptions(factory)
      iter.map { row =>
        try {
          Utils.tryWithResource(factory.createParser(row)) { parser =>
            parser.nextToken()
            inferField(parser, configOptions)
          }
        } catch {
          case _: JsonParseException =>
            StructType(Seq(StructField(columnNameOfCorruptRecords, StringType)))
        }
      }
    }.treeAggregate[DataType](
      StructType(Seq()))(
      compatibleRootType(columnNameOfCorruptRecords),
      compatibleRootType(columnNameOfCorruptRecords))

    canonicalizeType(rootType) match {
      case Some(st: StructType) => st
      case _ =>
        // canonicalizeType erases all empty structs, including the only one we want to keep
        StructType(Seq())
    }
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
        val builder = Seq.newBuilder[StructField]
        while (nextUntil(parser, END_OBJECT)) {
          builder += StructField(
            parser.getCurrentName,
            inferField(parser, configOptions),
            nullable = true)
        }

        StructType(builder.result().sortBy(_.name))

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
            DecimalType(v.precision(), v.scale())
          case FLOAT | DOUBLE =>
            // TODO(davies): Should we use decimal if possible?
            DoubleType
        }

      case VALUE_TRUE | VALUE_FALSE => BooleanType
    }
  }

  /**
   * Convert NullType to StringType and remove StructTypes with no fields
   */
  private def canonicalizeType: DataType => Option[DataType] = {
    case at @ ArrayType(elementType, _) =>
      for {
        canonicalType <- canonicalizeType(elementType)
      } yield {
        at.copy(canonicalType)
      }

    case StructType(fields) =>
      val canonicalFields = for {
        field <- fields
        if field.name.nonEmpty
        canonicalType <- canonicalizeType(field.dataType)
      } yield {
        field.copy(dataType = canonicalType)
      }

      if (canonicalFields.nonEmpty) {
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
      columnNameOfCorruptRecords: String): StructType = {
    if (!struct.fieldNames.contains(columnNameOfCorruptRecords)) {
      // If this given struct does not have a column used for corrupt records,
      // add this field.
      struct.add(columnNameOfCorruptRecords, StringType, nullable = true)
    } else {
      // Otherwise, just return this struct.
      struct
    }
  }

  /**
   * Remove top-level ArrayType wrappers and merge the remaining schemas
   */
  private def compatibleRootType(
      columnNameOfCorruptRecords: String): (DataType, DataType) => DataType = {
    // Since we support array of json objects at the top level,
    // we need to check the element type and find the root level data type.
    case (ArrayType(ty1, _), ty2) => compatibleRootType(columnNameOfCorruptRecords)(ty1, ty2)
    case (ty1, ArrayType(ty2, _)) => compatibleRootType(columnNameOfCorruptRecords)(ty1, ty2)
    // If we see any other data type at the root level, we get records that cannot be
    // parsed. So, we use the struct as the data type and add the corrupt field to the schema.
    case (struct: StructType, NullType) => struct
    case (NullType, struct: StructType) => struct
    case (struct: StructType, o) if !o.isInstanceOf[StructType] =>
      withCorruptField(struct, columnNameOfCorruptRecords)
    case (o, struct: StructType) if !o.isInstanceOf[StructType] =>
      withCorruptField(struct, columnNameOfCorruptRecords)
    // If we get anything else, we call compatibleType.
    // Usually, when we reach here, ty1 and ty2 are two StructTypes.
    case (ty1, ty2) => compatibleType(ty1, ty2)
  }

  /**
   * Returns the most general data type for two given data types.
   */
  def compatibleType(t1: DataType, t2: DataType): DataType = {
    HiveTypeCoercion.findTightestCommonTypeOfTwo(t1, t2).getOrElse {
      // t1 or t2 is a StructType, ArrayType, or an unexpected type.
      (t1, t2) match {
        // Double support larger range than fixed decimal, DecimalType.Maximum should be enough
        // in most case, also have better precision.
        case (DoubleType, t: DecimalType) =>
          DoubleType
        case (t: DecimalType, DoubleType) =>
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
          val newFields = (fields1 ++ fields2).groupBy(field => field.name).map {
            case (name, fieldTypes) =>
              val dataType = fieldTypes.view.map(_.dataType).reduce(compatibleType)
              StructField(name, dataType, nullable = true)
          }
          StructType(newFields.toSeq.sortBy(_.name))

        case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
          ArrayType(compatibleType(elementType1, elementType2), containsNull1 || containsNull2)

        // strings and every string is a Json object.
        case (_, _) => StringType
      }
    }
  }
}
