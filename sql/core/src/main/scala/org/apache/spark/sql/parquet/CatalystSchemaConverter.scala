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

package org.apache.spark.sql.parquet

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema.OriginalType._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition._
import org.apache.parquet.schema.{GroupType, MessageType, PrimitiveType => ParquetPrimitiveType, Type, Types}

import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, SQLConf}

/**
 * This converter class is used to convert Parquet [[MessageType]] to Spark SQL [[StructType]] and
 * vice versa.
 *
 * Parquet format backwards-compatibility rules are respected when converting Parquet
 * [[MessageType]] schemas.
 *
 * @see https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
 *
 * @constructor
 * @param assumeInt96IsTimestamp Whether unannotated INT96 fields should be assumed to be timestamp
 *        fields when converting Parquet a [[MessageType]] to Spark SQL [[StructType]].
 * @param assumeBinaryIsString Whether unannotated BINARY fields should be assumed to be string
 *        fields when converting Parquet a [[MessageType]] to Spark SQL [[StructType]].
 */
private[parquet] class CatalystSchemaConverter(
    private val assumeInt96IsTimestamp: Boolean,
    private val assumeBinaryIsString: Boolean) {

  // Only used when constructing converter for converting Spark SQL schema to Parquet schema, in
  // which case `assumeInt96IsTimestamp` and `assumeBinaryIsString` are irrelevant.
  def this() = this(assumeInt96IsTimestamp = true, assumeBinaryIsString = true)

  def this(conf: SQLConf) = this(
    conf.isParquetINT96AsTimestamp,
    conf.isParquetBinaryAsString)

  def this(conf: Configuration) = this(
    conf.getBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP, true),
    conf.getBoolean(SQLConf.PARQUET_BINARY_AS_STRING, true))

  /**
   * Converts Parquet [[MessageType]] `parquetSchema` to a Spark SQL [[StructType]].
   */
  def convert(parquetSchema: MessageType): StructType = convert(parquetSchema.asGroupType())

  private def convert(parquetSchema: GroupType): StructType = {
    val fields = parquetSchema.getFields.map { field =>
      field.getRepetition match {
        case OPTIONAL =>
          StructField(field.getName, convertField(field), nullable = true)

        case REQUIRED =>
          StructField(field.getName, convertField(field), nullable = false)

        case REPEATED =>
          throw new UnsupportedOperationException(
            s"REPEATED not supported outside LIST or MAP. Type: $field")
      }
    }

    StructType(fields)
  }

  /**
   * Converts a Parquet [[Type]] to a Spark SQL [[DataType]].
   */
  def convertField(parquetType: Type): DataType = parquetType match {
    case t: ParquetPrimitiveType => convertPrimitiveField(t)
    case t: GroupType => convertGroupField(t.asGroupType())
  }

  private def convertPrimitiveField(field: ParquetPrimitiveType): DataType = {
    val typeName = field.getPrimitiveTypeName
    val originalType = field.getOriginalType

    def typeNotImplemented() =
      throw new UnsupportedOperationException(s"Not yet implemented: $typeName ($originalType)")

    def illegalType() =
      throw new IllegalArgumentException(s"Illegal type: $typeName ($originalType)")

    // When maxPrecision = -1, we skip precision range check, and always respect the precision
    // specified in field.getDecimalMetadata.  This is useful when interpreting decimal types stored
    // as binaries with variable lengths.
    def makeDecimalType(maxPrecision: Long = -1): DecimalType = {
      val precision = field.getDecimalMetadata.getPrecision
      val scale = field.getDecimalMetadata.getScale

      CatalystSchemaConverter.analysisRequire(
        maxPrecision == -1 || 1 <= precision && precision <= maxPrecision,
        s"Invalid decimal precision: $typeName cannot store $precision digits (max $maxPrecision)")

      DecimalType(precision, scale)
    }

    field.getPrimitiveTypeName match {
      case BOOLEAN => BooleanType

      case FLOAT => FloatType

      case DOUBLE => DoubleType

      case INT32 =>
        field.getOriginalType match {
          case INT_8 => ByteType
          case INT_16 => ShortType
          case INT_32 | null => IntegerType
          case DATE => DateType
          case DECIMAL => makeDecimalType(maxPrecision(4))
          case TIME_MILLIS => typeNotImplemented()
          case _ => illegalType()
        }

      case INT64 =>
        field.getOriginalType match {
          case INT_64 | null => LongType
          case DECIMAL => makeDecimalType(maxPrecision(8))
          case TIMESTAMP_MILLIS => typeNotImplemented()
          case _ => illegalType()
        }

      case INT96 =>
        CatalystSchemaConverter.analysisRequire(
          assumeInt96IsTimestamp,
          "INT96 is not supported unless it's interpreted as timestamp. " +
            s"Please try to set ${SQLConf.PARQUET_INT96_AS_TIMESTAMP} to true.")
        TimestampType

      case BINARY =>
        field.getOriginalType match {
          case UTF8 => StringType
          case null if assumeBinaryIsString => StringType
          case null => BinaryType
          case DECIMAL => makeDecimalType()
          case _ => illegalType()
        }

      case FIXED_LEN_BYTE_ARRAY =>
        field.getOriginalType match {
          case DECIMAL => makeDecimalType(maxPrecision(field.getTypeLength))
          case INTERVAL => typeNotImplemented()
          case _ => illegalType()
        }
    }
  }

  private def convertGroupField(field: GroupType): DataType = {
    Option(field.getOriginalType).fold(convert(field): DataType) {
      // A Parquet list is represented as a 3-level structure:
      //
      //   <list-repetition> group <name> (LIST) {
      //     repeated group list {
      //       <element-repetition> <element-type> element;
      //     }
      //   }
      //
      // However, according to the most recent Parquet format spec (not released yet up until
      // writing), some 2-level structures are also recognized for backwards-compatibility.  Thus,
      // we need to check whether the 2nd level or the 3rd level refers to list element type.
      //
      // See: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
      case LIST =>
        CatalystSchemaConverter.analysisRequire(
          field.getFieldCount == 1, s"Invalid list type $field")

        val repeatedType = field.getType(0)
        CatalystSchemaConverter.analysisRequire(
          repeatedType.isRepetition(REPEATED), s"Invalid list type $field")

        if (isElementType(repeatedType, field.getName)) {
          ArrayType(convertField(repeatedType), containsNull = true)
        } else {
          val elementType = repeatedType.asGroupType().getType(0)
          val optional = elementType.isRepetition(OPTIONAL)
          ArrayType(convertField(elementType), containsNull = optional)
        }

      // scalastyle:off
      // `MAP_KEY_VALUE` is for backwards-compatibility
      // See: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules-1
      // scalastyle:on
      case MAP | MAP_KEY_VALUE =>
        CatalystSchemaConverter.analysisRequire(
          field.getFieldCount == 1 && !field.getType(0).isPrimitive,
          s"Invalid map type $field")

        val keyValueType = field.getType(0).asGroupType()
        CatalystSchemaConverter.analysisRequire(
          keyValueType.isRepetition(REPEATED) &&
            keyValueType.getOriginalType != MAP_KEY_VALUE &&
            keyValueType.getFieldCount == 2,
          s"Invalid map type $field")

        val keyType = keyValueType.getType(0)
        CatalystSchemaConverter.analysisRequire(
          keyType.isPrimitive, s"Map key type must be some primitive type.")

        val valueType = keyValueType.getType(1)
        val valueOptional = valueType.isRepetition(OPTIONAL)
        MapType(
          convertField(keyType),
          convertField(valueType),
          valueContainsNull = valueOptional)

      case _ =>
        throw new UnsupportedOperationException(s"Cannot convert Parquet type $field")
    }
  }

  // scalastyle:off
  // Here we implement Parquet LIST backwards-compatibility rules.
  // See: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
  // scalastyle:on
  private def isElementType(repeatedType: Type, parentName: String) = {
    {
      // For legacy 2-level list types with primitive element type, e.g.:
      //
      //    // List<Integer> (nullable list, non-null elements)
      //    optional group my_list (LIST) {
      //      repeated int32 element;
      //    }
      //
      repeatedType.isPrimitive
    } || {
      // For legacy 2-level list types whose element type is a group type with more than 1 fields,
      // e.g.:
      //
      //    // List<Tuple<String, Integer>> (nullable list, non-null elements)
      //    optional group my_list (LIST) {
      //      repeated group element {
      //        required binary str (UTF8);
      //        required int32 num;
      //      };
      //    }
      //
      repeatedType.asGroupType().getFieldCount > 1
    } || {
      // For legacy Parquet data generated by parquet-avro (Parquet version < 1.6.0) e.g.:
      //
      //    // List<OneTuple<String>> (nullable list, non-null elements)
      //    optional group my_list (LIST) {
      //      repeated group array {
      //        required binary str (UTF8);
      //      };
      //    }
      //
      repeatedType.getName == "array"
    } || {
      // For Parquet data generated by parquet-thrift, e.g.:
      //
      //    // List<OneTuple<String>> (nullable list, non-null elements)
      //    optional group my_list (LIST) {
      //      repeated group my_list_tuple {
      //        required binary str (UTF8);
      //      };
      //    }
      //
      repeatedType.getName == s"${parentName}_tuple"
    }
  }

  /**
   * Converts a Spark SQL [[StructType]] to a Parquet [[MessageType]].
   */
  def convert(catalystSchema: StructType): MessageType = {
    Types.buildMessage().addFields(catalystSchema.map(convertField): _*).named("root")
  }

  /**
   * Converts a Spark SQL [[StructField]] to a Parquet [[Type]].
   */
  def convertField(field: StructField): Type = {
    CatalystSchemaConverter.checkFieldName(field.name)

    val repetition = if (field.nullable) OPTIONAL else REQUIRED

    val builder = field.dataType match {
      // === Simple atomic types ===

      case BooleanType =>
        Types.primitive(BOOLEAN, repetition)

      case ByteType =>
        Types.primitive(INT32, repetition).as(INT_8)

      case ShortType =>
        Types.primitive(INT32, repetition).as(INT_16)

      case IntegerType =>
        Types.primitive(INT32, repetition)

      case LongType =>
        Types.primitive(INT64, repetition)

      case FloatType =>
        Types.primitive(FLOAT, repetition)

      case DoubleType =>
        Types.primitive(DOUBLE, repetition)

      case StringType =>
        Types.primitive(BINARY, repetition).as(UTF8)

      case DateType =>
        Types.primitive(INT32, repetition).as(DATE)

      // NOTE: !! This is not specified in Parquet format spec !!
      // However, older versions of Spark SQL and Impala use INT96 to store timestamps with
      // nanosecond precision (not TIME_MILLIS or TIMESTAMP_MILLIS described in the spec).
      case TimestampType =>
        Types.primitive(INT96, repetition)

      case BinaryType =>
        Types.primitive(BINARY, repetition)

      // === Decimals ===

      // TODO Enable the 4 case arms below
      //
      // Currently, Spark SQL only uses fixed-length byte array to store decimals, and only support
      // decimals with precision <= 18.
      //
      // To enable all 4 case arms below, we need also update decimal related logic in
      // CatalystPrimitiveConverter, RowReadSupport, RowWriteSupport and MutableRowWriteSupport.

      case DecimalType.Fixed(precision, scale) if precision <= maxPrecision(8) =>
        Types
          .primitive(FIXED_LEN_BYTE_ARRAY, repetition)
          .as(DECIMAL)
          .precision(precision)
          .scale(scale)
          .length(ParquetTypesConverter.BYTES_FOR_PRECISION(precision))

      // case DecimalType.Fixed(precision, scale) if precision <= maxPrecision(4) =>
      //   // Use INT32 for 1 <= precision <= 9
      //   Types.primitive(INT32, repetition).as(DECIMAL).precision(precision).scale(scale)

      // case DecimalType.Fixed(precision, scale) if precision <= maxPrecision(8) =>
      //   // Use INT64 for 1 <= precision <= 18
      //   Types.primitive(INT64, repetition).as(DECIMAL).precision(precision).scale(scale)

      // case DecimalType.Fixed(precision, scale) =>
      //   // Use BINARY for other precisions
      //   // TODO Should we use FIXED_LEN_BYTE_ARRAY here?  Is it more efficient?
      //   Types.primitive(BINARY, repetition).as(DECIMAL).precision(precision).scale(scale)

      // case DecimalType.Unlimited =>
      //   // For decimals with unknown precision and scale, use default precision 10 and scale 0,
      //   // which can be squeezed into INT64.
      //   Types.primitive(INT64, repetition).as(DECIMAL).precision(10).scale(0)

      // === Complex types ===

      case ArrayType(elementType, containsNull) =>
        // <list-repetition> group <name> (LIST) {
        //   repeated group list {
        //     <element-repetition> <element-type> element;
        //   }
        // }
        Types
          .buildGroup(repetition).as(LIST)
          .addField(
            Types.repeatedGroup()
              .addField(convertField(StructField("element", elementType, containsNull)))
              .named("list"))

      case MapType(keyType, valueType, valueContainsNull) =>
        // <map-repetition> group <name> (MAP) {
        //   repeated group key_value {
        //     required <key-type> key;
        //     <value-repetition> <value-type> value;
        //   }
        // }
        Types
          .buildGroup(repetition).as(MAP)
          .addField(
            Types
              .repeatedGroup()
              .addField(convertField(StructField("key", keyType, nullable = false)))
              .addField(convertField(StructField("value", valueType, valueContainsNull)))
              .named("key_value"))

      case StructType(fields) =>
        fields.foldLeft(Types.buildGroup(repetition)) { (builder, field) =>
          builder.addField(convertField(field))
        }

      case _ =>
        throw new AnalysisException(s"Unsupported data type $field.dataType")
    }

    builder.named(field.name)
  }

  // Max precision of a decimal value stored in `numBytes` bytes
  private def maxPrecision(numBytes: Int): Long = {
    Math.round(                               // convert double to long
      Math.floor(Math.log10(                  // number of base-10 digits
        Math.pow(2, 8 * numBytes - 1) - 1)))  // max value stored in numBytes
  }
}


private[parquet] object CatalystSchemaConverter {
  def checkFieldName(name: String): Unit = {
    // ,;{}()\n\t= and space are special characters in Parquet schema
    analysisRequire(
      !name.matches(".*[ ,;{}()\n\t=].*"),
      s"""Attribute name "$name" contains invalid character(s) among " ,;{}()\\n\\t=".
         |Please use alias to rename it.
       """.stripMargin.split("\n").mkString(" "))
  }

  def analysisRequire(f: => Boolean, message: String): Unit = {
    if (!f) {
      throw new AnalysisException(message)
    }
  }
}
