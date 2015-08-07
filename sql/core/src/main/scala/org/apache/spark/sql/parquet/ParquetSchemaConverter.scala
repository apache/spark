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
import org.apache.parquet.schema._

import org.apache.spark.sql.parquet.ParquetSchemaConverter.{MAX_PRECISION_FOR_INT32, MAX_PRECISION_FOR_INT64, maxPrecisionForBytes, minBytesForPrecision}
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
 * @param assumeBinaryIsString Whether unannotated BINARY fields should be assumed to be Spark SQL
 *        [[StringType]] fields when converting Parquet a [[MessageType]] to Spark SQL
 *        [[StructType]].  This argument only affects Parquet read path.
 * @param assumeInt96IsTimestamp Whether unannotated INT96 fields should be assumed to be Spark SQL
 *        [[TimestampType]] fields when converting Parquet a [[MessageType]] to Spark SQL
 *        [[StructType]].  Note that Spark SQL [[TimestampType]] is similar to Hive timestamp, which
 *        has optional nanosecond precision, but different from `TIME_MILLS` and `TIMESTAMP_MILLIS`
 *        described in Parquet format spec.  This argument only affects Parquet read path.
 * @param writeLegacyParquetFormat Whether to use legacy Parquet format compatible with Spark 1.4
 *        and prior versions when converting a Catalyst [[StructType]] to a Parquet [[MessageType]].
 *        When set to false, use standard format defined in parquet-format spec.  This argument only
 *        affects Parquet write path.
 */
private[parquet] class ParquetSchemaConverter(
    private val assumeBinaryIsString: Boolean,
    private val assumeInt96IsTimestamp: Boolean,
    private val writeLegacyParquetFormat: Boolean) {

  // Only used when constructing converter for converting Spark SQL schema to Parquet schema, in
  // which case `assumeInt96IsTimestamp` and `assumeBinaryIsString` are irrelevant.
  def this() = this(
    assumeBinaryIsString = SQLConf.PARQUET_BINARY_AS_STRING.defaultValue.get,
    assumeInt96IsTimestamp = SQLConf.PARQUET_INT96_AS_TIMESTAMP.defaultValue.get,
    writeLegacyParquetFormat = SQLConf.PARQUET_WRITE_LEGACY_FORMAT.defaultValue.get)

  def this(conf: SQLConf) = this(
    assumeBinaryIsString = conf.isParquetBinaryAsString,
    assumeInt96IsTimestamp = conf.isParquetINT96AsTimestamp,
    writeLegacyParquetFormat = conf.writeLegacyParquetFormat)

  def this(conf: Configuration) = this(
    assumeBinaryIsString =
      conf.getBoolean(
        SQLConf.PARQUET_BINARY_AS_STRING.key,
        SQLConf.PARQUET_BINARY_AS_STRING.defaultValue.get),
    assumeInt96IsTimestamp =
      conf.getBoolean(
        SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
        SQLConf.PARQUET_INT96_AS_TIMESTAMP.defaultValue.get),
    writeLegacyParquetFormat =
      conf.getBoolean(
        SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key,
        SQLConf.PARQUET_WRITE_LEGACY_FORMAT.defaultValue.get))

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
          throw new AnalysisException(
            s"REPEATED not supported outside LIST or MAP. Type: $field")
      }
    }

    StructType(fields)
  }

  /**
   * Converts a Parquet [[Type]] to a Spark SQL [[DataType]].
   */
  def convertField(parquetType: Type): DataType = parquetType match {
    case t: PrimitiveType => convertPrimitiveField(t)
    case t: GroupType => convertGroupField(t.asGroupType())
  }

  private def convertPrimitiveField(field: PrimitiveType): DataType = {
    val typeName = field.getPrimitiveTypeName
    val originalType = field.getOriginalType

    def typeString =
      if (originalType == null) s"$typeName" else s"$typeName ($originalType)"

    def typeNotImplemented() =
      throw new AnalysisException(s"Parquet type not yet supported: $typeString")

    def illegalType() =
      throw new AnalysisException(s"Illegal Parquet type: $typeString")

    // When maxPrecision = -1, we skip precision range check, and always respect the precision
    // specified in field.getDecimalMetadata.  This is useful when interpreting decimal types stored
    // as binaries with variable lengths.
    def makeDecimalType(maxPrecision: Int = -1): DecimalType = {
      val precision = field.getDecimalMetadata.getPrecision
      val scale = field.getDecimalMetadata.getScale

      ParquetSchemaConverter.checkConversionRequirement(
        maxPrecision == -1 || 1 <= precision && precision <= maxPrecision,
        s"Invalid decimal precision: $typeName cannot store $precision digits (max $maxPrecision)")

      DecimalType(precision, scale)
    }

    typeName match {
      case BOOLEAN => BooleanType

      case FLOAT => FloatType

      case DOUBLE => DoubleType

      case INT32 =>
        originalType match {
          case INT_8 => ByteType
          case INT_16 => ShortType
          case INT_32 | null => IntegerType
          case DATE => DateType
          case DECIMAL => makeDecimalType(MAX_PRECISION_FOR_INT32)
          case TIME_MILLIS => typeNotImplemented()
          case _ => illegalType()
        }

      case INT64 =>
        originalType match {
          case INT_64 | null => LongType
          case DECIMAL => makeDecimalType(MAX_PRECISION_FOR_INT64)
          case TIMESTAMP_MILLIS => typeNotImplemented()
          case _ => illegalType()
        }

      case INT96 =>
        ParquetSchemaConverter.checkConversionRequirement(
          assumeInt96IsTimestamp,
          "INT96 is not supported unless it's interpreted as timestamp. " +
            s"Please try to set ${SQLConf.PARQUET_INT96_AS_TIMESTAMP.key} to true.")
        TimestampType

      case BINARY =>
        originalType match {
          case UTF8 | ENUM => StringType
          case null if assumeBinaryIsString => StringType
          case null => BinaryType
          case DECIMAL => makeDecimalType()
          case _ => illegalType()
        }

      case FIXED_LEN_BYTE_ARRAY =>
        originalType match {
          case DECIMAL => makeDecimalType(maxPrecisionForBytes(field.getTypeLength))
          case INTERVAL => typeNotImplemented()
          case _ => illegalType()
        }

      case _ => illegalType()
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
        ParquetSchemaConverter.checkConversionRequirement(
          field.getFieldCount == 1, s"Invalid list type $field")

        val repeatedType = field.getType(0)
        ParquetSchemaConverter.checkConversionRequirement(
          repeatedType.isRepetition(REPEATED), s"Invalid list type $field")

        if (isElementType(repeatedType, field.getName)) {
          ArrayType(convertField(repeatedType), containsNull = false)
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
        ParquetSchemaConverter.checkConversionRequirement(
          field.getFieldCount == 1 && !field.getType(0).isPrimitive,
          s"Invalid map type: $field")

        val keyValueType = field.getType(0).asGroupType()
        ParquetSchemaConverter.checkConversionRequirement(
          keyValueType.isRepetition(REPEATED) && keyValueType.getFieldCount == 2,
          s"Invalid map type: $field")

        val keyType = keyValueType.getType(0)
        ParquetSchemaConverter.checkConversionRequirement(
          keyType.isPrimitive,
          s"Map key type is expected to be a primitive type, but found: $keyType")

        val valueType = keyValueType.getType(1)
        val valueOptional = valueType.isRepetition(OPTIONAL)
        MapType(
          convertField(keyType),
          convertField(valueType),
          valueContainsNull = valueOptional)

      case _ =>
        throw new AnalysisException(s"Unrecognized Parquet type: $field")
    }
  }

  // scalastyle:off
  // Here we implement Parquet LIST backwards-compatibility rules.
  // See: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
  // scalastyle:on
  private def isElementType(repeatedType: Type, parentName: String): Boolean = {
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
      // For legacy 2-level list types whose element type is a group type with 2 or more fields,
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
      // For legacy 2-level list types generated by parquet-avro (Parquet version < 1.6.0), e.g.:
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
    Types
      .buildMessage()
      .addFields(catalystSchema.map(convertField): _*)
      .named(ParquetSchemaConverter.SPARK_PARQUET_SCHEMA_NAME)
  }

  /**
   * Converts a Spark SQL [[StructField]] to a Parquet [[Type]].
   */
  def convertField(field: StructField): Type = {
    convertField(field, if (field.nullable) OPTIONAL else REQUIRED)
  }

  private def convertField(field: StructField, repetition: Type.Repetition): Type = {
    ParquetSchemaConverter.checkFieldName(field.name)

    field.dataType match {
      case BooleanType =>
        Types.primitive(BOOLEAN, repetition).named(field.name)

      case ByteType =>
        Types.primitive(INT32, repetition).as(INT_8).named(field.name)

      case ShortType =>
        Types.primitive(INT32, repetition).as(INT_16).named(field.name)

      case IntegerType =>
        Types.primitive(INT32, repetition).named(field.name)

      case LongType =>
        Types.primitive(INT64, repetition).named(field.name)

      case FloatType =>
        Types.primitive(FLOAT, repetition).named(field.name)

      case DoubleType =>
        Types.primitive(DOUBLE, repetition).named(field.name)

      case StringType =>
        Types.primitive(BINARY, repetition).as(UTF8).named(field.name)

      case DateType =>
        Types.primitive(INT32, repetition).as(DATE).named(field.name)

      // NOTE: Spark SQL TimestampType is NOT a well defined type in Parquet format spec.
      //
      // As stated in PARQUET-323, Parquet `INT96` was originally introduced to represent nanosecond
      // timestamp in Impala for some historical reasons.  It's not recommended to be used for any
      // other types and will probably be deprecated in some future version of parquet-format spec.
      // That's the reason why parquet-format spec only defines `TIMESTAMP_MILLIS` and
      // `TIMESTAMP_MICROS` which are both logical types annotating `INT64`.
      //
      // Originally, Spark SQL uses the same nanosecond timestamp type as Impala and Hive.  Starting
      // from Spark 1.5.0, we resort to microsecond timestamp type.
      //
      // We plan to write all `TimestampType` values as `TIMESTAMP_MICROS`, but up to writing, the
      // most recent version of parquet-mr (1.8.1) hasn't implemented `TIMESTAMP_MICROS` yet.
      //
      // TODO Converts to `TIMESTAMP_MICROS` once parquet-mr implements that.
      case TimestampType =>
        Types.primitive(INT96, repetition).named(field.name)

      case BinaryType =>
        Types.primitive(BINARY, repetition).named(field.name)

      case DecimalType.Fixed(precision, scale) =>
        val builder = writeLegacyParquetFormat match {
          // Standard mode, 1 <= precision <= 9, converts to INT32 based DECIMAL
          case false if precision <= MAX_PRECISION_FOR_INT32 =>
            Types.primitive(INT32, repetition)

          // Standard mode, 10 <= precision <= 18, converts to INT64 based DECIMAL
          case false if precision <= MAX_PRECISION_FOR_INT64 =>
            Types.primitive(INT64, repetition)

          // All other cases:
          //  - Standard mode, 19 <= precision <= 38, converts to FIXED_LEN_BYTE_ARRAY based DECIMAL
          //  - Legacy mode, 1 <= precision <= 38, converts to FIXED_LEN_BYTE_ARRAY based DECIMAL
          case _ =>
            val numBytes = minBytesForPrecision(precision)
            Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition).length(numBytes)
        }

        builder.as(DECIMAL).precision(precision).scale(scale).named(field.name)

      case t: ArrayType =>
        val repeatedType = (writeLegacyParquetFormat, t.containsNull) match {
          case (true, true) =>
            // Legacy mode: Spark 1.4.x and prior versions convert `ArrayType` with nullable
            // elements into a 3-level `LIST` structure.  This behavior is somewhat a hybrid of
            // parquet-hive and parquet-avro (1.6.0rc3): the 3-level structure is similar to
            // parquet-hive while the 3rd level anonymous field name "array" is from parquet-avro.
            //
            //   <list-repetition> group <name> (LIST) {
            //     repeated group bag {                             |
            //       optional <element-type> array;                 |-  repeatedType
            //     }                                                |
            //   }
            Types
              .repeatedGroup()
              .addField(convertField(StructField("array", t.elementType, t.containsNull)))
              .named("bag")

          case (true, false) =>
            // Legacy mode: Spark 1.4.x and prior versions convert `ArrayType` with non-nullable
            // elements into a 2-level `LIST` structure.  This behavior mimics parquet-avro
            // (1.6.0rc3).
            //
            //   <list-repetition> group <name> (LIST) {
            //     repeated <element-type> array;                   <-  repeatedType
            //   }
            convertField(StructField("array", t.elementType, t.containsNull), REPEATED)

          case (false, _) =>
            // Standard mode:
            //
            //   <list-repetition> group <name> (LIST) {
            //     repeated group list {                            |
            //       <element-repetition> <element-type> element;   |-  repeatedType
            //     }                                                |
            //   }
            Types
              .repeatedGroup()
              .addField(convertField(StructField("element", t.elementType, t.containsNull)))
              .named("list")
        }

        Types.buildGroup(repetition).as(LIST).addField(repeatedType).named(field.name)

      case t: MapType =>
        val repeatedGroupBuilder =
          Types
            .repeatedGroup()
            .addField(convertField(StructField("key", t.keyType, nullable = false)))
            .addField(convertField(StructField("value", t.valueType, t.valueContainsNull)))

        val repeatedGroup = if (writeLegacyParquetFormat) {
          // Legacy mode: Spark 1.4.x and prior versions convert MapType into a 3-level group
          // annotated by MAP_KEY_VALUE.
          //
          //   <map-repetition> group <name> (MAP) {
          //     repeated group map (MAP_KEY_VALUE) {               |
          //       required <key-type> key;                         |-  repeatedGroup
          //       <value-repetition> <value-type> value;           |
          //     }                                                  |
          //   }
          repeatedGroupBuilder.as(MAP_KEY_VALUE).named("map")
        } else {
          // Standard mode:
          //
          //   <map-repetition> group <name> (MAP) {
          //     repeated group key_value {                         |
          //       required <key-type> key;                         |-  repeatedGroup
          //       <value-repetition> <value-type> value;           |
          //     }                                                  |
          //   }
          repeatedGroupBuilder.named("key_value")
        }

        Types.buildGroup(repetition).as(MAP).addField(repeatedGroup).named(field.name)

      case t: StructType =>
        val parquetFields = t.fields.map(convertField)
        Types.buildGroup(repetition).addFields(parquetFields: _*).named(field.name)

      case udt: UserDefinedType[_] =>
        convertField(field.copy(dataType = udt.sqlType))

      case _ =>
        throw new AnalysisException(s"Unsupported data type $field.dataType")
    }
  }
}


private[parquet] object ParquetSchemaConverter {
  val SPARK_PARQUET_SCHEMA_NAME = "spark_schema"

  def checkFieldName(name: String): Unit = {
    // ,;{}()\n\t= and space are special characters in Parquet schema
    checkConversionRequirement(
      !name.matches(".*[ ,;{}()\n\t=].*"),
      s"""Attribute name "$name" contains invalid character(s) among " ,;{}()\\n\\t=".
         |Please use alias to rename it.
       """.stripMargin.split("\n").mkString(" "))
  }

  def checkFieldNames(schema: StructType): StructType = {
    schema.fieldNames.foreach(checkFieldName)
    schema
  }

  def checkConversionRequirement(f: => Boolean, message: String): Unit = {
    if (!f) {
      throw new AnalysisException(message)
    }
  }

  // The minimum number of bytes needed to store a decimal with a given `precision`.
  val minBytesForPrecision = Array.tabulate[Int](DecimalType.MAX_PRECISION + 1) { precision =>
    var numBytes = 1
    while (math.pow(2.0, 8 * numBytes - 1) < math.pow(10.0, precision)) {
      numBytes += 1
    }
    numBytes
  }

  val MAX_PRECISION_FOR_INT32 = maxPrecisionForBytes(4)

  val MAX_PRECISION_FOR_INT64 = maxPrecisionForBytes(8)

  // Max precision of a decimal value stored in `numBytes` bytes
  def maxPrecisionForBytes(numBytes: Int): Int = {
    Math.round(                               // convert double to long
      Math.floor(Math.log10(                  // number of base-10 digits
        Math.pow(2, 8 * numBytes - 1) - 1)))  // max value stored in numBytes
      .asInstanceOf[Int]
  }
}
