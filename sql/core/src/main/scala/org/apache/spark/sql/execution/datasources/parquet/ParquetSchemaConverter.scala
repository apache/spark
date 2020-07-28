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

package org.apache.spark.sql.execution.datasources.parquet

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema._
import org.apache.parquet.schema.OriginalType
import org.apache.parquet.schema.OriginalType._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition._

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.ParquetRowConversionMode
import org.apache.spark.sql.types._


/**
 * This converter class is used to convert Parquet [[MessageType]] to Spark SQL [[StructType]].
 *
 * Parquet format backwards-compatibility rules are respected when converting Parquet
 * [[MessageType]] schemas.
 *
 * @see https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
 *
 * @param assumeBinaryIsString Whether unannotated BINARY fields should be assumed to be Spark SQL
 *        [[StringType]] fields.
 * @param assumeInt96IsTimestamp Whether unannotated INT96 fields should be assumed to be Spark SQL
 *        [[TimestampType]] fields.
 */
class ParquetToSparkSchemaConverter(
    assumeBinaryIsString: Boolean = SQLConf.PARQUET_BINARY_AS_STRING.defaultValue.get,
    assumeInt96IsTimestamp: Boolean = SQLConf.PARQUET_INT96_AS_TIMESTAMP.defaultValue.get) {

  def this(conf: SQLConf) = this(
    assumeBinaryIsString = conf.isParquetBinaryAsString,
    assumeInt96IsTimestamp = conf.isParquetINT96AsTimestamp)

  def this(conf: Configuration) = this(
    assumeBinaryIsString = conf.get(SQLConf.PARQUET_BINARY_AS_STRING.key).toBoolean,
    assumeInt96IsTimestamp = conf.get(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key).toBoolean)


  /**
   * Converts Parquet [[MessageType]] `parquetSchema` to a Spark SQL [[StructType]].
   */
  def convert(parquetSchema: MessageType): StructType = convert(parquetSchema.asGroupType())

  private def convert(parquetSchema: GroupType): StructType = {
    val fields = parquetSchema.getFields.asScala.map { field =>
      field.getRepetition match {
        case OPTIONAL =>
          StructField(field.getName, convertField(field), nullable = true)

        case REQUIRED =>
          StructField(field.getName, convertField(field), nullable = false)

        case REPEATED =>
          // A repeated field that is neither contained by a `LIST`- or `MAP`-annotated group nor
          // annotated by `LIST` or `MAP` should be interpreted as a required list of required
          // elements where the element type is the type of the field.
          val arrayType = ArrayType(convertField(field), containsNull = false)
          StructField(field.getName, arrayType, nullable = false)
      }
    }

    StructType(fields.toSeq)
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

    def typeNotSupported() =
      throw new AnalysisException(s"Parquet type not supported: $typeString")

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
          case DECIMAL => makeDecimalType(Decimal.MAX_INT_DIGITS)
          case UINT_8 => typeNotSupported()
          case UINT_16 => typeNotSupported()
          case UINT_32 => typeNotSupported()
          case TIME_MILLIS => typeNotImplemented()
          case _ => illegalType()
        }

      case INT64 =>
        originalType match {
          case INT_64 | null => LongType
          case DECIMAL => makeDecimalType(Decimal.MAX_LONG_DIGITS)
          case UINT_64 => typeNotSupported()
          case TIMESTAMP_MICROS => TimestampType
          case TIMESTAMP_MILLIS => TimestampType
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
          case UTF8 | ENUM | JSON => StringType
          case null if assumeBinaryIsString => StringType
          case null => BinaryType
          case BSON => BinaryType
          case DECIMAL => makeDecimalType()
          case _ => illegalType()
        }

      case FIXED_LEN_BYTE_ARRAY =>
        originalType match {
          case DECIMAL => makeDecimalType(Decimal.maxPrecisionForBytes(field.getTypeLength))
          case INTERVAL => typeNotImplemented()
          case _ => illegalType()
        }

      case _ => illegalType()
    }
  }

  private def convertGroupField(field: GroupType): DataType = {
    Option(field.getOriginalType).fold(convert(field): DataType) {
      case LIST =>
        val (elementType, containsNull) =
          ParquetSchemaConverter.getParquetListElementTypeAndContainsNull(field)
          ArrayType(convertField(elementType), containsNull)

      case MAP | MAP_KEY_VALUE =>
        val (keyType, valueType) = ParquetSchemaConverter.getParquetMapKeyValueType(field)
        val valueOptional = valueType.isRepetition(OPTIONAL)
        MapType(
          convertField(keyType),
          convertField(valueType),
          valueContainsNull = valueOptional)

      case _ =>
        throw new AnalysisException(s"Unrecognized Parquet type: $field")
    }
  }
}

/**
 * This converter class is used to convert Spark SQL [[StructType]] to Parquet [[MessageType]].
 *
 * @param writeLegacyParquetFormat Whether to use legacy Parquet format compatible with Spark 1.4
 *        and prior versions when converting a Catalyst [[StructType]] to a Parquet [[MessageType]].
 *        When set to false, use standard format defined in parquet-format spec.  This argument only
 *        affects Parquet write path.
 * @param outputTimestampType which parquet timestamp type to use when writing.
 */
class SparkToParquetSchemaConverter(
    writeLegacyParquetFormat: Boolean = SQLConf.PARQUET_WRITE_LEGACY_FORMAT.defaultValue.get,
    outputTimestampType: SQLConf.ParquetOutputTimestampType.Value =
      SQLConf.ParquetOutputTimestampType.INT96) {

  def this(conf: SQLConf) = this(
    writeLegacyParquetFormat = conf.writeLegacyParquetFormat,
    outputTimestampType = conf.parquetOutputTimestampType)

  def this(conf: Configuration) = this(
    writeLegacyParquetFormat = conf.get(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key).toBoolean,
    outputTimestampType = SQLConf.ParquetOutputTimestampType.withName(
      conf.get(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key)))

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
      // ===================
      // Simple atomic types
      // ===================

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

      // NOTE: Spark SQL can write timestamp values to Parquet using INT96, TIMESTAMP_MICROS or
      // TIMESTAMP_MILLIS. TIMESTAMP_MICROS is recommended but INT96 is the default to keep the
      // behavior same as before.
      //
      // As stated in PARQUET-323, Parquet `INT96` was originally introduced to represent nanosecond
      // timestamp in Impala for some historical reasons.  It's not recommended to be used for any
      // other types and will probably be deprecated in some future version of parquet-format spec.
      // That's the reason why parquet-format spec only defines `TIMESTAMP_MILLIS` and
      // `TIMESTAMP_MICROS` which are both logical types annotating `INT64`.
      //
      // Originally, Spark SQL uses the same nanosecond timestamp type as Impala and Hive.  Starting
      // from Spark 1.5.0, we resort to a timestamp type with microsecond precision so that we can
      // store a timestamp into a `Long`.  This design decision is subject to change though, for
      // example, we may resort to nanosecond precision in the future.
      case TimestampType =>
        outputTimestampType match {
          case SQLConf.ParquetOutputTimestampType.INT96 =>
            Types.primitive(INT96, repetition).named(field.name)
          case SQLConf.ParquetOutputTimestampType.TIMESTAMP_MICROS =>
            Types.primitive(INT64, repetition).as(TIMESTAMP_MICROS).named(field.name)
          case SQLConf.ParquetOutputTimestampType.TIMESTAMP_MILLIS =>
            Types.primitive(INT64, repetition).as(TIMESTAMP_MILLIS).named(field.name)
        }

      case BinaryType =>
        Types.primitive(BINARY, repetition).named(field.name)

      // ======================
      // Decimals (legacy mode)
      // ======================

      // Spark 1.4.x and prior versions only support decimals with a maximum precision of 18 and
      // always store decimals in fixed-length byte arrays.  To keep compatibility with these older
      // versions, here we convert decimals with all precisions to `FIXED_LEN_BYTE_ARRAY` annotated
      // by `DECIMAL`.
      case DecimalType.Fixed(precision, scale) if writeLegacyParquetFormat =>
        Types
          .primitive(FIXED_LEN_BYTE_ARRAY, repetition)
          .as(DECIMAL)
          .precision(precision)
          .scale(scale)
          .length(Decimal.minBytesForPrecision(precision))
          .named(field.name)

      // ========================
      // Decimals (standard mode)
      // ========================

      // Uses INT32 for 1 <= precision <= 9
      case DecimalType.Fixed(precision, scale)
          if precision <= Decimal.MAX_INT_DIGITS && !writeLegacyParquetFormat =>
        Types
          .primitive(INT32, repetition)
          .as(DECIMAL)
          .precision(precision)
          .scale(scale)
          .named(field.name)

      // Uses INT64 for 1 <= precision <= 18
      case DecimalType.Fixed(precision, scale)
          if precision <= Decimal.MAX_LONG_DIGITS && !writeLegacyParquetFormat =>
        Types
          .primitive(INT64, repetition)
          .as(DECIMAL)
          .precision(precision)
          .scale(scale)
          .named(field.name)

      // Uses FIXED_LEN_BYTE_ARRAY for all other precisions
      case DecimalType.Fixed(precision, scale) if !writeLegacyParquetFormat =>
        Types
          .primitive(FIXED_LEN_BYTE_ARRAY, repetition)
          .as(DECIMAL)
          .precision(precision)
          .scale(scale)
          .length(Decimal.minBytesForPrecision(precision))
          .named(field.name)

      // ===================================
      // ArrayType and MapType (legacy mode)
      // ===================================

      // Spark 1.4.x and prior versions convert `ArrayType` with nullable elements into a 3-level
      // `LIST` structure.  This behavior is somewhat a hybrid of parquet-hive and parquet-avro
      // (1.6.0rc3): the 3-level structure is similar to parquet-hive while the 3rd level element
      // field name "array" is borrowed from parquet-avro.
      case ArrayType(elementType, nullable @ true) if writeLegacyParquetFormat =>
        // <list-repetition> group <name> (LIST) {
        //   optional group bag {
        //     repeated <element-type> array;
        //   }
        // }

        // This should not use `listOfElements` here because this new method checks if the
        // element name is `element` in the `GroupType` and throws an exception if not.
        // As mentioned above, Spark prior to 1.4.x writes `ArrayType` as `LIST` but with
        // `array` as its element name as below. Therefore, we build manually
        // the correct group type here via the builder. (See SPARK-16777)
        Types
          .buildGroup(repetition).as(LIST)
          .addField(Types
            .buildGroup(REPEATED)
            // "array" is the name chosen by parquet-hive (1.7.0 and prior version)
            .addField(convertField(StructField("array", elementType, nullable)))
            .named("bag"))
          .named(field.name)

      // Spark 1.4.x and prior versions convert ArrayType with non-nullable elements into a 2-level
      // LIST structure.  This behavior mimics parquet-avro (1.6.0rc3).  Note that this case is
      // covered by the backwards-compatibility rules implemented in `isElementType()`.
      case ArrayType(elementType, nullable @ false) if writeLegacyParquetFormat =>
        // <list-repetition> group <name> (LIST) {
        //   repeated <element-type> element;
        // }

        // Here too, we should not use `listOfElements`. (See SPARK-16777)
        Types
          .buildGroup(repetition).as(LIST)
          // "array" is the name chosen by parquet-avro (1.7.0 and prior version)
          .addField(convertField(StructField("array", elementType, nullable), REPEATED))
          .named(field.name)

      // Spark 1.4.x and prior versions convert MapType into a 3-level group annotated by
      // MAP_KEY_VALUE.  This is covered by `convertGroupField(field: GroupType): DataType`.
      case MapType(keyType, valueType, valueContainsNull) if writeLegacyParquetFormat =>
        // <map-repetition> group <name> (MAP) {
        //   repeated group map (MAP_KEY_VALUE) {
        //     required <key-type> key;
        //     <value-repetition> <value-type> value;
        //   }
        // }
        ConversionPatterns.mapType(
          repetition,
          field.name,
          convertField(StructField("key", keyType, nullable = false)),
          convertField(StructField("value", valueType, valueContainsNull)))

      // =====================================
      // ArrayType and MapType (standard mode)
      // =====================================

      case ArrayType(elementType, containsNull) if !writeLegacyParquetFormat =>
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
          .named(field.name)

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
          .named(field.name)

      // ===========
      // Other types
      // ===========

      case StructType(fields) =>
        fields.foldLeft(Types.buildGroup(repetition)) { (builder, field) =>
          builder.addField(convertField(field))
        }.named(field.name)

      case udt: UserDefinedType[_] =>
        convertField(field.copy(dataType = udt.sqlType))

      case _ =>
        throw new AnalysisException(s"Unsupported data type ${field.dataType.catalogString}")
    }
  }
}

/**
 * This checker class is used to check the matching relationship between
 * Spark SQL[[StructType]] and Parquet[[MessageType]].
 *
 * @see docs/sql-data-sources-parquet.md#conversion-mode
 *
 * @param conversionMode Set the mode [[ParquetRowConversionMode]] to convert the data in the
 *                       parquet file to the internal spark data.
 * @param assumeBinaryIsString Whether unannotated BINARY fields should be assumed to be Spark SQL
 *        [[StringType]] fields.
 * @param assumeInt96IsTimestamp Whether unannotated INT96 fields should be assumed to be Spark SQL
 *        [[TimestampType]] fields.
 */
class SparkParquetSchemaChecker(
    conversionMode: ParquetRowConversionMode.Value,
    assumeBinaryIsString: Boolean,
    assumeInt96IsTimestamp: Boolean) {

  def this(conf: Configuration) = this(
    conversionMode = ParquetRowConversionMode.withName(
      conf.get(SQLConf.PARQUET_ROW_CONVERSION_MODE.key)),
    assumeBinaryIsString = conf.get(SQLConf.PARQUET_BINARY_AS_STRING.key).toBoolean,
    assumeInt96IsTimestamp = conf.get(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key).toBoolean
  )

  private val lossPrecisionModeEnable = conversionMode == ParquetRowConversionMode.LOSS_PRECISION
  private val noSideEffectsModeEnable =
    conversionMode == ParquetRowConversionMode.NO_SIDE_EFFECTS || lossPrecisionModeEnable

  def checkSchema(sparkSchema: StructType, parquetSchema: MessageType): Unit = {
    val parquetGroup = parquetSchema.asGroupType()
    val errorFieldNames = sparkSchema.fields.flatMap(checkField(_, parquetGroup))
    assert(errorFieldNames.isEmpty,
      s""":The currently configured parquet file reading conversion mode is $conversionMode.
         | The [${errorFieldNames.mkString(", ")}] fields cannot be converted.
         |Parquet schema:
         |$parquetGroup
         |Catalyst schema:
         |${sparkSchema.prettyJson}
     """.stripMargin)
  }

  /**
   * @return fieldNames that cannot be converted.
   */
  private def checkField(sparkField: StructField, parquetParentGroup: GroupType): Seq[String] = {
    val fieldName = sparkField.name
    if (!parquetParentGroup.containsField(fieldName)) {
      return sparkField.nullable match {
        case true => Seq.empty
        case false => Seq(fieldName)
      }
    }
    val sparkType = sparkField.dataType
    val parquetType = parquetParentGroup.getType(fieldName)
    sparkType match {
      case udt: UserDefinedType[_] => checkType(udt.sqlType, parquetType)
      case _ => checkType(sparkType, parquetType)
    }
  }

  /**
   * @return fieldNames that cannot be converted.
   */
  private def checkType(sparkType: DataType, parquetType: Type,
                        ignoreRepeated: Boolean = false): Seq[String] = {
    val fieldName = parquetType.getName
    // A repeated field that is neither contained by a `LIST`- or `MAP`-annotated group nor
    // annotated by `LIST` or `MAP` should be interpreted as a required list of required
    // elements where the element type is the type of the field.
    if (sparkType.isInstanceOf[ArrayType] && parquetType.getOriginalType != LIST) {
      val arrayType = sparkType.asInstanceOf[ArrayType]
      return checkType(arrayType.elementType, parquetType, ignoreRepeated = true)
    }
    if (parquetType.getRepetition == REPEATED && !ignoreRepeated) {
      return Seq(s"$fieldName(REPEATED)")
    }

    if (parquetType.isPrimitive) {
      val primitiveType = parquetType.asPrimitiveType()
      val typeName = primitiveType.getPrimitiveTypeName
      val originalType = primitiveType.getOriginalType
      val canConvert = sparkType match {
        case BooleanType => canConvertToBoolean(typeName)
        case FloatType => canConvertToFloat(typeName, originalType)
        case DoubleType => canConvertToDouble(typeName, originalType)
        case t: ByteType => canConvertToIntegral(t, typeName, originalType, primitiveType)
        case t: ShortType => canConvertToIntegral(t, typeName, originalType, primitiveType)
        case t: IntegerType => canConvertToIntegral(t, typeName, originalType, primitiveType)
        case t: LongType => canConvertToIntegral(t, typeName, originalType, primitiveType)
        case StringType => canConvertToString(typeName, originalType)
        case DateType => canConvertToDate(typeName, originalType)
        case TimestampType => canConvertToTimestamp(typeName, originalType)
        case BinaryType => canConvertToBinary(typeName, originalType)
        case t: DecimalType => canConvertToDecimal(t, typeName, originalType, primitiveType)
        case _ => false
      }
      canConvert match {
        case true => Seq.empty
        case false => Seq(fieldName)
      }
    } else {
      val parquetGroupType = parquetType.asGroupType()
      sparkType match {
        case ArrayType(elementType, _) => checkArrayField(elementType, parquetGroupType)
        case MapType(keyType, valueType, _) => checkMapField(keyType, valueType, parquetGroupType)
        case StructType(fields) =>
          fields.flatMap(checkField(_, parquetGroupType)).map(fieldName + "." + _)
        case _ => Seq(fieldName)
      }
    }
  }

  private def isBoolean(name: PrimitiveTypeName): Boolean = {
    name == BOOLEAN
  }

  private def isFloat(name: PrimitiveTypeName): Boolean = {
    name == FLOAT || name == DOUBLE
  }

  private def isInteger(name: PrimitiveTypeName, original: OriginalType): Boolean = {
    name == INT32 && {
      original match {
        case null | INT_8 | INT_16 | INT_32 => true
        case  _ => false
      }
    }
  }

  private def isLong(name: PrimitiveTypeName, original: OriginalType): Boolean = {
    name == INT64 && {
      original match {
        case null | INT_64 => true
        case  _ => false
      }
    }
  }

  private def isDecimal(name: PrimitiveTypeName, original: OriginalType): Boolean = {
    original == DECIMAL && {
      name match {
        case INT32 | INT64 | BINARY | FIXED_LEN_BYTE_ARRAY => true
        case _ => false
      }
    }
  }

  private def decimalIsIntegral(parquetType: PrimitiveType): Boolean = {
    parquetType.getDecimalMetadata.getScale == 0
  }

  private def decimalToDecimalIsMatch(sparkType: DecimalType,
                                      parquetType: PrimitiveType): Boolean = {
    sparkType.precision == parquetType.getDecimalMetadata.getPrecision &&
      sparkType.scale == parquetType.getDecimalMetadata.getScale
  }

  private def decimalToDecimalIsNoSideEffects(sparkType: DecimalType,
                                              parquetType: PrimitiveType): Boolean = {
    sparkType.scale >= parquetType.getDecimalMetadata.getScale
  }

  private def isTimestamp(name: PrimitiveTypeName, original: OriginalType): Boolean = {
    name match {
      case INT64 =>
        original match {
          case TIMESTAMP_MICROS | TIMESTAMP_MILLIS => true
          case _ => false
        }
      case INT96 if assumeInt96IsTimestamp => true
      case _ => false
    }
  }

  private def isDate(name: PrimitiveTypeName, original: OriginalType): Boolean = {
    name == INT32 && original == DATE
  }

  private def isString(name: PrimitiveTypeName, original: OriginalType,
                       ignoreJson: Boolean): Boolean = {
    name == BINARY && {
      original match {
        case UTF8 | ENUM => true
        case null if assumeBinaryIsString => true
        case JSON => !ignoreJson
        case _ => false
      }
    }
  }

  private def canConvertToBoolean(name: PrimitiveTypeName): Boolean = {
    def matchMode() = isBoolean(name)
    matchMode()
  }

  private def canConvertToFloat(name: PrimitiveTypeName, original: OriginalType): Boolean = {
    def matchMode() = name == FLOAT
    def noSideEffectsMode() = name == INT32 && (original == INT_8 || original == INT_16)
    def lossPrecisionMode() = name == DOUBLE || isInteger(name, original) ||
      isLong(name, original) || isDecimal(name, original) ||
      isString(name, original, ignoreJson = true)
    matchMode() || (noSideEffectsModeEnable && noSideEffectsMode()) ||
      (lossPrecisionModeEnable && lossPrecisionMode())
  }

  private def canConvertToDouble(name: PrimitiveTypeName, original: OriginalType): Boolean = {
    def matchMode() = name == DOUBLE
    def noSideEffectsMode() = name == FLOAT || isInteger(name, original)
    def lossPrecisionMode() = isLong(name, original) ||
      isDecimal(name, original) || isString(name, original, ignoreJson = true)
    matchMode() || (noSideEffectsModeEnable && noSideEffectsMode()) ||
      (lossPrecisionModeEnable && lossPrecisionMode())
  }

  private def canConvertToIntegral(sparkType: IntegralType, name: PrimitiveTypeName,
                               original: OriginalType, parquetType: PrimitiveType): Boolean = {
    def matchMode() = {
      sparkType match {
        case ByteType => name == INT32 && original == INT_8
        case ShortType => name == INT32 && original == INT_16
        case IntegerType => name == INT32 && (original == null || original == INT_32)
        case LongType => isLong(name, original)
      }
    }
    def noSideEffectsMode() = isInteger(name, original) || isLong(name, original) ||
      (isDecimal(name, original) && decimalIsIntegral(parquetType)) ||
      isString(name, original, ignoreJson = true)
    def lossPrecisionMode() = isFloat(name) || isDecimal(name, original)
    matchMode() || (noSideEffectsModeEnable && noSideEffectsMode()) ||
      (lossPrecisionModeEnable && lossPrecisionMode())
  }

  private def canConvertToDate(name: PrimitiveTypeName, original: OriginalType): Boolean = {
    def matchMode() = isDate(name, original)
    def noSideEffectsMode() = isString(name, original, ignoreJson = true)
    def lossPrecisionMode() = isTimestamp(name, original)
    matchMode() || (noSideEffectsModeEnable && noSideEffectsMode()) ||
      (lossPrecisionModeEnable && lossPrecisionMode())
  }

  private def canConvertToDecimal(sparkType: DecimalType, name: PrimitiveTypeName,
                                  original: OriginalType, parquetType: PrimitiveType): Boolean = {
    val parquetIsDecimal = isDecimal(name, original)
    def matchMode() = parquetIsDecimal && decimalToDecimalIsMatch(sparkType, parquetType)
    def noSideEffectsMode() = isInteger(name, original) || isLong(name, original) ||
      (parquetIsDecimal && decimalToDecimalIsNoSideEffects(sparkType, parquetType))
    def lossPrecisionMode() = parquetIsDecimal || isFloat(name) ||
      isString(name, original, ignoreJson = true)
    matchMode() || (noSideEffectsModeEnable && noSideEffectsMode()) ||
      (lossPrecisionModeEnable && lossPrecisionMode())
  }

  private def canConvertToTimestamp(name: PrimitiveTypeName, original: OriginalType): Boolean = {
    def matchMode() = isTimestamp(name, original)
    def noSideEffectsMode() = isDate(name, original) ||
      isString(name, original, ignoreJson = true)
    matchMode() || (noSideEffectsModeEnable && noSideEffectsMode())
  }

  private def canConvertToString(name: PrimitiveTypeName, original: OriginalType): Boolean = {
    def matchMode() = isString(name, original, ignoreJson = false)
    def noSideEffectsMode() = isBoolean(name) || isFloat(name) || isInteger(name, original) ||
      isDate(name, original) || isDecimal(name, original) || isLong(name, original) ||
      isTimestamp(name, original)
    matchMode() || (noSideEffectsModeEnable && noSideEffectsMode())
  }

  private def canConvertToBinary(name: PrimitiveTypeName, original: OriginalType): Boolean = {
    def matchMode() = name == BINARY && {
      original match {
        case BSON => true
        case null => !assumeBinaryIsString
        case _ => false
      }
    }
    def noSideEffectsMode() = isString(name, original, ignoreJson = false)
    matchMode() || (noSideEffectsModeEnable && noSideEffectsMode())
  }

  /**
   * @return fieldNames that cannot be converted.
   */
  private def checkArrayField(elementType: DataType, parquetGroup: GroupType): Seq[String] = {
    val fieldName = parquetGroup.getName
    val (parquetElementType, _) =
      ParquetSchemaConverter.getParquetListElementTypeAndContainsNull(parquetGroup)
    val ignoreRepeated = parquetGroup.getType(0).eq(parquetElementType)
    val errorFieldNames = checkType(elementType, parquetElementType, ignoreRepeated)
    errorFieldNames.map(fieldName + "." + _)
  }

  /**
   * @return fieldNames that cannot be converted.
   */
  private def checkMapField(keyType: DataType, valueType: DataType,
                            parquetGroup: GroupType): Seq[String] = {
    val fieldName = parquetGroup.getName
    if (parquetGroup.getOriginalType != MAP && parquetGroup.getOriginalType != MAP_KEY_VALUE) {
      return Seq(fieldName)
    }
    val (parquetKeyType, parquetValueType) =
      ParquetSchemaConverter.getParquetMapKeyValueType(parquetGroup)
    val errorFieldNames =
      checkType(keyType, parquetKeyType) ++ checkType(valueType, parquetValueType)
    errorFieldNames.map(fieldName + "." + _)
  }
}

private[sql] object ParquetSchemaConverter {
  val SPARK_PARQUET_SCHEMA_NAME = "spark_schema"

  val EMPTY_MESSAGE: MessageType =
    Types.buildMessage().named(ParquetSchemaConverter.SPARK_PARQUET_SCHEMA_NAME)

  def checkFieldName(name: String): Unit = {
    // ,;{}()\n\t= and space are special characters in Parquet schema
    checkConversionRequirement(
      !name.matches(".*[ ,;{}()\n\t=].*"),
      s"""Attribute name "$name" contains invalid character(s) among " ,;{}()\\n\\t=".
         |Please use alias to rename it.
       """.stripMargin.split("\n").mkString(" ").trim)
  }

  def checkFieldNames(names: Seq[String]): Unit = {
    names.foreach(checkFieldName)
  }

  def checkConversionRequirement(f: => Boolean, message: String): Unit = {
    if (!f) {
      throw new AnalysisException(message)
    }
  }

  def getParquetListElementTypeAndContainsNull(field: GroupType): (Type, Boolean) = {
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
    ParquetSchemaConverter.checkConversionRequirement(
      field.getFieldCount == 1, s"Invalid list type $field")

    val repeatedType = field.getType(0)
    ParquetSchemaConverter.checkConversionRequirement(
      repeatedType.isRepetition(REPEATED), s"Invalid list type $field")

    if (ParquetSchemaConverter.isElementType(repeatedType, field.getName)) {
      (repeatedType, false)
    } else {
      val elementType = repeatedType.asGroupType().getType(0)
      val optional = elementType.isRepetition(OPTIONAL)
      (elementType, optional)
    }
  }

  def getParquetMapKeyValueType(field: GroupType): (Type, Type) = {
    // scalastyle:off
    // `MAP_KEY_VALUE` is for backwards-compatibility
    // See: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules-1
    // scalastyle:on
    ParquetSchemaConverter.checkConversionRequirement(
      field.getFieldCount == 1 && !field.getType(0).isPrimitive,
      s"Invalid map type: $field")

    val keyValueType = field.getType(0).asGroupType()
    ParquetSchemaConverter.checkConversionRequirement(
      keyValueType.isRepetition(REPEATED) && keyValueType.getFieldCount == 2,
      s"Invalid map type: $field")

    val keyType = keyValueType.getType(0)
    val valueType = keyValueType.getType(1)
    (keyType, valueType)
  }

  // scalastyle:off
  // Here we implement Parquet LIST backwards-compatibility rules.
  // See: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
  // scalastyle:on
  def isElementType(repeatedType: Type, parentName: String): Boolean = {
    {
      // For legacy 2-level list types with primitive element type, e.g.:
      //
      //    // ARRAY<INT> (nullable list, non-null elements)
      //    optional group my_list (LIST) {
      //      repeated int32 element;
      //    }
      //
      repeatedType.isPrimitive
    } || {
      // For legacy 2-level list types whose element type is a group type with 2 or more fields,
      // e.g.:
      //
      //    // ARRAY<STRUCT<str: STRING, num: INT>> (nullable list, non-null elements)
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
      //    // ARRAY<STRUCT<str: STRING>> (nullable list, non-null elements)
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
      //    // ARRAY<STRUCT<str: STRING>> (nullable list, non-null elements)
      //    optional group my_list (LIST) {
      //      repeated group my_list_tuple {
      //        required binary str (UTF8);
      //      };
      //    }
      //
      repeatedType.getName == s"${parentName}_tuple"
    }
  }
}
