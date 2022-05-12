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

import java.util.Locale

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.io.{ColumnIO, ColumnIOFactory, GroupColumnIO, PrimitiveColumnIO}
import org.apache.parquet.schema._
import org.apache.parquet.schema.LogicalTypeAnnotation._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition._

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * This converter class is used to convert Parquet [[MessageType]] to Spark SQL [[StructType]]
 * (via the `convert` method) as well as [[ParquetColumn]] (via the `convertParquetColumn`
 * method). The latter contains richer information about the Parquet type, including its
 * associated repetition & definition level, column path, column descriptor etc.
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
 * @param caseSensitive Whether use case sensitive analysis when comparing Spark catalyst read
 *                      schema with Parquet schema.
 * @param timestampNTZEnabled Whether TimestampNTZType type is enabled.
 */
class ParquetToSparkSchemaConverter(
    assumeBinaryIsString: Boolean = SQLConf.PARQUET_BINARY_AS_STRING.defaultValue.get,
    assumeInt96IsTimestamp: Boolean = SQLConf.PARQUET_INT96_AS_TIMESTAMP.defaultValue.get,
    caseSensitive: Boolean = SQLConf.CASE_SENSITIVE.defaultValue.get,
    timestampNTZEnabled: Boolean = SQLConf.PARQUET_TIMESTAMP_NTZ_ENABLED.defaultValue.get) {

  def this(conf: SQLConf) = this(
    assumeBinaryIsString = conf.isParquetBinaryAsString,
    assumeInt96IsTimestamp = conf.isParquetINT96AsTimestamp,
    caseSensitive = conf.caseSensitiveAnalysis,
    timestampNTZEnabled = conf.parquetTimestampNTZEnabled)

  def this(conf: Configuration) = this(
    assumeBinaryIsString = conf.get(SQLConf.PARQUET_BINARY_AS_STRING.key).toBoolean,
    assumeInt96IsTimestamp = conf.get(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key).toBoolean,
    caseSensitive = conf.get(SQLConf.CASE_SENSITIVE.key).toBoolean,
    timestampNTZEnabled = conf.get(SQLConf.PARQUET_TIMESTAMP_NTZ_ENABLED.key).toBoolean)

  /**
   * Returns true if TIMESTAMP_NTZ type is enabled in this ParquetToSparkSchemaConverter.
   */
  def isTimestampNTZEnabled(): Boolean = {
    timestampNTZEnabled
  }

  /**
   * Converts Parquet [[MessageType]] `parquetSchema` to a Spark SQL [[StructType]].
   */
  def convert(parquetSchema: MessageType): StructType = {
    val column = new ColumnIOFactory().getColumnIO(parquetSchema)
    val converted = convertInternal(column)
    converted.sparkType.asInstanceOf[StructType]
  }

  /**
   * Convert `parquetSchema` into a [[ParquetColumn]] which contains its corresponding Spark
   * SQL [[StructType]] along with other information such as the maximum repetition and definition
   * level of each node, column descriptor for the leave nodes, etc.
   *
   * If `sparkReadSchema` is not empty, when deriving Spark SQL type from a Parquet field this will
   * check if the same field also exists in the schema. If so, it will use the Spark SQL type.
   * This is necessary since conversion from Parquet to Spark could cause precision loss. For
   * instance, Spark read schema is smallint/tinyint but Parquet only support int.
   */
  def convertParquetColumn(
      parquetSchema: MessageType,
      sparkReadSchema: Option[StructType] = None): ParquetColumn = {
    val column = new ColumnIOFactory().getColumnIO(parquetSchema)
    convertInternal(column, sparkReadSchema)
  }

  private def convertInternal(
      groupColumn: GroupColumnIO,
      sparkReadSchema: Option[StructType] = None): ParquetColumn = {
    // First convert the read schema into a map from field name to the field itself, to avoid O(n)
    // lookup cost below.
    val schemaMapOpt = sparkReadSchema.map { schema =>
      schema.map(f => normalizeFieldName(f.name) -> f).toMap
    }

    val converted = (0 until groupColumn.getChildrenCount).map { i =>
      val field = groupColumn.getChild(i)
      val fieldFromReadSchema = schemaMapOpt.flatMap { schemaMap =>
        schemaMap.get(normalizeFieldName(field.getName))
      }
      var fieldReadType = fieldFromReadSchema.map(_.dataType)

      // If a field is repeated here then it is neither contained by a `LIST` nor `MAP`
      // annotated group (these should've been handled in `convertGroupField`), e.g.:
      //
      //  message schema {
      //    repeated int32 int_array;
      //  }
      // or
      //  message schema {
      //    repeated group struct_array {
      //      optional int32 field;
      //    }
      //  }
      //
      // the corresponding Spark read type should be an array and we should pass the element type
      // to the group or primitive type conversion method.
      if (field.getType.getRepetition == REPEATED) {
        fieldReadType = fieldReadType.flatMap {
          case at: ArrayType => Some(at.elementType)
          case _ =>
            throw QueryCompilationErrors.illegalParquetTypeError(groupColumn.toString)
        }
      }

      val convertedField = convertField(field, fieldReadType)
      val fieldName = fieldFromReadSchema.map(_.name).getOrElse(field.getType.getName)

      field.getType.getRepetition match {
        case OPTIONAL | REQUIRED =>
          val nullable = field.getType.getRepetition == OPTIONAL
          (StructField(fieldName, convertedField.sparkType, nullable = nullable),
              convertedField)

        case REPEATED =>
          // A repeated field that is neither contained by a `LIST`- or `MAP`-annotated group nor
          // annotated by `LIST` or `MAP` should be interpreted as a required list of required
          // elements where the element type is the type of the field.
          val arrayType = ArrayType(convertedField.sparkType, containsNull = false)
          (StructField(fieldName, arrayType, nullable = false),
              ParquetColumn(arrayType, None, convertedField.repetitionLevel - 1,
                convertedField.definitionLevel - 1, required = true, convertedField.path,
                Seq(convertedField.copy(required = true))))
      }
    }

    ParquetColumn(StructType(converted.map(_._1)), groupColumn, converted.map(_._2))
  }

  private def normalizeFieldName(name: String): String =
    if (caseSensitive) name else name.toLowerCase(Locale.ROOT)

  /**
   * Converts a Parquet [[Type]] to a [[ParquetColumn]] which wraps a Spark SQL [[DataType]] with
   * additional information such as the Parquet column's repetition & definition level, column
   * path, column descriptor etc.
   */
  def convertField(
      field: ColumnIO,
      sparkReadType: Option[DataType] = None): ParquetColumn = {
    val targetType = sparkReadType.map {
      case udt: UserDefinedType[_] => udt.sqlType
      case otherType => otherType
    }
    field match {
      case primitiveColumn: PrimitiveColumnIO => convertPrimitiveField(primitiveColumn, targetType)
      case groupColumn: GroupColumnIO => convertGroupField(groupColumn, targetType)
    }
  }

  private def convertPrimitiveField(
      primitiveColumn: PrimitiveColumnIO,
      sparkReadType: Option[DataType] = None): ParquetColumn = {
    val parquetType = primitiveColumn.getType.asPrimitiveType()
    val typeAnnotation = primitiveColumn.getType.getLogicalTypeAnnotation
    val typeName = primitiveColumn.getPrimitive

    def typeString =
      if (typeAnnotation == null) s"$typeName" else s"$typeName ($typeAnnotation)"

    def typeNotImplemented() =
      throw QueryCompilationErrors.parquetTypeUnsupportedYetError(typeString)

    def illegalType() =
      throw QueryCompilationErrors.illegalParquetTypeError(typeString)

    // When maxPrecision = -1, we skip precision range check, and always respect the precision
    // specified in field.getDecimalMetadata.  This is useful when interpreting decimal types stored
    // as binaries with variable lengths.
    def makeDecimalType(maxPrecision: Int = -1): DecimalType = {
      val decimalLogicalTypeAnnotation = typeAnnotation
        .asInstanceOf[DecimalLogicalTypeAnnotation]
      val precision = decimalLogicalTypeAnnotation.getPrecision
      val scale = decimalLogicalTypeAnnotation.getScale

      ParquetSchemaConverter.checkConversionRequirement(
        maxPrecision == -1 || 1 <= precision && precision <= maxPrecision,
        s"Invalid decimal precision: $typeName cannot store $precision digits (max $maxPrecision)")

      DecimalType(precision, scale)
    }

    val sparkType = sparkReadType.getOrElse(typeName match {
      case BOOLEAN => BooleanType

      case FLOAT => FloatType

      case DOUBLE => DoubleType

      case INT32 =>
        typeAnnotation match {
          case intTypeAnnotation: IntLogicalTypeAnnotation if intTypeAnnotation.isSigned =>
            intTypeAnnotation.getBitWidth match {
              case 8 => ByteType
              case 16 => ShortType
              case 32 => IntegerType
              case _ => illegalType()
            }
          case null => IntegerType
          case _: DateLogicalTypeAnnotation => DateType
          case _: DecimalLogicalTypeAnnotation => makeDecimalType(Decimal.MAX_INT_DIGITS)
          case intTypeAnnotation: IntLogicalTypeAnnotation if !intTypeAnnotation.isSigned =>
            intTypeAnnotation.getBitWidth match {
              case 8 => ShortType
              case 16 => IntegerType
              case 32 => LongType
              case _ => illegalType()
            }
          case t: TimestampLogicalTypeAnnotation if t.getUnit == TimeUnit.MILLIS =>
            typeNotImplemented()
          case _ => illegalType()
        }

      case INT64 =>
        typeAnnotation match {
          case intTypeAnnotation: IntLogicalTypeAnnotation if intTypeAnnotation.isSigned =>
            intTypeAnnotation.getBitWidth match {
              case 64 => LongType
              case _ => illegalType()
            }
          case null => LongType
          case _: DecimalLogicalTypeAnnotation => makeDecimalType(Decimal.MAX_LONG_DIGITS)
          case intTypeAnnotation: IntLogicalTypeAnnotation if !intTypeAnnotation.isSigned =>
            intTypeAnnotation.getBitWidth match {
              // The precision to hold the largest unsigned long is:
              // `java.lang.Long.toUnsignedString(-1).length` = 20
              case 64 => DecimalType(20, 0)
              case _ => illegalType()
            }
          case timestamp: TimestampLogicalTypeAnnotation
            if timestamp.getUnit == TimeUnit.MICROS || timestamp.getUnit == TimeUnit.MILLIS =>
            if (timestamp.isAdjustedToUTC || !timestampNTZEnabled) {
              TimestampType
            } else {
              TimestampNTZType
            }
          case _ => illegalType()
        }

      case INT96 =>
        ParquetSchemaConverter.checkConversionRequirement(
          assumeInt96IsTimestamp,
          "INT96 is not supported unless it's interpreted as timestamp. " +
            s"Please try to set ${SQLConf.PARQUET_INT96_AS_TIMESTAMP.key} to true.")
        TimestampType

      case BINARY =>
        typeAnnotation match {
          case _: StringLogicalTypeAnnotation | _: EnumLogicalTypeAnnotation |
               _: JsonLogicalTypeAnnotation => StringType
          case null if assumeBinaryIsString => StringType
          case null => BinaryType
          case _: BsonLogicalTypeAnnotation => BinaryType
          case _: DecimalLogicalTypeAnnotation => makeDecimalType()
          case _ => illegalType()
        }

      case FIXED_LEN_BYTE_ARRAY =>
        typeAnnotation match {
          case _: DecimalLogicalTypeAnnotation =>
            makeDecimalType(Decimal.maxPrecisionForBytes(parquetType.getTypeLength))
          case _: IntervalLogicalTypeAnnotation => typeNotImplemented()
          case _ => illegalType()
        }

      case _ => illegalType()
    })

    ParquetColumn(sparkType, primitiveColumn)
  }

  private def convertGroupField(
      groupColumn: GroupColumnIO,
      sparkReadType: Option[DataType] = None): ParquetColumn = {
    val field = groupColumn.getType.asGroupType()
    Option(field.getLogicalTypeAnnotation).fold(
      convertInternal(groupColumn, sparkReadType.map(_.asInstanceOf[StructType]))) {
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
      case _: ListLogicalTypeAnnotation =>
        ParquetSchemaConverter.checkConversionRequirement(
          field.getFieldCount == 1, s"Invalid list type $field")
        ParquetSchemaConverter.checkConversionRequirement(
          sparkReadType.forall(_.isInstanceOf[ArrayType]),
          s"Invalid Spark read type: expected $field to be list type but found $sparkReadType")

        val repeated = groupColumn.getChild(0)
        val repeatedType = repeated.getType
        ParquetSchemaConverter.checkConversionRequirement(
          repeatedType.isRepetition(REPEATED), s"Invalid list type $field")
        val sparkReadElementType = sparkReadType.map(_.asInstanceOf[ArrayType].elementType)

        if (isElementType(repeatedType, field.getName)) {
          var converted = convertField(repeated, sparkReadElementType)
          val convertedType = sparkReadElementType.getOrElse(converted.sparkType)

          // legacy format such as:
          //   optional group my_list (LIST) {
          //     repeated int32 element;
          //   }
          // we should mark the primitive field as required
          if (repeatedType.isPrimitive) converted = converted.copy(required = true)

          ParquetColumn(ArrayType(convertedType, containsNull = false),
            groupColumn, Seq(converted))
        } else {
          val element = repeated.asInstanceOf[GroupColumnIO].getChild(0)
          val converted = convertField(element, sparkReadElementType)
          val convertedType = sparkReadElementType.getOrElse(converted.sparkType)
          val optional = element.getType.isRepetition(OPTIONAL)
          ParquetColumn(ArrayType(convertedType, containsNull = optional),
            groupColumn, Seq(converted))
        }

      // scalastyle:off
      // `MAP_KEY_VALUE` is for backwards-compatibility
      // See: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules-1
      // scalastyle:on
      case _: MapLogicalTypeAnnotation | _: MapKeyValueTypeAnnotation =>
        ParquetSchemaConverter.checkConversionRequirement(
          field.getFieldCount == 1 && !field.getType(0).isPrimitive,
          s"Invalid map type: $field")
        ParquetSchemaConverter.checkConversionRequirement(
          sparkReadType.forall(_.isInstanceOf[MapType]),
          s"Invalid Spark read type: expected $field to be map type but found $sparkReadType")

        val keyValue = groupColumn.getChild(0).asInstanceOf[GroupColumnIO]
        val keyValueType = keyValue.getType.asGroupType()
        ParquetSchemaConverter.checkConversionRequirement(
          keyValueType.isRepetition(REPEATED) && keyValueType.getFieldCount == 2,
          s"Invalid map type: $field")

        val key = keyValue.getChild(0)
        val value = keyValue.getChild(1)
        val sparkReadKeyType = sparkReadType.map(_.asInstanceOf[MapType].keyType)
        val sparkReadValueType = sparkReadType.map(_.asInstanceOf[MapType].valueType)
        val convertedKey = convertField(key, sparkReadKeyType)
        val convertedValue = convertField(value, sparkReadValueType)
        val convertedKeyType = sparkReadKeyType.getOrElse(convertedKey.sparkType)
        val convertedValueType = sparkReadValueType.getOrElse(convertedValue.sparkType)
        val valueOptional = value.getType.isRepetition(OPTIONAL)
        ParquetColumn(
          MapType(convertedKeyType, convertedValueType,
            valueContainsNull = valueOptional),
          groupColumn, Seq(convertedKey, convertedValue))
      case _ =>
        throw QueryCompilationErrors.unrecognizedParquetTypeError(field.toString)
    }
  }

  // scalastyle:off
  // Here we implement Parquet LIST backwards-compatibility rules.
  // See: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
  // scalastyle:on
  private[parquet] def isElementType(repeatedType: Type, parentName: String): Boolean = {
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

/**
 * This converter class is used to convert Spark SQL [[StructType]] to Parquet [[MessageType]].
 *
 * @param writeLegacyParquetFormat Whether to use legacy Parquet format compatible with Spark 1.4
 *        and prior versions when converting a Catalyst [[StructType]] to a Parquet [[MessageType]].
 *        When set to false, use standard format defined in parquet-format spec.  This argument only
 *        affects Parquet write path.
 * @param outputTimestampType which parquet timestamp type to use when writing.
 * @param useFieldId whether we should include write field id to Parquet schema. Set this to false
 *        via `spark.sql.parquet.fieldId.write.enabled = false` to disable writing field ids.
 * @param timestampNTZEnabled whether TIMESTAMP_NTZ type support is enabled.
 */
class SparkToParquetSchemaConverter(
    writeLegacyParquetFormat: Boolean = SQLConf.PARQUET_WRITE_LEGACY_FORMAT.defaultValue.get,
    outputTimestampType: SQLConf.ParquetOutputTimestampType.Value =
      SQLConf.ParquetOutputTimestampType.INT96,
    useFieldId: Boolean = SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.defaultValue.get,
    timestampNTZEnabled: Boolean = SQLConf.PARQUET_TIMESTAMP_NTZ_ENABLED.defaultValue.get) {

  def this(conf: SQLConf) = this(
    writeLegacyParquetFormat = conf.writeLegacyParquetFormat,
    outputTimestampType = conf.parquetOutputTimestampType,
    useFieldId = conf.parquetFieldIdWriteEnabled,
    timestampNTZEnabled = conf.parquetTimestampNTZEnabled)

  def this(conf: Configuration) = this(
    writeLegacyParquetFormat = conf.get(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key).toBoolean,
    outputTimestampType = SQLConf.ParquetOutputTimestampType.withName(
      conf.get(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key)),
    useFieldId = conf.get(SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.key).toBoolean,
    timestampNTZEnabled = conf.get(SQLConf.PARQUET_TIMESTAMP_NTZ_ENABLED.key).toBoolean)

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
    val converted = convertField(field, if (field.nullable) OPTIONAL else REQUIRED)
    if (useFieldId && ParquetUtils.hasFieldId(field)) {
      converted.withId(ParquetUtils.getFieldId(field))
    } else {
      converted
    }
  }

  private def convertField(field: StructField, repetition: Type.Repetition): Type = {

    field.dataType match {
      // ===================
      // Simple atomic types
      // ===================

      case BooleanType =>
        Types.primitive(BOOLEAN, repetition).named(field.name)

      case ByteType =>
        Types.primitive(INT32, repetition)
          .as(LogicalTypeAnnotation.intType(8, true)).named(field.name)

      case ShortType =>
        Types.primitive(INT32, repetition)
          .as(LogicalTypeAnnotation.intType(16, true)).named(field.name)

      case IntegerType | _: YearMonthIntervalType =>
        Types.primitive(INT32, repetition).named(field.name)

      case LongType | _: DayTimeIntervalType =>
        Types.primitive(INT64, repetition).named(field.name)

      case FloatType =>
        Types.primitive(FLOAT, repetition).named(field.name)

      case DoubleType =>
        Types.primitive(DOUBLE, repetition).named(field.name)

      case StringType =>
        Types.primitive(BINARY, repetition)
          .as(LogicalTypeAnnotation.stringType()).named(field.name)

      case DateType =>
        Types.primitive(INT32, repetition)
          .as(LogicalTypeAnnotation.dateType()).named(field.name)

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
            Types.primitive(INT64, repetition)
              .as(LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS)).named(field.name)
          case SQLConf.ParquetOutputTimestampType.TIMESTAMP_MILLIS =>
            Types.primitive(INT64, repetition)
              .as(LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS)).named(field.name)
        }

      case TimestampNTZType if timestampNTZEnabled =>
        Types.primitive(INT64, repetition)
          .as(LogicalTypeAnnotation.timestampType(false, TimeUnit.MICROS)).named(field.name)
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
          .as(LogicalTypeAnnotation.decimalType(scale, precision))
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
          .as(LogicalTypeAnnotation.decimalType(scale, precision))
          .named(field.name)

      // Uses INT64 for 1 <= precision <= 18
      case DecimalType.Fixed(precision, scale)
          if precision <= Decimal.MAX_LONG_DIGITS && !writeLegacyParquetFormat =>
        Types
          .primitive(INT64, repetition)
          .as(LogicalTypeAnnotation.decimalType(scale, precision))
          .named(field.name)

      // Uses FIXED_LEN_BYTE_ARRAY for all other precisions
      case DecimalType.Fixed(precision, scale) if !writeLegacyParquetFormat =>
        Types
          .primitive(FIXED_LEN_BYTE_ARRAY, repetition)
          .as(LogicalTypeAnnotation.decimalType(scale, precision))
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
          .buildGroup(repetition).as(LogicalTypeAnnotation.listType())
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
          .buildGroup(repetition).as(LogicalTypeAnnotation.listType())
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
          .buildGroup(repetition).as(LogicalTypeAnnotation.listType())
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
          .buildGroup(repetition).as(LogicalTypeAnnotation.mapType())
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
        throw QueryCompilationErrors.cannotConvertDataTypeToParquetTypeError(field)
    }
  }
}

private[sql] object ParquetSchemaConverter {
  val SPARK_PARQUET_SCHEMA_NAME = "spark_schema"

  val EMPTY_MESSAGE: MessageType =
    Types.buildMessage().named(ParquetSchemaConverter.SPARK_PARQUET_SCHEMA_NAME)

  def checkConversionRequirement(f: => Boolean, message: String): Unit = {
    if (!f) {
      throw new AnalysisException(message)
    }
  }
}
