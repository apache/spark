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

package org.apache.spark.sql.util

import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, IntervalUnit, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import org.apache.spark.SparkException
import org.apache.spark.sql.errors.ExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._

private[sql] object ArrowUtils {

  val rootAllocator = new RootAllocator(Long.MaxValue)

  // todo: support more types.

  /** Maps data type from Spark to Arrow. NOTE: timeZoneId required for TimestampTypes */
  def toArrowType(dt: DataType, timeZoneId: String, largeVarTypes: Boolean = false): ArrowType =
    dt match {
      case BooleanType => ArrowType.Bool.INSTANCE
      case ByteType => new ArrowType.Int(8, true)
      case ShortType => new ArrowType.Int(8 * 2, true)
      case IntegerType => new ArrowType.Int(8 * 4, true)
      case LongType => new ArrowType.Int(8 * 8, true)
      case FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
      case DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
      case _: StringType if !largeVarTypes => ArrowType.Utf8.INSTANCE
      case BinaryType if !largeVarTypes => ArrowType.Binary.INSTANCE
      case _: StringType if largeVarTypes => ArrowType.LargeUtf8.INSTANCE
      case BinaryType if largeVarTypes => ArrowType.LargeBinary.INSTANCE
      case DecimalType.Fixed(precision, scale) => new ArrowType.Decimal(precision, scale, 8 * 16)
      case DateType => new ArrowType.Date(DateUnit.DAY)
      case TimestampType if timeZoneId == null =>
        throw SparkException.internalError("Missing timezoneId where it is mandatory.")
      case TimestampType => new ArrowType.Timestamp(TimeUnit.MICROSECOND, timeZoneId)
      case TimestampNTZType =>
        new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)
      case NullType => ArrowType.Null.INSTANCE
      case _: YearMonthIntervalType => new ArrowType.Interval(IntervalUnit.YEAR_MONTH)
      case _: DayTimeIntervalType => new ArrowType.Duration(TimeUnit.MICROSECOND)
      case CalendarIntervalType => new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO)
      case _ =>
        throw ExecutionErrors.unsupportedDataTypeError(dt)
    }

  def fromArrowType(dt: ArrowType): DataType = dt match {
    case ArrowType.Bool.INSTANCE => BooleanType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 => ByteType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 2 => ShortType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 4 => IntegerType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 8 => LongType
    case float: ArrowType.FloatingPoint
        if float.getPrecision() == FloatingPointPrecision.SINGLE =>
      FloatType
    case float: ArrowType.FloatingPoint
        if float.getPrecision() == FloatingPointPrecision.DOUBLE =>
      DoubleType
    case ArrowType.Utf8.INSTANCE => StringType
    case ArrowType.Binary.INSTANCE => BinaryType
    case ArrowType.LargeUtf8.INSTANCE => StringType
    case ArrowType.LargeBinary.INSTANCE => BinaryType
    case d: ArrowType.Decimal => DecimalType(d.getPrecision, d.getScale)
    case date: ArrowType.Date if date.getUnit == DateUnit.DAY => DateType
    case ts: ArrowType.Timestamp
        if ts.getUnit == TimeUnit.MICROSECOND && ts.getTimezone == null =>
      TimestampNTZType
    case ts: ArrowType.Timestamp if ts.getUnit == TimeUnit.MICROSECOND => TimestampType
    case ArrowType.Null.INSTANCE => NullType
    case yi: ArrowType.Interval if yi.getUnit == IntervalUnit.YEAR_MONTH =>
      YearMonthIntervalType()
    case di: ArrowType.Duration if di.getUnit == TimeUnit.MICROSECOND => DayTimeIntervalType()
    case ci: ArrowType.Interval if ci.getUnit == IntervalUnit.MONTH_DAY_NANO =>
      CalendarIntervalType
    case _ => throw ExecutionErrors.unsupportedArrowTypeError(dt)
  }

  /** Maps field from Spark to Arrow. NOTE: timeZoneId required for TimestampType */
  def toArrowField(
      name: String,
      dt: DataType,
      nullable: Boolean,
      timeZoneId: String,
      largeVarTypes: Boolean = false): Field = {
    dt match {
      case ArrayType(elementType, containsNull) =>
        val fieldType = new FieldType(nullable, ArrowType.List.INSTANCE, null)
        new Field(
          name,
          fieldType,
          Seq(
            toArrowField("element", elementType, containsNull, timeZoneId, largeVarTypes)).asJava)
      case StructType(fields) =>
        val fieldType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null)
        new Field(
          name,
          fieldType,
          fields
            .map { field =>
              toArrowField(field.name, field.dataType, field.nullable, timeZoneId, largeVarTypes)
            }
            .toImmutableArraySeq
            .asJava)
      case MapType(keyType, valueType, valueContainsNull) =>
        val mapType = new FieldType(nullable, new ArrowType.Map(false), null)
        // Note: Map Type struct can not be null, Struct Type key field can not be null
        new Field(
          name,
          mapType,
          Seq(
            toArrowField(
              MapVector.DATA_VECTOR_NAME,
              new StructType()
                .add(MapVector.KEY_NAME, keyType, nullable = false)
                .add(MapVector.VALUE_NAME, valueType, nullable = valueContainsNull),
              nullable = false,
              timeZoneId,
              largeVarTypes)).asJava)
      case udt: UserDefinedType[_] =>
        toArrowField(name, udt.sqlType, nullable, timeZoneId, largeVarTypes)
      case _: VariantType =>
        val fieldType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null)
        // The metadata field is tagged with additional metadata so we can identify that the arrow
        // struct actually represents a variant schema.
        val metadataFieldType = new FieldType(
          false,
          toArrowType(BinaryType, timeZoneId, largeVarTypes),
          null,
          Map("variant" -> "true").asJava)
        new Field(
          name,
          fieldType,
          Seq(
            toArrowField("value", BinaryType, false, timeZoneId, largeVarTypes),
            new Field("metadata", metadataFieldType, Seq.empty[Field].asJava)).asJava)
      case dataType =>
        val fieldType =
          new FieldType(nullable, toArrowType(dataType, timeZoneId, largeVarTypes), null)
        new Field(name, fieldType, Seq.empty[Field].asJava)
    }
  }

  def isVariantField(field: Field): Boolean = {
    assert(field.getType.isInstanceOf[ArrowType.Struct])
    field.getChildren.asScala
      .map(_.getName)
      .asJava
      .containsAll(Seq("value", "metadata").asJava) && field.getChildren.asScala.exists { child =>
      child.getName == "metadata" && child.getMetadata.getOrDefault("variant", "false") == "true"
    }
  }

  def fromArrowField(field: Field): DataType = {
    field.getType match {
      case _: ArrowType.Map =>
        val elementField = field.getChildren.get(0)
        val keyType = fromArrowField(elementField.getChildren.get(0))
        val valueType = fromArrowField(elementField.getChildren.get(1))
        MapType(keyType, valueType, elementField.getChildren.get(1).isNullable)
      case ArrowType.List.INSTANCE =>
        val elementField = field.getChildren().get(0)
        val elementType = fromArrowField(elementField)
        ArrayType(elementType, containsNull = elementField.isNullable)
      case ArrowType.Struct.INSTANCE if isVariantField(field) =>
        VariantType
      case ArrowType.Struct.INSTANCE =>
        val fields = field.getChildren().asScala.map { child =>
          val dt = fromArrowField(child)
          StructField(child.getName, dt, child.isNullable)
        }
        StructType(fields.toArray)
      case arrowType => fromArrowType(arrowType)
    }
  }

  /**
   * Maps schema from Spark to Arrow. NOTE: timeZoneId required for TimestampType in StructType
   */
  def toArrowSchema(
      schema: StructType,
      timeZoneId: String,
      errorOnDuplicatedFieldNames: Boolean,
      largeVarTypes: Boolean = false): Schema = {
    new Schema(schema.map { field =>
      toArrowField(
        field.name,
        deduplicateFieldNames(field.dataType, errorOnDuplicatedFieldNames),
        field.nullable,
        timeZoneId,
        largeVarTypes)
    }.asJava)
  }

  def fromArrowSchema(schema: Schema): StructType = {
    StructType(schema.getFields.asScala.map { field =>
      val dt = fromArrowField(field)
      StructField(field.getName, dt, field.isNullable)
    }.toArray)
  }

  private def deduplicateFieldNames(
      dt: DataType,
      errorOnDuplicatedFieldNames: Boolean): DataType = dt match {
    case udt: UserDefinedType[_] =>
      deduplicateFieldNames(udt.sqlType, errorOnDuplicatedFieldNames)
    case st @ StructType(fields) =>
      val newNames = if (st.names.toSet.size == st.names.length) {
        st.names
      } else {
        if (errorOnDuplicatedFieldNames) {
          throw ExecutionErrors.duplicatedFieldNameInArrowStructError(
            st.names.toImmutableArraySeq)
        }
        val genNawName = st.names.groupBy(identity).map {
          case (name, names) if names.length > 1 =>
            val i = new AtomicInteger()
            name -> { () => s"${name}_${i.getAndIncrement()}" }
          case (name, _) => name -> { () => name }
        }
        st.names.map(genNawName(_)())
      }
      val newFields =
        fields.zip(newNames).map { case (StructField(_, dataType, nullable, metadata), name) =>
          StructField(
            name,
            deduplicateFieldNames(dataType, errorOnDuplicatedFieldNames),
            nullable,
            metadata)
        }
      StructType(newFields)
    case ArrayType(elementType, containsNull) =>
      ArrayType(deduplicateFieldNames(elementType, errorOnDuplicatedFieldNames), containsNull)
    case MapType(keyType, valueType, valueContainsNull) =>
      MapType(
        deduplicateFieldNames(keyType, errorOnDuplicatedFieldNames),
        deduplicateFieldNames(valueType, errorOnDuplicatedFieldNames),
        valueContainsNull)
    case _ => dt
  }
}
