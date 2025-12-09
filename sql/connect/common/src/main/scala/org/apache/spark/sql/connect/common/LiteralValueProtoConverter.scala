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

package org.apache.spark.sql.connect.common

import java.lang.{Boolean => JBoolean, Byte => JByte, Character => JChar, Double => JDouble, Float => JFloat, Integer => JInteger, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Timestamp}
import java.time._

import scala.collection.{immutable, mutable}
import scala.util.Try

import com.google.protobuf.ByteString

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.Expression.Literal
import org.apache.spark.connect.proto.Expression.Literal.{Array => CArray, LiteralTypeCase, Map => CMap, Struct}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.catalyst.util.{SparkDateTimeUtils, SparkIntervalUtils}
import org.apache.spark.sql.connect.common.InferringDataTypeBuilder.mergeDataTypes
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Conversions from Scala literals to Connect literals.
 */
object LiteralValueProtoConverter {
  private val missing: Any = new Object

  case class ToLiteralProtoOptions(useDeprecatedDataTypeFields: Boolean)

  /**
   * Transforms literal value to the `proto.Expression.Literal`.
   *
   * @return
   *   proto.Expression.Literal
   */
  def toLiteralProto(literal: Any): Literal = {
    toLiteralProtoWithOptions(
      literal,
      None,
      ToLiteralProtoOptions(useDeprecatedDataTypeFields = true))
  }

  def toLiteralProto(literal: Any, dataType: DataType): Literal = {
    toLiteralProtoWithOptions(
      literal,
      Some(dataType),
      ToLiteralProtoOptions(useDeprecatedDataTypeFields = true))
  }

  private[connect] def toLiteralProtoWithOptions(
      value: Any,
      dataTypeOpt: Option[DataType],
      options: ToLiteralProtoOptions): Literal = {
    val dataTypeBuilder = dataTypeOpt
      .map(dataType => FixedDataTypeBuilder(dataType, value == null))
      .getOrElse(InferringDataTypeBuilder())
    val (literal, _) = toLiteralProtoBuilderInternal(
      value,
      dataTypeBuilder,
      options,
      enclosed = false)
    literal
  }

  private def toLiteralProtoBuilderInternal(
      value: Any,
      dataTypeBuilder: DataTypeBuilder,
      options: ToLiteralProtoOptions,
      enclosed: Boolean): (Literal, DataTypeBuilder) = {
    def result(
        f: Literal.Builder => Literal.Builder,
        dataType: DataType): (Literal, DataTypeBuilder) = {
      val literal = f(Literal.newBuilder()).build()
      (literal, dataTypeBuilder.merge(dataType, literal.hasNull))
    }
    value match {
      case null | None =>
        val dataType = dataTypeBuilder.result()
        val protoDataType = if (enclosed) {
          // Enclosed NULL value. The dataType is recorded in the enclosing dataType.
          ProtoDataTypes.NullType
        } else {
          // Standalone NULL value. This needs the actual dataType.
          DataTypeProtoConverter.toConnectProtoType(dataType)
        }
        result(_.setNull(protoDataType), dataType)
      case v: Boolean =>
        result(_.setBoolean(v), BooleanType)
      case v: Byte =>
        result(_.setByte(v), ByteType)
      case v: Short =>
        result(_.setShort(v), ShortType)
      case v: Int =>
        result(_.setInteger(v), IntegerType)
      case v: Long =>
        result(_.setLong(v), LongType)
      case v: Float =>
        result(_.setFloat(v), FloatType)
      case v: Double =>
        result(_.setDouble(v), DoubleType)
      case v: BigDecimal =>
        val dataType = DecimalType(v.precision, v.scale)
        result(_.setDecimal(toProtoDecimal(v.toString(), dataType)), dataType)
      case v: JBigDecimal =>
        val dataType = DecimalType(v.precision, v.scale)
        result(_.setDecimal(toProtoDecimal(v.toString, dataType)), dataType)
      case v: Decimal =>
        val dataType = DecimalType(Math.max(v.precision, v.scale), v.scale)
        result(_.setDecimal(toProtoDecimal(v.toString, dataType)), dataType)
      case v: String =>
        result(_.setString(v), StringType)
      case v: Char =>
        result(_.setString(v.toString), StringType)
      case v: Array[Char] =>
        result(_.setString(String.valueOf(v)), StringType)
      case v: Array[Byte] =>
        result(_.setBinary(ByteString.copyFrom(v)), BinaryType)
      case v: Instant =>
        result(_.setTimestamp(SparkDateTimeUtils.instantToMicros(v)), TimestampType)
      case v: Timestamp =>
        result(_.setTimestamp(SparkDateTimeUtils.fromJavaTimestamp(v)), TimestampType)
      case v: LocalDateTime =>
        result(_.setTimestampNtz(SparkDateTimeUtils.localDateTimeToMicros(v)), TimestampNTZType)
      case v: LocalDate =>
        result(_.setDate(v.toEpochDay.toInt), DateType)
      case v: Date =>
        result(_.setDate(SparkDateTimeUtils.fromJavaDate(v)), DateType)
      case v: LocalTime =>
        val time = Literal.Time.newBuilder()
          .setNano(SparkDateTimeUtils.localTimeToNanos(v))
          .setPrecision(TimeType.DEFAULT_PRECISION)
          .build()
        result(_.setTime(time), TimeType())
      case v: Duration =>
        result(_.setDayTimeInterval(SparkIntervalUtils.durationToMicros(v)), DayTimeIntervalType())
      case v: Period =>
        result(
          _.setYearMonthInterval(SparkIntervalUtils.periodToMonths(v)),
          YearMonthIntervalType())
      case v: CalendarInterval =>
        val interval = Literal.CalendarInterval.newBuilder()
          .setMonths(v.months)
          .setDays(v.days)
          .setMicroseconds(v.microseconds)
          .build()
        result(_.setCalendarInterval(interval), CalendarIntervalType)
      case v: mutable.ArraySeq[_] =>
        toProtoArrayOrBinary(v.array, dataTypeBuilder, options, enclosed)
      case v: immutable.ArraySeq[_] =>
        toProtoArrayOrBinary((v.unsafeArray, dataTypeBuilder, options, enclosed)
      case v: Array[_] =>
        toProtoArray(v, dataTypeBuilder, options, enclosed)
      case s: scala.collection.Seq[_] =>
        toProtoArray(s, dataTypeBuilder, options, enclosed, () => None)
      case map: scala.collection.Map[_, _] =>
        toProtoMap(map, dataTypeBuilder, options, enclosed)
      case Some(value) =>
        toLiteralProtoBuilderInternal(value, dataTypeBuilder, options, enclosed)
      case product: Product =>
        // If we don't have a schema, we could try to extract one from the class.
        toProtoStruct(product.productIterator, dataTypeBuilder, options, enclosed)
      case row: Row =>
        var structTypeBuilder = dataTypeBuilder
        if (row.schema != null) {
          structTypeBuilder = structTypeBuilder.merge(row.schema, isNullable = false)
        }
        toProtoStruct(row.toSeq.iterator, structTypeBuilder, options, enclosed)
      case v =>
        throw new UnsupportedOperationException(s"literal $v not supported (yet).")
    }
  }

  private def toProtoDecimal(value: String, dataType: DecimalType): Literal.Decimal = {
    Literal.Decimal.newBuilder().setValue(value)
      .setPrecision(dataType.precision)
      .setScale(dataType.scale)
      .build()
  }

  private def toProtoMap(
      map: scala.collection.Map[_, _],
      mapTypeBuilder: DataTypeBuilder,
      options: ToLiteralProtoOptions,
      enclosed: Boolean): (Literal, DataTypeBuilder) = {
    var (keyTypeBuilder, valueTypeBuilder) = mapTypeBuilder.keyValueTypeBuilder()
    val builder = Literal.newBuilder()
    val mapBuilder = builder.getMapBuilder
    map.foreach { case (k, v) =>
      val (keyLiteral, updatedKeyTypeBuilder) = toLiteralProtoBuilderInternal(
        k, keyTypeBuilder, options, enclosed = true)
      mapBuilder.addKeys(keyLiteral)
      keyTypeBuilder = updatedKeyTypeBuilder
      val (valueLiteral, updatedValueTypeBuilder) = toLiteralProtoBuilderInternal(
        v, valueTypeBuilder, options, enclosed = true)
      mapBuilder.addValues(valueLiteral)
      valueTypeBuilder = updatedValueTypeBuilder
    }
    val updatedMapTypeBuilder = mapTypeBuilder.mergeKeyValueBuilder(
      keyTypeBuilder,
      valueTypeBuilder)
    lazy val protoMapType = DataTypeProtoConverter
      .toConnectProtoType(updatedMapTypeBuilder.result())
      .getMap
    if (options.useDeprecatedDataTypeFields) {
      mapBuilder.setKeyType(protoMapType.getKeyType)
      mapBuilder.setValueType(protoMapType.getValueType)
    } else if (!enclosed && updatedMapTypeBuilder.mustRecordDataType) {
      mapBuilder.setDataType(protoMapType)
    }
    (builder.build(), updatedMapTypeBuilder)
  }

  private def toProtoArrayOrBinary(
      array: Array[_],
      arrayTypeBuilder: DataTypeBuilder,
      options: ToLiteralProtoOptions,
      enclosed: Boolean): (Literal, DataTypeBuilder) = {
    (array, arrayTypeBuilder) match {
      case (_: Array[Byte], _: InferringDataTypeBuilder | FixedDataTypeBuilder(BinaryType, _))=>
        toLiteralProtoBuilderInternal(array, arrayTypeBuilder, options, enclosed)
      case _ =>
        toProtoArray(array, arrayTypeBuilder, options, enclosed)
    }
  }

  private def toProtoArray(
      array: Array[_],
      arrayTypeBuilder: DataTypeBuilder,
      options: ToLiteralProtoOptions,
      enclosed: Boolean): (Literal, DataTypeBuilder) = {
    val fallback = () => Try(toDataType(array.getClass.getComponentType)).toOption
    toProtoArray(array, arrayTypeBuilder, options, enclosed, fallback)
  }

  private def toProtoArray(
      iterable: Iterable[_],
      arrayTypeBuilder: DataTypeBuilder,
      options: ToLiteralProtoOptions,
      enclosed: Boolean,
      fallbackElementTypeGetter: () => Option[DataType]): (Literal, DataTypeBuilder) = {
    var elementTypeBuilder = arrayTypeBuilder.elementTypeBuilder()
    val builder = Literal.newBuilder()
    val arrayBuilder = builder.getArrayBuilder
    iterable.foreach { e =>
      val (literal, updatedElementTypeBuilder) = toLiteralProtoBuilderInternal(
        e, elementTypeBuilder, options, enclosed = true)
      arrayBuilder.addElements(literal)
      elementTypeBuilder = updatedElementTypeBuilder
    }
    if (!elementTypeBuilder.isDefined) {
      val fallbackElementType = fallbackElementTypeGetter()
      if (fallbackElementType.isDefined) {
        elementTypeBuilder.merge(fallbackElementType.get, isNullable = false)
      }
    }
    val updatedArrayTypeBuilder = arrayTypeBuilder.mergeElementTypeBuilder(elementTypeBuilder)
    lazy val protoArrayType = DataTypeProtoConverter
      .toConnectProtoType(updatedArrayTypeBuilder.result())
      .getArray
    if (options.useDeprecatedDataTypeFields) {
      arrayBuilder.setElementType(protoArrayType.getElementType)
    } else if (!enclosed && updatedArrayTypeBuilder.mustRecordDataType) {
      arrayBuilder.setDataType(protoArrayType)
    }
    (builder.build(), updatedArrayTypeBuilder)
  }

  private def toProtoStruct(
      fields: Iterator[_],
      structTypeBuilder: DataTypeBuilder,
      options: ToLiteralProtoOptions,
      enclosed: Boolean): (Literal, DataTypeBuilder) = {
    val structType = structTypeBuilder.result().asInstanceOf[StructType]
    val builder = Literal.newBuilder()
    val structBuilder = builder.getStructBuilder
    fields.zipAll(structType.fields, missing, null).foreach {
      case (value, field: StructField) =>
        require(missing != null)
        require(field != null)
        val (literal, _) = toLiteralProtoBuilderInternal(
          value,
          FixedDataTypeBuilder(field.dataType, field.nullable),
          options,
          enclosed = true)
        structBuilder.addElements(literal)
    }
    def protoStructType = DataTypeProtoConverter.toConnectProtoType(structType)
    if (options.useDeprecatedDataTypeFields) {
     structBuilder.setStructType(protoStructType)
    } else if (!enclosed) {
      structBuilder.setDataTypeStruct(protoStructType.getStruct)
    }
    (builder.build(), structTypeBuilder)
  }

  private def toDataType(clz: Class[_]): DataType = clz match {
    // primitive types
    case JShort.TYPE => ShortType
    case JInteger.TYPE => IntegerType
    case JLong.TYPE => LongType
    case JDouble.TYPE => DoubleType
    case JByte.TYPE => ByteType
    case JFloat.TYPE => FloatType
    case JBoolean.TYPE => BooleanType
    case JChar.TYPE => StringType

    // java classes
    case _ if clz == classOf[LocalDate] || clz == classOf[Date] => DateType
    case _ if clz == classOf[Instant] || clz == classOf[Timestamp] => TimestampType
    case _ if clz == classOf[LocalDateTime] => TimestampNTZType
    case _ if clz == classOf[LocalTime] => TimeType(TimeType.DEFAULT_PRECISION)
    case _ if clz == classOf[Duration] => DayTimeIntervalType.DEFAULT
    case _ if clz == classOf[Period] => YearMonthIntervalType.DEFAULT
    case _ if clz == classOf[JBigDecimal] => DecimalType.SYSTEM_DEFAULT
    case _ if clz == classOf[Array[Byte]] => BinaryType
    case _ if clz == classOf[Array[Char]] => StringType
    case _ if clz == classOf[JShort] => ShortType
    case _ if clz == classOf[JInteger] => IntegerType
    case _ if clz == classOf[JLong] => LongType
    case _ if clz == classOf[JDouble] => DoubleType
    case _ if clz == classOf[JByte] => ByteType
    case _ if clz == classOf[JFloat] => FloatType
    case _ if clz == classOf[JBoolean] => BooleanType
    case _ if clz == classOf[JChar] => StringType

    // other scala classes
    case _ if clz == classOf[String] => StringType
    case _ if clz == classOf[BigInt] => DecimalType.SYSTEM_DEFAULT
    case _ if clz == classOf[BigDecimal] => DecimalType.SYSTEM_DEFAULT
    case _ if clz == classOf[Decimal] => DecimalType.SYSTEM_DEFAULT
    case _ if clz == classOf[CalendarInterval] => CalendarIntervalType
    case _ if clz.isArray => ArrayType(toDataType(clz.getComponentType))
    case _ =>
      throw new UnsupportedOperationException(s"Unsupported component type $clz in arrays.")
  }
}

/**
 * Base trait for converting a [[proto.Expression.Literal]] into either its Scala representation or
 * into its Catalyst representation.
 */
trait FromProtoConvertor {
  def convertToValue(literal: proto.Expression.Literal): Any = {
    convert(literal, EmptyDataTypeBuilder)._2
  }

  def convert(literal: proto.Expression.Literal): (DataType, Any) = {
    val (builder, value) = convert(literal, EmptyDataTypeBuilder)
    (builder.result(), value)
  }

  private def convert(
      literal: proto.Expression.Literal,
      dataTypeBuilder: DataTypeBuilder): (DataTypeBuilder, Any) = {
    def result(dataType: DataType, value: Any): (DataTypeBuilder, Any) = {
      (dataTypeBuilder.merge(dataType, value == null), value)
    }
    literal.getLiteralTypeCase match {
      case LiteralTypeCase.NULL =>
        result(DataTypeProtoConverter.toCatalystType(literal.getNull), null)
      case LiteralTypeCase.BOOLEAN =>
        result(BooleanType, literal.getBoolean)
      case LiteralTypeCase.BYTE =>
        result(ByteType, literal.getByte.toByte)
      case LiteralTypeCase.SHORT =>
        result(ShortType, literal.getShort.toShort)
      case LiteralTypeCase.INTEGER =>
        result(IntegerType, literal.getInteger)
      case LiteralTypeCase.LONG =>
        result(LongType, literal.getLong)
      case LiteralTypeCase.FLOAT =>
        result(FloatType, literal.getFloat)
      case LiteralTypeCase.DOUBLE =>
        result(DoubleType, literal.getDouble)
      case LiteralTypeCase.DECIMAL =>
        val d = literal.getDecimal
        val decimal = Decimal(d.getValue)
        if (d.hasPrecision && d.getPrecision != decimal.precision) {
          throw InvalidPlanInput("<DECIMAL PRECISION MISMATCH>")
        }
        if (d.hasScale && d.getScale != decimal.scale) {
          throw InvalidPlanInput("<DECIMAL SCALE MISMATCH>")
        }
        result(DecimalType(decimal.precision, decimal.scale), decimal)
      case LiteralTypeCase.TIME =>
        val time = literal.getTime
        val dataType = if (time.hasPrecision) {
          TimeType(time.getPrecision)
        } else {
          TimeType()
        }
        result(dataType, convertTime(time))
      case LiteralTypeCase.DATE =>
        result(DateType, convertDate(literal.getDate))
      case LiteralTypeCase.TIMESTAMP =>
        result(TimestampType, convertTimestamp(literal.getTimestamp))
      case LiteralTypeCase.TIMESTAMP_NTZ =>
        result(TimestampNTZType, convertTimestampNTZ(literal.getTimestampNtz))
      case LiteralTypeCase.STRING =>
        // We do not support collations yet.
        result(StringType, convertString(literal.getStringBytes))
      case LiteralTypeCase.BINARY =>
        result(BinaryType, literal.getBinary.toByteArray)
      case LiteralTypeCase.CALENDAR_INTERVAL =>
        val i = literal.getCalendarInterval
        result(
          CalendarIntervalType,
          new CalendarInterval(i.getMonths, i.getDays, i.getMicroseconds))
      case LiteralTypeCase.DAY_TIME_INTERVAL =>
        result(DayTimeIntervalType.DEFAULT, convertDayTimeInterval(literal.getDayTimeInterval))
      case LiteralTypeCase.YEAR_MONTH_INTERVAL =>
        result(
          YearMonthIntervalType.DEFAULT,
          convertYearMonthInterval(literal.getYearMonthInterval))
      case LiteralTypeCase.ARRAY =>
        convertArray(literal.getArray, dataTypeBuilder)
      case LiteralTypeCase.MAP =>
        convertMap(literal.getMap, dataTypeBuilder)
      case LiteralTypeCase.STRUCT =>
        var structTypeBuilder = dataTypeBuilder
        val struct = literal.getStruct
        if (struct.hasDataTypeStruct) {
          val structType = DataTypeProtoConverter.toCatalystStructType(struct.getDataTypeStruct)
          structTypeBuilder = structTypeBuilder.merge(structType, isNullable = false)
        }
        if (struct.hasStructType) {
          val structType = DataTypeProtoConverter.toCatalystType(struct.getStructType)
          structTypeBuilder = structTypeBuilder.merge(structType, isNullable = false)
        }
        val result = structTypeBuilder.result() match {
          case structType: StructType =>
            convertStruct(struct, structType)
          case udt: UserDefinedType[_] =>
            convertUdt(struct, udt)
          case _ =>
            throw InvalidPlanInput("<NO STRUCT SCHEMA>")
        }
        (structTypeBuilder, result)
      case literalTypeCase =>
        // At some point we may want to support specialized arrays...
        throw InvalidPlanInput(
          s"Unsupported Literal Type: ${literalTypeCase.name}(${literalTypeCase.getNumber})")
    }
  }

  private def convertArray(
      array: CArray,
      arrayTypeBuilder: DataTypeBuilder): (DataTypeBuilder, Any) = {
    var elementTypeBuilder = arrayTypeBuilder.elementTypeBuilder()
    if (array.hasDataType) {
      val arrayType = DataTypeProtoConverter.toCatalystArrayType(array.getDataType)
      elementTypeBuilder = elementTypeBuilder.merge(
        arrayType.elementType,
        arrayType.containsNull)
    }
    if (array.hasElementType) {
      elementTypeBuilder = elementTypeBuilder.merge(
        DataTypeProtoConverter.toCatalystType(array.getElementType),
        isNullable = true)
    }
    val numElements = array.getElementsCount
    val builder = arrayBuilder(numElements)
    var i = 0
    while (i < numElements) {
      val (updatedElementTypeBuilder, element) = convert(
        array.getElements(i),
        elementTypeBuilder)
      elementTypeBuilder = updatedElementTypeBuilder
      builder += element
      i += 1
    }
    (arrayTypeBuilder.mergeElementTypeBuilder(elementTypeBuilder), builder.result())
  }

  private def convertMap(map: CMap, dataTypeBuilder: DataTypeBuilder): (DataTypeBuilder, Any) = {
    var (keyTypeBuilder, valueTypeBuilder): (DataTypeBuilder, DataTypeBuilder) =
      dataTypeBuilder.keyValueTypeBuilder()
    if (map.hasDataType) {
      val mapType = DataTypeProtoConverter.toCatalystMapType(map.getDataType)
      keyTypeBuilder = keyTypeBuilder.merge(mapType.keyType, isNullable = false)
      valueTypeBuilder = valueTypeBuilder.merge(mapType.valueType, mapType.valueContainsNull)
    }
    if (map.hasKeyType) {
      val keyType = DataTypeProtoConverter.toCatalystType(map.getKeyType)
      keyTypeBuilder = keyTypeBuilder.merge(keyType, isNullable = false)
    }
    if (map.hasValueType) {
      val valueType = DataTypeProtoConverter.toCatalystType(map.getValueType)
      valueTypeBuilder = valueTypeBuilder.merge(valueType, isNullable = true)
    }
    val numElements = map.getKeysCount
    if (numElements != map.getValuesCount) {
      throw InvalidPlanInput("<LENGTH MISMATCH>")
    }
    val builder = mapBuilder(numElements)
    var i = 0
    while (i < numElements) {
      val (updatedKeyTypeBuilder, key) = convert(map.getKeys(i), keyTypeBuilder)
      val (updatedValueTypeBuilder, value) = convert(map.getValues(i), valueTypeBuilder)
      keyTypeBuilder = updatedKeyTypeBuilder
      valueTypeBuilder = updatedValueTypeBuilder
      builder += key -> value
      i += 1
    }
    (dataTypeBuilder.mergeKeyValueBuilder(keyTypeBuilder, valueTypeBuilder), builder.result())
  }

  private def convertStruct(struct: Struct, structType: StructType): Any = {
    val numFields = structType.length
    if (numFields != struct.getElementsCount) {
      throw InvalidPlanInput("<NUM FIELDS MISMATCH>")
    }
    val builder = structBuilder(structType)
    var i = 0
    while (i < numFields) {
      val field = structType(i)
      val fieldDataTypeBuilder = FixedDataTypeBuilder(field.dataType, field.nullable)
      val (_, value) = convert(struct.getElements(i), fieldDataTypeBuilder)
      builder += value
      i += 1
    }
    builder.result()
  }

  protected def convertUdt(struct: Struct, udt: UserDefinedType[_]): Any =
    throw InvalidPlanInput("<UDT_NOT_SUPPORTED")

  protected def convertString(string: ByteString): Any
  protected def convertTime(value: proto.Expression.Literal.Time): Any
  protected def convertDate(value: Int): Any
  protected def convertTimestamp(value: Long): Any
  protected def convertTimestampNTZ(value: Long): Any
  protected def convertDayTimeInterval(value: Long): Any
  protected def convertYearMonthInterval(value: Int): Any

  protected def arrayBuilder(size: Int): mutable.Builder[Any, Any]
  protected def mapBuilder(size: Int): mutable.Builder[(Any, Any), Any]
  protected def structBuilder(schema: StructType): mutable.Builder[Any, Any]
}

/**
 * Class for converting a [[proto.Expression.Literal]] into either its Scala representation.
 */
class FromProtoToScalaConverter extends FromProtoConvertor {
  override protected def convertString(value: ByteString): String = value.toStringUtf8
  override protected def convertTime(value: Literal.Time): Any =
    SparkDateTimeUtils.nanosToLocalTime(value.getNano)
  override protected def convertDate(value: Int): Any =
    SparkDateTimeUtils.toJavaDate(value)
  override protected def convertTimestamp(value: Long): Any =
    SparkDateTimeUtils.toJavaTimestamp(value)
  override protected def convertTimestampNTZ(value: Long): Any =
    SparkDateTimeUtils.microsToLocalDateTime(value)
  override protected def convertDayTimeInterval(value: Long): Any =
    SparkIntervalUtils.microsToDuration(value)
  override protected def convertYearMonthInterval(value: Int): Any =
    SparkIntervalUtils.monthsToPeriod(value)
  override protected def arrayBuilder(size: Int): mutable.Builder[Any, Any] = Seq.newBuilder
  override protected def mapBuilder(size: Int): mutable.Builder[(Any, Any), Any] = Map.newBuilder
  override protected def structBuilder(schema: StructType): mutable.Builder[Any, Any] = {
    new mutable.Builder[Any, Row] {
      private var index = 0
      private var values: Array[Any] = _
      clear()

      override def clear(): Unit = {
        index = 0
        values = new Array[Any](schema.length)
      }

      override def addOne(elem: Any): this.type = {
        values(index) = elem
        index += 1
        this
      }

      override def result(): Row = {
        assert(index == values.length)
        new GenericRowWithSchema(values, schema)
      }
    }
  }
}

object FromProtoToScalaConverter extends FromProtoToScalaConverter

sealed trait DataTypeBuilder {
  def merge(dataType: DataType, isNullable: Boolean): DataTypeBuilder
  def nullable(): Boolean
  def result(): DataType
  def isDefined: Boolean
  def mustRecordDataType: Boolean

  protected def throwIncompatibleDataTypeException(
      expectedDataType: DataType,
      mergeDataType: DataType): Nothing = {
    throw InvalidPlanInput(s"Literal contains a field of DataType `$mergeDataType` which is " +
      s"incompatible with the expected DataType `$expectedDataType`")
  }

  def keyValueTypeBuilder(): (DataTypeBuilder, DataTypeBuilder) = this match {
    case FixedDataTypeBuilder(mapType: MapType, _) =>
      (FixedDataTypeBuilder(mapType.keyType, isNullable = false),
        FixedDataTypeBuilder(mapType.valueType, mapType.valueContainsNull))
    case _: FixedDataTypeBuilder => throw InvalidPlanInput("<NO MAP TYPE>")
    case _ => (InferringDataTypeBuilder(), InferringDataTypeBuilder())
  }

  def mergeKeyValueBuilder(
      keyTypeBuilder: DataTypeBuilder,
      valueTypeBuilder: DataTypeBuilder): DataTypeBuilder = {
    if (keyTypeBuilder.nullable()) {
      throw InvalidPlanInput("<KEY NOT NULL>")
    }
    val mapType = MapType(
      keyTypeBuilder.result(),
      valueTypeBuilder.result(),
      valueTypeBuilder.nullable())
    merge(mapType, isNullable = false)
  }

  def elementTypeBuilder(): DataTypeBuilder = this match {
    case FixedDataTypeBuilder(ArrayType(elementType, containsNull), _) =>
      FixedDataTypeBuilder(elementType, containsNull)
    case _: FixedDataTypeBuilder => throw InvalidPlanInput("<NO ARRAY TYPE>")
    case _ => InferringDataTypeBuilder()
  }

  def mergeElementTypeBuilder(elementTypeBuilder: DataTypeBuilder): DataTypeBuilder = {
    val arrayType = ArrayType(elementTypeBuilder.result(), elementTypeBuilder.nullable())
    merge(arrayType, isNullable = false)
  }
}

object EmptyDataTypeBuilder extends DataTypeBuilder {
  override def merge(dataType: DataType, isNull: Boolean): DataTypeBuilder =
    FixedDataTypeBuilder(dataType, isNull)
  override def nullable(): Boolean = throw new NoSuchElementException("nullable()")
  override def result(): DataType = throw new NoSuchElementException("result()")
  override def isDefined: Boolean = false
  override def mustRecordDataType: Boolean = false
}

case class FixedDataTypeBuilder(dataType: DataType, isNullable: Boolean) extends DataTypeBuilder {
  override def merge(mergeDataType: DataType, mergeIsNull: Boolean): DataTypeBuilder = {
    if (!isNullable && mergeIsNull) {
      throw InvalidPlanInput("<NO NULLS ALLOWED>")
    }
    val ok = (dataType, mergeDataType) match {
      case (l: DecimalType, r: DecimalType) =>
        l.isWiderThan(r)
      case (_, _: NullType) =>
        mergeIsNull
      case _ =>
        // A merged dataType is allowed to be more strict than the dataType we are enforcing.
        DataType.equalsIgnoreCompatibleNullability(mergeDataType, dataType)
    }
    if (!ok) {
      throwIncompatibleDataTypeException(dataType, mergeDataType)
    }
    this
  }
  override def nullable(): Boolean = isNullable
  override def result(): DataType = dataType
  override def isDefined: Boolean = true

  // This can be more subtle. We could also check if the inferred
  // dataType matches the fixed dataType.
  override def mustRecordDataType: Boolean = true
}

case class InferringDataTypeBuilder() extends DataTypeBuilder {
  private var dataType: DataType = NullType
  private var isNullable: Boolean = false
  override def isDefined: Boolean = dataType != NullType

  override def mustRecordDataType: Boolean = dataType.existsRecursively { dt =>
    dt.isInstanceOf[StructType] || dt == NullType
  }

  override def merge(mergeDataType: DataType, mergeIsNull: Boolean): DataTypeBuilder = {
    assert(mergeIsNull || mergeDataType != NullType)
    dataType = mergeDataTypes(dataType, mergeDataType).getOrElse {
      throwIncompatibleDataTypeException(dataType, mergeDataType)
    }
    isNullable |= mergeIsNull
    this
  }
  override def nullable(): Boolean = isNullable
  override def result(): DataType = dataType
}

object InferringDataTypeBuilder {
  def mergeDataTypes(left: DataType, right: DataType): Option[DataType] = {
    (left, right) match {
      // Add Decimal support...
      case (l, r) if l eq r => Some(l)
      case (l: AtomicType, r: AtomicType) if l == r => Some(l)
      case (l: DecimalType, r: DecimalType) =>
        if (l.isWiderThan(r)) {
          Some(l)
        } else if (r.isWiderThan(l)) {
          Some(r)
        } else {
          None
        }
      case (_: AtomicType, _: AtomicType) => None
      case (_: NullType, dt) if !dt.isInstanceOf[NullType] => Some(dt)
      case (dt, _: NullType) if !dt.isInstanceOf[NullType] => Some(dt)
      case (l: NullType, _: NullType) => Some(l)

      case (ArrayType(leftElemType, leftNulls),
            ArrayType(rightElemType, rightNulls)) =>
        mergeDataTypes(leftElemType, rightElemType).map { elemType =>
          ArrayType(elemType, leftNulls || rightNulls)
        }
      case (MapType(leftKeyType, leftValueType, leftValueNulls),
            MapType(rightKeyType, rightValueType, rightValueNulls)) =>
        mergeDataTypes(leftKeyType, rightKeyType)
          .zip(mergeDataTypes(leftValueType, rightValueType))
          .map {
            case (keyType, valueType) =>
              MapType(keyType, valueType, leftValueNulls || rightValueNulls)
          }
      case (StructType(leftFields), StructType(rightFields)) =>
        if (leftFields.length != rightFields.length) {
          return None
        }
        val fields = leftFields.zip(rightFields).flatMap {
          case (leftField, rightField) if leftField.name == rightField.name =>
            mergeDataTypes(leftField.dataType, rightField.dataType).map { fieldType =>
              val nullable = leftField.nullable || rightField.nullable
              leftField.copy(dataType = fieldType, nullable = nullable)
            }
          case _ => None
        }
        if (fields.length == leftFields.length) {
          Some(StructType(fields))
        } else {
          None
        }
      case _ => None
    }
  }
}
