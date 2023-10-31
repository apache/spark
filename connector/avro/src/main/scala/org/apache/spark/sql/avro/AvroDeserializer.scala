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

package org.apache.spark.sql.avro

import java.math.BigDecimal
import java.nio.ByteBuffer

import scala.collection.JavaConverters._

import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.apache.avro.Conversions.DecimalConversion
import org.apache.avro.LogicalTypes.{LocalTimestampMicros, LocalTimestampMillis, TimestampMicros, TimestampMillis}
import org.apache.avro.Schema.Type._
import org.apache.avro.generic._
import org.apache.avro.util.Utf8

import org.apache.spark.sql.avro.AvroUtils.{nonNullUnionBranches, toFieldStr, AvroMatchedField}
import org.apache.spark.sql.catalyst.{InternalRow, NoopFilters, StructFilters}
import org.apache.spark.sql.catalyst.expressions.{SpecificInternalRow, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MILLIS_PER_DAY
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A deserializer to deserialize data in avro format to data in catalyst format.
 */
private[sql] class AvroDeserializer(
    rootAvroType: Schema,
    rootCatalystType: DataType,
    positionalFieldMatch: Boolean,
    datetimeRebaseSpec: RebaseSpec,
    filters: StructFilters,
    useStableIdForUnionType: Boolean) {

  def this(
      rootAvroType: Schema,
      rootCatalystType: DataType,
      datetimeRebaseMode: String,
      useStableIdForUnionType: Boolean) = {
    this(
      rootAvroType,
      rootCatalystType,
      positionalFieldMatch = false,
      RebaseSpec(LegacyBehaviorPolicy.withName(datetimeRebaseMode)),
      new NoopFilters,
      useStableIdForUnionType)
  }

  private lazy val decimalConversions = new DecimalConversion()

  private val dateRebaseFunc = DataSourceUtils.createDateRebaseFuncInRead(
    datetimeRebaseSpec.mode, "Avro")

  private val timestampRebaseFunc = DataSourceUtils.createTimestampRebaseFuncInRead(
    datetimeRebaseSpec, "Avro")

  private val converter: Any => Option[Any] = try {
    rootCatalystType match {
      // A shortcut for empty schema.
      case st: StructType if st.isEmpty =>
        (_: Any) => Some(InternalRow.empty)

      case st: StructType =>
        val resultRow = new SpecificInternalRow(st.map(_.dataType))
        val fieldUpdater = new RowUpdater(resultRow)
        val applyFilters = filters.skipRow(resultRow, _)
        val writer = getRecordWriter(rootAvroType, st, Nil, Nil, applyFilters)
        (data: Any) => {
          val record = data.asInstanceOf[GenericRecord]
          val skipRow = writer(fieldUpdater, record)
          if (skipRow) None else Some(resultRow)
        }

      case _ =>
        val tmpRow = new SpecificInternalRow(Seq(rootCatalystType))
        val fieldUpdater = new RowUpdater(tmpRow)
        val writer = newWriter(rootAvroType, rootCatalystType, Nil, Nil)
        (data: Any) => {
          writer(fieldUpdater, 0, data)
          Some(tmpRow.get(0, rootCatalystType))
        }
    }
  } catch {
    case ise: IncompatibleSchemaException => throw new IncompatibleSchemaException(
      s"Cannot convert Avro type $rootAvroType to SQL type ${rootCatalystType.sql}.", ise)
  }

  private lazy val preventReadingIncorrectType = !SQLConf.get
    .getConf(SQLConf.LEGACY_AVRO_ALLOW_INCOMPATIBLE_SCHEMA)

  def deserialize(data: Any): Option[Any] = converter(data)

  /**
   * Creates a writer to write avro values to Catalyst values at the given ordinal with the given
   * updater.
   */
  private def newWriter(
      avroType: Schema,
      catalystType: DataType,
      avroPath: Seq[String],
      catalystPath: Seq[String]): (CatalystDataUpdater, Int, Any) => Unit = {
    val errorPrefix = s"Cannot convert Avro ${toFieldStr(avroPath)} to " +
        s"SQL ${toFieldStr(catalystPath)} because "
    val incompatibleMsg = errorPrefix +
        s"schema is incompatible (avroType = $avroType, sqlType = ${catalystType.sql})"

    val realDataType = SchemaConverters.toSqlType(avroType, useStableIdForUnionType).dataType

    (avroType.getType, catalystType) match {
      case (NULL, NullType) => (updater, ordinal, _) =>
        updater.setNullAt(ordinal)

      // TODO: we can avoid boxing if future version of avro provide primitive accessors.
      case (BOOLEAN, BooleanType) => (updater, ordinal, value) =>
        updater.setBoolean(ordinal, value.asInstanceOf[Boolean])

      case (INT, IntegerType) => (updater, ordinal, value) =>
        updater.setInt(ordinal, value.asInstanceOf[Int])

      case (INT, dt: DatetimeType)
        if preventReadingIncorrectType && realDataType.isInstanceOf[YearMonthIntervalType] =>
        throw QueryCompilationErrors.avroIncompatibleReadError(toFieldStr(avroPath),
          toFieldStr(catalystPath), realDataType.catalogString, dt.catalogString)

      case (INT, DateType) => (updater, ordinal, value) =>
        updater.setInt(ordinal, dateRebaseFunc(value.asInstanceOf[Int]))

      case (LONG, dt: DatetimeType)
        if preventReadingIncorrectType && realDataType.isInstanceOf[DayTimeIntervalType] =>
        throw QueryCompilationErrors.avroIncompatibleReadError(toFieldStr(avroPath),
          toFieldStr(catalystPath), realDataType.catalogString, dt.catalogString)

      case (LONG, LongType) => (updater, ordinal, value) =>
        updater.setLong(ordinal, value.asInstanceOf[Long])

      case (LONG, TimestampType) => avroType.getLogicalType match {
        // For backward compatibility, if the Avro type is Long and it is not logical type
        // (the `null` case), the value is processed as timestamp type with millisecond precision.
        case null | _: TimestampMillis => (updater, ordinal, value) =>
          val millis = value.asInstanceOf[Long]
          val micros = DateTimeUtils.millisToMicros(millis)
          updater.setLong(ordinal, timestampRebaseFunc(micros))
        case _: TimestampMicros => (updater, ordinal, value) =>
          val micros = value.asInstanceOf[Long]
          updater.setLong(ordinal, timestampRebaseFunc(micros))
        case other => throw new IncompatibleSchemaException(errorPrefix +
          s"Avro logical type $other cannot be converted to SQL type ${TimestampType.sql}.")
      }

      case (LONG, TimestampNTZType) => avroType.getLogicalType match {
        // To keep consistent with TimestampType, if the Avro type is Long and it is not
        // logical type (the `null` case), the value is processed as TimestampNTZ
        // with millisecond precision.
        case null | _: LocalTimestampMillis => (updater, ordinal, value) =>
          val millis = value.asInstanceOf[Long]
          val micros = DateTimeUtils.millisToMicros(millis)
          updater.setLong(ordinal, micros)
        case _: LocalTimestampMicros => (updater, ordinal, value) =>
          val micros = value.asInstanceOf[Long]
          updater.setLong(ordinal, micros)
        case other => throw new IncompatibleSchemaException(errorPrefix +
          s"Avro logical type $other cannot be converted to SQL type ${TimestampNTZType.sql}.")
      }

      // Before we upgrade Avro to 1.8 for logical type support, spark-avro converts Long to Date.
      // For backward compatibility, we still keep this conversion.
      case (LONG, DateType) => (updater, ordinal, value) =>
        updater.setInt(ordinal, (value.asInstanceOf[Long] / MILLIS_PER_DAY).toInt)

      case (FLOAT, FloatType) => (updater, ordinal, value) =>
        updater.setFloat(ordinal, value.asInstanceOf[Float])

      case (DOUBLE, DoubleType) => (updater, ordinal, value) =>
        updater.setDouble(ordinal, value.asInstanceOf[Double])

      case (STRING, StringType) => (updater, ordinal, value) =>
        val str = value match {
          case s: String => UTF8String.fromString(s)
          case s: Utf8 =>
            val bytes = new Array[Byte](s.getByteLength)
            System.arraycopy(s.getBytes, 0, bytes, 0, s.getByteLength)
            UTF8String.fromBytes(bytes)
        }
        updater.set(ordinal, str)

      case (ENUM, StringType) => (updater, ordinal, value) =>
        updater.set(ordinal, UTF8String.fromString(value.toString))

      case (FIXED, BinaryType) => (updater, ordinal, value) =>
        updater.set(ordinal, value.asInstanceOf[GenericFixed].bytes().clone())

      case (BYTES, BinaryType) => (updater, ordinal, value) =>
        val bytes = value match {
          case b: ByteBuffer =>
            val bytes = new Array[Byte](b.remaining)
            b.get(bytes)
            // Do not forget to reset the position
            b.rewind()
            bytes
          case b: Array[Byte] => b
          case other =>
            throw new RuntimeException(errorPrefix + s"$other is not a valid avro binary.")
        }
        updater.set(ordinal, bytes)

      case (FIXED, dt: DecimalType) =>
        val d = avroType.getLogicalType.asInstanceOf[LogicalTypes.Decimal]
        if (preventReadingIncorrectType &&
          d.getPrecision - d.getScale > dt.precision - dt.scale) {
          throw QueryCompilationErrors.avroIncompatibleReadError(toFieldStr(avroPath),
            toFieldStr(catalystPath), realDataType.catalogString, dt.catalogString)
        }
        (updater, ordinal, value) =>
          val bigDecimal =
            decimalConversions.fromFixed(value.asInstanceOf[GenericFixed], avroType, d)
          val decimal = createDecimal(bigDecimal, d.getPrecision, d.getScale)
          updater.setDecimal(ordinal, decimal)

      case (BYTES, dt: DecimalType) =>
        val d = avroType.getLogicalType.asInstanceOf[LogicalTypes.Decimal]
        if (preventReadingIncorrectType &&
          d.getPrecision - d.getScale > dt.precision - dt.scale) {
          throw QueryCompilationErrors.avroIncompatibleReadError(toFieldStr(avroPath),
            toFieldStr(catalystPath), realDataType.catalogString, dt.catalogString)
        }
        (updater, ordinal, value) =>
          val bigDecimal = decimalConversions.fromBytes(value.asInstanceOf[ByteBuffer], avroType, d)
          val decimal = createDecimal(bigDecimal, d.getPrecision, d.getScale)
          updater.setDecimal(ordinal, decimal)

      case (RECORD, st: StructType) =>
        // Avro datasource doesn't accept filters with nested attributes. See SPARK-32328.
        // We can always return `false` from `applyFilters` for nested records.
        val writeRecord =
          getRecordWriter(avroType, st, avroPath, catalystPath, applyFilters = _ => false)
        (updater, ordinal, value) =>
          val row = new SpecificInternalRow(st)
          writeRecord(new RowUpdater(row), value.asInstanceOf[GenericRecord])
          updater.set(ordinal, row)

      case (ARRAY, ArrayType(elementType, containsNull)) =>
        val avroElementPath = avroPath :+ "element"
        val elementWriter = newWriter(avroType.getElementType, elementType,
          avroElementPath, catalystPath :+ "element")
        (updater, ordinal, value) =>
          val collection = value.asInstanceOf[java.util.Collection[Any]]
          val result = createArrayData(elementType, collection.size())
          val elementUpdater = new ArrayDataUpdater(result)

          var i = 0
          val iter = collection.iterator()
          while (iter.hasNext) {
            val element = iter.next()
            if (element == null) {
              if (!containsNull) {
                throw new RuntimeException(
                  s"Array value at path ${toFieldStr(avroElementPath)} is not allowed to be null")
              } else {
                elementUpdater.setNullAt(i)
              }
            } else {
              elementWriter(elementUpdater, i, element)
            }
            i += 1
          }

          updater.set(ordinal, result)

      case (MAP, MapType(keyType, valueType, valueContainsNull)) if keyType == StringType =>
        val keyWriter = newWriter(SchemaBuilder.builder().stringType(), StringType,
          avroPath :+ "key", catalystPath :+ "key")
        val valueWriter = newWriter(avroType.getValueType, valueType,
          avroPath :+ "value", catalystPath :+ "value")
        (updater, ordinal, value) =>
          val map = value.asInstanceOf[java.util.Map[AnyRef, AnyRef]]
          val keyArray = createArrayData(keyType, map.size())
          val keyUpdater = new ArrayDataUpdater(keyArray)
          val valueArray = createArrayData(valueType, map.size())
          val valueUpdater = new ArrayDataUpdater(valueArray)
          val iter = map.entrySet().iterator()
          var i = 0
          while (iter.hasNext) {
            val entry = iter.next()
            assert(entry.getKey != null)
            keyWriter(keyUpdater, i, entry.getKey)
            if (entry.getValue == null) {
              if (!valueContainsNull) {
                throw new RuntimeException(
                  s"Map value at path ${toFieldStr(avroPath :+ "value")} is not allowed to be null")
              } else {
                valueUpdater.setNullAt(i)
              }
            } else {
              valueWriter(valueUpdater, i, entry.getValue)
            }
            i += 1
          }

          // The Avro map will never have null or duplicated map keys, it's safe to create a
          // ArrayBasedMapData directly here.
          updater.set(ordinal, new ArrayBasedMapData(keyArray, valueArray))

      case (UNION, _) =>
        val nonNullTypes = nonNullUnionBranches(avroType)
        val nonNullAvroType = Schema.createUnion(nonNullTypes.asJava)
        if (nonNullTypes.nonEmpty) {
          if (nonNullTypes.length == 1) {
            newWriter(nonNullTypes.head, catalystType, avroPath, catalystPath)
          } else {
            nonNullTypes.map(_.getType).toSeq match {
              case Seq(a, b) if Set(a, b) == Set(INT, LONG) && catalystType == LongType =>
                (updater, ordinal, value) =>
                  value match {
                    case null => updater.setNullAt(ordinal)
                    case l: java.lang.Long => updater.setLong(ordinal, l)
                    case i: java.lang.Integer => updater.setLong(ordinal, i.longValue())
                  }

              case Seq(a, b) if Set(a, b) == Set(FLOAT, DOUBLE) && catalystType == DoubleType =>
                (updater, ordinal, value) =>
                  value match {
                    case null => updater.setNullAt(ordinal)
                    case d: java.lang.Double => updater.setDouble(ordinal, d)
                    case f: java.lang.Float => updater.setDouble(ordinal, f.doubleValue())
                  }

              case _ =>
                catalystType match {
                  case st: StructType if st.length == nonNullTypes.size =>
                    val fieldWriters = nonNullTypes.zip(st.fields).map {
                      case (schema, field) =>
                        newWriter(schema, field.dataType, avroPath, catalystPath :+ field.name)
                    }.toArray
                    (updater, ordinal, value) => {
                      val row = new SpecificInternalRow(st)
                      val fieldUpdater = new RowUpdater(row)
                      val i = GenericData.get().resolveUnion(nonNullAvroType, value)
                      fieldWriters(i)(fieldUpdater, i, value)
                      updater.set(ordinal, row)
                    }

                  case _ => throw new IncompatibleSchemaException(incompatibleMsg)
                }
            }
          }
        } else {
          (updater, ordinal, _) => updater.setNullAt(ordinal)
        }

      case (INT, _: YearMonthIntervalType) => (updater, ordinal, value) =>
        updater.setInt(ordinal, value.asInstanceOf[Int])

      case (LONG, _: DayTimeIntervalType) => (updater, ordinal, value) =>
        updater.setLong(ordinal, value.asInstanceOf[Long])

      case (LONG, _: DecimalType) => (updater, ordinal, value) =>
        val d = avroType.getLogicalType.asInstanceOf[CustomDecimal]
        updater.setDecimal(ordinal, Decimal(value.asInstanceOf[Long], d.precision, d.scale))

      case _ => throw new IncompatibleSchemaException(incompatibleMsg)
    }
  }

  // TODO: move the following method in Decimal object on creating Decimal from BigDecimal?
  private def createDecimal(decimal: BigDecimal, precision: Int, scale: Int): Decimal = {
    if (precision <= Decimal.MAX_LONG_DIGITS) {
      // Constructs a `Decimal` with an unscaled `Long` value if possible.
      Decimal(decimal.unscaledValue().longValue(), precision, scale)
    } else {
      // Otherwise, resorts to an unscaled `BigInteger` instead.
      Decimal(decimal, precision, scale)
    }
  }

  private def getRecordWriter(
      avroType: Schema,
      catalystType: StructType,
      avroPath: Seq[String],
      catalystPath: Seq[String],
      applyFilters: Int => Boolean): (CatalystDataUpdater, GenericRecord) => Boolean = {

    val avroSchemaHelper = new AvroUtils.AvroSchemaHelper(
      avroType, catalystType, avroPath, catalystPath, positionalFieldMatch)

    avroSchemaHelper.validateNoExtraCatalystFields(ignoreNullable = true)
    // no need to validateNoExtraAvroFields since extra Avro fields are ignored

    val (validFieldIndexes, fieldWriters) = avroSchemaHelper.matchedFields.map {
      case AvroMatchedField(catalystField, ordinal, avroField) =>
        val baseWriter = newWriter(avroField.schema(), catalystField.dataType,
          avroPath :+ avroField.name, catalystPath :+ catalystField.name)
        val fieldWriter = (fieldUpdater: CatalystDataUpdater, value: Any) => {
          if (value == null) {
            fieldUpdater.setNullAt(ordinal)
          } else {
            baseWriter(fieldUpdater, ordinal, value)
          }
        }
        (avroField.pos(), fieldWriter)
    }.toArray.unzip

    (fieldUpdater, record) => {
      var i = 0
      var skipRow = false
      while (i < validFieldIndexes.length && !skipRow) {
        fieldWriters(i)(fieldUpdater, record.get(validFieldIndexes(i)))
        skipRow = applyFilters(i)
        i += 1
      }
      skipRow
    }
  }

  private def createArrayData(elementType: DataType, length: Int): ArrayData = elementType match {
    case BooleanType => UnsafeArrayData.fromPrimitiveArray(new Array[Boolean](length))
    case ByteType => UnsafeArrayData.fromPrimitiveArray(new Array[Byte](length))
    case ShortType => UnsafeArrayData.fromPrimitiveArray(new Array[Short](length))
    case IntegerType => UnsafeArrayData.fromPrimitiveArray(new Array[Int](length))
    case LongType => UnsafeArrayData.fromPrimitiveArray(new Array[Long](length))
    case FloatType => UnsafeArrayData.fromPrimitiveArray(new Array[Float](length))
    case DoubleType => UnsafeArrayData.fromPrimitiveArray(new Array[Double](length))
    case _ => new GenericArrayData(new Array[Any](length))
  }

  /**
   * A base interface for updating values inside catalyst data structure like `InternalRow` and
   * `ArrayData`.
   */
  sealed trait CatalystDataUpdater {
    def set(ordinal: Int, value: Any): Unit

    def setNullAt(ordinal: Int): Unit = set(ordinal, null)
    def setBoolean(ordinal: Int, value: Boolean): Unit = set(ordinal, value)
    def setByte(ordinal: Int, value: Byte): Unit = set(ordinal, value)
    def setShort(ordinal: Int, value: Short): Unit = set(ordinal, value)
    def setInt(ordinal: Int, value: Int): Unit = set(ordinal, value)
    def setLong(ordinal: Int, value: Long): Unit = set(ordinal, value)
    def setDouble(ordinal: Int, value: Double): Unit = set(ordinal, value)
    def setFloat(ordinal: Int, value: Float): Unit = set(ordinal, value)
    def setDecimal(ordinal: Int, value: Decimal): Unit = set(ordinal, value)
  }

  final class RowUpdater(row: InternalRow) extends CatalystDataUpdater {
    override def set(ordinal: Int, value: Any): Unit = row.update(ordinal, value)

    override def setNullAt(ordinal: Int): Unit = row.setNullAt(ordinal)
    override def setBoolean(ordinal: Int, value: Boolean): Unit = row.setBoolean(ordinal, value)
    override def setByte(ordinal: Int, value: Byte): Unit = row.setByte(ordinal, value)
    override def setShort(ordinal: Int, value: Short): Unit = row.setShort(ordinal, value)
    override def setInt(ordinal: Int, value: Int): Unit = row.setInt(ordinal, value)
    override def setLong(ordinal: Int, value: Long): Unit = row.setLong(ordinal, value)
    override def setDouble(ordinal: Int, value: Double): Unit = row.setDouble(ordinal, value)
    override def setFloat(ordinal: Int, value: Float): Unit = row.setFloat(ordinal, value)
    override def setDecimal(ordinal: Int, value: Decimal): Unit =
      row.setDecimal(ordinal, value, value.precision)
  }

  final class ArrayDataUpdater(array: ArrayData) extends CatalystDataUpdater {
    override def set(ordinal: Int, value: Any): Unit = array.update(ordinal, value)

    override def setNullAt(ordinal: Int): Unit = array.setNullAt(ordinal)
    override def setBoolean(ordinal: Int, value: Boolean): Unit = array.setBoolean(ordinal, value)
    override def setByte(ordinal: Int, value: Byte): Unit = array.setByte(ordinal, value)
    override def setShort(ordinal: Int, value: Short): Unit = array.setShort(ordinal, value)
    override def setInt(ordinal: Int, value: Int): Unit = array.setInt(ordinal, value)
    override def setLong(ordinal: Int, value: Long): Unit = array.setLong(ordinal, value)
    override def setDouble(ordinal: Int, value: Double): Unit = array.setDouble(ordinal, value)
    override def setFloat(ordinal: Int, value: Float): Unit = array.setFloat(ordinal, value)
    override def setDecimal(ordinal: Int, value: Decimal): Unit = array.update(ordinal, value)
  }
}
