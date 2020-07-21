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

import java.io.IOException
import java.math.BigDecimal
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.apache.avro.Conversions.DecimalConversion
import org.apache.avro.LogicalTypes.{TimestampMicros, TimestampMillis}
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.{GenericDatumReader, GenericFixed}
import org.apache.avro.io.ResolvingDecoder
import org.apache.avro.util.Utf8

import org.apache.spark.sql.catalyst.{InternalRow, NoopFilters, StructFilters}
import org.apache.spark.sql.catalyst.expressions.{SpecificInternalRow, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, DateTimeUtils,
  GenericArrayData}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MILLIS_PER_DAY
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Reader to read data as catalyst format from avro decoders. Multiple calls
 * return the same [[InternalRow]] object. If needed, caller should copy before making a new call.
 *
 * @param actual
 * @param expected
 * @param rootCatalystType
 * @param datetimeRebaseMode
 * @tparam T
 */
class SparkAvroDatumReader[T](
    actual: Schema,
    expected: Schema,
    rootCatalystType: DataType,
    private[this] var datetimeRebaseMode: LegacyBehaviorPolicy.Value,
    filters: StructFilters) extends GenericDatumReader[Option[T]](actual, expected) {

  def this(actual: Schema, expected: Schema, rootCatalystType: DataType) {
    this(actual, expected, rootCatalystType,
      LegacyBehaviorPolicy.withName(
        SQLConf.get.getConf(SQLConf.LEGACY_AVRO_REBASE_MODE_IN_READ)),
      new NoopFilters)
  }

  def this(actual: Schema, expected: Schema, rootCatalystType: DataType, filters: StructFilters) {
    this(actual, expected, rootCatalystType,
      LegacyBehaviorPolicy.withName(
        SQLConf.get.getConf(SQLConf.LEGACY_AVRO_REBASE_MODE_IN_READ)),
      filters)
  }

  def this(schema: Schema, rootCatalystType: DataType, filters: StructFilters) {
    this(schema, schema, rootCatalystType, filters)
  }

  def this(rootCatalystType: DataType, filters: StructFilters) {
    this(null, rootCatalystType, filters)
  }

  @throws[IOException]
  override def read(old: Any, expected: Schema, in: ResolvingDecoder): Object = {
    if (expected eq this.getExpected) {
      // this is the top level read
      topLevelReader.apply(in).asInstanceOf[Object]
    } else {
      throw new RuntimeException("Called read without top level schema")
    }
  }

  private lazy val decimalConversions = new DecimalConversion()

  // lazy so that datetimeRebaseMode can be set after constructor
  private lazy val dateRebaseFunc = DataSourceUtils.creteDateRebaseFuncInRead(
    datetimeRebaseMode, "Avro")

  // lazy so that datetimeRebaseMode can be set after constructor
  private lazy val timestampRebaseFunc = DataSourceUtils.creteTimestampRebaseFuncInRead(
    datetimeRebaseMode, "Avro")

  def setDatetimeRebaseMode(datetimeRebaseMode: LegacyBehaviorPolicy.Value): Unit = {
    this.datetimeRebaseMode = datetimeRebaseMode
  }

  private lazy val topLevelReader: ResolvingDecoder => Any = rootCatalystType match {
    // A shortcut for empty schema.
    case st: StructType if st.isEmpty =>
      val skip = makeSkip(this.getExpected)
      (in: ResolvingDecoder) => {
        skip(in)
        Some(InternalRow.empty)
      }

    case st: StructType =>
      val resultRow = new SpecificInternalRow(st.map(_.dataType))
      val fieldUpdater = new RowUpdater(resultRow)
      val applyFilters = filters.skipRow(resultRow, _)
      val reader = newStructReader(this.getExpected, st, Nil, applyFilters, true)
      (in: ResolvingDecoder) => {
        val skipRow = reader(fieldUpdater, in)
        if (skipRow) None else Some(resultRow)
      }

    case _ =>
      val tmpRow = new SpecificInternalRow(Seq(rootCatalystType))
      val fieldUpdater = new RowUpdater(tmpRow)
      val reader = newReader(this.getExpected, rootCatalystType, Nil, true)
      (in: ResolvingDecoder) => {
        reader(fieldUpdater, 0, in)
        Some(tmpRow.get(0, rootCatalystType))
      }
  }

  /**
   * Creates a reader to read avro values as Catalyst values at the given ordinal with the given
   * updater.
   */
  private[this] def newReader(avroType: Schema,
      catalystType: DataType,
      path: List[String],
      reuseObj: Boolean
  ): (CatalystDataUpdater, Int, ResolvingDecoder) => Unit = (avroType.getType, catalystType) match {
    case (NULL, NullType) => makeNullReader(avroType)

    // TODO: we can avoid boxing if future version of avro provide primitive accessors.
    case (BOOLEAN, BooleanType) => makeBooleanReader(avroType)
    case (INT, IntegerType) => makeIntReader(avroType)
    case (INT, DateType) => makeIntAsDateReader(avroType)
    case (LONG, LongType) => makeLongReader(avroType)
    case (LONG, TimestampType) => makeTimestampReader(avroType)

    // Before we upgrade Avro to 1.8 for logical type support, spark-avro converts Long to Date.
    // For backward compatibility, we still keep this conversion.
    case (LONG, DateType) => makeLongAsDateReader(avroType)
    case (FLOAT, FloatType) => makeFloatReader(avroType)
    case (DOUBLE, DoubleType) => makeDoubleReader(avroType)
    case (STRING, StringType) => makeStringReader(avroType)
    case (ENUM, StringType) => makeEnumAsStringReader(avroType)
    case (FIXED, BinaryType) => makeFixedAsBinaryReader(avroType)
    case (BYTES, BinaryType) => makeBytesAsBinaryReader(avroType)
    case (FIXED, d: DecimalType) => makeFixedAsDecimalReader(avroType, d)
    case (BYTES, d: DecimalType) => makeBytesAsDecimalReader(avroType, d)
    case (RECORD, st: StructType) => makeRecordReader(avroType, st, path, reuseObj)
    case (ARRAY, ArrayType(elementType, _)) => makeArrayReader(avroType, elementType, path)
    case (MAP, MapType(keyType, valueType, _)) if keyType == StringType =>
      makeMapReader(avroType, keyType, valueType, path)

    case (UNION, _) => makeUnionReader(avroType, catalystType, path, reuseObj)

    case _ =>
      throw new IncompatibleSchemaException(
        s"Cannot convert Avro to catalyst because schema at path ${path.mkString(".")} " +
            s"is not compatible (avroType = $avroType, sqlType = $catalystType).\n" +
            s"Source Avro schema: ${this.getExpected}.\n" +
            s"Target Catalyst type: $rootCatalystType")
  }

  private[this] def newStructReader(avroType: Schema,
      sqlType: StructType,
      path: List[String],
      applyFilters: Int => Boolean,
      reuseObj: Boolean
  ): (CatalystDataUpdater, ResolvingDecoder) => Boolean = {
    val sqlLength = sqlType.length
    val avroLength = avroType.getFields.size()

    // stored against avro indices
    val fieldReaders = new Array[(CatalystDataUpdater, ResolvingDecoder) => Boolean](avroLength)

    var i = 0
    while (i < sqlLength) {
      val sqlField = sqlType.fields(i)
      val avroField = avroType.getField(sqlField.name)
      if (avroField != null) {
        val baseReader =
          newReader(avroField.schema(), sqlField.dataType, path :+ sqlField.name, reuseObj)
        val ordinal = i
        val fieldReader = (fieldUpdater: CatalystDataUpdater, in: ResolvingDecoder) => {
          baseReader(fieldUpdater, ordinal, in)
          applyFilters(ordinal)
        }
        fieldReaders(avroField.pos()) = fieldReader
      } else if (!sqlField.nullable) {
        throw new IncompatibleSchemaException(
          s"""
             |Cannot find non-nullable field ${path.mkString(".")}.${sqlField.name} in Avro schema.
             |Source Avro schema: ${this.getExpected}.
             |Target Catalyst type: $rootCatalystType.
           """.stripMargin)
      }
      i += 1
    }

    val skipReaders = new Array[ResolvingDecoder => Unit](avroLength)

    // Now go through avro types once to create skip functions
    avroType.getFields.asScala.foreach { avroField =>
      val j = avroField.pos()
      val skip = makeSkip(avroField.schema())
      skipReaders(j) = skip
      if (fieldReaders(j) == null) {
        fieldReaders(j) = (_, in) => {
          skip (in)
          false
        }
      }
    }

    (fieldUpdater, in) => {
      val fieldOrder = in.readFieldOrder()
      var i = 0
      var skipRow = false
      while (i < fieldOrder.length) {
        val j = fieldOrder(i).pos()
        if (skipRow) skipReaders(j)(in)
        else skipRow = fieldReaders(j)(fieldUpdater, in)
        i += 1
      }
      skipRow
    }
  }

  /** Functions to get readers for different data types */

  private[this] def makeNullReader(avroType: Schema):
      (CatalystDataUpdater, Int, ResolvingDecoder) => Unit = (updater, ordinal, in) => {
    super.read(null, avroType, in).asInstanceOf[Null]
    updater.setNullAt(ordinal)
  }

  private[this] def makeBooleanReader(avroType: Schema):
      (CatalystDataUpdater, Int, ResolvingDecoder) => Unit = (updater, ordinal, in) => {
    val value = super.read(null, avroType, in).asInstanceOf[Boolean]
    updater.setBoolean(ordinal, value.asInstanceOf[Boolean])
  }

  private[this] def makeIntReader(avroType: Schema):
      (CatalystDataUpdater, Int, ResolvingDecoder) => Unit = (updater, ordinal, in) => {
    val value = super.read(null, avroType, in).asInstanceOf[Int]
    updater.setInt(ordinal, value.asInstanceOf[Int])
  }

  private[this] def makeIntAsLongReader(avroType: Schema):
      (CatalystDataUpdater, Int, ResolvingDecoder) => Unit =
  (updater: CatalystDataUpdater, ordinal: Int, in: ResolvingDecoder) => {
    val value = super.read(null, avroType, in).asInstanceOf[Int]
    updater.setLong(ordinal, value.asInstanceOf[Int].toLong)
  }

  private[this] def makeIntAsDateReader(avroType: Schema):
      (CatalystDataUpdater, Int, ResolvingDecoder) => Unit = (updater, ordinal, in) => {
    val value = super.read(null, avroType, in).asInstanceOf[Int]
    updater.setInt(ordinal, dateRebaseFunc(value.asInstanceOf[Int]))
  }

  private[this] def makeLongReader(avroType: Schema):
      (CatalystDataUpdater, Int, ResolvingDecoder) => Unit = (updater, ordinal, in) => {
    val value = super.read(null, avroType, in).asInstanceOf[Long]
    updater.setLong(ordinal, value.asInstanceOf[Long])
  }

  private[this] def makeTimestampReader(avroType: Schema):
      (CatalystDataUpdater, Int, ResolvingDecoder) => Unit = avroType.getLogicalType match {
    // For backward compatibility, if the Avro type is Long and it is not logical type
    // (the `null` case), the value is processed as timestamp type with millisecond precision.
    case null | _: TimestampMillis => (updater, ordinal, in) => {
      val value = super.read(null, avroType, in)
      val millis = value.asInstanceOf[Long]
      val micros = DateTimeUtils.millisToMicros(millis)
      updater.setLong(ordinal, timestampRebaseFunc(micros))
    }

    case _: TimestampMicros => (updater, ordinal, in) => {
      val value = super.read(null, avroType, in)
      val micros = value.asInstanceOf[Long]
      updater.setLong(ordinal, timestampRebaseFunc(micros))
    }

    case other => throw new IncompatibleSchemaException(
      s"Cannot convert Avro logical type ${other} to Catalyst Timestamp type.")
  }

  private[this] def makeLongAsDateReader(avroType: Schema):
      (CatalystDataUpdater, Int, ResolvingDecoder) => Unit = (updater, ordinal, in) => {
    val value = super.read(null, avroType, in).asInstanceOf[Long]
    updater.setInt(ordinal, (value.asInstanceOf[Long] / MILLIS_PER_DAY).toInt)
  }

  private[this] def makeFloatReader(avroType: Schema):
      (CatalystDataUpdater, Int, ResolvingDecoder) => Unit = (updater, ordinal, in) => {
    val value = super.read(null, avroType, in).asInstanceOf[Float]
    updater.setFloat(ordinal, value.asInstanceOf[Float])
  }

  private[this] def makeFloatAsDoubleReader(avroType: Schema):
      (CatalystDataUpdater, Int, ResolvingDecoder) => Unit = (updater, ordinal, in) => {
    val value = super.read(null, avroType, in).asInstanceOf[Float]
    updater.setDouble(ordinal, value.asInstanceOf[Float].toDouble)
  }

  private[this] def makeDoubleReader(avroType: Schema):
      (CatalystDataUpdater, Int, ResolvingDecoder) => Unit = (updater, ordinal, in) => {
    val value = super.read(null, avroType, in).asInstanceOf[Double]
    updater.setDouble(ordinal, value.asInstanceOf[Double])
  }

  private[this] def makeStringReader(avroType: Schema):
      (CatalystDataUpdater, Int, ResolvingDecoder) => Unit = (updater, ordinal, in) => {
    val value = super.read(null, avroType, in)
    val str = value match {
      case s: String => UTF8String.fromString(s)
      case s: Utf8 =>
        val bytes = new Array[Byte](s.getByteLength)
        System.arraycopy(s.getBytes, 0, bytes, 0, s.getByteLength)
        UTF8String.fromBytes(bytes)
    }
    updater.set(ordinal, str)
  }

  private[this] def makeEnumAsStringReader(avroType: Schema):
      (CatalystDataUpdater, Int, ResolvingDecoder) => Unit = (updater, ordinal, in) => {
    val value = super.read(null, avroType, in)
    updater.set(ordinal, UTF8String.fromString(value.toString))
  }

  private[this] def makeFixedAsBinaryReader(avroType: Schema):
      (CatalystDataUpdater, Int, ResolvingDecoder) => Unit = (updater, ordinal, in) => {
    // TODO: perhaps override super.readFixed method
    val value = super.read(null, avroType, in)
    updater.set(ordinal, value.asInstanceOf[GenericFixed].bytes().clone())
  }

  def makeBytesAsBinaryReader(avroType: Schema):
      (CatalystDataUpdater, Int, ResolvingDecoder) => Unit = (updater, ordinal, in) => {
    val value = super.read(null, avroType, in)
    val bytes = value match {
      case b: ByteBuffer =>
        val bytes = new Array[Byte](b.remaining)
        b.get(bytes)
        bytes
      case b: Array[Byte] => b
      case other => throw new RuntimeException(s"$other is not a valid avro binary.")
    }
    updater.set(ordinal, bytes)
  }

  private[this] def makeFixedAsDecimalReader(avroType: Schema, d: DecimalType):
      (CatalystDataUpdater, Int, ResolvingDecoder) => Unit = (updater, ordinal, in) => {
    // TODO: perhaps override super.readFixed method
    val value = super.read(null, avroType, in).asInstanceOf[GenericFixed]
    val bigDecimal = decimalConversions.fromFixed(value.asInstanceOf[GenericFixed], avroType,
      LogicalTypes.decimal(d.precision, d.scale))
    val decimal = createDecimal(bigDecimal, d.precision, d.scale)
    updater.setDecimal(ordinal, decimal)
  }

  private[this] def makeBytesAsDecimalReader(avroType: Schema, d: DecimalType):
      (CatalystDataUpdater, Int, ResolvingDecoder) => Unit = (updater, ordinal, in) => {
    val value = super.read(null, avroType, in).asInstanceOf[ByteBuffer]
    val bigDecimal = decimalConversions.fromBytes(value.asInstanceOf[ByteBuffer], avroType,
      LogicalTypes.decimal(d.precision, d.scale))
    val decimal = createDecimal(bigDecimal, d.precision, d.scale)
    updater.setDecimal(ordinal, decimal)
  }

  private[this] def makeRecordReader(avroType: Schema,
      st: StructType,
      path: List[String],
      reuseObj: Boolean
  ): (CatalystDataUpdater, Int, ResolvingDecoder) => Unit = {
    // Avro datasource doesn't accept filters with nested attributes. See SPARK-32328.
    // We can always return `false` from `applyFilters` for nested records.
    val structReader = newStructReader(avroType, st, path, applyFilters = _ => false, reuseObj)

    if (reuseObj) {
      val row = new SpecificInternalRow(st)
      val rowUpdater = new RowUpdater(row)
      (updater, ordinal, in) =>
        structReader(rowUpdater, in)
        updater.set(ordinal, row)
    } else {
      (updater, ordinal, in) => {
        val row = new SpecificInternalRow(st)
        val rowUpdater = new RowUpdater(row)
        structReader(rowUpdater, in)
        updater.set(ordinal, row)
      }
    }
  }

  private[this] def makeArrayReader(avroType: Schema,
      elementType: DataType,
      path: List[String]
  ): (CatalystDataUpdater, Int, ResolvingDecoder) => Unit = {
    val elementReader = newReader(avroType.getElementType, elementType, path, false)

    (updater, ordinal, in) => {
      var length = in.readArrayStart()
      var array = createArrayData(elementType, length)
      val arrayUpdater = new ArrayDataUpdater(array)

      var base: Int = 0
      while (length > 0) {
        var i = 0
        while (i < length) {
          elementReader(arrayUpdater, base + i, in)
          i += 1
        }
        base += length.toInt
        length = in.arrayNext()
        array = expandArrayData(elementType, arrayUpdater, length)
      }

      updater.set(ordinal, array)
    }
  }

  private[this] def makeMapReader(avroType: Schema,
      keyType: DataType,
      valueType: DataType,
      path: List[String]
  ): (CatalystDataUpdater, Int, ResolvingDecoder) => Unit = {
    val keyReader = newReader(SchemaBuilder.builder().stringType(), StringType, path, false)

    val valueReader = newReader(avroType.getValueType, valueType, path, false)

    (updater, ordinal, in) => {
      var length = in.readMapStart()
      var keyArray = createArrayData(keyType, length)
      val keyArrayUpdater = new ArrayDataUpdater(keyArray)
      var valueArray = createArrayData(valueType, length)
      val valueArrayUpdater = new ArrayDataUpdater(valueArray)

      var base: Int = 0
      while (length > 0) {
        var i = 0
        while (i < length) {
          keyReader(keyArrayUpdater, base + i, in)
          valueReader(valueArrayUpdater, base + i, in)
          i += 1
        }
        base += length.toInt
        length = in.mapNext()
        keyArray = expandArrayData(keyType, keyArrayUpdater, length)
        valueArray = expandArrayData(valueType, valueArrayUpdater, length)
      }

      // The Avro map will never have null or duplicated map keys, it's safe to create a
      // ArrayBasedMapData directly here.
      updater.set(ordinal, new ArrayBasedMapData(keyArray, valueArray))
    }
  }

  private[this] def makeUnionReader(avroType: Schema,
      catalystType: DataType,
      path: List[String],
      reuseObj: Boolean
  ): (CatalystDataUpdater, Int, ResolvingDecoder) => Unit = {
    val getIncompExcep: () => IncompatibleSchemaException = () => {
      new IncompatibleSchemaException(
        s"Cannot convert Avro to catalyst because schema at path " +
            s"${path.mkString(".")} is not compatible " +
            s"(avroType = $avroType, sqlType = $catalystType).\n" +
            s"Source Avro schema: $expected.\n" +
            s"Target Catalyst type: $rootCatalystType")
    }

    val unionTypesAsScala = avroType.getTypes.asScala
    val indexOfNull = unionTypesAsScala.indexWhere(_.getType == NULL)
    val nonNullTypes = unionTypesAsScala.filter(_.getType != NULL)
    val nullReader = if (indexOfNull > -1) makeNullReader(unionTypesAsScala(indexOfNull)) else null

    withResolveUnion(unionTypesAsScala, indexOfNull, nullReader) {
      nonNullTypes match {
        case Seq(a) =>
          makeUnionSingleTypeReader(nonNullTypes.head, catalystType, path, reuseObj)

        case Seq(a, b) if Set(a.getType, b.getType) == Set(INT, LONG) && catalystType == LongType =>
          makeUnionIntLongReader(a, b, getIncompExcep)


        case Seq(a, b) if Set(a.getType, b.getType) == Set(FLOAT, DOUBLE) &&
            catalystType == DoubleType =>
          makeUnionFloatDoubleReader(a, b, getIncompExcep)

        case _ => catalystType match {
          case st: StructType if st.length == nonNullTypes.size =>
            makeUnionStructReader(nonNullTypes, indexOfNull, st, path, reuseObj)

          case _ =>
            throw getIncompExcep()
        }
      }
    }
  }

  /** Helper functions for makeUnionReader */

  private[this] def makeUnionSingleTypeReader(avroType: Schema, catalystType: DataType,
      path: List[String], reuseObj: Boolean
  ): (Int, Schema, CatalystDataUpdater, Int, ResolvingDecoder) => Unit = {
    val reader = newReader(avroType, catalystType, path, reuseObj)

    (_, _, updater, ordinal, in) => reader(updater, ordinal, in)
  }

  private[this] def makeUnionIntLongReader(avroTypeA: Schema, avroTypeB: Schema,
      getIncompExcep: () => IncompatibleSchemaException
  ): (Int, Schema, CatalystDataUpdater, Int, ResolvingDecoder) => Unit = {
    val (intReader, longReader) = (avroTypeA.getType, avroTypeB.getType) match {
      case (INT, LONG) => (makeIntAsLongReader(avroTypeA), makeLongReader(avroTypeB))
      case (LONG, INT) => (makeIntAsLongReader(avroTypeB), makeLongReader(avroTypeA))
      case _ => throw getIncompExcep()
    }

    (_, resolvedAvroTypeSchema, updater, ordinal, in) => {
      resolvedAvroTypeSchema.getType match {
        case INT => intReader(updater, ordinal, in)
        case LONG => longReader(updater, ordinal, in)
        case _ => throw getIncompExcep()
      }
    }
  }

  private[this] def makeUnionFloatDoubleReader(avroTypeA: Schema, avroTypeB: Schema,
      getIncompExcep: () => IncompatibleSchemaException
  ): (Int, Schema, CatalystDataUpdater, Int, ResolvingDecoder) => Unit = {
    val (floatReader, doubleReader) = (avroTypeA.getType, avroTypeB.getType) match {
      case (FLOAT, DOUBLE) => (makeFloatAsDoubleReader(avroTypeA), makeDoubleReader(avroTypeB))
      case (DOUBLE, FLOAT) => (makeFloatAsDoubleReader(avroTypeB), makeDoubleReader(avroTypeA))
      case _ => throw getIncompExcep()
    }

    (_, resolvedAvroTypeSchema, updater, ordinal, in) => {
      resolvedAvroTypeSchema.getType match {
        case FLOAT => floatReader(updater, ordinal, in)
        case DOUBLE => doubleReader(updater, ordinal, in)
        case _ => throw getIncompExcep() // THIS CAN NOT HAPPEN
      }
    }
  }

  private[this] def makeUnionStructReader(nonNullTypes: mutable.Buffer[Schema],
      indexOfNull: Int,
      st: StructType,
      path: List[String],
      reuseObj: Boolean
  ): (Int, Schema, CatalystDataUpdater, Int, ResolvingDecoder) => Unit = {
    val fieldReaders = nonNullTypes.zip(st.fields).map {
      case (schema, field) => newReader(schema, field.dataType, path :+ field.name, reuseObj)
    }.toArray

    (unionIndex, _, updater, ordinal, in) => {
      val row = new SpecificInternalRow(st)
      val rowUpdater = new RowUpdater(row)

      val catalystIndex = if ((indexOfNull < 0) || (unionIndex < indexOfNull)) {
        unionIndex
      } else {
        unionIndex - 1
      }

      val fieldReader = fieldReaders(catalystIndex)
      fieldReader(rowUpdater, catalystIndex, in)
      updater.set(ordinal, row)
    }
  }

  private[this] def withResolveUnion(
      unionTypes: mutable.Buffer[Schema],
      indexOfNull: Int,
      nullReader: (CatalystDataUpdater, Int, ResolvingDecoder) => Unit
  )(reader: (Int, Schema, CatalystDataUpdater, Int, ResolvingDecoder) => Unit):
      (CatalystDataUpdater, Int, ResolvingDecoder) => Unit = (updater, ordinal, in) => {
    val unionIndex = in.readIndex()
    val resolvedAvroTypeSchema = unionTypes(unionIndex)
    if (unionIndex == indexOfNull) {
      nullReader(updater, ordinal, in)
    } else {
      reader(unionIndex, resolvedAvroTypeSchema, updater, ordinal, in)
    }
  }

  /** Helper functions to create objects */

  private[this] def createArrayData(elementType: DataType, length: Long): ArrayData = {
    elementType match {
      case BooleanType => UnsafeArrayData.createFreshArray(length.toInt, 1)
      case ByteType => UnsafeArrayData.createFreshArray(length.toInt, 1)
      case ShortType => UnsafeArrayData.createFreshArray(length.toInt, 2)
      case IntegerType => UnsafeArrayData.createFreshArray(length.toInt, 4)
      case LongType => UnsafeArrayData.createFreshArray(length.toInt, 8)
      case FloatType => UnsafeArrayData.createFreshArray(length.toInt, 4)
      case DoubleType => UnsafeArrayData.createFreshArray(length.toInt, 8)
      case _ => new GenericArrayData(new Array[Any](length.toInt))
    }
  }

  private[this] def expandArrayData(elementType: DataType, updater: ArrayDataUpdater,
      expandBy: Long): ArrayData = withArrayDataExpanderHelper(updater, expandBy) {
    elementType match {
      case BooleanType => unsafeArrayDataExpander(1)
      case ByteType => unsafeArrayDataExpander(1)
      case ShortType => unsafeArrayDataExpander(2)
      case IntegerType => unsafeArrayDataExpander(4)
      case LongType => unsafeArrayDataExpander(8)
      case FloatType => unsafeArrayDataExpander(4)
      case DoubleType => unsafeArrayDataExpander(8)
      case _ => (arrayData, expandBy) => {
        val newLength = arrayData.numElements() + expandBy
        val newArray = new Array[Any](newLength.toInt)
        val oldArray = arrayData.asInstanceOf[GenericArrayData].array
        System.arraycopy(oldArray, 0, newArray, 0, oldArray.length)
        new GenericArrayData(newArray)
      }
    }
  }

  private[this] def withArrayDataExpanderHelper(updater: ArrayDataUpdater, expandBy: Long)
      (f: (ArrayData, Long) => ArrayData): ArrayData = {
    if (expandBy <= 0) {
      updater.array
    } else {
      val newArray = f(updater.array, expandBy)
      updater.array = newArray
      newArray
    }
  }

  private[this] def unsafeArrayDataExpander(elementSize: Int)
      (array: ArrayData, expandBy: Long): ArrayData = {
    val newLength = array.numElements() + expandBy
    val newArray = UnsafeArrayData.createFreshArray(newLength.toInt, elementSize)
    val oldArray = array.asInstanceOf[UnsafeArrayData]
    oldArray.copyTo(newArray)
    newArray
  }

  // TODO: move the following method in Decimal object on creating Decimal from BigDecimal?
  private[this] def createDecimal(decimal: BigDecimal, precision: Int, scale: Int): Decimal = {
    if (precision <= Decimal.MAX_LONG_DIGITS) {
      // Constructs a `Decimal` with an unscaled `Long` value if possible.
      Decimal(decimal.unscaledValue().longValue(), precision, scale)
    } else {
      // Otherwise, resorts to an unscaled `BigInteger` instead.
      Decimal(decimal, precision, scale)
    }
  }

  /** Function for skipping */

  // Note: Currently GenericDatumReader.skip is not working as expected, eg reading an
  // INT for an ENUM type and not reading NULL (the later has been fixed in
  // https://github.com/apache/avro/commit/d3a2b149aa92608956fb0b163eb1bea06ef2c05a)
  def makeSkip(schema: Schema): ResolvingDecoder => Unit = schema.getType match {
    case RECORD =>
      val fieldSkips = schema.getFields.asScala.map(field => makeSkip(field.schema))
      in => fieldSkips.foreach(f => f(in))

    case ARRAY =>
      val elementSkip = makeSkip(schema.getElementType)
      in => {
        var l = in.skipArray()
        while (l > 0) {
          var i = 0
          while (i < l) {
            elementSkip(in)
            i += 1
          }
          l = in.skipArray()
        }
      }

    case MAP =>
      val valueSkip = makeSkip(schema.getValueType)
      in => {
        var l = in.skipMap()
        while (l > 0) {
          var i = 0
          while (i < l) {
            in.skipString()
            valueSkip(in)
            i += 1
          }
          l = in.skipMap()
        }
      }

    case UNION =>
      val typeSkips = schema.getTypes.asScala.map(typ => makeSkip(typ))
      in => typeSkips(in.readIndex)(in)

    case FIXED =>
      val fixedSize = schema.getFixedSize
      in => in.skipFixed(fixedSize)

    case ENUM => in => in.readEnum()
    case STRING => in => in.skipString()
    case BYTES => in => in.skipBytes()
    case INT => in => in.readInt()
    case LONG => in => in.readLong()
    case FLOAT => in => in.readFloat()
    case DOUBLE => in => in.readDouble()
    case BOOLEAN => in => in.readBoolean()
    case NULL => in => in.readNull()
    case _ =>
      throw new RuntimeException("Unknown type: " + schema)
  }
}

/**
 * A base interface for updating values inside catalyst data structure like `InternalRow` and
 * `ArrayData`.
 */
private[avro] sealed trait CatalystDataUpdater {
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

private[avro] final class RowUpdater(row: InternalRow) extends CatalystDataUpdater {
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

private[avro] final class ArrayDataUpdater(var array: ArrayData)
    extends CatalystDataUpdater {
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
