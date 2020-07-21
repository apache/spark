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

import java.nio.ByteBuffer

import scala.collection.JavaConverters._

import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.apache.avro.Conversions.DecimalConversion
import org.apache.avro.LogicalTypes.{TimestampMicros, TimestampMillis}
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData.{EnumSymbol, Fixed}
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.Encoder
import org.apache.avro.util.Utf8

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecializedGetters, SpecificInternalRow}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.types._

/**
 * Writer to write data in catalyst format to avro encoders
 *
 * @param rootAvroType
 * @param rootCatalystType
 * @param nullable
 * @tparam D
 */
class SparkAvroDatumWriter[D](rootAvroType: Schema, rootCatalystType: DataType, nullable: Boolean)
    extends GenericDatumWriter[D](rootAvroType) with Logging {
  def this(rootAvroType: Schema, rootCatalystType: DataType) {
    this(rootAvroType, rootCatalystType, false)
  }

  def this(rootCatalystType: DataType) {
    this(null, rootCatalystType)
  }

  override def write(schema: Schema, datum: Any, out: Encoder): Unit = {
    topLevelWriter.apply(datum, out)
  }

  private[this] val datetimeRebaseMode = LegacyBehaviorPolicy.withName(SQLConf.get.getConf(
    SQLConf.LEGACY_AVRO_REBASE_MODE_IN_WRITE))

  private val dateRebaseFunc = DataSourceUtils.creteDateRebaseFuncInWrite(
    datetimeRebaseMode, "Avro")

  private val timestampRebaseFunc = DataSourceUtils.creteTimestampRebaseFuncInWrite(
    datetimeRebaseMode, "Avro")

  private lazy val decimalConversions = new DecimalConversion()

  private val topLevelWriter: (Any, Encoder) => Unit = {
    val (actualAvroType, indexOfNull) = resolveNullableType(rootAvroType, nullable)
    val baseWriter: (Any, Encoder) => Unit = rootCatalystType match {
      case st: StructType =>
        newStructWriter(st, actualAvroType, List[String](actualAvroType.getFullName))
            .asInstanceOf[(Any, Encoder) => Unit]
      case _ =>
        val tmpRow = new SpecificInternalRow(Seq(rootCatalystType))
        val writer =
          newWriter(rootCatalystType, actualAvroType, List[String](actualAvroType.getFullName))
        (data: Any, out: Encoder) =>
          tmpRow.update(0, data)
          writer.apply(tmpRow, 0, out)
    }

    if (indexOfNull >= 0) {
      // Avro type is a union and the catalyst type is nullable.
      // Handle null and writing the union index
      val indexOfNotNull = 1 - indexOfNull
      (data: Any, out: Encoder) =>
        if (data == null) {
          out.writeIndex(indexOfNull)
          super.write(SchemaBuilder.builder().nullType(), null, out)
        } else {
          out.writeIndex(indexOfNotNull)
          baseWriter.apply(data, out)
        }
    } else {
      baseWriter
    }
  }

  private type Writer = (SpecializedGetters, Int, Encoder) => Unit

  private def newWriter(
      catalystType: DataType, avroType: Schema, path: List[String], nullable: Boolean
  ): Writer = {
    val (actualType, indexOfNull) = resolveNullableType(avroType, nullable)
    val baseWriter = newWriter(catalystType, actualType, path)

    if ((indexOfNull >= 0) && (actualType.getType != NULL)) {
      // Avro type is a union and the catalyst type is nullable
      // so have to handle null and writing the union index
      makeNullableAsUnionWriter(baseWriter, indexOfNull)
    } else {
      baseWriter
    }
  }

  private def newWriter(
      catalystType: DataType, avroType: Schema, path: List[String]
  ): Writer = {
    val baseWriter: Writer = (catalystType, avroType.getType) match {
      case (NullType, NULL) => makeNullWriter(avroType)
      case (BooleanType, BOOLEAN) => makeBooleanWriter(avroType)
      case (ByteType, INT) => makeByteAsIntWriter(avroType)
      case (ShortType, INT) => makeShortAsIntWriter(avroType)
      case (IntegerType, INT) => makeIntWriter(avroType)
      case (LongType, LONG) => makeLongWriter(avroType)
      case (FloatType, FLOAT) => makeFloatWriter(avroType)
      case (DoubleType, DOUBLE) => makeDoubleWriter(avroType)

      case (d: DecimalType, FIXED)
        if avroType.getLogicalType == LogicalTypes.decimal(d.precision, d.scale) =>
        makeDecimalAsFixedWriter(d, avroType)

      case (d: DecimalType, BYTES)
        if avroType.getLogicalType == LogicalTypes.decimal(d.precision, d.scale) =>
        makeDecimalAsBytesWriter(d, avroType)

      case (StringType, ENUM) => makeStringAsEnumWriter(avroType)
      case (StringType, STRING) => makeStringWriter(avroType)
      case (BinaryType, FIXED) => makeBinaryAsFixedWriter(avroType)
      case (BinaryType, BYTES) => makeBinaryAsBytesWriter(avroType)
      case (DateType, INT) => makeDateAsIntWriter(avroType)

      case (TimestampType, LONG) => avroType.getLogicalType match {
        // For backward compatibility, if the Avro type is Long and it is not logical type
        // (the `null` case), output the timestamp value as with millisecond precision.
        case null | _: TimestampMillis => makeTimestampMillisWriter(avroType)
        case _: TimestampMicros => makeTimestmapMicrosWriter(avroType)
        case other => throw new IncompatibleSchemaException(
          s"Cannot convert Catalyst Timestamp type to Avro logical type ${other}")
      }

      case (ArrayType(et, containsNull), ARRAY) => makeArrayWriter(et, containsNull, avroType, path)
      case (st: StructType, RECORD) => makeRecordWriter(st, avroType, path)
      case (MapType(kt, vt, valueContainsNull), MAP) if kt == StringType =>
        makeMapWriter(kt, vt, valueContainsNull, avroType, path)

      case _ =>
        throw new IncompatibleSchemaException(s"Cannot convert Catalyst type $catalystType to " +
            s"Avro type $avroType.")
    }

    (getter, ordinal, out) => {
      try {
        baseWriter.apply(getter, ordinal, out)
      } catch {
        case e: NullPointerException => throw nullPointerException(e, path)
      }
    }
  }

  private def newStructWriter(
      catalystStruct: StructType, avroStruct: Schema, path: List[String]
  ): (InternalRow, Encoder) => Unit = {
    if (avroStruct.getType != RECORD || avroStruct.getFields.size() != catalystStruct.length) {
      throw new IncompatibleSchemaException(s"Cannot convert Catalyst type $catalystStruct to " +
          s"Avro type $avroStruct.")
    }

    val numFields = avroStruct.getFields.size()

    val indicesFromAvroToCatalyst = new Array[Int](numFields)
    val fieldWriters = new Array[Writer](numFields)

    // For each catalyst field index i, match it against the avro field index j and do
    // indiesFromAvroToCatalyst(j) = i, and
    // fieldWriters(j) = corresponding field writer
    catalystStruct.zipWithIndex.foreach { case (catalystField, i) =>
      val avroField = avroStruct.getField(catalystField.name)
      if (avroField == null) {
        throw new IncompatibleSchemaException(
          s"Cannot convert Catalyst type $catalystStruct to Avro type $avroStruct.")
      }
      val writer = newWriter(catalystField.dataType, avroField.schema(),
        path :+ avroField.name(), catalystField.nullable)

      val j = avroField.pos() // j = avro index
      indicesFromAvroToCatalyst(j) = i // i = catalyst index
      fieldWriters(j) = writer
    }

    (row: InternalRow, out: Encoder) => {
      var j = 0 // j = avro index
      while (j < numFields) {
        val i = indicesFromAvroToCatalyst(j) // i = catalyst index
        fieldWriters(j).apply(row, i, out)
        j += 1
      }
    }
  }

  private def resolveNullableType(avroType: Schema, nullable: Boolean): (Schema, Int) = {
    if (avroType.getType == UNION && nullable) {
      // avro uses union to represent nullable type.
      val fields = avroType.getTypes.asScala
      assert(fields.length == 2)
      val actualType = fields.filter(_.getType != NULL)
      assert(actualType.length == 1)

      val indexOfNull = fields.indexWhere(_.getType == NULL)
      (actualType.head, indexOfNull)
    } else {
      if (nullable) {
        logWarning("Writing avro files with non-nullable avro schema with nullable catalyst " +
            "schema will throw runtime exception if there is a record with null value.")
      }
      (avroType, -1)
    }
  }

  private def makeNullWriter(avroType: Schema): Writer = {
    (getter, ordinal, out) => super.write(avroType, null, out)
  }

  private def makeBooleanWriter(avroType: Schema): Writer = {
    (getter, ordinal, out) => super.write(avroType, getter.getBoolean(ordinal), out)
  }

  private def makeByteAsIntWriter(avroType: Schema): Writer = {
    (getter, ordinal, out) => super.write(avroType, getter.getByte(ordinal).toInt, out)
  }

  private def makeShortAsIntWriter(avroType: Schema): Writer = {
    (getter, ordinal, out) => super.write(avroType, getter.getShort(ordinal).toInt, out)
  }

  private def makeIntWriter(avroType: Schema): Writer = {
    (getter, ordinal, out) => super.write(avroType, getter.getInt(ordinal), out)
  }

  private def makeLongWriter(avroType: Schema): Writer = {
    (getter, ordinal, out) => super.write(avroType, getter.getLong(ordinal), out)
  }

  private def makeFloatWriter(avroType: Schema): Writer = {
    (getter, ordinal, out) => super.write(avroType, getter.getFloat(ordinal), out)
  }

  private def makeDoubleWriter(avroType: Schema): Writer = {
    (getter, ordinal, out) => super.write(avroType, getter.getDouble(ordinal), out)
  }

  private def makeDecimalAsFixedWriter(d: DecimalType, avroType: Schema): Writer = {
    (getter, ordinal, out) =>
      val decimal = getter.getDecimal(ordinal, d.precision, d.scale)
      val value = decimalConversions.toFixed(decimal.toJavaBigDecimal, avroType,
        LogicalTypes.decimal(d.precision, d.scale))
      super.write(avroType, value, out)
  }

  private def makeDecimalAsBytesWriter(d: DecimalType, avroType: Schema): Writer = {
    (getter, ordinal, out) =>
      val decimal = getter.getDecimal(ordinal, d.precision, d.scale)
      val value = decimalConversions.toBytes(decimal.toJavaBigDecimal, avroType,
        LogicalTypes.decimal(d.precision, d.scale))
      super.write(avroType, value, out)
  }

  private def makeStringAsEnumWriter(avroType: Schema): Writer = {
    val enumSymbols: Set[String] = avroType.getEnumSymbols.asScala.toSet
    (getter, ordinal, out) =>
      val data = getter.getUTF8String(ordinal)
      if ((data == null) || !enumSymbols.contains(data.toString)) {
        throw new IncompatibleSchemaException(
          "Cannot write \"" + data + "\" since it's not defined in enum \"" +
              enumSymbols.mkString("\", \"") + "\"")
      }
      super.write(avroType, new EnumSymbol(avroType, data), out)
  }

  private def makeStringWriter(avroType: Schema): Writer = {
    (getter, ordinal, out) =>
      super.write(avroType, new Utf8(getter.getUTF8String(ordinal).getBytes), out)
  }

  private def makeBinaryAsFixedWriter(avroType: Schema): Writer = {
    val size = avroType.getFixedSize()
    (getter, ordinal, out) =>
      val data: Array[Byte] = getter.getBinary(ordinal)
      if (data.length != size) {
        throw new IncompatibleSchemaException(
          s"Cannot write ${data.length} ${if (data.length > 1) "bytes" else "byte"} of " +
              "binary data into FIXED Type with size of " +
              s"$size ${if (size > 1) "bytes" else "byte"}")
      }
      super.write(avroType, new Fixed(avroType, data), out)
  }

  private def makeBinaryAsBytesWriter(avroType: Schema): Writer = {
    (getter, ordinal, out) => super.write(avroType, ByteBuffer.wrap(getter.getBinary(ordinal)), out)
  }

  private def makeDateAsIntWriter(avroType: Schema): Writer = {
    (getter, ordinal, out) => super.write(avroType, dateRebaseFunc(getter.getInt(ordinal)), out)
  }

  private def makeTimestampMillisWriter(avroType: Schema): Writer = {
    (getter, ordinal, out) =>
      val value = DateTimeUtils.microsToMillis(timestampRebaseFunc(getter.getLong(ordinal)))
      super.write(avroType, value, out)
  }

  private def makeTimestmapMicrosWriter(avroType: Schema): Writer = {
    (getter, ordinal, out) =>
      val value = timestampRebaseFunc(getter.getLong(ordinal))
      super.write(avroType, value, out)
  }

  private def makeArrayWriter(
      et: DataType, containsNull: Boolean, avroType: Schema, path: List[String]
  ): Writer = {
    val elementWriter = newWriter(et, avroType.getElementType, path, containsNull)

    (getter, ordinal, out) => {
      val arrayData = getter.getArray(ordinal)
      val len = arrayData.numElements()

      out.writeArrayStart()
      out.setItemCount(len)

      var i = 0
      while (i < len) {
        out.startItem()
        elementWriter(arrayData, i, out)
        i += 1
      }

      out.writeArrayEnd()
    }
  }

  private def makeRecordWriter(st: StructType, avroType: Schema, path: List[String]): Writer = {
    val structWriter = newStructWriter(st, avroType, path)
    val numFields = st.length
    (getter, ordinal, out) => {
      try {
        structWriter.apply(getter.getStruct(ordinal, numFields), out)
      } catch {
        case e: NullPointerException => throw nullPointerException(e, path)
      }
    }
  }

  private def makeMapWriter(
      kt: DataType, vt: DataType, valueContainsNull: Boolean, avroType: Schema, path: List[String]
  ): Writer = {
    val valueWriter = newWriter(vt, avroType.getValueType, path, valueContainsNull)
    val keyWriter = newWriter(kt, SchemaBuilder.builder().stringType(), path)

    (getter, ordinal, out) => {
      val mapData = getter.getMap(ordinal)
      val len = mapData.numElements()
      val keyArray = mapData.keyArray()
      val valueArray = mapData.valueArray()

      out.writeMapStart()
      out.setItemCount(len)

      var i = 0
      while (i < len) {
        out.startItem()
        keyWriter(keyArray, i, out)
        valueWriter(valueArray, i, out)
        i += 1
      }

      out.writeMapEnd()
    }
  }

  private def makeNullableAsUnionWriter(baseWriter: Writer, indexOfNull: Int): Writer = {
    val indexOfNotNull = 1 - indexOfNull
    val nullWriter = makeNullWriter(SchemaBuilder.builder().nullType())

    (getter, ordinal, out) => {
      if (getter.isNullAt(ordinal)) {
        out.writeIndex(indexOfNull)
        nullWriter(getter, ordinal, out)
      } else {
        out.writeIndex(indexOfNotNull)
        baseWriter(getter, ordinal, out)
      }
    }
  }

  private def nullPointerException(
      original: NullPointerException, path: List[String]
  ): Throwable = {
    val npe = new NullPointerException("at path " + path.mkString("."))
    npe.initCause(if (original.getCause == null) original else original.getCause)
  }
}
