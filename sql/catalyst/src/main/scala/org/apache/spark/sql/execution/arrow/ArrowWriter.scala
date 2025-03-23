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

package org.apache.spark.sql.execution.arrow

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.errors.ExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils

object ArrowWriter {

  def create(
      schema: StructType,
      timeZoneId: String,
      errorOnDuplicatedFieldNames: Boolean = true,
      largeVarTypes: Boolean = false): ArrowWriter = {
    val arrowSchema = ArrowUtils.toArrowSchema(
      schema, timeZoneId, errorOnDuplicatedFieldNames, largeVarTypes)
    val root = VectorSchemaRoot.create(arrowSchema, ArrowUtils.rootAllocator)
    create(root)
  }

  def create(root: VectorSchemaRoot): ArrowWriter = {
    val children = root.getFieldVectors().asScala.map { vector =>
      vector.allocateNew()
      createFieldWriter(vector)
    }
    new ArrowWriter(root, children.toArray)
  }

  private[sql] def createFieldWriter(vector: ValueVector): ArrowFieldWriter = {
    val field = vector.getField()
    (ArrowUtils.fromArrowField(field), vector) match {
      case (BooleanType, vector: BitVector) => new BooleanWriter(vector)
      case (ByteType, vector: TinyIntVector) => new ByteWriter(vector)
      case (ShortType, vector: SmallIntVector) => new ShortWriter(vector)
      case (IntegerType, vector: IntVector) => new IntegerWriter(vector)
      case (LongType, vector: BigIntVector) => new LongWriter(vector)
      case (FloatType, vector: Float4Vector) => new FloatWriter(vector)
      case (DoubleType, vector: Float8Vector) => new DoubleWriter(vector)
      case (DecimalType.Fixed(precision, scale), vector: DecimalVector) =>
        new DecimalWriter(vector, precision, scale)
      case (StringType, vector: VarCharVector) => new StringWriter(vector)
      case (StringType, vector: LargeVarCharVector) => new LargeStringWriter(vector)
      case (BinaryType, vector: VarBinaryVector) => new BinaryWriter(vector)
      case (BinaryType, vector: LargeVarBinaryVector) => new LargeBinaryWriter(vector)
      case (DateType, vector: DateDayVector) => new DateWriter(vector)
      case (TimestampType, vector: TimeStampMicroTZVector) => new TimestampWriter(vector)
      case (TimestampNTZType, vector: TimeStampMicroVector) => new TimestampNTZWriter(vector)
      case (ArrayType(_, _), vector: ListVector) =>
        val elementVector = createFieldWriter(vector.getDataVector())
        new ArrayWriter(vector, elementVector)
      case (MapType(_, _, _), vector: MapVector) =>
        val structVector = vector.getDataVector.asInstanceOf[StructVector]
        val keyWriter = createFieldWriter(structVector.getChild(MapVector.KEY_NAME))
        val valueWriter = createFieldWriter(structVector.getChild(MapVector.VALUE_NAME))
        new MapWriter(vector, structVector, keyWriter, valueWriter)
      case (StructType(_), vector: StructVector) =>
        val children = (0 until vector.size()).map { ordinal =>
          createFieldWriter(vector.getChildByOrdinal(ordinal))
        }
        new StructWriter(vector, children.toArray)
      case (NullType, vector: NullVector) => new NullWriter(vector)
      case (_: YearMonthIntervalType, vector: IntervalYearVector) => new IntervalYearWriter(vector)
      case (_: DayTimeIntervalType, vector: DurationVector) => new DurationWriter(vector)
      case (CalendarIntervalType, vector: IntervalMonthDayNanoVector) =>
        new IntervalMonthDayNanoWriter(vector)
      case (VariantType, vector: StructVector) =>
        val children = (0 until vector.size()).map { ordinal =>
          createFieldWriter(vector.getChildByOrdinal(ordinal))
        }
        new StructWriter(vector, children.toArray)
      case (dt, _) =>
        throw ExecutionErrors.unsupportedDataTypeError(dt)
    }
  }
}

class ArrowWriter(val root: VectorSchemaRoot, fields: Array[ArrowFieldWriter]) {

  def schema: StructType = ArrowUtils.fromArrowSchema(root.getSchema())

  private var count: Int = 0

  def write(row: InternalRow): Unit = {
    var i = 0
    while (i < fields.length) {
      fields(i).write(row, i)
      i += 1
    }
    count += 1
  }

  def sizeInBytes(): Int = {
    var i = 0
    var bytes = 0
    while (i < fields.size) {
      bytes += fields(i).getSizeInBytes()
      i += 1
    }
    bytes
  }

  def finish(): Unit = {
    root.setRowCount(count)
    fields.foreach(_.finish())
  }

  def reset(): Unit = {
    root.setRowCount(0)
    count = 0
    fields.foreach(_.reset())
  }
}

private[arrow] abstract class ArrowFieldWriter {

  def valueVector: ValueVector

  def name: String = valueVector.getField().getName()
  def dataType: DataType = ArrowUtils.fromArrowField(valueVector.getField())
  def nullable: Boolean = valueVector.getField().isNullable()

  def setNull(): Unit
  def setValue(input: SpecializedGetters, ordinal: Int): Unit

  private[arrow] var count: Int = 0

  def write(input: SpecializedGetters, ordinal: Int): Unit = {
    if (input.isNullAt(ordinal)) {
      setNull()
    } else {
      setValue(input, ordinal)
    }
    count += 1
  }

  def finish(): Unit = {
    valueVector.setValueCount(count)
  }

  def getSizeInBytes(): Int = {
    valueVector.setValueCount(count)
    // Before calling getBufferSizeFor, we need to call
    // `setValueCount`, see https://github.com/apache/arrow/pull/9187#issuecomment-763362710
    valueVector.getBufferSizeFor(count)
  }

  def reset(): Unit = {
    valueVector.reset()
    count = 0
  }
}

private[arrow] class BooleanWriter(val valueVector: BitVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, if (input.getBoolean(ordinal)) 1 else 0)
  }
}

private[arrow] class ByteWriter(val valueVector: TinyIntVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getByte(ordinal))
  }
}

private[arrow] class ShortWriter(val valueVector: SmallIntVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getShort(ordinal))
  }
}

private[arrow] class IntegerWriter(val valueVector: IntVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getInt(ordinal))
  }
}

private[arrow] class LongWriter(val valueVector: BigIntVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }
}

private[arrow] class FloatWriter(val valueVector: Float4Vector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getFloat(ordinal))
  }
}

private[arrow] class DoubleWriter(val valueVector: Float8Vector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getDouble(ordinal))
  }
}

private[arrow] class DecimalWriter(
    val valueVector: DecimalVector,
    precision: Int,
    scale: Int) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val decimal = input.getDecimal(ordinal, precision, scale)
    if (decimal.changePrecision(precision, scale)) {
      valueVector.setSafe(count, decimal.toJavaBigDecimal)
    } else {
      setNull()
    }
  }
}

private[arrow] class StringWriter(val valueVector: VarCharVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val utf8 = input.getUTF8String(ordinal)
    val utf8ByteBuffer = utf8.getByteBuffer
    // todo: for off-heap UTF8String, how to pass in to arrow without copy?
    valueVector.setSafe(count, utf8ByteBuffer, utf8ByteBuffer.position(), utf8.numBytes())
  }
}

private[arrow] class LargeStringWriter(
    val valueVector: LargeVarCharVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val utf8 = input.getUTF8String(ordinal)
    val utf8ByteBuffer = utf8.getByteBuffer
    // todo: for off-heap UTF8String, how to pass in to arrow without copy?
    valueVector.setSafe(count, utf8ByteBuffer, utf8ByteBuffer.position(), utf8.numBytes())
  }
}

private[arrow] class BinaryWriter(
    val valueVector: VarBinaryVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val bytes = input.getBinary(ordinal)
    valueVector.setSafe(count, bytes, 0, bytes.length)
  }
}

private[arrow] class LargeBinaryWriter(
    val valueVector: LargeVarBinaryVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val bytes = input.getBinary(ordinal)
    valueVector.setSafe(count, bytes, 0, bytes.length)
  }
}

private[arrow] class DateWriter(val valueVector: DateDayVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getInt(ordinal))
  }
}

private[arrow] class TimestampWriter(
    val valueVector: TimeStampMicroTZVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }
}

private[arrow] class TimestampNTZWriter(
    val valueVector: TimeStampMicroVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }
}

private[arrow] class ArrayWriter(
    val valueVector: ListVector,
    val elementWriter: ArrowFieldWriter) extends ArrowFieldWriter {

  override def setNull(): Unit = {
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val array = input.getArray(ordinal)
    var i = 0
    valueVector.startNewValue(count)
    while (i < array.numElements()) {
      elementWriter.write(array, i)
      i += 1
    }
    valueVector.endValue(count, array.numElements())
  }

  override def finish(): Unit = {
    super.finish()
    elementWriter.finish()
  }

  override def reset(): Unit = {
    super.reset()
    elementWriter.reset()
  }
}

private[arrow] class StructWriter(
    val valueVector: StructVector,
    children: Array[ArrowFieldWriter]) extends ArrowFieldWriter {

  lazy val isVariant = ArrowUtils.isVariantField(valueVector.getField)

  override def setNull(): Unit = {
    var i = 0
    while (i < children.length) {
      children(i).setNull()
      children(i).count += 1
      i += 1
    }
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    if (isVariant) {
      valueVector.setIndexDefined(count)
      val v = input.getVariant(ordinal)
      val row = InternalRow(v.getValue, v.getMetadata)
      children(0).write(row, 0)
      children(1).write(row, 1)
    } else {
      val struct = input.getStruct(ordinal, children.length)
      var i = 0
      valueVector.setIndexDefined(count)
      while (i < struct.numFields) {
        children(i).write(struct, i)
        i += 1
      }
    }
  }

  override def finish(): Unit = {
    super.finish()
    children.foreach(_.finish())
  }

  override def reset(): Unit = {
    super.reset()
    children.foreach(_.reset())
  }
}

private[arrow] class MapWriter(
    val valueVector: MapVector,
    val structVector: StructVector,
    val keyWriter: ArrowFieldWriter,
    val valueWriter: ArrowFieldWriter) extends ArrowFieldWriter {

  override def setNull(): Unit = {}

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val map = input.getMap(ordinal)
    valueVector.startNewValue(count)
    val keys = map.keyArray()
    val values = map.valueArray()
    var i = 0
    while (i <  map.numElements()) {
      structVector.setIndexDefined(keyWriter.count)
      keyWriter.write(keys, i)
      valueWriter.write(values, i)
      i += 1
    }

    valueVector.endValue(count, map.numElements())
  }

  override def finish(): Unit = {
    super.finish()
    keyWriter.finish()
    valueWriter.finish()
  }

  override def reset(): Unit = {
    super.reset()
    keyWriter.reset()
    valueWriter.reset()
  }
}

private[arrow] class NullWriter(val valueVector: NullVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
  }
}

private[arrow] class IntervalYearWriter(val valueVector: IntervalYearVector)
  extends ArrowFieldWriter {
  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getInt(ordinal));
  }
}

private[arrow] class DurationWriter(val valueVector: DurationVector)
  extends ArrowFieldWriter {
  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }
}

private[arrow] class IntervalMonthDayNanoWriter(val valueVector: IntervalMonthDayNanoVector)
  extends ArrowFieldWriter {
  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val ci = input.getInterval(ordinal)
    valueVector.setSafe(count, ci.months, ci.days, Math.multiplyExact(ci.microseconds, 1000L))
  }
}
