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

import scala.collection.JavaConverters._

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex._
import org.apache.arrow.vector.types.pojo.ArrowType

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types._

object ArrowWriter {

  def create(schema: StructType, timeZoneId: String): ArrowWriter = {
    val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
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

  private def createFieldWriter(vector: ValueVector): ArrowFieldWriter = {
    val field = vector.getField()
    (ArrowUtils.fromArrowField(field), vector) match {
      case (BooleanType, vector: NullableBitVector) => new BooleanWriter(vector)
      case (ByteType, vector: NullableTinyIntVector) => new ByteWriter(vector)
      case (ShortType, vector: NullableSmallIntVector) => new ShortWriter(vector)
      case (IntegerType, vector: NullableIntVector) => new IntegerWriter(vector)
      case (LongType, vector: NullableBigIntVector) => new LongWriter(vector)
      case (FloatType, vector: NullableFloat4Vector) => new FloatWriter(vector)
      case (DoubleType, vector: NullableFloat8Vector) => new DoubleWriter(vector)
      case (StringType, vector: NullableVarCharVector) => new StringWriter(vector)
      case (BinaryType, vector: NullableVarBinaryVector) => new BinaryWriter(vector)
      case (DateType, vector: NullableDateDayVector) => new DateWriter(vector)
      case (TimestampType, vector: NullableTimeStampMicroTZVector) => new TimestampWriter(vector)
      case (ArrayType(_, _), vector: ListVector) =>
        val elementVector = createFieldWriter(vector.getDataVector())
        new ArrayWriter(vector, elementVector)
      case (StructType(_), vector: NullableMapVector) =>
        val children = (0 until vector.size()).map { ordinal =>
          createFieldWriter(vector.getChildByOrdinal(ordinal))
        }
        new StructWriter(vector, children.toArray)
      case (dt, _) =>
        throw new UnsupportedOperationException(s"Unsupported data type: ${dt.simpleString}")
    }
  }
}

class ArrowWriter(val root: VectorSchemaRoot, fields: Array[ArrowFieldWriter]) {

  def schema: StructType = StructType(fields.map { f =>
    StructField(f.name, f.dataType, f.nullable)
  })

  private var count: Int = 0

  def write(row: InternalRow): Unit = {
    var i = 0
    while (i < fields.size) {
      fields(i).write(row, i)
      i += 1
    }
    count += 1
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
  def valueMutator: ValueVector.Mutator

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
    valueMutator.setValueCount(count)
  }

  def reset(): Unit = {
    valueMutator.reset()
    count = 0
  }
}

private[arrow] class BooleanWriter(val valueVector: NullableBitVector) extends ArrowFieldWriter {

  override def valueMutator: NullableBitVector#Mutator = valueVector.getMutator()

  override def setNull(): Unit = {
    valueMutator.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueMutator.setSafe(count, if (input.getBoolean(ordinal)) 1 else 0)
  }
}

private[arrow] class ByteWriter(val valueVector: NullableTinyIntVector) extends ArrowFieldWriter {

  override def valueMutator: NullableTinyIntVector#Mutator = valueVector.getMutator()

  override def setNull(): Unit = {
    valueMutator.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueMutator.setSafe(count, input.getByte(ordinal))
  }
}

private[arrow] class ShortWriter(val valueVector: NullableSmallIntVector) extends ArrowFieldWriter {

  override def valueMutator: NullableSmallIntVector#Mutator = valueVector.getMutator()

  override def setNull(): Unit = {
    valueMutator.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueMutator.setSafe(count, input.getShort(ordinal))
  }
}

private[arrow] class IntegerWriter(val valueVector: NullableIntVector) extends ArrowFieldWriter {

  override def valueMutator: NullableIntVector#Mutator = valueVector.getMutator()

  override def setNull(): Unit = {
    valueMutator.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueMutator.setSafe(count, input.getInt(ordinal))
  }
}

private[arrow] class LongWriter(val valueVector: NullableBigIntVector) extends ArrowFieldWriter {

  override def valueMutator: NullableBigIntVector#Mutator = valueVector.getMutator()

  override def setNull(): Unit = {
    valueMutator.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueMutator.setSafe(count, input.getLong(ordinal))
  }
}

private[arrow] class FloatWriter(val valueVector: NullableFloat4Vector) extends ArrowFieldWriter {

  override def valueMutator: NullableFloat4Vector#Mutator = valueVector.getMutator()

  override def setNull(): Unit = {
    valueMutator.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueMutator.setSafe(count, input.getFloat(ordinal))
  }
}

private[arrow] class DoubleWriter(val valueVector: NullableFloat8Vector) extends ArrowFieldWriter {

  override def valueMutator: NullableFloat8Vector#Mutator = valueVector.getMutator()

  override def setNull(): Unit = {
    valueMutator.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueMutator.setSafe(count, input.getDouble(ordinal))
  }
}

private[arrow] class StringWriter(val valueVector: NullableVarCharVector) extends ArrowFieldWriter {

  override def valueMutator: NullableVarCharVector#Mutator = valueVector.getMutator()

  override def setNull(): Unit = {
    valueMutator.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val utf8 = input.getUTF8String(ordinal)
    val utf8ByteBuffer = utf8.getByteBuffer
    // todo: for off-heap UTF8String, how to pass in to arrow without copy?
    valueMutator.setSafe(count, utf8ByteBuffer, utf8ByteBuffer.position(), utf8.numBytes())
  }
}

private[arrow] class BinaryWriter(
    val valueVector: NullableVarBinaryVector) extends ArrowFieldWriter {

  override def valueMutator: NullableVarBinaryVector#Mutator = valueVector.getMutator()

  override def setNull(): Unit = {
    valueMutator.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val bytes = input.getBinary(ordinal)
    valueMutator.setSafe(count, bytes, 0, bytes.length)
  }
}

private[arrow] class DateWriter(val valueVector: NullableDateDayVector) extends ArrowFieldWriter {

  override def valueMutator: NullableDateDayVector#Mutator = valueVector.getMutator()

  override def setNull(): Unit = {
    valueMutator.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueMutator.setSafe(count, input.getInt(ordinal))
  }
}

private[arrow] class TimestampWriter(
    val valueVector: NullableTimeStampMicroTZVector) extends ArrowFieldWriter {

  override def valueMutator: NullableTimeStampMicroTZVector#Mutator = valueVector.getMutator()

  override def setNull(): Unit = {
    valueMutator.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueMutator.setSafe(count, input.getLong(ordinal))
  }
}

private[arrow] class ArrayWriter(
    val valueVector: ListVector,
    val elementWriter: ArrowFieldWriter) extends ArrowFieldWriter {

  override def valueMutator: ListVector#Mutator = valueVector.getMutator()

  override def setNull(): Unit = {
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val array = input.getArray(ordinal)
    var i = 0
    valueMutator.startNewValue(count)
    while (i < array.numElements()) {
      elementWriter.write(array, i)
      i += 1
    }
    valueMutator.endValue(count, array.numElements())
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
    val valueVector: NullableMapVector,
    children: Array[ArrowFieldWriter]) extends ArrowFieldWriter {

  override def valueMutator: NullableMapVector#Mutator = valueVector.getMutator()

  override def setNull(): Unit = {
    var i = 0
    while (i < children.length) {
      children(i).setNull()
      children(i).count += 1
      i += 1
    }
    valueMutator.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val struct = input.getStruct(ordinal, children.length)
    var i = 0
    while (i < struct.numFields) {
      children(i).write(struct, i)
      i += 1
    }
    valueMutator.setIndexDefined(count)
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
