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
import org.apache.arrow.vector.util.DecimalUtility

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types._

object ArrowWriter {

  def create(schema: StructType): ArrowWriter = {
    val arrowSchema = ArrowUtils.toArrowSchema(schema)
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
    ArrowUtils.fromArrowField(field) match {
      case BooleanType =>
        new BooleanWriter(vector.asInstanceOf[NullableBitVector])
      case ByteType =>
        new ByteWriter(vector.asInstanceOf[NullableTinyIntVector])
      case ShortType =>
        new ShortWriter(vector.asInstanceOf[NullableSmallIntVector])
      case IntegerType =>
        new IntegerWriter(vector.asInstanceOf[NullableIntVector])
      case LongType =>
        new LongWriter(vector.asInstanceOf[NullableBigIntVector])
      case FloatType =>
        new FloatWriter(vector.asInstanceOf[NullableFloat4Vector])
      case DoubleType =>
        new DoubleWriter(vector.asInstanceOf[NullableFloat8Vector])
      case StringType =>
        new StringWriter(vector.asInstanceOf[NullableVarCharVector])
      case BinaryType =>
        new BinaryWriter(vector.asInstanceOf[NullableVarBinaryVector])
      case ArrayType(_, _) =>
        val v = vector.asInstanceOf[ListVector]
        val elementVector = createFieldWriter(v.getDataVector())
        new ArrayWriter(v, elementVector)
      case StructType(_) =>
        val v = vector.asInstanceOf[NullableMapVector]
        val children = (0 until v.size()).map { ordinal =>
          createFieldWriter(v.getChildByOrdinal(ordinal))
        }
        new StructWriter(v, children.toArray)
      case dt =>
        throw new UnsupportedOperationException(s"Unsupported data type: ${dt.simpleString}")
    }
  }
}

class ArrowWriter(
    val root: VectorSchemaRoot,
    fields: Array[ArrowFieldWriter]) {

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
  def skip(): Unit

  protected var count: Int = 0

  def write(input: SpecializedGetters, ordinal: Int): Unit = {
    if (input.isNullAt(ordinal)) {
      setNull()
    } else {
      setValue(input, ordinal)
    }
    count += 1
  }

  def writeSkip(): Unit = {
    skip()
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

  override def skip(): Unit = {
    valueMutator.setIndexDefined(count)
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

  override def skip(): Unit = {
    valueMutator.setIndexDefined(count)
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

  override def skip(): Unit = {
    valueMutator.setIndexDefined(count)
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

  override def skip(): Unit = {
    valueMutator.setIndexDefined(count)
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

  override def skip(): Unit = {
    valueMutator.setIndexDefined(count)
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

  override def skip(): Unit = {
    valueMutator.setIndexDefined(count)
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

  override def skip(): Unit = {
    valueMutator.setIndexDefined(count)
  }
}

private[arrow] class StringWriter(val valueVector: NullableVarCharVector) extends ArrowFieldWriter {

  override def valueMutator: NullableVarCharVector#Mutator = valueVector.getMutator()

  override def setNull(): Unit = {
    valueMutator.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val utf8 = input.getUTF8String(ordinal)
    // todo: for off-heap UTF8String, how to pass in to arrow without copy?
    valueMutator.setSafe(count, utf8.getByteBuffer, 0, utf8.numBytes())
  }

  override def skip(): Unit = {
    valueMutator.setIndexDefined(count)
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

  override def skip(): Unit = {
    valueMutator.setIndexDefined(count)
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

  override def skip(): Unit = {
    valueMutator.setNotNull(count)
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
      children(i).writeSkip()
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

  override def skip(): Unit = {
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
