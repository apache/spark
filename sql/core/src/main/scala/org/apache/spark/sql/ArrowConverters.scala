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

package org.apache.spark.sql

import java.io.ByteArrayOutputStream
import java.nio.channels.Channels

import scala.collection.JavaConverters._

import io.netty.buffer.ArrowBuf
import org.apache.arrow.memory.{BaseAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.BaseValueVector.BaseMutator
import org.apache.arrow.vector.file.ArrowWriter
import org.apache.arrow.vector.schema.{ArrowFieldNode, ArrowRecordBatch}
import org.apache.arrow.vector.types.{FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

/**
 * Intermediate data structure returned from Arrow conversions
 */
private[sql] abstract class ArrowPayload extends Iterator[ArrowRecordBatch]

/**
 * Class that wraps an Arrow RootAllocator used in conversion
 */
private[sql] class ArrowConverters {
  private val _allocator = new RootAllocator(Long.MaxValue)

  private[sql] def allocator: RootAllocator = _allocator

  private class ArrowStaticPayload(batches: ArrowRecordBatch*) extends ArrowPayload {
    private val iter = batches.iterator

    override def next(): ArrowRecordBatch = iter.next()
    override def hasNext: Boolean = iter.hasNext
  }

  def internalRowsToPayload(rows: Array[InternalRow], schema: StructType): ArrowPayload = {
    val batch = ArrowConverters.internalRowsToArrowRecordBatch(rows, schema, allocator)
    new ArrowStaticPayload(batch)
  }
}

private[sql] object ArrowConverters {

  /**
   * Map a Spark Dataset type to ArrowType.
   */
  private[sql] def sparkTypeToArrowType(dataType: DataType): ArrowType = {
    dataType match {
      case BooleanType => ArrowType.Bool.INSTANCE
      case ShortType => new ArrowType.Int(8 * ShortType.defaultSize, true)
      case IntegerType => new ArrowType.Int(8 * IntegerType.defaultSize, true)
      case LongType => new ArrowType.Int(8 * LongType.defaultSize, true)
      case FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
      case DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
      case ByteType => new ArrowType.Int(8, true)
      case StringType => ArrowType.Utf8.INSTANCE
      case BinaryType => ArrowType.Binary.INSTANCE
      case DateType => ArrowType.Date.INSTANCE
      case TimestampType => new ArrowType.Timestamp(TimeUnit.MILLISECOND)
      case _ => throw new UnsupportedOperationException(s"Unsupported data type: $dataType")
    }
  }

  /**
   * Transfer an array of InternalRow to an ArrowRecordBatch.
   */
  private[sql] def internalRowsToArrowRecordBatch(
      rows: Array[InternalRow],
      schema: StructType,
      allocator: RootAllocator): ArrowRecordBatch = {
    val fieldAndBuf = schema.fields.zipWithIndex.map { case (field, ordinal) =>
      internalRowToArrowBuf(rows, ordinal, field, allocator)
    }.unzip
    val fieldNodes = fieldAndBuf._1.flatten
    val buffers = fieldAndBuf._2.flatten

    val recordBatch = new ArrowRecordBatch(rows.length,
      fieldNodes.toList.asJava, buffers.toList.asJava)

    buffers.foreach(_.release())
    recordBatch
  }

  /**
   * Write a Field from array of InternalRow to an ArrowBuf.
   */
  private def internalRowToArrowBuf(
      rows: Array[InternalRow],
      ordinal: Int,
      field: StructField,
      allocator: RootAllocator): (Array[ArrowFieldNode], Array[ArrowBuf]) = {
    val numOfRows = rows.length
    val columnWriter = ColumnWriter(allocator, field.dataType)
    columnWriter.init(numOfRows)
    var index = 0

    while(index < numOfRows) {
      val row = rows(index)
      if (row.isNullAt(ordinal)) {
        columnWriter.writeNull()
      } else {
        columnWriter.write(row, ordinal)
      }
      index += 1
    }

    val (arrowFieldNodes, arrowBufs) = columnWriter.finish()
    (arrowFieldNodes.toArray, arrowBufs.toArray)
  }

  /**
   * Convert a Spark Dataset schema to Arrow schema.
   */
  private[sql] def schemaToArrowSchema(schema: StructType): Schema = {
    val arrowFields = schema.fields.map { f =>
      new Field(f.name, f.nullable, sparkTypeToArrowType(f.dataType), List.empty[Field].asJava)
    }
    new Schema(arrowFields.toList.asJava)
  }

  /**
   * Write an ArrowPayload to a byte array
   */
  private[sql] def payloadToByteArray(payload: ArrowPayload, schema: StructType): Array[Byte] = {
    val arrowSchema = ArrowConverters.schemaToArrowSchema(schema)
    val out = new ByteArrayOutputStream()
    val writer = new ArrowWriter(Channels.newChannel(out), arrowSchema)
    try {
      payload.foreach(writer.writeRecordBatch)
    } catch {
      case e: Exception =>
        throw e
    } finally {
      writer.close()
      payload.foreach(_.close())
    }
    out.toByteArray
  }
}

private[sql] trait ColumnWriter {
  def init(initialSize: Int): Unit
  def writeNull(): Unit
  def write(row: InternalRow, ordinal: Int): Unit

  /**
   * Clear the column writer and return the ArrowFieldNode and ArrowBuf.
   * This should be called only once after all the data is written.
   */
  def finish(): (Seq[ArrowFieldNode], Seq[ArrowBuf])
}

/**
 * Base class for flat arrow column writer, i.e., column without children.
 */
private[sql] abstract class PrimitiveColumnWriter(protected val allocator: BaseAllocator)
    extends ColumnWriter {
  protected def valueVector: BaseDataValueVector
  protected def valueMutator: BaseMutator

  protected def setNull(): Unit
  protected def setValue(row: InternalRow, ordinal: Int): Unit

  protected var count = 0
  protected var nullCount = 0

  override def init(initialSize: Int): Unit = {
    valueVector.allocateNew()
  }

  override def writeNull(): Unit = {
    setNull()
    nullCount += 1
    count += 1
  }

  override def write(row: InternalRow, ordinal: Int): Unit = {
    setValue(row, ordinal)
    count += 1
  }

  override def finish(): (Seq[ArrowFieldNode], Seq[ArrowBuf]) = {
    valueMutator.setValueCount(count)
    val fieldNode = new ArrowFieldNode(count, nullCount)
    val valueBuffers: Seq[ArrowBuf] = valueVector.getBuffers(true)
    (List(fieldNode), valueBuffers)
  }
}

private[sql] class BooleanColumnWriter(allocator: BaseAllocator)
    extends PrimitiveColumnWriter(allocator) {
  private def bool2int(b: Boolean): Int = if (b) 1 else 0

  override protected val valueVector: NullableBitVector
    = new NullableBitVector("BooleanValue", allocator)
  override protected val valueMutator: NullableBitVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow, ordinal: Int): Unit
    = valueMutator.setSafe(count, bool2int(row.getBoolean(ordinal)))
}

private[sql] class ShortColumnWriter(allocator: BaseAllocator)
    extends PrimitiveColumnWriter(allocator) {
  override protected val valueVector: NullableSmallIntVector
    = new NullableSmallIntVector("ShortValue", allocator)
  override protected val valueMutator: NullableSmallIntVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow, ordinal: Int): Unit
    = valueMutator.setSafe(count, row.getShort(ordinal))
}

private[sql] class IntegerColumnWriter(allocator: BaseAllocator)
    extends PrimitiveColumnWriter(allocator) {
  override protected val valueVector: NullableIntVector
    = new NullableIntVector("IntValue", allocator)
  override protected val valueMutator: NullableIntVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow, ordinal: Int): Unit
    = valueMutator.setSafe(count, row.getInt(ordinal))
}

private[sql] class LongColumnWriter(allocator: BaseAllocator)
    extends PrimitiveColumnWriter(allocator) {
  override protected val valueVector: NullableBigIntVector
    = new NullableBigIntVector("LongValue", allocator)
  override protected val valueMutator: NullableBigIntVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow, ordinal: Int): Unit
    = valueMutator.setSafe(count, row.getLong(ordinal))
}

private[sql] class FloatColumnWriter(allocator: BaseAllocator)
    extends PrimitiveColumnWriter(allocator) {
  override protected val valueVector: NullableFloat4Vector
    = new NullableFloat4Vector("FloatValue", allocator)
  override protected val valueMutator: NullableFloat4Vector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow, ordinal: Int): Unit
    = valueMutator.setSafe(count, row.getFloat(ordinal))
}

private[sql] class DoubleColumnWriter(allocator: BaseAllocator)
    extends PrimitiveColumnWriter(allocator) {
  override protected val valueVector: NullableFloat8Vector
    = new NullableFloat8Vector("DoubleValue", allocator)
  override protected val valueMutator: NullableFloat8Vector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow, ordinal: Int): Unit
    = valueMutator.setSafe(count, row.getDouble(ordinal))
}

private[sql] class ByteColumnWriter(allocator: BaseAllocator)
    extends PrimitiveColumnWriter(allocator) {
  override protected val valueVector: NullableUInt1Vector
    = new NullableUInt1Vector("ByteValue", allocator)
  override protected val valueMutator: NullableUInt1Vector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow, ordinal: Int): Unit
    = valueMutator.setSafe(count, row.getByte(ordinal))
}

private[sql] class UTF8StringColumnWriter(allocator: BaseAllocator)
    extends PrimitiveColumnWriter(allocator) {
  override protected val valueVector: NullableVarBinaryVector
    = new NullableVarBinaryVector("UTF8StringValue", allocator)
  override protected val valueMutator: NullableVarBinaryVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow, ordinal: Int): Unit = {
    val bytes = row.getUTF8String(ordinal).getBytes
    valueMutator.setSafe(count, bytes, 0, bytes.length)
  }
}

private[sql] class BinaryColumnWriter(allocator: BaseAllocator)
    extends PrimitiveColumnWriter(allocator) {
  override protected val valueVector: NullableVarBinaryVector
    = new NullableVarBinaryVector("BinaryValue", allocator)
  override protected val valueMutator: NullableVarBinaryVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow, ordinal: Int): Unit = {
    val bytes = row.getBinary(ordinal)
    valueMutator.setSafe(count, bytes, 0, bytes.length)
  }
}

private[sql] class DateColumnWriter(allocator: BaseAllocator)
    extends PrimitiveColumnWriter(allocator) {
  override protected val valueVector: NullableDateVector
    = new NullableDateVector("DateValue", allocator)
  override protected val valueMutator: NullableDateVector#Mutator = valueVector.getMutator

  override protected def setNull(): Unit = valueMutator.setNull(count)
  override protected def setValue(row: InternalRow, ordinal: Int): Unit = {
    // TODO: comment on diff btw value representations of date/timestamp
    valueMutator.setSafe(count, row.getInt(ordinal).toLong * 24 * 3600 * 1000)
  }
}

private[sql] class TimeStampColumnWriter(allocator: BaseAllocator)
    extends PrimitiveColumnWriter(allocator) {
  override protected val valueVector: NullableTimeStampVector
    = new NullableTimeStampVector("TimeStampValue", allocator)
  override protected val valueMutator: NullableTimeStampVector#Mutator = valueVector.getMutator

  override protected def setNull(): Unit = valueMutator.setNull(count)

  override protected def setValue(row: InternalRow, ordinal: Int): Unit = {
    // TODO: use microsecond timestamp when ARROW-477 is resolved
    valueMutator.setSafe(count, row.getLong(ordinal) / 1000)
  }
}

private[sql] object ColumnWriter {
  def apply(allocator: BaseAllocator, dataType: DataType): ColumnWriter = {
    dataType match {
      case BooleanType => new BooleanColumnWriter(allocator)
      case ShortType => new ShortColumnWriter(allocator)
      case IntegerType => new IntegerColumnWriter(allocator)
      case LongType => new LongColumnWriter(allocator)
      case FloatType => new FloatColumnWriter(allocator)
      case DoubleType => new DoubleColumnWriter(allocator)
      case ByteType => new ByteColumnWriter(allocator)
      case StringType => new UTF8StringColumnWriter(allocator)
      case BinaryType => new BinaryColumnWriter(allocator)
      case DateType => new DateColumnWriter(allocator)
      case TimestampType => new TimeStampColumnWriter(allocator)
      case _ => throw new UnsupportedOperationException(s"Unsupported data type: $dataType")
    }
  }
}
