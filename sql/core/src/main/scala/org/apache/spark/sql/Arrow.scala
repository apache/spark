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

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import io.netty.buffer.ArrowBuf
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.schema.{ArrowFieldNode, ArrowRecordBatch}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

object Arrow {

  /**
   * Compute the number of bytes needed to build validity map. According to
   * [Arrow Layout](https://github.com/apache/arrow/blob/master/format/Layout.md#null-bitmaps),
   * the length of the validity bitmap should be multiples of 64 bytes.
   */
  private def numBytesOfBitmap(numOfRows: Int): Int = {
    Math.ceil(numOfRows / 64.0).toInt * 8
  }

  private def fillArrow(buf: ArrowBuf, dataType: DataType): Unit = {
    dataType match {
      case NullType =>
      case BooleanType =>
        buf.writeBoolean(false)
      case ShortType =>
        buf.writeShort(0)
      case IntegerType =>
        buf.writeInt(0)
      case LongType =>
        buf.writeLong(0L)
      case FloatType =>
        buf.writeFloat(0f)
      case DoubleType =>
        buf.writeDouble(0d)
      case ByteType =>
        buf.writeByte(0)
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported data type ${dataType.simpleString}")
    }
  }

  /**
   * Get an entry from the InternalRow, and then set to ArrowBuf.
   * Note: No Null check for the entry.
   */
  private def getAndSetToArrow(
      row: InternalRow,
      buf: ArrowBuf,
      dataType: DataType,
      ordinal: Int): Unit = {
    dataType match {
      case NullType =>
      case BooleanType =>
        buf.writeBoolean(row.getBoolean(ordinal))
      case ShortType =>
        buf.writeShort(row.getShort(ordinal))
      case IntegerType =>
        buf.writeInt(row.getInt(ordinal))
      case LongType =>
        buf.writeLong(row.getLong(ordinal))
      case FloatType =>
        buf.writeFloat(row.getFloat(ordinal))
      case DoubleType =>
        buf.writeDouble(row.getDouble(ordinal))
      case ByteType =>
        buf.writeByte(row.getByte(ordinal))
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported data type ${dataType.simpleString}")
    }
  }

  /**
   * Transfer an array of InternalRow to an ArrowRecordBatch.
   */
  def internalRowsToArrowRecordBatch(
      rows: Array[InternalRow],
      schema: StructType,
      allocator: RootAllocator): ArrowRecordBatch = {
    val bufAndField = schema.fields.zipWithIndex.map { case (field, ordinal) =>
      internalRowToArrowBuf(rows, ordinal, field, allocator)
    }

    val buffers = bufAndField.flatMap(_._1).toList.asJava
    val fieldNodes = bufAndField.flatMap(_._2).toList.asJava

    new ArrowRecordBatch(rows.length, fieldNodes, buffers)
  }

  /**
   * Convert an array of InternalRow to an ArrowBuf.
   */
  def internalRowToArrowBuf(
      rows: Array[InternalRow],
      ordinal: Int,
      field: StructField,
      allocator: RootAllocator): (Array[ArrowBuf], Array[ArrowFieldNode]) = {
    val numOfRows = rows.length

    field.dataType match {
      case IntegerType | LongType | DoubleType | FloatType | BooleanType | ByteType =>
        val validityVector = new BitVector("validity", allocator)
        val validityMutator = validityVector.getMutator
        validityVector.allocateNew(numOfRows)
        validityMutator.setValueCount(numOfRows)

        val buf = allocator.buffer(numOfRows * field.dataType.defaultSize)
        var nullCount = 0
        var index = 0
        while (index < rows.length) {
          val row = rows(index)
          if (row.isNullAt(ordinal)) {
            nullCount += 1
            validityMutator.set(index, 0)
            fillArrow(buf, field.dataType)
          } else {
            validityMutator.set(index, 1)
            getAndSetToArrow(row, buf, field.dataType, ordinal)
          }
          index += 1
        }

        val fieldNode = new ArrowFieldNode(numOfRows, nullCount)

        (Array(validityVector.getBuffer, buf), Array(fieldNode))

      case StringType =>
        val validityVector = new BitVector("validity", allocator)
        val validityMutator = validityVector.getMutator()
        validityVector.allocateNew(numOfRows)
        validityMutator.setValueCount(numOfRows)

        val bufOffset = allocator.buffer((numOfRows + 1) * IntegerType.defaultSize)
        var bytesCount = 0
        bufOffset.writeInt(bytesCount)
        val bufValues = allocator.buffer(1024)
        var nullCount = 0
        rows.zipWithIndex.foreach { case (row, index) =>
          if (row.isNullAt(ordinal)) {
            nullCount += 1
            validityMutator.set(index, 0)
            bufOffset.writeInt(bytesCount)
          } else {
            validityMutator.set(index, 1)
            val bytes = row.getUTF8String(ordinal).getBytes
            bytesCount += bytes.length
            bufOffset.writeInt(bytesCount)
            bufValues.writeBytes(bytes)
          }
        }

        val fieldNode = new ArrowFieldNode(numOfRows, nullCount)

        (Array(validityVector.getBuffer, bufOffset, bufValues),
            Array(fieldNode))
    }
  }

  private[sql] def schemaToArrowSchema(schema: StructType): Schema = {
    val arrowFields = schema.fields.map(sparkFieldToArrowField(_))
    new Schema(arrowFields.toList.asJava)
  }

  private[sql] def sparkFieldToArrowField(sparkField: StructField): Field = {
    val name = sparkField.name
    val dataType = sparkField.dataType
    val nullable = sparkField.nullable

    dataType match {
      case StructType(fields) =>
        val childrenFields = fields.map(sparkFieldToArrowField(_)).toList.asJava
        new Field(name, nullable, ArrowType.Struct.INSTANCE, childrenFields)
      case _ =>
        new Field(name, nullable, dataTypeToArrowType(dataType), List.empty[Field].asJava)
    }
  }

  /**
   * Transform Spark DataType to Arrow ArrowType.
   */
  private[sql] def dataTypeToArrowType(dt: DataType): ArrowType = {
    dt match {
      case IntegerType =>
        new ArrowType.Int(8 * IntegerType.defaultSize, true)
      case LongType =>
        new ArrowType.Int(8 * LongType.defaultSize, true)
      case StringType =>
        ArrowType.Utf8.INSTANCE
      case DoubleType =>
        new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
      case FloatType =>
        new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
      case BooleanType =>
        ArrowType.Bool.INSTANCE
      case ByteType =>
        new ArrowType.Int(8, false)
      case StructType(_) =>
        ArrowType.Struct.INSTANCE
      case _ =>
        throw new IllegalArgumentException(s"Unsupported data type")
    }
  }
}
