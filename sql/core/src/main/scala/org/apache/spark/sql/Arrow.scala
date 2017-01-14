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

  private case class TypeFuncs(getType: () => ArrowType,
                               fill: ArrowBuf => Unit,
                               write: (InternalRow, Int, ArrowBuf) => Unit)

  private def getTypeFuncs(dataType: DataType): TypeFuncs = {
    val err = s"Unsupported data type ${dataType.simpleString}"

    dataType match {
      case NullType =>
        TypeFuncs(
          () => ArrowType.Null.INSTANCE,
          (buf: ArrowBuf) => (),
          (row: InternalRow, ordinal: Int, buf: ArrowBuf) => ())
      case BooleanType =>
        TypeFuncs(
          () => ArrowType.Bool.INSTANCE,
          (buf: ArrowBuf) => buf.writeBoolean(false),
          (row: InternalRow, ordinal: Int, buf: ArrowBuf) =>
            buf.writeBoolean(row.getBoolean(ordinal)))
      case ShortType =>
        TypeFuncs(
          () => new ArrowType.Int(8 * ShortType.defaultSize, true),
          (buf: ArrowBuf) => buf.writeShort(0),
          (row: InternalRow, ordinal: Int, buf: ArrowBuf) => buf.writeShort(row.getShort(ordinal)))
      case IntegerType =>
        TypeFuncs(
          () => new ArrowType.Int(8 * IntegerType.defaultSize, true),
          (buf: ArrowBuf) => buf.writeInt(0),
          (row: InternalRow, ordinal: Int, buf: ArrowBuf) => buf.writeInt(row.getInt(ordinal)))
      case LongType =>
        TypeFuncs(
          () => new ArrowType.Int(8 * LongType.defaultSize, true),
          (buf: ArrowBuf) => buf.writeLong(0L),
          (row: InternalRow, ordinal: Int, buf: ArrowBuf) => buf.writeLong(row.getLong(ordinal)))
      case FloatType =>
        TypeFuncs(
          () => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
          (buf: ArrowBuf) => buf.writeFloat(0f),
          (row: InternalRow, ordinal: Int, buf: ArrowBuf) => buf.writeFloat(row.getFloat(ordinal)))
      case DoubleType =>
        TypeFuncs(
          () => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
          (buf: ArrowBuf) => buf.writeDouble(0d),
          (row: InternalRow, ordinal: Int, buf: ArrowBuf) =>
            buf.writeDouble(row.getDouble(ordinal)))
      case ByteType =>
        TypeFuncs(
          () => new ArrowType.Int(8, false),
          (buf: ArrowBuf) => buf.writeByte(0),
          (row: InternalRow, ordinal: Int, buf: ArrowBuf) => buf.writeByte(row.getByte(ordinal)))
      case StringType =>
        TypeFuncs(
          () => ArrowType.Utf8.INSTANCE,
          (buf: ArrowBuf) => throw new UnsupportedOperationException(err),  // TODO
          (row: InternalRow, ordinal: Int, buf: ArrowBuf) =>
            throw new UnsupportedOperationException(err))
      case StructType(_) =>
        TypeFuncs(
          () => ArrowType.Struct.INSTANCE,
          (buf: ArrowBuf) => throw new UnsupportedOperationException(err),  // TODO
          (row: InternalRow, ordinal: Int, buf: ArrowBuf) =>
            throw new UnsupportedOperationException(err))
      case _ =>
        throw new IllegalArgumentException(err)
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
      case ShortType | IntegerType | LongType | DoubleType | FloatType | BooleanType | ByteType =>
        val validityVector = new BitVector("validity", allocator)
        val validityMutator = validityVector.getMutator
        validityVector.allocateNew(numOfRows)
        validityMutator.setValueCount(numOfRows)

        val buf = allocator.buffer(numOfRows * field.dataType.defaultSize)
        val typeFunc = getTypeFuncs(field.dataType)
        var nullCount = 0
        var index = 0
        while (index < rows.length) {
          val row = rows(index)
          if (row.isNullAt(ordinal)) {
            nullCount += 1
            validityMutator.set(index, 0)
            typeFunc.fill(buf)
          } else {
            validityMutator.set(index, 1)
            typeFunc.write(row, ordinal, buf)
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
    val arrowFields = schema.fields.map(sparkFieldToArrowField)
    new Schema(arrowFields.toList.asJava)
  }

  private[sql] def sparkFieldToArrowField(sparkField: StructField): Field = {
    val name = sparkField.name
    val dataType = sparkField.dataType
    val nullable = sparkField.nullable

    dataType match {
      case StructType(fields) =>
        val childrenFields = fields.map(sparkFieldToArrowField).toList.asJava
        new Field(name, nullable, ArrowType.Struct.INSTANCE, childrenFields)
      case _ =>
        new Field(name, nullable, getTypeFuncs(dataType).getType(), List.empty[Field].asJava)
    }
  }
}
