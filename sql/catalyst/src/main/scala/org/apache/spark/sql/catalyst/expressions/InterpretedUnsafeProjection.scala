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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeArrayWriter, UnsafeRowWriter, UnsafeWriter}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{UserDefinedType, _}
import org.apache.spark.unsafe.Platform

/**
 * An interpreted unsafe projection. This class reuses the [[UnsafeRow]] it produces, a consumer
 * should copy the row if it is being buffered. This class is not thread safe.
 *
 * @param expressions that produces the resulting fields. These expressions must be bound
 *                    to a schema.
 */
class InterpretedUnsafeProjection(expressions: Array[Expression]) extends UnsafeProjection {
  import InterpretedUnsafeProjection._

  /** Number of (top level) fields in the resulting row. */
  private[this] val numFields = expressions.length

  /** Array that expression results. */
  private[this] val values = new Array[Any](numFields)

  /** The row representing the expression results. */
  private[this] val intermediate = new GenericInternalRow(values)

  /** The row returned by the projection. */
  private[this] val result = new UnsafeRow(numFields)

  /** The buffer which holds the resulting row's backing data. */
  private[this] val holder = new BufferHolder(result, numFields * 32)

  /** The writer that writes the intermediate result to the result row. */
  private[this] val writer: InternalRow => Unit = {
    val rowWriter = new UnsafeRowWriter(holder, numFields)
    val baseWriter = generateStructWriter(
      holder,
      rowWriter,
      expressions.map(e => StructField("", e.dataType, e.nullable)))
    if (!expressions.exists(_.nullable)) {
      // No nullable fields. The top-level null bit mask will always be zeroed out.
      baseWriter
    } else {
      // Zero out the null bit mask before we write the row.
      row => {
        rowWriter.zeroOutNullBytes()
        baseWriter(row)
      }
    }
  }

  override def initialize(partitionIndex: Int): Unit = {
    expressions.foreach(_.foreach {
      case n: Nondeterministic => n.initialize(partitionIndex)
      case _ =>
    })
  }

  override def apply(row: InternalRow): UnsafeRow = {
    // Put the expression results in the intermediate row.
    var i = 0
    while (i < numFields) {
      values(i) = expressions(i).eval(row)
      i += 1
    }

    // Write the intermediate row to an unsafe row.
    holder.reset()
    writer(intermediate)
    result.setTotalSize(holder.totalSize())
    result
  }
}

/**
 * Helper functions for creating an [[InterpretedUnsafeProjection]].
 */
object InterpretedUnsafeProjection extends UnsafeProjectionCreator {

  /**
   * Returns an [[UnsafeProjection]] for given sequence of bound Expressions.
   */
  override protected def createProjection(exprs: Seq[Expression]): UnsafeProjection = {
    // We need to make sure that we do not reuse stateful expressions.
    val cleanedExpressions = exprs.map(_.transform {
      case s: Stateful => s.freshCopy()
    })
    new InterpretedUnsafeProjection(cleanedExpressions.toArray)
  }

  /**
   * Generate a struct writer function. The generated function writes an [[InternalRow]] to the
   * given buffer using the given [[UnsafeRowWriter]].
   */
  private def generateStructWriter(
      bufferHolder: BufferHolder,
      rowWriter: UnsafeRowWriter,
      fields: Array[StructField]): InternalRow => Unit = {
    val numFields = fields.length

    // Create field writers.
    val fieldWriters = fields.map { field =>
      generateFieldWriter(bufferHolder, rowWriter, field.dataType, field.nullable)
    }
    // Create basic writer.
    row => {
      var i = 0
      while (i < numFields) {
        fieldWriters(i).apply(row, i)
        i += 1
      }
    }
  }

  /**
   * Generate a writer function for a struct field, array element, map key or map value. The
   * generated function writes the element at an index in a [[SpecializedGetters]] object (row
   * or array) to the given buffer using the given [[UnsafeWriter]].
   */
  private def generateFieldWriter(
      bufferHolder: BufferHolder,
      writer: UnsafeWriter,
      dt: DataType,
      nullable: Boolean): (SpecializedGetters, Int) => Unit = {

    // Create the the basic writer.
    val unsafeWriter: (SpecializedGetters, Int) => Unit = dt match {
      case BooleanType =>
        (v, i) => writer.write(i, v.getBoolean(i))

      case ByteType =>
        (v, i) => writer.write(i, v.getByte(i))

      case ShortType =>
        (v, i) => writer.write(i, v.getShort(i))

      case IntegerType | DateType =>
        (v, i) => writer.write(i, v.getInt(i))

      case LongType | TimestampType =>
        (v, i) => writer.write(i, v.getLong(i))

      case FloatType =>
        (v, i) => writer.write(i, v.getFloat(i))

      case DoubleType =>
        (v, i) => writer.write(i, v.getDouble(i))

      case DecimalType.Fixed(precision, scale) =>
        (v, i) => writer.write(i, v.getDecimal(i, precision, scale), precision, scale)

      case CalendarIntervalType =>
        (v, i) => writer.write(i, v.getInterval(i))

      case BinaryType =>
        (v, i) => writer.write(i, v.getBinary(i))

      case StringType =>
        (v, i) => writer.write(i, v.getUTF8String(i))

      case StructType(fields) =>
        val numFields = fields.length
        val rowWriter = new UnsafeRowWriter(bufferHolder, numFields)
        val structWriter = generateStructWriter(bufferHolder, rowWriter, fields)
        (v, i) => {
          val tmpCursor = bufferHolder.cursor
          v.getStruct(i, fields.length) match {
            case row: UnsafeRow =>
              writeUnsafeData(
                bufferHolder,
                row.getBaseObject,
                row.getBaseOffset,
                row.getSizeInBytes)
            case row =>
              // Nested struct. We don't know where this will start because a row can be
              // variable length, so we need to update the offsets and zero out the bit mask.
              rowWriter.reset()
              structWriter.apply(row)
          }
          writer.setOffsetAndSize(i, tmpCursor, bufferHolder.cursor - tmpCursor)
        }

      case ArrayType(elementType, containsNull) =>
        val arrayWriter = new UnsafeArrayWriter
        val elementSize = getElementSize(elementType)
        val elementWriter = generateFieldWriter(
          bufferHolder,
          arrayWriter,
          elementType,
          containsNull)
        (v, i) => {
          val tmpCursor = bufferHolder.cursor
          writeArray(bufferHolder, arrayWriter, elementWriter, v.getArray(i), elementSize)
          writer.setOffsetAndSize(i, tmpCursor, bufferHolder.cursor - tmpCursor)
        }

      case MapType(keyType, valueType, valueContainsNull) =>
        val keyArrayWriter = new UnsafeArrayWriter
        val keySize = getElementSize(keyType)
        val keyWriter = generateFieldWriter(
          bufferHolder,
          keyArrayWriter,
          keyType,
          nullable = false)
        val valueArrayWriter = new UnsafeArrayWriter
        val valueSize = getElementSize(valueType)
        val valueWriter = generateFieldWriter(
          bufferHolder,
          valueArrayWriter,
          valueType,
          valueContainsNull)
        (v, i) => {
          val tmpCursor = bufferHolder.cursor
          v.getMap(i) match {
            case map: UnsafeMapData =>
              writeUnsafeData(
                bufferHolder,
                map.getBaseObject,
                map.getBaseOffset,
                map.getSizeInBytes)
            case map =>
              // preserve 8 bytes to write the key array numBytes later.
              bufferHolder.grow(8)
              bufferHolder.cursor += 8

              // Write the keys and write the numBytes of key array into the first 8 bytes.
              writeArray(bufferHolder, keyArrayWriter, keyWriter, map.keyArray(), keySize)
              Platform.putLong(bufferHolder.buffer, tmpCursor, bufferHolder.cursor - tmpCursor - 8)

              // Write the values.
              writeArray(bufferHolder, valueArrayWriter, valueWriter, map.valueArray(), valueSize)
          }
          writer.setOffsetAndSize(i, tmpCursor, bufferHolder.cursor - tmpCursor)
        }

      case udt: UserDefinedType[_] =>
        generateFieldWriter(bufferHolder, writer, udt.sqlType, nullable)

      case NullType =>
        (_, _) => {}

      case _ =>
        throw new SparkException(s"Unsupported data type $dt")
    }

    // Always wrap the writer with a null safe version.
    dt match {
      case _: UserDefinedType[_] =>
        // The null wrapper depends on the sql type and not on the UDT.
        unsafeWriter
      case DecimalType.Fixed(precision, _) if precision > Decimal.MAX_LONG_DIGITS =>
        // We can't call setNullAt() for DecimalType with precision larger than 18, we call write
        // directly. We can use the unwrapped writer directly.
        unsafeWriter
      case BooleanType | ByteType =>
        (v, i) => {
          if (!v.isNullAt(i)) {
            unsafeWriter(v, i)
          } else {
            writer.setNull1Bytes(i)
          }
        }
      case ShortType =>
        (v, i) => {
          if (!v.isNullAt(i)) {
            unsafeWriter(v, i)
          } else {
            writer.setNull2Bytes(i)
          }
        }
      case IntegerType | DateType | FloatType =>
        (v, i) => {
          if (!v.isNullAt(i)) {
            unsafeWriter(v, i)
          } else {
            writer.setNull4Bytes(i)
          }
        }
      case _ =>
        (v, i) => {
          if (!v.isNullAt(i)) {
            unsafeWriter(v, i)
          } else {
            writer.setNull8Bytes(i)
          }
        }
    }
  }

  /**
   * Get the number of bytes elements of a data type will occupy in the fixed part of an
   * [[UnsafeArrayData]] object. Reference types are stored as an 8 byte combination of an
   * offset (upper 4 bytes) and a length (lower 4 bytes), these point to the variable length
   * portion of the array object. Primitives take up to 8 bytes, depending on the size of the
   * underlying data type.
   */
  private def getElementSize(dataType: DataType): Int = dataType match {
    case NullType | StringType | BinaryType | CalendarIntervalType |
         _: DecimalType | _: StructType | _: ArrayType | _: MapType => 8
    case _ => dataType.defaultSize
  }

  /**
   * Write an array to the buffer. If the array is already in serialized form (an instance of
   * [[UnsafeArrayData]]) then we copy the bytes directly, otherwise we do an element-by-element
   * copy.
   */
  private def writeArray(
      bufferHolder: BufferHolder,
      arrayWriter: UnsafeArrayWriter,
      elementWriter: (SpecializedGetters, Int) => Unit,
      array: ArrayData,
      elementSize: Int): Unit = array match {
    case unsafe: UnsafeArrayData =>
      writeUnsafeData(
        bufferHolder,
        unsafe.getBaseObject,
        unsafe.getBaseOffset,
        unsafe.getSizeInBytes)
    case _ =>
      val numElements = array.numElements()
      arrayWriter.initialize(bufferHolder, numElements, elementSize)
      var i = 0
      while (i < numElements) {
        elementWriter.apply(array, i)
        i += 1
      }
  }

  /**
   * Write an opaque block of data to the buffer. This is used to copy
   * [[UnsafeRow]], [[UnsafeArrayData]] and [[UnsafeMapData]] objects.
   */
  private def writeUnsafeData(
      bufferHolder: BufferHolder,
      baseObject: AnyRef,
      baseOffset: Long,
      sizeInBytes: Int) : Unit = {
    bufferHolder.grow(sizeInBytes)
    Platform.copyMemory(
      baseObject,
      baseOffset,
      bufferHolder.buffer,
      bufferHolder.cursor,
      sizeInBytes)
    bufferHolder.cursor += sizeInBytes
  }
}
