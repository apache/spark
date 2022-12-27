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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{UnsafeArrayWriter, UnsafeRowWriter, UnsafeWriter}
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.internal.SQLConf
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

  private[this] val subExprEliminationEnabled = SQLConf.get.subexpressionEliminationEnabled
  private[this] val exprs = prepareExpressions(expressions, subExprEliminationEnabled)

  /** Number of (top level) fields in the resulting row. */
  private[this] val numFields = expressions.length

  /** Array that expression results. */
  private[this] val values = new Array[Any](numFields)

  /** The row representing the expression results. */
  private[this] val intermediate = new GenericInternalRow(values)

  /* The row writer for UnsafeRow result */
  private[this] val rowWriter = new UnsafeRowWriter(numFields, numFields * 32)

  /** The writer that writes the intermediate result to the result row. */
  private[this] val writer: InternalRow => Unit = {
    val baseWriter = generateStructWriter(
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
    exprs.foreach(_.foreach {
      case n: Nondeterministic => n.initialize(partitionIndex)
      case _ =>
    })
  }

  override def apply(row: InternalRow): UnsafeRow = {
    if (subExprEliminationEnabled) {
      runtime.setInput(row)
    }

    // Put the expression results in the intermediate row.
    var i = 0
    while (i < numFields) {
      values(i) = exprs(i).eval(row)
      i += 1
    }

    // Write the intermediate row to an unsafe row.
    rowWriter.reset()
    writer(intermediate)
    rowWriter.getRow()
  }
}

/**
 * Helper functions for creating an [[InterpretedUnsafeProjection]].
 */
object InterpretedUnsafeProjection {
  /**
   * Returns an [[UnsafeProjection]] for given sequence of bound Expressions.
   */
  def createProjection(exprs: Seq[Expression]): UnsafeProjection = {
    new InterpretedUnsafeProjection(exprs.toArray)
  }

  /**
   * Generate a struct writer function. The generated function writes an [[InternalRow]] to the
   * given buffer using the given [[UnsafeRowWriter]].
   */
  private def generateStructWriter(
      rowWriter: UnsafeRowWriter,
      fields: Array[StructField]): InternalRow => Unit = {
    val numFields = fields.length

    // Create field writers.
    val fieldWriters = fields.map { field =>
      generateFieldWriter(rowWriter, field.dataType, field.nullable)
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
      writer: UnsafeWriter,
      dt: DataType,
      nullable: Boolean): (SpecializedGetters, Int) => Unit = {

    // Create the basic writer.
    val unsafeWriter: (SpecializedGetters, Int) => Unit = dt match {
      case udt: UserDefinedType[_] => generateFieldWriter(writer, udt.sqlType, nullable)
      case _ => dt.physicalDataType match {
        case PhysicalBooleanType => (v, i) => writer.write(i, v.getBoolean(i))

        case PhysicalByteType => (v, i) => writer.write(i, v.getByte(i))

        case PhysicalShortType => (v, i) => writer.write(i, v.getShort(i))

        case PhysicalIntegerType => (v, i) => writer.write(i, v.getInt(i))

        case PhysicalLongType => (v, i) => writer.write(i, v.getLong(i))

        case PhysicalFloatType => (v, i) => writer.write(i, v.getFloat(i))

        case PhysicalDoubleType => (v, i) => writer.write(i, v.getDouble(i))

        case PhysicalDecimalType(precision, scale) =>
          (v, i) => writer.write(i, v.getDecimal(i, precision, scale), precision, scale)

        case PhysicalCalendarIntervalType => (v, i) => writer.write(i, v.getInterval(i))

        case PhysicalBinaryType => (v, i) => writer.write(i, v.getBinary(i))

        case PhysicalStringType => (v, i) => writer.write(i, v.getUTF8String(i))

        case PhysicalStructType(fields) =>
          val numFields = fields.length
          val rowWriter = new UnsafeRowWriter(writer, numFields)
          val structWriter = generateStructWriter(rowWriter, fields)
            (v, i) => {
            v.getStruct(i, fields.length) match {
              case row: UnsafeRow =>
                writer.write(i, row)
              case row =>
                val previousCursor = writer.cursor()
                // Nested struct. We don't know where this will start because a row can be
                // variable length, so we need to update the offsets and zero out the bit mask.
                rowWriter.resetRowWriter()
                structWriter.apply(row)
                writer.setOffsetAndSizeFromPreviousCursor(i, previousCursor)
            }
          }

        case PhysicalArrayType(elementType, containsNull) =>
          val arrayWriter = new UnsafeArrayWriter(writer, getElementSize(elementType))
          val elementWriter = generateFieldWriter(
            arrayWriter,
            elementType,
            containsNull)
            (v, i) => {
            val previousCursor = writer.cursor()
            writeArray(arrayWriter, elementWriter, v.getArray(i))
            writer.setOffsetAndSizeFromPreviousCursor(i, previousCursor)
          }

        case PhysicalMapType(keyType, valueType, valueContainsNull) =>
          val keyArrayWriter = new UnsafeArrayWriter(writer, getElementSize(keyType))
          val keyWriter = generateFieldWriter(
            keyArrayWriter,
            keyType,
            nullable = false)
          val valueArrayWriter = new UnsafeArrayWriter(writer, getElementSize(valueType))
          val valueWriter = generateFieldWriter(
            valueArrayWriter,
            valueType,
            valueContainsNull)
            (v, i) => {
            v.getMap(i) match {
              case map: UnsafeMapData =>
                writer.write(i, map)
              case map =>
                val previousCursor = writer.cursor()

                // preserve 8 bytes to write the key array numBytes later.
                valueArrayWriter.grow(8)
                valueArrayWriter.increaseCursor(8)

                // Write the keys and write the numBytes of key array into the first 8 bytes.
                writeArray(keyArrayWriter, keyWriter, map.keyArray())
                Platform.putLong(
                  valueArrayWriter.getBuffer,
                  previousCursor,
                  valueArrayWriter.cursor - previousCursor - 8
                )

                // Write the values.
                writeArray(valueArrayWriter, valueWriter, map.valueArray())
                writer.setOffsetAndSizeFromPreviousCursor(i, previousCursor)
            }
          }

        case PhysicalNullType => (_, _) => {}

        case _ =>
          throw new IllegalStateException(s"The data type '${dt.typeName}' is not supported in " +
            "generating a writer function for a struct field, array element, map key or map value.")
      }
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
      case CalendarIntervalType =>
        // We can't call setNullAt() for CalendarIntervalType, we call write directly.
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
      arrayWriter: UnsafeArrayWriter,
      elementWriter: (SpecializedGetters, Int) => Unit,
      array: ArrayData): Unit = array match {
    case unsafe: UnsafeArrayData =>
      arrayWriter.write(unsafe)
    case _ =>
      val numElements = array.numElements()
      arrayWriter.initialize(numElements)
      var i = 0
      while (i < numElements) {
        elementWriter.apply(array, i)
        i += 1
      }
  }
}
