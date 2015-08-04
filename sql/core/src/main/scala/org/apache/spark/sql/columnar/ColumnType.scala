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

package org.apache.spark.sql.columnar

import java.nio.ByteBuffer

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.MutableRow
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * An abstract class that represents type of a column. Used to append/extract Java objects into/from
 * the underlying [[ByteBuffer]] of a column.
 *
 * @tparam JvmType Underlying Java type to represent the elements.
 */
private[sql] sealed abstract class ColumnType[JvmType] {

  // The catalyst data type of this column.
  def dataType: DataType

  // A unique ID representing the type.
  def typeId: Int

  // Default size in bytes for one element of type T (e.g. 4 for `Int`).
  def defaultSize: Int

  /**
   * Extracts a value out of the buffer at the buffer's current position.
   */
  def extract(buffer: ByteBuffer): JvmType

  /**
   * Extracts a value out of the buffer at the buffer's current position and stores in
   * `row(ordinal)`. Subclasses should override this method to avoid boxing/unboxing costs whenever
   * possible.
   */
  def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    setField(row, ordinal, extract(buffer))
  }

  /**
   * Appends the given value v of type T into the given ByteBuffer.
   */
  def append(v: JvmType, buffer: ByteBuffer): Unit

  /**
   * Appends `row(ordinal)` of type T into the given ByteBuffer. Subclasses should override this
   * method to avoid boxing/unboxing costs whenever possible.
   */
  def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    append(getField(row, ordinal), buffer)
  }

  /**
   * Returns the size of the value `row(ordinal)`. This is used to calculate the size of variable
   * length types such as byte arrays and strings.
   */
  def actualSize(row: InternalRow, ordinal: Int): Int = defaultSize

  /**
   * Returns `row(ordinal)`. Subclasses should override this method to avoid boxing/unboxing costs
   * whenever possible.
   */
  def getField(row: InternalRow, ordinal: Int): JvmType

  /**
   * Sets `row(ordinal)` to `field`. Subclasses should override this method to avoid boxing/unboxing
   * costs whenever possible.
   */
  def setField(row: MutableRow, ordinal: Int, value: JvmType): Unit

  /**
   * Copies `from(fromOrdinal)` to `to(toOrdinal)`. Subclasses should override this method to avoid
   * boxing/unboxing costs whenever possible.
   */
  def copyField(from: InternalRow, fromOrdinal: Int, to: MutableRow, toOrdinal: Int): Unit = {
    to.update(toOrdinal, from.get(fromOrdinal, dataType))
  }

  /**
   * Creates a duplicated copy of the value.
   */
  def clone(v: JvmType): JvmType = v

  override def toString: String = getClass.getSimpleName.stripSuffix("$")
}

private[sql] abstract class NativeColumnType[T <: AtomicType](
    val dataType: T,
    val typeId: Int,
    val defaultSize: Int)
  extends ColumnType[T#InternalType] {

  /**
   * Scala TypeTag. Can be used to create primitive arrays and hash tables.
   */
  def scalaTag: TypeTag[dataType.InternalType] = dataType.tag
}

private[sql] object INT extends NativeColumnType(IntegerType, 0, 4) {
  override def append(v: Int, buffer: ByteBuffer): Unit = {
    buffer.putInt(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putInt(row.getInt(ordinal))
  }

  override def extract(buffer: ByteBuffer): Int = {
    buffer.getInt()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setInt(ordinal, buffer.getInt())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Int): Unit = {
    row.setInt(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Int = row.getInt(ordinal)

  override def copyField(from: InternalRow, fromOrdinal: Int, to: MutableRow, toOrdinal: Int) {
    to.setInt(toOrdinal, from.getInt(fromOrdinal))
  }
}

private[sql] object LONG extends NativeColumnType(LongType, 1, 8) {
  override def append(v: Long, buffer: ByteBuffer): Unit = {
    buffer.putLong(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putLong(row.getLong(ordinal))
  }

  override def extract(buffer: ByteBuffer): Long = {
    buffer.getLong()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setLong(ordinal, buffer.getLong())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Long): Unit = {
    row.setLong(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Long = row.getLong(ordinal)

  override def copyField(from: InternalRow, fromOrdinal: Int, to: MutableRow, toOrdinal: Int) {
    to.setLong(toOrdinal, from.getLong(fromOrdinal))
  }
}

private[sql] object FLOAT extends NativeColumnType(FloatType, 2, 4) {
  override def append(v: Float, buffer: ByteBuffer): Unit = {
    buffer.putFloat(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putFloat(row.getFloat(ordinal))
  }

  override def extract(buffer: ByteBuffer): Float = {
    buffer.getFloat()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setFloat(ordinal, buffer.getFloat())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Float): Unit = {
    row.setFloat(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Float = row.getFloat(ordinal)

  override def copyField(from: InternalRow, fromOrdinal: Int, to: MutableRow, toOrdinal: Int) {
    to.setFloat(toOrdinal, from.getFloat(fromOrdinal))
  }
}

private[sql] object DOUBLE extends NativeColumnType(DoubleType, 3, 8) {
  override def append(v: Double, buffer: ByteBuffer): Unit = {
    buffer.putDouble(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putDouble(row.getDouble(ordinal))
  }

  override def extract(buffer: ByteBuffer): Double = {
    buffer.getDouble()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setDouble(ordinal, buffer.getDouble())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Double): Unit = {
    row.setDouble(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Double = row.getDouble(ordinal)

  override def copyField(from: InternalRow, fromOrdinal: Int, to: MutableRow, toOrdinal: Int) {
    to.setDouble(toOrdinal, from.getDouble(fromOrdinal))
  }
}

private[sql] object BOOLEAN extends NativeColumnType(BooleanType, 4, 1) {
  override def append(v: Boolean, buffer: ByteBuffer): Unit = {
    buffer.put(if (v) 1: Byte else 0: Byte)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.put(if (row.getBoolean(ordinal)) 1: Byte else 0: Byte)
  }

  override def extract(buffer: ByteBuffer): Boolean = buffer.get() == 1

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setBoolean(ordinal, buffer.get() == 1)
  }

  override def setField(row: MutableRow, ordinal: Int, value: Boolean): Unit = {
    row.setBoolean(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Boolean = row.getBoolean(ordinal)

  override def copyField(from: InternalRow, fromOrdinal: Int, to: MutableRow, toOrdinal: Int) {
    to.setBoolean(toOrdinal, from.getBoolean(fromOrdinal))
  }
}

private[sql] object BYTE extends NativeColumnType(ByteType, 5, 1) {
  override def append(v: Byte, buffer: ByteBuffer): Unit = {
    buffer.put(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.put(row.getByte(ordinal))
  }

  override def extract(buffer: ByteBuffer): Byte = {
    buffer.get()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setByte(ordinal, buffer.get())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Byte): Unit = {
    row.setByte(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Byte = row.getByte(ordinal)

  override def copyField(from: InternalRow, fromOrdinal: Int, to: MutableRow, toOrdinal: Int) {
    to.setByte(toOrdinal, from.getByte(fromOrdinal))
  }
}

private[sql] object SHORT extends NativeColumnType(ShortType, 6, 2) {
  override def append(v: Short, buffer: ByteBuffer): Unit = {
    buffer.putShort(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putShort(row.getShort(ordinal))
  }

  override def extract(buffer: ByteBuffer): Short = {
    buffer.getShort()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setShort(ordinal, buffer.getShort())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Short): Unit = {
    row.setShort(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Short = row.getShort(ordinal)

  override def copyField(from: InternalRow, fromOrdinal: Int, to: MutableRow, toOrdinal: Int) {
    to.setShort(toOrdinal, from.getShort(fromOrdinal))
  }
}

private[sql] object STRING extends NativeColumnType(StringType, 7, 8) {
  override def actualSize(row: InternalRow, ordinal: Int): Int = {
    row.getUTF8String(ordinal).numBytes() + 4
  }

  override def append(v: UTF8String, buffer: ByteBuffer): Unit = {
    val stringBytes = v.getBytes
    buffer.putInt(stringBytes.length).put(stringBytes, 0, stringBytes.length)
  }

  override def extract(buffer: ByteBuffer): UTF8String = {
    val length = buffer.getInt()
    val stringBytes = new Array[Byte](length)
    buffer.get(stringBytes, 0, length)
    UTF8String.fromBytes(stringBytes)
  }

  override def setField(row: MutableRow, ordinal: Int, value: UTF8String): Unit = {
    row.update(ordinal, value.clone())
  }

  override def getField(row: InternalRow, ordinal: Int): UTF8String = {
    row.getUTF8String(ordinal)
  }

  override def copyField(from: InternalRow, fromOrdinal: Int, to: MutableRow, toOrdinal: Int) {
    setField(to, toOrdinal, getField(from, fromOrdinal))
  }
}

private[sql] object DATE extends NativeColumnType(DateType, 8, 4) {
  override def extract(buffer: ByteBuffer): Int = {
    buffer.getInt
  }

  override def append(v: Int, buffer: ByteBuffer): Unit = {
    buffer.putInt(v)
  }

  override def getField(row: InternalRow, ordinal: Int): Int = {
    row.getInt(ordinal)
  }

  def setField(row: MutableRow, ordinal: Int, value: Int): Unit = {
    row(ordinal) = value
  }
}

private[sql] object TIMESTAMP extends NativeColumnType(TimestampType, 9, 8) {
  override def extract(buffer: ByteBuffer): Long = {
    buffer.getLong
  }

  override def append(v: Long, buffer: ByteBuffer): Unit = {
    buffer.putLong(v)
  }

  override def getField(row: InternalRow, ordinal: Int): Long = {
    row.getLong(ordinal)
  }

  override def setField(row: MutableRow, ordinal: Int, value: Long): Unit = {
    row(ordinal) = value
  }
}

private[sql] case class FIXED_DECIMAL(precision: Int, scale: Int)
  extends NativeColumnType(
    DecimalType(precision, scale),
    10,
    FIXED_DECIMAL.defaultSize) {

  override def extract(buffer: ByteBuffer): Decimal = {
    Decimal(buffer.getLong(), precision, scale)
  }

  override def append(v: Decimal, buffer: ByteBuffer): Unit = {
    buffer.putLong(v.toUnscaledLong)
  }

  override def getField(row: InternalRow, ordinal: Int): Decimal = {
    row.getDecimal(ordinal, precision, scale)
  }

  override def setField(row: MutableRow, ordinal: Int, value: Decimal): Unit = {
    row.setDecimal(ordinal, value, precision)
  }

  override def copyField(from: InternalRow, fromOrdinal: Int, to: MutableRow, toOrdinal: Int) {
    setField(to, toOrdinal, getField(from, fromOrdinal))
  }
}

private[sql] object FIXED_DECIMAL {
  val defaultSize = 8
}

private[sql] sealed abstract class ByteArrayColumnType(
    val typeId: Int,
    val defaultSize: Int)
  extends ColumnType[Array[Byte]] {

  override def actualSize(row: InternalRow, ordinal: Int): Int = {
    getField(row, ordinal).length + 4
  }

  override def append(v: Array[Byte], buffer: ByteBuffer): Unit = {
    buffer.putInt(v.length).put(v, 0, v.length)
  }

  override def extract(buffer: ByteBuffer): Array[Byte] = {
    val length = buffer.getInt()
    val bytes = new Array[Byte](length)
    buffer.get(bytes, 0, length)
    bytes
  }
}

private[sql] object BINARY extends ByteArrayColumnType(11, 16) {

  def dataType: DataType = BooleanType

  override def setField(row: MutableRow, ordinal: Int, value: Array[Byte]): Unit = {
    row.update(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Array[Byte] = {
    row.getBinary(ordinal)
  }
}

// Used to process generic objects (all types other than those listed above). Objects should be
// serialized first before appending to the column `ByteBuffer`, and is also extracted as serialized
// byte array.
private[sql] case class GENERIC(dataType: DataType) extends ByteArrayColumnType(12, 16) {
  override def setField(row: MutableRow, ordinal: Int, value: Array[Byte]): Unit = {
    row.update(ordinal, SparkSqlSerializer.deserialize[Any](value))
  }

  override def getField(row: InternalRow, ordinal: Int): Array[Byte] = {
    SparkSqlSerializer.serialize(row.get(ordinal, dataType))
  }
}

private[sql] object ColumnType {
  def apply(dataType: DataType): ColumnType[_] = {
    dataType match {
      case BooleanType => BOOLEAN
      case ByteType => BYTE
      case ShortType => SHORT
      case IntegerType => INT
      case DateType => DATE
      case LongType => LONG
      case TimestampType => TIMESTAMP
      case FloatType => FLOAT
      case DoubleType => DOUBLE
      case StringType => STRING
      case BinaryType => BINARY
      case DecimalType.Fixed(precision, scale) if precision < 19 =>
        FIXED_DECIMAL(precision, scale)
      case other => GENERIC(other)
    }
  }
}
