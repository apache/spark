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

package org.apache.spark.sql.execution.columnar

import java.math.{BigDecimal, BigInteger}
import java.nio.ByteBuffer

import scala.annotation.tailrec
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}


/**
 * A help class for fast reading Int/Long/Float/Double from ByteBuffer in native order.
 *
 * Note: There is not much difference between ByteBuffer.getByte/getShort and
 * Unsafe.getByte/getShort, so we do not have helper methods for them.
 *
 * The unrolling (building columnar cache) is already slow, putLong/putDouble will not help much,
 * so we do not have helper methods for them.
 *
 *
 * WARNING: This only works with HeapByteBuffer
 */
private[columnar] object ByteBufferHelper {
  def getShort(buffer: ByteBuffer): Short = {
    val pos = buffer.position()
    buffer.position(pos + 2)
    Platform.getShort(buffer.array(), Platform.BYTE_ARRAY_OFFSET + pos)
  }

  def getInt(buffer: ByteBuffer): Int = {
    val pos = buffer.position()
    buffer.position(pos + 4)
    Platform.getInt(buffer.array(), Platform.BYTE_ARRAY_OFFSET + pos)
  }

  def getLong(buffer: ByteBuffer): Long = {
    val pos = buffer.position()
    buffer.position(pos + 8)
    Platform.getLong(buffer.array(), Platform.BYTE_ARRAY_OFFSET + pos)
  }

  def getFloat(buffer: ByteBuffer): Float = {
    val pos = buffer.position()
    buffer.position(pos + 4)
    Platform.getFloat(buffer.array(), Platform.BYTE_ARRAY_OFFSET + pos)
  }

  def getDouble(buffer: ByteBuffer): Double = {
    val pos = buffer.position()
    buffer.position(pos + 8)
    Platform.getDouble(buffer.array(), Platform.BYTE_ARRAY_OFFSET + pos)
  }

  def putShort(buffer: ByteBuffer, value: Short): Unit = {
    val pos = buffer.position()
    buffer.position(pos + 2)
    Platform.putShort(buffer.array(), Platform.BYTE_ARRAY_OFFSET + pos, value)
  }

  def putInt(buffer: ByteBuffer, value: Int): Unit = {
    val pos = buffer.position()
    buffer.position(pos + 4)
    Platform.putInt(buffer.array(), Platform.BYTE_ARRAY_OFFSET + pos, value)
  }

  def putLong(buffer: ByteBuffer, value: Long): Unit = {
    val pos = buffer.position()
    buffer.position(pos + 8)
    Platform.putLong(buffer.array(), Platform.BYTE_ARRAY_OFFSET + pos, value)
  }

  def copyMemory(src: ByteBuffer, dst: ByteBuffer, len: Int): Unit = {
    val srcPos = src.position()
    val dstPos = dst.position()
    src.position(srcPos + len)
    dst.position(dstPos + len)
    Platform.copyMemory(src.array(), Platform.BYTE_ARRAY_OFFSET + srcPos,
      dst.array(), Platform.BYTE_ARRAY_OFFSET + dstPos, len)
  }
}

/**
 * An abstract class that represents type of a column. Used to append/extract Java objects into/from
 * the underlying [[ByteBuffer]] of a column.
 *
 * @tparam JvmType Underlying Java type to represent the elements.
 */
private[columnar] sealed abstract class ColumnType[JvmType] {

  // The catalyst data type of this column.
  def dataType: DataType

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
  def extract(buffer: ByteBuffer, row: InternalRow, ordinal: Int): Unit = {
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
  def setField(row: InternalRow, ordinal: Int, value: JvmType): Unit

  /**
   * Copies `from(fromOrdinal)` to `to(toOrdinal)`. Subclasses should override this method to avoid
   * boxing/unboxing costs whenever possible.
   */
  def copyField(from: InternalRow, fromOrdinal: Int, to: InternalRow, toOrdinal: Int): Unit = {
    setField(to, toOrdinal, getField(from, fromOrdinal))
  }

  /**
   * Creates a duplicated copy of the value.
   */
  def clone(v: JvmType): JvmType = v

  override def toString: String = getClass.getSimpleName.stripSuffix("$")
}

private[columnar] object NULL extends ColumnType[Any] {

  override def dataType: DataType = NullType
  override def defaultSize: Int = 0
  override def append(v: Any, buffer: ByteBuffer): Unit = {}
  override def extract(buffer: ByteBuffer): Any = null
  override def setField(row: InternalRow, ordinal: Int, value: Any): Unit = row.setNullAt(ordinal)
  override def getField(row: InternalRow, ordinal: Int): Any = null
}

private[columnar] abstract class NativeColumnType[T <: AtomicType](
    val dataType: T,
    val defaultSize: Int)
  extends ColumnType[T#InternalType] {

  /**
   * Scala TypeTag. Can be used to create primitive arrays and hash tables.
   */
  def scalaTag: TypeTag[dataType.InternalType] = dataType.tag
}

private[columnar] object INT extends NativeColumnType(IntegerType, 4) {
  override def append(v: Int, buffer: ByteBuffer): Unit = {
    buffer.putInt(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putInt(row.getInt(ordinal))
  }

  override def extract(buffer: ByteBuffer): Int = {
    ByteBufferHelper.getInt(buffer)
  }

  override def extract(buffer: ByteBuffer, row: InternalRow, ordinal: Int): Unit = {
    row.setInt(ordinal, ByteBufferHelper.getInt(buffer))
  }

  override def setField(row: InternalRow, ordinal: Int, value: Int): Unit = {
    row.setInt(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Int = row.getInt(ordinal)


  override def copyField(from: InternalRow, fromOrdinal: Int,
      to: InternalRow, toOrdinal: Int): Unit = {
    to.setInt(toOrdinal, from.getInt(fromOrdinal))
  }
}

private[columnar] object LONG extends NativeColumnType(LongType, 8) {
  override def append(v: Long, buffer: ByteBuffer): Unit = {
    buffer.putLong(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putLong(row.getLong(ordinal))
  }

  override def extract(buffer: ByteBuffer): Long = {
    ByteBufferHelper.getLong(buffer)
  }

  override def extract(buffer: ByteBuffer, row: InternalRow, ordinal: Int): Unit = {
    row.setLong(ordinal, ByteBufferHelper.getLong(buffer))
  }

  override def setField(row: InternalRow, ordinal: Int, value: Long): Unit = {
    row.setLong(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Long = row.getLong(ordinal)

  override def copyField(from: InternalRow, fromOrdinal: Int,
      to: InternalRow, toOrdinal: Int): Unit = {
    to.setLong(toOrdinal, from.getLong(fromOrdinal))
  }
}

private[columnar] object YEAR_MONTH_INTERVAL extends NativeColumnType(YearMonthIntervalType(), 4) {
  override def append(v: Int, buffer: ByteBuffer): Unit = {
    buffer.putInt(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putInt(row.getInt(ordinal))
  }

  override def extract(buffer: ByteBuffer): Int = {
    ByteBufferHelper.getInt(buffer)
  }

  override def extract(buffer: ByteBuffer, row: InternalRow, ordinal: Int): Unit = {
    row.setInt(ordinal, ByteBufferHelper.getInt(buffer))
  }

  override def setField(row: InternalRow, ordinal: Int, value: Int): Unit = {
    row.setInt(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Int = row.getInt(ordinal)


  override def copyField(
      from: InternalRow,
      fromOrdinal: Int,
      to: InternalRow,
      toOrdinal: Int): Unit = {
    to.setInt(toOrdinal, from.getInt(fromOrdinal))
  }
}

private[columnar] object DAY_TIME_INTERVAL extends NativeColumnType(DayTimeIntervalType(), 8) {
  override def append(v: Long, buffer: ByteBuffer): Unit = {
    buffer.putLong(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putLong(row.getLong(ordinal))
  }

  override def extract(buffer: ByteBuffer): Long = {
    ByteBufferHelper.getLong(buffer)
  }

  override def extract(buffer: ByteBuffer, row: InternalRow, ordinal: Int): Unit = {
    row.setLong(ordinal, ByteBufferHelper.getLong(buffer))
  }

  override def setField(row: InternalRow, ordinal: Int, value: Long): Unit = {
    row.setLong(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Long = row.getLong(ordinal)

  override def copyField(
      from: InternalRow,
      fromOrdinal: Int,
      to: InternalRow,
      toOrdinal: Int): Unit = {
    to.setLong(toOrdinal, from.getLong(fromOrdinal))
  }
}

private[columnar] object FLOAT extends NativeColumnType(FloatType, 4) {
  override def append(v: Float, buffer: ByteBuffer): Unit = {
    buffer.putFloat(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putFloat(row.getFloat(ordinal))
  }

  override def extract(buffer: ByteBuffer): Float = {
    ByteBufferHelper.getFloat(buffer)
  }

  override def extract(buffer: ByteBuffer, row: InternalRow, ordinal: Int): Unit = {
    row.setFloat(ordinal, ByteBufferHelper.getFloat(buffer))
  }

  override def setField(row: InternalRow, ordinal: Int, value: Float): Unit = {
    row.setFloat(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Float = row.getFloat(ordinal)

  override def copyField(from: InternalRow, fromOrdinal: Int,
      to: InternalRow, toOrdinal: Int): Unit = {
    to.setFloat(toOrdinal, from.getFloat(fromOrdinal))
  }
}

private[columnar] object DOUBLE extends NativeColumnType(DoubleType, 8) {
  override def append(v: Double, buffer: ByteBuffer): Unit = {
    buffer.putDouble(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putDouble(row.getDouble(ordinal))
  }

  override def extract(buffer: ByteBuffer): Double = {
    ByteBufferHelper.getDouble(buffer)
  }

  override def extract(buffer: ByteBuffer, row: InternalRow, ordinal: Int): Unit = {
    row.setDouble(ordinal, ByteBufferHelper.getDouble(buffer))
  }

  override def setField(row: InternalRow, ordinal: Int, value: Double): Unit = {
    row.setDouble(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Double = row.getDouble(ordinal)

  override def copyField(from: InternalRow, fromOrdinal: Int,
      to: InternalRow, toOrdinal: Int): Unit = {
    to.setDouble(toOrdinal, from.getDouble(fromOrdinal))
  }
}

private[columnar] object BOOLEAN extends NativeColumnType(BooleanType, 1) {
  override def append(v: Boolean, buffer: ByteBuffer): Unit = {
    buffer.put(if (v) 1: Byte else 0: Byte)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.put(if (row.getBoolean(ordinal)) 1: Byte else 0: Byte)
  }

  override def extract(buffer: ByteBuffer): Boolean = buffer.get() == 1

  override def extract(buffer: ByteBuffer, row: InternalRow, ordinal: Int): Unit = {
    row.setBoolean(ordinal, buffer.get() == 1)
  }

  override def setField(row: InternalRow, ordinal: Int, value: Boolean): Unit = {
    row.setBoolean(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Boolean = row.getBoolean(ordinal)

  override def copyField(from: InternalRow, fromOrdinal: Int,
      to: InternalRow, toOrdinal: Int): Unit = {
    to.setBoolean(toOrdinal, from.getBoolean(fromOrdinal))
  }
}

private[columnar] object BYTE extends NativeColumnType(ByteType, 1) {
  override def append(v: Byte, buffer: ByteBuffer): Unit = {
    buffer.put(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.put(row.getByte(ordinal))
  }

  override def extract(buffer: ByteBuffer): Byte = {
    buffer.get()
  }

  override def extract(buffer: ByteBuffer, row: InternalRow, ordinal: Int): Unit = {
    row.setByte(ordinal, buffer.get())
  }

  override def setField(row: InternalRow, ordinal: Int, value: Byte): Unit = {
    row.setByte(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Byte = row.getByte(ordinal)

  override def copyField(from: InternalRow, fromOrdinal: Int,
      to: InternalRow, toOrdinal: Int): Unit = {
    to.setByte(toOrdinal, from.getByte(fromOrdinal))
  }
}

private[columnar] object SHORT extends NativeColumnType(ShortType, 2) {
  override def append(v: Short, buffer: ByteBuffer): Unit = {
    buffer.putShort(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putShort(row.getShort(ordinal))
  }

  override def extract(buffer: ByteBuffer): Short = {
    buffer.getShort()
  }

  override def extract(buffer: ByteBuffer, row: InternalRow, ordinal: Int): Unit = {
    row.setShort(ordinal, buffer.getShort())
  }

  override def setField(row: InternalRow, ordinal: Int, value: Short): Unit = {
    row.setShort(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Short = row.getShort(ordinal)

  override def copyField(from: InternalRow, fromOrdinal: Int,
      to: InternalRow, toOrdinal: Int): Unit = {
    to.setShort(toOrdinal, from.getShort(fromOrdinal))
  }
}

/**
 * A fast path to copy var-length bytes between ByteBuffer and UnsafeRow without creating wrapper
 * objects.
 */
private[columnar] trait DirectCopyColumnType[JvmType] extends ColumnType[JvmType] {

  // copy the bytes from ByteBuffer to UnsafeRow
  override def extract(buffer: ByteBuffer, row: InternalRow, ordinal: Int): Unit = {
    if (row.isInstanceOf[MutableUnsafeRow]) {
      val numBytes = buffer.getInt
      val cursor = buffer.position()
      buffer.position(cursor + numBytes)
      row.asInstanceOf[MutableUnsafeRow].writer.write(ordinal, buffer.array(),
        buffer.arrayOffset() + cursor, numBytes)
    } else {
      setField(row, ordinal, extract(buffer))
    }
  }

  // copy the bytes from UnsafeRow to ByteBuffer
  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    if (row.isInstanceOf[UnsafeRow]) {
      row.asInstanceOf[UnsafeRow].writeFieldTo(ordinal, buffer)
    } else {
      super.append(row, ordinal, buffer)
    }
  }
}

private[columnar] object STRING
  extends NativeColumnType(StringType, 8) with DirectCopyColumnType[UTF8String] {

  override def actualSize(row: InternalRow, ordinal: Int): Int = {
    row.getUTF8String(ordinal).numBytes() + 4
  }

  override def append(v: UTF8String, buffer: ByteBuffer): Unit = {
    buffer.putInt(v.numBytes())
    v.writeTo(buffer)
  }

  override def extract(buffer: ByteBuffer): UTF8String = {
    val length = buffer.getInt()
    val cursor = buffer.position()
    buffer.position(cursor + length)
    UTF8String.fromBytes(buffer.array(), buffer.arrayOffset() + cursor, length)
  }

  override def setField(row: InternalRow, ordinal: Int, value: UTF8String): Unit = {
    if (row.isInstanceOf[MutableUnsafeRow]) {
      row.asInstanceOf[MutableUnsafeRow].writer.write(ordinal, value)
    } else {
      row.update(ordinal, value.clone())
    }
  }

  override def getField(row: InternalRow, ordinal: Int): UTF8String = {
    row.getUTF8String(ordinal)
  }

  override def copyField(from: InternalRow, fromOrdinal: Int,
      to: InternalRow, toOrdinal: Int): Unit = {
    setField(to, toOrdinal, getField(from, fromOrdinal))
  }

  override def clone(v: UTF8String): UTF8String = v.clone()
}

private[columnar] case class COMPACT_DECIMAL(precision: Int, scale: Int)
  extends NativeColumnType(DecimalType(precision, scale), 8) {

  override def extract(buffer: ByteBuffer): Decimal = {
    Decimal(ByteBufferHelper.getLong(buffer), precision, scale)
  }

  override def extract(buffer: ByteBuffer, row: InternalRow, ordinal: Int): Unit = {
    if (row.isInstanceOf[MutableUnsafeRow]) {
      // copy it as Long
      row.setLong(ordinal, ByteBufferHelper.getLong(buffer))
    } else {
      setField(row, ordinal, extract(buffer))
    }
  }

  override def append(v: Decimal, buffer: ByteBuffer): Unit = {
    buffer.putLong(v.toUnscaledLong)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    if (row.isInstanceOf[UnsafeRow]) {
      // copy it as Long
      buffer.putLong(row.getLong(ordinal))
    } else {
      append(getField(row, ordinal), buffer)
    }
  }

  override def getField(row: InternalRow, ordinal: Int): Decimal = {
    row.getDecimal(ordinal, precision, scale)
  }

  override def setField(row: InternalRow, ordinal: Int, value: Decimal): Unit = {
    row.setDecimal(ordinal, value, precision)
  }

  override def copyField(from: InternalRow, fromOrdinal: Int,
      to: InternalRow, toOrdinal: Int): Unit = {
    setField(to, toOrdinal, getField(from, fromOrdinal))
  }
}

private[columnar] object COMPACT_DECIMAL {
  def apply(dt: DecimalType): COMPACT_DECIMAL = {
    COMPACT_DECIMAL(dt.precision, dt.scale)
  }
}

private[columnar] sealed abstract class ByteArrayColumnType[JvmType](val defaultSize: Int)
  extends ColumnType[JvmType] with DirectCopyColumnType[JvmType] {

  def serialize(value: JvmType): Array[Byte]
  def deserialize(bytes: Array[Byte]): JvmType

  override def append(v: JvmType, buffer: ByteBuffer): Unit = {
    val bytes = serialize(v)
    buffer.putInt(bytes.length).put(bytes, 0, bytes.length)
  }

  override def extract(buffer: ByteBuffer): JvmType = {
    val length = buffer.getInt()
    val bytes = new Array[Byte](length)
    buffer.get(bytes, 0, length)
    deserialize(bytes)
  }
}

private[columnar] object BINARY extends ByteArrayColumnType[Array[Byte]](16) {

  def dataType: DataType = BinaryType

  override def setField(row: InternalRow, ordinal: Int, value: Array[Byte]): Unit = {
    row.update(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Array[Byte] = {
    row.getBinary(ordinal)
  }

  override def actualSize(row: InternalRow, ordinal: Int): Int = {
    row.getBinary(ordinal).length + 4
  }

  def serialize(value: Array[Byte]): Array[Byte] = value
  def deserialize(bytes: Array[Byte]): Array[Byte] = bytes
}

private[columnar] case class LARGE_DECIMAL(precision: Int, scale: Int)
  extends ByteArrayColumnType[Decimal](12) {

  override val dataType: DataType = DecimalType(precision, scale)

  override def getField(row: InternalRow, ordinal: Int): Decimal = {
    row.getDecimal(ordinal, precision, scale)
  }

  override def setField(row: InternalRow, ordinal: Int, value: Decimal): Unit = {
    row.setDecimal(ordinal, value, precision)
  }

  override def actualSize(row: InternalRow, ordinal: Int): Int = {
    4 + getField(row, ordinal).toJavaBigDecimal.unscaledValue().bitLength() / 8 + 1
  }

  override def serialize(value: Decimal): Array[Byte] = {
    value.toJavaBigDecimal.unscaledValue().toByteArray
  }

  override def deserialize(bytes: Array[Byte]): Decimal = {
    val javaDecimal = new BigDecimal(new BigInteger(bytes), scale)
    Decimal.apply(javaDecimal, precision, scale)
  }
}

private[columnar] object LARGE_DECIMAL {
  def apply(dt: DecimalType): LARGE_DECIMAL = {
    LARGE_DECIMAL(dt.precision, dt.scale)
  }
}

private[columnar] case class STRUCT(dataType: StructType)
  extends ColumnType[UnsafeRow] with DirectCopyColumnType[UnsafeRow] {

  private val numOfFields: Int = dataType.fields.length

  override def defaultSize: Int = 20

  override def setField(row: InternalRow, ordinal: Int, value: UnsafeRow): Unit = {
    row.update(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): UnsafeRow = {
    row.getStruct(ordinal, numOfFields).asInstanceOf[UnsafeRow]
  }

  override def actualSize(row: InternalRow, ordinal: Int): Int = {
    4 + getField(row, ordinal).getSizeInBytes
  }

  override def append(value: UnsafeRow, buffer: ByteBuffer): Unit = {
    buffer.putInt(value.getSizeInBytes)
    value.writeTo(buffer)
  }

  override def extract(buffer: ByteBuffer): UnsafeRow = {
    val sizeInBytes = ByteBufferHelper.getInt(buffer)
    assert(buffer.hasArray)
    val cursor = buffer.position()
    buffer.position(cursor + sizeInBytes)
    val unsafeRow = new UnsafeRow(numOfFields)
    unsafeRow.pointTo(
      buffer.array(),
      Platform.BYTE_ARRAY_OFFSET + buffer.arrayOffset() + cursor,
      sizeInBytes)
    unsafeRow
  }

  override def clone(v: UnsafeRow): UnsafeRow = v.copy()
}

private[columnar] case class ARRAY(dataType: ArrayType)
  extends ColumnType[UnsafeArrayData] with DirectCopyColumnType[UnsafeArrayData] {

  override def defaultSize: Int = 28

  override def setField(row: InternalRow, ordinal: Int, value: UnsafeArrayData): Unit = {
    row.update(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): UnsafeArrayData = {
    row.getArray(ordinal).asInstanceOf[UnsafeArrayData]
  }

  override def actualSize(row: InternalRow, ordinal: Int): Int = {
    val unsafeArray = getField(row, ordinal)
    4 + unsafeArray.getSizeInBytes
  }

  override def append(value: UnsafeArrayData, buffer: ByteBuffer): Unit = {
    buffer.putInt(value.getSizeInBytes)
    value.writeTo(buffer)
  }

  override def extract(buffer: ByteBuffer): UnsafeArrayData = {
    val numBytes = buffer.getInt
    assert(buffer.hasArray)
    val cursor = buffer.position()
    buffer.position(cursor + numBytes)
    val array = new UnsafeArrayData
    array.pointTo(
      buffer.array(),
      Platform.BYTE_ARRAY_OFFSET + buffer.arrayOffset() + cursor,
      numBytes)
    array
  }

  override def clone(v: UnsafeArrayData): UnsafeArrayData = v.copy()
}

private[columnar] case class MAP(dataType: MapType)
  extends ColumnType[UnsafeMapData] with DirectCopyColumnType[UnsafeMapData] {

  override def defaultSize: Int = 68

  override def setField(row: InternalRow, ordinal: Int, value: UnsafeMapData): Unit = {
    row.update(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): UnsafeMapData = {
    row.getMap(ordinal).asInstanceOf[UnsafeMapData]
  }

  override def actualSize(row: InternalRow, ordinal: Int): Int = {
    val unsafeMap = getField(row, ordinal)
    4 + unsafeMap.getSizeInBytes
  }

  override def append(value: UnsafeMapData, buffer: ByteBuffer): Unit = {
    buffer.putInt(value.getSizeInBytes)
    value.writeTo(buffer)
  }

  override def extract(buffer: ByteBuffer): UnsafeMapData = {
    val numBytes = buffer.getInt
    val cursor = buffer.position()
    buffer.position(cursor + numBytes)
    val map = new UnsafeMapData
    map.pointTo(
      buffer.array(),
      Platform.BYTE_ARRAY_OFFSET + buffer.arrayOffset() + cursor,
      numBytes)
    map
  }

  override def clone(v: UnsafeMapData): UnsafeMapData = v.copy()
}

private[columnar] object CALENDAR_INTERVAL extends ColumnType[CalendarInterval] {

  override def dataType: DataType = CalendarIntervalType

  override def defaultSize: Int = 16

  override def getField(row: InternalRow, ordinal: Int): CalendarInterval = row.getInterval(ordinal)

  override def setField(row: InternalRow, ordinal: Int, value: CalendarInterval): Unit = {
    row.setInterval(ordinal, value)
  }

  override def extract(buffer: ByteBuffer): CalendarInterval = {
    val months = ByteBufferHelper.getInt(buffer)
    val days = ByteBufferHelper.getInt(buffer)
    val microseconds = ByteBufferHelper.getLong(buffer)
    new CalendarInterval(months, days, microseconds)
  }

  // copy the bytes from ByteBuffer to UnsafeRow
  override def extract(buffer: ByteBuffer, row: InternalRow, ordinal: Int): Unit = {
    if (row.isInstanceOf[MutableUnsafeRow]) {
      val cursor = buffer.position()
      buffer.position(cursor + defaultSize)
      row.asInstanceOf[MutableUnsafeRow].writer.write(ordinal, buffer.array(),
        buffer.arrayOffset() + cursor, defaultSize)
    } else {
      setField(row, ordinal, extract(buffer))
    }
  }

  override def append(v: CalendarInterval, buffer: ByteBuffer): Unit = {
    ByteBufferHelper.putInt(buffer, v.months)
    ByteBufferHelper.putInt(buffer, v.days)
    ByteBufferHelper.putLong(buffer, v.microseconds)
  }
}

private[columnar] object ColumnType {
  @tailrec
  def apply(dataType: DataType): ColumnType[_] = {
    dataType match {
      case NullType => NULL
      case BooleanType => BOOLEAN
      case ByteType => BYTE
      case ShortType => SHORT
      case IntegerType | DateType | _: YearMonthIntervalType => INT
      case LongType | TimestampType | TimestampNTZType | _: DayTimeIntervalType => LONG
      case FloatType => FLOAT
      case DoubleType => DOUBLE
      case StringType => STRING
      case BinaryType => BINARY
      case i: CalendarIntervalType => CALENDAR_INTERVAL
      case dt: DecimalType if dt.precision <= Decimal.MAX_LONG_DIGITS => COMPACT_DECIMAL(dt)
      case dt: DecimalType => LARGE_DECIMAL(dt)
      case arr: ArrayType => ARRAY(arr)
      case map: MapType => MAP(map)
      case struct: StructType => STRUCT(struct)
      case udt: UserDefinedType[_] => apply(udt.sqlType)
      case other => throw QueryExecutionErrors.unsupportedTypeError(other)
    }
  }
}
