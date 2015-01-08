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
import java.sql.{Date, Timestamp}

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.MutableRow
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.sql.types._

/**
 * An abstract class that represents type of a column. Used to append/extract Java objects into/from
 * the underlying [[ByteBuffer]] of a column.
 *
 * @param typeId A unique ID representing the type.
 * @param defaultSize Default size in bytes for one element of type T (e.g. 4 for `Int`).
 * @tparam T Scala data type for the column.
 * @tparam JvmType Underlying Java type to represent the elements.
 */
private[sql] sealed abstract class ColumnType[T <: DataType, JvmType](
    val typeId: Int,
    val defaultSize: Int) {

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
  def append(row: Row, ordinal: Int, buffer: ByteBuffer): Unit = {
    append(getField(row, ordinal), buffer)
  }

  /**
   * Returns the size of the value `row(ordinal)`. This is used to calculate the size of variable
   * length types such as byte arrays and strings.
   */
  def actualSize(row: Row, ordinal: Int): Int = defaultSize

  /**
   * Returns `row(ordinal)`. Subclasses should override this method to avoid boxing/unboxing costs
   * whenever possible.
   */
  def getField(row: Row, ordinal: Int): JvmType

  /**
   * Sets `row(ordinal)` to `field`. Subclasses should override this method to avoid boxing/unboxing
   * costs whenever possible.
   */
  def setField(row: MutableRow, ordinal: Int, value: JvmType): Unit

  /**
   * Copies `from(fromOrdinal)` to `to(toOrdinal)`. Subclasses should override this method to avoid
   * boxing/unboxing costs whenever possible.
   */
  def copyField(from: Row, fromOrdinal: Int, to: MutableRow, toOrdinal: Int): Unit = {
    to(toOrdinal) = from(fromOrdinal)
  }

  /**
   * Creates a duplicated copy of the value.
   */
  def clone(v: JvmType): JvmType = v

  override def toString = getClass.getSimpleName.stripSuffix("$")
}

private[sql] abstract class NativeColumnType[T <: NativeType](
    val dataType: T,
    typeId: Int,
    defaultSize: Int)
  extends ColumnType[T, T#JvmType](typeId, defaultSize) {

  /**
   * Scala TypeTag. Can be used to create primitive arrays and hash tables.
   */
  def scalaTag: TypeTag[dataType.JvmType] = dataType.tag
}

private[sql] object INT extends NativeColumnType(IntegerType, 0, 4) {
  def append(v: Int, buffer: ByteBuffer): Unit = {
    buffer.putInt(v)
  }

  override def append(row: Row, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putInt(row.getInt(ordinal))
  }

  def extract(buffer: ByteBuffer) = {
    buffer.getInt()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setInt(ordinal, buffer.getInt())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Int): Unit = {
    row.setInt(ordinal, value)
  }

  override def getField(row: Row, ordinal: Int) = row.getInt(ordinal)

  override def copyField(from: Row, fromOrdinal: Int, to: MutableRow, toOrdinal: Int): Unit = {
    to.setInt(toOrdinal, from.getInt(fromOrdinal))
  }
}

private[sql] object LONG extends NativeColumnType(LongType, 1, 8) {
  override def append(v: Long, buffer: ByteBuffer): Unit = {
    buffer.putLong(v)
  }

  override def append(row: Row, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putLong(row.getLong(ordinal))
  }

  override def extract(buffer: ByteBuffer) = {
    buffer.getLong()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setLong(ordinal, buffer.getLong())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Long): Unit = {
    row.setLong(ordinal, value)
  }

  override def getField(row: Row, ordinal: Int) = row.getLong(ordinal)

  override def copyField(from: Row, fromOrdinal: Int, to: MutableRow, toOrdinal: Int): Unit = {
    to.setLong(toOrdinal, from.getLong(fromOrdinal))
  }
}

private[sql] object FLOAT extends NativeColumnType(FloatType, 2, 4) {
  override def append(v: Float, buffer: ByteBuffer): Unit = {
    buffer.putFloat(v)
  }

  override def append(row: Row, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putFloat(row.getFloat(ordinal))
  }

  override def extract(buffer: ByteBuffer) = {
    buffer.getFloat()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setFloat(ordinal, buffer.getFloat())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Float): Unit = {
    row.setFloat(ordinal, value)
  }

  override def getField(row: Row, ordinal: Int) = row.getFloat(ordinal)

  override def copyField(from: Row, fromOrdinal: Int, to: MutableRow, toOrdinal: Int): Unit = {
    to.setFloat(toOrdinal, from.getFloat(fromOrdinal))
  }
}

private[sql] object DOUBLE extends NativeColumnType(DoubleType, 3, 8) {
  override def append(v: Double, buffer: ByteBuffer): Unit = {
    buffer.putDouble(v)
  }

  override def append(row: Row, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putDouble(row.getDouble(ordinal))
  }

  override def extract(buffer: ByteBuffer) = {
    buffer.getDouble()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setDouble(ordinal, buffer.getDouble())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Double): Unit = {
    row.setDouble(ordinal, value)
  }

  override def getField(row: Row, ordinal: Int) = row.getDouble(ordinal)

  override def copyField(from: Row, fromOrdinal: Int, to: MutableRow, toOrdinal: Int): Unit = {
    to.setDouble(toOrdinal, from.getDouble(fromOrdinal))
  }
}

private[sql] object BOOLEAN extends NativeColumnType(BooleanType, 4, 1) {
  override def append(v: Boolean, buffer: ByteBuffer): Unit = {
    buffer.put(if (v) 1: Byte else 0: Byte)
  }

  override def append(row: Row, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.put(if (row.getBoolean(ordinal)) 1: Byte else 0: Byte)
  }

  override def extract(buffer: ByteBuffer) = buffer.get() == 1

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setBoolean(ordinal, buffer.get() == 1)
  }

  override def setField(row: MutableRow, ordinal: Int, value: Boolean): Unit = {
    row.setBoolean(ordinal, value)
  }

  override def getField(row: Row, ordinal: Int) = row.getBoolean(ordinal)

  override def copyField(from: Row, fromOrdinal: Int, to: MutableRow, toOrdinal: Int): Unit = {
    to.setBoolean(toOrdinal, from.getBoolean(fromOrdinal))
  }
}

private[sql] object BYTE extends NativeColumnType(ByteType, 5, 1) {
  override def append(v: Byte, buffer: ByteBuffer): Unit = {
    buffer.put(v)
  }

  override def append(row: Row, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.put(row.getByte(ordinal))
  }

  override def extract(buffer: ByteBuffer) = {
    buffer.get()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setByte(ordinal, buffer.get())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Byte): Unit = {
    row.setByte(ordinal, value)
  }

  override def getField(row: Row, ordinal: Int) = row.getByte(ordinal)

  override def copyField(from: Row, fromOrdinal: Int, to: MutableRow, toOrdinal: Int): Unit = {
    to.setByte(toOrdinal, from.getByte(fromOrdinal))
  }
}

private[sql] object SHORT extends NativeColumnType(ShortType, 6, 2) {
  override def append(v: Short, buffer: ByteBuffer): Unit = {
    buffer.putShort(v)
  }

  override def append(row: Row, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putShort(row.getShort(ordinal))
  }

  override def extract(buffer: ByteBuffer) = {
    buffer.getShort()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setShort(ordinal, buffer.getShort())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Short): Unit = {
    row.setShort(ordinal, value)
  }

  override def getField(row: Row, ordinal: Int) = row.getShort(ordinal)

  override def copyField(from: Row, fromOrdinal: Int, to: MutableRow, toOrdinal: Int): Unit = {
    to.setShort(toOrdinal, from.getShort(fromOrdinal))
  }
}

private[sql] object STRING extends NativeColumnType(StringType, 7, 8) {
  override def actualSize(row: Row, ordinal: Int): Int = {
    row.getString(ordinal).getBytes("utf-8").length + 4
  }

  override def append(v: String, buffer: ByteBuffer): Unit = {
    val stringBytes = v.getBytes("utf-8")
    buffer.putInt(stringBytes.length).put(stringBytes, 0, stringBytes.length)
  }

  override def extract(buffer: ByteBuffer) = {
    val length = buffer.getInt()
    val stringBytes = new Array[Byte](length)
    buffer.get(stringBytes, 0, length)
    new String(stringBytes, "utf-8")
  }

  override def setField(row: MutableRow, ordinal: Int, value: String): Unit = {
    row.setString(ordinal, value)
  }

  override def getField(row: Row, ordinal: Int) = row.getString(ordinal)

  override def copyField(from: Row, fromOrdinal: Int, to: MutableRow, toOrdinal: Int): Unit = {
    to.setString(toOrdinal, from.getString(fromOrdinal))
  }
}

private[sql] object DATE extends NativeColumnType(DateType, 8, 8) {
  override def extract(buffer: ByteBuffer) = {
    val date = new Date(buffer.getLong())
    date
  }

  override def append(v: Date, buffer: ByteBuffer): Unit = {
    buffer.putLong(v.getTime)
  }

  override def getField(row: Row, ordinal: Int) = {
    row(ordinal).asInstanceOf[Date]
  }

  override def setField(row: MutableRow, ordinal: Int, value: Date): Unit = {
    row(ordinal) = value
  }
}

private[sql] object TIMESTAMP extends NativeColumnType(TimestampType, 9, 12) {
  override def extract(buffer: ByteBuffer) = {
    val timestamp = new Timestamp(buffer.getLong())
    timestamp.setNanos(buffer.getInt())
    timestamp
  }

  override def append(v: Timestamp, buffer: ByteBuffer): Unit = {
    buffer.putLong(v.getTime).putInt(v.getNanos)
  }

  override def getField(row: Row, ordinal: Int) = {
    row(ordinal).asInstanceOf[Timestamp]
  }

  override def setField(row: MutableRow, ordinal: Int, value: Timestamp): Unit = {
    row(ordinal) = value
  }
}

private[sql] sealed abstract class ByteArrayColumnType[T <: DataType](
    typeId: Int,
    defaultSize: Int)
  extends ColumnType[T, Array[Byte]](typeId, defaultSize) {

  override def actualSize(row: Row, ordinal: Int) = {
    getField(row, ordinal).length + 4
  }

  override def append(v: Array[Byte], buffer: ByteBuffer): Unit = {
    buffer.putInt(v.length).put(v, 0, v.length)
  }

  override def extract(buffer: ByteBuffer) = {
    val length = buffer.getInt()
    val bytes = new Array[Byte](length)
    buffer.get(bytes, 0, length)
    bytes
  }
}

private[sql] object BINARY extends ByteArrayColumnType[BinaryType.type](10, 16) {
  override def setField(row: MutableRow, ordinal: Int, value: Array[Byte]): Unit = {
    row(ordinal) = value
  }

  override def getField(row: Row, ordinal: Int) = row(ordinal).asInstanceOf[Array[Byte]]
}

// Used to process generic objects (all types other than those listed above). Objects should be
// serialized first before appending to the column `ByteBuffer`, and is also extracted as serialized
// byte array.
private[sql] object GENERIC extends ByteArrayColumnType[DataType](11, 16) {
  override def setField(row: MutableRow, ordinal: Int, value: Array[Byte]): Unit = {
    row(ordinal) = SparkSqlSerializer.deserialize[Any](value)
  }

  override def getField(row: Row, ordinal: Int) = SparkSqlSerializer.serialize(row(ordinal))
}

private[sql] object ColumnType {
  def apply(dataType: DataType): ColumnType[_, _] = {
    dataType match {
      case IntegerType   => INT
      case LongType      => LONG
      case FloatType     => FLOAT
      case DoubleType    => DOUBLE
      case BooleanType   => BOOLEAN
      case ByteType      => BYTE
      case ShortType     => SHORT
      case StringType    => STRING
      case BinaryType    => BINARY
      case DateType      => DATE
      case TimestampType => TIMESTAMP
      case _             => GENERIC
    }
  }
}
