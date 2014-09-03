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

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.MutableRow
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.execution.SparkSqlSerializer

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
   * Appends the given value v of type T into the given ByteBuffer.
   */
  def append(v: JvmType, buffer: ByteBuffer)

  /**
   * Returns the size of the value. This is used to calculate the size of variable length types
   * such as byte arrays and strings.
   */
  def actualSize(v: JvmType): Int = defaultSize

  /**
   * Returns `row(ordinal)`. Subclasses should override this method to avoid boxing/unboxing costs
   * whenever possible.
   */
  def getField(row: Row, ordinal: Int): JvmType

  /**
   * Sets `row(ordinal)` to `field`. Subclasses should override this method to avoid boxing/unboxing
   * costs whenever possible.
   */
  def setField(row: MutableRow, ordinal: Int, value: JvmType)

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
  def append(v: Int, buffer: ByteBuffer) {
    buffer.putInt(v)
  }

  def extract(buffer: ByteBuffer) = {
    buffer.getInt()
  }

  override def setField(row: MutableRow, ordinal: Int, value: Int) {
    row.setInt(ordinal, value)
  }

  override def getField(row: Row, ordinal: Int) = row.getInt(ordinal)
}

private[sql] object LONG extends NativeColumnType(LongType, 1, 8) {
  override def append(v: Long, buffer: ByteBuffer) {
    buffer.putLong(v)
  }

  override def extract(buffer: ByteBuffer) = {
    buffer.getLong()
  }

  override def setField(row: MutableRow, ordinal: Int, value: Long) {
    row.setLong(ordinal, value)
  }

  override def getField(row: Row, ordinal: Int) = row.getLong(ordinal)
}

private[sql] object FLOAT extends NativeColumnType(FloatType, 2, 4) {
  override def append(v: Float, buffer: ByteBuffer) {
    buffer.putFloat(v)
  }

  override def extract(buffer: ByteBuffer) = {
    buffer.getFloat()
  }

  override def setField(row: MutableRow, ordinal: Int, value: Float) {
    row.setFloat(ordinal, value)
  }

  override def getField(row: Row, ordinal: Int) = row.getFloat(ordinal)
}

private[sql] object DOUBLE extends NativeColumnType(DoubleType, 3, 8) {
  override def append(v: Double, buffer: ByteBuffer) {
    buffer.putDouble(v)
  }

  override def extract(buffer: ByteBuffer) = {
    buffer.getDouble()
  }

  override def setField(row: MutableRow, ordinal: Int, value: Double) {
    row.setDouble(ordinal, value)
  }

  override def getField(row: Row, ordinal: Int) = row.getDouble(ordinal)
}

private[sql] object BOOLEAN extends NativeColumnType(BooleanType, 4, 1) {
  override def append(v: Boolean, buffer: ByteBuffer) {
    buffer.put(if (v) 1.toByte else 0.toByte)
  }

  override def extract(buffer: ByteBuffer) = buffer.get() == 1

  override def setField(row: MutableRow, ordinal: Int, value: Boolean) {
    row.setBoolean(ordinal, value)
  }

  override def getField(row: Row, ordinal: Int) = row.getBoolean(ordinal)
}

private[sql] object BYTE extends NativeColumnType(ByteType, 5, 1) {
  override def append(v: Byte, buffer: ByteBuffer) {
    buffer.put(v)
  }

  override def extract(buffer: ByteBuffer) = {
    buffer.get()
  }

  override def setField(row: MutableRow, ordinal: Int, value: Byte) {
    row.setByte(ordinal, value)
  }

  override def getField(row: Row, ordinal: Int) = row.getByte(ordinal)
}

private[sql] object SHORT extends NativeColumnType(ShortType, 6, 2) {
  override def append(v: Short, buffer: ByteBuffer) {
    buffer.putShort(v)
  }

  override def extract(buffer: ByteBuffer) = {
    buffer.getShort()
  }

  override def setField(row: MutableRow, ordinal: Int, value: Short) {
    row.setShort(ordinal, value)
  }

  override def getField(row: Row, ordinal: Int) = row.getShort(ordinal)
}

private[sql] object STRING extends NativeColumnType(StringType, 7, 8) {
  override def actualSize(v: String): Int = v.getBytes("utf-8").length + 4

  override def append(v: String, buffer: ByteBuffer) {
    val stringBytes = v.getBytes("utf-8")
    buffer.putInt(stringBytes.length).put(stringBytes, 0, stringBytes.length)
  }

  override def extract(buffer: ByteBuffer) = {
    val length = buffer.getInt()
    val stringBytes = new Array[Byte](length)
    buffer.get(stringBytes, 0, length)
    new String(stringBytes, "utf-8")
  }

  override def setField(row: MutableRow, ordinal: Int, value: String) {
    row.setString(ordinal, value)
  }

  override def getField(row: Row, ordinal: Int) = row.getString(ordinal)
}

private[sql] object TIMESTAMP extends NativeColumnType(TimestampType, 8, 12) {
  override def extract(buffer: ByteBuffer) = {
    val timestamp = new Timestamp(buffer.getLong())
    timestamp.setNanos(buffer.getInt())
    timestamp
  }

  override def append(v: Timestamp, buffer: ByteBuffer) {
    buffer.putLong(v.getTime).putInt(v.getNanos)
  }

  override def getField(row: Row, ordinal: Int) = {
    row(ordinal).asInstanceOf[Timestamp]
  }

  override def setField(row: MutableRow, ordinal: Int, value: Timestamp) {
    row(ordinal) = value
  }
}

private[sql] sealed abstract class ByteArrayColumnType[T <: DataType](
    typeId: Int,
    defaultSize: Int)
  extends ColumnType[T, Array[Byte]](typeId, defaultSize) {

  override def actualSize(v: Array[Byte]) = v.length + 4

  override def append(v: Array[Byte], buffer: ByteBuffer) {
    buffer.putInt(v.length).put(v, 0, v.length)
  }

  override def extract(buffer: ByteBuffer) = {
    val length = buffer.getInt()
    val bytes = new Array[Byte](length)
    buffer.get(bytes, 0, length)
    bytes
  }
}

private[sql] object BINARY extends ByteArrayColumnType[BinaryType.type](9, 16) {
  override def setField(row: MutableRow, ordinal: Int, value: Array[Byte]) {
    row(ordinal) = value
  }

  override def getField(row: Row, ordinal: Int) = row(ordinal).asInstanceOf[Array[Byte]]
}

// Used to process generic objects (all types other than those listed above). Objects should be
// serialized first before appending to the column `ByteBuffer`, and is also extracted as serialized
// byte array.
private[sql] object GENERIC extends ByteArrayColumnType[DataType](10, 16) {
  override def setField(row: MutableRow, ordinal: Int, value: Array[Byte]) {
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
      case TimestampType => TIMESTAMP
      case _             => GENERIC
    }
  }
}
