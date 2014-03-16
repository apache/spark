package org.apache.spark.sql
package columnar

import java.nio.ByteBuffer

import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.execution.KryoSerializer

/**
 * An abstract class that represents type of a column. Used to append/extract Java objects into/from
 * the underlying [[ByteBuffer]] of a column.
 *
 * @param typeId A unique ID representing the type.
 * @param defaultSize Default size in bytes for one element of type T (e.g. 4 for `Int`).
 * @tparam T Scala data type for the column.
 * @tparam JvmType Underlying Java type to represent the elements.
 */
sealed abstract class ColumnType[T <: DataType, JvmType](
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
   * Creates a duplicated copy of the value.
   */
  def clone(v: JvmType): JvmType = v
}

private[columnar] abstract class NativeColumnType[T <: NativeType](
    val dataType: T,
    typeId: Int,
    defaultSize: Int)
  extends ColumnType[T, T#JvmType](typeId, defaultSize) {

  /**
   * Scala TypeTag. Can be used to create primitive arrays and hash tables.
   */
  def scalaTag = dataType.tag
}

object INT extends NativeColumnType(IntegerType, 0, 4) {
  def append(v: Int, buffer: ByteBuffer) {
    buffer.putInt(v)
  }

  def extract(buffer: ByteBuffer) = {
    buffer.getInt()
  }
}

object LONG extends NativeColumnType(LongType, 1, 8) {
  override def append(v: Long, buffer: ByteBuffer) {
    buffer.putLong(v)
  }

  override def extract(buffer: ByteBuffer) = {
    buffer.getLong()
  }
}

object FLOAT extends NativeColumnType(FloatType, 2, 4) {
  override def append(v: Float, buffer: ByteBuffer) {
    buffer.putFloat(v)
  }

  override def extract(buffer: ByteBuffer) = {
    buffer.getFloat()
  }
}

object DOUBLE extends NativeColumnType(DoubleType, 3, 8) {
  override def append(v: Double, buffer: ByteBuffer) {
    buffer.putDouble(v)
  }

  override def extract(buffer: ByteBuffer) = {
    buffer.getDouble()
  }
}

object BOOLEAN extends NativeColumnType(BooleanType, 4, 1) {
  override def append(v: Boolean, buffer: ByteBuffer) {
    buffer.put(if (v) 1.toByte else 0.toByte)
  }

  override def extract(buffer: ByteBuffer) = {
    if (buffer.get() == 1) true else false
  }
}

object BYTE extends NativeColumnType(ByteType, 5, 1) {
  override def append(v: Byte, buffer: ByteBuffer) {
    buffer.put(v)
  }

  override def extract(buffer: ByteBuffer) = {
    buffer.get()
  }
}

object SHORT extends NativeColumnType(ShortType, 6, 2) {
  override def append(v: Short, buffer: ByteBuffer) {
    buffer.putShort(v)
  }

  override def extract(buffer: ByteBuffer) = {
    buffer.getShort()
  }
}

object STRING extends NativeColumnType(StringType, 7, 8) {
  override def actualSize(v: String): Int = v.getBytes.length + 4

  override def append(v: String, buffer: ByteBuffer) {
    val stringBytes = v.getBytes()
    buffer.putInt(stringBytes.length).put(stringBytes, 0, stringBytes.length)
  }

  override def extract(buffer: ByteBuffer) = {
    val length = buffer.getInt()
    val stringBytes = new Array[Byte](length)
    buffer.get(stringBytes, 0, length)
    new String(stringBytes)
  }
}

object BINARY extends ColumnType[BinaryType.type, Array[Byte]](8, 16) {
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

object GENERIC extends ColumnType[DataType, Any](9, 16) {
  // TODO (lian) Can we avoid serialization here?
  override def actualSize(v: Any) = KryoSerializer.serialize(v).size

  override def append(v: Any, buffer: ByteBuffer) {
    val serialized = KryoSerializer.serialize(v)
    buffer.putInt(serialized.length).put(serialized)
  }

  override def extract(buffer: ByteBuffer) = {
    val length = buffer.getInt()
    val serialized = new Array[Byte](length)
    buffer.get(serialized, 0, length)
    KryoSerializer.deserialize[Any](serialized)
  }
}
