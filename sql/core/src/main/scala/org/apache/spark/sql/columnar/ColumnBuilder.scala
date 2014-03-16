package org.apache.spark.sql
package columnar

import java.nio.{ByteOrder, ByteBuffer}

import org.apache.spark.sql.catalyst.types._

trait ColumnBuilder {
  /**
   * Initializes with an approximate lower bound on the expected number of elements in this column.
   */
  def initialize(initialSize: Int, columnName: String = "")

  def append(row: Row, ordinal: Int)

  def build(): ByteBuffer
}

object ColumnBuilder {
  val DEFAULT_INITIAL_BUFFER_SIZE = 10 * 1024 * 104

  def ensureFreeSpace(orig: ByteBuffer, size: Int) = {
    if (orig.remaining >= size) {
      orig
    } else {
      // grow in steps of initial size
      val capacity = orig.capacity()
      val newSize = capacity + size.max(capacity / 8 + 1)
      val pos = orig.position()

      orig.clear()
      ByteBuffer
        .allocate(newSize)
        .order(ByteOrder.nativeOrder())
        .put(orig.array(), 0, pos)
    }
  }
}

abstract class BasicColumnBuilder[T <: DataType, JvmType] extends ColumnBuilder {
  import ColumnBuilder._

  private var _columnName: String = _
  private var _buffer: ByteBuffer = _

  def columnType: ColumnType[T, JvmType]

  override def initialize(initialSize: Int, columnName: String = "") = {
    val size = if (initialSize == 0) DEFAULT_INITIAL_BUFFER_SIZE else initialSize
    _columnName = columnName
    _buffer = ByteBuffer.allocate(4 + 4 + size * columnType.defaultSize)
    _buffer.order(ByteOrder.nativeOrder()).putInt(columnType.typeId)
  }

  // Have to give a concrete implementation to make mixin possible
  override def append(row: Row, ordinal: Int) {
    doAppend(row, ordinal)
  }

  // Concrete `ColumnBuilder`s can override this method to append values
  protected def doAppend(row: Row, ordinal: Int)

  // Helper method to append primitive values (to avoid boxing cost)
  protected def appendValue(v: JvmType) {
    _buffer = ensureFreeSpace(_buffer, columnType.actualSize(v))
    columnType.append(v, _buffer)
  }

  override def build() = {
    _buffer.limit(_buffer.position()).rewind()
    _buffer
  }
}

abstract class NativeColumnBuilder[T <: NativeType](val columnType: NativeColumnType[T])
  extends BasicColumnBuilder[T, T#JvmType]
  with NullableColumnBuilder[T, T#JvmType]

class BooleanColumnBuilder extends NativeColumnBuilder(BOOLEAN) {
  override def doAppend(row: Row, ordinal: Int) {
    appendValue(row.getBoolean(ordinal))
  }
}

class IntColumnBuilder extends NativeColumnBuilder(INT) {
  override def doAppend(row: Row, ordinal: Int) {
    appendValue(row.getInt(ordinal))
  }
}

class ShortColumnBuilder extends NativeColumnBuilder(SHORT) {
  override def doAppend(row: Row, ordinal: Int) {
    appendValue(row.getShort(ordinal))
  }
}

class LongColumnBuilder extends NativeColumnBuilder(LONG) {
  override def doAppend(row: Row, ordinal: Int) {
    appendValue(row.getLong(ordinal))
  }
}

class ByteColumnBuilder extends NativeColumnBuilder(BYTE) {
  override def doAppend(row: Row, ordinal: Int) {
    appendValue(row.getByte(ordinal))
  }
}

class DoubleColumnBuilder extends NativeColumnBuilder(DOUBLE) {
  override def doAppend(row: Row, ordinal: Int) {
    appendValue(row.getDouble(ordinal))
  }
}

class FloatColumnBuilder extends NativeColumnBuilder(FLOAT) {
  override def doAppend(row: Row, ordinal: Int) {
    appendValue(row.getFloat(ordinal))
  }
}

class StringColumnBuilder extends NativeColumnBuilder(STRING) {
  override def doAppend(row: Row, ordinal: Int) {
    appendValue(row.getString(ordinal))
  }
}

class BinaryColumnBuilder
  extends BasicColumnBuilder[BinaryType.type, Array[Byte]]
  with NullableColumnBuilder[BinaryType.type, Array[Byte]] {

  val columnType = BINARY

  override def doAppend(row: Row, ordinal: Int) {
    appendValue(row(ordinal).asInstanceOf[Array[Byte]])
  }
}

// TODO (lian) Add support for array, struct and map
class GenericColumnBuilder
  extends BasicColumnBuilder[DataType, Any]
  with NullableColumnBuilder[DataType, Any] {

  val columnType = GENERIC

  override def doAppend(row: Row, ordinal: Int) {
    appendValue(row(ordinal))
  }
}
