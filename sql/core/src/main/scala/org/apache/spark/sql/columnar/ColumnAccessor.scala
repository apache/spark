package org.apache.spark.sql
package columnar

import java.nio.{ByteOrder, ByteBuffer}

import org.apache.spark.sql.catalyst.types.{BinaryType, NativeType, DataType}
import org.apache.spark.sql.catalyst.expressions.MutableRow
import org.apache.spark.sql.execution.KryoSerializer

/**
 * An `Iterator` like trait used to extract values from columnar byte buffer. When a value is
 * extracted from the buffer, instead of directly returning it, the value is set into some field of
 * a [[MutableRow]]. In this way, boxing cost can be avoided by leveraging the setter methods
 * for primitive values provided by [[MutableRow]].
 */
trait ColumnAccessor {
  initialize()

  protected def initialize()

  def hasNext: Boolean

  def extractTo(row: MutableRow, ordinal: Int)

  protected def underlyingBuffer: ByteBuffer
}

abstract class BasicColumnAccessor[T <: DataType, JvmType](buffer: ByteBuffer)
  extends ColumnAccessor {

  protected def initialize() {}

  def columnType: ColumnType[T, JvmType]

  def hasNext = buffer.hasRemaining

  def extractTo(row: MutableRow, ordinal: Int) {
    doExtractTo(row, ordinal)
  }

  protected def doExtractTo(row: MutableRow, ordinal: Int)

  protected def underlyingBuffer = buffer
}

abstract class NativeColumnAccessor[T <: NativeType](
    buffer: ByteBuffer,
    val columnType: NativeColumnType[T])
  extends BasicColumnAccessor[T, T#JvmType](buffer)
  with NullableColumnAccessor

class BooleanColumnAccessor(buffer: ByteBuffer) extends NativeColumnAccessor(buffer, BOOLEAN) {
  override protected def doExtractTo(row: MutableRow, ordinal: Int) {
    row.setBoolean(ordinal, columnType.extract(buffer))
  }
}

class IntColumnAccessor(buffer: ByteBuffer) extends NativeColumnAccessor(buffer, INT) {
  override protected def doExtractTo(row: MutableRow, ordinal: Int) {
    row.setInt(ordinal, columnType.extract(buffer))
  }
}

class ShortColumnAccessor(buffer: ByteBuffer) extends NativeColumnAccessor(buffer, SHORT) {
  override protected def doExtractTo(row: MutableRow, ordinal: Int) {
    row.setShort(ordinal, columnType.extract(buffer))
  }
}

class LongColumnAccessor(buffer: ByteBuffer) extends NativeColumnAccessor(buffer, LONG) {
  override protected def doExtractTo(row: MutableRow, ordinal: Int) {
    row.setLong(ordinal, columnType.extract(buffer))
  }
}

class ByteColumnAccessor(buffer: ByteBuffer) extends NativeColumnAccessor(buffer, BYTE) {
  override protected def doExtractTo(row: MutableRow, ordinal: Int) {
    row.setByte(ordinal, columnType.extract(buffer))
  }
}

class DoubleColumnAccessor(buffer: ByteBuffer) extends NativeColumnAccessor(buffer, DOUBLE) {
  override protected def doExtractTo(row: MutableRow, ordinal: Int) {
    row.setDouble(ordinal, columnType.extract(buffer))
  }
}

class FloatColumnAccessor(buffer: ByteBuffer) extends NativeColumnAccessor(buffer, FLOAT) {
  override protected def doExtractTo(row: MutableRow, ordinal: Int) {
    row.setFloat(ordinal, columnType.extract(buffer))
  }
}

class StringColumnAccessor(buffer: ByteBuffer) extends NativeColumnAccessor(buffer, STRING) {
  override protected def doExtractTo(row: MutableRow, ordinal: Int) {
    row.setString(ordinal, columnType.extract(buffer))
  }
}

class BinaryColumnAccessor(buffer: ByteBuffer)
  extends BasicColumnAccessor[BinaryType.type, Array[Byte]](buffer)
  with NullableColumnAccessor {

  def columnType = BINARY

  override protected def doExtractTo(row: MutableRow, ordinal: Int) {
    row(ordinal) = columnType.extract(buffer)
  }
}

class GenericColumnAccessor(buffer: ByteBuffer)
  extends BasicColumnAccessor[DataType, Array[Byte]](buffer)
  with NullableColumnAccessor {

  def columnType = GENERIC

  override protected def doExtractTo(row: MutableRow, ordinal: Int) {
    val serialized = columnType.extract(buffer)
    row(ordinal) = KryoSerializer.deserialize[Any](serialized)
  }
}

object ColumnAccessor {
  def apply(b: ByteBuffer): ColumnAccessor = {
    // The first 4 bytes in the buffer indicates the column type.
    val buffer = b.duplicate().order(ByteOrder.nativeOrder())
    val columnTypeId = buffer.getInt()

    columnTypeId match {
      case INT.typeId     => new IntColumnAccessor(buffer)
      case LONG.typeId    => new LongColumnAccessor(buffer)
      case FLOAT.typeId   => new FloatColumnAccessor(buffer)
      case DOUBLE.typeId  => new DoubleColumnAccessor(buffer)
      case BOOLEAN.typeId => new BooleanColumnAccessor(buffer)
      case BYTE.typeId    => new ByteColumnAccessor(buffer)
      case SHORT.typeId   => new ShortColumnAccessor(buffer)
      case STRING.typeId  => new StringColumnAccessor(buffer)
      case BINARY.typeId  => new BinaryColumnAccessor(buffer)
      case GENERIC.typeId => new GenericColumnAccessor(buffer)
    }
  }
}
