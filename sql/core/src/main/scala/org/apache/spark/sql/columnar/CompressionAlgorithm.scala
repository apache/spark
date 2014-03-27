package org.apache.spark.sql.columnar

import java.nio.{ByteOrder, ByteBuffer}

import org.apache.spark.sql.catalyst.types.NativeType

private[sql] object CompressionType extends Enumeration {
  type CompressionType = Value

  val Default, Noop, RLE, Dictionary, BooleanBitSet, IntDelta, LongDelta = Value
}

private[sql] trait CompressionAlgorithm {
  def compressionType: CompressionType.Value

  def supports(columnType: ColumnType[_, _]): Boolean

  def gatherCompressibilityStats[T <: NativeType](
      value: T#JvmType,
      columnType: ColumnType[T, T#JvmType]) {}

  def compressedSize: Int

  def uncompressedSize: Int

  def compressionRatio: Double = compressedSize.toDouble / uncompressedSize

  def compress[T <: NativeType](from: ByteBuffer, columnType: ColumnType[T, T#JvmType]): ByteBuffer
}

private[sql] object CompressionAlgorithm {
  def apply(typeId: Int) = typeId match {
    case CompressionType.Noop => new CompressionAlgorithm.Noop
    case _ => throw new UnsupportedOperationException()
  }

  class Noop extends CompressionAlgorithm {
    override def uncompressedSize = 0
    override def compressedSize = 0
    override def compressionRatio = 1.0
    override def supports(columnType: ColumnType[_, _]) = true
    override def compressionType = CompressionType.Noop

    override def compress[T <: NativeType](
        from: ByteBuffer,
        columnType: ColumnType[T, T#JvmType]) = {

      // Reserves 4 bytes for compression type
      val to = ByteBuffer.allocate(from.limit + 4).order(ByteOrder.nativeOrder)
      copyHeader(from, to)

      // Writes compression type ID and copies raw contents
      to.putInt(CompressionType.Noop.id).put(from).rewind()
      to
    }
  }

  class NoopDecoder[T <: NativeType](buffer: ByteBuffer, columnType: ColumnType[T, T#JvmType])
    extends Iterator[T#JvmType] {

    override def next() = columnType.extract(buffer)

    override def hasNext = buffer.hasRemaining
  }

  def copyNullInfo(from: ByteBuffer, to: ByteBuffer) {
    // Writes null count
    val nullCount = from.getInt()
    to.putInt(nullCount)

    // Writes null positions
    var i = 0
    while (i < nullCount) {
      to.putInt(from.getInt())
      i += 1
    }
  }

  def copyHeader(from: ByteBuffer, to: ByteBuffer) {
    // Writes column type ID
    to.putInt(from.getInt())

    // Copies null count and null positions
    copyNullInfo(from, to)
  }
}
