package org.apache.spark.sql
package columnar

import java.nio.{ByteOrder, ByteBuffer}

import org.apache.spark.sql.catalyst.types.DataType

/**
 * Builds a nullable column. The byte buffer of a nullable column contains:
 * - 4 bytes for the null count (number of nulls)
 * - positions for each null, in ascending order
 * - the non-null data (column data type, compression type, data...)
 */
trait NullableColumnBuilder[T <: DataType, JvmType] extends ColumnBuilder {
  import ColumnBuilder._

  private var _nulls: ByteBuffer = _
  private var _pos: Int = _
  private var _nullCount: Int = _

  abstract override def initialize(initialSize: Int, columnName: String) {
    _nulls = ByteBuffer.allocate(1024)
    _nulls.order(ByteOrder.nativeOrder())
    _pos = 0
    _nullCount = 0
    super.initialize(initialSize, columnName)
  }

  abstract override def append(row: Row, ordinal: Int) {
    if (row.isNullAt(ordinal)) {
      _nulls = ensureFreeSpace(_nulls, 4)
      _nulls.putInt(_pos)
      _nullCount += 1
    } else {
      super.append(row, ordinal)
    }
    _pos += 1
  }

  abstract override def build(): ByteBuffer = {
    val nonNulls = super.build()
    val nullDataLen = _nulls.position()

    _nulls.limit(nullDataLen)
    _nulls.rewind()

    // 4 bytes for null count + null positions + non nulls
    ByteBuffer
      .allocate(4 + nullDataLen + nonNulls.limit)
      .order(ByteOrder.nativeOrder())
      .putInt(_nullCount)
      .put(_nulls)
      .put(nonNulls)
      .rewind()
      .asInstanceOf[ByteBuffer]
  }
}
