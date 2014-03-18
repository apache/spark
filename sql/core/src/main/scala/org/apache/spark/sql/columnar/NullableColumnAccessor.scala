package org.apache.spark.sql.columnar

import java.nio.{ByteOrder, ByteBuffer}

import org.apache.spark.sql.catalyst.expressions.MutableRow

trait NullableColumnAccessor extends ColumnAccessor {
  private var nullsBuffer: ByteBuffer = _
  private var nullCount: Int = _
  private var seenNulls: Int = 0

  private var nextNullIndex: Int = _
  private var pos: Int = 0

  abstract override def initialize() {
    nullsBuffer = underlyingBuffer.duplicate().order(ByteOrder.nativeOrder())
    nullCount = nullsBuffer.getInt()
    nextNullIndex = if (nullCount > 0) nullsBuffer.getInt() else -1
    pos = 0

    underlyingBuffer.position(underlyingBuffer.position + 4 + nullCount * 4)
    super.initialize()
  }

  abstract override def extractTo(row: MutableRow, ordinal: Int) {
    if (pos == nextNullIndex) {
      seenNulls += 1

      if (seenNulls < nullCount) {
        nextNullIndex = nullsBuffer.getInt()
      }

      row.setNullAt(ordinal)
    } else {
      super.extractTo(row, ordinal)
    }

    pos += 1
  }
}
