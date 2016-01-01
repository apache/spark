package org.apache.spark.sql.execution.vectorized;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.unsafe.Platform;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;

/**
 * Column data backed using offheap memory.
 */
public final class OffHeapColumnVector extends ColumnVector {
  // The data stored in these two allocations need to maintain binary compatible. We can
  // directly pass this buffer to external components.
  private long nulls;
  private long data;

  protected OffHeapColumnVector(int capacity, DataType type) {
    super(capacity, type);
    this.nulls = Platform.allocateMemory(capacity);
    if (type instanceof IntegerType) {
      this.data = Platform.allocateMemory(capacity * 4);
    } else if (type instanceof DoubleType) {
      this.data = Platform.allocateMemory(capacity * 8);
    } else {
      throw new RuntimeException("Unhandled " + type);
    }
    reset();
  }

  @Override
  public final long valuesNativeAddress() {
    return data;
  }

  @Override
  public long nullsNativeAddress() {
    return nulls;
  }

  @Override
  public final void close() {
    Platform.freeMemory(nulls);
    Platform.freeMemory(data);
    nulls = 0;
    data = 0;
  }

  //
  // APIs dealing with nulls
  //

  @Override
  public final void putNotNull(int rowId) {
    Platform.putByte(null, nulls + rowId, (byte) 0);
  }

  @Override
  public final void putNull(int rowId) {
    Platform.putByte(null, nulls + rowId, (byte) 1);
    ++numNulls;
    anyNullsSet = true;
  }

  @Override
  public final void putNulls(int rowId, int count) {
    long offset = nulls + rowId;
    for (int i = 0; i < count; ++i, ++offset) {
      Platform.putByte(null, offset, (byte) 1);
    }
    anyNullsSet = true;
    numNulls += count;
  }

  @Override
  public final void putNotNulls(int rowId, int count) {
    long offset = nulls + rowId;
    for (int i = 0; i < count; ++i, ++offset) {
      Platform.putByte(null, offset, (byte) 0);
    }
  }

  @Override
  public final boolean getIsNull(int rowId) {
    return Platform.getByte(null, nulls + rowId) == 1;
  }

  //
  // APIs dealing with ints
  //

  @Override
  public final void putInt(int rowId, int value) {
    Platform.putInt(null, data + 4 * rowId, value);
  }

  @Override
  public final void putInts(int rowId, int count, int value) {
    long offset = data + 4 * rowId;
    for (int i = 0; i < count; ++i, offset += 4) {
      Platform.putInt(null, offset, value);
    }
  }

  @Override
  public final void putInts(int rowId, int count, int[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.INT_ARRAY_OFFSET + srcIndex * 4,
        null, data + 4 * rowId, count * 4);
  }

  @Override
  public final void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, srcIndex + Platform.BYTE_ARRAY_OFFSET,
        null, data + 4 * rowId, count * 4);
  }

  @Override
  public final int getInt(int rowId) {
    return Platform.getInt(null, data + 4 * rowId);
  }

  //
  // APIs dealing with doubles
  //

  @Override
  public final void putDouble(int rowId, double value) {
    Platform.putDouble(null, data + rowId * 8, value);
  }

  @Override
  public final void putDoubles(int rowId, int count, double value) {
    long offset = data + 8 * rowId;
    for (int i = 0; i < count; ++i, offset += 8) {
      Platform.putDouble(null, offset, value);
    }
  }

  @Override
  public final void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.DOUBLE_ARRAY_OFFSET + srcIndex * 8,
      null, data + 8 * rowId, count * 8);
  }

  @Override
  public final void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    // TODO: there must be a faster way to do this.
    DoubleBuffer values = ByteBuffer.wrap(src, srcIndex, src.length - srcIndex).asDoubleBuffer();
    long offset = data + 8 * rowId;
    for (int i = 0; i < count; ++i) {
      Platform.putDouble(null, offset, values.get());
      offset += 8;
    }
  }

  @Override
  public final double getDouble(int rowId) {
    return Platform.getDouble(null, data + rowId * 8);
  }
}
