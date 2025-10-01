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
package org.apache.spark.sql.execution.vectorized;

import java.nio.*;
import java.util.List;
import java.util.TimeZone;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

public final class ArrowWritableColumnVector extends WritableColumnVector {
  /**
   * allocateColumns: create VectorSchemaRoot once, reuse FieldVectors per column.
   */
  public static ArrowWritableColumnVector[] allocateColumns(int capacity, StructType schema) {
    VectorSchemaRoot root = VectorSchemaRoot.create(
      ArrowUtils.toArrowSchema(schema, TimeZone.getDefault().getID(), false, false),
      ArrowUtils.rootAllocator());
    List<FieldVector> fieldVectors = root.getFieldVectors();

    ArrowWritableColumnVector[] vectors = new ArrowWritableColumnVector[fieldVectors.size()];
    for (int i = 0; i < fieldVectors.size(); i++) {
      vectors[i] = new ArrowWritableColumnVector(capacity, schema.fields()[i].dataType(), fieldVectors.get(i));
    }
    return vectors;
  }

  public static ArrowWritableColumnVector[] allocateColumns(int capacity, StructField[] fields) {
    return allocateColumns(capacity, new StructType(fields));
  }

  private final ValueVector valueVector;
  private final BaseFixedWidthVector baseFixedWidthVector;
  private long data;
  private long validity;

  public ArrowWritableColumnVector(int capacity, DataType dataType) {
    super(capacity, dataType);
    FieldVector fieldVector = VectorSchemaRoot.create(
      ArrowUtils.toArrowSchema(
        new StructType(new StructField[] {
          new StructField("dummy", type, true, Metadata.empty())
        }),
        TimeZone.getDefault().getID(),
        false,
        false),
      ArrowUtils.rootAllocator()).getFieldVectors().get(0);

    this.valueVector = fieldVector;

    // typed caches
    this.baseFixedWidthVector = valueVector instanceof BaseFixedWidthVector ? (BaseFixedWidthVector) valueVector : null;
    assert baseFixedWidthVector != null;
    reserveInternal(capacity);
  }

  /**
   * New constructor that wraps an existing FieldVector (avoid re-creating VectorSchemaRoot).
   */
  public ArrowWritableColumnVector(int capacity, DataType dataType, FieldVector fieldVector) {
    super(capacity, dataType);
    this.valueVector = fieldVector;
    this.baseFixedWidthVector = valueVector instanceof BaseFixedWidthVector ? (BaseFixedWidthVector) valueVector : null;
    assert baseFixedWidthVector != null;
    reserveInternal(capacity);
  }

  @Override
  protected void releaseMemory() {
    valueVector.reset();
  }

  @Override
  public void close() { valueVector.close(); }

  @Override
  public void setNum(int num) {
    valueVector.setValueCount(num);
  }

  //
  // Validity / nulls
  //

  @Override
  public void putNull(int rowId) { setValidityBitsBulk(rowId, 1, false); }

  @Override
  public void putNotNull(int rowId) { setValidityBitsBulk(rowId, 1, true); }

  @Override
  public void putNulls(int rowId, int count) { setValidityBitsBulk(rowId, count, false); }

  @Override
  public void putNotNulls(int rowId, int count) { setValidityBitsBulk(rowId, count, true); }

  /**
   * Instance-based highly-optimized validity setter using per-instance fast paths.
   */
  private void setValidityBitsBulk(int rowId, int count, boolean isValid) {
    if (validity == 0L) return;
    long base = validity + (rowId >>> 3);
    int bits = (count + 7) >>> 3;
    long fill = isValid ? 0xFFFFFFFFFFFFFFFFL : 0L;
    int i = 0;
    for (; i + 8 <= bits; i += 8) {
      Platform.putLong(null, base + i, fill);
    }
    for (; i < bits; i++) {
      Platform.putByte(null, base + i, (byte)(isValid ? 0xFF : 0x00));
    }
  }

  @Override
  public boolean isNullAt(int rowId) {
    if (isAllNull) return true;
    if (validity == 0L) return false;
    byte b = Platform.getByte(null, validity + (rowId >>> 3));
    int bitMask = 1 << (rowId & 7);
    return (b & bitMask) == 0;
  }

  //
  // Booleans
  //

  @Override
  public void putBoolean(int rowId, boolean value) {
    long addr = data + (rowId >>> 3);
    int mask = 1 << (rowId & 7);
    byte b = Platform.getByte(null, addr);
    if (value) b |= mask; else b &= ~mask;
    Platform.putByte(null, addr, b);
    setValidityBitsBulk(rowId, 1, true);
  }

  @Override
  public void putBooleans(int rowId, int count, boolean value) {
    int startBit = rowId & 7;
    int endBit = (rowId + count - 1) & 7;
    int startByte = rowId >>> 3;
    int endByte = (rowId + count - 1) >>> 3;
    byte fill = (byte)(value ? 0xFF : 0x00);

    if (startByte == endByte) {
      int mask = ((1 << (endBit - startBit + 1)) - 1) << startBit;
      byte orig = Platform.getByte(null, data + startByte);
      byte updated = (byte)(value ? (orig | mask) : (orig & ~mask));
      Platform.putByte(null, data + startByte, updated);
    } else {
      if (startBit != 0) {
        byte orig = Platform.getByte(null, data + startByte);
        int mask = 0xFF & (~((1 << startBit) - 1));
        byte updated = (byte)(value ? (orig | mask) : (orig & ~mask));
        Platform.putByte(null, data + startByte, updated);
        startByte++;
      }

      int fullBytes = endByte - startByte;
      long addr = data + startByte;
      int i = 0;
      int unroll = 16;
      if (fullBytes >= unroll) {
        long pattern = 0x0101010101010101L * (fill & 0xFFL);
        for (; i + unroll <= fullBytes; i += unroll) {
          Platform.putLong(null, addr + i, pattern);
          Platform.putLong(null, addr + i + 8, pattern);
        }
      }
      for (; i < fullBytes; i++) {
        Platform.putByte(null, addr + i, fill);
      }

      if (endBit != 7) {
        byte orig = Platform.getByte(null, data + endByte);
        int mask = (1 << (endBit + 1)) - 1;
        byte updated = (byte)(value ? (orig | mask) : (orig & ~mask));
        Platform.putByte(null, data + endByte, updated);
      }
    }

    setValidityBitsBulk(rowId, count, true);
  }

  @Override
  public void putBooleans(int rowId, byte src) {
    // Arrow boolean bit-packed 방식: 한 byte당 8개 boolean
    long base = data + (rowId >>> 3);
    int bitOffset = rowId & 7;

    byte b = Platform.getByte(null, base);
    b |= (src << bitOffset);
    Platform.putByte(null, base, b);

    if (bitOffset > 0) {
      // 다음 byte에도 spillover 발생
      byte next = Platform.getByte(null, base + 1);
      next |= (src >>> (8 - bitOffset));
      Platform.putByte(null, base + 1, next);
    }

    setValidityBitsBulk(rowId, 8, true);
  }

  @Override
  public boolean getBoolean(int rowId) {
    byte b = Platform.getByte(null, data + (rowId >>> 3));
    int bitMask = 1 << (rowId & 7);
    return (b & bitMask) != 0;
  }

  @Override
  public boolean[] getBooleans(int rowId, int count) {
    boolean[] res = new boolean[count];
    int startByte = rowId >>> 3;
    int bitOffset = rowId & 7;
    long addr = data + startByte;

    int idx = 0;
    int remaining = count;
    while (remaining >= 64) {
      long word = Platform.getLong(null, addr);
      for (int i = 0; i < 64; i++) {
        res[idx + i] = ((word >>> (i + bitOffset)) & 1L) != 0L;
      }
      idx += 64;
      remaining -= 64;
      addr += 8;
    }

    if (remaining > 0) {
      long word = Platform.getLong(null, addr);
      for (int i = 0; i < remaining; i++) {
        res[idx + i] = ((word >>> (i + bitOffset)) & 1L) != 0L;
      }
    }
    return res;
  }

  //
  // Bytes
  //
  @Override
  public void putByte(int rowId, byte value) {
    Platform.putByte(null, data + rowId, value);
    setValidityBitsBulk(rowId, 1, true);
  }

  @Override
  public void putBytes(int rowId, int count, byte value) {
    long base = data + rowId;
    int i = 0;
    int unroll = 16;
    for (; i + unroll <= count; i += unroll) {
      long v = (value & 0xFFL) * 0x0101010101010101L;
      Platform.putLong(null, base + i, v);
      Platform.putLong(null, base + i + 8, v);
    }
    for (; i < count; i++) Platform.putByte(null, base + i, value);
    setValidityBitsBulk(rowId, count, true);
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, null,
       data + rowId, count);
    setValidityBitsBulk(rowId, count, true);
  }

  @Override
  public byte getByte(int rowId) { return Platform.getByte(null, data + rowId); }

  @Override
  public byte[] getBytes(int rowId, int count) {
    byte[] result = new byte[count];
    Platform.copyMemory(null, data + rowId, result, Platform.BYTE_ARRAY_OFFSET, count);
    return result;
  }

  @Override
  protected UTF8String getBytesAsUTF8String(int rowId, int count) {
    return UTF8String.fromBytes(getBinary(rowId));
  }

  @Override
  public ByteBuffer getByteBuffer(int rowId, int count) {
    throw new UnsupportedOperationException();
  }

  //
  // Shorts
  //

  @Override
  public void putShort(int rowId, short value) {
    Platform.putShort(null, data + rowId * 2L, value);
    setValidityBitsBulk(rowId, 1, true);
  }

  @Override
  public void putShorts(int rowId, int count, short value) {
    long base = data + rowId * 2L;
    long packed = ((long) value & 0xFFFFL);
    packed |= (packed << 16) | (packed << 32) | (packed << 48);
    int i = 0;
    int unroll = 8;
    for (; i + unroll <= count; i += unroll) {
      Platform.putLong(null, base + i * 2L, packed);
      Platform.putLong(null, base + (i + 4) * 2L, packed);
    }
    for (; i < count; i++) Platform.putShort(null, base + i * 2L, value);
    setValidityBitsBulk(rowId, count, true);
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.SHORT_ARRAY_OFFSET + srcIndex * 2L, null,
      data + rowId * 2L, count * 2L);
    setValidityBitsBulk(rowId, count, true);
  }

  @Override
  public void putShorts(int rowId, int count, byte[] src, int srcIndex) {
    long dest = data + (long) rowId * 2L;
    long srcAddr = Platform.BYTE_ARRAY_OFFSET + srcIndex;
    long bytes = (long) count * 2L;

    Platform.copyMemory(src, srcAddr, null, dest, bytes);
    setValidityBitsBulk(rowId, count, true);
  }

  @Override
  public short getShort(int rowId) {
    return Platform.getShort(null, data + rowId * 2L);
  }

  @Override
  public short[] getShorts(int rowId, int count) {
    short[] result = new short[count];
    Platform.copyMemory(null, data + rowId * 2L, result, Platform.SHORT_ARRAY_OFFSET, count * 2L);
    return result;
  }

  //
  // Ints
  //

  @Override
  public void putInt(int rowId, int value) {
    // Arrow int vector: 4 bytes per element
    long dest = data + (long) rowId * 4L;
    Platform.putInt(null, dest, value);
    setValidityBitsBulk(rowId, 1, true);
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    long base = data + rowId * 4L;
    long v = ((long) value & 0xFFFFFFFFL);
    long packed = v | (v << 32);
    int i = 0;
    int unroll = 8;
    for (; i + unroll <= count; i += unroll) {
      Platform.putLong(null, base + i * 4L, packed);
      Platform.putLong(null, base + (i + 2) * 4L, packed);
      Platform.putLong(null, base + (i + 4) * 4L, packed);
      Platform.putLong(null, base + (i + 6) * 4L, packed);
    }
    for (; i < count; i++) Platform.putInt(null, base + i * 4L, value);
    setValidityBitsBulk(rowId, count, true);
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.INT_ARRAY_OFFSET + srcIndex * 4L, null,
      data + rowId * 4L, count * 4L);
    setValidityBitsBulk(rowId, count, true);
  }

  @Override
  public void putInts(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, null,
      data + rowId * 4L, count * 4L);
    setValidityBitsBulk(rowId, count, true);
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, null,
      data + rowId * 4L, count * 4L);
    setValidityBitsBulk(rowId, count, true);
  }


  @Override
  public int getInt(int rowId) {
    return Platform.getInt(null, data + 4L * rowId);
  }

  @Override
  public int[] getInts(int rowId, int count) {
    int[] array = new int[count];
    Platform.copyMemory(
      null, data + rowId * 4L, array, Platform.INT_ARRAY_OFFSET, count * 4L);
    return array;
  }

  @Override
  public int getDictId(int rowId) {
    throw new UnsupportedOperationException();
  }

  //
  // Longs
  //

  @Override
  public void putLong(int rowId, long value) {
    Platform.putLong(null, data + rowId * 8L, value);
    setValidityBitsBulk(rowId, 1, true);
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    long base = data + rowId * 8L;
    int i = 0;
    int unroll = 16;
    for (; i + unroll <= count; i += unroll) {
      for (int j = 0; j < unroll; j++) {
        Platform.putLong(null, base + (i + j) * 8L, value);
      }
    }
    for (; i < count; i++) Platform.putLong(null, base + i * 8L, value);
    setValidityBitsBulk(rowId, count, true);
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.LONG_ARRAY_OFFSET + srcIndex * 8L, null,
      data + rowId * 8L, count * 8L);
    setValidityBitsBulk(rowId, count, true);
  }

  @Override
  public void putLongs(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, null,
      data + rowId * 8L, count * 8L);
    setValidityBitsBulk(rowId, count, true);
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, null,
      data + rowId * 8L, count * 8L);
    setValidityBitsBulk(rowId, count, true);
  }

  @Override
  public long getLong(int rowId) {
    return Platform.getLong(null, data + 8L * rowId);
  }

  @Override
  public long[] getLongs(int rowId, int count) {
    long[] array = new long[count];
    Platform.copyMemory(null, data + rowId * 8L, array, Platform.LONG_ARRAY_OFFSET, count * 8L);
    return array;
  }

  //
  // Floats
  //

  @Override
  public void putFloat(int rowId, float value) {
    Platform.putFloat(null, data + rowId * 4L, value);
    setValidityBitsBulk(rowId, 1, true);
  }

  @Override
  public void putFloats(int rowId, int count, float value) {
    long base = data + rowId * 4L;
    for (int i = 0; i < count; i++) Platform.putFloat(null, base + i * 4L, value);
    setValidityBitsBulk(rowId, count, true);
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.FLOAT_ARRAY_OFFSET + srcIndex * 4L, null,
      data + rowId * 4L, count * 4L);
    setValidityBitsBulk(rowId, count, true);
  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, null,
      data + rowId * 4L, count * 4L);
    setValidityBitsBulk(rowId, count, true);
  }

  @Override
  public void putFloatsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, null,
      data + rowId * 4L, count * 4L);
    setValidityBitsBulk(rowId, count, true);
  }

  @Override
  public float getFloat(int rowId) {
    return Platform.getFloat(null, data + rowId * 4L);
  }

  @Override
  public float[] getFloats(int rowId, int count) {
    float[] array = new float[count];
    Platform.copyMemory(null, data + rowId * 4L, array, Platform.FLOAT_ARRAY_OFFSET, count * 4L);
    return array;
  }

  //
  // Doubles
  //

  @Override
  public void putDouble(int rowId, double value) {
    Platform.putDouble(null, data + rowId * 8L, value);
    setValidityBitsBulk(rowId, 1, true);
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {
    long base = data + rowId * 8L;
    int i = 0;
    int unroll = 16;
    for (; i + unroll <= count; i += unroll) {
      for (int j = 0; j < unroll; j++) {
        Platform.putDouble(null, base + (i + j) * 8L, value);
      }
    }
    for (; i < count; i++) Platform.putDouble(null, base + i * 8L, value);
    setValidityBitsBulk(rowId, count, true);
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.DOUBLE_ARRAY_OFFSET + srcIndex * 8L, null,
      data + rowId * 8L, count * 8L);
    setValidityBitsBulk(rowId, count, true);
  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, null,
      data + rowId * 8L, count * 8L);
    setValidityBitsBulk(rowId, count, true);
  }

  @Override
  public void putDoublesLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, null,
      data + rowId * 8L, count * 8L);
    setValidityBitsBulk(rowId, count, true);
  }

  @Override
  public double getDouble(int rowId) {
    return Platform.getDouble(null, data + rowId * 8L);
  }

  @Override
  public double[] getDoubles(int rowId, int count) {
    double[] array = new double[count];
    Platform.copyMemory(null, data + rowId * 8L, array, Platform.DOUBLE_ARRAY_OFFSET,
            count * 8L);
    return array;
  }

  //
  // Arrays unsupported
  //

  @Override
  public void putArray(int rowId, int offset, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int putByteArray(int rowId, byte[] value, int offset, int count) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void reserveInternal(int capacity) {
    baseFixedWidthVector.allocateNew(capacity);
    ArrowBuf dataBuf = valueVector.getDataBuffer();
    ArrowBuf validBuf = valueVector.getValidityBuffer();
    this.data = (dataBuf != null) ? dataBuf.memoryAddress() : 0L;
    this.validity = (validBuf != null) ? validBuf.memoryAddress() : 0L;
  }

  @Override
  public int getArrayLength(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getArrayOffset(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int numNulls() { return valueVector.getNullCount(); }

  @Override
  public boolean hasNull() { return valueVector.getNullCount() > 0; }

  @Override
  protected ArrowWritableColumnVector reserveNewColumn(int capacity, DataType type) {
    return ArrowWritableColumnVector.allocateColumns(
      capacity,
      new StructField[] { new StructField("dummy", type, true, Metadata.empty()) })[0];
  }
}
