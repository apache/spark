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
package org.apache.spark.sql.execution.datasources.parquet;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.ParquetDecodingException;

import org.apache.spark.sql.catalyst.util.RebaseDateTime;
import org.apache.spark.sql.execution.datasources.DataSourceUtils;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

/**
 * An implementation of the Parquet PLAIN decoder that supports the vectorized interface.
 */
public class VectorizedPlainValuesReader extends ValuesReader implements VectorizedValuesReader {
  private ByteBufferInputStream in = null;

  // Only used for booleans.
  private int bitOffset;
  private byte currentByte = 0;

  public VectorizedPlainValuesReader() {
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
    this.in = in;
  }

  @Override
  public void skip() {
    throw new UnsupportedOperationException();
  }

  private void updateCurrentByte() {
    try {
      currentByte = (byte) in.read();
    } catch (IOException e) {
      throw new ParquetDecodingException("Failed to read a byte", e);
    }
  }

  @Override
  public final void readBooleans(int total, WritableColumnVector c, int rowId) {
    int i = 0;
    if (bitOffset > 0) {
      i = Math.min(8 - bitOffset, total);
      c.putBooleans(rowId, i, currentByte, bitOffset);
      bitOffset = (bitOffset + i) & 7;
    }
    for (; i + 7 < total; i += 8) {
      updateCurrentByte();
      c.putBooleans(rowId + i, currentByte);
    }
    if (i < total) {
      updateCurrentByte();
      bitOffset = total - i;
      c.putBooleans(rowId + i, bitOffset, currentByte, 0);
    }
  }

  @Override
  public final void skipBooleans(int total) {
    int i = 0;
    if (bitOffset > 0) {
      i = Math.min(8 - bitOffset, total);
      bitOffset = (bitOffset + i) & 7;
    }
    if (i + 7 < total) {
      int numBytesToSkip = (total - i) / 8;
      try {
        in.skipFully(numBytesToSkip);
      } catch (IOException e) {
        throw new ParquetDecodingException("Failed to skip bytes", e);
      }
      i += numBytesToSkip * 8;
    }
    if (i < total) {
      updateCurrentByte();
      bitOffset = total - i;
    }
  }

  private ByteBuffer getBuffer(int length) {
    try {
      return in.slice(length).order(ByteOrder.LITTLE_ENDIAN);
    } catch (IOException e) {
      throw new ParquetDecodingException("Failed to read " + length + " bytes", e);
    }
  }

  @Override
  public final void readIntegers(int total, WritableColumnVector c, int rowId) {
    int requiredBytes = total * 4;
    ByteBuffer buffer = getBuffer(requiredBytes);

    if (buffer.hasArray()) {
      int offset = buffer.arrayOffset() + buffer.position();
      c.putIntsLittleEndian(rowId, total, buffer.array(), offset);
    } else {
      for (int i = 0; i < total; i += 1) {
        c.putInt(rowId + i, buffer.getInt());
      }
    }
  }

  @Override
  public void skipIntegers(int total) {
    in.skip(total * 4L);
  }

  @Override
  public final void readUnsignedIntegers(int total, WritableColumnVector c, int rowId) {
    int requiredBytes = total * 4;
    ByteBuffer buffer = getBuffer(requiredBytes);
    for (int i = 0; i < total; i += 1) {
      c.putLong(rowId + i, Integer.toUnsignedLong(buffer.getInt()));
    }
  }

  // A fork of `readIntegers` to rebase the date values. For performance reasons, this method
  // iterates the values twice: check if we need to rebase first, then go to the optimized branch
  // if rebase is not needed.
  @Override
  public final void readIntegersWithRebase(
      int total, WritableColumnVector c, int rowId, boolean failIfRebase) {
    int requiredBytes = total * 4;
    ByteBuffer buffer = getBuffer(requiredBytes);
    boolean rebase = false;
    for (int i = 0; i < total; i += 1) {
      rebase |= buffer.getInt(buffer.position() + i * 4) < RebaseDateTime.lastSwitchJulianDay();
    }
    if (rebase) {
      if (failIfRebase) {
        throw DataSourceUtils.newRebaseExceptionInRead("Parquet");
      } else {
        for (int i = 0; i < total; i += 1) {
          c.putInt(rowId + i, RebaseDateTime.rebaseJulianToGregorianDays(buffer.getInt()));
        }
      }
    } else {
      if (buffer.hasArray()) {
        int offset = buffer.arrayOffset() + buffer.position();
        c.putIntsLittleEndian(rowId, total, buffer.array(), offset);
      } else {
        for (int i = 0; i < total; i += 1) {
          c.putInt(rowId + i, buffer.getInt());
        }
      }
    }
  }

  @Override
  public final void readLongs(int total, WritableColumnVector c, int rowId) {
    int requiredBytes = total * 8;
    ByteBuffer buffer = getBuffer(requiredBytes);

    if (buffer.hasArray()) {
      int offset = buffer.arrayOffset() + buffer.position();
      c.putLongsLittleEndian(rowId, total, buffer.array(), offset);
    } else {
      for (int i = 0; i < total; i += 1) {
        c.putLong(rowId + i, buffer.getLong());
      }
    }
  }

  @Override
  public void skipLongs(int total) {
    in.skip(total * 8L);
  }

  @Override
  public final void readUnsignedLongs(int total, WritableColumnVector c, int rowId) {
    int requiredBytes = total * 8;
    ByteBuffer buffer = getBuffer(requiredBytes);
    for (int i = 0; i < total; i += 1) {
      c.putByteArray(
        rowId + i, new BigInteger(Long.toUnsignedString(buffer.getLong())).toByteArray());
    }
  }

  // A fork of `readLongs` to rebase the timestamp values. For performance reasons, this method
  // iterates the values twice: check if we need to rebase first, then go to the optimized branch
  // if rebase is not needed.
  @Override
  public final void readLongsWithRebase(
      int total,
      WritableColumnVector c,
      int rowId,
      boolean failIfRebase,
      String timeZone) {
    int requiredBytes = total * 8;
    ByteBuffer buffer = getBuffer(requiredBytes);
    boolean rebase = false;
    for (int i = 0; i < total; i += 1) {
      rebase |= buffer.getLong(buffer.position() + i * 8) < RebaseDateTime.lastSwitchJulianTs();
    }
    if (rebase) {
      if (failIfRebase) {
        throw DataSourceUtils.newRebaseExceptionInRead("Parquet");
      } else {
        for (int i = 0; i < total; i += 1) {
          c.putLong(
            rowId + i,
            RebaseDateTime.rebaseJulianToGregorianMicros(timeZone, buffer.getLong()));
        }
      }
    } else {
      if (buffer.hasArray()) {
        int offset = buffer.arrayOffset() + buffer.position();
        c.putLongsLittleEndian(rowId, total, buffer.array(), offset);
      } else {
        for (int i = 0; i < total; i += 1) {
          c.putLong(rowId + i, buffer.getLong());
        }
      }
    }
  }

  @Override
  public final void readFloats(int total, WritableColumnVector c, int rowId) {
    int requiredBytes = total * 4;
    ByteBuffer buffer = getBuffer(requiredBytes);

    if (buffer.hasArray()) {
      int offset = buffer.arrayOffset() + buffer.position();
      c.putFloatsLittleEndian(rowId, total, buffer.array(), offset);
    } else {
      for (int i = 0; i < total; i += 1) {
        c.putFloat(rowId + i, buffer.getFloat());
      }
    }
  }

  @Override
  public void skipFloats(int total) {
    in.skip(total * 4L);
  }

  @Override
  public final void readDoubles(int total, WritableColumnVector c, int rowId) {
    int requiredBytes = total * 8;
    ByteBuffer buffer = getBuffer(requiredBytes);

    if (buffer.hasArray()) {
      int offset = buffer.arrayOffset() + buffer.position();
      c.putDoublesLittleEndian(rowId, total, buffer.array(), offset);
    } else {
      for (int i = 0; i < total; i += 1) {
        c.putDouble(rowId + i, buffer.getDouble());
      }
    }
  }

  @Override
  public void skipDoubles(int total) {
    in.skip(total * 8L);
  }

  @Override
  public final void readBytes(int total, WritableColumnVector c, int rowId) {
    // Bytes are stored as a 4-byte little endian int. Just read the first byte.
    // TODO: consider pushing this in ColumnVector by adding a readBytes with a stride.
    int requiredBytes = total * 4;
    ByteBuffer buffer = getBuffer(requiredBytes);

    for (int i = 0; i < total; i += 1) {
      c.putByte(rowId + i, buffer.get());
      // skip the next 3 bytes
      buffer.position(buffer.position() + 3);
    }
  }

  @Override
  public final void skipBytes(int total) {
    in.skip(total * 4L);
  }

  @Override
  public final void readShorts(int total, WritableColumnVector c, int rowId) {
    int requiredBytes = total * 4;
    ByteBuffer buffer = getBuffer(requiredBytes);

    for (int i = 0; i < total; i += 1) {
      c.putShort(rowId + i, (short) buffer.getInt());
    }
  }

  @Override
  public void skipShorts(int total) {
    in.skip(total * 4L);
  }

  @Override
  public final boolean readBoolean() {
    if (bitOffset == 0) {
      updateCurrentByte();
    }

    boolean v = (currentByte & (1 << bitOffset)) != 0;
    bitOffset += 1;
    if (bitOffset == 8) {
      bitOffset = 0;
    }
    return v;
  }

  @Override
  public final int readInteger() {
    return getBuffer(4).getInt();
  }

  @Override
  public final long readLong() {
    return getBuffer(8).getLong();
  }

  @Override
  public final byte readByte() {
    return (byte) readInteger();
  }

  @Override
  public short readShort() {
    return (short) readInteger();
  }

  @Override
  public final float readFloat() {
    return getBuffer(4).getFloat();
  }

  @Override
  public final double readDouble() {
    return getBuffer(8).getDouble();
  }

  @Override
  public final void readBinary(int total, WritableColumnVector v, int rowId) {
    for (int i = 0; i < total; i++) {
      int len = readInteger();
      ByteBuffer buffer = getBuffer(len);
      if (buffer.hasArray()) {
        v.putByteArray(rowId + i, buffer.array(), buffer.arrayOffset() + buffer.position(), len);
      } else {
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        v.putByteArray(rowId + i, bytes);
      }
    }
  }

  @Override
  public void skipBinary(int total) {
    for (int i = 0; i < total; i++) {
      int len = readInteger();
      in.skip(len);
    }
  }

  @Override
  public final Binary readBinary(int len) {
    ByteBuffer buffer = getBuffer(len);
    if (buffer.hasArray()) {
      return Binary.fromConstantByteArray(
          buffer.array(), buffer.arrayOffset() + buffer.position(), len);
    } else {
      byte[] bytes = new byte[len];
      buffer.get(bytes);
      return Binary.fromConstantByteArray(bytes);
    }
  }

  @Override
  public void skipFixedLenByteArray(int total, int len) {
    in.skip(total * (long) len);
  }
}
