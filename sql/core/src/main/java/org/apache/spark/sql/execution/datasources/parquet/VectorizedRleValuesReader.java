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
import java.nio.ByteBuffer;

import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;

import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

/**
 * A values reader for Parquet's run-length encoded data. This is based off of the version in
 * parquet-mr with these changes:
 *  - Supports the vectorized interface.
 *  - Works on byte arrays(byte[]) instead of making byte streams.
 *
 * This encoding is used in multiple places:
 *  - Definition/Repetition levels
 *  - Dictionary ids.
 */
public final class VectorizedRleValuesReader extends ValuesReader
    implements VectorizedValuesReader {
  // Current decoding mode. The encoded data contains groups of either run length encoded data
  // (RLE) or bit packed data. Each group contains a header that indicates which group it is and
  // the number of values in the group.
  // More details here: https://github.com/apache/parquet-format/blob/master/Encodings.md
  private enum MODE {
    RLE,
    PACKED
  }

  // Encoded data.
  private ByteBufferInputStream in;

  // bit/byte width of decoded data and utility to batch unpack them.
  private int bitWidth;
  private int bytesWidth;
  private BytePacker packer;

  // Current decoding mode and values
  private MODE mode;
  private int currentCount;
  private int currentValue;

  // Buffer of decoded values if the values are PACKED.
  private int[] currentBuffer = new int[16];
  private int currentBufferIdx = 0;

  // If true, the bit width is fixed. This decoder is used in different places and this also
  // controls if we need to read the bitwidth from the beginning of the data stream.
  private final boolean fixedWidth;
  private final boolean readLength;

  public VectorizedRleValuesReader() {
    this.fixedWidth = false;
    this.readLength = false;
  }

  public VectorizedRleValuesReader(int bitWidth) {
    this.fixedWidth = true;
    this.readLength = bitWidth != 0;
    init(bitWidth);
  }

  public VectorizedRleValuesReader(int bitWidth, boolean readLength) {
    this.fixedWidth = true;
    this.readLength = readLength;
    init(bitWidth);
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
    this.in = in;
    if (fixedWidth) {
      // initialize for repetition and definition levels
      if (readLength) {
        int length = readIntLittleEndian();
        this.in = in.sliceStream(length);
      }
    } else {
      // initialize for values
      if (in.available() > 0) {
        init(in.read());
      }
    }
    if (bitWidth == 0) {
      // 0 bit width, treat this as an RLE run of valueCount number of 0's.
      this.mode = MODE.RLE;
      this.currentCount = valueCount;
      this.currentValue = 0;
    } else {
      this.currentCount = 0;
    }
  }

  /**
   * Initializes the internal state for decoding ints of `bitWidth`.
   */
  private void init(int bitWidth) {
    Preconditions.checkArgument(bitWidth >= 0 && bitWidth <= 32, "bitWidth must be >= 0 and <= 32");
    this.bitWidth = bitWidth;
    this.bytesWidth = BytesUtils.paddedByteCountFromBits(bitWidth);
    this.packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
  }

  @Override
  public boolean readBoolean() {
    return this.readInteger() != 0;
  }

  @Override
  public void skip() {
    this.readInteger();
  }

  @Override
  public int readValueDictionaryId() {
    return readInteger();
  }

  @Override
  public int readInteger() {
    if (this.currentCount == 0) { this.readNextGroup(); }

    this.currentCount--;
    switch (mode) {
      case RLE:
        return this.currentValue;
      case PACKED:
        return this.currentBuffer[currentBufferIdx++];
    }
    throw new RuntimeException("Unreachable");
  }

  /**
   * Reads a batch of values into vector `values`, using `valueReader`. The related states such
   * as row index, offset, number of values left in the batch and page, etc, are tracked by
   * `state`. The type-specific `updater` is used to update or skip values.
   * <p>
   * This reader reads the definition levels and then will read from `valueReader` for the
   * non-null values. If the value is null, `values` will be populated with null value.
   */
  public void readBatch(
      ParquetReadState state,
      WritableColumnVector values,
      VectorizedValuesReader valueReader,
      ParquetVectorUpdater updater) {
    readBatchInternal(state, values, values, valueReader, updater);
  }

  /**
   * Decoding for dictionary ids. The IDs are populated into `values` and the nullability is
   * populated into `nulls`.
   */
  public void readIntegers(
      ParquetReadState state,
      WritableColumnVector values,
      WritableColumnVector nulls,
      VectorizedValuesReader data) {
    readBatchInternal(state, values, nulls, data, new ParquetVectorUpdaterFactory.IntegerUpdater());
  }

  private void readBatchInternal(
      ParquetReadState state,
      WritableColumnVector values,
      WritableColumnVector nulls,
      VectorizedValuesReader valueReader,
      ParquetVectorUpdater updater) {

    int offset = state.offset;
    long rowId = state.rowId;
    int leftInBatch = state.valuesToReadInBatch;
    int leftInPage = state.valuesToReadInPage;

    while (leftInBatch > 0 && leftInPage > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(leftInBatch, Math.min(leftInPage, this.currentCount));

      long rangeStart = state.currentRangeStart();
      long rangeEnd = state.currentRangeEnd();

      if (rowId + n < rangeStart) {
        updater.skipValues(n, valueReader);
        advance(n);
        rowId += n;
        leftInPage -= n;
      } else if (rowId > rangeEnd) {
        state.nextRange();
      } else {
        // the range [rowId, rowId + n) overlaps with the current row range in state
        long start = Math.max(rangeStart, rowId);
        long end = Math.min(rangeEnd, rowId + n - 1);

        // skip the part [rowId, start)
        int toSkip = (int) (start - rowId);
        if (toSkip > 0) {
          updater.skipValues(toSkip, valueReader);
          advance(toSkip);
          rowId += toSkip;
          leftInPage -= toSkip;
        }

        // read the part [start, end]
        n = (int) (end - start + 1);

        switch (mode) {
          case RLE:
            if (currentValue == state.maxDefinitionLevel) {
              updater.readValues(n, offset, values, valueReader);
            } else {
              nulls.putNulls(offset, n);
            }
            break;
          case PACKED:
            for (int i = 0; i < n; ++i) {
              if (currentBuffer[currentBufferIdx++] == state.maxDefinitionLevel) {
                updater.readValue(offset + i, values, valueReader);
              } else {
                nulls.putNull(offset + i);
              }
            }
            break;
        }
        offset += n;
        leftInBatch -= n;
        rowId += n;
        leftInPage -= n;
        currentCount -= n;
      }
    }

    state.advanceOffsetAndRowId(offset, rowId);
  }


  // The RLE reader implements the vectorized decoding interface when used to decode dictionary
  // IDs. This is different than the above APIs that decodes definitions levels along with values.
  // Since this is only used to decode dictionary IDs, only decoding integers is supported.
  @Override
  public void readIntegers(int total, WritableColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          c.putInts(rowId, n, currentValue);
          break;
        case PACKED:
          c.putInts(rowId, n, currentBuffer, currentBufferIdx);
          currentBufferIdx += n;
          break;
      }
      rowId += n;
      left -= n;
      currentCount -= n;
    }
  }

  @Override
  public void readUnsignedIntegers(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void readUnsignedLongs(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void readIntegersWithRebase(
      int total, WritableColumnVector c, int rowId, boolean failIfRebase) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public byte readByte() {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public short readShort() {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void readBytes(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void readShorts(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void readLongs(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void readLongsWithRebase(
      int total, WritableColumnVector c, int rowId, boolean failIfRebase) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void readBinary(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void readBooleans(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void readFloats(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void readDoubles(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public Binary readBinary(int len) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void skipIntegers(int total) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      advance(n);
      left -= n;
    }
  }

  @Override
  public void skipBooleans(int total) {
    throw new UnsupportedOperationException("only skipIntegers is valid");
  }

  @Override
  public void skipBytes(int total) {
    throw new UnsupportedOperationException("only skipIntegers is valid");
  }

  @Override
  public void skipShorts(int total) {
    throw new UnsupportedOperationException("only skipIntegers is valid");
  }

  @Override
  public void skipLongs(int total) {
    throw new UnsupportedOperationException("only skipIntegers is valid");
  }

  @Override
  public void skipFloats(int total) {
    throw new UnsupportedOperationException("only skipIntegers is valid");
  }

  @Override
  public void skipDoubles(int total) {
    throw new UnsupportedOperationException("only skipIntegers is valid");
  }

  @Override
  public void skipBinary(int total) {
    throw new UnsupportedOperationException("only skipIntegers is valid");
  }

  @Override
  public void skipFixedLenByteArray(int total, int len) {
    throw new UnsupportedOperationException("only skipIntegers is valid");
  }

  /**
   * Advance and skip the next `n` values in the current block. `n` MUST be <= `currentCount`.
   */
  private void advance(int n) {
    switch (mode) {
      case RLE:
        break;
      case PACKED:
        currentBufferIdx += n;
        break;
    }
    currentCount -= n;
  }

  /**
   * Reads the next varint encoded int.
   */
  private int readUnsignedVarInt() throws IOException {
    int value = 0;
    int shift = 0;
    int b;
    do {
      b = in.read();
      value |= (b & 0x7F) << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    return value;
  }

  /**
   * Reads the next 4 byte little endian int.
   */
  private int readIntLittleEndian() throws IOException {
    int ch4 = in.read();
    int ch3 = in.read();
    int ch2 = in.read();
    int ch1 = in.read();
    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
  }

  /**
   * Reads the next byteWidth little endian int.
   */
  private int readIntLittleEndianPaddedOnBitWidth() throws IOException {
    switch (bytesWidth) {
      case 0:
        return 0;
      case 1:
        return in.read();
      case 2: {
        int ch2 = in.read();
        int ch1 = in.read();
        return (ch1 << 8) + ch2;
      }
      case 3: {
        int ch3 = in.read();
        int ch2 = in.read();
        int ch1 = in.read();
        return (ch1 << 16) + (ch2 << 8) + (ch3 << 0);
      }
      case 4: {
        return readIntLittleEndian();
      }
    }
    throw new RuntimeException("Unreachable");
  }

  /**
   * Reads the next group.
   */
  private void readNextGroup() {
    try {
      int header = readUnsignedVarInt();
      this.mode = (header & 1) == 0 ? MODE.RLE : MODE.PACKED;
      switch (mode) {
        case RLE:
          this.currentCount = header >>> 1;
          this.currentValue = readIntLittleEndianPaddedOnBitWidth();
          return;
        case PACKED:
          int numGroups = header >>> 1;
          this.currentCount = numGroups * 8;

          if (this.currentBuffer.length < this.currentCount) {
            this.currentBuffer = new int[this.currentCount];
          }
          currentBufferIdx = 0;
          int valueIndex = 0;
          while (valueIndex < this.currentCount) {
            // values are bit packed 8 at a time, so reading bitWidth will always work
            ByteBuffer buffer = in.slice(bitWidth);
            this.packer.unpack8Values(buffer, buffer.position(), this.currentBuffer, valueIndex);
            valueIndex += 8;
          }
          return;
        default:
          throw new ParquetDecodingException("not a valid mode " + this.mode);
      }
    } catch (IOException e) {
      throw new ParquetDecodingException("Failed to read from input stream", e);
    }
  }
}
