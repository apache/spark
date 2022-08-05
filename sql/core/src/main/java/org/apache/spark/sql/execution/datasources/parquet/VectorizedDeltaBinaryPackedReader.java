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
import java.util.Arrays;

import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.bitpacking.BytePackerForLong;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.spark.sql.catalyst.util.RebaseDateTime;
import org.apache.spark.sql.execution.datasources.DataSourceUtils;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

/**
 * An implementation of the Parquet DELTA_BINARY_PACKED decoder that supports the vectorized
 * interface. DELTA_BINARY_PACKED is a delta encoding for integer and long types that stores values
 * as a delta between consecutive values. Delta values are themselves bit packed. Similar to RLE but
 * is more effective in the case of large variation of values in the encoded column.
 * <p>
 * DELTA_BINARY_PACKED is the default encoding for integer and long columns in Parquet V2.
 * <p>
 * Supported Types: INT32, INT64
 * <p>
 *
 * @see <a href="https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-encoding-delta_binary_packed--5">
 * Parquet format encodings: DELTA_BINARY_PACKED</a>
 */
public class VectorizedDeltaBinaryPackedReader extends VectorizedReaderBase {

  // header data
  private int blockSizeInValues;
  private int miniBlockNumInABlock;
  private int totalValueCount;
  private long firstValue;

  private int miniBlockSizeInValues;

  // values read by the caller
  private int valuesRead = 0;

  // variables to keep state of the current block and miniblock
  private long lastValueRead;  // needed to compute the next value
  private long minDeltaInCurrentBlock; // needed to compute the next value
  // currentMiniBlock keeps track of the mini block within the current block that
  // we read and decoded most recently. Only used as an index into
  // bitWidths array
  private int currentMiniBlock = 0;
  private int[] bitWidths; // bit widths for each miniBlock in the current block
  private int remainingInBlock = 0; // values in current block still to be read
  private int remainingInMiniBlock = 0; // values in current mini block still to be read
  private long[] unpackedValuesBuffer;

  private ByteBufferInputStream in;

  // temporary buffers used by readByte, readShort, readInteger, and readLong
  private byte byteVal;
  private short shortVal;
  private int intVal;
  private long longVal;

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
    Preconditions.checkArgument(valueCount >= 1,
        "Page must have at least one value, but it has " + valueCount);
    this.in = in;
    // Read the header
    this.blockSizeInValues = BytesUtils.readUnsignedVarInt(in);
    this.miniBlockNumInABlock = BytesUtils.readUnsignedVarInt(in);
    double miniSize = (double) blockSizeInValues / miniBlockNumInABlock;
    Preconditions.checkArgument(miniSize % 8 == 0,
        "miniBlockSize must be multiple of 8, but it's " + miniSize);
    this.miniBlockSizeInValues = (int) miniSize;
    // True value count. May be less than valueCount because of nulls
    this.totalValueCount = BytesUtils.readUnsignedVarInt(in);
    this.bitWidths = new int[miniBlockNumInABlock];
    this.unpackedValuesBuffer = new long[miniBlockSizeInValues];
    // read the first value
    firstValue = BytesUtils.readZigZagVarLong(in);
  }

  // True value count. May be less than valueCount because of nulls
  int getTotalValueCount() {
    return totalValueCount;
  }

  @Override
  public byte readByte() {
    readValues(1, null, 0, (w, r, v) -> byteVal = (byte) v);
    return byteVal;
  }

  @Override
  public short readShort() {
    readValues(1, null, 0, (w, r, v) -> shortVal = (short) v);
    return shortVal;
  }

  @Override
  public int readInteger() {
    readValues(1, null, 0, (w, r, v) -> intVal = (int) v);
    return intVal;
  }

  @Override
  public long readLong() {
    readValues(1, null, 0, (w, r, v) -> longVal = v);
    return longVal;
  }

  @Override
  public void readBytes(int total, WritableColumnVector c, int rowId) {
    readValues(total, c, rowId, (w, r, v) -> w.putByte(r, (byte) v));
  }

  @Override
  public void readShorts(int total, WritableColumnVector c, int rowId) {
    readValues(total, c, rowId, (w, r, v) -> w.putShort(r, (short) v));
  }

  @Override
  public void readIntegers(int total, WritableColumnVector c, int rowId) {
    readValues(total, c, rowId, (w, r, v) -> w.putInt(r, (int) v));
  }

  // Based on VectorizedPlainValuesReader.readIntegersWithRebase
  @Override
  public final void readIntegersWithRebase(
      int total, WritableColumnVector c, int rowId, boolean failIfRebase) {
    readValues(total, c, rowId, (w, r, v) -> {
      if (v < RebaseDateTime.lastSwitchJulianDay()) {
        if (failIfRebase) {
          throw DataSourceUtils.newRebaseExceptionInRead("Parquet");
        } else {
          w.putInt(r, RebaseDateTime.rebaseJulianToGregorianDays((int) v));
        }
      } else {
        w.putInt(r, (int) v);
      }
    });
  }

  @Override
  public void readUnsignedIntegers(int total, WritableColumnVector c, int rowId) {
    readValues(total, c, rowId, (w, r, v) -> {
      w.putLong(r, Integer.toUnsignedLong((int) v));
    });
  }

  @Override
  public void readUnsignedLongs(int total, WritableColumnVector c, int rowId) {
    readValues(total, c, rowId, (w, r, v) -> {
      w.putByteArray(r, new BigInteger(Long.toUnsignedString(v)).toByteArray());
    });
  }

  @Override
  public void readLongs(int total, WritableColumnVector c, int rowId) {
    readValues(total, c, rowId, WritableColumnVector::putLong);
  }

  @Override
  public final void readLongsWithRebase(
      int total, WritableColumnVector c, int rowId, boolean failIfRebase, String timeZone) {
    readValues(total, c, rowId, (w, r, v) -> {
      if (v < RebaseDateTime.lastSwitchJulianTs()) {
        if (failIfRebase) {
          throw DataSourceUtils.newRebaseExceptionInRead("Parquet");
        } else {
          w.putLong(r, RebaseDateTime.rebaseJulianToGregorianMicros(timeZone, v));
        }
      } else {
        w.putLong(r, v);
      }
    });
  }

  @Override
  public void skipBytes(int total) {
    skipValues(total);
  }

  @Override
  public void skipShorts(int total) {
    skipValues(total);
  }

  @Override
  public void skipIntegers(int total) {
    skipValues(total);
  }

  @Override
  public void skipLongs(int total) {
    skipValues(total);
  }

  private void readValues(int total, WritableColumnVector c, int rowId,
      IntegerOutputWriter outputWriter) {
    if (valuesRead + total > totalValueCount) {
      throw new ParquetDecodingException(
          "No more values to read. Total values read:  " + valuesRead + ", total count: "
              + totalValueCount + ", trying to read " + total + " more.");
    }
    int remaining = total;
    // First value
    if (valuesRead == 0) {
      outputWriter.write(c, rowId, firstValue);
      lastValueRead = firstValue;
      rowId++;
      remaining--;
    }
    while (remaining > 0) {
      int n;
      try {
        n = loadMiniBlockToOutput(remaining, c, rowId, outputWriter);
      } catch (IOException e) {
        throw new ParquetDecodingException("Error reading mini block.", e);
      }
      rowId += n;
      remaining -= n;
    }
    valuesRead = total - remaining;
  }


  /**
   * Read from a mini block.  Read at most 'remaining' values into output.
   *
   * @return the number of values read into output
   */
  private int loadMiniBlockToOutput(int remaining, WritableColumnVector c, int rowId,
      IntegerOutputWriter outputWriter) throws IOException {

    // new block; read the block header
    if (remainingInBlock == 0) {
      readBlockHeader();
    }

    // new miniblock, unpack the miniblock
    if (remainingInMiniBlock == 0) {
      unpackMiniBlock();
    }

    // read values from miniblock
    int valuesRead = 0;
    for (int i = miniBlockSizeInValues - remainingInMiniBlock;
        i < miniBlockSizeInValues && valuesRead < remaining; i++) {
      // calculate values from deltas unpacked for current block
      long outValue = lastValueRead + minDeltaInCurrentBlock + unpackedValuesBuffer[i];
      lastValueRead = outValue;
      outputWriter.write(c, rowId + valuesRead, outValue);
      remainingInBlock--;
      remainingInMiniBlock--;
      valuesRead++;
    }

    return valuesRead;
  }

  private void readBlockHeader() {
    try {
      minDeltaInCurrentBlock = BytesUtils.readZigZagVarLong(in);
    } catch (IOException e) {
      throw new ParquetDecodingException("Can not read min delta in current block", e);
    }
    readBitWidthsForMiniBlocks();
    remainingInBlock = blockSizeInValues;
    currentMiniBlock = 0;
    remainingInMiniBlock = 0;
  }

  /**
   * mini block has a size of 8*n, unpack 32 value each time
   *
   * see org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader#unpackMiniBlock
   */
  private void unpackMiniBlock() throws IOException {
    Arrays.fill(this.unpackedValuesBuffer, 0);
    BytePackerForLong packer = Packer.LITTLE_ENDIAN.newBytePackerForLong(
        bitWidths[currentMiniBlock]);
    for (int j = 0; j < miniBlockSizeInValues; j += 8) {
      ByteBuffer buffer = in.slice(packer.getBitWidth());
      packer.unpack8Values(buffer.array(),
        buffer.arrayOffset() + buffer.position(), unpackedValuesBuffer, j);
    }
    remainingInMiniBlock = miniBlockSizeInValues;
    currentMiniBlock++;
  }

  // From org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader
  private void readBitWidthsForMiniBlocks() {
    for (int i = 0; i < miniBlockNumInABlock; i++) {
      try {
        bitWidths[i] = BytesUtils.readIntLittleEndianOnOneByte(in);
      } catch (IOException e) {
        throw new ParquetDecodingException("Can not decode bitwidth in block header", e);
      }
    }
  }

  private void skipValues(int total) {
    // Read the values but don't write them out (the writer output method is a no-op)
    readValues(total, null, -1, (w, r, v) -> {});
  }

}
