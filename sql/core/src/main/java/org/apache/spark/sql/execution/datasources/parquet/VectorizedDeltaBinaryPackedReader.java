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

import java.nio.ByteBuffer;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.bitpacking.BytePackerForLong;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

/**
 * An implementation of the Parquet DELTA_BINARY_PACKED decoder that supports the vectorized
 * interface.
 */
public class VectorizedDeltaBinaryPackedReader extends ValuesReader
    implements VectorizedValuesReader {

  // header data
  private int blockSizeInValues;
  private int miniBlockNumInABlock;
  private int totalValueCount;
  private long firstValue;

  private int miniBlockSizeInValues;

  // values read by the caller
  private int valuesRead = 0;

  //variables to keep state of the current block and miniblock
  private long lastValueRead;
  private long minDeltaInCurrentBlock;
  private int currentMiniBlock = 0;
  private int[] bitWidths; // bit widths for each miniblock in the current block
  private int remainingInBlock = 0; // values in current block still to be read
  private int remainingInMiniBlock = 0; // values in current mini block still to be read
  private long[] unpackedValuesBuffer;

  private ByteBufferInputStream in;

  @SuppressWarnings("unused")
  @Override
  public void initFromPage(/*unused*/int valueCount, ByteBufferInputStream in) throws IOException {
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
    this.totalValueCount = BytesUtils.readUnsignedVarInt(in);
    this.bitWidths = new int[miniBlockNumInABlock];

    // read the first value
    firstValue = BytesUtils.readZigZagVarLong(in);

  }

  @Override
  public void skip() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte readByte() {
    throw new UnsupportedOperationException();
  }

  @Override
  public short readShort() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Binary readBinary(int len) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readBooleans(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readBytes(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readShorts(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readIntegers(int total, WritableColumnVector c, int rowId) {
    readValues(total, c, rowId, (w, r, v) -> {
      c.putInt(r, (int) v);
    });
  }

  @Override
  public void readIntegersWithRebase(int total, WritableColumnVector c, int rowId,
      boolean failIfRebase) {
    throw new UnsupportedOperationException("Only readIntegers is valid.");
  }

  @Override
  public void readUnsignedIntegers(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readUnsignedLongs(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readLongs(int total, WritableColumnVector c, int rowId) {
    readValues(total, c, rowId, WritableColumnVector::putLong);
  }

  @Override
  public void readLongsWithRebase(int total, WritableColumnVector c, int rowId,
      boolean failIfRebase) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readFloats(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readDoubles(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readBinary(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void skipBooleans(int total) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void skipBytes(int total) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void skipShorts(int total) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void skipIntegers(int total) {
    // Read the values but don't write them out (the writer output method is a no-op)
    readValues(total, null, -1, (w, r, v) -> {
    });
  }

  @Override
  public void skipLongs(int total) {
    // Read the values but don't write them out (the writer output method is a no-op)
    readValues(total, null, -1, (w, r, v) -> {
    });
  }

  @Override
  public void skipFloats(int total) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void skipDoubles(int total) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void skipBinary(int total) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void skipFixedLenByteArray(int total, int len) {
    throw new UnsupportedOperationException();
  }

  private void readValues(int total, WritableColumnVector c, int rowId,
      IntegerOutputWriter outputWriter) {
    int remaining = total;
    if (valuesRead + total > totalValueCount) {
      throw new ParquetDecodingException(
          "no more values to read, total value count is " + valuesRead);
    }
    // First value
    if (valuesRead == 0) {
      //c.putInt(rowId, (int)firstValue);
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

    //read values from miniblock
    int valuesRead = 0;
    for (int i = miniBlockSizeInValues - remainingInMiniBlock;
        i < miniBlockSizeInValues && valuesRead < remaining; i++) {
      //calculate values from deltas unpacked for current block
      long outValue = lastValueRead + minDeltaInCurrentBlock + unpackedValuesBuffer[i];
      lastValueRead = outValue;
      outputWriter.write(c, rowId + valuesRead, outValue);
      remaining--;
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
      throw new ParquetDecodingException("can not read min delta in current block", e);
    }
    readBitWidthsForMiniBlocks();
    remainingInBlock = blockSizeInValues;
    currentMiniBlock = 0;
    remainingInMiniBlock = 0;
  }

  /**
   * mini block has a size of 8*n, unpack 8 value each time
   * @see org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader#unpackMiniBlock
   */
  private void unpackMiniBlock() throws IOException {
    this.unpackedValuesBuffer = new long[miniBlockSizeInValues];
    BytePackerForLong packer = Packer.LITTLE_ENDIAN.newBytePackerForLong(
        bitWidths[currentMiniBlock]);
    for (int j = 0; j < miniBlockSizeInValues; j += 8) {
      ByteBuffer buffer = in.slice(packer.getBitWidth());
      packer.unpack8Values(buffer, buffer.position(), unpackedValuesBuffer, j);
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

}
