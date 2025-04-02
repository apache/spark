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

import org.apache.spark.SparkUnsupportedOperationException;
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
 *  - Boolean type values of Parquet DataPageV2
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
      // Initialize for repetition and definition levels
      if (readLength) {
        int length = readIntLittleEndian();
        this.in = in.sliceStream(length);
      }
    } else {
      // Initialize for values
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
    return switch (mode) {
      case RLE -> this.currentValue;
      case PACKED -> this.currentBuffer[currentBufferIdx++];
    };
  }

  /**
   * Reads a batch of definition levels and values into vector 'defLevels' and 'values'
   * respectively. The values are read using 'valueReader'.
   * <p>
   * The related states such as row index, offset, number of values left in the batch and page,
   * are tracked by 'state'. The type-specific 'updater' is used to update or skip values.
   * <p>
   * This reader reads the definition levels and then will read from 'valueReader' for the
   * non-null values. If the value is null, 'values' will be populated with null value.
   */
  public void readBatch(
      ParquetReadState state,
      WritableColumnVector values,
      WritableColumnVector defLevels,
      VectorizedValuesReader valueReader,
      ParquetVectorUpdater updater) {
    if (defLevels == null) {
      readBatchInternal(state, values, values, valueReader, updater);
    } else {
      readBatchInternalWithDefLevels(state, values, values, defLevels, valueReader, updater);
    }
  }

  /**
   * Decoding for dictionary ids. The IDs are populated into 'values' and the nullability is
   * populated into 'nulls'.
   */
  public void readIntegers(
      ParquetReadState state,
      WritableColumnVector values,
      WritableColumnVector nulls,
      WritableColumnVector defLevels,
      VectorizedValuesReader valueReader) {
    if (defLevels == null) {
      readBatchInternal(state, values, nulls, valueReader,
        new ParquetVectorUpdaterFactory.IntegerUpdater());
    } else {
      readBatchInternalWithDefLevels(state, values, nulls, defLevels, valueReader,
        new ParquetVectorUpdaterFactory.IntegerUpdater());
    }
  }

  private void readBatchInternal(
      ParquetReadState state,
      WritableColumnVector values,
      WritableColumnVector nulls,
      VectorizedValuesReader valueReader,
      ParquetVectorUpdater updater) {

    long rowId = state.rowId;
    int leftInBatch = state.rowsToReadInBatch;
    int leftInPage = state.valuesToReadInPage;

    while (leftInBatch > 0 && leftInPage > 0) {
      if (currentCount == 0 && !readNextGroup()) break;
      int n = Math.min(leftInBatch, Math.min(leftInPage, this.currentCount));

      long rangeStart = state.currentRangeStart();
      long rangeEnd = state.currentRangeEnd();

      if (rowId + n < rangeStart) {
        skipValues(n, state, valueReader, updater);
        rowId += n;
        leftInPage -= n;
      } else if (rowId > rangeEnd) {
        state.nextRange();
      } else {
        // The range [rowId, rowId + n) overlaps with the current row range in state
        long start = Math.max(rangeStart, rowId);
        long end = Math.min(rangeEnd, rowId + n - 1);

        // Skip the part [rowId, start)
        int toSkip = (int) (start - rowId);
        if (toSkip > 0) {
          skipValues(toSkip, state, valueReader, updater);
          rowId += toSkip;
          leftInPage -= toSkip;
        }

        // Read the part [start, end]
        n = (int) (end - start + 1);

        switch (mode) {
          case RLE -> {
            if (currentValue == state.maxDefinitionLevel) {
              updater.readValues(n, state.valueOffset, values, valueReader);
            } else {
              nulls.putNulls(state.valueOffset, n);
            }
            state.valueOffset += n;
          }
          case PACKED -> {
            for (int i = 0; i < n; ++i) {
              int currentValue = currentBuffer[currentBufferIdx++];
              if (currentValue == state.maxDefinitionLevel) {
                updater.readValue(state.valueOffset++, values, valueReader);
              } else {
                nulls.putNull(state.valueOffset++);
              }
            }
          }
        }
        state.levelOffset += n;
        leftInBatch -= n;
        rowId += n;
        leftInPage -= n;
        currentCount -= n;
      }
    }

    state.rowsToReadInBatch = leftInBatch;
    state.valuesToReadInPage = leftInPage;
    state.rowId = rowId;
  }

  private void readBatchInternalWithDefLevels(
      ParquetReadState state,
      WritableColumnVector values,
      WritableColumnVector nulls,
      WritableColumnVector defLevels,
      VectorizedValuesReader valueReader,
      ParquetVectorUpdater updater) {

    long rowId = state.rowId;
    int leftInBatch = state.rowsToReadInBatch;
    int leftInPage = state.valuesToReadInPage;

    while (leftInBatch > 0 && leftInPage > 0) {
      if (currentCount == 0 && !readNextGroup()) break;
      int n = Math.min(leftInBatch, Math.min(leftInPage, this.currentCount));

      long rangeStart = state.currentRangeStart();
      long rangeEnd = state.currentRangeEnd();

      if (rowId + n < rangeStart) {
        skipValues(n, state, valueReader, updater);
        rowId += n;
        leftInPage -= n;
      } else if (rowId > rangeEnd) {
        state.nextRange();
      } else {
        // The range [rowId, rowId + n) overlaps with the current row range in state
        long start = Math.max(rangeStart, rowId);
        long end = Math.min(rangeEnd, rowId + n - 1);

        // Skip the part [rowId, start)
        int toSkip = (int) (start - rowId);
        if (toSkip > 0) {
          skipValues(toSkip, state, valueReader, updater);
          rowId += toSkip;
          leftInPage -= toSkip;
        }

        // Read the part [start, end]
        n = (int) (end - start + 1);
        readValuesN(n, state, defLevels, values, nulls, valueReader, updater);

        state.levelOffset += n;
        leftInBatch -= n;
        rowId += n;
        leftInPage -= n;
        currentCount -= n;
        defLevels.addElementsAppended(n);
      }
    }

    state.rowsToReadInBatch = leftInBatch;
    state.valuesToReadInPage = leftInPage;
    state.rowId = rowId;
  }

  /**
   * Reads a batch of repetition levels, definition levels and values into 'repLevels',
   * 'defLevels' and 'values' respectively. The definition levels and values are read via
   * 'defLevelsReader' and 'valueReader' respectively.
   * <p>
   * The related states such as row index, offset, number of rows left in the batch and page,
   * are tracked by 'state'. The type-specific 'updater' is used to update or skip values.
   */
  public void readBatchRepeated(
      ParquetReadState state,
      WritableColumnVector repLevels,
      VectorizedRleValuesReader defLevelsReader,
      WritableColumnVector defLevels,
      WritableColumnVector values,
      VectorizedValuesReader valueReader,
      ParquetVectorUpdater updater) {
    readBatchRepeatedInternal(state, repLevels, defLevelsReader, defLevels, values, values, true,
      valueReader, updater);
  }

  /**
   * Reads a batch of repetition levels, definition levels and integer values into 'repLevels',
   * 'defLevels', 'values' and 'nulls' respectively. The definition levels and values are read via
   * 'defLevelsReader' and 'valueReader' respectively.
   * <p>
   * The 'values' vector is used to hold non-null values, while 'nulls' vector is used to hold
   * null values.
   * <p>
   * The related states such as row index, offset, number of rows left in the batch and page,
   * are tracked by 'state'.
   * <p>
   * Unlike 'readBatchRepeated', this is used to decode dictionary indices in dictionary encoding.
   */
  public void readIntegersRepeated(
      ParquetReadState state,
      WritableColumnVector repLevels,
      VectorizedRleValuesReader defLevelsReader,
      WritableColumnVector defLevels,
      WritableColumnVector values,
      WritableColumnVector nulls,
      VectorizedValuesReader valueReader) {
    readBatchRepeatedInternal(state, repLevels, defLevelsReader, defLevels, values, nulls, false,
      valueReader, new ParquetVectorUpdaterFactory.IntegerUpdater());
  }

  /**
   * Keep reading repetition level values from the page until either: 1) we've read enough
   * top-level rows to fill the current batch, or 2) we've drained the data page completely.
   *
   * @param valuesReused whether 'values' vector is reused for 'nulls'
   */
  public void readBatchRepeatedInternal(
      ParquetReadState state,
      WritableColumnVector repLevels,
      VectorizedRleValuesReader defLevelsReader,
      WritableColumnVector defLevels,
      WritableColumnVector values,
      WritableColumnVector nulls,
      boolean valuesReused,
      VectorizedValuesReader valueReader,
      ParquetVectorUpdater updater) {

    int leftInBatch = state.rowsToReadInBatch;
    int leftInPage = state.valuesToReadInPage;
    long rowId = state.rowId;

    DefLevelProcessor defLevelProcessor = new DefLevelProcessor(defLevelsReader, state, defLevels,
      values, nulls, valuesReused, valueReader, updater);

    while ((leftInBatch > 0 || !state.lastListCompleted) && leftInPage > 0) {
      if (currentCount == 0 && !readNextGroup()) break;

      // Values to read in the current RLE/PACKED block, must be <= what's left in the page
      int valuesLeftInBlock = Math.min(leftInPage, currentCount);

      // The current row range start and end
      long rangeStart = state.currentRangeStart();
      long rangeEnd = state.currentRangeEnd();

      switch (mode) {
        case RLE -> {
          // This RLE block is consist of top-level rows, so we'll need to check
          // if the rows should be skipped according to row indexes.
          if (currentValue == 0) {
            if (leftInBatch == 0) {
              state.lastListCompleted = true;
            } else {
              // # of rows to read in the block, must be <= what's left in the current batch
              int n = Math.min(leftInBatch, valuesLeftInBlock);

              if (rowId + n < rangeStart) {
                // Need to skip all rows in [rowId, rowId + n)
                defLevelProcessor.skipValues(n);
                rowId += n;
                currentCount -= n;
                leftInPage -= n;
              } else if (rowId > rangeEnd) {
                // The current row index already beyond the current range: move to the next range
                // and repeat
                state.nextRange();
              } else {
                // The range [rowId, rowId + n) overlaps with the current row range
                long start = Math.max(rangeStart, rowId);
                long end = Math.min(rangeEnd, rowId + n - 1);

                // Skip the rows in [rowId, start)
                int toSkip = (int) (start - rowId);
                if (toSkip > 0) {
                  defLevelProcessor.skipValues(toSkip);
                  rowId += toSkip;
                  currentCount -= toSkip;
                  leftInPage -= toSkip;
                }

                // Read the rows in [start, end]
                n = (int) (end - start + 1);

                if (n > 0) {
                  repLevels.appendInts(n, 0);
                  defLevelProcessor.readValues(n);
                }

                rowId += n;
                currentCount -= n;
                leftInBatch -= n;
                leftInPage -= n;
              }
            }
          } else {
            // Not a top-level row: just read all the repetition levels in the block if the row
            // should be included according to row indexes, else skip the rows.
            if (!state.shouldSkip) {
              repLevels.appendInts(valuesLeftInBlock, currentValue);
            }
            state.numBatchedDefLevels += valuesLeftInBlock;
            leftInPage -= valuesLeftInBlock;
            currentCount -= valuesLeftInBlock;
          }
        }
        case PACKED -> {
          int i = 0;

          for (; i < valuesLeftInBlock; i++) {
            int currentValue = currentBuffer[currentBufferIdx + i];
            if (currentValue == 0) {
              if (leftInBatch == 0) {
                state.lastListCompleted = true;
                break;
              } else if (rowId < rangeStart) {
                // This is a top-level row, therefore check if we should skip it with row indexes
                // the row is before the current range, skip it
                defLevelProcessor.skipValues(1);
              } else if (rowId > rangeEnd) {
                // The row is after the current range, move to the next range and compare again
                state.nextRange();
                break;
              } else {
                // The row is in the current range, decrement the row counter and read it
                leftInBatch--;
                repLevels.appendInt(0);
                defLevelProcessor.readValues(1);
              }
              rowId++;
            } else {
              if (!state.shouldSkip) {
                repLevels.appendInt(currentValue);
              }
              state.numBatchedDefLevels += 1;
            }
          }

          leftInPage -= i;
          currentCount -= i;
          currentBufferIdx += i;
        }
      }
    }

    // Process all the batched def levels
    defLevelProcessor.finish();

    state.rowsToReadInBatch = leftInBatch;
    state.valuesToReadInPage = leftInPage;
    state.rowId = rowId;
  }

  private static class DefLevelProcessor {
    private final VectorizedRleValuesReader reader;
    private final ParquetReadState state;
    private final WritableColumnVector defLevels;
    private final WritableColumnVector values;
    private final WritableColumnVector nulls;
    private final boolean valuesReused;
    private final VectorizedValuesReader valueReader;
    private final ParquetVectorUpdater updater;

    DefLevelProcessor(
        VectorizedRleValuesReader reader,
        ParquetReadState state,
        WritableColumnVector defLevels,
        WritableColumnVector values,
        WritableColumnVector nulls,
        boolean valuesReused,
        VectorizedValuesReader valueReader,
        ParquetVectorUpdater updater) {
      this.reader = reader;
      this.state = state;
      this.defLevels = defLevels;
      this.values = values;
      this.nulls = nulls;
      this.valuesReused = valuesReused;
      this.valueReader = valueReader;
      this.updater = updater;
    }

    void readValues(int n) {
      if (!state.shouldSkip) {
        state.numBatchedDefLevels += n;
      } else {
        reader.skipValues(state.numBatchedDefLevels, state, valueReader, updater);
        state.numBatchedDefLevels = n;
        state.shouldSkip = false;
      }
    }

    void skipValues(int n) {
      if (state.shouldSkip) {
        state.numBatchedDefLevels += n;
      } else {
        reader.readValues(state.numBatchedDefLevels, state, defLevels, values, nulls, valuesReused,
          valueReader, updater);
        state.numBatchedDefLevels = n;
        state.shouldSkip = true;
      }
    }

    void finish() {
      if (state.numBatchedDefLevels > 0) {
        if (state.shouldSkip) {
          reader.skipValues(state.numBatchedDefLevels, state, valueReader, updater);
        } else {
          reader.readValues(state.numBatchedDefLevels, state, defLevels, values, nulls,
            valuesReused, valueReader, updater);
        }
        state.numBatchedDefLevels = 0;
      }
    }
  }

  /**
   * Read the next 'total' values (either null or non-null) from this definition level reader and
   * 'valueReader'. The definition levels are read into 'defLevels'. If a value is not
   * null, it is appended to 'values'. Otherwise, a null bit will be set in 'nulls'.
   *
   * This is only used when reading repeated values.
   */
  private void readValues(
      int total,
      ParquetReadState state,
      WritableColumnVector defLevels,
      WritableColumnVector values,
      WritableColumnVector nulls,
      boolean valuesReused,
      VectorizedValuesReader valueReader,
      ParquetVectorUpdater updater) {

    defLevels.reserveAdditional(total);
    values.reserveAdditional(total);
    if (!valuesReused) {
      // 'nulls' is a separate column vector so we'll have to reserve it separately
      nulls.reserveAdditional(total);
    }

    int n = total;
    int initialValueOffset = state.valueOffset;
    while (n > 0) {
      if (currentCount == 0 && !readNextGroup()) break;
      int num = Math.min(n, this.currentCount);
      readValuesN(num, state, defLevels, values, nulls, valueReader, updater);
      state.levelOffset += num;
      currentCount -= num;
      n -= num;
    }

    defLevels.addElementsAppended(total);

    int valuesRead = state.valueOffset - initialValueOffset;
    values.addElementsAppended(valuesRead);
    if (!valuesReused) {
      nulls.addElementsAppended(valuesRead);
    }
  }

  private void readValuesN(
      int n,
      ParquetReadState state,
      WritableColumnVector defLevels,
      WritableColumnVector values,
      WritableColumnVector nulls,
      VectorizedValuesReader valueReader,
      ParquetVectorUpdater updater) {
    switch (mode) {
      case RLE -> {
        if (currentValue == state.maxDefinitionLevel) {
          updater.readValues(n, state.valueOffset, values, valueReader);
        } else {
          nulls.putNulls(state.valueOffset, n);
        }
        state.valueOffset += n;
        defLevels.putInts(state.levelOffset, n, currentValue);
      }
      case PACKED -> {
        for (int i = 0; i < n; ++i) {
          int currentValue = currentBuffer[currentBufferIdx++];
          if (currentValue == state.maxDefinitionLevel) {
            updater.readValue(state.valueOffset++, values, valueReader);
          } else {
            nulls.putNull(state.valueOffset++);
          }
          defLevels.putInt(state.levelOffset + i, currentValue);
        }
      }
    }
  }

  /**
   * Skip the next `n` values (either null or non-null) from this definition level reader and
   * `valueReader`.
   */
  private void skipValues(
      int n,
      ParquetReadState state,
      VectorizedValuesReader valuesReader,
      ParquetVectorUpdater updater) {
    while (n > 0) {
      if (currentCount == 0 && !readNextGroup()) break;
      int num = Math.min(n, this.currentCount);
      switch (mode) {
        case RLE -> {
          // We only need to skip non-null values from `valuesReader` since nulls are represented
          // via definition levels which are skipped here via decrementing `currentCount`.
          if (currentValue == state.maxDefinitionLevel) {
            updater.skipValues(num, valuesReader);
          }
        }
        case PACKED -> {
          int totalSkipNum = 0;
          for (int i = 0; i < num; ++i) {
            // Same as above, only skip non-null values from `valuesReader`
            if (currentBuffer[currentBufferIdx++] == state.maxDefinitionLevel) {
              ++totalSkipNum;
            }
          }
          updater.skipValues(totalSkipNum, valuesReader);
        }
      }
      currentCount -= num;
      n -= num;
    }
  }

  // The RLE reader implements the vectorized decoding interface when used to decode dictionary
  // IDs. This is different than the above APIs that decodes definitions levels along with values.
  // Since this is only used to decode dictionary IDs, only decoding integers is supported.
  @Override
  public void readIntegers(int total, WritableColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      if (currentCount == 0 && !readNextGroup()) break;
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE -> c.putInts(rowId, n, currentValue);
        case PACKED -> {
          c.putInts(rowId, n, currentBuffer, currentBufferIdx);
          currentBufferIdx += n;
        }
      }
      rowId += n;
      left -= n;
      currentCount -= n;
    }
  }

  @Override
  public void readUnsignedIntegers(int total, WritableColumnVector c, int rowId) {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3187");
  }

  @Override
  public void readUnsignedLongs(int total, WritableColumnVector c, int rowId) {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3187");
  }

  @Override
  public void readIntegersWithRebase(
      int total, WritableColumnVector c, int rowId, boolean failIfRebase) {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3187");
  }

  @Override
  public byte readByte() {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3187");
  }

  @Override
  public short readShort() {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3187");
  }

  @Override
  public void readBytes(int total, WritableColumnVector c, int rowId) {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3187");
  }

  @Override
  public void readShorts(int total, WritableColumnVector c, int rowId) {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3187");
  }

  @Override
  public void readLongs(int total, WritableColumnVector c, int rowId) {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3187");
  }

  @Override
  public void readLongsWithRebase(
      int total,
      WritableColumnVector c,
      int rowId,
      boolean failIfRebase,
      String timeZone) {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3187");
  }

  @Override
  public void readBinary(int total, WritableColumnVector c, int rowId) {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3187");
  }

  @Override
  public void readBooleans(int total, WritableColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE -> c.putBooleans(rowId, n, currentValue != 0);
        case PACKED -> {
          for (int i = 0; i < n; ++i) {
            // For Boolean types, `currentBuffer[currentBufferIdx++]` can only be 0 or 1
            c.putByte(rowId + i, (byte) currentBuffer[currentBufferIdx++]);
          }
        }
      }
      rowId += n;
      left -= n;
      currentCount -= n;
    }
  }

  @Override
  public void readFloats(int total, WritableColumnVector c, int rowId) {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3187");
  }

  @Override
  public void readDoubles(int total, WritableColumnVector c, int rowId) {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3187");
  }

  @Override
  public Binary readBinary(int len) {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3187");
  }

  @Override
  public void skipIntegers(int total) {
    skipValues(total);
  }

  @Override
  public void skipBooleans(int total) {
    skipValues(total);
  }

  @Override
  public void skipBytes(int total) {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3188");
  }

  @Override
  public void skipShorts(int total) {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3188");
  }

  @Override
  public void skipLongs(int total) {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3188");
  }

  @Override
  public void skipFloats(int total) {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3188");
  }

  @Override
  public void skipDoubles(int total) {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3188");
  }

  @Override
  public void skipBinary(int total) {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3188");
  }

  @Override
  public void skipFixedLenByteArray(int total, int len) {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3188");
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
    return switch (bytesWidth) {
      case 0 -> 0;
      case 1 -> in.read();
      case 2 -> {
        int ch2 = in.read();
        int ch1 = in.read();
        yield (ch1 << 8) + ch2;
      }
      case 3 -> {
        int ch3 = in.read();
        int ch2 = in.read();
        int ch1 = in.read();
        yield (ch1 << 16) + (ch2 << 8) + (ch3 << 0);
      }
      case 4 -> readIntLittleEndian();
      default -> throw new RuntimeException("Unreachable");
    };
  }

  /**
   * Reads the next group. Returns false if no more group available.
   */
  private boolean readNextGroup() {
    if (in.available() <= 0) {
      currentCount = 0;
      return false;
    }

    try {
      int header = readUnsignedVarInt();
      this.mode = (header & 1) == 0 ? MODE.RLE : MODE.PACKED;
      switch (mode) {
        case RLE -> {
          this.currentCount = header >>> 1;
          this.currentValue = readIntLittleEndianPaddedOnBitWidth();
        }
        case PACKED -> {
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
        }
      }
    } catch (IOException e) {
      throw new ParquetDecodingException("Failed to read from input stream", e);
    }

    return true;
  }

  /**
   * Skip `n` values from the current reader.
   */
  private void skipValues(int n) {
    int left = n;
    while (left > 0) {
      if (this.currentCount == 0 && !readNextGroup()) break;
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE -> {}
        case PACKED -> currentBufferIdx += num;
      }
      currentCount -= num;
      left -= num;
    }
  }
}
