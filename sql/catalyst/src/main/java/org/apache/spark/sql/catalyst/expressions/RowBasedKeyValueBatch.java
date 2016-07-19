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
package org.apache.spark.sql.catalyst.expressions;

import java.io.IOException;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.Platform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * RowBasedKeyValueBatch stores key value pairs in contiguous memory region.
 *
 * Each key or value is stored as a single UnsafeRow. The format for each record looks like this:
 * [4 bytes total size = (klen + vlen + 4)] [4 bytes key size = klen]
 * [UnsafeRow for key of length klen] [UnsafeRow for Value of length vlen]
 * [8 bytes pointer to next]
 * Thus, record length = 4 + 4 + klen + vlen + 8
 *
 * RowBasedKeyValueBatch is backed by a single page / MemoryBlock (defaults to 64MB). If the page
 * is full, the aggregate logic should fallback to a second level, larger hash map. We intentionally
 * use the single-page design because it simplifies memory address encoding & decoding for each
 * key-value pair. Because the maximum capacity for RowBasedKeyValueBatch is only 2^16, it is
 * unlikely we need a second page anyway. Filling the page requires an average size for key value
 * pairs to be larger than 1024 bytes.
 *
 */
public final class RowBasedKeyValueBatch extends MemoryConsumer {
  private final Logger logger = LoggerFactory.getLogger(RowBasedKeyValueBatch.class);

  private static final int DEFAULT_CAPACITY = 1 << 16;
  private static final long DEFAULT_PAGE_SIZE = 64 * 1024 * 1024;

  private final StructType keySchema;
  private final StructType valueSchema;
  private final int capacity;
  private int numRows = 0;

  // Staging row returned from getRow.
  final UnsafeRow keyRow;
  final UnsafeRow valueRow;

  // ids for current key row and value row being retrieved
  private int keyRowId = -1;

  // full addresses for key rows and value rows
  private long[] keyOffsets;

  // if all data types in the schema are fixed length
  private boolean allFixedLength;
  private int klen;
  private int vlen;
  private int recordLength;

  private MemoryBlock page = null;
  private Object base = null;
  private final long recordStartOffset;
  private long pageCursor = 0;

  public static RowBasedKeyValueBatch allocate(StructType keySchema, StructType valueSchema,
                                               TaskMemoryManager manager) {
    return new RowBasedKeyValueBatch(keySchema, valueSchema, DEFAULT_CAPACITY, manager);
  }

  public static RowBasedKeyValueBatch allocate(StructType keySchema, StructType valueSchema,
                                               TaskMemoryManager manager, int maxRows) {
    return new RowBasedKeyValueBatch(keySchema, valueSchema, maxRows, manager);
  }

  public int numRows() { return numRows; }

  public void close() {
    if (page != null) {
      freePage(page);
      page = null;
    }
  }

  private boolean acquirePage(long requiredSize) {
    try {
      page = allocatePage(requiredSize);
    } catch (OutOfMemoryError e) {
      logger.warn("Failed to allocate page ({} bytes).", requiredSize);
      return false;
    }
    base = page.getBaseObject();
    pageCursor = 0;
    return true;
  }

  private long getKeyOffsetForFixedLengthRecords(int rowId) {
    return recordStartOffset + rowId * (long) recordLength + 8;
  }

  /**
   * Append a key value pair.
   * It copies data into the backing MemoryBlock.
   * Returns an UnsafeRow pointing to the value if succeeds, otherwise returns null.
   */
  public UnsafeRow appendRow(Object kbase, long koff, int klen,
                             Object vbase, long voff, int vlen) {
    final long recordLength = 8 + klen + vlen + 8;
    // if run out of max supported rows or page size, return null
    if (numRows >= capacity || page == null || page.size() - pageCursor < recordLength) {
      return null;
    }

    long offset = page.getBaseOffset() + pageCursor;
    final long recordOffset = offset;
    if (!allFixedLength) { // we only put lengths info for variable length
      Platform.putInt(base, offset, klen + vlen + 4);
      Platform.putInt(base, offset + 4, klen);
    }
    offset += 8;
    Platform.copyMemory(kbase, koff, base, offset, klen);
    offset += klen;
    Platform.copyMemory(vbase, voff, base, offset, vlen);
    offset += vlen;
    Platform.putLong(base, offset, 0);

    pageCursor += recordLength;

    if (!allFixedLength) keyOffsets[numRows] = recordOffset + 8;

    keyRowId = numRows;
    keyRow.pointTo(base, recordOffset + 8, klen);
    valueRow.pointTo(base, recordOffset + 8 + klen, vlen + 4);
    numRows++;
    return valueRow;
  }

  /**
   * Returns the key row in this batch at `rowId`. Returned key row is reused across calls.
   */
  public UnsafeRow getKeyRow(int rowId) {
    assert(rowId >= 0);
    assert(rowId < numRows);
    if (keyRowId != rowId) { // if keyRowId == rowId, desired keyRow is already cached
      if (allFixedLength) {
        long offset = getKeyOffsetForFixedLengthRecords(rowId);
        keyRow.pointTo(base, offset, klen);
      } else {
        long offset = keyOffsets[rowId];
        klen = Platform.getInt(base, offset - 4);
        keyRow.pointTo(base, offset, klen);
      }
      // set keyRowId so we can check if desired row is cached
      keyRowId = rowId;
    }
    return keyRow;
  }

  /**
   * Returns the value row in this batch at `rowId`.
   * It can be a faster path if `keyRowId` is equal to `rowId`, which means the preceding
   * key row has just been accessed. This is always the case so far.
   * Returned value row is reused across calls.
   */
  public UnsafeRow getValueRow(int rowId) {
    return getValueFromKey(rowId);
  }

  /**
   * Returns the value row in this batch at `rowId`.
   * It can be a faster path if `keyRowId` is equal to `rowId`, which means the preceding
   * key row has just been accessed. This is always the case so far.
   * Returned value row is reused across calls.
   */
  private UnsafeRow getValueFromKey(int rowId) {
    if (keyRowId != rowId) {
      getKeyRow(rowId);
    }
    assert(rowId >= 0);
    if (allFixedLength) {
      valueRow.pointTo(base, keyRow.getBaseOffset() + klen, vlen + 4);
    } else {
      long offset = keyOffsets[rowId];
      vlen = Platform.getInt(base, offset - 8) - klen - 4;
      valueRow.pointTo(base,
              offset + klen,
              vlen + 4);
    }
    return valueRow;
  }

  public long spill(long size, MemoryConsumer trigger) throws IOException {
    logger.warn("RowBasedKeyValueBatch should never call spill().");
    throw new OutOfMemoryError("row batch should never spill");
  }

  /**
   * Returns an iterator to go through all rows
   */
  public org.apache.spark.unsafe.KVIterator<UnsafeRow, UnsafeRow> rowIterator() {
    return new org.apache.spark.unsafe.KVIterator<UnsafeRow, UnsafeRow>() {
      private final UnsafeRow key = new UnsafeRow(keySchema.length());
      private final UnsafeRow value = new UnsafeRow(valueSchema.length());

      private long offsetInPage = 0;
      private int recordsInPage = 0;

      private int currentklen;
      private int currentvlen;
      private int totalLength;

      private boolean initialized = false;

      private void init() {
        if (page != null) {
          offsetInPage = page.getBaseOffset();
          recordsInPage = numRows;
        }
        initialized = true;
      }

      @Override
      public boolean next() {
        if (!initialized) init();
        //searching for the next non empty page is records is now zero
        if (recordsInPage == 0) {
          freeCurrentPage();
          return false;
        }

        if (allFixedLength) {
          totalLength = klen + vlen;
          currentklen = klen;
          currentvlen = vlen;
        } else {
          totalLength = Platform.getInt(base, offsetInPage) - 4;
          currentklen = Platform.getInt(base, offsetInPage + 4);
          currentvlen = totalLength - currentklen;
        }

        key.pointTo(base, offsetInPage + 8, currentklen);
        value.pointTo(base, offsetInPage + 8 + currentklen, currentvlen + 4);

        offsetInPage += 8 + totalLength + 8;
        recordsInPage -= 1;
        return true;
      }

      @Override
      public UnsafeRow getKey() {
        return key;
      }

      @Override
      public UnsafeRow getValue() {
        return value;
      }

      @Override
      public void close() {
        // do nothing
      }

      private void freeCurrentPage() {
        if (page != null) {
          freePage(page);
          page = null;
        }
      }
    };
  }

  private RowBasedKeyValueBatch(StructType keySchema, StructType valueSchema, int maxRows,
                                TaskMemoryManager manager) {
    super(manager, manager.pageSizeBytes(), manager.getTungstenMemoryMode());

    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    this.capacity = maxRows;

    this.keyRow = new UnsafeRow(keySchema.length());
    this.valueRow = new UnsafeRow(valueSchema.length());

    // checking if there is any variable length fields
    // there is probably a more succinct impl of this
    allFixedLength = true;
    for (String name : keySchema.fieldNames()) {
      allFixedLength = allFixedLength
              && UnsafeRow.isFixedLength(keySchema.apply(name).dataType());
    }
    for (String name : valueSchema.fieldNames()) {
      allFixedLength = allFixedLength
              && UnsafeRow.isFixedLength(valueSchema.apply(name).dataType());
    }
    if (allFixedLength) {
      klen = keySchema.defaultSize()
              + UnsafeRow.calculateBitSetWidthInBytes(keySchema.length());
      vlen = valueSchema.defaultSize()
              + UnsafeRow.calculateBitSetWidthInBytes(valueSchema.length());
      recordLength = 8 + klen + vlen + 8;
    } else {
      // we only need the following data structures for variable length cases
      this.keyOffsets = new long[maxRows];
    }

    if (!acquirePage(DEFAULT_PAGE_SIZE)) {
      page = null;
      recordStartOffset = 0;
    } else {
      base = page.getBaseObject();
      recordStartOffset = page.getBaseOffset();
    }
  }
}
