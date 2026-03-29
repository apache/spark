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

import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;

/**
 * An implementation of `RowBasedKeyValueBatch` in which all key-value records have same length.
 *
 * The format for each record looks like this:
 * [UnsafeRow for key of length klen] [UnsafeRow for Value of length vlen]
 * [8 bytes pointer to next]
 * Thus, record length = klen + vlen + 8
 */
public final class FixedLengthRowBasedKeyValueBatch extends RowBasedKeyValueBatch {
  private final int klen;
  private final int vlen;
  private final int recordLength;

  private long getKeyOffsetForFixedLengthRecords(int rowId) {
    return recordStartOffset + rowId * (long) recordLength;
  }

  /**
   * Append a key value pair.
   * It copies data into the backing MemoryBlock.
   * Returns an UnsafeRow pointing to the value if succeeds, otherwise returns null.
   */
  @Override
  public UnsafeRow appendRow(Object kbase, long koff, int klen,
                             Object vbase, long voff, int vlen) {
    // if run out of max supported rows or page size, return null
    assert(vlen == this.vlen);
    assert(klen == this.klen);
    if (numRows >= capacity || page == null || page.size() - pageCursor < recordLength) {
      return null;
    }

    long offset = page.getBaseOffset() + pageCursor;
    final long recordOffset = offset;
    Platform.copyMemory(kbase, koff, base, offset, klen);
    offset += klen;
    Platform.copyMemory(vbase, voff, base, offset, vlen);
    offset += vlen;
    Platform.putLong(base, offset, 0);

    pageCursor += recordLength;

    keyRowId = numRows;
    keyRow.pointTo(base, recordOffset, klen);
    valueRow.pointTo(base, recordOffset + klen, vlen);
    numRows++;
    return valueRow;
  }

  /**
   * Returns the key row in this batch at `rowId`. Returned key row is reused across calls.
   */
  @Override
  public UnsafeRow getKeyRow(int rowId) {
    assert(rowId >= 0);
    assert(rowId < numRows);
    if (keyRowId != rowId) { // if keyRowId == rowId, desired keyRow is already cached
      long offset = getKeyOffsetForFixedLengthRecords(rowId);
      keyRow.pointTo(base, offset, klen);
      // set keyRowId so we can check if desired row is cached
      keyRowId = rowId;
    }
    return keyRow;
  }

  /**
   * Returns the value row by two steps:
   * 1) looking up the key row with the same id (skipped if the key row is cached)
   * 2) retrieve the value row by reusing the metadata from step 1)
   * In most times, 1) is skipped because `getKeyRow(id)` is often called before `getValueRow(id)`.
   */
  @Override
  protected UnsafeRow getValueFromKey(int rowId) {
    if (keyRowId != rowId) {
      getKeyRow(rowId);
    }
    assert(rowId >= 0);
    valueRow.pointTo(base, keyRow.getBaseOffset() + klen, vlen);
    return valueRow;
  }

  /**
   * Returns an iterator to go through all rows
   */
  @Override
  public org.apache.spark.unsafe.KVIterator<UnsafeRow, UnsafeRow> rowIterator() {
    return new org.apache.spark.unsafe.KVIterator<UnsafeRow, UnsafeRow>() {
      private final UnsafeRow key = new UnsafeRow(keySchema.length());
      private final UnsafeRow value = new UnsafeRow(valueSchema.length());

      private long offsetInPage = 0;
      private int recordsInPage = 0;

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

        key.pointTo(base, offsetInPage, klen);
        value.pointTo(base, offsetInPage + klen, vlen);

        offsetInPage += recordLength;
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

  FixedLengthRowBasedKeyValueBatch(StructType keySchema, StructType valueSchema,
      int maxRows, TaskMemoryManager manager) {
    super(keySchema, valueSchema, maxRows, manager);
    int keySize = keySchema.size() * 8; // each fixed-length field is stored in a 8-byte word
    int valueSize = valueSchema.size() * 8;
    klen = keySize + UnsafeRow.calculateBitSetWidthInBytes(keySchema.length());
    vlen = valueSize + UnsafeRow.calculateBitSetWidthInBytes(valueSchema.length());
    recordLength = klen + vlen + 8;
  }
}
