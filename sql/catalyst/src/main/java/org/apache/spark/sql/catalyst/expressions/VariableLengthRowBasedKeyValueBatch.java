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
 * An implementation of `RowBasedKeyValueBatch` in which key-value records have variable lengths.
 *
 *  The format for each record looks like this:
 * [4 bytes total size = (klen + vlen + 4)] [4 bytes key size = klen]
 * [UnsafeRow for key of length klen] [UnsafeRow for Value of length vlen]
 * [8 bytes pointer to next]
 * Thus, record length = 4 + 4 + klen + vlen + 8
 */
public final class VariableLengthRowBasedKeyValueBatch extends RowBasedKeyValueBatch {
  // full addresses for key rows and value rows
  private final long[] keyOffsets;

  /**
   * Append a key value pair.
   * It copies data into the backing MemoryBlock.
   * Returns an UnsafeRow pointing to the value if succeeds, otherwise returns null.
   */
  @Override
  public UnsafeRow appendRow(Object kbase, long koff, int klen,
                             Object vbase, long voff, int vlen) {
    final long recordLength = 8 + klen + vlen + 8;
    // if run out of max supported rows or page size, return null
    if (numRows >= capacity || page == null || page.size() - pageCursor < recordLength) {
      return null;
    }

    long offset = page.getBaseOffset() + pageCursor;
    final long recordOffset = offset;
    Platform.putInt(base, offset, klen + vlen + 4);
    Platform.putInt(base, offset + 4, klen);

    offset += 8;
    Platform.copyMemory(kbase, koff, base, offset, klen);
    offset += klen;
    Platform.copyMemory(vbase, voff, base, offset, vlen);
    offset += vlen;
    Platform.putLong(base, offset, 0);

    pageCursor += recordLength;

    keyOffsets[numRows] = recordOffset + 8;

    keyRowId = numRows;
    keyRow.pointTo(base, recordOffset + 8, klen);
    valueRow.pointTo(base, recordOffset + 8 + klen, vlen + 4);
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
      long offset = keyOffsets[rowId];
      int klen = Platform.getInt(base, offset - 4);
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
  public UnsafeRow getValueFromKey(int rowId) {
    if (keyRowId != rowId) {
      getKeyRow(rowId);
    }
    assert(rowId >= 0);
    long offset = keyRow.getBaseOffset();
    int klen = keyRow.getSizeInBytes();
    int vlen = Platform.getInt(base, offset - 8) - klen - 4;
    valueRow.pointTo(base, offset + klen, vlen + 4);
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

        totalLength = Platform.getInt(base, offsetInPage) - 4;
        currentklen = Platform.getInt(base, offsetInPage + 4);
        currentvlen = totalLength - currentklen;

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

  protected VariableLengthRowBasedKeyValueBatch(StructType keySchema, StructType valueSchema,
                                              int maxRows, TaskMemoryManager manager) {
    super(keySchema, valueSchema, maxRows, manager);
    this.keyOffsets = new long[maxRows];
  }
}
