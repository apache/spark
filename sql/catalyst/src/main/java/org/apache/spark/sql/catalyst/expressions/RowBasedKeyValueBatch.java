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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * RowBasedKeyValueBatch stores key value pairs in contiguous memory region.
 *
 * Each key or value is stored as a single UnsafeRow. Each record contains one key and one value
 * and some auxiliary data, which differs based on implementation:
 * i.e., `FixedLengthRowBasedKeyValueBatch` and `VariableLengthRowBasedKeyValueBatch`.
 *
 * We use `FixedLengthRowBasedKeyValueBatch` if all fields in the key and the value are fixed-length
 * data types. Otherwise we use `VariableLengthRowBasedKeyValueBatch`.
 *
 * RowBasedKeyValueBatch is backed by a single page / MemoryBlock (ranges from 1 to 64MB depending
 * on the system configuration). If the page is full, the aggregate logic should fallback to a
 * second level, larger hash map. We intentionally use the single-page design because it simplifies
 * memory address encoding & decoding for each key-value pair. Because the maximum capacity for
 * RowBasedKeyValueBatch is only 2^16, it is unlikely we need a second page anyway. Filling the
 * page requires an average size for key value pairs to be larger than 1024 bytes.
 *
 */
public abstract class RowBasedKeyValueBatch extends MemoryConsumer {
  protected final Logger logger = LoggerFactory.getLogger(RowBasedKeyValueBatch.class);

  private static final int DEFAULT_CAPACITY = 1 << 16;

  protected final StructType keySchema;
  protected final StructType valueSchema;
  protected final int capacity;
  protected int numRows = 0;

  // ids for current key row and value row being retrieved
  protected int keyRowId = -1;

  // placeholder for key and value corresponding to keyRowId.
  protected final UnsafeRow keyRow;
  protected final UnsafeRow valueRow;

  protected MemoryBlock page = null;
  protected Object base = null;
  protected final long recordStartOffset;
  protected long pageCursor = 0;

  public static RowBasedKeyValueBatch allocate(StructType keySchema, StructType valueSchema,
                                               TaskMemoryManager manager) {
    return allocate(keySchema, valueSchema, manager, DEFAULT_CAPACITY);
  }

  public static RowBasedKeyValueBatch allocate(StructType keySchema, StructType valueSchema,
                                               TaskMemoryManager manager, int maxRows) {
    boolean allFixedLength = true;
    // checking if there is any variable length fields
    // there is probably a more succinct impl of this
    for (String name : keySchema.fieldNames()) {
      allFixedLength = allFixedLength
              && UnsafeRow.isFixedLength(keySchema.apply(name).dataType());
    }
    for (String name : valueSchema.fieldNames()) {
      allFixedLength = allFixedLength
              && UnsafeRow.isFixedLength(valueSchema.apply(name).dataType());
    }

    if (allFixedLength) {
      return new FixedLengthRowBasedKeyValueBatch(keySchema, valueSchema, maxRows, manager);
    } else {
      return new VariableLengthRowBasedKeyValueBatch(keySchema, valueSchema, maxRows, manager);
    }
  }

  protected RowBasedKeyValueBatch(StructType keySchema, StructType valueSchema, int maxRows,
                                TaskMemoryManager manager) {
    super(manager, manager.pageSizeBytes(), manager.getTungstenMemoryMode());

    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    this.capacity = maxRows;

    this.keyRow = new UnsafeRow(keySchema.length());
    this.valueRow = new UnsafeRow(valueSchema.length());

    if (!acquirePage(manager.pageSizeBytes())) {
      page = null;
      recordStartOffset = 0;
    } else {
      base = page.getBaseObject();
      recordStartOffset = page.getBaseOffset();
    }
  }

  public final int numRows() { return numRows; }

  public final void close() {
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

  /**
   * Append a key value pair.
   * It copies data into the backing MemoryBlock.
   * Returns an UnsafeRow pointing to the value if succeeds, otherwise returns null.
   */
  public abstract UnsafeRow appendRow(Object kbase, long koff, int klen,
                                      Object vbase, long voff, int vlen);

  /**
   * Returns the key row in this batch at `rowId`. Returned key row is reused across calls.
   */
  public abstract UnsafeRow getKeyRow(int rowId);

  /**
   * Returns the value row in this batch at `rowId`. Returned value row is reused across calls.
   * Because `getValueRow(id)` is always called after `getKeyRow(id)` with the same id, we use
   * `getValueFromKey(id) to retrieve value row, which reuses metadata from the cached key.
   */
  public final UnsafeRow getValueRow(int rowId) {
    return getValueFromKey(rowId);
  }

  /**
   * Returns the value row by two steps:
   * 1) looking up the key row with the same id (skipped if the key row is cached)
   * 2) retrieve the value row by reusing the metadata from step 1)
   * In most times, 1) is skipped because `getKeyRow(id)` is often called before `getValueRow(id)`.
   */
  protected abstract UnsafeRow getValueFromKey(int rowId);

  /**
   * Sometimes the TaskMemoryManager may call spill() on its associated MemoryConsumers to make
   * space for new consumers. For RowBasedKeyValueBatch, we do not actually spill and return 0.
   * We should not throw OutOfMemory exception here because other associated consumers might spill
   */
  public final long spill(long size, MemoryConsumer trigger) throws IOException {
    logger.warn("Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.");
    return 0;
  }

  /**
   * Returns an iterator to go through all rows
   */
  public abstract org.apache.spark.unsafe.KVIterator<UnsafeRow, UnsafeRow> rowIterator();
}
