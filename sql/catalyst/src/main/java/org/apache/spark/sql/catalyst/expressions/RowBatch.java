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
import java.util.*;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.Platform;


/**
 * RowBatch stores key value pairs in contiguous memory region.
 *
 * Each key or value is stored as a single UnsafeRow. The format for each record looks like this:
 * [4 bytes total size = (klen + vlen + 4)] [4 bytes key size = klen]
 * [UnsafeRow for key of length klen] [UnsafeRow for Value of length vlen]
 * [8 bytes pointer to next]
 * Thus, record length = 8 + klen + vlen + 8
 *
 * RowBatch will automatically acquire new pages (MemoryBlock) when the current page is used up.
 *
 * TODO: making each entry more compact, e.g., combine key and value into a single UnsafeRow
 */
public final class RowBatch extends MemoryConsumer{
    private static final int DEFAULT_CAPACITY = 4 * 1024;

    private final TaskMemoryManager taskMemoryManager;

    private final StructType keySchema;
    private final StructType valueSchema;
    private final int capacity;
    private int numRows = 0;

    // Staging row returned from getRow.
    final UnsafeRow keyRow;
    final UnsafeRow valueRow;

    // ids for current key row and value row being retrieved
    private int keyRowId = -1;
    private int valueRowId = -1;

    // full addresses for key rows and value rows
    // TODO: opt: this could be eliminated if all fields are fixed length
    private long[] keyFullAddress;
    private long[] valueFullAddress;
    // shortcuts for lengths, which can also be retrieved directly from UnsafeRow
    // TODO: might want to remove this shortcut, retrieving directly from UnsafeRow could be
    // faster due to cache locality
    private int[] keyLength;
    private int[] valueLength;

    private MemoryBlock currentPage = null;
    private long pageCursor = 0;

    private final LinkedList<MemoryBlock> dataPages = new LinkedList<>();

    public static RowBatch allocate(StructType keySchema, StructType valueSchema,
                                    TaskMemoryManager manager) {
        return new RowBatch(keySchema, valueSchema, DEFAULT_CAPACITY, manager);
    }

    public static RowBatch allocate(StructType keySchema, StructType valueSchema,
                                    TaskMemoryManager manager, int maxRows) {
        return new RowBatch(keySchema, valueSchema, maxRows, manager);
    }

    public int numRows() { return numRows; }

    public void close() {
        // do nothing, pages should be freed already
    }

    private boolean acquireNewPage(long required) {
        try {
            currentPage = allocatePage(required);
        } catch (OutOfMemoryError e) {
            return false;
        }
        Platform.putInt(currentPage.getBaseObject(), currentPage.getBaseOffset(), 0);
        pageCursor = 4;

        //System.out.println("acquired a new page");
        dataPages.add(currentPage);
        //TODO: add code to recycle the pages when we destroy this map
        return true;
    }

    public UnsafeRow appendRow(Object kbase, long koff, int klen,
                               Object vbase, long voff, int vlen) {
        if (numRows < capacity) {
            return null;
        }

        final long recordLength = 8 + klen + vlen + 8;
        if (currentPage == null || currentPage.size() - pageCursor < recordLength) {
            // acquire new page
            if (!acquireNewPage(recordLength + 4L)) {
                return null;
            }
        }

        final Object base = currentPage.getBaseObject();
        long offset = currentPage.getBaseOffset() + pageCursor;
        final long recordOffset = offset;
        Platform.putInt(base, offset, klen + vlen + 4);
        Platform.putInt(base, offset + 4, klen);
        offset += 8;
        Platform.copyMemory(kbase, koff, base, offset, klen);
        offset += klen;
        Platform.copyMemory(vbase, voff, base, offset, vlen);
        offset += vlen;
        Platform.putLong(base, offset, 0);

        offset = currentPage.getBaseOffset();
        Platform.putInt(base, offset, Platform.getInt(base, offset) + 1);
        pageCursor += recordLength;

        final long storedKeyAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage,
                recordOffset);
        keyFullAddress[numRows] = storedKeyAddress + 8;
        valueFullAddress[numRows] = storedKeyAddress + 8 + klen;
        keyLength[numRows] = klen;
        valueLength[numRows] = vlen;

        keyRowId = numRows;
        valueRowId = numRows;
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
        if (keyRowId != rowId) {
            long fullAddress = keyFullAddress[rowId];
            //TODO: if decoding through manager
            //is not fast enough, we can do a simple version in this class
            Object base = taskMemoryManager.getPage(fullAddress);
            long offset = taskMemoryManager.getOffsetInPage(fullAddress);
            keyRow.pointTo(base, offset, keyLength[rowId]);
            keyRowId = rowId;
        }
        return keyRow;
    }

    /**
     * Returns the value row in this batch at `rowId`. Returned value row is reused across calls.
     * Should be avoided if `getValueFromKey()` gives better performance.
     */
    public UnsafeRow getValueRow(int rowId) {
        assert(rowId >= 0);
        assert(rowId < numRows);
        if (valueRowId != rowId) {
            long fullAddress = valueFullAddress[rowId];
            //TODO: if decoding through manager
            //is not fast enough, we can do a simple version in this class
            Object base = taskMemoryManager.getPage(fullAddress);
            long offset = taskMemoryManager.getOffsetInPage(fullAddress);
            valueRow.pointTo(base, offset, valueLength[rowId] + 4);
            valueRowId = rowId;
        }
        return valueRow;
    }

    /**
     * Returns the value row in this batch at `rowId`.
     * It can be a faster path if `keyRowId` is equal to `rowId`, which means the preceding
     * key row has just been accessed. As this is often the case, this method should be preferred
     * over `getValueRow()`.
     * This method is faster than `getValueRow()` because it avoids address decoding, instead reuse
     * the page and offset information from the preceding key row.
     * Returned value row is reused across calls.
     */
    public UnsafeRow getValueFromKey(int rowId) {
        if (keyRowId != rowId) {
            getKeyRow(rowId);
        }
        assert(rowId >= 0);
        valueRow.pointTo(keyRow.getBaseObject(),
                keyRow.getBaseOffset() + keyLength[rowId],
                valueLength[rowId] + 4);
        valueRowId = rowId;
        return valueRow;
    }

    public long spill(long size, MemoryConsumer trigger) throws IOException {
        throw new OutOfMemoryError("RowBatch should never spill");
    }

    /**
     * Returns an iterator to go through all rows
     */
    public org.apache.spark.unsafe.KVIterator<UnsafeRow, UnsafeRow> rowIterator() {
        return new org.apache.spark.unsafe.KVIterator<UnsafeRow, UnsafeRow>() {
            private final UnsafeRow key = new UnsafeRow(keySchema.length());
            private final UnsafeRow value = new UnsafeRow(valueSchema.length());

            private MemoryBlock currentPage = null;
            private Object pageBaseObject = null;
            private long offsetInPage = 0;
            private int recordsInPage = 0;

            private int klen;
            private int vlen;
            private int totalLength;

            private boolean inited = false;

            private void init() {
                if (dataPages.size() > 0) {
                    currentPage = dataPages.remove();
                    pageBaseObject = currentPage.getBaseObject();
                    offsetInPage = currentPage.getBaseOffset();
                    recordsInPage = Platform.getInt(pageBaseObject, offsetInPage);
                    offsetInPage += 4;
                }
                inited = true;
            }

            @Override
            public boolean next() {
                if (!inited) init();
                //searching for the next non empty page is records is now zero
                while (recordsInPage == 0) {
                    if (!advanceToNextPage()) return false;
                }

                totalLength = Platform.getInt(pageBaseObject, offsetInPage);
                klen = Platform.getInt(pageBaseObject, offsetInPage + 4);
                vlen = totalLength - klen;

                key.pointTo(pageBaseObject, offsetInPage + 8, klen);
                value.pointTo(pageBaseObject, offsetInPage + 8 + klen, vlen);
                offsetInPage += 4 + totalLength + 8;
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

            private boolean advanceToNextPage() {
                if (currentPage != null) freePage(currentPage); //free before advance
                if (dataPages.size() > 0) {
                    currentPage = dataPages.remove();
                    pageBaseObject = currentPage.getBaseObject();
                    offsetInPage = currentPage.getBaseOffset();
                    recordsInPage = Platform.getInt(pageBaseObject, offsetInPage);
                    offsetInPage += 4;
                    return true;
                } else {
                    return false;
                }
            }
        };
    }

    private RowBatch(StructType keySchema, StructType valueSchema, int maxRows,
                     TaskMemoryManager manager) {
        super(manager, manager.pageSizeBytes(), manager.getTungstenMemoryMode());

        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.capacity = maxRows;
        this.taskMemoryManager = manager;
        this.keyFullAddress = new long[maxRows];
        this.valueFullAddress = new long[maxRows];
        this.keyLength = new int[maxRows];
        this.valueLength = new int[maxRows];

        this.keyRow = new UnsafeRow(keySchema.length());
        this.valueRow = new UnsafeRow(valueSchema.length());
    }
}
