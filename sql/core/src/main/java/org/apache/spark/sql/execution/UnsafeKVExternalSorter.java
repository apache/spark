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

package org.apache.spark.sql.execution;

import java.io.IOException;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.TaskContext;
import org.apache.spark.shuffle.ShuffleMemoryManager;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BaseOrdering;
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.unsafe.KVIterator;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.map.BytesToBytesMap;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.TaskMemoryManager;
import org.apache.spark.util.collection.unsafe.sort.*;

/**
 * A class for performing external sorting on key-value records. Both key and value are UnsafeRows.
 *
 * Note that this class allows optionally passing in a {@link BytesToBytesMap} directly in order
 * to perform in-place sorting of records in the map.
 */
public final class UnsafeKVExternalSorter {

  private final StructType keySchema;
  private final StructType valueSchema;
  private final UnsafeExternalRowSorter.PrefixComputer prefixComputer;
  private final UnsafeExternalSorter sorter;

  public UnsafeKVExternalSorter(StructType keySchema, StructType valueSchema,
      BlockManager blockManager, ShuffleMemoryManager shuffleMemoryManager, long pageSizeBytes)
    throws IOException {
    this(keySchema, valueSchema, blockManager, shuffleMemoryManager, pageSizeBytes, null);
  }

  public UnsafeKVExternalSorter(StructType keySchema, StructType valueSchema,
      BlockManager blockManager, ShuffleMemoryManager shuffleMemoryManager, long pageSizeBytes,
      @Nullable BytesToBytesMap map) throws IOException {
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    final TaskContext taskContext = TaskContext.get();

    prefixComputer = SortPrefixUtils.createPrefixGenerator(keySchema);
    PrefixComparator prefixComparator = SortPrefixUtils.getPrefixComparator(keySchema);
    BaseOrdering ordering = GenerateOrdering.create(keySchema);
    KVComparator recordComparator = new KVComparator(ordering, keySchema.length());

    TaskMemoryManager taskMemoryManager = taskContext.taskMemoryManager();

    if (map == null) {
      sorter = UnsafeExternalSorter.create(
        taskMemoryManager,
        shuffleMemoryManager,
        blockManager,
        taskContext,
        recordComparator,
        prefixComparator,
        /* initialSize */ 4096,
        pageSizeBytes);
    } else {
      // Insert the records into the in-memory sorter.
      // We will use the number of elements in the map as the initialSize of the
      // UnsafeInMemorySorter. Because UnsafeInMemorySorter does not accept 0 as the initialSize,
      // we will use 1 as its initial size if the map is empty.
      final UnsafeInMemorySorter inMemSorter = new UnsafeInMemorySorter(
        taskMemoryManager, recordComparator, prefixComparator, Math.max(1, map.numElements()));

      // We cannot use the destructive iterator here because we are reusing the existing memory
      // pages in BytesToBytesMap to hold records during sorting.
      // The only new memory we are allocating is the pointer/prefix array.
      BytesToBytesMap.BytesToBytesMapIterator iter = map.iterator();
      final int numKeyFields = keySchema.size();
      UnsafeRow row = new UnsafeRow();
      while (iter.hasNext()) {
        final BytesToBytesMap.Location loc = iter.next();
        final Object baseObject = loc.getKeyAddress().getBaseObject();
        final long baseOffset = loc.getKeyAddress().getBaseOffset();

        // Get encoded memory address
        // baseObject + baseOffset point to the beginning of the key data in the map, but that
        // the KV-pair's length data is stored in the word immediately before that address
        MemoryBlock page = loc.getMemoryPage();
        long address = taskMemoryManager.encodePageNumberAndOffset(page, baseOffset - 8);

        // Compute prefix
        row.pointTo(baseObject, baseOffset, numKeyFields, loc.getKeyLength());
        final long prefix = prefixComputer.computePrefix(row);

        inMemSorter.insertRecord(address, prefix);
      }

      sorter = UnsafeExternalSorter.createWithExistingInMemorySorter(
        taskContext.taskMemoryManager(),
        shuffleMemoryManager,
        blockManager,
        taskContext,
        new KVComparator(ordering, keySchema.length()),
        prefixComparator,
        /* initialSize */ 4096,
        pageSizeBytes,
        inMemSorter);

      sorter.spill();
      map.free();
    }
  }

  /**
   * Inserts a key-value record into the sorter. If the sorter no longer has enough memory to hold
   * the record, the sorter sorts the existing records in-memory, writes them out as partially
   * sorted runs, and then reallocates memory to hold the new record.
   */
  public void insertKV(UnsafeRow key, UnsafeRow value) throws IOException {
    final long prefix = prefixComputer.computePrefix(key);
    sorter.insertKVRecord(
      key.getBaseObject(), key.getBaseOffset(), key.getSizeInBytes(),
      value.getBaseObject(), value.getBaseOffset(), value.getSizeInBytes(), prefix);
  }

  /**
   * Returns a sorted iterator. It is the caller's responsibility to call `cleanupResources()`
   * after consuming this iterator.
   */
  public KVSorterIterator sortedIterator() throws IOException {
    try {
      final UnsafeSorterIterator underlying = sorter.getSortedIterator();
      if (!underlying.hasNext()) {
        // Since we won't ever call next() on an empty iterator, we need to clean up resources
        // here in order to prevent memory leaks.
        cleanupResources();
      }
      return new KVSorterIterator(underlying);
    } catch (IOException e) {
      cleanupResources();
      throw e;
    }
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  public long getPeakMemoryUsedBytes() {
    return sorter.getPeakMemoryUsedBytes();
  }

  /**
   * Marks the current page as no-more-space-available, and as a result, either allocate a
   * new page or spill when we see the next record.
   */
  @VisibleForTesting
  void closeCurrentPage() {
    sorter.closeCurrentPage();
  }

  /**
   * Frees this sorter's in-memory data structures and cleans up its spill files.
   */
  public void cleanupResources() {
    sorter.cleanupResources();
  }

  private static final class KVComparator extends RecordComparator {
    private final BaseOrdering ordering;
    private final UnsafeRow row1 = new UnsafeRow();
    private final UnsafeRow row2 = new UnsafeRow();
    private final int numKeyFields;

    public KVComparator(BaseOrdering ordering, int numKeyFields) {
      this.numKeyFields = numKeyFields;
      this.ordering = ordering;
    }

    @Override
    public int compare(Object baseObj1, long baseOff1, Object baseObj2, long baseOff2) {
      // Note that since ordering doesn't need the total length of the record, we just pass -1
      // into the row.
      row1.pointTo(baseObj1, baseOff1 + 4, numKeyFields, -1);
      row2.pointTo(baseObj2, baseOff2 + 4, numKeyFields, -1);
      return ordering.compare(row1, row2);
    }
  }

  public class KVSorterIterator extends KVIterator<UnsafeRow, UnsafeRow> {
    private UnsafeRow key = new UnsafeRow();
    private UnsafeRow value = new UnsafeRow();
    private final int numKeyFields = keySchema.size();
    private final int numValueFields = valueSchema.size();
    private final UnsafeSorterIterator underlying;

    private KVSorterIterator(UnsafeSorterIterator underlying) {
      this.underlying = underlying;
    }

    @Override
    public boolean next() throws IOException {
      try {
        if (underlying.hasNext()) {
          underlying.loadNext();

          Object baseObj = underlying.getBaseObject();
          long recordOffset = underlying.getBaseOffset();
          int recordLen = underlying.getRecordLength();

          // Note that recordLen = keyLen + valueLen + 4 bytes (for the keyLen itself)
          int keyLen = Platform.getInt(baseObj, recordOffset);
          int valueLen = recordLen - keyLen - 4;
          key.pointTo(baseObj, recordOffset + 4, numKeyFields, keyLen);
          value.pointTo(baseObj, recordOffset + 4 + keyLen, numValueFields, valueLen);

          return true;
        } else {
          key = null;
          value = null;
          cleanupResources();
          return false;
        }
      } catch (IOException e) {
        cleanupResources();
        throw e;
      }
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
      cleanupResources();
    }
  };
}
