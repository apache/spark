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

package org.apache.spark.util.collection.unsafe.sort;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.TaskCompletionListener;
import org.apache.spark.util.Utils;

/**
 * External sorter based on {@link UnsafeInMemorySorter}.
 */
public final class UnsafeExternalSorter extends MemoryConsumer {

  private final Logger logger = LoggerFactory.getLogger(UnsafeExternalSorter.class);

  private final PrefixComparator prefixComparator;
  private final RecordComparator recordComparator;
  private final TaskMemoryManager taskMemoryManager;
  private final BlockManager blockManager;
  private final TaskContext taskContext;
  private ShuffleWriteMetrics writeMetrics;

  /** The buffer size to use when writing spills using DiskBlockObjectWriter */
  private final int fileBufferSizeBytes;

  /**
   * Memory pages that hold the records being sorted. The pages in this list are freed when
   * spilling, although in principle we could recycle these pages across spills (on the other hand,
   * this might not be necessary if we maintained a pool of re-usable pages in the TaskMemoryManager
   * itself).
   */
  private final LinkedList<MemoryBlock> allocatedPages = new LinkedList<>();

  private final LinkedList<UnsafeSorterSpillWriter> spillWriters = new LinkedList<>();

  // These variables are reset after spilling:
  @Nullable private volatile UnsafeInMemorySorter inMemSorter;

  private MemoryBlock currentPage = null;
  private long pageCursor = -1;
  private long peakMemoryUsedBytes = 0;
  private volatile SpillableIterator readingIterator = null;

  public static UnsafeExternalSorter createWithExistingInMemorySorter(
      TaskMemoryManager taskMemoryManager,
      BlockManager blockManager,
      TaskContext taskContext,
      RecordComparator recordComparator,
      PrefixComparator prefixComparator,
      int initialSize,
      long pageSizeBytes,
      UnsafeInMemorySorter inMemorySorter) throws IOException {
    UnsafeExternalSorter sorter = new UnsafeExternalSorter(taskMemoryManager, blockManager,
      taskContext, recordComparator, prefixComparator, initialSize, pageSizeBytes, inMemorySorter);
    sorter.spill(Long.MAX_VALUE, sorter);
    // The external sorter will be used to insert records, in-memory sorter is not needed.
    sorter.inMemSorter = null;
    return sorter;
  }

  public static UnsafeExternalSorter create(
      TaskMemoryManager taskMemoryManager,
      BlockManager blockManager,
      TaskContext taskContext,
      RecordComparator recordComparator,
      PrefixComparator prefixComparator,
      int initialSize,
      long pageSizeBytes) {
    return new UnsafeExternalSorter(taskMemoryManager, blockManager,
      taskContext, recordComparator, prefixComparator, initialSize, pageSizeBytes, null);
  }

  private UnsafeExternalSorter(
      TaskMemoryManager taskMemoryManager,
      BlockManager blockManager,
      TaskContext taskContext,
      RecordComparator recordComparator,
      PrefixComparator prefixComparator,
      int initialSize,
      long pageSizeBytes,
      @Nullable UnsafeInMemorySorter existingInMemorySorter) {
    super(taskMemoryManager, pageSizeBytes);
    this.taskMemoryManager = taskMemoryManager;
    this.blockManager = blockManager;
    this.taskContext = taskContext;
    this.recordComparator = recordComparator;
    this.prefixComparator = prefixComparator;
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility for units
    // this.fileBufferSizeBytes = (int) conf.getSizeAsKb("spark.shuffle.file.buffer", "32k") * 1024;
    this.fileBufferSizeBytes = 32 * 1024;
    // TODO: metrics tracking + integration with shuffle write metrics
    // need to connect the write metrics to task metrics so we count the spill IO somewhere.
    this.writeMetrics = new ShuffleWriteMetrics();

    if (existingInMemorySorter == null) {
      this.inMemSorter = new UnsafeInMemorySorter(
        this, taskMemoryManager, recordComparator, prefixComparator, initialSize);
    } else {
      this.inMemSorter = existingInMemorySorter;
    }
    this.peakMemoryUsedBytes = getMemoryUsage();

    // Register a cleanup task with TaskContext to ensure that memory is guaranteed to be freed at
    // the end of the task. This is necessary to avoid memory leaks in when the downstream operator
    // does not fully consume the sorter's output (e.g. sort followed by limit).
    taskContext.addTaskCompletionListener(
      new TaskCompletionListener() {
        @Override
        public void onTaskCompletion(TaskContext context) {
          cleanupResources();
        }
      }
    );
  }

  /**
   * Marks the current page as no-more-space-available, and as a result, either allocate a
   * new page or spill when we see the next record.
   */
  @VisibleForTesting
  public void closeCurrentPage() {
    if (currentPage != null) {
      pageCursor = currentPage.getBaseOffset() + currentPage.size();
    }
  }

  /**
   * Sort and spill the current records in response to memory pressure.
   */
  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    if (trigger != this) {
      if (readingIterator != null) {
        return readingIterator.spill();
      }
      return 0L; // this should throw exception
    }

    if (inMemSorter == null || inMemSorter.numRecords() <= 0) {
      return 0L;
    }

    logger.info("Thread {} spilling sort data of {} to disk ({} {} so far)",
      Thread.currentThread().getId(),
      Utils.bytesToString(getMemoryUsage()),
      spillWriters.size(),
      spillWriters.size() > 1 ? " times" : " time");

    // We only write out contents of the inMemSorter if it is not empty.
    if (inMemSorter.numRecords() > 0) {
      final UnsafeSorterSpillWriter spillWriter =
        new UnsafeSorterSpillWriter(blockManager, fileBufferSizeBytes, writeMetrics,
          inMemSorter.numRecords());
      spillWriters.add(spillWriter);
      final UnsafeSorterIterator sortedRecords = inMemSorter.getSortedIterator();
      while (sortedRecords.hasNext()) {
        sortedRecords.loadNext();
        final Object baseObject = sortedRecords.getBaseObject();
        final long baseOffset = sortedRecords.getBaseOffset();
        final int recordLength = sortedRecords.getRecordLength();
        spillWriter.write(baseObject, baseOffset, recordLength, sortedRecords.getKeyPrefix());
      }
      spillWriter.close();
    }

    final long spillSize = freeMemory();
    // Note that this is more-or-less going to be a multiple of the page size, so wasted space in
    // pages will currently be counted as memory spilled even though that space isn't actually
    // written to disk. This also counts the space needed to store the sorter's pointer array.
    inMemSorter.reset();
    // Reset the in-memory sorter's pointer array only after freeing up the memory pages holding the
    // records. Otherwise, if the task is over allocated memory, then without freeing the memory pages,
    // we might not be able to get memory for the pointer array.

    taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);

    return spillSize;
  }

  /**
   * Return the total memory usage of this sorter, including the data pages and the sorter's pointer
   * array.
   */
  private long getMemoryUsage() {
    long totalPageSize = 0;
    for (MemoryBlock page : allocatedPages) {
      totalPageSize += page.size();
    }
    return ((inMemSorter == null) ? 0 : inMemSorter.getMemoryUsage()) + totalPageSize;
  }

  private void updatePeakMemoryUsed() {
    long mem = getMemoryUsage();
    if (mem > peakMemoryUsedBytes) {
      peakMemoryUsedBytes = mem;
    }
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  @VisibleForTesting
  public int getNumberOfAllocatedPages() {
    return allocatedPages.size();
  }

  /**
   * Free this sorter's data pages.
   *
   * @return the number of bytes freed.
   */
  private long freeMemory() {
    updatePeakMemoryUsed();
    long memoryFreed = 0;
    for (MemoryBlock block : allocatedPages) {
      memoryFreed += block.size();
      freePage(block);
    }
    allocatedPages.clear();
    currentPage = null;
    pageCursor = 0;
    return memoryFreed;
  }

  /**
   * Deletes any spill files created by this sorter.
   */
  private void deleteSpillFiles() {
    for (UnsafeSorterSpillWriter spill : spillWriters) {
      File file = spill.getFile();
      if (file != null && file.exists()) {
        if (!file.delete()) {
          logger.error("Was unable to delete spill file {}", file.getAbsolutePath());
        }
      }
    }
  }

  /**
   * Frees this sorter's in-memory data structures and cleans up its spill files.
   */
  public void cleanupResources() {
    synchronized (this) {
      deleteSpillFiles();
      freeMemory();
      if (inMemSorter != null) {
        inMemSorter.free();
        inMemSorter = null;
      }
    }
  }

  /**
   * Checks whether there is enough space to insert an additional record in to the sort pointer
   * array and grows the array if additional space is required. If the required space cannot be
   * obtained, then the in-memory data will be spilled to disk.
   */
  private void growPointerArrayIfNecessary() throws IOException {
    assert(inMemSorter != null);
    if (!inMemSorter.hasSpaceForAnotherRecord()) {
      long used = inMemSorter.getMemoryUsage();
      LongArray array;
      try {
        // could trigger spilling
        array = allocateArray(used / 8 * 2);
      } catch (OutOfMemoryError e) {
        // should have trigger spilling
        if (!inMemSorter.hasSpaceForAnotherRecord()) {
          logger.error("Unable to grow the pointer array");
          throw e;
        }
        return;
      }
      // check if spilling is triggered or not
      if (inMemSorter.hasSpaceForAnotherRecord()) {
        freeArray(array);
      } else {
        inMemSorter.expandPointerArray(array);
      }
    }
  }

  /**
   * Allocates more memory in order to insert an additional record. This will request additional
   * memory from the memory manager and spill if the requested memory can not be obtained.
   *
   * @param required the required space in the data page, in bytes, including space for storing
   *                      the record size. This must be less than or equal to the page size (records
   *                      that exceed the page size are handled via a different code path which uses
   *                      special overflow pages).
   */
  private void acquireNewPageIfNecessary(int required) {
    if (currentPage == null ||
      pageCursor + required > currentPage.getBaseOffset() + currentPage.size()) {
      // TODO: try to find space on previous pages
      currentPage = allocatePage(required);
      pageCursor = currentPage.getBaseOffset();
      allocatedPages.add(currentPage);
    }
  }

  /**
   * Write a record to the sorter.
   */
  public void insertRecord(Object recordBase, long recordOffset, int length, long prefix)
    throws IOException {

    growPointerArrayIfNecessary();
    // Need 4 bytes to store the record length.
    final int required = length + 4;
    acquireNewPageIfNecessary(required);

    final Object base = currentPage.getBaseObject();
    final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);
    Platform.putInt(base, pageCursor, length);
    pageCursor += 4;
    Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);
    pageCursor += length;
    assert(inMemSorter != null);
    inMemSorter.insertRecord(recordAddress, prefix);
  }

  /**
   * Write a key-value record to the sorter. The key and value will be put together in-memory,
   * using the following format:
   *
   * record length (4 bytes), key length (4 bytes), key data, value data
   *
   * record length = key length + value length + 4
   */
  public void insertKVRecord(Object keyBase, long keyOffset, int keyLen,
      Object valueBase, long valueOffset, int valueLen, long prefix)
    throws IOException {

    growPointerArrayIfNecessary();
    final int required = keyLen + valueLen + 4 + 4;
    acquireNewPageIfNecessary(required);

    final Object base = currentPage.getBaseObject();
    final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);
    Platform.putInt(base, pageCursor, keyLen + valueLen + 4);
    pageCursor += 4;
    Platform.putInt(base, pageCursor, keyLen);
    pageCursor += 4;
    Platform.copyMemory(keyBase, keyOffset, base, pageCursor, keyLen);
    pageCursor += keyLen;
    Platform.copyMemory(valueBase, valueOffset, base, pageCursor, valueLen);
    pageCursor += valueLen;

    assert(inMemSorter != null);
    inMemSorter.insertRecord(recordAddress, prefix);
  }

  /**
   * Merges another UnsafeExternalSorters into this one, the other one will be emptied.
   *
   * @throws IOException
   */
  public void merge(UnsafeExternalSorter other) throws IOException {
    other.spill();
    spillWriters.addAll(other.spillWriters);
    // remove them from `spillWriters`, or the files will be deleted in `cleanupResources`.
    other.spillWriters.clear();
    other.cleanupResources();
  }

  /**
   * Returns a sorted iterator. It is the caller's responsibility to call `cleanupResources()`
   * after consuming this iterator.
   */
  public UnsafeSorterIterator getSortedIterator() throws IOException {
    if (spillWriters.isEmpty()) {
      assert(inMemSorter != null);
      readingIterator = new SpillableIterator(inMemSorter.getSortedIterator());
      return readingIterator;
    } else {
      final UnsafeSorterSpillMerger spillMerger =
        new UnsafeSorterSpillMerger(recordComparator, prefixComparator, spillWriters.size());
      for (UnsafeSorterSpillWriter spillWriter : spillWriters) {
        spillMerger.addSpillIfNotEmpty(spillWriter.getReader(blockManager));
      }
      if (inMemSorter != null) {
        readingIterator = new SpillableIterator(inMemSorter.getSortedIterator());
        spillMerger.addSpillIfNotEmpty(readingIterator);
      }
      return spillMerger.getSortedIterator();
    }
  }

  /**
   * An UnsafeSorterIterator that support spilling.
   */
  class SpillableIterator extends UnsafeSorterIterator {
    private UnsafeSorterIterator upstream;
    private UnsafeSorterIterator nextUpstream = null;
    private MemoryBlock lastPage = null;
    private boolean loaded = false;
    private int numRecords = 0;

    public SpillableIterator(UnsafeInMemorySorter.SortedIterator inMemIterator) {
      this.upstream = inMemIterator;
      this.numRecords = inMemIterator.numRecordsLeft();
    }

    public long spill() throws IOException {
      synchronized (this) {
        if (!(upstream instanceof UnsafeInMemorySorter.SortedIterator && nextUpstream == null
          && numRecords > 0)) {
          return 0L;
        }

        UnsafeInMemorySorter.SortedIterator inMemIterator =
          ((UnsafeInMemorySorter.SortedIterator) upstream).clone();

        // Iterate over the records that have not been returned and spill them.
        final UnsafeSorterSpillWriter spillWriter =
          new UnsafeSorterSpillWriter(blockManager, fileBufferSizeBytes, writeMetrics, numRecords);
        while (inMemIterator.hasNext()) {
          inMemIterator.loadNext();
          final Object baseObject = inMemIterator.getBaseObject();
          final long baseOffset = inMemIterator.getBaseOffset();
          final int recordLength = inMemIterator.getRecordLength();
          spillWriter.write(baseObject, baseOffset, recordLength, inMemIterator.getKeyPrefix());
        }
        spillWriter.close();
        spillWriters.add(spillWriter);
        nextUpstream = spillWriter.getReader(blockManager);

        long released = 0L;
        synchronized (UnsafeExternalSorter.this) {
          // release the pages except the one that is used. There can still be a caller that
          // is accessing the current record. We free this page in that caller's next loadNext()
          // call.
          for (MemoryBlock page : allocatedPages) {
            if (!loaded || page.getBaseObject() != upstream.getBaseObject()) {
              released += page.size();
              freePage(page);
            } else {
              lastPage = page;
            }
          }
          allocatedPages.clear();
        }

        // in-memory sorter will not be used after spilling
        assert(inMemSorter != null);
        released += inMemSorter.getMemoryUsage();
        inMemSorter.free();
        inMemSorter = null;
        return released;
      }
    }

    @Override
    public boolean hasNext() {
      return numRecords > 0;
    }

    @Override
    public void loadNext() throws IOException {
      synchronized (this) {
        loaded = true;
        if (nextUpstream != null) {
          // Just consumed the last record from in memory iterator
          if (lastPage != null) {
            freePage(lastPage);
            lastPage = null;
          }
          upstream = nextUpstream;
          nextUpstream = null;
        }
        numRecords--;
        upstream.loadNext();
      }
    }

    @Override
    public Object getBaseObject() {
      return upstream.getBaseObject();
    }

    @Override
    public long getBaseOffset() {
      return upstream.getBaseOffset();
    }

    @Override
    public int getRecordLength() {
      return upstream.getRecordLength();
    }

    @Override
    public long getKeyPrefix() {
      return upstream.getKeyPrefix();
    }
  }
}
