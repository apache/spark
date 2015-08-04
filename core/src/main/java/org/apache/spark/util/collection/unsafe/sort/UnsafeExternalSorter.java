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

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

import javax.annotation.Nullable;

import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.shuffle.ShuffleMemoryManager;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.TaskMemoryManager;
import org.apache.spark.util.Utils;

/**
 * External sorter based on {@link UnsafeInMemorySorter}.
 */
public final class UnsafeExternalSorter {

  private final Logger logger = LoggerFactory.getLogger(UnsafeExternalSorter.class);

  private final long pageSizeBytes;
  private final PrefixComparator prefixComparator;
  private final RecordComparator recordComparator;
  private final int initialSize;
  private final TaskMemoryManager taskMemoryManager;
  private final ShuffleMemoryManager shuffleMemoryManager;
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
  @Nullable private UnsafeInMemorySorter inMemSorter;
  // Whether the in-mem sorter is created internally, or passed in from outside.
  // If it is passed in from outside, we shouldn't release the in-mem sorter's memory.
  private boolean isInMemSorterExternal = false;
  private MemoryBlock currentPage = null;
  private long currentPagePosition = -1;
  private long freeSpaceInCurrentPage = 0;
  private long peakMemoryUsedBytes = 0;

  public static UnsafeExternalSorter createWithExistingInMemorySorter(
      TaskMemoryManager taskMemoryManager,
      ShuffleMemoryManager shuffleMemoryManager,
      BlockManager blockManager,
      TaskContext taskContext,
      RecordComparator recordComparator,
      PrefixComparator prefixComparator,
      int initialSize,
      long pageSizeBytes,
      UnsafeInMemorySorter inMemorySorter) throws IOException {
    return new UnsafeExternalSorter(taskMemoryManager, shuffleMemoryManager, blockManager,
      taskContext, recordComparator, prefixComparator, initialSize, pageSizeBytes, inMemorySorter);
  }

  public static UnsafeExternalSorter create(
      TaskMemoryManager taskMemoryManager,
      ShuffleMemoryManager shuffleMemoryManager,
      BlockManager blockManager,
      TaskContext taskContext,
      RecordComparator recordComparator,
      PrefixComparator prefixComparator,
      int initialSize,
      long pageSizeBytes) throws IOException {
    return new UnsafeExternalSorter(taskMemoryManager, shuffleMemoryManager, blockManager,
      taskContext, recordComparator, prefixComparator, initialSize, pageSizeBytes, null);
  }

  private UnsafeExternalSorter(
      TaskMemoryManager taskMemoryManager,
      ShuffleMemoryManager shuffleMemoryManager,
      BlockManager blockManager,
      TaskContext taskContext,
      RecordComparator recordComparator,
      PrefixComparator prefixComparator,
      int initialSize,
      long pageSizeBytes,
      @Nullable UnsafeInMemorySorter existingInMemorySorter) throws IOException {
    this.taskMemoryManager = taskMemoryManager;
    this.shuffleMemoryManager = shuffleMemoryManager;
    this.blockManager = blockManager;
    this.taskContext = taskContext;
    this.recordComparator = recordComparator;
    this.prefixComparator = prefixComparator;
    this.initialSize = initialSize;
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility for units
    // this.fileBufferSizeBytes = (int) conf.getSizeAsKb("spark.shuffle.file.buffer", "32k") * 1024;
    this.fileBufferSizeBytes = 32 * 1024;
    // this.pageSizeBytes = conf.getSizeAsBytes("spark.buffer.pageSize", "64m");
    this.pageSizeBytes = pageSizeBytes;
    this.writeMetrics = new ShuffleWriteMetrics();

    if (existingInMemorySorter == null) {
      initializeForWriting();
    } else {
      this.isInMemSorterExternal = true;
      this.inMemSorter = existingInMemorySorter;
    }

    // Register a cleanup task with TaskContext to ensure that memory is guaranteed to be freed at
    // the end of the task. This is necessary to avoid memory leaks in when the downstream operator
    // does not fully consume the sorter's output (e.g. sort followed by limit).
    taskContext.addOnCompleteCallback(new AbstractFunction0<BoxedUnit>() {
      @Override
      public BoxedUnit apply() {
        deleteSpillFiles();
        freeMemory();
        return null;
      }
    });
  }

  // TODO: metrics tracking + integration with shuffle write metrics
  // need to connect the write metrics to task metrics so we count the spill IO somewhere.

  /**
   * Allocates new sort data structures. Called when creating the sorter and after each spill.
   */
  private void initializeForWriting() throws IOException {
    this.writeMetrics = new ShuffleWriteMetrics();
    final long pointerArrayMemory =
      UnsafeInMemorySorter.getMemoryRequirementsForPointerArray(initialSize);
    final long memoryAcquired = shuffleMemoryManager.tryToAcquire(pointerArrayMemory);
    if (memoryAcquired != pointerArrayMemory) {
      shuffleMemoryManager.release(memoryAcquired);
      throw new IOException("Could not acquire " + pointerArrayMemory + " bytes of memory");
    }

    this.inMemSorter =
      new UnsafeInMemorySorter(taskMemoryManager, recordComparator, prefixComparator, initialSize);
    this.isInMemSorterExternal = false;
  }

  /**
   * Marks the current page as no-more-space-available, and as a result, either allocate a
   * new page or spill when we see the next record.
   */
  @VisibleForTesting
  public void closeCurrentPage() {
    freeSpaceInCurrentPage = 0;
  }

  /**
   * Sort and spill the current records in response to memory pressure.
   */
  public void spill() throws IOException {
    assert(inMemSorter != null);
    logger.info("Thread {} spilling sort data of {} to disk ({} {} so far)",
      Thread.currentThread().getId(),
      Utils.bytesToString(getMemoryUsage()),
      spillWriters.size(),
      spillWriters.size() > 1 ? " times" : " time");

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
    final long spillSize = freeMemory();
    // Note that this is more-or-less going to be a multiple of the page size, so wasted space in
    // pages will currently be counted as memory spilled even though that space isn't actually
    // written to disk. This also counts the space needed to store the sorter's pointer array.
    taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);
    initializeForWriting();
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
   * Free this sorter's in-memory data structures, including its data pages and pointer array.
   *
   * @return the number of bytes freed.
   */
  public long freeMemory() {
    updatePeakMemoryUsed();
    long memoryFreed = 0;
    for (MemoryBlock block : allocatedPages) {
      taskMemoryManager.freePage(block);
      shuffleMemoryManager.release(block.size());
      memoryFreed += block.size();
    }
    if (inMemSorter != null) {
      if (!isInMemSorterExternal) {
        long sorterMemoryUsage = inMemSorter.getMemoryUsage();
        memoryFreed += sorterMemoryUsage;
        shuffleMemoryManager.release(sorterMemoryUsage);
      }
      inMemSorter = null;
    }
    allocatedPages.clear();
    currentPage = null;
    currentPagePosition = -1;
    freeSpaceInCurrentPage = 0;
    return memoryFreed;
  }

  /**
   * Deletes any spill files created by this sorter.
   */
  public void deleteSpillFiles() {
    for (UnsafeSorterSpillWriter spill : spillWriters) {
      File file = spill.getFile();
      if (file != null && file.exists()) {
        if (!file.delete()) {
          logger.error("Was unable to delete spill file {}", file.getAbsolutePath());
        };
      }
    }
  }

  /**
   * Checks whether there is enough space to insert a new record into the sorter.
   *
   * @param requiredSpace the required space in the data page, in bytes, including space for storing
   *                      the record size.

   * @return true if the record can be inserted without requiring more allocations, false otherwise.
   */
  private boolean haveSpaceForRecord(int requiredSpace) {
    assert(requiredSpace > 0);
    assert(inMemSorter != null);
    return (inMemSorter.hasSpaceForAnotherRecord() && (requiredSpace <= freeSpaceInCurrentPage));
  }

  /**
   * Allocates more memory in order to insert an additional record. This will request additional
   * memory from the {@link ShuffleMemoryManager} and spill if the requested memory can not be
   * obtained.
   *
   * @param requiredSpace the required space in the data page, in bytes, including space for storing
   *                      the record size.
   */
  private void allocateSpaceForRecord(int requiredSpace) throws IOException {
    assert(inMemSorter != null);
    // TODO: merge these steps to first calculate total memory requirements for this insert,
    // then try to acquire; no point in acquiring sort buffer only to spill due to no space in the
    // data page.
    if (!inMemSorter.hasSpaceForAnotherRecord()) {
      logger.debug("Attempting to expand sort pointer array");
      final long oldPointerArrayMemoryUsage = inMemSorter.getMemoryUsage();
      final long memoryToGrowPointerArray = oldPointerArrayMemoryUsage * 2;
      final long memoryAcquired = shuffleMemoryManager.tryToAcquire(memoryToGrowPointerArray);
      if (memoryAcquired < memoryToGrowPointerArray) {
        shuffleMemoryManager.release(memoryAcquired);
        spill();
      } else {
        inMemSorter.expandPointerArray();
        shuffleMemoryManager.release(oldPointerArrayMemoryUsage);
      }
    }

    if (requiredSpace > freeSpaceInCurrentPage) {
      logger.trace("Required space {} is less than free space in current page ({})", requiredSpace,
        freeSpaceInCurrentPage);
      // TODO: we should track metrics on the amount of space wasted when we roll over to a new page
      // without using the free space at the end of the current page. We should also do this for
      // BytesToBytesMap.
      if (requiredSpace > pageSizeBytes) {
        throw new IOException("Required space " + requiredSpace + " is greater than page size (" +
          pageSizeBytes + ")");
      } else {
        final long memoryAcquired = shuffleMemoryManager.tryToAcquire(pageSizeBytes);
        if (memoryAcquired < pageSizeBytes) {
          if (memoryAcquired > 0) {
            shuffleMemoryManager.release(memoryAcquired);
          }
          spill();
          final long memoryAcquiredAfterSpilling = shuffleMemoryManager.tryToAcquire(pageSizeBytes);
          if (memoryAcquiredAfterSpilling != pageSizeBytes) {
            shuffleMemoryManager.release(memoryAcquiredAfterSpilling);
            throw new IOException("Unable to acquire " + pageSizeBytes + " bytes of memory");
          }
        }
        currentPage = taskMemoryManager.allocatePage(pageSizeBytes);
        currentPagePosition = currentPage.getBaseOffset();
        freeSpaceInCurrentPage = pageSizeBytes;
        allocatedPages.add(currentPage);
      }
    }
  }

  /**
   * Write a record to the sorter.
   */
  public void insertRecord(
      Object recordBaseObject,
      long recordBaseOffset,
      int lengthInBytes,
      long prefix) throws IOException {
    // Need 4 bytes to store the record length.
    final int totalSpaceRequired = lengthInBytes + 4;
    if (!haveSpaceForRecord(totalSpaceRequired)) {
      allocateSpaceForRecord(totalSpaceRequired);
    }
    assert(inMemSorter != null);

    final long recordAddress =
      taskMemoryManager.encodePageNumberAndOffset(currentPage, currentPagePosition);
    final Object dataPageBaseObject = currentPage.getBaseObject();
    PlatformDependent.UNSAFE.putInt(dataPageBaseObject, currentPagePosition, lengthInBytes);
    currentPagePosition += 4;
    PlatformDependent.copyMemory(
      recordBaseObject,
      recordBaseOffset,
      dataPageBaseObject,
      currentPagePosition,
      lengthInBytes);
    currentPagePosition += lengthInBytes;
    freeSpaceInCurrentPage -= totalSpaceRequired;
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
  public void insertKVRecord(
      Object keyBaseObj, long keyOffset, int keyLen,
      Object valueBaseObj, long valueOffset, int valueLen, long prefix) throws IOException {
    final int totalSpaceRequired = keyLen + valueLen + 4 + 4;
    if (!haveSpaceForRecord(totalSpaceRequired)) {
      allocateSpaceForRecord(totalSpaceRequired);
    }
    assert(inMemSorter != null);

    final long recordAddress =
      taskMemoryManager.encodePageNumberAndOffset(currentPage, currentPagePosition);
    final Object dataPageBaseObject = currentPage.getBaseObject();
    PlatformDependent.UNSAFE.putInt(dataPageBaseObject, currentPagePosition, keyLen + valueLen + 4);
    currentPagePosition += 4;

    PlatformDependent.UNSAFE.putInt(dataPageBaseObject, currentPagePosition, keyLen);
    currentPagePosition += 4;

    PlatformDependent.copyMemory(
      keyBaseObj, keyOffset, dataPageBaseObject, currentPagePosition, keyLen);
    currentPagePosition += keyLen;

    PlatformDependent.copyMemory(
      valueBaseObj, valueOffset, dataPageBaseObject, currentPagePosition, valueLen);
    currentPagePosition += valueLen;

    freeSpaceInCurrentPage -= totalSpaceRequired;
    inMemSorter.insertRecord(recordAddress, prefix);
  }

  public UnsafeSorterIterator getSortedIterator() throws IOException {
    assert(inMemSorter != null);
    final UnsafeInMemorySorter.SortedIterator inMemoryIterator = inMemSorter.getSortedIterator();
    int numIteratorsToMerge = spillWriters.size() + (inMemoryIterator.hasNext() ? 1 : 0);
    if (spillWriters.isEmpty()) {
      return inMemoryIterator;
    } else {
      final UnsafeSorterSpillMerger spillMerger =
        new UnsafeSorterSpillMerger(recordComparator, prefixComparator, numIteratorsToMerge);
      for (UnsafeSorterSpillWriter spillWriter : spillWriters) {
        spillMerger.addSpill(spillWriter.getReader(blockManager));
      }
      spillWriters.clear();
      if (inMemoryIterator.hasNext()) {
        spillMerger.addSpill(inMemoryIterator);
      }
      return spillMerger.getSortedIterator();
    }
  }
}
