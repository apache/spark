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
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.Platform;
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
    this.pageSizeBytes = pageSizeBytes;
    this.writeMetrics = new ShuffleWriteMetrics();

    if (existingInMemorySorter == null) {
      initializeForWriting();
      // Acquire a new page as soon as we construct the sorter to ensure that we have at
      // least one page to work with. Otherwise, other operators in the same task may starve
      // this sorter (SPARK-9709). We don't need to do this if we already have an existing sorter.
      acquireNewPage();
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
        cleanupResources();
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
    // Note: Do not track memory for the pointer array for now because of SPARK-10474.
    // In more detail, in TungstenAggregate we only reserve a page, but when we fall back to
    // sort-based aggregation we try to acquire a page AND a pointer array, which inevitably
    // fails if all other memory is already occupied. It should be safe to not track the array
    // because its memory footprint is frequently much smaller than that of a page. This is a
    // temporary hack that we should address in 1.6.0.
    // TODO: track the pointer array memory!
    this.writeMetrics = new ShuffleWriteMetrics();
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
  private long freeMemory() {
    updatePeakMemoryUsed();
    long memoryFreed = 0;
    for (MemoryBlock block : allocatedPages) {
      taskMemoryManager.freePage(block);
      shuffleMemoryManager.release(block.size());
      memoryFreed += block.size();
    }
    // TODO: track in-memory sorter memory usage (SPARK-10474)
    allocatedPages.clear();
    currentPage = null;
    currentPagePosition = -1;
    freeSpaceInCurrentPage = 0;
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
    deleteSpillFiles();
    freeMemory();
  }

  /**
   * Checks whether there is enough space to insert an additional record in to the sort pointer
   * array and grows the array if additional space is required. If the required space cannot be
   * obtained, then the in-memory data will be spilled to disk.
   */
  private void growPointerArrayIfNecessary() throws IOException {
    assert(inMemSorter != null);
    if (!inMemSorter.hasSpaceForAnotherRecord()) {
      // TODO: track the pointer array memory! (SPARK-10474)
      inMemSorter.expandPointerArray();
    }
  }

  /**
   * Allocates more memory in order to insert an additional record. This will request additional
   * memory from the {@link ShuffleMemoryManager} and spill if the requested memory can not be
   * obtained.
   *
   * @param requiredSpace the required space in the data page, in bytes, including space for storing
   *                      the record size. This must be less than or equal to the page size (records
   *                      that exceed the page size are handled via a different code path which uses
   *                      special overflow pages).
   */
  private void acquireNewPageIfNecessary(int requiredSpace) throws IOException {
    assert (requiredSpace <= pageSizeBytes);
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
        acquireNewPage();
      }
    }
  }

  /**
   * Acquire a new page from the {@link ShuffleMemoryManager}.
   *
   * If there is not enough space to allocate the new page, spill all existing ones
   * and try again. If there is still not enough space, report error to the caller.
   */
  private void acquireNewPage() throws IOException {
    final long memoryAcquired = shuffleMemoryManager.tryToAcquire(pageSizeBytes);
    if (memoryAcquired < pageSizeBytes) {
      shuffleMemoryManager.release(memoryAcquired);
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

  /**
   * Write a record to the sorter.
   */
  public void insertRecord(
      Object recordBaseObject,
      long recordBaseOffset,
      int lengthInBytes,
      long prefix) throws IOException {

    growPointerArrayIfNecessary();
    // Need 4 bytes to store the record length.
    final int totalSpaceRequired = lengthInBytes + 4;

    // --- Figure out where to insert the new record ----------------------------------------------

    final MemoryBlock dataPage;
    long dataPagePosition;
    boolean useOverflowPage = totalSpaceRequired > pageSizeBytes;
    if (useOverflowPage) {
      long overflowPageSize = ByteArrayMethods.roundNumberOfBytesToNearestWord(totalSpaceRequired);
      // The record is larger than the page size, so allocate a special overflow page just to hold
      // that record.
      final long memoryGranted = shuffleMemoryManager.tryToAcquire(overflowPageSize);
      if (memoryGranted != overflowPageSize) {
        shuffleMemoryManager.release(memoryGranted);
        spill();
        final long memoryGrantedAfterSpill = shuffleMemoryManager.tryToAcquire(overflowPageSize);
        if (memoryGrantedAfterSpill != overflowPageSize) {
          shuffleMemoryManager.release(memoryGrantedAfterSpill);
          throw new IOException("Unable to acquire " + overflowPageSize + " bytes of memory");
        }
      }
      MemoryBlock overflowPage = taskMemoryManager.allocatePage(overflowPageSize);
      allocatedPages.add(overflowPage);
      dataPage = overflowPage;
      dataPagePosition = overflowPage.getBaseOffset();
    } else {
      // The record is small enough to fit in a regular data page, but the current page might not
      // have enough space to hold it (or no pages have been allocated yet).
      acquireNewPageIfNecessary(totalSpaceRequired);
      dataPage = currentPage;
      dataPagePosition = currentPagePosition;
      // Update bookkeeping information
      freeSpaceInCurrentPage -= totalSpaceRequired;
      currentPagePosition += totalSpaceRequired;
    }
    final Object dataPageBaseObject = dataPage.getBaseObject();

    // --- Insert the record ----------------------------------------------------------------------

    final long recordAddress =
      taskMemoryManager.encodePageNumberAndOffset(dataPage, dataPagePosition);
    Platform.putInt(dataPageBaseObject, dataPagePosition, lengthInBytes);
    dataPagePosition += 4;
    Platform.copyMemory(
      recordBaseObject, recordBaseOffset, dataPageBaseObject, dataPagePosition, lengthInBytes);
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
  public void insertKVRecord(
      Object keyBaseObj, long keyOffset, int keyLen,
      Object valueBaseObj, long valueOffset, int valueLen, long prefix) throws IOException {

    growPointerArrayIfNecessary();
    final int totalSpaceRequired = keyLen + valueLen + 4 + 4;

    // --- Figure out where to insert the new record ----------------------------------------------

    final MemoryBlock dataPage;
    long dataPagePosition;
    boolean useOverflowPage = totalSpaceRequired > pageSizeBytes;
    if (useOverflowPage) {
      long overflowPageSize = ByteArrayMethods.roundNumberOfBytesToNearestWord(totalSpaceRequired);
      // The record is larger than the page size, so allocate a special overflow page just to hold
      // that record.
      final long memoryGranted = shuffleMemoryManager.tryToAcquire(overflowPageSize);
      if (memoryGranted != overflowPageSize) {
        shuffleMemoryManager.release(memoryGranted);
        spill();
        final long memoryGrantedAfterSpill = shuffleMemoryManager.tryToAcquire(overflowPageSize);
        if (memoryGrantedAfterSpill != overflowPageSize) {
          shuffleMemoryManager.release(memoryGrantedAfterSpill);
          throw new IOException("Unable to acquire " + overflowPageSize + " bytes of memory");
        }
      }
      MemoryBlock overflowPage = taskMemoryManager.allocatePage(overflowPageSize);
      allocatedPages.add(overflowPage);
      dataPage = overflowPage;
      dataPagePosition = overflowPage.getBaseOffset();
    } else {
      // The record is small enough to fit in a regular data page, but the current page might not
      // have enough space to hold it (or no pages have been allocated yet).
      acquireNewPageIfNecessary(totalSpaceRequired);
      dataPage = currentPage;
      dataPagePosition = currentPagePosition;
      // Update bookkeeping information
      freeSpaceInCurrentPage -= totalSpaceRequired;
      currentPagePosition += totalSpaceRequired;
    }
    final Object dataPageBaseObject = dataPage.getBaseObject();

    // --- Insert the record ----------------------------------------------------------------------

    final long recordAddress =
      taskMemoryManager.encodePageNumberAndOffset(dataPage, dataPagePosition);
    Platform.putInt(dataPageBaseObject, dataPagePosition, keyLen + valueLen + 4);
    dataPagePosition += 4;

    Platform.putInt(dataPageBaseObject, dataPagePosition, keyLen);
    dataPagePosition += 4;

    Platform.copyMemory(keyBaseObj, keyOffset, dataPageBaseObject, dataPagePosition, keyLen);
    dataPagePosition += keyLen;

    Platform.copyMemory(valueBaseObj, valueOffset, dataPageBaseObject, dataPagePosition, valueLen);

    assert(inMemSorter != null);
    inMemSorter.insertRecord(recordAddress, prefix);
  }

  /**
   * Returns a sorted iterator. It is the caller's responsibility to call `cleanupResources()`
   * after consuming this iterator.
   */
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
        spillMerger.addSpillIfNotEmpty(spillWriter.getReader(blockManager));
      }
      spillWriters.clear();
      spillMerger.addSpillIfNotEmpty(inMemoryIterator);

      return spillMerger.getSortedIterator();
    }
  }
}
