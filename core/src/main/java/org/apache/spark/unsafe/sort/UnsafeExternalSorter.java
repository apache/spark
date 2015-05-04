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

package org.apache.spark.unsafe.sort;

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.SparkConf;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.shuffle.ShuffleMemoryManager;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.TaskMemoryManager;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import static org.apache.spark.unsafe.sort.UnsafeSorter.*;

/**
 * External sorter based on {@link UnsafeSorter}.
 */
public final class UnsafeExternalSorter {

  private static final int PAGE_SIZE = 1024 * 1024;  // TODO: tune this

  private final PrefixComparator prefixComparator;
  private final RecordComparator recordComparator;
  private final int initialSize;
  private UnsafeSorter sorter;

  private final TaskMemoryManager memoryManager;
  private final ShuffleMemoryManager shuffleMemoryManager;
  private final BlockManager blockManager;
  private final LinkedList<MemoryBlock> allocatedPages = new LinkedList<MemoryBlock>();
  private final boolean spillingEnabled;
  private final int fileBufferSize;
  private ShuffleWriteMetrics writeMetrics;


  private MemoryBlock currentPage = null;
  private long currentPagePosition = -1;

  private final LinkedList<UnsafeSorterSpillWriter> spillWriters =
    new LinkedList<UnsafeSorterSpillWriter>();

  public UnsafeExternalSorter(
      TaskMemoryManager memoryManager,
      ShuffleMemoryManager shuffleMemoryManager,
      BlockManager blockManager,
      RecordComparator recordComparator,
      PrefixComparator prefixComparator,
      int initialSize,
      SparkConf conf) {
    this.memoryManager = memoryManager;
    this.shuffleMemoryManager = shuffleMemoryManager;
    this.blockManager = blockManager;
    this.recordComparator = recordComparator;
    this.prefixComparator = prefixComparator;
    this.initialSize = initialSize;
    this.spillingEnabled = conf.getBoolean("spark.shuffle.spill", true);
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility for units
    this.fileBufferSize = (int) conf.getSizeAsKb("spark.shuffle.file.buffer", "32k") * 1024;
    openSorter();
  }

  // TODO: metrics tracking + integration with shuffle write metrics

  private void openSorter() {
    this.writeMetrics = new ShuffleWriteMetrics();
    // TODO: connect write metrics to task metrics?
    this.sorter = new UnsafeSorter(memoryManager, recordComparator, prefixComparator, initialSize);
  }

  @VisibleForTesting
  public void spill() throws IOException {
    final UnsafeSorterSpillWriter spillWriter =
      new UnsafeSorterSpillWriter(blockManager, fileBufferSize, writeMetrics);
    spillWriters.add(spillWriter);
    final Iterator<RecordPointerAndKeyPrefix> sortedRecords = sorter.getSortedIterator();
    while (sortedRecords.hasNext()) {
      final RecordPointerAndKeyPrefix recordPointer = sortedRecords.next();
      final Object baseObject = memoryManager.getPage(recordPointer.recordPointer);
      final long baseOffset = memoryManager.getOffsetInPage(recordPointer.recordPointer);
      final int recordLength = (int) PlatformDependent.UNSAFE.getLong(baseObject, baseOffset);
      spillWriter.write(baseObject, baseOffset, recordLength, recordPointer.keyPrefix);
    }
    spillWriter.close();
    sorter = null;
    freeMemory();
    openSorter();
  }

  private void freeMemory() {
    final Iterator<MemoryBlock> iter = allocatedPages.iterator();
    while (iter.hasNext()) {
      memoryManager.freePage(iter.next());
      shuffleMemoryManager.release(PAGE_SIZE);
      iter.remove();
    }
    currentPage = null;
    currentPagePosition = -1;
  }

  private void ensureSpaceInDataPage(int requiredSpace) throws Exception {
    final long spaceInCurrentPage;
    if (currentPage != null) {
      spaceInCurrentPage = PAGE_SIZE - (currentPagePosition - currentPage.getBaseOffset());
    } else {
      spaceInCurrentPage = 0;
    }
    if (requiredSpace > PAGE_SIZE) {
      // TODO: throw a more specific exception?
      throw new Exception("Required space " + requiredSpace + " is greater than page size (" +
        PAGE_SIZE + ")");
    } else if (requiredSpace > spaceInCurrentPage) {
      if (spillingEnabled && shuffleMemoryManager.tryToAcquire(PAGE_SIZE) < PAGE_SIZE) {
        spill();
      }
      currentPage = memoryManager.allocatePage(PAGE_SIZE);
      currentPagePosition = currentPage.getBaseOffset();
      allocatedPages.add(currentPage);
    }
  }

  public void insertRecord(
      Object recordBaseObject,
      long recordBaseOffset,
      int lengthInBytes,
      long prefix) throws Exception {
    // Need 4 bytes to store the record length.
    ensureSpaceInDataPage(lengthInBytes + 4);

    final long recordAddress =
      memoryManager.encodePageNumberAndOffset(currentPage, currentPagePosition);
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

    sorter.insertRecord(recordAddress, prefix);
  }

  public Iterator<UnsafeExternalSortSpillMerger.RecordAddressAndKeyPrefix> getSortedIterator() throws IOException {
    final UnsafeExternalSortSpillMerger spillMerger =
      new UnsafeExternalSortSpillMerger(recordComparator, prefixComparator);
    for (UnsafeSorterSpillWriter spillWriter : spillWriters) {
      spillMerger.addSpill(spillWriter.getReader(blockManager));
    }
    spillWriters.clear();
    spillMerger.addSpill(sorter.getMergeableIterator());
    return spillMerger.getSortedIterator();
  }
}
