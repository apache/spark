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

package org.apache.spark.shuffle.unsafe;

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleMemoryManager;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.BlockObjectWriter;
import org.apache.spark.storage.TempLocalBlockId;
import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.TaskMemoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * External sorter based on {@link UnsafeShuffleSorter}.
 */
public final class UnsafeShuffleSpillWriter {

  private final Logger logger = LoggerFactory.getLogger(UnsafeShuffleSpillWriter.class);

  private static final int SER_BUFFER_SIZE = 1024 * 1024;  // TODO: tune this / don't duplicate
  private static final int PAGE_SIZE = 1024 * 1024;  // TODO: tune this

  private final int initialSize;
  private final int numPartitions;
  private UnsafeShuffleSorter sorter;

  private final TaskMemoryManager memoryManager;
  private final ShuffleMemoryManager shuffleMemoryManager;
  private final BlockManager blockManager;
  private final TaskContext taskContext;
  private final LinkedList<MemoryBlock> allocatedPages = new LinkedList<MemoryBlock>();
  private final boolean spillingEnabled;
  private final int fileBufferSize;
  private ShuffleWriteMetrics writeMetrics;


  private MemoryBlock currentPage = null;
  private long currentPagePosition = -1;

  private final LinkedList<SpillInfo> spills = new LinkedList<SpillInfo>();

  public UnsafeShuffleSpillWriter(
      TaskMemoryManager memoryManager,
      ShuffleMemoryManager shuffleMemoryManager,
      BlockManager blockManager,
      TaskContext taskContext,
      int initialSize,
      int numPartitions,
      SparkConf conf) throws IOException {
    this.memoryManager = memoryManager;
    this.shuffleMemoryManager = shuffleMemoryManager;
    this.blockManager = blockManager;
    this.taskContext = taskContext;
    this.initialSize = initialSize;
    this.numPartitions = numPartitions;
    this.spillingEnabled = conf.getBoolean("spark.shuffle.spill", true);
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility for units
    this.fileBufferSize = (int) conf.getSizeAsKb("spark.shuffle.file.buffer", "32k") * 1024;
    openSorter();
  }

  // TODO: metrics tracking + integration with shuffle write metrics

  private void openSorter() throws IOException {
    this.writeMetrics = new ShuffleWriteMetrics();
    // TODO: connect write metrics to task metrics?
    // TODO: move this sizing calculation logic into a static method of sorter:
    final long memoryRequested = initialSize * 8L;
    if (spillingEnabled) {
      final long memoryAcquired = shuffleMemoryManager.tryToAcquire(memoryRequested);
      if (memoryAcquired != memoryRequested) {
        shuffleMemoryManager.release(memoryAcquired);
        throw new IOException("Could not acquire memory!");
      }
    }

    this.sorter = new UnsafeShuffleSorter(initialSize);
  }

  private SpillInfo writeSpillFile() throws IOException {
    final UnsafeShuffleSorter.UnsafeShuffleSorterIterator sortedRecords = sorter.getSortedIterator();

    int currentPartition = -1;
    BlockObjectWriter writer = null;
    final byte[] arr = new byte[SER_BUFFER_SIZE];

    final Tuple2<TempLocalBlockId, File> spilledFileInfo =
      blockManager.diskBlockManager().createTempLocalBlock();
    final File file = spilledFileInfo._2();
    final BlockId blockId = spilledFileInfo._1();
    final SpillInfo spillInfo = new SpillInfo(numPartitions, file, blockId);
    spills.add(spillInfo);

    final SerializerInstance ser = new DummySerializerInstance();
    writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSize, writeMetrics);

    while (sortedRecords.hasNext()) {
      sortedRecords.loadNext();
      final int partition = sortedRecords.packedRecordPointer.getPartitionId();
      assert (partition >= currentPartition);
      if (partition != currentPartition) {
        // Switch to the new partition
        if (currentPartition != -1) {
          writer.commitAndClose();
          spillInfo.partitionLengths[currentPartition] = writer.fileSegment().length();
        }
        currentPartition = partition;
        writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSize, writeMetrics);
      }

      final long recordPointer = sortedRecords.packedRecordPointer.getRecordPointer();
      final int recordLength = PlatformDependent.UNSAFE.getInt(
        memoryManager.getPage(recordPointer), memoryManager.getOffsetInPage(recordPointer));
      PlatformDependent.copyMemory(
        memoryManager.getPage(recordPointer),
        memoryManager.getOffsetInPage(recordPointer) + 4, // skip over record length
        arr,
        PlatformDependent.BYTE_ARRAY_OFFSET,
        recordLength);
      assert (writer != null);  // To suppress an IntelliJ warning
      writer.write(arr, 0, recordLength);
      // TODO: add a test that detects whether we leave this call out:
      writer.recordWritten();
    }

    if (writer != null) {
      writer.commitAndClose();
      spillInfo.partitionLengths[currentPartition] = writer.fileSegment().length();
    }
    return spillInfo;
  }

  @VisibleForTesting
  public void spill() throws IOException {
    final SpillInfo spillInfo = writeSpillFile();

    final long sorterMemoryUsage = sorter.getMemoryUsage();
    sorter = null;
    shuffleMemoryManager.release(sorterMemoryUsage);
    final long spillSize = freeMemory();
    taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);
    taskContext.taskMetrics().incDiskBytesSpilled(spillInfo.file.length());
    final long threadId = Thread.currentThread().getId();
    // TODO: messy; log _before_ spill
    logger.info("Thread " + threadId + " spilling in-memory map of " +
      org.apache.spark.util.Utils.bytesToString(spillSize) + " to disk (" +
          (spills.size() + ((spills.size() > 1) ? " times" : " time")) + " so far)");
    openSorter();
  }

  private long freeMemory() {
    long memoryFreed = 0;
    final Iterator<MemoryBlock> iter = allocatedPages.iterator();
    while (iter.hasNext()) {
      memoryManager.freePage(iter.next());
      shuffleMemoryManager.release(PAGE_SIZE);
      memoryFreed += PAGE_SIZE;
      iter.remove();
    }
    currentPage = null;
    currentPagePosition = -1;
    return memoryFreed;
  }

  private void ensureSpaceInDataPage(int requiredSpace) throws Exception {
    // TODO: merge these steps to first calculate total memory requirements for this insert,
    // then try to acquire; no point in acquiring sort buffer only to spill due to no space in the
    // data page.
    if (!sorter.hasSpaceForAnotherRecord() && spillingEnabled) {
      final long oldSortBufferMemoryUsage = sorter.getMemoryUsage();
      final long memoryToGrowSortBuffer = oldSortBufferMemoryUsage * 2;
      final long memoryAcquired = shuffleMemoryManager.tryToAcquire(memoryToGrowSortBuffer);
      if (memoryAcquired < memoryToGrowSortBuffer) {
        shuffleMemoryManager.release(memoryAcquired);
        spill();
      } else {
        sorter.expandSortBuffer();
        shuffleMemoryManager.release(oldSortBufferMemoryUsage);
      }
    }

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
      if (spillingEnabled) {
        final long memoryAcquired = shuffleMemoryManager.tryToAcquire(PAGE_SIZE);
        if (memoryAcquired < PAGE_SIZE) {
          shuffleMemoryManager.release(memoryAcquired);
          spill();
          final long memoryAcquiredAfterSpill = shuffleMemoryManager.tryToAcquire(PAGE_SIZE);
          if (memoryAcquiredAfterSpill != PAGE_SIZE) {
            shuffleMemoryManager.release(memoryAcquiredAfterSpill);
            throw new Exception("Can't allocate memory!");
          }
        }
      }
      currentPage = memoryManager.allocatePage(PAGE_SIZE);
      currentPagePosition = currentPage.getBaseOffset();
      allocatedPages.add(currentPage);
      logger.info("Acquired new page! " + allocatedPages.size() * PAGE_SIZE);
    }
  }

  public void insertRecord(
      Object recordBaseObject,
      long recordBaseOffset,
      int lengthInBytes,
      int prefix) throws Exception {
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

  public SpillInfo[] closeAndGetSpills() throws IOException {
    if (sorter != null) {
      writeSpillFile();
    }
    return spills.toArray(new SpillInfo[0]);
  }

}
