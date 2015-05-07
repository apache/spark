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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.spark.storage.*;
import scala.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleMemoryManager;
import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.TaskMemoryManager;

/**
 * An external sorter that is specialized for sort-based shuffle.
 * <p>
 * Incoming records are appended to data pages. When all records have been inserted (or when the
 * current thread's shuffle memory limit is reached), the in-memory records are sorted according to
 * their partition ids (using a {@link UnsafeShuffleSorter}). The sorted records are then written
 * to a single output file (or multiple files, if we've spilled). The format of the output files is
 * the same as the format of the final output file written by
 * {@link org.apache.spark.shuffle.sort.SortShuffleWriter}: each output partition's records are
 * written as a single serialized, compressed stream that can be read with a new decompression and
 * deserialization stream.
 * <p>
 * Unlike {@link org.apache.spark.util.collection.ExternalSorter}, this sorter does not merge its
 * spill files. Instead, this merging is performed in {@link UnsafeShuffleWriter}, which uses a
 * specialized merge procedure that avoids extra serialization/deserialization.
 */
public final class UnsafeShuffleExternalSorter {

  private final Logger logger = LoggerFactory.getLogger(UnsafeShuffleExternalSorter.class);

  private static final int SER_BUFFER_SIZE = 1024 * 1024;  // TODO: tune this / don't duplicate
  private static final int PAGE_SIZE = 1024 * 1024;  // TODO: tune this

  private final int initialSize;
  private final int numPartitions;
  private final TaskMemoryManager memoryManager;
  private final ShuffleMemoryManager shuffleMemoryManager;
  private final BlockManager blockManager;
  private final TaskContext taskContext;
  private final boolean spillingEnabled;

  /** The buffer size to use when writing spills using DiskBlockObjectWriter */
  private final int fileBufferSize;

  /**
   * Memory pages that hold the records being sorted. The pages in this list are freed when
   * spilling, although in principle we could recycle these pages across spills (on the other hand,
   * this might not be necessary if we maintained a pool of re-usable pages in the TaskMemoryManager
   * itself).
   */
  private final LinkedList<MemoryBlock> allocatedPages = new LinkedList<MemoryBlock>();

  private final LinkedList<SpillInfo> spills = new LinkedList<SpillInfo>();

  // All three of these variables are reset after spilling:
  private UnsafeShuffleSorter sorter;
  private MemoryBlock currentPage = null;
  private long currentPagePosition = -1;

  public UnsafeShuffleExternalSorter(
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

  /**
   * Allocates a new sorter. Called when opening the spill writer for the first time and after
   * each spill.
   */
  private void openSorter() throws IOException {
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

  /**
   * Sorts the in-memory records and writes the sorted records to a spill file.
   * This method does not free the sort data structures.
   */
  private SpillInfo writeSpillFile() throws IOException {
    // This call performs the actual sort.
    final UnsafeShuffleSorter.UnsafeShuffleSorterIterator sortedRecords =
      sorter.getSortedIterator();

    // Currently, we need to open a new DiskBlockObjectWriter for each partition; we can avoid this
    // after SPARK-5581 is fixed.
    BlockObjectWriter writer = null;

    // Small writes to DiskBlockObjectWriter will be fairly inefficient. Since there doesn't seem to
    // be an API to directly transfer bytes from managed memory to the disk writer, we buffer
    // records in a byte array. This array only needs to be big enough to hold a single record.
    final byte[] arr = new byte[SER_BUFFER_SIZE];

    // Because this output will be read during shuffle, its compression codec must be controlled by
    // spark.shuffle.compress instead of spark.shuffle.spill.compress, so we need to use
    // createTempShuffleBlock here; see SPARK-3426 for more details.
    final Tuple2<TempShuffleBlockId, File> spilledFileInfo =
      blockManager.diskBlockManager().createTempShuffleBlock();
    final File file = spilledFileInfo._2();
    final BlockId blockId = spilledFileInfo._1();
    final SpillInfo spillInfo = new SpillInfo(numPartitions, file, blockId);

    // Unfortunately, we need a serializer instance in order to construct a DiskBlockObjectWriter.
    // Our write path doesn't actually use this serializer (since we end up calling the `write()`
    // OutputStream methods), but DiskBlockObjectWriter still calls some methods on it. To work
    // around this, we pass a dummy no-op serializer.
    final SerializerInstance ser = DummySerializerInstance.INSTANCE;
    // TODO: audit the metrics-related code and ensure proper metrics integration:
    // It's not clear how we should handle shuffle write metrics for spill files; currently, Spark
    // doesn't report IO time spent writing spill files (see SPARK-7413). This method,
    // writeSpillFile(), is called both when writing spill files and when writing the single output
    // file in cases where we didn't spill. As a result, we don't necessarily know whether this
    // should be reported as bytes spilled or as shuffle bytes written. We could defer the updating
    // of these metrics until the end of the shuffle write, but that would mean that that users
    // wouldn't get useful metrics updates in the UI from long-running tasks. Given this complexity,
    // I'm deferring these decisions to a separate follow-up commit or patch.
    writer =
      blockManager.getDiskWriter(blockId, file, ser, fileBufferSize, new ShuffleWriteMetrics());

    int currentPartition = -1;
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
        writer =
          blockManager.getDiskWriter(blockId, file, ser, fileBufferSize, new ShuffleWriteMetrics());
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
      // If `writeSpillFile()` was called from `closeAndGetSpills()` and no records were inserted,
      // then the spill file might be empty. Note that it might be better to avoid calling
      // writeSpillFile() in that case.
      if (currentPartition != -1) {
        spillInfo.partitionLengths[currentPartition] = writer.fileSegment().length();
        spills.add(spillInfo);
      }
    }
    return spillInfo;
  }

  /**
   * Sort and spill the current records in response to memory pressure.
   */
  private void spill() throws IOException {
    final long threadId = Thread.currentThread().getId();
    logger.info("Thread " + threadId + " spilling sort data of " +
      org.apache.spark.util.Utils.bytesToString(getMemoryUsage()) + " to disk (" +
      (spills.size() + (spills.size() > 1 ? " times" : " time")) + " so far)");

    final SpillInfo spillInfo = writeSpillFile();
    final long sorterMemoryUsage = sorter.getMemoryUsage();
    sorter = null;
    shuffleMemoryManager.release(sorterMemoryUsage);
    final long spillSize = freeMemory();
    taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);
    taskContext.taskMetrics().incDiskBytesSpilled(spillInfo.file.length());

    openSorter();
  }

  private long getMemoryUsage() {
    return sorter.getMemoryUsage() + (allocatedPages.size() * PAGE_SIZE);
  }

  private long freeMemory() {
    long memoryFreed = 0;
    for (MemoryBlock block : allocatedPages) {
      memoryManager.freePage(block);
      shuffleMemoryManager.release(block.size());
      memoryFreed += block.size();
    }
    allocatedPages.clear();
    currentPage = null;
    currentPagePosition = -1;
    return memoryFreed;
  }

  /**
   * Checks whether there is enough space to insert a new record into the sorter. If there is
   * insufficient space, either allocate more memory or spill the current sort data (if spilling
   * is enabled), then insert the record.
   */
  private void ensureSpaceInDataPage(int requiredSpace) throws IOException {
    // TODO: we should re-order the `if` cases in this function so that the most common case (there
    // is enough space) appears first.

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
      throw new IOException("Required space " + requiredSpace + " is greater than page size (" +
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
            throw new IOException("Can't allocate memory!");
          }
        }
      }
      currentPage = memoryManager.allocatePage(PAGE_SIZE);
      currentPagePosition = currentPage.getBaseOffset();
      allocatedPages.add(currentPage);
    }
  }

  /**
   * Write a record to the shuffle sorter.
   */
  public void insertRecord(
      Object recordBaseObject,
      long recordBaseOffset,
      int lengthInBytes,
      int partitionId) throws IOException {
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

    sorter.insertRecord(recordAddress, partitionId);
  }

  /**
   * Close the sorter, causing any buffered data to be sorted and written out to disk.
   *
   * @return metadata for the spill files written by this sorter. If no records were ever inserted
   *         into this sorter, then this will return an empty array.
   * @throws IOException
   */
  public SpillInfo[] closeAndGetSpills() throws IOException {
    if (sorter != null) {
      writeSpillFile();
      freeMemory();
    }
    return spills.toArray(new SpillInfo[0]);
  }

}
