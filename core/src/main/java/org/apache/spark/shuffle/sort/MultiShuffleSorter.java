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

package org.apache.spark.shuffle.sort;

/**
 * A multi shuffle sorter that internally dispatches to multiple external shuffle sorters.
 * <p>
 * The benefit of running multiple shuffle sorters is that we can parallelize the insertion and
 * sort, spill cycles for better I/O performance.  A shuffle sorter can be spilling to disk while
 * another one is inserting records from a different file system or network.  This has been
 * shown to reduce the end to end latency of large shuffle operations.  The multi shuffle sorter
 * manages the pool of shuffle sorters.
 */

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import org.apache.spark.SparkConf;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.ThreadUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import scala.collection.JavaConversions;

public class MultiShuffleSorter extends ShuffleSorter {
  private final Logger logger = LoggerFactory.getLogger(MultiShuffleSorter.class);

  private int numSorters;
  private TaskMetrics[] sorterMetrics;
  private List<ShuffleSorter> allSorters;
  private LinkedBlockingDeque<ShuffleSorter> availableSorters;
  private ShuffleSorter currentSorter;
  private ExecutorService executorService;
  private ReentrantLock spillCompleteLock = new ReentrantLock();
  private final Condition spillCompleteCondition = spillCompleteLock.newCondition();

  private TaskMetrics taskMetrics;
  private boolean cleanedUp;

  /**
   * If the shuffle sorter has allocated at least this number of bytes, then trigger a spill.
   */
  private long spillThresholdBytes;

  /**
   * Minimum bound on spill threshold.  If we change the spill threshold, then we never set
   * below this lower limit.
   */
  private long minSpillThresholdBytes;

  /**
   * Capture exceptions that may have occurred in asynchronously and propagate them on the
   * main spill thread.
   */
  private AtomicReference<IOException> pendingException;

  private long waitingIntervalMs;

  MultiShuffleSorter(
      TaskMemoryManager memoryManager,
      BlockManager blockManager,
      TaskMetrics taskMetrics,
      int initialSize,
      int numPartitions,
      SparkConf conf,
      int numSorters,
      long initialMaxMemoryPerSorter,
      ShuffleSorterFactory factory) {
    super(memoryManager,
      (int) Math.min(PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES, memoryManager.pageSizeBytes()),
      memoryManager.getTungstenMemoryMode());
    assert(numSorters >= 1);

    this.numSorters = numSorters;
    this.sorterMetrics = new TaskMetrics[numSorters];

    this.allSorters = new ArrayList<>(numSorters);
    this.availableSorters = new LinkedBlockingDeque<>(numSorters);

    for (int i=0; i < numSorters; i++) {
      sorterMetrics[i] = TaskMetrics.empty();
      ShuffleSorter sorter = factory.createShuffleExternalSorter(
        memoryManager, blockManager, sorterMetrics[i], initialSize, numPartitions, conf, true);
      allSorters.add(sorter);
      availableSorters.add(sorter);
    }

    this.executorService = ThreadUtils.newDaemonFixedThreadPool(numSorters, "multishuffle-sorter-spill-thread");
    this.taskMetrics = taskMetrics;
    this.cleanedUp = false;

    this.waitingIntervalMs = conf.getLong("spark.shuffle.writer.waiting.interval.ms", 10 * 1000);
    this.minSpillThresholdBytes = conf.getSizeAsBytes("spark.shuffle.writer.spill.min.size", "100m");
    this.spillThresholdBytes = Math.max(minSpillThresholdBytes, initialMaxMemoryPerSorter);
    this.pendingException = new AtomicReference<>(null);

    logger.info(
      "MultiShuffleSorter on thread [{}] started with {} sorters and spill threshold {} bytes",
      Thread.currentThread().getId(), numSorters, spillThresholdBytes);

    try {
      currentSorter = nextAvailableSorter();
    } catch (IOException e) {
      pendingException.set(e);
    }
  }

  /**
   * Note this is not called because the MultiShuffleSorter oes not directly allocate memory,
   * rather this is delegated to individual ShuffleShuffleSorters.
   */
  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    doAsyncSpillOnCurrentSorter();
    currentSorter = nextAvailableSorter();
    return 0L;
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  @Override
  public long getPeakMemoryUsedBytes() {
    long peakMemoryUsedBytes = 0;
    for (int i=0; i < numSorters; i++) {
      peakMemoryUsedBytes += allSorters.get(i).getPeakMemoryUsedBytes();
    }
    return peakMemoryUsedBytes;
  }

  /**
   * Force all memory and spill files to be deleted; called by shuffle error-handling code.
   */
  @Override
  public void cleanupResources() {
    logger.info("Cleaning up shuffle sorters.");
    if (!cleanedUp) {

      if (currentSorter != null) {
        availableSorters.addLast(currentSorter);
        currentSorter = null;
      }

      for (int i = 0; i < numSorters; i++) {
        try {
          ShuffleSorter sorter = nextAvailableSorter();
          logger.info("Cleaning up resources for sorter {}", sorter);
          sorter.cleanupResources();
        } catch (IOException e) {
          logger.error("Failed to clean up sorter", e);
        }
      }

      cleanedUp = true;
    }

    logger.info("MultiShuffleSorter done with clean up");
  }

  /**
   * Write a record to the shuffle sorter.
   */
  @Override
  public void insertRecord(final Object recordBase, long recordOffset, int length, int partitionId)
    throws IOException {
    checkForPendingException();

    if (!hasSpaceForInsertRecord()) {
      logger.info("Forcing a spill on sorter {} because over spill threshold " +
          "(used {} bytes > spill threshold {})",
          currentSorter, currentSorter.getUsed(), spillThresholdBytes);
      spill();
    }

    boolean retryInsert;
    do {
      retryInsert = false;
      checkForPendingException();
      try {
        currentSorter.insertRecord(recordBase, recordOffset, length, partitionId);
      } catch (OutOfMemoryError e) {
        // If the current sorter fails to allocate memory and there's at least one
        // outstanding sorter in the process of spilling, then we should wait for
        // it to spill and tune the spillThresholdBytes down.
        int numAvailableSorters = getNumAvailableSorters();
        if (allSorters.size() - numAvailableSorters > 1) {
          long newSpillThresholdBytes = Math.max(minSpillThresholdBytes, (spillThresholdBytes * 9) / 10);

          logger.info("Sorter {} ran out of memory, waiting for asynchronous " +
            "spilling to finish, adjusting spill threshold to {} bytes", currentSorter,
            newSpillThresholdBytes);
          spillThresholdBytes = newSpillThresholdBytes;

          waitForAvailableSorters(numAvailableSorters + 1);

          retryInsert = true;
        } else {
          // If there are no other sorters to spill and we still run out of memory, it
          // could be our initial spillThresholdBytes is just too far off.  In this case
          // we can spill this sorter and re-adjust the spillThresholdBytes based on peak
          // memory used.

          // Calculate all used memory across sorters
          long used = 0;
          for (int i = 0; i < allSorters.size(); i++) {
            used += allSorters.get(i).getUsed();
          }

          // Calculate the spillThresholdBytes based on the total allocated memory across
          // all sorters divided across the number of sorters.  Also reduce by 10% to
          // buffer overhead to avoid OOM in the future.
          long newSpillThresholdBytes = Math.max(minSpillThresholdBytes, ((used / numSorters) * 9) / 10);

          logger.info("Sorter {} ran out of memory, no other sorter to spill!  Adjusting spill " +
            "threshold to {} bytes", currentSorter, newSpillThresholdBytes);

          spillThresholdBytes = newSpillThresholdBytes;

          long spilledBytes = currentSorter.spill(Long.MAX_VALUE, currentSorter);

          if (spilledBytes > 0) {
            retryInsert = true;
          } else {
            // If spilling didn't release any memory then we are no different a situation
            // and must give up!
            logger.warn("Failed to free any memory when spilling, so throwing original error!");
            throw e;
          }
        }
      }
    } while (retryInsert);
  }

  @Override
  public boolean hasSpaceForInsertRecord() {
    return currentSorter.getUsed() < spillThresholdBytes || currentSorter.hasSpaceForInsertRecord();
  }

  private void doAsyncSpillOnCurrentSorter() {
    final ShuffleSorter sorter = currentSorter;

    executorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          logger.info("Sorter {} starting to spill.", sorter);
          sorter.spill();
        } catch (IOException e) {
          logger.error("Sorter {} failed to spill.", e);
          pendingException.set(e);
        } catch (Throwable e) {
          pendingException.set(new IOException("Failed to spill", e));
        } finally {
          logger.info("Sorter {} finished spilling and is available.", sorter);
          availableSorters.add(sorter);
          signalAsyncSpillComplete();
        }
      }
    });
  }

  /**
   * Close the sorter, causing any buffered data to be sorted and written out to disk.
   *
   * @return metadata for the spill files written by this sorter. If no records were ever inserted
   *         into this sorter, then this will return an empty array.
   * @throws IOException
   */
  @Override
  public SpillInfo[] closeAndGetSpills() throws IOException {
    ShuffleSorter lastSorter = currentSorter;

    final List<SpillInfo> spillInfos = new ArrayList<>();
    for (int i = 0; i < numSorters - 1; i++) {
      ShuffleSorter sorter = nextAvailableSorter();
      spillInfos.addAll(Lists.newArrayList(sorter.closeAndGetSpills()));
    }

    checkForPendingException();

    // The UnsafeShuffleWriter makes an assumption that the last spill info comes from
    // a non-spilled last file write, and adjusts the shuffle write metrics accordingly.
    // To preserve this assumption, we ensure the current sorter spills is returned last.
    spillInfos.addAll(Lists.newArrayList(lastSorter.closeAndGetSpills()));

    updateTaskMetrics();

    availableSorters.addAll(allSorters);

    return spillInfos.toArray(new SpillInfo[0]);
  }

  /**
   * This updates the task metrics based on the aggregation of metrics across the external sorters.U
   * This also includes all shuffle write metrics.
   */
  private void updateTaskMetrics() {
    for (TaskMetrics metrics: sorterMetrics) {
      for (AccumulatorV2<?,?> accumulator : JavaConversions.asJavaIterable(metrics.accumulators())) {
        String accumulatorName = accumulator.name().get().toString();
        AccumulatorV2<?,?> taskAccumulator = taskMetrics.lookForAccumulatorByName(accumulatorName).get();
        assert(taskAccumulator.getClass().getName().equals(accumulator.getClass().getName()));
        taskAccumulator.merge(taskAccumulator.getClass().cast(accumulator));
      }
    }
  }

  private void waitForAvailableSorter() throws IOException {
    waitForAvailableSorters(1);
  }

  @VisibleForTesting
  protected void waitForAvailableSorters(int numSorters) throws IOException {
    try {
      spillCompleteLock.lock();
      while (availableSorters.size() < numSorters) {
        waitForAsyncSpillComplete();
      }
    } finally {
      spillCompleteLock.unlock();
    }
  }

  private void waitForAsyncSpillComplete() throws IOException {
    spillCompleteLock.lock();
    try {
      spillCompleteCondition.await();
    } catch (InterruptedException e) {
      throw new IOException("Waiting for available sorter interrupted", e);
    } finally {
      spillCompleteLock.unlock();
    }
  }

  private void signalAsyncSpillComplete() {
    spillCompleteLock.lock();
    try {
      spillCompleteCondition.signalAll();
    } finally {
      spillCompleteLock.unlock();
    }
  }

  private ShuffleSorter nextAvailableSorter() throws IOException {
    ShuffleSorter sorter;
    try {
      sorter = availableSorters.takeFirst();
    } catch (InterruptedException e) {
      throw new IOException("Failed to get next available sorter.", e);
    }
    return sorter;
  }

  private void checkForPendingException() throws IOException {
    if (pendingException.get() != null) {
      throw pendingException.get();
    }
  }

  @VisibleForTesting
  protected long getSpillThresholdBytes() {
    return spillThresholdBytes;
  }

  @VisibleForTesting
  protected int getNumAvailableSorters() {
    return availableSorters.size();
  }
}
