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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.storage.BlockManager;

/**
 * Performs a bounded multi-round k-way merge of spill files.
 *
 * <p>When the number of spill files exceeds the merge factor, this class merges them in
 * rounds of at most {@code mergeFactor} files at a time, writing intermediate results to
 * new spill files. This bounds the number of concurrently open file readers and prevents
 * OOM during the merge phase of external sorting.</p>
 *
 * <p>Unlike the default single-round merge in {@link UnsafeSorterSpillMerger} which opens
 * all spill readers simultaneously (~3MB per reader), this approach caps concurrent readers
 * at {@code mergeFactor}, making memory usage predictable regardless of spill count.</p>
 *
 * <p><b>Trade-off:</b> Intermediate merge rounds incur additional disk I/O — each record
 * is read and rewritten once per intermediate round. A smaller merge factor requires more
 * rounds (and thus more I/O), while a larger factor uses more memory. The default factor
 * of 64 typically requires at most one intermediate round for up to ~4000 spill files.
 * Consumed files (both original spills and prior-round intermediates) are deleted eagerly
 * after each group merge, keeping peak disk overhead to roughly one group's worth of data
 * above the original spill total.</p>
 */
final class UnsafeSorterBoundedSpillMerger {

  private static final SparkLogger logger =
      SparkLoggerFactory.getLogger(UnsafeSorterBoundedSpillMerger.class);

  private final int mergeFactor;
  private final RecordComparator recordComparator;
  private final PrefixComparator prefixComparator;
  private final BlockManager blockManager;
  private final SerializerManager serializerManager;
  private final int fileBufferSizeBytes;
  private int intermediateRoundsCompleted;
  // Tracks files created by intermediate merge rounds for safety-net cleanup
  // in cleanupIntermediateFiles() if merge() fails partway through.
  private final Set<File> intermediateFiles = new HashSet<>();

  UnsafeSorterBoundedSpillMerger(
      int mergeFactor,
      RecordComparator recordComparator,
      PrefixComparator prefixComparator,
      BlockManager blockManager,
      SerializerManager serializerManager,
      int fileBufferSizeBytes) {
    this.mergeFactor = mergeFactor;
    this.recordComparator = recordComparator;
    this.prefixComparator = prefixComparator;
    this.blockManager = blockManager;
    this.serializerManager = serializerManager;
    this.fileBufferSizeBytes = fileBufferSizeBytes;
  }

  /**
   * Performs bounded multi-round merge of spill writers and returns a sorted iterator.
   *
   * <p>If {@code inMemIterator} is non-null, it is included in the final merge round
   * (not spilled to disk in intermediate rounds).</p>
   *
   * @param spillWriters the list of spill writers to merge
   * @param inMemIterator optional in-memory sorted iterator to include in the final merge
   * @return a sorted iterator over all records
   */
  public UnsafeSorterIterator merge(
      List<UnsafeSorterSpillWriter> spillWriters,
      @Nullable UnsafeSorterIterator inMemIterator) throws IOException {

    List<UnsafeSorterSpillWriter> spillsToMerge = new ArrayList<>(spillWriters);
    int round = 0;

    while (spillsToMerge.size() > mergeFactor) {
      round++;
      List<UnsafeSorterSpillWriter> nextRoundSpills = new ArrayList<>();
      long roundBytesWritten = 0;

      logger.info("Bounded merge round {}: merging {} spill files with merge factor {}",
          MDC.of(LogKeys.MERGE_ROUND, round),
          MDC.of(LogKeys.NUM_SPILL_WRITERS, spillsToMerge.size()),
          MDC.of(LogKeys.MERGE_FACTOR, mergeFactor));

      // Partition writers into groups bounded by both merge factor (for memory) and
      // Integer.MAX_VALUE total records (to prevent int overflow in spill file headers).
      List<List<UnsafeSorterSpillWriter>> groups = partitionWriters(spillsToMerge);
      for (List<UnsafeSorterSpillWriter> group : groups) {
        if (group.size() == 1) {
          // Single file in this group, no merge needed — carry forward
          nextRoundSpills.add(group.get(0));
        } else {
          ShuffleWriteMetrics groupMetrics = new ShuffleWriteMetrics();
          UnsafeSorterSpillWriter merged = mergeGroupToSpill(group, groupMetrics);
          nextRoundSpills.add(merged);
          roundBytesWritten += groupMetrics.bytesWritten();

          // Eagerly delete all consumed files (original + intermediate) to
          // reduce peak disk usage.
          deleteConsumedFiles(group);
        }
      }

      logger.info("Bounded merge round {} complete: wrote {} bytes to {} intermediate files",
          MDC.of(LogKeys.MERGE_ROUND, round),
          MDC.of(LogKeys.MERGE_BYTES_WRITTEN, roundBytesWritten),
          MDC.of(LogKeys.NUM_SPILL_WRITERS, nextRoundSpills.size()));
      intermediateRoundsCompleted++;

      // If no merging occurred this round (e.g., all groups were size 1 due to
      // high per-writer record counts), break to avoid an infinite loop.
      if (nextRoundSpills.size() == spillsToMerge.size()) {
        logger.warn("Bounded merge made no progress in round {} — {} writers remain, " +
            "falling back to unbounded final merge",
            MDC.of(LogKeys.MERGE_ROUND, round),
            MDC.of(LogKeys.NUM_SPILL_WRITERS, spillsToMerge.size()));
        spillsToMerge = nextRoundSpills;
        break;
      }
      spillsToMerge = nextRoundSpills;
    }

    // Final merge: remaining writers fit within the merge factor.
    logger.info("Final merge round: merging {} spill files",
        MDC.of(LogKeys.NUM_SPILL_WRITERS, spillsToMerge.size()));

    final UnsafeSorterSpillMerger finalMerger = new UnsafeSorterSpillMerger(
        recordComparator, prefixComparator,
        spillsToMerge.size() + (inMemIterator != null && inMemIterator.hasNext() ? 1 : 0));
    for (UnsafeSorterSpillWriter writer : spillsToMerge) {
      finalMerger.addSpillIfNotEmpty(writer.getReader(serializerManager));
    }
    if (inMemIterator != null) {
      finalMerger.addSpillIfNotEmpty(inMemIterator);
    }
    return finalMerger.getSortedIterator();
  }

  /**
   * Partitions writers into groups bounded by both the merge factor (for memory) and
   * Integer.MAX_VALUE total records (to prevent int overflow in spill file headers).
   */
  private List<List<UnsafeSorterSpillWriter>> partitionWriters(
      List<UnsafeSorterSpillWriter> writers) {
    List<List<UnsafeSorterSpillWriter>> groups = new ArrayList<>();
    List<UnsafeSorterSpillWriter> currentGroup = new ArrayList<>();
    long currentGroupRecords = 0;

    for (UnsafeSorterSpillWriter writer : writers) {
      long writerRecords = writer.recordsSpilled();
      if (!currentGroup.isEmpty()
          && (currentGroup.size() >= mergeFactor
              || currentGroupRecords + writerRecords > Integer.MAX_VALUE)) {
        groups.add(currentGroup);
        currentGroup = new ArrayList<>();
        currentGroupRecords = 0;
      }
      currentGroup.add(writer);
      currentGroupRecords += writerRecords;
    }
    if (!currentGroup.isEmpty()) {
      groups.add(currentGroup);
    }
    return groups;
  }

  /**
   * Merges a group of spill writers into a single new spill file.
   */
  private UnsafeSorterSpillWriter mergeGroupToSpill(
      List<UnsafeSorterSpillWriter> group,
      ShuffleWriteMetrics writeMetrics) throws IOException {
    long totalRecords = 0;
    UnsafeSorterSpillMerger merger = new UnsafeSorterSpillMerger(
        recordComparator, prefixComparator, group.size());

    for (UnsafeSorterSpillWriter sw : group) {
      UnsafeSorterSpillReader reader = sw.getReader(serializerManager);
      totalRecords += reader.getNumRecords();
      merger.addSpillIfNotEmpty(reader);
    }

    // Defensive check: partitionWriters() already bounds each group's total records
    // to Integer.MAX_VALUE (since individual writers use int record counts, overflow
    // can only happen from summing multiple writers). This assertion guards against
    // bugs in the partitioning logic, as UnsafeSorterSpillWriter requires an int.
    if (totalRecords > Integer.MAX_VALUE) {
      throw new IllegalStateException(
          "Group record count exceeds Integer.MAX_VALUE: " + totalRecords);
    }
    UnsafeSorterSpillWriter outputWriter = new UnsafeSorterSpillWriter(
        blockManager, fileBufferSizeBytes, writeMetrics, (int) totalRecords);

    // Track the intermediate file immediately so cleanupIntermediateFiles() can
    // clean it up if an exception occurs during the merge write loop below.
    intermediateFiles.add(outputWriter.getFile());

    UnsafeSorterIterator sorted = merger.getSortedIterator();
    while (sorted.hasNext()) {
      sorted.loadNext();
      outputWriter.write(
          sorted.getBaseObject(), sorted.getBaseOffset(),
          sorted.getRecordLength(), sorted.getKeyPrefix());
    }
    outputWriter.close();
    return outputWriter;
  }

  private void deleteConsumedFiles(List<UnsafeSorterSpillWriter> writers) {
    for (UnsafeSorterSpillWriter writer : writers) {
      File file = writer.getFile();
      // Delete all consumed files eagerly — both original spill files and intermediate
      // files from prior rounds. UnsafeExternalSorter.deleteSpillFiles() handles
      // already-deleted files gracefully via file.exists() check.
      if (file != null && file.exists()) {
        if (!file.delete()) {
          logger.warn("Failed to delete consumed spill file {}",
              MDC.of(LogKeys.PATH, file.getAbsolutePath()));
        }
      }
    }
  }

  int getIntermediateRoundsCompleted() {
    return intermediateRoundsCompleted;
  }

  /**
   * Cleans up any intermediate files created during multi-round merge.
   * Called during resource cleanup as a safety net for files not yet consumed.
   */
  public void cleanupIntermediateFiles() {
    for (File file : intermediateFiles) {
      if (file != null && file.exists()) {
        if (!file.delete()) {
          logger.warn("Failed to delete intermediate spill file {}",
              MDC.of(LogKeys.PATH, file.getAbsolutePath()));
        }
      }
    }
    intermediateFiles.clear();
  }
}
