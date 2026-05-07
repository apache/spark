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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.UUID;

import scala.Tuple2$;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.internal.config.package$;
import org.apache.spark.memory.TestMemoryManager;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.storage.*;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.Utils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Answers.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.*;

public class UnsafeExternalSorterSuite {

  private final SparkConf conf = new SparkConf();

  final LinkedList<File> spillFilesCreated = new LinkedList<>();
  final TestMemoryManager memoryManager =
    new TestMemoryManager(conf.clone().set(package$.MODULE$.MEMORY_OFFHEAP_ENABLED(), false));
  final TaskMemoryManager taskMemoryManager = new TaskMemoryManager(memoryManager, 0);
  final SerializerManager serializerManager = new SerializerManager(
    new JavaSerializer(conf),
    conf.clone().set(package$.MODULE$.SHUFFLE_SPILL_COMPRESS(), false));
  // Use integer comparison for comparing prefixes (which are partition ids, in this case)
  final PrefixComparator prefixComparator = PrefixComparators.LONG;
  // Since the key fits within the 8-byte prefix, we don't need to do any record comparison, so
  // use a dummy comparator
  final RecordComparator recordComparator = new RecordComparator() {
    @Override
    public int compare(
      Object leftBaseObject,
      long leftBaseOffset,
      int leftBaseLength,
      Object rightBaseObject,
      long rightBaseOffset,
      int rightBaseLength) {
      return 0;
    }
  };

  File tempDir;
  @Mock(answer = RETURNS_SMART_NULLS) BlockManager blockManager;
  @Mock(answer = RETURNS_SMART_NULLS) DiskBlockManager diskBlockManager;
  @Mock(answer = RETURNS_SMART_NULLS) TaskContext taskContext;

  protected boolean shouldUseRadixSort() { return false; }

  private final long pageSizeBytes = conf.getSizeAsBytes(
          package$.MODULE$.BUFFER_PAGESIZE().key(), "4m");

  private final int spillElementsThreshold =
    (int) conf.get(package$.MODULE$.SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD());

  private final long spillSizeThreshold =
    (long) conf.get(package$.MODULE$.SHUFFLE_SPILL_MAX_SIZE_FORCE_SPILL_THRESHOLD());


  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    tempDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "unsafe-test");
    spillFilesCreated.clear();
    taskContext = mock(TaskContext.class);
    when(taskContext.taskMetrics()).thenReturn(new TaskMetrics());
    when(blockManager.diskBlockManager()).thenReturn(diskBlockManager);
    when(diskBlockManager.createTempLocalBlock()).thenAnswer(invocationOnMock -> {
      TempLocalBlockId blockId = new TempLocalBlockId(UUID.randomUUID());
      File file = File.createTempFile("spillFile", ".spill", tempDir);
      spillFilesCreated.add(file);
      return Tuple2$.MODULE$.apply(blockId, file);
    });
    when(blockManager.getDiskWriter(
      any(BlockId.class),
      any(File.class),
      any(SerializerInstance.class),
      anyInt(),
      any(ShuffleWriteMetrics.class))).thenAnswer(invocationOnMock -> {
        Object[] args = invocationOnMock.getArguments();

        return new DiskBlockObjectWriter(
          (File) args[1],
          serializerManager,
          (SerializerInstance) args[2],
          (Integer) args[3],
          false,
          (ShuffleWriteMetrics) args[4],
          (BlockId) args[0]
        );
      });
  }

  @AfterEach
  public void tearDown() {
    try {
      assertEquals(0L, taskMemoryManager.cleanUpAllAllocatedMemory());
    } finally {
      Utils.deleteRecursively(tempDir);
      tempDir = null;
    }
  }

  private void assertSpillFilesWereCleanedUp() {
    for (File spillFile : spillFilesCreated) {
      assertFalse(spillFile.exists(),
        "Spill file " + spillFile.getPath() + " was not cleaned up");
    }
  }

  private static void insertNumber(UnsafeExternalSorter sorter, int value) throws Exception {
    final int[] arr = new int[]{ value };
    sorter.insertRecord(arr, Platform.INT_ARRAY_OFFSET, 4, value, false);
  }

  private static void insertRecord(
      UnsafeExternalSorter sorter,
      int[] record,
      long prefix) throws IOException {
    sorter.insertRecord(record, Platform.INT_ARRAY_OFFSET, record.length * 4, prefix, false);
  }

  private UnsafeExternalSorter newSorter() throws IOException {
    return UnsafeExternalSorter.create(
      taskMemoryManager,
      blockManager,
      serializerManager,
      taskContext,
      () -> recordComparator,
      prefixComparator,
      /* initialSize */ 1024,
      pageSizeBytes,
      spillElementsThreshold,
      spillSizeThreshold,
      /* spillMergeFactor */ -1,
      shouldUseRadixSort());
  }

  @Test
  public void testSortingOnlyByPrefix() throws Exception {
    final UnsafeExternalSorter sorter = newSorter();
    insertNumber(sorter, 5);
    insertNumber(sorter, 1);
    insertNumber(sorter, 3);
    sorter.spill();
    insertNumber(sorter, 4);
    sorter.spill();
    insertNumber(sorter, 2);

    UnsafeSorterIterator iter = sorter.getSortedIterator();

    for (int i = 1; i <= 5; i++) {
      iter.loadNext();
      assertEquals(i, iter.getKeyPrefix());
      assertEquals(4, iter.getRecordLength());
      assertEquals(i, Platform.getInt(iter.getBaseObject(), iter.getBaseOffset()));
    }

    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void testSortingEmptyArrays() throws Exception {
    final UnsafeExternalSorter sorter = newSorter();
    sorter.insertRecord(null, 0, 0, 0, false);
    sorter.insertRecord(null, 0, 0, 0, false);
    sorter.spill();
    sorter.insertRecord(null, 0, 0, 0, false);
    sorter.spill();
    sorter.insertRecord(null, 0, 0, 0, false);
    sorter.insertRecord(null, 0, 0, 0, false);

    UnsafeSorterIterator iter = sorter.getSortedIterator();

    for (int i = 1; i <= 5; i++) {
      iter.loadNext();
      assertEquals(0, iter.getKeyPrefix());
      assertEquals(0, iter.getRecordLength());
    }

    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void testSortTimeMetric() throws Exception {
    final UnsafeExternalSorter sorter = newSorter();
    long prevSortTime = sorter.getSortTimeNanos();
    assertEquals(0, prevSortTime);

    sorter.insertRecord(null, 0, 0, 0, false);
    sorter.spill();
    assertTrue(sorter.getSortTimeNanos() > prevSortTime);
    prevSortTime = sorter.getSortTimeNanos();

    sorter.spill();  // no sort needed
    assertEquals(prevSortTime, sorter.getSortTimeNanos());

    sorter.insertRecord(null, 0, 0, 0, false);
    UnsafeSorterIterator iter = sorter.getSortedIterator();
    assertTrue(sorter.getSortTimeNanos() > prevSortTime);

    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void spillingOccursInResponseToMemoryPressure() throws Exception {
    final UnsafeExternalSorter sorter = newSorter();
    // This should be enough records to completely fill up a data page:
    final int numRecords = (int) (pageSizeBytes / (4 + 4));
    for (int i = 0; i < numRecords; i++) {
      insertNumber(sorter, numRecords - i);
    }
    assertEquals(1, sorter.getNumberOfAllocatedPages());
    memoryManager.markExecutionAsOutOfMemoryOnce();
    // The insertion of this record should trigger a spill:
    insertNumber(sorter, 0);
    // Ensure that spill files were created
    assertTrue(tempDir.listFiles().length >= 1);
    // Read back the sorted data:
    UnsafeSorterIterator iter = sorter.getSortedIterator();

    int i = 0;
    while (iter.hasNext()) {
      iter.loadNext();
      assertEquals(i, iter.getKeyPrefix());
      assertEquals(4, iter.getRecordLength());
      assertEquals(i, Platform.getInt(iter.getBaseObject(), iter.getBaseOffset()));
      i++;
    }
    assertEquals(numRecords + 1, i);
    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void testFillingPage() throws Exception {
    final UnsafeExternalSorter sorter = newSorter();
    byte[] record = new byte[16];
    while (sorter.getNumberOfAllocatedPages() < 2) {
      sorter.insertRecord(record, Platform.BYTE_ARRAY_OFFSET, record.length, 0, false);
    }
    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void sortingRecordsThatExceedPageSize() throws Exception {
    final UnsafeExternalSorter sorter = newSorter();
    final int[] largeRecord = new int[(int) pageSizeBytes + 16];
    Arrays.fill(largeRecord, 456);
    final int[] smallRecord = new int[100];
    Arrays.fill(smallRecord, 123);

    insertRecord(sorter, largeRecord, 456);
    sorter.spill();
    insertRecord(sorter, smallRecord, 123);
    sorter.spill();
    insertRecord(sorter, smallRecord, 123);
    insertRecord(sorter, largeRecord, 456);

    UnsafeSorterIterator iter = sorter.getSortedIterator();
    // Small record
    assertTrue(iter.hasNext());
    iter.loadNext();
    assertEquals(123, iter.getKeyPrefix());
    assertEquals(smallRecord.length * 4, iter.getRecordLength());
    assertEquals(123, Platform.getInt(iter.getBaseObject(), iter.getBaseOffset()));
    // Small record
    assertTrue(iter.hasNext());
    iter.loadNext();
    assertEquals(123, iter.getKeyPrefix());
    assertEquals(smallRecord.length * 4, iter.getRecordLength());
    assertEquals(123, Platform.getInt(iter.getBaseObject(), iter.getBaseOffset()));
    // Large record
    assertTrue(iter.hasNext());
    iter.loadNext();
    assertEquals(456, iter.getKeyPrefix());
    assertEquals(largeRecord.length * 4, iter.getRecordLength());
    assertEquals(456, Platform.getInt(iter.getBaseObject(), iter.getBaseOffset()));
    // Large record
    assertTrue(iter.hasNext());
    iter.loadNext();
    assertEquals(456, iter.getKeyPrefix());
    assertEquals(largeRecord.length * 4, iter.getRecordLength());
    assertEquals(456, Platform.getInt(iter.getBaseObject(), iter.getBaseOffset()));

    assertFalse(iter.hasNext());
    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void forcedSpillingWithReadIterator() throws Exception {
    final UnsafeExternalSorter sorter = newSorter();
    long[] record = new long[100];
    int recordSize = record.length * 8;
    int n = (int) pageSizeBytes / recordSize * 3;
    for (int i = 0; i < n; i++) {
      record[0] = (long) i;
      sorter.insertRecord(record, Platform.LONG_ARRAY_OFFSET, recordSize, 0, false);
    }
    assertTrue(sorter.getNumberOfAllocatedPages() >= 2);
    UnsafeExternalSorter.SpillableIterator iter =
      (UnsafeExternalSorter.SpillableIterator) sorter.getSortedIterator();
    int lastv = 0;
    for (int i = 0; i < n / 3; i++) {
      iter.hasNext();
      iter.loadNext();
      assertEquals(i, Platform.getLong(iter.getBaseObject(), iter.getBaseOffset()));
      lastv = i;
    }
    assertTrue(iter.spill() > 0);
    assertEquals(0, iter.spill());
    assertEquals(lastv, Platform.getLong(iter.getBaseObject(), iter.getBaseOffset()));
    for (int i = n / 3; i < n; i++) {
      iter.hasNext();
      iter.loadNext();
      assertEquals(i, Platform.getLong(iter.getBaseObject(), iter.getBaseOffset()));
    }
    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void forcedSpillingNullsWithReadIterator() throws Exception {
    final UnsafeExternalSorter sorter = newSorter();
    long[] record = new long[100];
    final int recordSize = record.length * 8;
    final int n = (int) pageSizeBytes / recordSize * 3;
    for (int i = 0; i < n; i++) {
      boolean isNull = i % 2 == 0;
      sorter.insertRecord(record, Platform.LONG_ARRAY_OFFSET, recordSize, 0, isNull);
    }
    assertTrue(sorter.getNumberOfAllocatedPages() >= 2);

    UnsafeExternalSorter.SpillableIterator iter =
            (UnsafeExternalSorter.SpillableIterator) sorter.getSortedIterator();
    final int numRecordsToReadBeforeSpilling = n / 3;
    for (int i = 0; i < numRecordsToReadBeforeSpilling; i++) {
      assertTrue(iter.hasNext());
      iter.loadNext();
    }

    assertTrue(iter.spill() > 0);
    assertEquals(0, iter.spill());

    for (int i = numRecordsToReadBeforeSpilling; i < n; i++) {
      assertTrue(iter.hasNext());
      iter.loadNext();
    }
    assertFalse(iter.hasNext());

    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void forcedSpillingWithFullyReadIterator() throws Exception {
    final UnsafeExternalSorter sorter = newSorter();
    long[] record = new long[100];
    final int recordSize = record.length * 8;
    final int n = (int) pageSizeBytes / recordSize * 3;
    for (int i = 0; i < n; i++) {
      record[0] = i;
      sorter.insertRecord(record, Platform.LONG_ARRAY_OFFSET, recordSize, 0, false);
    }
    assertTrue(sorter.getNumberOfAllocatedPages() >= 2);

    UnsafeExternalSorter.SpillableIterator iter =
            (UnsafeExternalSorter.SpillableIterator) sorter.getSortedIterator();
    for (int i = 0; i < n; i++) {
      assertTrue(iter.hasNext());
      iter.loadNext();
      assertEquals(i, Platform.getLong(iter.getBaseObject(), iter.getBaseOffset()));
    }
    assertFalse(iter.hasNext());

    assertTrue(iter.spill() > 0);
    assertEquals(0, iter.spill());
    assertEquals(n - 1, Platform.getLong(iter.getBaseObject(), iter.getBaseOffset()));
    assertFalse(iter.hasNext());

    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void forcedSpillingWithNotReadIterator() throws Exception {
    final UnsafeExternalSorter sorter = newSorter();
    long[] record = new long[100];
    int recordSize = record.length * 8;
    int n = (int) pageSizeBytes / recordSize * 3;
    for (int i = 0; i < n; i++) {
      record[0] = (long) i;
      sorter.insertRecord(record, Platform.LONG_ARRAY_OFFSET, recordSize, 0, false);
    }
    assertTrue(sorter.getNumberOfAllocatedPages() >= 2);
    UnsafeExternalSorter.SpillableIterator iter =
      (UnsafeExternalSorter.SpillableIterator) sorter.getSortedIterator();
    assertTrue(iter.spill() > 0);
    assertEquals(0, iter.spill());
    for (int i = 0; i < n; i++) {
      iter.hasNext();
      iter.loadNext();
      assertEquals(i, Platform.getLong(iter.getBaseObject(), iter.getBaseOffset()));
    }
    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void forcedSpillingWithoutComparator() throws Exception {
    final UnsafeExternalSorter sorter = UnsafeExternalSorter.create(
      taskMemoryManager,
      blockManager,
      serializerManager,
      taskContext,
      null,
      null,
      /* initialSize */ 1024,
      pageSizeBytes,
      spillElementsThreshold,
      spillSizeThreshold,
      /* spillMergeFactor */ -1,
      shouldUseRadixSort());
    long[] record = new long[100];
    int recordSize = record.length * 8;
    int n = (int) pageSizeBytes / recordSize * 3;
    int batch = n / 4;
    for (int i = 0; i < n; i++) {
      record[0] = (long) i;
      sorter.insertRecord(record, Platform.LONG_ARRAY_OFFSET, recordSize, 0, false);
      if (i % batch == batch - 1) {
        sorter.spill();
      }
    }
    UnsafeSorterIterator iter = sorter.getIterator(0);
    for (int i = 0; i < n; i++) {
      iter.hasNext();
      iter.loadNext();
      assertEquals(i, Platform.getLong(iter.getBaseObject(), iter.getBaseOffset()));
    }
    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void testDiskSpilledBytes() throws Exception {
    final UnsafeExternalSorter sorter = newSorter();
    long[] record = new long[100];
    int recordSize = record.length * 8;
    int n = (int) pageSizeBytes / recordSize * 3;
    for (int i = 0; i < n; i++) {
      record[0] = (long) i;
      sorter.insertRecord(record, Platform.LONG_ARRAY_OFFSET, recordSize, 0, false);
    }
    // We will have at-least 2 memory pages allocated because of rounding happening due to
    // integer division of pageSizeBytes and recordSize.
    assertTrue(sorter.getNumberOfAllocatedPages() >= 2);
    assertEquals(0, taskContext.taskMetrics().diskBytesSpilled());
    UnsafeExternalSorter.SpillableIterator iter =
            (UnsafeExternalSorter.SpillableIterator) sorter.getSortedIterator();
    assertTrue(iter.spill() > 0);
    assertTrue(taskContext.taskMetrics().diskBytesSpilled() > 0);
    assertEquals(0, iter.spill());
    // Even if we did not spill second time, the disk spilled bytes should still be non-zero
    assertTrue(taskContext.taskMetrics().diskBytesSpilled() > 0);
    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void testPeakMemoryUsed() throws Exception {
    final long recordLengthBytes = 8;
    final long pageSizeBytes = 256;
    final long numRecordsPerPage = pageSizeBytes / recordLengthBytes;
    final UnsafeExternalSorter sorter = UnsafeExternalSorter.create(
      taskMemoryManager,
      blockManager,
      serializerManager,
      taskContext,
      () -> recordComparator,
      prefixComparator,
      1024,
      pageSizeBytes,
      spillElementsThreshold,
      spillSizeThreshold,
      /* spillMergeFactor */ -1,
      shouldUseRadixSort());

    // Peak memory should be monotonically increasing. More specifically, every time
    // we allocate a new page it should increase by exactly the size of the page.
    long previousPeakMemory = sorter.getPeakMemoryUsedBytes();
    long newPeakMemory;
    try {
      for (int i = 0; i < numRecordsPerPage * 10; i++) {
        insertNumber(sorter, i);
        newPeakMemory = sorter.getPeakMemoryUsedBytes();
        if (i % numRecordsPerPage == 0) {
          // We allocated a new page for this record, so peak memory should change
          assertEquals(previousPeakMemory + pageSizeBytes, newPeakMemory);
        } else {
          assertEquals(previousPeakMemory, newPeakMemory);
        }
        previousPeakMemory = newPeakMemory;
      }

      // Spilling should not change peak memory
      sorter.spill();
      newPeakMemory = sorter.getPeakMemoryUsedBytes();
      assertEquals(previousPeakMemory, newPeakMemory);
      for (int i = 0; i < numRecordsPerPage; i++) {
        insertNumber(sorter, i);
      }
      newPeakMemory = sorter.getPeakMemoryUsedBytes();
      assertEquals(previousPeakMemory, newPeakMemory);
    } finally {
      sorter.cleanupResources();
      assertSpillFilesWereCleanedUp();
    }
  }

  @Test
  public void testGetIterator() throws Exception {
    final UnsafeExternalSorter sorter = newSorter();
    for (int i = 0; i < 100; i++) {
      insertNumber(sorter, i);
    }
    verifyIntIterator(sorter.getIterator(0), 0, 100);
    verifyIntIterator(sorter.getIterator(79), 79, 100);

    sorter.spill();
    for (int i = 100; i < 200; i++) {
      insertNumber(sorter, i);
    }
    sorter.spill();
    verifyIntIterator(sorter.getIterator(79), 79, 200);

    for (int i = 200; i < 300; i++) {
      insertNumber(sorter, i);
    }
    verifyIntIterator(sorter.getIterator(79), 79, 300);
    verifyIntIterator(sorter.getIterator(139), 139, 300);
    verifyIntIterator(sorter.getIterator(279), 279, 300);
    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void testNoOOMDuringSpill() throws Exception {
    final UnsafeExternalSorter sorter = newSorter();
    for (int i = 0; i < 100; i++) {
      insertNumber(sorter, i);
    }

    // Check that spilling still succeeds when the task is starved for memory.
    memoryManager.markConsequentOOM(Integer.MAX_VALUE);
    sorter.spill();
    memoryManager.resetConsequentOOM();

    // Ensure that records can be appended after spilling, i.e. check that the sorter will allocate
    // the new pointer array that it could not allocate while spilling.
    for (int i = 0; i < 100; ++i) {
      insertNumber(sorter, i);
    }

    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }


  private void verifyIntIterator(UnsafeSorterIterator iter, int start, int end)
      throws IOException {
    for (int i = start; i < end; i++) {
      assertTrue(iter.hasNext());
      iter.loadNext();
      assertEquals(Platform.getInt(iter.getBaseObject(), iter.getBaseOffset()), i);
    }
  }

  @Test
  public void testBoundedMergeWithSmallMergeFactor() throws Exception {
    // Set merge factor to 3 so bounded merge is triggered with just a few spills
    final UnsafeExternalSorter sorter = newSorter();
    sorter.setSpillMergeFactor(3);

    // Insert records and spill multiple times to create 6 spill files
    for (int spill = 0; spill < 6; spill++) {
      for (int i = spill * 10; i < (spill + 1) * 10; i++) {
        insertNumber(sorter, i);
      }
      sorter.spill();
    }

    // Verify bounded merge was triggered with 1 intermediate round
    // 6 spills, factor 3: round 1 merges [3,3] → 2 files, then final merge
    UnsafeSorterIterator iter = sorter.getSortedIterator();
    assertEquals(1, sorter.getSpillMergeRounds());
    verifyIntIterator(iter, 0, 60);
    assertFalse(iter.hasNext());

    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void testBoundedMergeDisabled() throws Exception {
    // Set merge factor to -1 to disable bounded merge (legacy behavior)
    final UnsafeExternalSorter sorter = newSorter();
    sorter.setSpillMergeFactor(-1);

    for (int spill = 0; spill < 5; spill++) {
      for (int i = spill * 5; i < (spill + 1) * 5; i++) {
        insertNumber(sorter, i);
      }
      sorter.spill();
    }

    UnsafeSorterIterator iter = sorter.getSortedIterator();
    assertEquals(0, sorter.getSpillMergeRounds());
    verifyIntIterator(iter, 0, 25);
    assertFalse(iter.hasNext());

    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void testBoundedMergeWithInMemoryData() throws Exception {
    // Set merge factor to 2 to force multi-round merge
    final UnsafeExternalSorter sorter = newSorter();
    sorter.setSpillMergeFactor(2);

    // Create 4 spill files
    for (int spill = 0; spill < 4; spill++) {
      for (int i = spill * 5; i < (spill + 1) * 5; i++) {
        insertNumber(sorter, i);
      }
      sorter.spill();
    }
    // Leave some data in memory (not spilled)
    insertNumber(sorter, 20);
    insertNumber(sorter, 21);
    insertNumber(sorter, 22);

    // 4 spills, factor 2: round 1 merges [2,2] → 2 files, then final merge
    UnsafeSorterIterator iter = sorter.getSortedIterator();
    assertEquals(1, sorter.getSpillMergeRounds());
    verifyIntIterator(iter, 0, 23);
    assertFalse(iter.hasNext());

    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void testBoundedMergeMergeFactor2StressTest() throws Exception {
    // Merge factor 2 is the worst case — maximizes number of intermediate rounds
    final UnsafeExternalSorter sorter = newSorter();
    sorter.setSpillMergeFactor(2);

    int numSpills = 8;
    int recordsPerSpill = 10;
    int totalRecords = numSpills * recordsPerSpill;

    for (int spill = 0; spill < numSpills; spill++) {
      for (int i = spill * recordsPerSpill; i < (spill + 1) * recordsPerSpill; i++) {
        insertNumber(sorter, i);
      }
      sorter.spill();
    }

    // 8 spills, factor 2: round 1 [2,2,2,2]→4, round 2 [2,2]→2, then final
    UnsafeSorterIterator iter = sorter.getSortedIterator();
    assertEquals(2, sorter.getSpillMergeRounds());
    verifyIntIterator(iter, 0, totalRecords);
    assertFalse(iter.hasNext());

    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void testBoundedMergeOddSpillCountWithCarryForward() throws Exception {
    // 7 spills with factor 2: exercises carry-forward of original file across rounds.
    // Round 1: [S1,S2]->T1, [S3,S4]->T2, [S5,S6]->T3, [S7]->carry
    // Round 2: [T1,T2]->T4, [T3,S7]->T5 (S7 is original, must not be deleted by merger)
    // Final:   [T4,T5] -> sorted
    final UnsafeExternalSorter sorter = newSorter();
    sorter.setSpillMergeFactor(2);

    int numSpills = 7;
    int recordsPerSpill = 8;
    int totalRecords = numSpills * recordsPerSpill;

    for (int spill = 0; spill < numSpills; spill++) {
      for (int i = spill * recordsPerSpill; i < (spill + 1) * recordsPerSpill; i++) {
        insertNumber(sorter, i);
      }
      sorter.spill();
    }

    UnsafeSorterIterator iter = sorter.getSortedIterator();
    verifyIntIterator(iter, 0, totalRecords);
    assertFalse(iter.hasNext());

    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void testBoundedMergeSpillCountExactlyEqualsFactor() throws Exception {
    // When spill count == merge factor, should use single-round path (not bounded merge)
    // because the condition is spillWriters.size() > spillMergeFactor
    final UnsafeExternalSorter sorter = newSorter();
    sorter.setSpillMergeFactor(5);

    for (int spill = 0; spill < 5; spill++) {
      for (int i = spill * 6; i < (spill + 1) * 6; i++) {
        insertNumber(sorter, i);
      }
      sorter.spill();
    }

    // 5 spills == factor 5: single-round merge, no bounded merge triggered
    UnsafeSorterIterator iter = sorter.getSortedIterator();
    assertEquals(0, sorter.getSpillMergeRounds());
    verifyIntIterator(iter, 0, 30);
    assertFalse(iter.hasNext());

    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void testBoundedMergeMultipleIntermediateRounds() throws Exception {
    // 20 spills with factor 3: exercises multiple intermediate rounds.
    // Round 1: 20 -> 7 (6 groups of 3 + 1 group of 2)
    // Round 2: 7 -> 3 (2 groups of 3 + 1 group of 1)
    // Final:   3 -> sorted
    final UnsafeExternalSorter sorter = newSorter();
    sorter.setSpillMergeFactor(3);

    int numSpills = 20;
    int recordsPerSpill = 5;
    int totalRecords = numSpills * recordsPerSpill;

    for (int spill = 0; spill < numSpills; spill++) {
      for (int i = spill * recordsPerSpill; i < (spill + 1) * recordsPerSpill; i++) {
        insertNumber(sorter, i);
      }
      sorter.spill();
    }

    // 20 spills, factor 3: round 1 → 7 files, round 2 → 3 files, then final
    UnsafeSorterIterator iter = sorter.getSortedIterator();
    assertEquals(2, sorter.getSpillMergeRounds());
    verifyIntIterator(iter, 0, totalRecords);
    assertFalse(iter.hasNext());

    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void testBoundedMergeSingleSpillWithBoundedMergeEnabled() throws Exception {
    // Single spill file with bounded merge enabled — should use single-round path
    // since 1 > 2 is false.
    final UnsafeExternalSorter sorter = newSorter();
    sorter.setSpillMergeFactor(2);

    for (int i = 0; i < 10; i++) {
      insertNumber(sorter, i);
    }
    sorter.spill();

    UnsafeSorterIterator iter = sorter.getSortedIterator();
    verifyIntIterator(iter, 0, 10);
    assertFalse(iter.hasNext());

    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void testBoundedMergeWithInterleavedData() throws Exception {
    // Each spill has data that interleaves with other spills' data.
    // This verifies that intermediate merge actually sorts (not just concatenates).
    // Spill 0: [0, 4, 8, 12, 16]  Spill 1: [1, 5, 9, 13, 17]
    // Spill 2: [2, 6, 10, 14, 18] Spill 3: [3, 7, 11, 15, 19]
    // With factor 2:
    //   Round 1: merge(S0,S1)->[0,1,4,5,8,9,12,13,16,17], merge(S2,S3)->[2,3,6,7,...]
    //   Final:   merge both -> [0,1,2,...,19]
    final UnsafeExternalSorter sorter = newSorter();
    sorter.setSpillMergeFactor(2);

    int numSpills = 4;
    int recordsPerSpill = 5;
    for (int spill = 0; spill < numSpills; spill++) {
      for (int j = 0; j < recordsPerSpill; j++) {
        insertNumber(sorter, spill + j * numSpills);
      }
      sorter.spill();
    }

    int totalRecords = numSpills * recordsPerSpill;
    UnsafeSorterIterator iter = sorter.getSortedIterator();
    verifyIntIterator(iter, 0, totalRecords);
    assertFalse(iter.hasNext());

    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void testBoundedMergeWithDuplicateKeys() throws Exception {
    // Multiple spills contain identical keys. Verifies that all duplicates are
    // preserved through intermediate merge rounds and the full comparator chain
    // (prefix + record) works correctly.
    final UnsafeExternalSorter sorter = newSorter();
    sorter.setSpillMergeFactor(2);

    int numSpills = 4;
    int valuesPerSpill = 5;
    for (int spill = 0; spill < numSpills; spill++) {
      for (int i = 0; i < valuesPerSpill; i++) {
        insertNumber(sorter, i);
      }
      sorter.spill();
    }

    // All 20 records should come back, sorted with duplicates adjacent:
    // [0,0,0,0, 1,1,1,1, 2,2,2,2, 3,3,3,3, 4,4,4,4]
    int totalRecords = numSpills * valuesPerSpill;
    UnsafeSorterIterator iter = sorter.getSortedIterator();
    for (int i = 0; i < totalRecords; i++) {
      assertTrue(iter.hasNext());
      iter.loadNext();
      int expectedValue = i / numSpills;
      assertEquals(expectedValue, iter.getKeyPrefix());
      assertEquals(expectedValue,
          Platform.getInt(iter.getBaseObject(), iter.getBaseOffset()));
    }
    assertFalse(iter.hasNext());

    sorter.cleanupResources();
    assertSpillFilesWereCleanedUp();
  }
}
