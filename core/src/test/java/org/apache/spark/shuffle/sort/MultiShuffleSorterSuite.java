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

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import org.apache.spark.*;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.memory.*;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.TempShuffleBlockId;
import org.apache.spark.unsafe.Platform;

import static org.junit.Assert.*;
import static org.mockito.Answers.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.*;

public class MultiShuffleSorterSuite {

  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

  static final int NUM_PARTITITONS = 4;

  static final long DEFAULT_MAX_MEMORY_PER_SORTER = 4000;
  static final long RECORD_SIZE = 1024;

  SparkConf conf;
  TestMemoryManager memoryManager;
  TaskMemoryManager taskMemoryManager;
  TaskMetrics taskMetrics;
  HashPartitioner partitioner;
  SequenceCounter forceOomsCount;

  @Mock(answer = RETURNS_SMART_NULLS)
  BlockManager blockManager;

  class SequenceCounter {
    private List<Integer> counts;

    SequenceCounter(Integer... counts) {
      reset(counts);
    }

    public void reset(Integer... counts) {
      this.counts = Lists.newArrayList(counts);
    }

    public synchronized boolean nextCount() {
      if (counts.size() > 0) {
        counts.set(0, counts.get(0) - 1);
        if (counts.get(0) == 0) {
          counts.remove(0);
          return true;
        }
      }
      return false;
    }
  }

  class TestShuffleSorter extends ShuffleSorter {
    TestShuffleSorter(
        int sorterId,
        TaskMemoryManager taskMemoryManager,
        long pageSize,
        MemoryMode mode,
        SequenceCounter forceOomsCount
        ) {
      super(taskMemoryManager, pageSize, mode);
      this.sorterId = sorterId;
      this.forceOomsCount = forceOomsCount;
    }

    private int sorterId;
    private List<SpillInfo> spillInfos = new ArrayList<>();
    private long[] partitionCount = new long[NUM_PARTITITONS];
    private long peakMemoryUsedBytes = 0;
    private SequenceCounter forceOomsCount;
    private List<Long> spillThresholdHistory = new ArrayList<>();

    public boolean isEmpty() {
      return getUsed() == 0;
    }

    @Override
    public long getPeakMemoryUsedBytes() {
      return peakMemoryUsedBytes;
    }

    @Override
    public void cleanupResources() {
      used = 0;
    }

    @Override
    public void insertRecord(Object recordBase, long recordOffset, int length, int partitionId)
        throws IOException {
      if (forceOomsCount.nextCount()) {
        throw new OutOfMemoryError();
      }

      partitionCount[partitionId]++;

      incUsedBytes(RECORD_SIZE);
    }

    @Override
    public SpillInfo[] closeAndGetSpills() throws IOException {
      boolean hasAnyRecords = false;
      for (int i=0; i<NUM_PARTITITONS; i++) {
        hasAnyRecords |= (partitionCount[i] > 0);
      }
      if (hasAnyRecords) {
        writeSortedFile();
      }

      used = 0;

      return spillInfos.toArray(new SpillInfo[spillInfos.size()]);
    }

    @Override
    public boolean hasSpaceForInsertRecord() {
      return false;
    }

    @Override
    public long spill(long size, MemoryConsumer trigger) throws IOException {
      writeSortedFile();

      long spilledBytes = used;
      used = 0;

      return spilledBytes;
    }

    private void writeSortedFile() {
      TempShuffleBlockId blockId = new TempShuffleBlockId(UUID.randomUUID());
      File file = new File(Integer.toString(sorterId) + "." + blockId.toString());

      SpillInfo spillInfo = new SpillInfo(NUM_PARTITITONS, file, blockId);

      for (int i=0; i<NUM_PARTITITONS; i++) {
        spillInfo.partitionLengths[i] = partitionCount[i];
        partitionCount[i] = 0;
      }

      spillInfos.add(spillInfo);
    }

    private void incUsedBytes(long usedBytes) {
      used += usedBytes;
      peakMemoryUsedBytes = Math.max(used, peakMemoryUsedBytes);
    }
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    conf = new SparkConf()
      .set("spark.buffer.pageSize", "1m")
      .set("spark.memory.offHeap.enabled", "false")
      .set("spark.shuffle.writer.async.log.period", "120s")
      .set("spark.shuffle.writer.spill.min.size", "0")
      .set("spark.shuffle.writer.spill.size", "0")
      .set("spark.shuffle.writer.waiting.interval.ms", "0");
    taskMetrics = TaskMetrics.empty();
    memoryManager = new TestMemoryManager(conf);
    taskMemoryManager = new TaskMemoryManager(memoryManager, 0);
    partitioner = new HashPartitioner(NUM_PARTITITONS);
    forceOomsCount = new SequenceCounter();
  }

  @Test
  public void testCalculateNewSpillOnOOMWithNoAsyncSpilling() throws IOException {
    ShuffleSorter sorter1 = Mockito.mock(ShuffleSorter.class);
    ShuffleSorter sorter2 = Mockito.mock(ShuffleSorter.class);

    MultiShuffleSorter msorter = createMultiShuffleSorter(
      Long.MAX_VALUE / RECORD_SIZE, sorter1, sorter2);

    final AtomicInteger count = new AtomicInteger(0);
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        int curCount = count.getAndIncrement();
        if (curCount == 0) {
          throw new OutOfMemoryError();
        }
        return null;
      }
    }).when(sorter1).insertRecord(Matchers.anyObject(), anyLong(), anyInt(), anyInt());

    when(sorter1.getUsed()).thenReturn(256000L);
    when(sorter2.getUsed()).thenReturn(256000L);

    when(sorter1.spill(anyLong(), any(MemoryConsumer.class))).thenAnswer(
      new Answer<Long>() {
        @Override
        public Long answer(InvocationOnMock mock) {
          return 1L;
        }
      }
    );

    insertRecordIntoSorter(0, 0, msorter);

    // Sum up the total used memory and share across sorters, scale down by 10% buffer.
    long newSpillThresholdBytes = Math.max(200, ((512000 / 2) * 9) / 10);
    assertEquals(newSpillThresholdBytes, msorter.getSpillThresholdBytes());
  }

  @Test
  public void testCalculateNewSpillOnOOMWithAsyncSpilling() throws IOException {
    final ShuffleSorter sorter1 = Mockito.mock(ShuffleSorter.class);
    final ShuffleSorter sorter2 = Mockito.mock(ShuffleSorter.class);
    final ShuffleSorter sorter3 = Mockito.mock(ShuffleSorter.class);

    long maxMemory = (Long.MAX_VALUE / RECORD_SIZE) * RECORD_SIZE;

    MultiShuffleSorter msorter = createMultiShuffleSorterNoBlocking(
      maxMemory / RECORD_SIZE, 0, sorter1, sorter2, sorter3);

    when(sorter1.getUsed()).thenReturn(0L, maxMemory);
    doNothing().when(sorter1).insertRecord(Matchers.anyObject(), anyLong(), anyInt(), anyInt());
    when(sorter1.spill(anyLong(), any(MemoryConsumer.class))).thenReturn(1L);

    when(sorter2.getUsed()).thenReturn(maxMemory);
    doNothing().when(sorter2).insertRecord(Matchers.anyObject(), anyLong(), anyInt(), anyInt());
    when(sorter2.spill(anyLong(), any(MemoryConsumer.class))).thenReturn(1L);

    when(sorter3.getUsed()).thenReturn(256000L);
    doThrow(new OutOfMemoryError()).doNothing().when(sorter3)
      .insertRecord(Matchers.anyObject(), anyLong(), anyInt(), anyInt());

    // Insert into sorter 1
    insertRecordIntoSorter(0, 0, msorter);

    // Async spill sorter 1 and insert into sorter 2
    insertRecordIntoSorter(0, 0, msorter);

    // Async spill sorter 2 and out of memory on sorter 3
    insertRecordIntoSorter(0, 0, msorter);

    // Take previous spill threshold and reduce by 10%
    long newSpillThresholdBytes = Math.max(200, ((maxMemory / 3) * 9) / 10);
    assertEquals(newSpillThresholdBytes, msorter.getSpillThresholdBytes());
  }

  @Test
  public void testAsyncInsertSpillWithNoOutOfMemory() throws IOException {
    TestShuffleSorter sorter1 = createTestShuffleSorter(0);
    TestShuffleSorter sorter2 = createTestShuffleSorter(1);

    List<Long> thresholdHistory = new ArrayList<>();
    MultiShuffleSorter msorter = createMultiShuffleSorter(8, thresholdHistory, sorter1, sorter2);

    for (int i=0; i<16; i++) {
      insertRecordIntoSorter(i, i, msorter);
    }

    assertTrue(sorter1.getPeakMemoryUsedBytes() > 0);
    assertTrue(sorter2.getPeakMemoryUsedBytes() > 0);

    SpillInfo[] spillInfos = msorter.closeAndGetSpills();
    assertSpillPartitions(spillInfos, 2, 4, 4, 4, 4, 4);
    assertThresholdHistory(thresholdHistory, 4096L);

    assertTrue(sorter1.isEmpty());
    assertTrue(sorter2.isEmpty());
  }

  @Test
  public void testAsyncInsertSpillWithOneEarlyOutOfMemory() throws IOException {
    TestShuffleSorter sorter1 = createTestShuffleSorter(0);
    TestShuffleSorter sorter2 = createTestShuffleSorter(1);

    List<Long> thresholdHistory = new ArrayList<>();
    MultiShuffleSorter msorter = createMultiShuffleSorter(8, thresholdHistory,
      sorter1, sorter2);

    forceOomsCount.reset(2);
    for (int i=0; i<16; i++) {
      insertRecordIntoSorter(i, i, msorter);
    }

    assertTrue(sorter1.getPeakMemoryUsedBytes() > 0);
    assertTrue(sorter2.getPeakMemoryUsedBytes() > 0);

    SpillInfo[] spillInfos = msorter.closeAndGetSpills();
    assertSpillPartitions(spillInfos, 2, 16, 4, 4, 4, 4);
    assertThresholdHistory(thresholdHistory, 4096L, 460L);

    assertTrue(sorter1.isEmpty());
    assertTrue(sorter2.isEmpty());
  }

  @Test
  public void testAsyncInsertSpillWithOneLaterOutOfMemory() throws IOException {
    TestShuffleSorter sorter1 = createTestShuffleSorter(0);
    TestShuffleSorter sorter2 = createTestShuffleSorter(1);

    List<Long> thresholdHistory = new ArrayList<>();
    MultiShuffleSorter msorter = createMultiShuffleSorter(16, thresholdHistory,
      sorter1, sorter2);

    forceOomsCount.reset(7);
    for (int i=0; i<32; i++) {
      insertRecordIntoSorter(i, i, msorter);
    }

    assertTrue(sorter1.getPeakMemoryUsedBytes() > 0);
    assertTrue(sorter2.getPeakMemoryUsedBytes() > 0);

    SpillInfo[] spillInfos = msorter.closeAndGetSpills();
    assertSpillPartitions(spillInfos, 2, 10, 8, 8, 8, 8);
    assertThresholdHistory(thresholdHistory, 8192L, 2764L);

    assertTrue(sorter1.isEmpty());
    assertTrue(sorter2.isEmpty());
  }

  @Test
  public void testAsyncInsertSpillWithMultipleOutOfMemory() throws IOException {
    TestShuffleSorter sorter1 = createTestShuffleSorter(0);
    TestShuffleSorter sorter2 = createTestShuffleSorter(1);

    List<Long> thresholdHistory = new ArrayList<>();
    MultiShuffleSorter msorter = createMultiShuffleSorter(32, thresholdHistory,
      sorter1, sorter2);

    forceOomsCount.reset(32, 16);
    for (int i=0; i<128; i++) {
      insertRecordIntoSorter(i, i, msorter);
    }

    assertTrue(sorter1.getPeakMemoryUsedBytes() > 0);
    assertTrue(sorter2.getPeakMemoryUsedBytes() > 0);

    SpillInfo[] spillInfos = msorter.closeAndGetSpills();
    assertSpillPartitions(spillInfos, 2, 10, 32, 32, 32, 32);
    assertThresholdHistory(thresholdHistory, 16384L, 14745L, 13270L);

    assertTrue(sorter1.isEmpty());
    assertTrue(sorter2.isEmpty());
  }

  @Test
  public void testMultipleAsyncInsertSpillWithMultipleOutOfMemory() throws IOException {
    TestShuffleSorter sorter1 = createTestShuffleSorter(0);
    TestShuffleSorter sorter2 = createTestShuffleSorter(1);
    TestShuffleSorter sorter3 = createTestShuffleSorter(2);
    TestShuffleSorter sorter4 = createTestShuffleSorter(3);

    List<Long> thresholdHistory = new ArrayList<>();
    MultiShuffleSorter msorter = createMultiShuffleSorter(32, thresholdHistory,
      sorter1, sorter2, sorter3, sorter4);

    for (int i = 0; i < 128; i++) {
      insertRecordIntoSorter(i, i, msorter);
    }

    assertTrue(sorter1.getPeakMemoryUsedBytes() > 0);
    assertTrue(sorter2.getPeakMemoryUsedBytes() > 0);
    assertTrue(sorter3.getPeakMemoryUsedBytes() > 0);
    assertTrue(sorter4.getPeakMemoryUsedBytes() > 0);

    SpillInfo[] spillInfos = msorter.closeAndGetSpills();
    assertSpillPartitions(spillInfos, 4, 16, 32, 32, 32, 32);
    assertThresholdHistory(thresholdHistory, 8192L);

    assertTrue(sorter1.isEmpty());
    assertTrue(sorter2.isEmpty());
    assertTrue(sorter3.isEmpty());
    assertTrue(sorter4.isEmpty());
  }

  private void assertSpillPartitions(
      SpillInfo[] spillInfo, int expectedSorters, int expectedSpills,
      int... expectedPartitionLengths) {
    Set<Integer> sorters = new HashSet<>();
    int[] partitionsTotal = new int[NUM_PARTITITONS];
    for (int i=0; i<spillInfo.length; i++) {
      String filename = spillInfo[i].file.getName();
      int sorterId = Integer.parseInt(filename.substring(0, filename.indexOf('.')));
      sorters.add(sorterId);

      for (int j=0; j<NUM_PARTITITONS; j++) {
        partitionsTotal[j] += spillInfo[i].partitionLengths[j];
      }
    }
    assertEquals(expectedSorters, sorters.size());
    assertEquals(expectedSpills, spillInfo.length);

    for (int j=0; j<NUM_PARTITITONS; j++) {
      assertEquals(expectedPartitionLengths[j], partitionsTotal[j]);
    }
  }

  private TestShuffleSorter createTestShuffleSorter(int sorterId) {
    return new TestShuffleSorter(sorterId, taskMemoryManager,
      0, MemoryMode.ON_HEAP, forceOomsCount);
  }

  private MultiShuffleSorter createMultiShuffleSorter(final ShuffleSorter... sorters) {
    return createMultiShuffleSorter(DEFAULT_MAX_MEMORY_PER_SORTER, sorters);
  }

  private MultiShuffleSorter createMultiShuffleSorter(
    final long maxRecordsInMemory, final ShuffleSorter... sorters) {
    return createMultiShuffleSorter(maxRecordsInMemory, new ArrayList<Long>(), sorters);
  }

  private MultiShuffleSorter createMultiShuffleSorter(
    final long maxRecordsInMemory,
    final List<Long> spillThresholdHistory,
    final ShuffleSorter... sorters) {
    final List<Long> history = Collections.synchronizedList(spillThresholdHistory);

    return new MultiShuffleSorter(
      taskMemoryManager, blockManager, taskMetrics, 0,
      NUM_PARTITITONS, conf, sorters.length,
      (maxRecordsInMemory * RECORD_SIZE) / sorters.length,
      getShuffleSorterFactoryMock(sorters)) {

      @Override
      public void insertRecord(Object recordBase, long recordOffset, int length, int partitionId)
          throws IOException {
        super.insertRecord(recordBase, recordOffset, length, partitionId);
        if (history != null) {
          if (history.size() == 0 || history.get(history.size() - 1) != getSpillThresholdBytes()) {
            history.add(getSpillThresholdBytes());
          }
        }
      }
    };
  }

  private MultiShuffleSorter createMultiShuffleSorterNoBlocking(
    final long maxRecordsInMemory,
    final int numAvailableSortersOnCheck,
    final ShuffleSorter... sorters) {
    return new MultiShuffleSorter(
        taskMemoryManager, blockManager, taskMetrics, 0,
        NUM_PARTITITONS, conf, sorters.length,
        (maxRecordsInMemory * RECORD_SIZE) / sorters.length,
        getShuffleSorterFactoryMock(sorters)) {

      @Override
      protected int getNumAvailableSorters() {
        return numAvailableSortersOnCheck;
      }

      @Override
      protected void waitForAvailableSorters(int numSorters) throws IOException {
        return;
      }
    };
  }

  private ShuffleSorterFactory getShuffleSorterFactoryMock(final ShuffleSorter[] sorters) {
    ShuffleSorterFactory factory = Mockito.mock(ShuffleSorterFactory.class);
    final AtomicInteger sorterIndex = new AtomicInteger(0);
    when(factory.createShuffleExternalSorter(any(TaskMemoryManager.class),
      any(BlockManager.class), any(TaskMetrics.class), anyInt(), anyInt(),
      any(SparkConf.class), anyBoolean()))
      .thenAnswer(new Answer<ShuffleSorter>() {
        @Override
        public ShuffleSorter answer(InvocationOnMock invocation) throws Throwable {
          ShuffleSorter sorter = sorters[sorterIndex.get()];
          sorterIndex.incrementAndGet();
          return sorter;
        }
      });
    return factory;
  }

  private void insertRecordIntoSorter(Object key, Object value, ShuffleSorter sorter)
      throws IOException {
    final int partitionId = partitioner.getPartition(key);

    byte[] dummyRecord = new byte[(int) RECORD_SIZE];
    sorter.insertRecord(dummyRecord, Platform.BYTE_ARRAY_OFFSET, dummyRecord.length, partitionId);
  }

  private void assertThresholdHistory(List<Long> thresholdHistory, Long... expected) {
    assertEquals(thresholdHistory.size(), expected.length);
    for (int i=0; i<thresholdHistory.size(); i++) {
      assertEquals(thresholdHistory.get(i), expected[i]);
    }
  }
}
