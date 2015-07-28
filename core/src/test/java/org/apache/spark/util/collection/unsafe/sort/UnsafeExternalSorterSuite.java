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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

import scala.Tuple2;
import scala.Tuple2$;
import scala.runtime.AbstractFunction1;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.junit.Assert.*;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.AdditionalAnswers.returnsSecondArg;
import static org.mockito.Answers.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.*;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleMemoryManager;
import org.apache.spark.storage.*;
import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.memory.ExecutorMemoryManager;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.unsafe.memory.TaskMemoryManager;
import org.apache.spark.util.Utils;

public class UnsafeExternalSorterSuite {

  final TaskMemoryManager memoryManager =
    new TaskMemoryManager(new ExecutorMemoryManager(MemoryAllocator.HEAP));
  // Use integer comparison for comparing prefixes (which are partition ids, in this case)
  final PrefixComparator prefixComparator = new PrefixComparator() {
    @Override
    public int compare(long prefix1, long prefix2) {
      return (int) prefix1 - (int) prefix2;
    }
  };
  // Since the key fits within the 8-byte prefix, we don't need to do any record comparison, so
  // use a dummy comparator
  final RecordComparator recordComparator = new RecordComparator() {
    @Override
    public int compare(
      Object leftBaseObject,
      long leftBaseOffset,
      Object rightBaseObject,
      long rightBaseOffset) {
      return 0;
    }
  };

  @Mock(answer = RETURNS_SMART_NULLS) ShuffleMemoryManager shuffleMemoryManager;
  @Mock(answer = RETURNS_SMART_NULLS) BlockManager blockManager;
  @Mock(answer = RETURNS_SMART_NULLS) DiskBlockManager diskBlockManager;
  @Mock(answer = RETURNS_SMART_NULLS) TaskContext taskContext;

  File tempDir;

  private static final class CompressStream extends AbstractFunction1<OutputStream, OutputStream> {
    @Override
    public OutputStream apply(OutputStream stream) {
      return stream;
    }
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    tempDir = new File(Utils.createTempDir$default$1());
    taskContext = mock(TaskContext.class);
    when(taskContext.taskMetrics()).thenReturn(new TaskMetrics());
    when(shuffleMemoryManager.tryToAcquire(anyLong())).then(returnsFirstArg());
    when(blockManager.diskBlockManager()).thenReturn(diskBlockManager);
    when(diskBlockManager.createTempLocalBlock()).thenAnswer(new Answer<Tuple2<TempLocalBlockId, File>>() {
      @Override
      public Tuple2<TempLocalBlockId, File> answer(InvocationOnMock invocationOnMock) throws Throwable {
        TempLocalBlockId blockId = new TempLocalBlockId(UUID.randomUUID());
        File file = File.createTempFile("spillFile", ".spill", tempDir);
        return Tuple2$.MODULE$.apply(blockId, file);
      }
    });
    when(blockManager.getDiskWriter(
      any(BlockId.class),
      any(File.class),
      any(SerializerInstance.class),
      anyInt(),
      any(ShuffleWriteMetrics.class))).thenAnswer(new Answer<DiskBlockObjectWriter>() {
      @Override
      public DiskBlockObjectWriter answer(InvocationOnMock invocationOnMock) throws Throwable {
        Object[] args = invocationOnMock.getArguments();

        return new DiskBlockObjectWriter(
          (BlockId) args[0],
          (File) args[1],
          (SerializerInstance) args[2],
          (Integer) args[3],
          new CompressStream(),
          false,
          (ShuffleWriteMetrics) args[4]
        );
      }
    });
    when(blockManager.wrapForCompression(any(BlockId.class), any(InputStream.class)))
      .then(returnsSecondArg());
  }

  private static void insertNumber(UnsafeExternalSorter sorter, int value) throws Exception {
    final int[] arr = new int[] { value };
    sorter.insertRecord(arr, PlatformDependent.INT_ARRAY_OFFSET, 4, value);
  }

  @Test
  public void testSortingOnlyByPrefix() throws Exception {

    final UnsafeExternalSorter sorter = new UnsafeExternalSorter(
      memoryManager,
      shuffleMemoryManager,
      blockManager,
      taskContext,
      recordComparator,
      prefixComparator,
      1024,
      new SparkConf());

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
      // TODO: read rest of value.
    }

    // TODO: test for cleanup:
    // assert(tempDir.isEmpty)
  }

  @Test
  public void testSortingEmptyArrays() throws Exception {

    final UnsafeExternalSorter sorter = new UnsafeExternalSorter(
      memoryManager,
      shuffleMemoryManager,
      blockManager,
      taskContext,
      recordComparator,
      prefixComparator,
      1024,
      new SparkConf());

    sorter.insertRecord(null, 0, 0, 0);
    sorter.insertRecord(null, 0, 0, 0);
    sorter.spill();
    sorter.insertRecord(null, 0, 0, 0);
    sorter.spill();
    sorter.insertRecord(null, 0, 0, 0);
    sorter.insertRecord(null, 0, 0, 0);

    UnsafeSorterIterator iter = sorter.getSortedIterator();

    for (int i = 1; i <= 5; i++) {
      iter.loadNext();
      assertEquals(0, iter.getKeyPrefix());
      assertEquals(0, iter.getRecordLength());
    }
  }

  @Test
  public void testFillingPage() throws Exception {
    final UnsafeExternalSorter sorter = new UnsafeExternalSorter(
      memoryManager,
      shuffleMemoryManager,
      blockManager,
      taskContext,
      recordComparator,
      prefixComparator,
      1024,
      new SparkConf());

    byte[] record = new byte[16];
    while (sorter.getNumberOfAllocatedPages() < 2) {
      sorter.insertRecord(record, PlatformDependent.BYTE_ARRAY_OFFSET, record.length, 0);
    }
    sorter.freeMemory();
  }

}
