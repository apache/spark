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


import org.apache.spark.HashPartitioner;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import scala.Tuple2;
import scala.Tuple2$;
import scala.runtime.AbstractFunction1;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

import static org.mockito.Mockito.*;
import static org.mockito.AdditionalAnswers.*;

public class UnsafeExternalSorterSuite {

  final TaskMemoryManager memoryManager =
    new TaskMemoryManager(new ExecutorMemoryManager(MemoryAllocator.HEAP));
  // Compute key prefixes based on the records' partition ids
  final HashPartitioner hashPartitioner = new HashPartitioner(4);
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

  ShuffleMemoryManager shuffleMemoryManager;
  BlockManager blockManager;
  DiskBlockManager diskBlockManager;
  File tempDir;
  TaskContext taskContext;

  private static final class CompressStream extends AbstractFunction1<OutputStream, OutputStream> {
    @Override
    public OutputStream apply(OutputStream stream) {
      return stream;
    }
  }

  @Before
  public void setUp() {
    shuffleMemoryManager = mock(ShuffleMemoryManager.class);
    diskBlockManager = mock(DiskBlockManager.class);
    blockManager = mock(BlockManager.class);
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

  /**
   * Tests the type of sorting that's used in the non-combiner path of sort-based shuffle.
   */
  @Test
  public void testSortingOnlyByPartitionId() throws Exception {

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
    insertNumber(sorter, 2);

    UnsafeSorterIterator iter = sorter.getSortedIterator();

    iter.loadNext();
    Assert.assertEquals(1, iter.getKeyPrefix());
    iter.loadNext();
    Assert.assertEquals(2, iter.getKeyPrefix());
    iter.loadNext();
    Assert.assertEquals(3, iter.getKeyPrefix());
    iter.loadNext();
    Assert.assertEquals(4, iter.getKeyPrefix());
    iter.loadNext();
    Assert.assertEquals(5, iter.getKeyPrefix());
    Assert.assertFalse(iter.hasNext());
    // TODO: check that the values are also read back properly.

    // TODO: test for cleanup:
    // assert(tempDir.isEmpty)
  }

}
