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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.UUID;

import scala.*;
import scala.runtime.AbstractFunction1;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.AdditionalAnswers.returnsSecondArg;
import static org.mockito.Mockito.*;

import org.apache.spark.*;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.shuffle.IndexShuffleBlockManager;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleMemoryManager;
import org.apache.spark.storage.*;
import org.apache.spark.unsafe.memory.ExecutorMemoryManager;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.unsafe.memory.TaskMemoryManager;
import org.apache.spark.util.Utils;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.scheduler.MapStatus;

public class UnsafeShuffleWriterSuite {

  final TaskMemoryManager memoryManager =
    new TaskMemoryManager(new ExecutorMemoryManager(MemoryAllocator.HEAP));
  // Compute key prefixes based on the records' partition ids
  final HashPartitioner hashPartitioner = new HashPartitioner(4);

  ShuffleMemoryManager shuffleMemoryManager;
  BlockManager blockManager;
  IndexShuffleBlockManager shuffleBlockManager;
  DiskBlockManager diskBlockManager;
  File tempDir;
  TaskContext taskContext;
  SparkConf sparkConf;

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
    shuffleBlockManager = mock(IndexShuffleBlockManager.class);
    tempDir = new File(Utils.createTempDir$default$1());
    taskContext = mock(TaskContext.class);
    sparkConf = new SparkConf();
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

  @Test
  public void basicShuffleWriting() throws Exception {

    final ShuffleDependency<Object, Object, Object> dep = mock(ShuffleDependency.class);
    when(dep.serializer()).thenReturn(Option.<Serializer>apply(new KryoSerializer(sparkConf)));
    when(dep.partitioner()).thenReturn(hashPartitioner);

    final File mergedOutputFile = File.createTempFile("mergedoutput", "", tempDir);
    when(shuffleBlockManager.getDataFile(anyInt(), anyInt())).thenReturn(mergedOutputFile);
    final long[] partitionSizes = new long[hashPartitioner.numPartitions()];
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
        long[] receivedPartitionSizes = (long[]) invocationOnMock.getArguments()[2];
        System.arraycopy(
          receivedPartitionSizes, 0, partitionSizes, 0, receivedPartitionSizes.length);
        return null;
      }
    }).when(shuffleBlockManager).writeIndexFile(anyInt(), anyInt(), any(long[].class));

    final UnsafeShuffleWriter<Object, Object> writer = new UnsafeShuffleWriter<Object, Object>(
      blockManager,
      shuffleBlockManager,
      memoryManager,
      shuffleMemoryManager,
      new UnsafeShuffleHandle<Object, Object>(0, 1, dep),
      0, // map id
      taskContext,
      sparkConf
    );

    final ArrayList<Product2<Object, Object>> numbersToSort =
      new ArrayList<Product2<Object, Object>>();
    numbersToSort.add(new Tuple2<Object, Object>(5, 5));
    numbersToSort.add(new Tuple2<Object, Object>(1, 1));
    numbersToSort.add(new Tuple2<Object, Object>(3, 3));
    numbersToSort.add(new Tuple2<Object, Object>(2, 2));
    numbersToSort.add(new Tuple2<Object, Object>(4, 4));


    writer.write(numbersToSort.iterator());
    final MapStatus mapStatus = writer.stop(true).get();

    long sumOfPartitionSizes = 0;
    for (long size: partitionSizes) {
      sumOfPartitionSizes += size;
    }
    Assert.assertEquals(mergedOutputFile.length(), sumOfPartitionSizes);

    // TODO: test that the temporary spill files were cleaned up after the merge.
  }

}
