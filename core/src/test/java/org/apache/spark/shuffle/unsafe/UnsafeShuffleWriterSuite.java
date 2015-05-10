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

import java.io.*;
import java.util.*;

import scala.*;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;

import com.google.common.collect.HashMultiset;
import com.google.common.io.ByteStreams;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.AdditionalAnswers.returnsSecondArg;
import static org.mockito.Answers.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.*;

import org.apache.spark.*;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.ShuffleMemoryManager;
import org.apache.spark.storage.*;
import org.apache.spark.unsafe.memory.ExecutorMemoryManager;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.unsafe.memory.TaskMemoryManager;
import org.apache.spark.util.Utils;

public class UnsafeShuffleWriterSuite {

  static final int NUM_PARTITITONS = 4;
  final TaskMemoryManager taskMemoryManager =
    new TaskMemoryManager(new ExecutorMemoryManager(MemoryAllocator.HEAP));
  final HashPartitioner hashPartitioner = new HashPartitioner(NUM_PARTITITONS);
  File mergedOutputFile;
  File tempDir;
  long[] partitionSizesInMergedFile;
  final LinkedList<File> spillFilesCreated = new LinkedList<File>();
  final Serializer serializer = new KryoSerializer(new SparkConf());

  @Mock(answer = RETURNS_SMART_NULLS) ShuffleMemoryManager shuffleMemoryManager;
  @Mock(answer = RETURNS_SMART_NULLS) BlockManager blockManager;
  @Mock(answer = RETURNS_SMART_NULLS) IndexShuffleBlockResolver shuffleBlockResolver;
  @Mock(answer = RETURNS_SMART_NULLS) DiskBlockManager diskBlockManager;
  @Mock(answer = RETURNS_SMART_NULLS) TaskContext taskContext;
  @Mock(answer = RETURNS_SMART_NULLS) ShuffleDependency<Object, Object, Object> shuffleDep;

  private static final class CompressStream extends AbstractFunction1<OutputStream, OutputStream> {
    @Override
    public OutputStream apply(OutputStream stream) {
      return stream;
    }
  }

  @After
  public void tearDown() {
    Utils.deleteRecursively(tempDir);
    final long leakedMemory = taskMemoryManager.cleanUpAllAllocatedMemory();
    if (leakedMemory != 0) {
      Assert.fail("Test leaked " + leakedMemory + " bytes of managed memory");
    }
  }

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws IOException {
    MockitoAnnotations.initMocks(this);
    tempDir = Utils.createTempDir("test", "test");
    mergedOutputFile = File.createTempFile("mergedoutput", "", tempDir);
    partitionSizesInMergedFile = null;
    spillFilesCreated.clear();

    when(shuffleMemoryManager.tryToAcquire(anyLong())).then(returnsFirstArg());

    when(blockManager.diskBlockManager()).thenReturn(diskBlockManager);
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

    when(shuffleBlockResolver.getDataFile(anyInt(), anyInt())).thenReturn(mergedOutputFile);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
        partitionSizesInMergedFile = (long[]) invocationOnMock.getArguments()[2];
        return null;
      }
    }).when(shuffleBlockResolver).writeIndexFile(anyInt(), anyInt(), any(long[].class));

    when(diskBlockManager.createTempShuffleBlock()).thenAnswer(
      new Answer<Tuple2<TempLocalBlockId, File>>() {
        @Override
        public Tuple2<TempLocalBlockId, File> answer(
          InvocationOnMock invocationOnMock) throws Throwable {
          TempLocalBlockId blockId = new TempLocalBlockId(UUID.randomUUID());
          File file = File.createTempFile("spillFile", ".spill", tempDir);
          spillFilesCreated.add(file);
          return Tuple2$.MODULE$.apply(blockId, file);
        }
      });

    when(taskContext.taskMetrics()).thenReturn(new TaskMetrics());

    when(shuffleDep.serializer()).thenReturn(Option.<Serializer>apply(serializer));
    when(shuffleDep.partitioner()).thenReturn(hashPartitioner);
  }

  private UnsafeShuffleWriter<Object, Object> createWriter(boolean transferToEnabled) {
    SparkConf conf = new SparkConf();
    conf.set("spark.file.transferTo", String.valueOf(transferToEnabled));
    return new UnsafeShuffleWriter<Object, Object>(
      blockManager,
      shuffleBlockResolver,
      taskMemoryManager,
      shuffleMemoryManager,
      new UnsafeShuffleHandle<Object, Object>(0, 1, shuffleDep),
      0, // map id
      taskContext,
      new SparkConf()
    );
  }

  private void assertSpillFilesWereCleanedUp() {
    for (File spillFile : spillFilesCreated) {
      Assert.assertFalse("Spill file " + spillFile.getPath() + " was not cleaned up",
        spillFile.exists());
    }
  }

  private List<Tuple2<Object, Object>> readRecordsFromFile() throws IOException {
    final ArrayList<Tuple2<Object, Object>> recordsList = new ArrayList<Tuple2<Object, Object>>();
    long startOffset = 0;
    for (int i = 0; i < NUM_PARTITITONS; i++) {
      final long partitionSize = partitionSizesInMergedFile[i];
      if (partitionSize > 0) {
        InputStream in = new FileInputStream(mergedOutputFile);
        ByteStreams.skipFully(in, startOffset);
        DeserializationStream recordsStream = serializer.newInstance().deserializeStream(
          new LimitedInputStream(in, partitionSize));
        Iterator<Tuple2<Object, Object>> records = recordsStream.asKeyValueIterator();
        while (records.hasNext()) {
          Tuple2<Object, Object> record = records.next();
          Assert.assertEquals(i, hashPartitioner.getPartition(record._1()));
          recordsList.add(record);
        }
        recordsStream.close();
        startOffset += partitionSize;
      }
    }
    return recordsList;
  }

  @Test(expected=IllegalStateException.class)
  public void mustCallWriteBeforeSuccessfulStop() {
    createWriter(false).stop(true);
  }

  @Test
  public void doNotNeedToCallWriteBeforeUnsuccessfulStop() {
    createWriter(false).stop(false);
  }

  @Test
  public void writeEmptyIterator() throws Exception {
    final UnsafeShuffleWriter<Object, Object> writer = createWriter(true);
    writer.write(Collections.<Product2<Object, Object>>emptyIterator());
    final Option<MapStatus> mapStatus = writer.stop(true);
    Assert.assertTrue(mapStatus.isDefined());
    Assert.assertTrue(mergedOutputFile.exists());
    Assert.assertArrayEquals(new long[NUM_PARTITITONS], partitionSizesInMergedFile);
  }

  @Test
  public void writeWithoutSpilling() throws Exception {
    // In this example, each partition should have exactly one record:
    final ArrayList<Product2<Object, Object>> dataToWrite =
      new ArrayList<Product2<Object, Object>>();
    for (int i = 0; i < NUM_PARTITITONS; i++) {
      dataToWrite.add(new Tuple2<Object, Object>(i, i));
    }
    final UnsafeShuffleWriter<Object, Object> writer = createWriter(true);
    writer.write(dataToWrite.iterator());
    final Option<MapStatus> mapStatus = writer.stop(true);
    Assert.assertTrue(mapStatus.isDefined());
    Assert.assertTrue(mergedOutputFile.exists());

    long sumOfPartitionSizes = 0;
    for (long size: partitionSizesInMergedFile) {
      // All partitions should be the same size:
      Assert.assertEquals(partitionSizesInMergedFile[0], size);
      sumOfPartitionSizes += size;
    }
    Assert.assertEquals(mergedOutputFile.length(), sumOfPartitionSizes);
    Assert.assertEquals(
      HashMultiset.create(dataToWrite),
      HashMultiset.create(readRecordsFromFile()));
    assertSpillFilesWereCleanedUp();
  }

  private void testMergingSpills(boolean transferToEnabled) throws IOException {
    final UnsafeShuffleWriter<Object, Object> writer = createWriter(transferToEnabled);
    final ArrayList<Product2<Object, Object>> dataToWrite =
      new ArrayList<Product2<Object, Object>>();
    for (int i : new int[] { 1, 2, 3, 4, 4, 2 }) {
      dataToWrite.add(new Tuple2<Object, Object>(i, i));
    }
    writer.insertRecordIntoSorter(dataToWrite.get(0));
    writer.insertRecordIntoSorter(dataToWrite.get(1));
    writer.insertRecordIntoSorter(dataToWrite.get(2));
    writer.insertRecordIntoSorter(dataToWrite.get(3));
    writer.forceSorterToSpill();
    writer.insertRecordIntoSorter(dataToWrite.get(4));
    writer.insertRecordIntoSorter(dataToWrite.get(5));
    writer.closeAndWriteOutput();
    final Option<MapStatus> mapStatus = writer.stop(true);
    Assert.assertTrue(mapStatus.isDefined());
    Assert.assertTrue(mergedOutputFile.exists());
    Assert.assertEquals(2, spillFilesCreated.size());

    long sumOfPartitionSizes = 0;
    for (long size: partitionSizesInMergedFile) {
      sumOfPartitionSizes += size;
    }
    Assert.assertEquals(mergedOutputFile.length(), sumOfPartitionSizes);
    Assert.assertEquals(
      HashMultiset.create(dataToWrite),
      HashMultiset.create(readRecordsFromFile()));
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void mergeSpillsWithTransferTo() throws Exception {
    testMergingSpills(true);
  }

  @Test
  public void mergeSpillsWithFileStream() throws Exception {
    testMergingSpills(false);
  }

  @Test
  public void spillFilesAreDeletedWhenStoppingAfterError() throws IOException {
    final UnsafeShuffleWriter<Object, Object> writer = createWriter(false);
    writer.insertRecordIntoSorter(new Tuple2<Object, Object>(1, 1));
    writer.insertRecordIntoSorter(new Tuple2<Object, Object>(2, 2));
    writer.forceSorterToSpill();
    writer.insertRecordIntoSorter(new Tuple2<Object, Object>(2, 2));
    writer.stop(false);
    assertSpillFilesWereCleanedUp();
  }
}
