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
import java.nio.ByteBuffer;
import java.util.*;

import scala.*;
import scala.collection.Iterator;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction1;

import com.google.common.collect.Iterators;
import com.google.common.collect.HashMultiset;
import com.google.common.io.ByteStreams;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.*;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.Answers.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.*;

import org.apache.spark.*;
import org.apache.spark.io.CompressionCodec$;
import org.apache.spark.io.LZ4CompressionCodec;
import org.apache.spark.io.LZFCompressionCodec;
import org.apache.spark.io.SnappyCompressionCodec;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.serializer.*;
import org.apache.spark.scheduler.MapStatus;
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
  SparkConf conf;
  final Serializer serializer = new KryoSerializer(new SparkConf());
  TaskMetrics taskMetrics;

  @Mock(answer = RETURNS_SMART_NULLS) ShuffleMemoryManager shuffleMemoryManager;
  @Mock(answer = RETURNS_SMART_NULLS) BlockManager blockManager;
  @Mock(answer = RETURNS_SMART_NULLS) IndexShuffleBlockResolver shuffleBlockResolver;
  @Mock(answer = RETURNS_SMART_NULLS) DiskBlockManager diskBlockManager;
  @Mock(answer = RETURNS_SMART_NULLS) TaskContext taskContext;
  @Mock(answer = RETURNS_SMART_NULLS) ShuffleDependency<Object, Object, Object> shuffleDep;

  private final class CompressStream extends AbstractFunction1<OutputStream, OutputStream> {
    @Override
    public OutputStream apply(OutputStream stream) {
      if (conf.getBoolean("spark.shuffle.compress", true)) {
        return CompressionCodec$.MODULE$.createCodec(conf).compressedOutputStream(stream);
      } else {
        return stream;
      }
    }
  }

  @After
  public void tearDown() {
    Utils.deleteRecursively(tempDir);
    final long leakedMemory = taskMemoryManager.cleanUpAllAllocatedMemory();
    if (leakedMemory != 0) {
      fail("Test leaked " + leakedMemory + " bytes of managed memory");
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
    conf = new SparkConf().set("spark.buffer.pageSize", "128m");
    taskMetrics = new TaskMetrics();

    when(shuffleMemoryManager.tryToAcquire(anyLong())).then(returnsFirstArg());
    when(shuffleMemoryManager.pageSizeBytes()).thenReturn(128L * 1024 * 1024);

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
    when(blockManager.wrapForCompression(any(BlockId.class), any(InputStream.class))).thenAnswer(
      new Answer<InputStream>() {
        @Override
        public InputStream answer(InvocationOnMock invocation) throws Throwable {
          assert (invocation.getArguments()[0] instanceof TempShuffleBlockId);
          InputStream is = (InputStream) invocation.getArguments()[1];
          if (conf.getBoolean("spark.shuffle.compress", true)) {
            return CompressionCodec$.MODULE$.createCodec(conf).compressedInputStream(is);
          } else {
            return is;
          }
        }
      }
    );

    when(blockManager.wrapForCompression(any(BlockId.class), any(OutputStream.class))).thenAnswer(
      new Answer<OutputStream>() {
        @Override
        public OutputStream answer(InvocationOnMock invocation) throws Throwable {
          assert (invocation.getArguments()[0] instanceof TempShuffleBlockId);
          OutputStream os = (OutputStream) invocation.getArguments()[1];
          if (conf.getBoolean("spark.shuffle.compress", true)) {
            return CompressionCodec$.MODULE$.createCodec(conf).compressedOutputStream(os);
          } else {
            return os;
          }
        }
      }
    );

    when(shuffleBlockResolver.getDataFile(anyInt(), anyInt())).thenReturn(mergedOutputFile);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
        partitionSizesInMergedFile = (long[]) invocationOnMock.getArguments()[2];
        return null;
      }
    }).when(shuffleBlockResolver).writeIndexFile(anyInt(), anyInt(), any(long[].class));

    when(diskBlockManager.createTempShuffleBlock()).thenAnswer(
      new Answer<Tuple2<TempShuffleBlockId, File>>() {
        @Override
        public Tuple2<TempShuffleBlockId, File> answer(
          InvocationOnMock invocationOnMock) throws Throwable {
          TempShuffleBlockId blockId = new TempShuffleBlockId(UUID.randomUUID());
          File file = File.createTempFile("spillFile", ".spill", tempDir);
          spillFilesCreated.add(file);
          return Tuple2$.MODULE$.apply(blockId, file);
        }
      });

    when(taskContext.taskMetrics()).thenReturn(taskMetrics);
    when(taskContext.internalMetricsToAccumulators()).thenReturn(null);

    when(shuffleDep.serializer()).thenReturn(Option.<Serializer>apply(serializer));
    when(shuffleDep.partitioner()).thenReturn(hashPartitioner);
  }

  private UnsafeShuffleWriter<Object, Object> createWriter(
      boolean transferToEnabled) throws IOException {
    conf.set("spark.file.transferTo", String.valueOf(transferToEnabled));
    return new UnsafeShuffleWriter<Object, Object>(
      blockManager,
      shuffleBlockResolver,
      taskMemoryManager,
      shuffleMemoryManager,
      new UnsafeShuffleHandle<Object, Object>(0, 1, shuffleDep),
      0, // map id
      taskContext,
      conf
    );
  }

  private void assertSpillFilesWereCleanedUp() {
    for (File spillFile : spillFilesCreated) {
      assertFalse("Spill file " + spillFile.getPath() + " was not cleaned up",
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
        in = new LimitedInputStream(in, partitionSize);
        if (conf.getBoolean("spark.shuffle.compress", true)) {
          in = CompressionCodec$.MODULE$.createCodec(conf).compressedInputStream(in);
        }
        DeserializationStream recordsStream = serializer.newInstance().deserializeStream(in);
        Iterator<Tuple2<Object, Object>> records = recordsStream.asKeyValueIterator();
        while (records.hasNext()) {
          Tuple2<Object, Object> record = records.next();
          assertEquals(i, hashPartitioner.getPartition(record._1()));
          recordsList.add(record);
        }
        recordsStream.close();
        startOffset += partitionSize;
      }
    }
    return recordsList;
  }

  @Test(expected=IllegalStateException.class)
  public void mustCallWriteBeforeSuccessfulStop() throws IOException {
    createWriter(false).stop(true);
  }

  @Test
  public void doNotNeedToCallWriteBeforeUnsuccessfulStop() throws IOException {
    createWriter(false).stop(false);
  }

  class PandaException extends RuntimeException {
  }

  @Test(expected=PandaException.class)
  public void writeFailurePropagates() throws Exception {
    class BadRecords extends scala.collection.AbstractIterator<Product2<Object, Object>> {
      @Override public boolean hasNext() {
        throw new PandaException();
      }
      @Override public Product2<Object, Object> next() {
        return null;
      }
    }
    final UnsafeShuffleWriter<Object, Object> writer = createWriter(true);
    writer.write(new BadRecords());
  }

  @Test
  public void writeEmptyIterator() throws Exception {
    final UnsafeShuffleWriter<Object, Object> writer = createWriter(true);
    writer.write(Iterators.<Product2<Object, Object>>emptyIterator());
    final Option<MapStatus> mapStatus = writer.stop(true);
    assertTrue(mapStatus.isDefined());
    assertTrue(mergedOutputFile.exists());
    assertArrayEquals(new long[NUM_PARTITITONS], partitionSizesInMergedFile);
    assertEquals(0, taskMetrics.shuffleWriteMetrics().get().shuffleRecordsWritten());
    assertEquals(0, taskMetrics.shuffleWriteMetrics().get().shuffleBytesWritten());
    assertEquals(0, taskMetrics.diskBytesSpilled());
    assertEquals(0, taskMetrics.memoryBytesSpilled());
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
    assertTrue(mapStatus.isDefined());
    assertTrue(mergedOutputFile.exists());

    long sumOfPartitionSizes = 0;
    for (long size: partitionSizesInMergedFile) {
      // All partitions should be the same size:
      assertEquals(partitionSizesInMergedFile[0], size);
      sumOfPartitionSizes += size;
    }
    assertEquals(mergedOutputFile.length(), sumOfPartitionSizes);
    assertEquals(
      HashMultiset.create(dataToWrite),
      HashMultiset.create(readRecordsFromFile()));
    assertSpillFilesWereCleanedUp();
    ShuffleWriteMetrics shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics().get();
    assertEquals(dataToWrite.size(), shuffleWriteMetrics.shuffleRecordsWritten());
    assertEquals(0, taskMetrics.diskBytesSpilled());
    assertEquals(0, taskMetrics.memoryBytesSpilled());
    assertEquals(mergedOutputFile.length(), shuffleWriteMetrics.shuffleBytesWritten());
  }

  private void testMergingSpills(
      boolean transferToEnabled,
      String compressionCodecName) throws IOException {
    if (compressionCodecName != null) {
      conf.set("spark.shuffle.compress", "true");
      conf.set("spark.io.compression.codec", compressionCodecName);
    } else {
      conf.set("spark.shuffle.compress", "false");
    }
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
    assertTrue(mapStatus.isDefined());
    assertTrue(mergedOutputFile.exists());
    assertEquals(2, spillFilesCreated.size());

    long sumOfPartitionSizes = 0;
    for (long size: partitionSizesInMergedFile) {
      sumOfPartitionSizes += size;
    }
    assertEquals(sumOfPartitionSizes, mergedOutputFile.length());

    assertEquals(
      HashMultiset.create(dataToWrite),
      HashMultiset.create(readRecordsFromFile()));
    assertSpillFilesWereCleanedUp();
    ShuffleWriteMetrics shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics().get();
    assertEquals(dataToWrite.size(), shuffleWriteMetrics.shuffleRecordsWritten());
    assertThat(taskMetrics.diskBytesSpilled(), greaterThan(0L));
    assertThat(taskMetrics.diskBytesSpilled(), lessThan(mergedOutputFile.length()));
    assertThat(taskMetrics.memoryBytesSpilled(), greaterThan(0L));
    assertEquals(mergedOutputFile.length(), shuffleWriteMetrics.shuffleBytesWritten());
  }

  @Test
  public void mergeSpillsWithTransferToAndLZF() throws Exception {
    testMergingSpills(true, LZFCompressionCodec.class.getName());
  }

  @Test
  public void mergeSpillsWithFileStreamAndLZF() throws Exception {
    testMergingSpills(false, LZFCompressionCodec.class.getName());
  }

  @Test
  public void mergeSpillsWithTransferToAndLZ4() throws Exception {
    testMergingSpills(true, LZ4CompressionCodec.class.getName());
  }

  @Test
  public void mergeSpillsWithFileStreamAndLZ4() throws Exception {
    testMergingSpills(false, LZ4CompressionCodec.class.getName());
  }

  @Test
  public void mergeSpillsWithTransferToAndSnappy() throws Exception {
    testMergingSpills(true, SnappyCompressionCodec.class.getName());
  }

  @Test
  public void mergeSpillsWithFileStreamAndSnappy() throws Exception {
    testMergingSpills(false, SnappyCompressionCodec.class.getName());
  }

  @Test
  public void mergeSpillsWithTransferToAndNoCompression() throws Exception {
    testMergingSpills(true, null);
  }

  @Test
  public void mergeSpillsWithFileStreamAndNoCompression() throws Exception {
    testMergingSpills(false, null);
  }

  @Test
  public void writeEnoughDataToTriggerSpill() throws Exception {
    when(shuffleMemoryManager.tryToAcquire(anyLong()))
      .then(returnsFirstArg()) // Allocate initial sort buffer
      .then(returnsFirstArg()) // Allocate initial data page
      .thenReturn(0L) // Deny request to allocate new data page
      .then(returnsFirstArg());  // Grant new sort buffer and data page.
    final UnsafeShuffleWriter<Object, Object> writer = createWriter(false);
    final ArrayList<Product2<Object, Object>> dataToWrite = new ArrayList<Product2<Object, Object>>();
    final byte[] bigByteArray = new byte[PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES / 128];
    for (int i = 0; i < 128 + 1; i++) {
      dataToWrite.add(new Tuple2<Object, Object>(i, bigByteArray));
    }
    writer.write(dataToWrite.iterator());
    verify(shuffleMemoryManager, times(5)).tryToAcquire(anyLong());
    assertEquals(2, spillFilesCreated.size());
    writer.stop(true);
    readRecordsFromFile();
    assertSpillFilesWereCleanedUp();
    ShuffleWriteMetrics shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics().get();
    assertEquals(dataToWrite.size(), shuffleWriteMetrics.shuffleRecordsWritten());
    assertThat(taskMetrics.diskBytesSpilled(), greaterThan(0L));
    assertThat(taskMetrics.diskBytesSpilled(), lessThan(mergedOutputFile.length()));
    assertThat(taskMetrics.memoryBytesSpilled(), greaterThan(0L));
    assertEquals(mergedOutputFile.length(), shuffleWriteMetrics.shuffleBytesWritten());
  }

  @Test
  public void writeEnoughRecordsToTriggerSortBufferExpansionAndSpill() throws Exception {
    when(shuffleMemoryManager.tryToAcquire(anyLong()))
      .then(returnsFirstArg()) // Allocate initial sort buffer
      .then(returnsFirstArg()) // Allocate initial data page
      .thenReturn(0L) // Deny request to grow sort buffer
      .then(returnsFirstArg());  // Grant new sort buffer and data page.
    final UnsafeShuffleWriter<Object, Object> writer = createWriter(false);
    final ArrayList<Product2<Object, Object>> dataToWrite = new ArrayList<Product2<Object, Object>>();
    for (int i = 0; i < UnsafeShuffleWriter.INITIAL_SORT_BUFFER_SIZE; i++) {
      dataToWrite.add(new Tuple2<Object, Object>(i, i));
    }
    writer.write(dataToWrite.iterator());
    verify(shuffleMemoryManager, times(5)).tryToAcquire(anyLong());
    assertEquals(2, spillFilesCreated.size());
    writer.stop(true);
    readRecordsFromFile();
    assertSpillFilesWereCleanedUp();
    ShuffleWriteMetrics shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics().get();
    assertEquals(dataToWrite.size(), shuffleWriteMetrics.shuffleRecordsWritten());
    assertThat(taskMetrics.diskBytesSpilled(), greaterThan(0L));
    assertThat(taskMetrics.diskBytesSpilled(), lessThan(mergedOutputFile.length()));
    assertThat(taskMetrics.memoryBytesSpilled(), greaterThan(0L));
    assertEquals(mergedOutputFile.length(), shuffleWriteMetrics.shuffleBytesWritten());
  }

  @Test
  public void writeRecordsThatAreBiggerThanDiskWriteBufferSize() throws Exception {
    final UnsafeShuffleWriter<Object, Object> writer = createWriter(false);
    final ArrayList<Product2<Object, Object>> dataToWrite =
      new ArrayList<Product2<Object, Object>>();
    final byte[] bytes = new byte[(int) (UnsafeShuffleExternalSorter.DISK_WRITE_BUFFER_SIZE * 2.5)];
    new Random(42).nextBytes(bytes);
    dataToWrite.add(new Tuple2<Object, Object>(1, ByteBuffer.wrap(bytes)));
    writer.write(dataToWrite.iterator());
    writer.stop(true);
    assertEquals(
      HashMultiset.create(dataToWrite),
      HashMultiset.create(readRecordsFromFile()));
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void writeRecordsThatAreBiggerThanMaxRecordSize() throws Exception {
    final UnsafeShuffleWriter<Object, Object> writer = createWriter(false);
    final ArrayList<Product2<Object, Object>> dataToWrite = new ArrayList<Product2<Object, Object>>();
    dataToWrite.add(new Tuple2<Object, Object>(1, ByteBuffer.wrap(new byte[1])));
    // We should be able to write a record that's right _at_ the max record size
    final byte[] atMaxRecordSize = new byte[writer.maxRecordSizeBytes()];
    new Random(42).nextBytes(atMaxRecordSize);
    dataToWrite.add(new Tuple2<Object, Object>(2, ByteBuffer.wrap(atMaxRecordSize)));
    // Inserting a record that's larger than the max record size
    final byte[] exceedsMaxRecordSize = new byte[writer.maxRecordSizeBytes() + 1];
    new Random(42).nextBytes(exceedsMaxRecordSize);
    dataToWrite.add(new Tuple2<Object, Object>(3, ByteBuffer.wrap(exceedsMaxRecordSize)));
    writer.write(dataToWrite.iterator());
    writer.stop(true);
    assertEquals(
      HashMultiset.create(dataToWrite),
      HashMultiset.create(readRecordsFromFile()));
    assertSpillFilesWereCleanedUp();
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

  @Test
  public void testPeakMemoryUsed() throws Exception {
    final long recordLengthBytes = 8;
    final long pageSizeBytes = 256;
    final long numRecordsPerPage = pageSizeBytes / recordLengthBytes;
    when(shuffleMemoryManager.pageSizeBytes()).thenReturn(pageSizeBytes);
    final UnsafeShuffleWriter<Object, Object> writer =
      new UnsafeShuffleWriter<Object, Object>(
        blockManager,
        shuffleBlockResolver,
        taskMemoryManager,
        shuffleMemoryManager,
        new UnsafeShuffleHandle<>(0, 1, shuffleDep),
        0, // map id
        taskContext,
        conf);

    // Peak memory should be monotonically increasing. More specifically, every time
    // we allocate a new page it should increase by exactly the size of the page.
    long previousPeakMemory = writer.getPeakMemoryUsedBytes();
    long newPeakMemory;
    try {
      for (int i = 0; i < numRecordsPerPage * 10; i++) {
        writer.insertRecordIntoSorter(new Tuple2<Object, Object>(1, 1));
        newPeakMemory = writer.getPeakMemoryUsedBytes();
        if (i % numRecordsPerPage == 0 && i != 0) {
          // The first page is allocated in constructor, another page will be allocated after
          // every numRecordsPerPage records (peak memory should change).
          assertEquals(previousPeakMemory + pageSizeBytes, newPeakMemory);
        } else {
          assertEquals(previousPeakMemory, newPeakMemory);
        }
        previousPeakMemory = newPeakMemory;
      }

      // Spilling should not change peak memory
      writer.forceSorterToSpill();
      newPeakMemory = writer.getPeakMemoryUsedBytes();
      assertEquals(previousPeakMemory, newPeakMemory);
      for (int i = 0; i < numRecordsPerPage; i++) {
        writer.insertRecordIntoSorter(new Tuple2<Object, Object>(1, 1));
      }
      newPeakMemory = writer.getPeakMemoryUsedBytes();
      assertEquals(previousPeakMemory, newPeakMemory);

      // Closing the writer should not change peak memory
      writer.closeAndWriteOutput();
      newPeakMemory = writer.getPeakMemoryUsedBytes();
      assertEquals(previousPeakMemory, newPeakMemory);
    } finally {
      writer.stop(false);
    }
  }
}
