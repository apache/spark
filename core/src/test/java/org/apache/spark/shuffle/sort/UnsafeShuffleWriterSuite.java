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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.*;

import org.apache.spark.*;
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper;
import org.apache.spark.shuffle.ShuffleChecksumTestHelper;
import org.mockito.stubbing.Answer;
import scala.*;
import scala.collection.Iterator;

import com.google.common.collect.HashMultiset;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.internal.config.package$;
import org.apache.spark.io.CompressionCodec$;
import org.apache.spark.io.LZ4CompressionCodec;
import org.apache.spark.io.LZFCompressionCodec;
import org.apache.spark.io.SnappyCompressionCodec;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.TestMemoryManager;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.security.CryptoStreamUtils;
import org.apache.spark.serializer.*;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleExecutorComponents;
import org.apache.spark.storage.*;
import org.apache.spark.util.Utils;

import static org.junit.Assert.*;
import static org.mockito.Answers.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.*;

public class UnsafeShuffleWriterSuite implements ShuffleChecksumTestHelper {

  static final int DEFAULT_INITIAL_SORT_BUFFER_SIZE = 4096;
  static final int NUM_PARTITIONS = 4;
  TestMemoryManager memoryManager;
  TaskMemoryManager taskMemoryManager;
  final HashPartitioner hashPartitioner = new HashPartitioner(NUM_PARTITIONS);
  File mergedOutputFile;
  File tempDir;
  long[] partitionSizesInMergedFile;
  final LinkedList<File> spillFilesCreated = new LinkedList<>();
  SparkConf conf;
  final Serializer serializer = new KryoSerializer(new SparkConf());
  TaskMetrics taskMetrics;

  @Mock(answer = RETURNS_SMART_NULLS) BlockManager blockManager;
  @Mock(answer = RETURNS_SMART_NULLS) IndexShuffleBlockResolver shuffleBlockResolver;
  @Mock(answer = RETURNS_SMART_NULLS) DiskBlockManager diskBlockManager;
  @Mock(answer = RETURNS_SMART_NULLS) TaskContext taskContext;
  @Mock(answer = RETURNS_SMART_NULLS) ShuffleDependency<Object, Object, Object> shuffleDep;

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
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    tempDir = Utils.createTempDir(null, "test");
    mergedOutputFile = File.createTempFile("mergedoutput", "", tempDir);
    partitionSizesInMergedFile = null;
    spillFilesCreated.clear();
    conf = new SparkConf()
      .set(package$.MODULE$.BUFFER_PAGESIZE().key(), "1m")
      .set(package$.MODULE$.MEMORY_OFFHEAP_ENABLED(), false);
    taskMetrics = new TaskMetrics();
    memoryManager = new TestMemoryManager(conf);
    taskMemoryManager = new TaskMemoryManager(memoryManager, 0);

    // Some tests will override this manager because they change the configuration. This is a
    // default for tests that don't need a specific one.
    SerializerManager manager = new SerializerManager(serializer, conf);
    when(blockManager.serializerManager()).thenReturn(manager);

    when(blockManager.diskBlockManager()).thenReturn(diskBlockManager);
    when(blockManager.getDiskWriter(
      any(BlockId.class),
      any(File.class),
      any(SerializerInstance.class),
      anyInt(),
      any(ShuffleWriteMetrics.class))).thenAnswer(invocationOnMock -> {
        Object[] args = invocationOnMock.getArguments();
        return new DiskBlockObjectWriter(
          (File) args[1],
          blockManager.serializerManager(),
          (SerializerInstance) args[2],
          (Integer) args[3],
          false,
          (ShuffleWriteMetrics) args[4],
          (BlockId) args[0]
        );
      });

    when(shuffleBlockResolver.getDataFile(anyInt(), anyLong())).thenReturn(mergedOutputFile);

    Answer<?> renameTempAnswer = invocationOnMock -> {
      partitionSizesInMergedFile = (long[]) invocationOnMock.getArguments()[2];
      File tmp = (File) invocationOnMock.getArguments()[4];
      if (!mergedOutputFile.delete()) {
        throw new RuntimeException("Failed to delete old merged output file.");
      }
      if (tmp != null) {
        Files.move(tmp.toPath(), mergedOutputFile.toPath());
      } else if (!mergedOutputFile.createNewFile()) {
        throw new RuntimeException("Failed to create empty merged output file.");
      }
      return null;
    };

    doAnswer(renameTempAnswer)
        .when(shuffleBlockResolver)
        .writeMetadataFileAndCommit(
          anyInt(), anyLong(), any(long[].class), any(long[].class), any(File.class));

    doAnswer(renameTempAnswer)
        .when(shuffleBlockResolver)
        .writeMetadataFileAndCommit(
          anyInt(), anyLong(), any(long[].class), any(long[].class), eq(null));

    when(diskBlockManager.createTempShuffleBlock()).thenAnswer(invocationOnMock -> {
      TempShuffleBlockId blockId = new TempShuffleBlockId(UUID.randomUUID());
      File file = File.createTempFile("spillFile", ".spill", tempDir);
      spillFilesCreated.add(file);
      return Tuple2$.MODULE$.apply(blockId, file);
    });

    when(taskContext.taskMetrics()).thenReturn(taskMetrics);
    when(shuffleDep.serializer()).thenReturn(serializer);
    when(shuffleDep.partitioner()).thenReturn(hashPartitioner);
    when(taskContext.taskMemoryManager()).thenReturn(taskMemoryManager);
  }

  private UnsafeShuffleWriter<Object, Object> createWriter(boolean transferToEnabled)
    throws SparkException {
    return createWriter(transferToEnabled, shuffleBlockResolver);
  }

  private UnsafeShuffleWriter<Object, Object> createWriter(
    boolean transferToEnabled,
    IndexShuffleBlockResolver blockResolver) throws SparkException {
    conf.set("spark.file.transferTo", String.valueOf(transferToEnabled));
    return new UnsafeShuffleWriter<>(
      blockManager,
      taskMemoryManager,
      new SerializedShuffleHandle<>(0, shuffleDep),
      0L, // map id
      taskContext,
      conf,
      taskContext.taskMetrics().shuffleWriteMetrics(),
      new LocalDiskShuffleExecutorComponents(conf, blockManager, blockResolver));
  }

  private void assertSpillFilesWereCleanedUp() {
    for (File spillFile : spillFilesCreated) {
      assertFalse("Spill file " + spillFile.getPath() + " was not cleaned up",
        spillFile.exists());
    }
  }

  private List<Tuple2<Object, Object>> readRecordsFromFile() throws IOException {
    final ArrayList<Tuple2<Object, Object>> recordsList = new ArrayList<>();
    long startOffset = 0;
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      final long partitionSize = partitionSizesInMergedFile[i];
      if (partitionSize > 0) {
        FileInputStream fin = new FileInputStream(mergedOutputFile);
        fin.getChannel().position(startOffset);
        InputStream in = new LimitedInputStream(fin, partitionSize);
        in = blockManager.serializerManager().wrapForEncryption(in);
        if ((boolean) conf.get(package$.MODULE$.SHUFFLE_COMPRESS())) {
          in = CompressionCodec$.MODULE$.createCodec(conf).compressedInputStream(in);
        }
        try (DeserializationStream recordsStream = serializer.newInstance().deserializeStream(in)) {
          Iterator<Tuple2<Object, Object>> records = recordsStream.asKeyValueIterator();
          while (records.hasNext()) {
            Tuple2<Object, Object> record = records.next();
            assertEquals(i, hashPartitioner.getPartition(record._1()));
            recordsList.add(record);
          }
        }
        startOffset += partitionSize;
      }
    }
    return recordsList;
  }

  @Test(expected=IllegalStateException.class)
  public void mustCallWriteBeforeSuccessfulStop() throws IOException, SparkException {
    createWriter(false).stop(true);
  }

  @Test
  public void doNotNeedToCallWriteBeforeUnsuccessfulStop() throws IOException, SparkException {
    createWriter(false).stop(false);
  }

  static class PandaException extends RuntimeException {
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
    writer.write(Collections.emptyIterator());
    final Option<MapStatus> mapStatus = writer.stop(true);
    assertTrue(mapStatus.isDefined());
    assertTrue(mergedOutputFile.exists());
    assertEquals(0, spillFilesCreated.size());
    assertArrayEquals(new long[NUM_PARTITIONS], partitionSizesInMergedFile);
    assertEquals(0, taskMetrics.shuffleWriteMetrics().recordsWritten());
    assertEquals(0, taskMetrics.shuffleWriteMetrics().bytesWritten());
    assertEquals(0, taskMetrics.diskBytesSpilled());
    assertEquals(0, taskMetrics.memoryBytesSpilled());
  }

  @Test
  public void writeWithoutSpilling() throws Exception {
    // In this example, each partition should have exactly one record:
    final ArrayList<Product2<Object, Object>> dataToWrite = new ArrayList<>();
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      dataToWrite.add(new Tuple2<>(i, i));
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
    ShuffleWriteMetrics shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics();
    assertEquals(dataToWrite.size(), shuffleWriteMetrics.recordsWritten());
    assertEquals(0, taskMetrics.diskBytesSpilled());
    assertEquals(0, taskMetrics.memoryBytesSpilled());
    assertEquals(mergedOutputFile.length(), shuffleWriteMetrics.bytesWritten());
  }

  @Test
  public void writeChecksumFileWithoutSpill() throws Exception {
    IndexShuffleBlockResolver blockResolver = new IndexShuffleBlockResolver(conf, blockManager);
    ShuffleChecksumBlockId checksumBlockId =
      new ShuffleChecksumBlockId(0, 0, IndexShuffleBlockResolver.NOOP_REDUCE_ID());
    String checksumAlgorithm = conf.get(package$.MODULE$.SHUFFLE_CHECKSUM_ALGORITHM());
    String checksumFileName = ShuffleChecksumHelper.getChecksumFileName(
      checksumBlockId.name(), checksumAlgorithm);
    File checksumFile = new File(tempDir, checksumFileName);
    File dataFile = new File(tempDir, "data");
    File indexFile = new File(tempDir, "index");
    when(diskBlockManager.getFile(checksumFileName)).thenReturn(checksumFile);
    when(diskBlockManager.getFile(new ShuffleDataBlockId(shuffleDep.shuffleId(), 0, 0)))
      .thenReturn(dataFile);
    when(diskBlockManager.getFile(new ShuffleIndexBlockId(shuffleDep.shuffleId(), 0, 0)))
      .thenReturn(indexFile);

    // In this example, each partition should have exactly one record:
    final ArrayList<Product2<Object, Object>> dataToWrite = new ArrayList<>();
    for (int i = 0; i < NUM_PARTITIONS; i ++) {
      dataToWrite.add(new Tuple2<>(i, i));
    }
    final UnsafeShuffleWriter<Object, Object> writer1 = createWriter(true, blockResolver);
    writer1.write(dataToWrite.iterator());
    writer1.stop(true);
    assertTrue(checksumFile.exists());
    assertEquals(checksumFile.length(), 8 * NUM_PARTITIONS);
    compareChecksums(NUM_PARTITIONS, checksumAlgorithm, checksumFile, dataFile, indexFile);
  }

  @Test
  public void writeChecksumFileWithSpill() throws Exception {
    IndexShuffleBlockResolver blockResolver = new IndexShuffleBlockResolver(conf, blockManager);
    ShuffleChecksumBlockId checksumBlockId =
      new ShuffleChecksumBlockId(0, 0, IndexShuffleBlockResolver.NOOP_REDUCE_ID());
    String checksumAlgorithm = conf.get(package$.MODULE$.SHUFFLE_CHECKSUM_ALGORITHM());
    String checksumFileName = ShuffleChecksumHelper.getChecksumFileName(
      checksumBlockId.name(), checksumAlgorithm);
    File checksumFile = new File(tempDir, checksumFileName);
    File dataFile = new File(tempDir, "data");
    File indexFile = new File(tempDir, "index");
    when(diskBlockManager.getFile(checksumFileName)).thenReturn(checksumFile);
    when(diskBlockManager.getFile(new ShuffleDataBlockId(shuffleDep.shuffleId(), 0, 0)))
      .thenReturn(dataFile);
    when(diskBlockManager.getFile(new ShuffleIndexBlockId(shuffleDep.shuffleId(), 0, 0)))
      .thenReturn(indexFile);

    final UnsafeShuffleWriter<Object, Object> writer1 = createWriter(true, blockResolver);
    writer1.insertRecordIntoSorter(new Tuple2<>(0, 0));
    writer1.forceSorterToSpill();
    writer1.insertRecordIntoSorter(new Tuple2<>(1, 0));
    writer1.insertRecordIntoSorter(new Tuple2<>(2, 0));
    writer1.forceSorterToSpill();
    writer1.insertRecordIntoSorter(new Tuple2<>(0, 1));
    writer1.insertRecordIntoSorter(new Tuple2<>(3, 0));
    writer1.forceSorterToSpill();
    writer1.insertRecordIntoSorter(new Tuple2<>(1, 1));
    writer1.forceSorterToSpill();
    writer1.insertRecordIntoSorter(new Tuple2<>(0, 2));
    writer1.forceSorterToSpill();
    writer1.closeAndWriteOutput();
    assertTrue(checksumFile.exists());
    assertEquals(checksumFile.length(), 8 * NUM_PARTITIONS);
    compareChecksums(NUM_PARTITIONS, checksumAlgorithm, checksumFile, dataFile, indexFile);
  }

  private void testMergingSpills(
      final boolean transferToEnabled,
      String compressionCodecName,
      boolean encrypt) throws Exception {
    if (compressionCodecName != null) {
      conf.set(package$.MODULE$.SHUFFLE_COMPRESS(), true);
      conf.set("spark.io.compression.codec", compressionCodecName);
    } else {
      conf.set(package$.MODULE$.SHUFFLE_COMPRESS(), false);
    }
    conf.set(package$.MODULE$.IO_ENCRYPTION_ENABLED(), encrypt);

    SerializerManager manager;
    if (encrypt) {
      manager = new SerializerManager(serializer, conf,
        Option.apply(CryptoStreamUtils.createKey(conf)));
    } else {
      manager = new SerializerManager(serializer, conf);
    }

    when(blockManager.serializerManager()).thenReturn(manager);
    testMergingSpills(transferToEnabled, encrypt);
  }

  private void testMergingSpills(
      boolean transferToEnabled,
      boolean encrypted) throws IOException, SparkException {
    final UnsafeShuffleWriter<Object, Object> writer = createWriter(transferToEnabled);
    final ArrayList<Product2<Object, Object>> dataToWrite = new ArrayList<>();
    for (int i : new int[] { 1, 2, 3, 4, 4, 2 }) {
      dataToWrite.add(new Tuple2<>(i, i));
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

    assertEquals(HashMultiset.create(dataToWrite), HashMultiset.create(readRecordsFromFile()));
    assertSpillFilesWereCleanedUp();
    ShuffleWriteMetrics shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics();
    assertEquals(dataToWrite.size(), shuffleWriteMetrics.recordsWritten());
    assertTrue(taskMetrics.diskBytesSpilled() > 0L);
    assertTrue(taskMetrics.diskBytesSpilled() < mergedOutputFile.length());
    assertTrue(taskMetrics.memoryBytesSpilled() > 0L);
    assertEquals(mergedOutputFile.length(), shuffleWriteMetrics.bytesWritten());
  }

  @Test
  public void mergeSpillsWithTransferToAndLZF() throws Exception {
    testMergingSpills(true, LZFCompressionCodec.class.getName(), false);
  }

  @Test
  public void mergeSpillsWithFileStreamAndLZF() throws Exception {
    testMergingSpills(false, LZFCompressionCodec.class.getName(), false);
  }

  @Test
  public void mergeSpillsWithTransferToAndLZ4() throws Exception {
    testMergingSpills(true, LZ4CompressionCodec.class.getName(), false);
  }

  @Test
  public void mergeSpillsWithFileStreamAndLZ4() throws Exception {
    testMergingSpills(false, LZ4CompressionCodec.class.getName(), false);
  }

  @Test
  public void mergeSpillsWithTransferToAndSnappy() throws Exception {
    testMergingSpills(true, SnappyCompressionCodec.class.getName(), false);
  }

  @Test
  public void mergeSpillsWithFileStreamAndSnappy() throws Exception {
    testMergingSpills(false, SnappyCompressionCodec.class.getName(), false);
  }

  @Test
  public void mergeSpillsWithTransferToAndNoCompression() throws Exception {
    testMergingSpills(true, null, false);
  }

  @Test
  public void mergeSpillsWithFileStreamAndNoCompression() throws Exception {
    testMergingSpills(false, null, false);
  }

  @Test
  public void mergeSpillsWithCompressionAndEncryption() throws Exception {
    // This should actually be translated to a "file stream merge" internally, just have the
    // test to make sure that it's the case.
    testMergingSpills(true, LZ4CompressionCodec.class.getName(), true);
  }

  @Test
  public void mergeSpillsWithFileStreamAndCompressionAndEncryption() throws Exception {
    testMergingSpills(false, LZ4CompressionCodec.class.getName(), true);
  }

  @Test
  public void mergeSpillsWithCompressionAndEncryptionSlowPath() throws Exception {
    conf.set(package$.MODULE$.SHUFFLE_UNSAFE_FAST_MERGE_ENABLE(), false);
    testMergingSpills(false, LZ4CompressionCodec.class.getName(), true);
  }

  @Test
  public void mergeSpillsWithEncryptionAndNoCompression() throws Exception {
    // This should actually be translated to a "file stream merge" internally, just have the
    // test to make sure that it's the case.
    testMergingSpills(true, null, true);
  }

  @Test
  public void mergeSpillsWithFileStreamAndEncryptionAndNoCompression() throws Exception {
    testMergingSpills(false, null, true);
  }

  @Test
  public void writeEnoughDataToTriggerSpill() throws Exception {
    memoryManager.limit(PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES);
    final UnsafeShuffleWriter<Object, Object> writer = createWriter(false);
    final ArrayList<Product2<Object, Object>> dataToWrite = new ArrayList<>();
    final byte[] bigByteArray = new byte[PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES / 10];
    for (int i = 0; i < 10 + 1; i++) {
      dataToWrite.add(new Tuple2<>(i, bigByteArray));
    }
    writer.write(dataToWrite.iterator());
    assertEquals(2, spillFilesCreated.size());
    writer.stop(true);
    readRecordsFromFile();
    assertSpillFilesWereCleanedUp();
    ShuffleWriteMetrics shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics();
    assertEquals(dataToWrite.size(), shuffleWriteMetrics.recordsWritten());
    assertTrue(taskMetrics.diskBytesSpilled() > 0L);
    assertTrue(taskMetrics.diskBytesSpilled() < mergedOutputFile.length());
    assertTrue(taskMetrics.memoryBytesSpilled()> 0L);
    assertEquals(mergedOutputFile.length(), shuffleWriteMetrics.bytesWritten());
  }

  @Test
  public void writeEnoughRecordsToTriggerSortBufferExpansionAndSpillRadixOff() throws Exception {
    conf.set(package$.MODULE$.SHUFFLE_SORT_USE_RADIXSORT(), false);
    writeEnoughRecordsToTriggerSortBufferExpansionAndSpill();
    assertEquals(2, spillFilesCreated.size());
  }

  @Test
  public void writeEnoughRecordsToTriggerSortBufferExpansionAndSpillRadixOn() throws Exception {
    conf.set(package$.MODULE$.SHUFFLE_SORT_USE_RADIXSORT(), true);
    writeEnoughRecordsToTriggerSortBufferExpansionAndSpill();
    assertEquals(3, spillFilesCreated.size());
  }

  private void writeEnoughRecordsToTriggerSortBufferExpansionAndSpill() throws Exception {
    memoryManager.limit(DEFAULT_INITIAL_SORT_BUFFER_SIZE * 16);
    final UnsafeShuffleWriter<Object, Object> writer = createWriter(false);
    final ArrayList<Product2<Object, Object>> dataToWrite = new ArrayList<>();
    for (int i = 0; i < DEFAULT_INITIAL_SORT_BUFFER_SIZE + 1; i++) {
      dataToWrite.add(new Tuple2<>(i, i));
    }
    writer.write(dataToWrite.iterator());
    writer.stop(true);
    readRecordsFromFile();
    assertSpillFilesWereCleanedUp();
    ShuffleWriteMetrics shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics();
    assertEquals(dataToWrite.size(), shuffleWriteMetrics.recordsWritten());
    assertTrue(taskMetrics.diskBytesSpilled() > 0L);
    assertTrue(taskMetrics.diskBytesSpilled() < mergedOutputFile.length());
    assertTrue(taskMetrics.memoryBytesSpilled()> 0L);
    assertEquals(mergedOutputFile.length(), shuffleWriteMetrics.bytesWritten());
  }

  @Test
  public void writeRecordsThatAreBiggerThanDiskWriteBufferSize() throws Exception {
    final UnsafeShuffleWriter<Object, Object> writer = createWriter(false);
    final ArrayList<Product2<Object, Object>> dataToWrite = new ArrayList<>();
    final byte[] bytes = new byte[(int) (ShuffleExternalSorter.DISK_WRITE_BUFFER_SIZE * 2.5)];
    new Random(42).nextBytes(bytes);
    dataToWrite.add(new Tuple2<>(1, ByteBuffer.wrap(bytes)));
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
    final ArrayList<Product2<Object, Object>> dataToWrite = new ArrayList<>();
    dataToWrite.add(new Tuple2<>(1, ByteBuffer.wrap(new byte[1])));
    // We should be able to write a record that's right _at_ the max record size
    final byte[] atMaxRecordSize = new byte[(int) taskMemoryManager.pageSizeBytes() - 4];
    new Random(42).nextBytes(atMaxRecordSize);
    dataToWrite.add(new Tuple2<>(2, ByteBuffer.wrap(atMaxRecordSize)));
    // Inserting a record that's larger than the max record size
    final byte[] exceedsMaxRecordSize = new byte[(int) taskMemoryManager.pageSizeBytes()];
    new Random(42).nextBytes(exceedsMaxRecordSize);
    dataToWrite.add(new Tuple2<>(3, ByteBuffer.wrap(exceedsMaxRecordSize)));
    writer.write(dataToWrite.iterator());
    writer.stop(true);
    assertEquals(
      HashMultiset.create(dataToWrite),
      HashMultiset.create(readRecordsFromFile()));
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void spillFilesAreDeletedWhenStoppingAfterError() throws IOException, SparkException {
    final UnsafeShuffleWriter<Object, Object> writer = createWriter(false);
    writer.insertRecordIntoSorter(new Tuple2<>(1, 1));
    writer.insertRecordIntoSorter(new Tuple2<>(2, 2));
    writer.forceSorterToSpill();
    writer.insertRecordIntoSorter(new Tuple2<>(2, 2));
    writer.stop(false);
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void testPeakMemoryUsed() throws Exception {
    final long recordLengthBytes = 8;
    final long pageSizeBytes = 256;
    final long numRecordsPerPage = pageSizeBytes / recordLengthBytes;
    taskMemoryManager = spy(taskMemoryManager);
    when(taskMemoryManager.pageSizeBytes()).thenReturn(pageSizeBytes);
    final UnsafeShuffleWriter<Object, Object> writer = new UnsafeShuffleWriter<>(
        blockManager,
        taskMemoryManager,
        new SerializedShuffleHandle<>(0, shuffleDep),
        0L, // map id
        taskContext,
        conf,
        taskContext.taskMetrics().shuffleWriteMetrics(),
        new LocalDiskShuffleExecutorComponents(conf, blockManager, shuffleBlockResolver));

    // Peak memory should be monotonically increasing. More specifically, every time
    // we allocate a new page it should increase by exactly the size of the page.
    long previousPeakMemory = writer.getPeakMemoryUsedBytes();
    long newPeakMemory;
    try {
      for (int i = 0; i < numRecordsPerPage * 10; i++) {
        writer.insertRecordIntoSorter(new Tuple2<>(1, 1));
        newPeakMemory = writer.getPeakMemoryUsedBytes();
        if (i % numRecordsPerPage == 0) {
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
        writer.insertRecordIntoSorter(new Tuple2<>(1, 1));
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
