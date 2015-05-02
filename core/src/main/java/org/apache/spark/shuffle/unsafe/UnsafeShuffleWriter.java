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

import scala.Option;
import scala.Product2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;

import com.esotericsoftware.kryo.io.ByteBufferOutputStream;

import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.IndexShuffleBlockManager;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.BlockObjectWriter;
import org.apache.spark.storage.ShuffleBlockId;
import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.TaskMemoryManager;
import org.apache.spark.unsafe.sort.UnsafeSorter;
import static org.apache.spark.unsafe.sort.UnsafeSorter.*;

// IntelliJ gets confused and claims that this class should be abstract, but this actually compiles
public class UnsafeShuffleWriter<K, V> implements ShuffleWriter<K, V> {

  private static final int PAGE_SIZE = 1024 * 1024;  // TODO: tune this
  private static final int SER_BUFFER_SIZE = 1024 * 1024;  // TODO: tune this
  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

  private final IndexShuffleBlockManager shuffleBlockManager;
  private final BlockManager blockManager = SparkEnv.get().blockManager();
  private final int shuffleId;
  private final int mapId;
  private final TaskMemoryManager memoryManager;
  private final SerializerInstance serializer;
  private final Partitioner partitioner;
  private final ShuffleWriteMetrics writeMetrics;
  private final LinkedList<MemoryBlock> allocatedPages = new LinkedList<MemoryBlock>();
  private final int fileBufferSize;
  private MapStatus mapStatus = null;

  private MemoryBlock currentPage = null;
  private long currentPagePosition = PAGE_SIZE;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private boolean stopping = false;

  public UnsafeShuffleWriter(
      IndexShuffleBlockManager shuffleBlockManager,
      UnsafeShuffleHandle<K, V> handle,
      int mapId,
      TaskContext context) {
    this.shuffleBlockManager = shuffleBlockManager;
    this.mapId = mapId;
    this.memoryManager = context.taskMemoryManager();
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.shuffleId = dep.shuffleId();
    this.serializer = Serializer.getSerializer(dep.serializer()).newInstance();
    this.partitioner = dep.partitioner();
    this.writeMetrics = new ShuffleWriteMetrics();
    context.taskMetrics().shuffleWriteMetrics_$eq(Option.apply(writeMetrics));
    this.fileBufferSize =
      // Use getSizeAsKb (not bytes) to maintain backwards compatibility for units
      (int) SparkEnv.get().conf().getSizeAsKb("spark.shuffle.file.buffer", "32k") * 1024;
  }

  public void write(scala.collection.Iterator<Product2<K, V>> records) {
    try {
      final long[] partitionLengths = writeSortedRecordsToFile(sortRecords(records));
      shuffleBlockManager.writeIndexFile(shuffleId, mapId, partitionLengths);
      mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
    } catch (Exception e) {
      PlatformDependent.throwException(e);
    }
  }

  private void ensureSpaceInDataPage(long requiredSpace) throws Exception {
    if (requiredSpace > PAGE_SIZE) {
      // TODO: throw a more specific exception?
      throw new Exception("Required space " + requiredSpace + " is greater than page size (" +
        PAGE_SIZE + ")");
    } else if (requiredSpace > (PAGE_SIZE - currentPagePosition)) {
      currentPage = memoryManager.allocatePage(PAGE_SIZE);
      currentPagePosition = currentPage.getBaseOffset();
      allocatedPages.add(currentPage);
    }
  }

  private void freeMemory() {
    final Iterator<MemoryBlock> iter = allocatedPages.iterator();
    while (iter.hasNext()) {
      memoryManager.freePage(iter.next());
      iter.remove();
    }
  }

  private Iterator<RecordPointerAndKeyPrefix> sortRecords(
      scala.collection.Iterator<? extends Product2<K, V>> records) throws Exception {
    final UnsafeSorter sorter = new UnsafeSorter(
      memoryManager,
      RECORD_COMPARATOR,
      PREFIX_COMPARATOR,
      4096 // Initial size (TODO: tune this!)
    );

    final byte[] serArray = new byte[SER_BUFFER_SIZE];
    final ByteBuffer serByteBuffer = ByteBuffer.wrap(serArray);
    // TODO: we should not depend on this class from Kryo; copy its source or find an alternative
    final SerializationStream serOutputStream =
      serializer.serializeStream(new ByteBufferOutputStream(serByteBuffer));

    while (records.hasNext()) {
      final Product2<K, V> record = records.next();
      final K key = record._1();
      final int partitionId = partitioner.getPartition(key);
      serByteBuffer.position(0);
      serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
      serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
      serOutputStream.flush();

      final int serializedRecordSize = serByteBuffer.position();
      assert (serializedRecordSize > 0);
      // Need 4 bytes to store the record length.
      ensureSpaceInDataPage(serializedRecordSize + 4);

      final long recordAddress =
        memoryManager.encodePageNumberAndOffset(currentPage, currentPagePosition);
      final Object baseObject = currentPage.getBaseObject();
      PlatformDependent.UNSAFE.putInt(baseObject, currentPagePosition, serializedRecordSize);
      currentPagePosition += 4;
      PlatformDependent.copyMemory(
        serArray,
        PlatformDependent.BYTE_ARRAY_OFFSET,
        baseObject,
        currentPagePosition,
        serializedRecordSize);
      currentPagePosition += serializedRecordSize;

      sorter.insertRecord(recordAddress, partitionId);
    }

    return sorter.getSortedIterator();
  }

  private long[] writeSortedRecordsToFile(
      Iterator<RecordPointerAndKeyPrefix> sortedRecords) throws IOException {
    final File outputFile = shuffleBlockManager.getDataFile(shuffleId, mapId);
    final ShuffleBlockId blockId =
      new ShuffleBlockId(shuffleId, mapId, IndexShuffleBlockManager.NOOP_REDUCE_ID());
    final long[] partitionLengths = new long[partitioner.numPartitions()];

    int currentPartition = -1;
    BlockObjectWriter writer = null;

    final byte[] arr = new byte[SER_BUFFER_SIZE];
    while (sortedRecords.hasNext()) {
      final RecordPointerAndKeyPrefix recordPointer = sortedRecords.next();
      final int partition = (int) recordPointer.keyPrefix;
      assert (partition >= currentPartition);
      if (partition != currentPartition) {
        // Switch to the new partition
        if (currentPartition != -1) {
          writer.commitAndClose();
          partitionLengths[currentPartition] = writer.fileSegment().length();
        }
        currentPartition = partition;
        writer =
          blockManager.getDiskWriter(blockId, outputFile, serializer, fileBufferSize, writeMetrics);
      }

      final Object baseObject = memoryManager.getPage(recordPointer.recordPointer);
      final long baseOffset = memoryManager.getOffsetInPage(recordPointer.recordPointer);
      final int recordLength = (int) PlatformDependent.UNSAFE.getLong(baseObject, baseOffset);
      PlatformDependent.copyMemory(
        baseObject,
        baseOffset + 4,
        arr,
        PlatformDependent.BYTE_ARRAY_OFFSET,
        recordLength);
      assert (writer != null);  // To suppress an IntelliJ warning
      writer.write(arr, 0, recordLength);
      // TODO: add a test that detects whether we leave this call out:
      writer.recordWritten();
    }

    if (writer != null) {
      writer.commitAndClose();
      partitionLengths[currentPartition] = writer.fileSegment().length();
    }

    return partitionLengths;
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      if (stopping) {
        return Option.apply(null);
      } else {
        stopping = true;
        freeMemory();
        if (success) {
          return Option.apply(mapStatus);
        } else {
          // The map task failed, so delete our output data.
          shuffleBlockManager.removeDataByMap(shuffleId, mapId);
          return Option.apply(null);
        }
      }
    } finally {
      freeMemory();
      // TODO: increment the shuffle write time metrics
    }
  }

  private static final RecordComparator RECORD_COMPARATOR = new RecordComparator() {
    @Override
    public int compare(
        Object leftBaseObject, long leftBaseOffset, Object rightBaseObject, long rightBaseOffset) {
      return 0;
    }
  };

  private static final PrefixComparator PREFIX_COMPARATOR = new PrefixComparator() {
    @Override
    public int compare(long prefix1, long prefix2) {
      return (int) (prefix1 - prefix2);
    }
  };
}
