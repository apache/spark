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

package org.apache.spark.sql.execution;

import java.io.*;
import java.util.*;

import scala.Tuple2;
import scala.math.Ordering;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.serializer.DummySerializerInstance;
import org.apache.spark.shuffle.ShuffleMemoryManager;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction2;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.DiskBlockObjectWriter;
import org.apache.spark.storage.TempLocalBlockId;
import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.MemoryLocation;
import org.apache.spark.unsafe.memory.TaskMemoryManager;
import org.apache.spark.util.collection.unsafe.map.BytesToBytesMap;
import org.apache.spark.util.Utils;

/**
 * Unsafe sort-based external aggregation.
 */
public final class UnsafeExternalAggregation {

  private final Logger logger = LoggerFactory.getLogger(UnsafeExternalAggregation.class);

  /**
   * Special record length that is placed after the last record in a data page.
   */
  private static final int END_OF_PAGE_MARKER = -1;

  private final TaskMemoryManager memoryManager;

  private final ShuffleMemoryManager shuffleMemoryManager;

  private final BlockManager blockManager;

  private final TaskContext taskContext;

  private ShuffleWriteMetrics writeMetrics;

  /**
   * The buffer size to use when writing spills using DiskBlockObjectWriter
   */
  private final int fileBufferSizeBytes;

  /**
   * A linked list for tracking all allocated data pages so that we can free all of our memory.
   */
  private final List<MemoryBlock> dataPages = new LinkedList<MemoryBlock>();

  /**
   * The data page that will be used to store keys and values for new hashtable entries. When this
   * page becomes full, a new page will be allocated and this pointer will change to point to that
   * new page.
   */
  private MemoryBlock currentDataPage = null;

  /**
   * Offset into `currentDataPage` that points to the location where new data can be inserted into
   * the page. This does not incorporate the page's base offset.
   */
  private long pageCursor = 0;

  private long freeSpaceInCurrentPage = 0;

  /**
   * The size of the data pages that hold key and value data. Map entries cannot span multiple
   * pages, so this limits the maximum entry size.
   */
  private static final long PAGE_SIZE_BYTES = 1L << 26; // 64 megabytes

  private final int initialCapacity;

  /**
   * A hashmap which maps from opaque byteArray keys to byteArray values.
   */
  private BytesToBytesMap map;

  /**
   * Re-used pointer to the current aggregation buffer
   */
  private final UnsafeRow currentBuffer = new UnsafeRow();

  private final JoinedRow joinedRow = new JoinedRow();

  private MutableProjection algebraicUpdateProjection;

  private MutableProjection algebraicMergeProjection;

  private AggregateFunction2[] nonAlgebraicAggregateFunctions;

  /**
   * An empty aggregation buffer, encoded in UnsafeRow format. When inserting a new key into the
   * map, we copy this buffer and use it as the value.
   */
  private final byte[] emptyAggregationBuffer;

  private final StructType aggregationBufferSchema;

  private final StructType groupingKeySchema;

  private final UnsafeProjection groupingKeyProjection;

  /**
   * Encodes grouping keys or buffers as UnsafeRows.
   */
  private final Ordering<InternalRow> groupingKeyOrdering;

  private boolean enablePerfMetrics;

  private int testSpillFrequency = 0;

  private long numRowsInserted = 0;

  private final LinkedList<UnsafeSorterKVSpillWriter> spillWriters = new LinkedList<>();

  public UnsafeExternalAggregation(
      TaskMemoryManager memoryManager,
      ShuffleMemoryManager shuffleMemoryManager,
      BlockManager blockManager,
      TaskContext taskContext,
      MutableProjection algebraicUpdateProjection,
      MutableProjection algebraicMergeProjection,
      AggregateFunction2[] nonAlgebraicAggregateFunctions,
      InternalRow emptyAggregationBuffer,
      StructType aggregationBufferSchema,
      StructType groupingKeySchema,
      Ordering<InternalRow> groupingKeyOrdering,
      int initialCapacity,
      SparkConf conf,
      boolean enablePerfMetrics) throws IOException {
    this.memoryManager = memoryManager;
    this.shuffleMemoryManager = shuffleMemoryManager;
    this.blockManager = blockManager;
    this.taskContext = taskContext;
    this.algebraicUpdateProjection = algebraicUpdateProjection;
    this.algebraicMergeProjection = algebraicMergeProjection;
    this.nonAlgebraicAggregateFunctions = nonAlgebraicAggregateFunctions;
    this.aggregationBufferSchema = aggregationBufferSchema;
    this.groupingKeySchema = groupingKeySchema;
    this.groupingKeyProjection = UnsafeProjection.create(groupingKeySchema);
    this.groupingKeyOrdering = groupingKeyOrdering;
    this.initialCapacity = initialCapacity;
    this.enablePerfMetrics = enablePerfMetrics;

    // Initialize the buffer for aggregation value
    final UnsafeProjection valueProjection = UnsafeProjection.create(aggregationBufferSchema);
    this.emptyAggregationBuffer = valueProjection.apply(emptyAggregationBuffer).getBytes();
    assert(this.emptyAggregationBuffer.length == aggregationBufferSchema.length() * 8 +
        UnsafeRow.calculateBitSetWidthInBytes(aggregationBufferSchema.length()));

    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    this.fileBufferSizeBytes = (int) conf.getSizeAsKb("spark.shuffle.file.buffer", "32k") * 1024;
    this.testSpillFrequency = conf.getInt("spark.test.aggregate.spillFrequency", 0);

    initializeUnsafeAppendMap();
  }

  /**
   * Allocates new sort data structures. Called when creating the sorter and after each spill.
   */
  private void initializeUnsafeAppendMap() throws IOException {
    this.writeMetrics = new ShuffleWriteMetrics();

    int capacity = BytesToBytesMap.getCapacity(initialCapacity);
    long memoryRequested = BytesToBytesMap.getMemoryUsage(capacity);
    final long memoryAcquired = shuffleMemoryManager.tryToAcquire(memoryRequested);
    if (memoryAcquired != memoryRequested) {
      shuffleMemoryManager.release(memoryAcquired);
      throw new IOException("Could not acquire " + memoryRequested + " bytes of memory");
    }
    this.map = new BytesToBytesMap(memoryManager, capacity, enablePerfMetrics);
  }

  /**
   * Return true if it can external aggregate with the groupKey schema & aggregationBuffer schema,
   * false otherwise
   */
  public static boolean supportSchema(StructType groupKeySchema,
      StructType aggregationBufferSchema) {
    for (StructField field : groupKeySchema.fields()) {
      if (!UnsafeRow.readableFieldTypes.contains(field.dataType())) {
        return false;
      }
    }
    for (StructField field : aggregationBufferSchema.fields()) {
      if (!UnsafeRow.readableFieldTypes.contains(field.dataType())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Forces spills to occur every `frequency` records. Only for use in tests.
   */
  @VisibleForTesting
  public void setTestSpillFrequency(int frequency) {
    assert frequency > 0 : "Frequency must be positive";
    testSpillFrequency = frequency;
  }

  public void insertRow(InternalRow groupingKey, InternalRow currentRow)
      throws IOException {
    UnsafeRow unsafeGroupingKeyRow = this.groupingKeyProjection.apply(groupingKey);
    numRowsInserted++;
    if (testSpillFrequency > 0 && (numRowsInserted % testSpillFrequency) == 0) {
      spill();
    }
    UnsafeRow aggregationBuffer = this.getAggregationBuffer(unsafeGroupingKeyRow);

    // Process all algebraic aggregate functions.
    this.algebraicUpdateProjection.target(aggregationBuffer).apply(
        joinedRow.apply(aggregationBuffer, currentRow));
    // Process all non-algebraic aggregate functions.
    int i = 0;
    while (i < nonAlgebraicAggregateFunctions.length) {
      nonAlgebraicAggregateFunctions[i].update(aggregationBuffer, currentRow);
      i += 1;
    }
  }

  /**
   * Return the aggregation buffer for the current group. For efficiency, all calls to this method
   * return the same object.
   */
  public UnsafeRow getAggregationBuffer(UnsafeRow unsafeGroupingKeyRow)
      throws IOException {
    // Probe our map using the serialized key
    final BytesToBytesMap.Location loc = map.lookup(
        unsafeGroupingKeyRow.getBaseObject(),
        unsafeGroupingKeyRow.getBaseOffset(),
        unsafeGroupingKeyRow.getSizeInBytes());
    if (!loc.isDefined()) {
      if (!this.putNewKey(
          unsafeGroupingKeyRow.getBaseObject(),
          unsafeGroupingKeyRow.getBaseOffset(),
          unsafeGroupingKeyRow.getSizeInBytes(),
          emptyAggregationBuffer,
          PlatformDependent.BYTE_ARRAY_OFFSET,
          emptyAggregationBuffer.length,
          loc)) {
        // because spill makes putting new key failed, it should get AggregationBuffer again
        return this.getAggregationBuffer(unsafeGroupingKeyRow);
      }
    }
    // Reset the pointer to point to the value that we just stored or looked up:
    final MemoryLocation address = loc.getValueAddress();
    currentBuffer.pointTo(
        address.getBaseObject(),
        address.getBaseOffset(),
        aggregationBufferSchema.length(),
        loc.getValueLength()
    );
    return currentBuffer;
  }

  public boolean putNewKey(
      Object keyBaseObject,
      long keyBaseOffset,
      int keyLengthBytes,
      Object valueBaseObject,
      long valueBaseOffset,
      int valueLengthBytes,
      BytesToBytesMap.Location location) throws IOException {
    assert (!location.isDefined()) : "Can only set value once for a key";

    assert (keyLengthBytes % 8 == 0);
    assert (valueLengthBytes % 8 == 0);

    // Here, we'll copy the data into our data pages. Because we only store a relative offset from
    // the key address instead of storing the absolute address of the value, the key and value
    // must be stored in the same memory page.
    // (8 byte key length) (key) (8 byte value length) (value)
    final int requiredSize = 8 + keyLengthBytes + 8 + valueLengthBytes;
    if (!haveSpaceForRecord(requiredSize)) {
      if (!allocateSpaceForRecord(requiredSize)){
        // if spill have been happened, re-insert current groupingKey
        return false;
      }
    }

    freeSpaceInCurrentPage -= requiredSize;
    // Compute all of our offsets up-front:
    final Object pageBaseObject = currentDataPage.getBaseObject();
    final long pageBaseOffset = currentDataPage.getBaseOffset();
    final long keySizeOffsetInPage = pageBaseOffset + pageCursor;
    pageCursor += 8; // word used to store the key size
    final long keyDataOffsetInPage = pageBaseOffset + pageCursor;
    pageCursor += keyLengthBytes;
    final long valueSizeOffsetInPage = pageBaseOffset + pageCursor;
    pageCursor += 8; // word used to store the value size
    final long valueDataOffsetInPage = pageBaseOffset + pageCursor;
    pageCursor += valueLengthBytes;

    // Copy the key
    PlatformDependent.UNSAFE.putLong(pageBaseObject, keySizeOffsetInPage, keyLengthBytes);
    PlatformDependent.copyMemory(
        keyBaseObject, keyBaseOffset, pageBaseObject, keyDataOffsetInPage, keyLengthBytes);
    // Copy the value
    PlatformDependent.UNSAFE.putLong(pageBaseObject, valueSizeOffsetInPage, valueLengthBytes);
    PlatformDependent.copyMemory(
        valueBaseObject, valueBaseOffset, pageBaseObject, valueDataOffsetInPage, valueLengthBytes);

    final long storedKeyAddress = memoryManager.encodePageNumberAndOffset(
        currentDataPage, keySizeOffsetInPage);
    location.putNewKey(storedKeyAddress);
    return true;
  }

  /**
   * Checks whether there is enough space to insert a new record into the sorter.
   *
   * @param requiredSpace the required space in the data page, in bytes, including space for storing
   *                      the record size.

   * @return true if the record can be inserted without requiring more allocations, false otherwise.
   */
  private boolean haveSpaceForRecord(int requiredSpace) {
    assert (requiredSpace > 0);
    return (map.hasSpaceForAnotherRecord() && (requiredSpace <= freeSpaceInCurrentPage));
  }

  /**
   * Allocates more memory in order to insert an additional record. This will request additional
   * memory from the {@link org.apache.spark.shuffle.ShuffleMemoryManager} and
   * spill if the requested memory can not be obtained.
   *
   * @param requiredSpace the required space in the data page, in bytes, including space for storing
   *                      the record size.
   */
  private boolean allocateSpaceForRecord(int requiredSpace) throws IOException {
    boolean noSpill = true;
    assert (requiredSpace <= PAGE_SIZE_BYTES - 8); // Reserve 8 bytes for the end-of-page marker.
    if (!map.hasSpaceForAnotherRecord()) {
      logger.debug("Attempting to grow size of hash table");
      final int nextCapacity = map.getNextCapacity();
      final long oldPointerArrayMemoryUsage = map.getTotalMemoryConsumption();
      final long memoryToGrowPointerArray = BytesToBytesMap.getMemoryUsage(nextCapacity);
      final long memoryAcquired = shuffleMemoryManager.tryToAcquire(memoryToGrowPointerArray);
      if (memoryAcquired < memoryToGrowPointerArray) {
        shuffleMemoryManager.release(memoryAcquired);
        spill();
      } else {
        map.growAndRehash();
        shuffleMemoryManager.release(oldPointerArrayMemoryUsage);
      }
    }

    // If there's not enough space in the current page, allocate a new page (8 bytes are reserved
    // for the end-of-page marker).
    if (currentDataPage == null || freeSpaceInCurrentPage < requiredSpace) {
      logger.trace("Required space {} is less than free space in current page ({})",
          requiredSpace, freeSpaceInCurrentPage);
      if (currentDataPage != null) {
        // There wasn't enough space in the current page, so write an end-of-page marker:
        final Object pageBaseObject = currentDataPage.getBaseObject();
        final long lengthOffsetInPage = currentDataPage.getBaseOffset() + pageCursor;
        PlatformDependent.UNSAFE.putLong(pageBaseObject, lengthOffsetInPage, END_OF_PAGE_MARKER);
      }
      long memoryAcquired = shuffleMemoryManager.tryToAcquire(PAGE_SIZE_BYTES);
      if (memoryAcquired < PAGE_SIZE_BYTES) {
        shuffleMemoryManager.release(memoryAcquired);
        spill();
        noSpill = false;
        final long memoryAcquiredAfterSpilling = shuffleMemoryManager.tryToAcquire(PAGE_SIZE_BYTES);
        if (memoryAcquiredAfterSpilling != PAGE_SIZE_BYTES) {
          shuffleMemoryManager.release(memoryAcquiredAfterSpilling);
          throw new IOException("Unable to acquire " + PAGE_SIZE_BYTES + " bytes of memory");
        } else {
          memoryAcquired = memoryAcquiredAfterSpilling;
        }
      }
      MemoryBlock newPage = memoryManager.allocatePage(memoryAcquired);
      dataPages.add(newPage);
      pageCursor = 0;
      freeSpaceInCurrentPage = PAGE_SIZE_BYTES - 8;
      currentDataPage = newPage;
    }
    return noSpill;
  }

  private static final class SortComparator implements Comparator<Long> {

    private final TaskMemoryManager memoryManager;
    private final Ordering<InternalRow> ordering;
    private final int numFields;
    private final UnsafeRow row1 = new UnsafeRow();
    private final UnsafeRow row2 = new UnsafeRow();

    SortComparator(TaskMemoryManager memoryManager, Ordering<InternalRow> ordering, int numFields) {
      this.memoryManager = memoryManager;
      this.numFields = numFields;
      this.ordering = ordering;
    }

    @Override
    public int compare(Long fullKeyAddress1, Long fullKeyAddress2) {
      final Object baseObject1 = memoryManager.getPage(fullKeyAddress1);
      final long baseOffset1 = memoryManager.getOffsetInPage(fullKeyAddress1) + 8;

      final Object baseObject2 = memoryManager.getPage(fullKeyAddress2);
      final long baseOffset2 = memoryManager.getOffsetInPage(fullKeyAddress2) + 8;

      row1.pointTo(baseObject1, baseOffset1, numFields, -1);
      row2.pointTo(baseObject2, baseOffset2, numFields, -1);
      return ordering.compare(row1, row2);
    }
  }

  /**
   * Sort and spill the current records in response to memory pressure.
   */
  @VisibleForTesting
  void spill() throws IOException {
    logger.info("Thread {} spilling sort data of {} to disk ({} {} so far)",
        Thread.currentThread().getId(),
        Utils.bytesToString(getMemoryUsage()),
        spillWriters.size(),
        spillWriters.size() > 1 ? " times" : " time");

    final UnsafeSorterKVSpillWriter spillWriter =
        new UnsafeSorterKVSpillWriter(blockManager, fileBufferSizeBytes, writeMetrics,
            map.size());
    spillWriters.add(spillWriter);
    final Iterator<BytesToBytesMap.Location> sortedRecords = map.getSortedIterator(
        new SortComparator(this.memoryManager, groupingKeyOrdering, groupingKeySchema.size()));
    while (sortedRecords.hasNext()) {
      BytesToBytesMap.Location location = sortedRecords.next();
      spillWriter.write(location);
    }
    spillWriter.close();

    final long spillSize = freeMemory();
    taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);

    initializeUnsafeAppendMap();
  }

  public AbstractScalaIterator<BytesToBytesMap.Location> getSortedIterator() {
    return new AbstractScalaIterator<BytesToBytesMap.Location>() {

      Iterator<BytesToBytesMap.Location> sorter = map.getSortedIterator(
          new SortComparator(memoryManager, groupingKeyOrdering, groupingKeySchema.size()));

      @Override
      public boolean hasNext() {
        return sorter.hasNext();
      }

      @Override
      public BytesToBytesMap.Location next() {
        return sorter.next();
      }
    };
  }

  /**
   * Return an iterator that merges the in-memory map with the spilled files.
   * If no spill has occurred, simply return the in-memory map's iterator.
   */
  public AbstractScalaIterator<MapEntry> iterator() throws IOException {
    if (spillWriters.isEmpty()) {
      return this.getMemoryIterator();
    } else {
      return this.merge(this.getSortedIterator());
    }
  }

  public UnsafeRow getKey(BytesToBytesMap.Location location) {
    UnsafeRow key = new UnsafeRow();
    key.pointTo(
        location.getKeyBaseObject(),
        location.getKeyBaseOffset(),
        1,
        location.getKeyLength());
    return key;
  }

  public UnsafeRow getValue(BytesToBytesMap.Location location) {
    UnsafeRow value = new UnsafeRow();
    value.pointTo(
        location.getValueBaseObject(),
        location.getValueBaseOffset(),
        1,
        location.getValueLength());
    return value;
  }

  /**
   * Returns an iterator over the keys and values in in-memory map.
   */
  public AbstractScalaIterator<MapEntry> getMemoryIterator() {
    return new BytesToBytesMapIterator(map.size());
  }

  /**
   * Merge aggregate of the in-memory map with the spilled files, giving an iterator over elements.
   */
  private AbstractScalaIterator<MapEntry> merge(
      AbstractScalaIterator<BytesToBytesMap.Location> inMemory) throws IOException {

    final Comparator<BufferedIterator> keyOrdering =
        new Comparator<BufferedIterator>() {
          private final UnsafeRow row1 = new UnsafeRow();
          private final UnsafeRow row2 = new UnsafeRow();

          public int compare(BufferedIterator o1, BufferedIterator o2){
            row1.pointTo(
                o1.getRecordLocation().getKeyBaseObject(),
                o1.getRecordLocation().getKeyBaseOffset(),
                groupingKeySchema.size(),
                o1.getRecordLocation().getKeyLength());
            row2.pointTo(
                o2.getRecordLocation().getKeyBaseObject(),
                o2.getRecordLocation().getKeyBaseOffset(),
                groupingKeySchema.size(),
                o2.getRecordLocation().getKeyLength());
            return groupingKeyOrdering.compare(row1, row2);
          }
        };
    final Queue<BufferedIterator> priorityQueue =
        new PriorityQueue<BufferedIterator>(spillWriters.size() + 1, keyOrdering);
    BufferedIterator inMemoryBuffer = this.asBuffered(inMemory);
    if (inMemoryBuffer.hasNext()) {
      inMemoryBuffer.loadNext();
      priorityQueue.add(inMemoryBuffer);
    }

    for (int i = 0; i < spillWriters.size(); i++) {
      BufferedIterator spillBuffer = this.asBuffered(spillWriters.get(i).getReader(blockManager));
      if (spillBuffer.hasNext()) {
        spillBuffer.loadNext();
        priorityQueue.add(spillBuffer);
      }
    }
    final AbstractScalaIterator<BytesToBytesMap.Location> mergeIter =
        new AbstractScalaIterator<BytesToBytesMap.Location>() {

          BufferedIterator topIter = null;

          @Override
          public boolean hasNext() {
            return !priorityQueue.isEmpty() || (topIter != null && topIter.hasNext());
          }

          @Override
          public BytesToBytesMap.Location next() throws IOException {
            if (topIter != null && topIter.hasNext()) {
              topIter.loadNext();
              priorityQueue.add(topIter);
            }
            topIter = priorityQueue.poll();
            return topIter.getRecordLocation();
          }
        };

    final BufferedIterator sorted = asBuffered(mergeIter);
    return new AbstractScalaIterator<MapEntry>() {

      private UnsafeRow currentKey = new UnsafeRow();
      private UnsafeRow currentValue = new UnsafeRow();
      private UnsafeRow nextKey = new UnsafeRow();
      private UnsafeRow nextValue = new UnsafeRow();
      private BytesToBytesMap.Location currentLocation = null;

      @Override
      public boolean hasNext() {
        return currentLocation != null || sorted.hasNext();
      }

      @Override
      public MapEntry next() throws IOException {
        try {
          if (currentLocation == null) {
            sorted.loadNext();
            currentLocation = sorted.getRecordLocation();
          }
          currentKey.pointTo(
              currentLocation.getKeyBaseObject(),
              currentLocation.getKeyBaseOffset(),
              groupingKeySchema.size(),
              currentLocation.getKeyLength());
          currentKey = currentKey.copy();
          currentValue.pointTo(
              currentLocation.getValueBaseObject(),
              currentLocation.getValueBaseOffset(),
              aggregationBufferSchema.size(),
              currentLocation.getValueLength());
          currentValue = currentValue.copy();
          currentLocation = null;
          while (sorted.hasNext()) {
            sorted.loadNext();
            BytesToBytesMap.Location nextLocation = sorted.getRecordLocation();
            nextKey.pointTo(
                nextLocation.getKeyBaseObject(),
                nextLocation.getKeyBaseOffset(),
                groupingKeySchema.size(),
                nextLocation.getKeyLength());
            nextValue.pointTo(
                nextLocation.getValueBaseObject(),
                nextLocation.getValueBaseOffset(),
                aggregationBufferSchema.size(),
                nextLocation.getValueLength());

            if (groupingKeyOrdering.compare(currentKey, nextKey) != 0) {
              currentLocation = nextLocation;
              break;
            }

            // Process all algebraic aggregate functions.
            algebraicMergeProjection.target(currentValue).apply(
                joinedRow.apply(currentValue, nextValue));
            // Process all non-algebraic aggregate functions.
            int i = 0;
            while (i < nonAlgebraicAggregateFunctions.length) {
              nonAlgebraicAggregateFunctions[i].merge(currentValue, nextValue);
              i += 1;
            }
          }

          return new MapEntry(currentKey, currentValue);
        } catch (IOException e) {
          cleanupResources();
          // Scala iterators don't declare any checked exceptions, so we need to use this hack
          // to re-throw the exception:
          PlatformDependent.throwException(e);
        }
        throw new RuntimeException("Exception should have been re-thrown in next()");
      }
    };
  }

  public BufferedIterator asBuffered(AbstractScalaIterator<BytesToBytesMap.Location> iterator) {
    return new BufferedIterator(iterator);
  }

  public interface AbstractScalaIterator<E> {

    public abstract boolean hasNext();

    public abstract E next() throws IOException;

  }

  public class BufferedIterator {

    private BytesToBytesMap.Location location = null;
    private AbstractScalaIterator<BytesToBytesMap.Location> iterator;

    public BufferedIterator(AbstractScalaIterator<BytesToBytesMap.Location> iterator) {
      this.iterator = iterator;
    }

    public boolean hasNext() {
      return iterator.hasNext();
    }

    public void loadNext() throws IOException {
      location = iterator.next();
    }

    public BytesToBytesMap.Location getRecordLocation() {
      return location;
    }
  }

  /**
   * Mutable pair object
   */
  public class MapEntry {

    public UnsafeRow key;
    public UnsafeRow value;

    public MapEntry() {
      this.key = new UnsafeRow();
      this.value = new UnsafeRow();
    }

    public MapEntry(UnsafeRow key, UnsafeRow value) {
      this.key = key;
      this.value = value;
    }
  }


  public class UnsafeSorterKVSpillWriter {

    static final int DISK_WRITE_BUFFER_SIZE = 1024 * 1024;

    // Small writes to DiskBlockObjectWriter will be fairly inefficient. Since there doesn't seem to
    // be an API to directly transfer bytes from managed memory to the disk writer, we buffer
    // data through a byte array.
    private byte[] writeBuffer = new byte[DISK_WRITE_BUFFER_SIZE];

    private final File file;
    private final BlockId blockId;
    private final int numRecordsToWrite;
    private DiskBlockObjectWriter writer;
    private int numRecordsSpilled = 0;

    public UnsafeSorterKVSpillWriter(
        BlockManager blockManager,
        int fileBufferSize,
        ShuffleWriteMetrics writeMetrics,
        int numRecordsToWrite) throws IOException {
      final Tuple2<TempLocalBlockId, File> spilledFileInfo =
          blockManager.diskBlockManager().createTempLocalBlock();
      this.file = spilledFileInfo._2();
      this.blockId = spilledFileInfo._1();
      this.numRecordsToWrite = numRecordsToWrite;
      // Unfortunately, we need a serializer instance in order to construct a DiskBlockObjectWriter.
      // Our write path doesn't actually use this serializer (since we end up calling the `write()`
      // OutputStream methods), but DiskBlockObjectWriter still calls some methods on it. To work
      // around this, we pass a dummy no-op serializer.
      writer = blockManager.getDiskWriter(
          blockId, file, DummySerializerInstance.INSTANCE, fileBufferSize, writeMetrics);
      // Write the number of records
      writeIntToBuffer(numRecordsToWrite, 0);
      writer.write(writeBuffer, 0, 4);
    }

    // Based on DataOutputStream.writeInt.
    private void writeIntToBuffer(int v, int offset) throws IOException {
      writeBuffer[offset + 0] = (byte)(v >>> 24);
      writeBuffer[offset + 1] = (byte)(v >>> 16);
      writeBuffer[offset + 2] = (byte)(v >>>  8);
      writeBuffer[offset + 3] = (byte)(v >>>  0);
    }

    public void write(BytesToBytesMap.Location loc) throws IOException {
      if (numRecordsSpilled == numRecordsToWrite) {
        throw new IllegalStateException(
            "Number of records written exceeded numRecordsToWrite = " + numRecordsToWrite);
      } else {
        numRecordsSpilled++;
      }
      this.write(loc.getKeyAddress().getBaseObject(), loc.getKeyAddress().getBaseOffset(),
          loc.getKeyLength());
      this.write(loc.getValueAddress().getBaseObject(), loc.getValueAddress().getBaseOffset(),
          loc.getValueLength());
      writer.recordWritten();
    }


    /**
     * Write a record to a spill file.
     *
     * @param baseObject the base object / memory page containing the record
     * @param baseOffset the base offset which points directly to the record data.
     * @param recordLength the length of the record.
     */
    public void write(
        Object baseObject,
        long baseOffset,
        int recordLength) throws IOException {
      writeIntToBuffer(recordLength, 0);
      int dataRemaining = recordLength;
      int freeSpaceInWriteBuffer = DISK_WRITE_BUFFER_SIZE - 4; // space used by len
      long recordReadPosition = baseOffset;
      while (dataRemaining > 0) {
        final int toTransfer = Math.min(freeSpaceInWriteBuffer, dataRemaining);
        PlatformDependent.copyMemory(
            baseObject,
            recordReadPosition,
            writeBuffer,
            PlatformDependent.BYTE_ARRAY_OFFSET + (DISK_WRITE_BUFFER_SIZE - freeSpaceInWriteBuffer),
            toTransfer);
        writer.write(writeBuffer, 0, (DISK_WRITE_BUFFER_SIZE - freeSpaceInWriteBuffer) + toTransfer);
        recordReadPosition += toTransfer;
        dataRemaining -= toTransfer;
        freeSpaceInWriteBuffer = DISK_WRITE_BUFFER_SIZE;
      }
      if (freeSpaceInWriteBuffer < DISK_WRITE_BUFFER_SIZE) {
        writer.write(writeBuffer, 0, (DISK_WRITE_BUFFER_SIZE - freeSpaceInWriteBuffer));
      }
    }

    public void close() throws IOException {
      writer.commitAndClose();
      writer = null;
      writeBuffer = null;
    }

    public AbstractScalaIterator<BytesToBytesMap.Location> getReader(BlockManager blockManager)
        throws IOException {
      return new UnsafeSorterKVSpillReader(blockManager, file, blockId);
    }
  }

  final class UnsafeSorterKVSpillReader implements AbstractScalaIterator<BytesToBytesMap.Location> {

    private InputStream in;
    private DataInputStream din;

    // Variables that change with every kv read:
    private int numRecordsRemaining;

    private int keyLength;
    private byte[] keyArray = new byte[1024 * 1024];

    private int valueLength;
    private byte[] valueArray = new byte[1024 * 1024];
    private final BytesToBytesMap.Location location = map.getNewLocation();

    public UnsafeSorterKVSpillReader(
        BlockManager blockManager,
        File file,
        BlockId blockId) throws IOException {
      assert (file.length() > 0);
      final BufferedInputStream bs = new BufferedInputStream(new FileInputStream(file));
      this.in = blockManager.wrapForCompression(blockId, bs);
      this.din = new DataInputStream(this.in);
      numRecordsRemaining = din.readInt();
    }

    @Override
    public boolean hasNext() {
      return (numRecordsRemaining > 0);
    }

    @Override
    public BytesToBytesMap.Location next() throws IOException {
      keyLength = din.readInt();
      if (keyLength > keyArray.length) {
        keyArray = new byte[keyLength];
      }
      ByteStreams.readFully(in, keyArray, 0, keyLength);
      valueLength = din.readInt();
      if (valueLength > valueArray.length) {
        valueArray = new byte[valueLength];
      }
      ByteStreams.readFully(in, valueArray, 0, valueLength);
      numRecordsRemaining--;
      if (numRecordsRemaining == 0) {
        in.close();
        in = null;
        din = null;
      }
      location.with(keyLength, keyArray, valueLength, valueArray);
      return location;
    }
  }


  public class BytesToBytesMapIterator implements AbstractScalaIterator<MapEntry> {

    private final MapEntry entry = new MapEntry();
    private final BytesToBytesMap.Location loc = map.getNewLocation();
    private int currentRecordNumber = 0;
    private Object pageBaseObject;
    private long offsetInPage;
    private int numRecords;

    public BytesToBytesMapIterator(int numRecords) {
      this.numRecords = numRecords;
      if (dataPages.iterator().hasNext()) {
        advanceToNextPage();
      }
    }

    private void advanceToNextPage() {
      final MemoryBlock currentPage = dataPages.iterator().next();
      pageBaseObject = currentPage.getBaseObject();
      offsetInPage = currentPage.getBaseOffset();
    }

    @Override
    public boolean hasNext() {
      return currentRecordNumber != numRecords;
    }

    @Override
    public MapEntry next() {
      int keyLength = (int) PlatformDependent.UNSAFE.getLong(pageBaseObject, offsetInPage);
      if (keyLength == END_OF_PAGE_MARKER) {
        advanceToNextPage();
        keyLength = (int) PlatformDependent.UNSAFE.getLong(pageBaseObject, offsetInPage);
      }
      loc.with(pageBaseObject, offsetInPage);
      offsetInPage += 8 + 8 + keyLength + loc.getValueLength();

      MemoryLocation keyAddress = loc.getKeyAddress();
      MemoryLocation valueAddress = loc.getValueAddress();
      entry.key.pointTo(
          keyAddress.getBaseObject(),
          keyAddress.getBaseOffset(),
          groupingKeySchema.length(),
          loc.getKeyLength()
      );
      entry.value.pointTo(
          valueAddress.getBaseObject(),
          valueAddress.getBaseOffset(),
          aggregationBufferSchema.length(),
          loc.getValueLength()
      );
      currentRecordNumber++;
      return entry;
    }
  }

  private long getMemoryUsage() {
    return map.getTotalMemoryConsumption() + (dataPages.size() * PAGE_SIZE_BYTES);
  }

  private void cleanupResources() {
    this.freeMemory();
  }

  /**
   * Free the unsafe memory associated with this map.
   */
  public long freeMemory() {
    long memoryFreed = 0;
    for (MemoryBlock block : dataPages) {
      memoryManager.freePage(block);
      shuffleMemoryManager.release(block.size());
      memoryFreed += block.size();
    }
    if (map != null) {
      long sorterMemoryUsage = map.getTotalMemoryConsumption();
      map.free();
      map = null;
      shuffleMemoryManager.release(sorterMemoryUsage);
      memoryFreed += sorterMemoryUsage;
    }
    dataPages.clear();
    currentDataPage = null;
    pageCursor = 0;
    freeSpaceInCurrentPage = 0;
    return memoryFreed;
  }

  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public void printPerfMetrics() {
    if (!enablePerfMetrics) {
      throw new IllegalStateException("Perf metrics not enabled");
    }
    System.out.println("Average probes per lookup: " + map.getAverageProbesPerLookup());
    System.out.println("Number of hash collisions: " + map.getNumHashCollisions());
    System.out.println("Time spent resizing (ns): " + map.getTimeSpentResizingNs());
    System.out.println("Total memory consumption (bytes): " + map.getTotalMemoryConsumption());
  }

}
