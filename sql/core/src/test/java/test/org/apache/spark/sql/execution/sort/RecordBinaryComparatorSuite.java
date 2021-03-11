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

package test.org.apache.spark.sql.execution.sort;

import org.apache.spark.SparkConf;
import org.apache.spark.internal.config.package$;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.TestMemoryConsumer;
import org.apache.spark.memory.TestMemoryManager;
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.RecordBinaryComparator;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.util.collection.unsafe.sort.*;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteOrder;

/**
 * Test the RecordBinaryComparator, which compares two UnsafeRows by their binary form.
 */
public class RecordBinaryComparatorSuite {

  private final TaskMemoryManager memoryManager = new TaskMemoryManager(
      new TestMemoryManager(
        new SparkConf().set(package$.MODULE$.MEMORY_OFFHEAP_ENABLED(), false)), 0);
  private final TestMemoryConsumer consumer = new TestMemoryConsumer(memoryManager);

  private final int uaoSize = UnsafeAlignedOffset.getUaoSize();

  private MemoryBlock dataPage;
  private long pageCursor;

  private LongArray array;
  private int pos;

  @Before
  public void beforeEach() {
    // Only compare between two input rows.
    array = consumer.allocateArray(2);
    pos = 0;

    dataPage = memoryManager.allocatePage(4096, consumer);
    pageCursor = dataPage.getBaseOffset();
  }

  @After
  public void afterEach() {
    consumer.freePage(dataPage);
    dataPage = null;
    pageCursor = 0;

    consumer.freeArray(array);
    array = null;
    pos = 0;
  }

  private void insertRow(UnsafeRow row) {
    Object recordBase = row.getBaseObject();
    long recordOffset = row.getBaseOffset();
    int recordLength = row.getSizeInBytes();

    Object baseObject = dataPage.getBaseObject();
    Assert.assertTrue(pageCursor + recordLength <= dataPage.getBaseOffset() + dataPage.size());
    long recordAddress = memoryManager.encodePageNumberAndOffset(dataPage, pageCursor);
    UnsafeAlignedOffset.putSize(baseObject, pageCursor, recordLength);
    pageCursor += uaoSize;
    Platform.copyMemory(recordBase, recordOffset, baseObject, pageCursor, recordLength);
    pageCursor += recordLength;

    Assert.assertTrue(pos < 2);
    array.set(pos, recordAddress);
    pos++;
  }

  private int compare(int index1, int index2) {
    Object baseObject = dataPage.getBaseObject();

    long recordAddress1 = array.get(index1);
    long baseOffset1 = memoryManager.getOffsetInPage(recordAddress1) + uaoSize;
    int recordLength1 = UnsafeAlignedOffset.getSize(baseObject, baseOffset1 - uaoSize);

    long recordAddress2 = array.get(index2);
    long baseOffset2 = memoryManager.getOffsetInPage(recordAddress2) + uaoSize;
    int recordLength2 = UnsafeAlignedOffset.getSize(baseObject, baseOffset2 - uaoSize);

    return binaryComparator.compare(baseObject, baseOffset1, recordLength1, baseObject,
        baseOffset2, recordLength2);
  }

  private final RecordComparator binaryComparator = new RecordBinaryComparator();

  // Compute the most compact size for UnsafeRow's backing data.
  private int computeSizeInBytes(int originalSize) {
    // All the UnsafeRows in this suite contains less than 64 columns, so the bitSetSize shall
    // always be 8.
    return 8 + (originalSize + 7) / 8 * 8;
  }

  // Compute the relative offset of variable-length values.
  private long relativeOffset(int numFields) {
    // All the UnsafeRows in this suite contains less than 64 columns, so the bitSetSize shall
    // always be 8.
    return 8 + numFields * 8L;
  }

  @Test
  public void testBinaryComparatorForSingleColumnRow() throws Exception {
    int numFields = 1;

    UnsafeRow row1 = new UnsafeRow(numFields);
    byte[] data1 = new byte[100];
    row1.pointTo(data1, computeSizeInBytes(numFields * 8));
    row1.setInt(0, 11);

    UnsafeRow row2 = new UnsafeRow(numFields);
    byte[] data2 = new byte[100];
    row2.pointTo(data2, computeSizeInBytes(numFields * 8));
    row2.setInt(0, 42);

    insertRow(row1);
    insertRow(row2);

    Assert.assertEquals(0, compare(0, 0));
    Assert.assertTrue(compare(0, 1) < 0);
  }

  @Test
  public void testBinaryComparatorForMultipleColumnRow() throws Exception {
    int numFields = 5;

    UnsafeRow row1 = new UnsafeRow(numFields);
    byte[] data1 = new byte[100];
    row1.pointTo(data1, computeSizeInBytes(numFields * 8));
    for (int i = 0; i < numFields; i++) {
      row1.setDouble(i, i * 3.14);
    }

    UnsafeRow row2 = new UnsafeRow(numFields);
    byte[] data2 = new byte[100];
    row2.pointTo(data2, computeSizeInBytes(numFields * 8));
    for (int i = 0; i < numFields; i++) {
      row2.setDouble(i, 198.7 / (i + 1));
    }

    insertRow(row1);
    insertRow(row2);

    Assert.assertEquals(0, compare(0, 0));
    Assert.assertTrue(compare(0, 1) < 0);
  }

  @Test
  public void testBinaryComparatorForArrayColumn() throws Exception {
    int numFields = 1;

    UnsafeRow row1 = new UnsafeRow(numFields);
    byte[] data1 = new byte[100];
    UnsafeArrayData arrayData1 = UnsafeArrayData.fromPrimitiveArray(new int[]{11, 42, -1});
    row1.pointTo(data1, computeSizeInBytes(numFields * 8 + arrayData1.getSizeInBytes()));
    row1.setLong(0, (relativeOffset(numFields) << 32) | (long) arrayData1.getSizeInBytes());
    Platform.copyMemory(arrayData1.getBaseObject(), arrayData1.getBaseOffset(), data1,
        row1.getBaseOffset() + relativeOffset(numFields), arrayData1.getSizeInBytes());

    UnsafeRow row2 = new UnsafeRow(numFields);
    byte[] data2 = new byte[100];
    UnsafeArrayData arrayData2 = UnsafeArrayData.fromPrimitiveArray(new int[]{22});
    row2.pointTo(data2, computeSizeInBytes(numFields * 8 + arrayData2.getSizeInBytes()));
    row2.setLong(0, (relativeOffset(numFields) << 32) | (long) arrayData2.getSizeInBytes());
    Platform.copyMemory(arrayData2.getBaseObject(), arrayData2.getBaseOffset(), data2,
        row2.getBaseOffset() + relativeOffset(numFields), arrayData2.getSizeInBytes());

    insertRow(row1);
    insertRow(row2);

    Assert.assertEquals(0, compare(0, 0));
    Assert.assertTrue(compare(0, 1) > 0);
  }

  @Test
  public void testBinaryComparatorForMixedColumns() throws Exception {
    int numFields = 4;

    UnsafeRow row1 = new UnsafeRow(numFields);
    byte[] data1 = new byte[100];
    UTF8String str1 = UTF8String.fromString("Milk tea");
    row1.pointTo(data1, computeSizeInBytes(numFields * 8 + str1.numBytes()));
    row1.setInt(0, 11);
    row1.setDouble(1, 3.14);
    row1.setInt(2, -1);
    row1.setLong(3, (relativeOffset(numFields) << 32) | (long) str1.numBytes());
    Platform.copyMemory(str1.getBaseObject(), str1.getBaseOffset(), data1,
        row1.getBaseOffset() + relativeOffset(numFields), str1.numBytes());

    UnsafeRow row2 = new UnsafeRow(numFields);
    byte[] data2 = new byte[100];
    UTF8String str2 = UTF8String.fromString("Java");
    row2.pointTo(data2, computeSizeInBytes(numFields * 8 + str2.numBytes()));
    row2.setInt(0, 11);
    row2.setDouble(1, 3.14);
    row2.setInt(2, -1);
    row2.setLong(3, (relativeOffset(numFields) << 32) | (long) str2.numBytes());
    Platform.copyMemory(str2.getBaseObject(), str2.getBaseOffset(), data2,
        row2.getBaseOffset() + relativeOffset(numFields), str2.numBytes());

    insertRow(row1);
    insertRow(row2);

    Assert.assertEquals(0, compare(0, 0));
    Assert.assertTrue(compare(0, 1) > 0);
  }

  @Test
  public void testBinaryComparatorForNullColumns() throws Exception {
    int numFields = 3;

    UnsafeRow row1 = new UnsafeRow(numFields);
    byte[] data1 = new byte[100];
    row1.pointTo(data1, computeSizeInBytes(numFields * 8));
    for (int i = 0; i < numFields; i++) {
      row1.setNullAt(i);
    }

    UnsafeRow row2 = new UnsafeRow(numFields);
    byte[] data2 = new byte[100];
    row2.pointTo(data2, computeSizeInBytes(numFields * 8));
    for (int i = 0; i < numFields - 1; i++) {
      row2.setNullAt(i);
    }
    row2.setDouble(numFields - 1, 3.14);

    insertRow(row1);
    insertRow(row2);

    Assert.assertEquals(0, compare(0, 0));
    Assert.assertTrue(compare(0, 1) > 0);
  }

  @Test
  public void testBinaryComparatorWhenSubtractionIsDivisibleByMaxIntValue() throws Exception {
    int numFields = 1;

    // Place the following bytes (hex) into UnsafeRows for the comparison:
    //
    //   index | 00 01 02 03 04 05 06 07
    //   ------+------------------------
    //   row1  | 00 00 00 00 00 00 00 0b
    //   row2  | 00 00 00 00 80 00 00 0a
    //
    // The byte layout needs to be identical on all platforms regardless of
    // of endianness. To achieve this the bytes in each value are reversed
    // on little-endian platforms.
    long row1Data = 11L;
    long row2Data = 11L + Integer.MAX_VALUE;
    if (ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN)) {
      row1Data = Long.reverseBytes(row1Data);
      row2Data = Long.reverseBytes(row2Data);
    }

    UnsafeRow row1 = new UnsafeRow(numFields);
    byte[] data1 = new byte[100];
    row1.pointTo(data1, computeSizeInBytes(numFields * 8));
    row1.setLong(0, row1Data);

    UnsafeRow row2 = new UnsafeRow(numFields);
    byte[] data2 = new byte[100];
    row2.pointTo(data2, computeSizeInBytes(numFields * 8));
    row2.setLong(0, row2Data);

    insertRow(row1);
    insertRow(row2);

    Assert.assertTrue(compare(0, 1) < 0);
  }

  @Test
  public void testBinaryComparatorWhenSubtractionCanOverflowLongValue() throws Exception {
    int numFields = 1;

    // Place the following bytes (hex) into UnsafeRows for the comparison:
    //
    //   index | 00 01 02 03 04 05 06 07
    //   ------+------------------------
    //   row1  | 80 00 00 00 00 00 00 00
    //   row2  | 00 00 00 00 00 00 00 01
    //
    // The byte layout needs to be identical on all platforms regardless of
    // of endianness. To achieve this the bytes in each value are reversed
    // on little-endian platforms.
    long row1Data = Long.MIN_VALUE;
    long row2Data = 1L;
    if (ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN)) {
      row1Data = Long.reverseBytes(row1Data);
      row2Data = Long.reverseBytes(row2Data);
    }

    UnsafeRow row1 = new UnsafeRow(numFields);
    byte[] data1 = new byte[100];
    row1.pointTo(data1, computeSizeInBytes(numFields * 8));
    row1.setLong(0, row1Data);

    UnsafeRow row2 = new UnsafeRow(numFields);
    byte[] data2 = new byte[100];
    row2.pointTo(data2, computeSizeInBytes(numFields * 8));
    row2.setLong(0, row2Data);

    insertRow(row1);
    insertRow(row2);

    Assert.assertTrue(compare(0, 1) > 0);
  }

  @Test
  public void testBinaryComparatorWhenOnlyTheLastColumnDiffers() throws Exception {
    int numFields = 4;

    UnsafeRow row1 = new UnsafeRow(numFields);
    byte[] data1 = new byte[100];
    row1.pointTo(data1, computeSizeInBytes(numFields * 8));
    row1.setInt(0, 11);
    row1.setDouble(1, 3.14);
    row1.setInt(2, -1);
    row1.setLong(3, 0);

    UnsafeRow row2 = new UnsafeRow(numFields);
    byte[] data2 = new byte[100];
    row2.pointTo(data2, computeSizeInBytes(numFields * 8));
    row2.setInt(0, 11);
    row2.setDouble(1, 3.14);
    row2.setInt(2, -1);
    row2.setLong(3, 1);

    insertRow(row1);
    insertRow(row2);

    Assert.assertTrue(compare(0, 1) < 0);
  }

  @Test
  public void testCompareLongsAsLittleEndian() {
    long arrayOffset = Platform.LONG_ARRAY_OFFSET + 4;

    long[] arr1 = new long[2];
    Platform.putLong(arr1, arrayOffset, 0x0100000000000000L);
    long[] arr2 = new long[2];
    Platform.putLong(arr2, arrayOffset + 4, 0x0000000000000001L);
    // leftBaseOffset is not aligned while rightBaseOffset is aligned,
    // it will start by comparing long
    int result1 = binaryComparator.compare(arr1, arrayOffset, 8, arr2, arrayOffset + 4, 8);

    long[] arr3 = new long[2];
    Platform.putLong(arr3, arrayOffset, 0x0100000000000000L);
    long[] arr4 = new long[2];
    Platform.putLong(arr4, arrayOffset, 0x0000000000000001L);
    // both left and right offset is not aligned, it will start with byte-by-byte comparison
    int result2 = binaryComparator.compare(arr3, arrayOffset, 8, arr4, arrayOffset, 8);

    Assert.assertEquals(result1, result2);
  }

  @Test
  public void testCompareLongsAsUnsigned() {
    long arrayOffset = Platform.LONG_ARRAY_OFFSET + 4;

    long[] arr1 = new long[2];
    Platform.putLong(arr1, arrayOffset + 4, 0xa000000000000000L);
    long[] arr2 = new long[2];
    Platform.putLong(arr2, arrayOffset + 4, 0x0000000000000000L);
    // both leftBaseOffset and rightBaseOffset are aligned, so it will start by comparing long
    int result1 = binaryComparator.compare(arr1, arrayOffset + 4, 8, arr2, arrayOffset + 4, 8);

    long[] arr3 = new long[2];
    Platform.putLong(arr3, arrayOffset, 0xa000000000000000L);
    long[] arr4 = new long[2];
    Platform.putLong(arr4, arrayOffset, 0x0000000000000000L);
    // both leftBaseOffset and rightBaseOffset are not aligned,
    // so it will start with byte-by-byte comparison
    int result2 = binaryComparator.compare(arr3, arrayOffset, 8, arr4, arrayOffset, 8);

    Assert.assertEquals(result1, result2);
  }
}
