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

package org.apache.spark.sql.catalyst.expressions;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.TestMemoryManager;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.internal.config.package$;

import java.util.Random;

public class RowBasedKeyValueBatchSuite {

  private final Random rand = new Random(42);

  private TestMemoryManager memoryManager;
  private TaskMemoryManager taskMemoryManager;
  private StructType keySchema = new StructType().add("k1", DataTypes.LongType)
          .add("k2", DataTypes.StringType);
  private StructType fixedKeySchema = new StructType().add("k1", DataTypes.LongType)
          .add("k2", DataTypes.LongType);
  private StructType valueSchema = new StructType().add("count", DataTypes.LongType)
          .add("sum", DataTypes.LongType);
  private int DEFAULT_CAPACITY = 1 << 16;

  private String getRandomString(int length) {
    Assertions.assertTrue(length >= 0);
    final byte[] bytes = new byte[length];
    rand.nextBytes(bytes);
    return new String(bytes);
  }

  private UnsafeRow makeKeyRow(long k1, String k2) {
    UnsafeRowWriter writer = new UnsafeRowWriter(2);
    writer.reset();
    writer.write(0, k1);
    writer.write(1, UTF8String.fromString(k2));
    return writer.getRow();
  }

  private UnsafeRow makeKeyRow(long k1, long k2) {
    UnsafeRowWriter writer = new UnsafeRowWriter(2);
    writer.reset();
    writer.write(0, k1);
    writer.write(1, k2);
    return writer.getRow();
  }

  private UnsafeRow makeValueRow(long v1, long v2) {
    UnsafeRowWriter writer = new UnsafeRowWriter(2);
    writer.reset();
    writer.write(0, v1);
    writer.write(1, v2);
    return writer.getRow();
  }

  private UnsafeRow appendRow(RowBasedKeyValueBatch batch, UnsafeRow key, UnsafeRow value) {
    return batch.appendRow(key.getBaseObject(), key.getBaseOffset(), key.getSizeInBytes(),
            value.getBaseObject(), value.getBaseOffset(), value.getSizeInBytes());
  }

  private void updateValueRow(UnsafeRow row, long v1, long v2) {
    row.setLong(0, v1);
    row.setLong(1, v2);
  }

  private boolean checkKey(UnsafeRow row, long k1, String k2) {
    return (row.getLong(0) == k1)
            && (row.getUTF8String(1).equals(UTF8String.fromString(k2)));
  }

  private boolean checkKey(UnsafeRow row, long k1, long k2) {
    return (row.getLong(0) == k1)
            && (row.getLong(1) == k2);
  }

  private boolean checkValue(UnsafeRow row, long v1, long v2) {
    return (row.getLong(0) == v1) && (row.getLong(1) == v2);
  }

  @BeforeEach
  public void setup() {
    memoryManager = new TestMemoryManager(new SparkConf()
            .set(package$.MODULE$.MEMORY_OFFHEAP_ENABLED(), false)
            .set(package$.MODULE$.SHUFFLE_SPILL_COMPRESS(), false)
            .set(package$.MODULE$.SHUFFLE_COMPRESS(), false));
    taskMemoryManager = new TaskMemoryManager(memoryManager, 0);
  }

  @AfterEach
  public void tearDown() {
    if (taskMemoryManager != null) {
      Assertions.assertEquals(0L, taskMemoryManager.cleanUpAllAllocatedMemory());
      long leakedMemory = taskMemoryManager.getMemoryConsumptionForThisTask();
      taskMemoryManager = null;
      Assertions.assertEquals(0L, leakedMemory);
    }
  }


  @Test
  public void emptyBatch() throws Exception {
    try (RowBasedKeyValueBatch batch = RowBasedKeyValueBatch.allocate(keySchema,
        valueSchema, taskMemoryManager, DEFAULT_CAPACITY)) {
      Assertions.assertEquals(0, batch.numRows());
      Assertions.assertThrows(AssertionError.class, () -> batch.getKeyRow(-1));
      Assertions.assertThrows(AssertionError.class, () -> batch.getValueRow(-1));
      Assertions.assertThrows(AssertionError.class, () -> batch.getKeyRow(0));
      Assertions.assertThrows(AssertionError.class, () -> batch.getValueRow(0));
      Assertions.assertFalse(batch.rowIterator().next());
    }
  }

  @Test
  public void batchType() {
    try (RowBasedKeyValueBatch batch1 = RowBasedKeyValueBatch.allocate(keySchema,
        valueSchema, taskMemoryManager, DEFAULT_CAPACITY);
         RowBasedKeyValueBatch batch2 = RowBasedKeyValueBatch.allocate(fixedKeySchema,
        valueSchema, taskMemoryManager, DEFAULT_CAPACITY)) {
      Assertions.assertEquals(VariableLengthRowBasedKeyValueBatch.class, batch1.getClass());
      Assertions.assertEquals(FixedLengthRowBasedKeyValueBatch.class, batch2.getClass());
    }
  }

  @Test
  public void setAndRetrieve() {
    try (RowBasedKeyValueBatch batch = RowBasedKeyValueBatch.allocate(keySchema,
        valueSchema, taskMemoryManager, DEFAULT_CAPACITY)) {
      UnsafeRow ret1 = appendRow(batch, makeKeyRow(1, "A"), makeValueRow(1, 1));
      Assertions.assertTrue(checkValue(ret1, 1, 1));
      UnsafeRow ret2 = appendRow(batch, makeKeyRow(2, "B"), makeValueRow(2, 2));
      Assertions.assertTrue(checkValue(ret2, 2, 2));
      UnsafeRow ret3 = appendRow(batch, makeKeyRow(3, "C"), makeValueRow(3, 3));
      Assertions.assertTrue(checkValue(ret3, 3, 3));
      Assertions.assertEquals(3, batch.numRows());
      UnsafeRow retrievedKey1 = batch.getKeyRow(0);
      Assertions.assertTrue(checkKey(retrievedKey1, 1, "A"));
      UnsafeRow retrievedKey2 = batch.getKeyRow(1);
      Assertions.assertTrue(checkKey(retrievedKey2, 2, "B"));
      UnsafeRow retrievedValue1 = batch.getValueRow(1);
      Assertions.assertTrue(checkValue(retrievedValue1, 2, 2));
      UnsafeRow retrievedValue2 = batch.getValueRow(2);
      Assertions.assertTrue(checkValue(retrievedValue2, 3, 3));

      Assertions.assertThrows(AssertionError.class, () -> batch.getKeyRow(3));
      Assertions.assertThrows(AssertionError.class, () -> batch.getValueRow(3));
    }
  }

  @Test
  public void setUpdateAndRetrieve() {
    try (RowBasedKeyValueBatch batch = RowBasedKeyValueBatch.allocate(keySchema,
        valueSchema, taskMemoryManager, DEFAULT_CAPACITY)) {
      appendRow(batch, makeKeyRow(1, "A"), makeValueRow(1, 1));
      Assertions.assertEquals(1, batch.numRows());
      UnsafeRow retrievedValue = batch.getValueRow(0);
      updateValueRow(retrievedValue, 2, 2);
      UnsafeRow retrievedValue2 = batch.getValueRow(0);
      Assertions.assertTrue(checkValue(retrievedValue2, 2, 2));
    }
  }


  @Test
  public void iteratorTest() throws Exception {
    try (RowBasedKeyValueBatch batch = RowBasedKeyValueBatch.allocate(keySchema,
        valueSchema, taskMemoryManager, DEFAULT_CAPACITY)) {
      appendRow(batch, makeKeyRow(1, "A"), makeValueRow(1, 1));
      appendRow(batch, makeKeyRow(2, "B"), makeValueRow(2, 2));
      appendRow(batch, makeKeyRow(3, "C"), makeValueRow(3, 3));
      Assertions.assertEquals(3, batch.numRows());
      org.apache.spark.unsafe.KVIterator<UnsafeRow, UnsafeRow> iterator
              = batch.rowIterator();
      Assertions.assertTrue(iterator.next());
      UnsafeRow key1 = iterator.getKey();
      UnsafeRow value1 = iterator.getValue();
      Assertions.assertTrue(checkKey(key1, 1, "A"));
      Assertions.assertTrue(checkValue(value1, 1, 1));
      Assertions.assertTrue(iterator.next());
      UnsafeRow key2 = iterator.getKey();
      UnsafeRow value2 = iterator.getValue();
      Assertions.assertTrue(checkKey(key2, 2, "B"));
      Assertions.assertTrue(checkValue(value2, 2, 2));
      Assertions.assertTrue(iterator.next());
      UnsafeRow key3 = iterator.getKey();
      UnsafeRow value3 = iterator.getValue();
      Assertions.assertTrue(checkKey(key3, 3, "C"));
      Assertions.assertTrue(checkValue(value3, 3, 3));
      Assertions.assertFalse(iterator.next());
    }
  }

  @Test
  public void fixedLengthTest() throws Exception {
    try (RowBasedKeyValueBatch batch = RowBasedKeyValueBatch.allocate(fixedKeySchema,
        valueSchema, taskMemoryManager, DEFAULT_CAPACITY)) {
      appendRow(batch, makeKeyRow(11, 11), makeValueRow(1, 1));
      appendRow(batch, makeKeyRow(22, 22), makeValueRow(2, 2));
      appendRow(batch, makeKeyRow(33, 33), makeValueRow(3, 3));
      UnsafeRow retrievedKey1 = batch.getKeyRow(0);
      Assertions.assertTrue(checkKey(retrievedKey1, 11, 11));
      UnsafeRow retrievedKey2 = batch.getKeyRow(1);
      Assertions.assertTrue(checkKey(retrievedKey2, 22, 22));
      UnsafeRow retrievedValue1 = batch.getValueRow(1);
      Assertions.assertTrue(checkValue(retrievedValue1, 2, 2));
      UnsafeRow retrievedValue2 = batch.getValueRow(2);
      Assertions.assertTrue(checkValue(retrievedValue2, 3, 3));
      Assertions.assertEquals(3, batch.numRows());
      org.apache.spark.unsafe.KVIterator<UnsafeRow, UnsafeRow> iterator
              = batch.rowIterator();
      Assertions.assertTrue(iterator.next());
      UnsafeRow key1 = iterator.getKey();
      UnsafeRow value1 = iterator.getValue();
      Assertions.assertTrue(checkKey(key1, 11, 11));
      Assertions.assertTrue(checkValue(value1, 1, 1));
      Assertions.assertTrue(iterator.next());
      UnsafeRow key2 = iterator.getKey();
      UnsafeRow value2 = iterator.getValue();
      Assertions.assertTrue(checkKey(key2, 22, 22));
      Assertions.assertTrue(checkValue(value2, 2, 2));
      Assertions.assertTrue(iterator.next());
      UnsafeRow key3 = iterator.getKey();
      UnsafeRow value3 = iterator.getValue();
      Assertions.assertTrue(checkKey(key3, 33, 33));
      Assertions.assertTrue(checkValue(value3, 3, 3));
      Assertions.assertFalse(iterator.next());
    }
  }

  @Test
  public void appendRowUntilExceedingCapacity() throws Exception {
    try (RowBasedKeyValueBatch batch = RowBasedKeyValueBatch.allocate(keySchema,
        valueSchema, taskMemoryManager, 10)) {
      UnsafeRow key = makeKeyRow(1, "A");
      UnsafeRow value = makeValueRow(1, 1);
      for (int i = 0; i < 10; i++) {
        appendRow(batch, key, value);
      }
      UnsafeRow ret = appendRow(batch, key, value);
      Assertions.assertEquals(10, batch.numRows());
      Assertions.assertNull(ret);
      org.apache.spark.unsafe.KVIterator<UnsafeRow, UnsafeRow> iterator
              = batch.rowIterator();
      for (int i = 0; i < 10; i++) {
        Assertions.assertTrue(iterator.next());
        UnsafeRow key1 = iterator.getKey();
        UnsafeRow value1 = iterator.getValue();
        Assertions.assertTrue(checkKey(key1, 1, "A"));
        Assertions.assertTrue(checkValue(value1, 1, 1));
      }
      Assertions.assertFalse(iterator.next());
    }
  }

  @Test
  public void appendRowUntilExceedingPageSize() throws Exception {
    // Use default size or spark.buffer.pageSize if specified
    int pageSizeToUse = (int) memoryManager.pageSizeBytes();
    try (RowBasedKeyValueBatch batch = RowBasedKeyValueBatch.allocate(keySchema,
        valueSchema, taskMemoryManager, pageSizeToUse)) {
      UnsafeRow key = makeKeyRow(1, "A");
      UnsafeRow value = makeValueRow(1, 1);
      int recordLength = 8 + key.getSizeInBytes() + value.getSizeInBytes() + 8;
      int totalSize = 4;
      int numRows = 0;
      while (totalSize + recordLength < pageSizeToUse) {
        appendRow(batch, key, value);
        totalSize += recordLength;
        numRows++;
      }
      UnsafeRow ret = appendRow(batch, key, value);
      Assertions.assertEquals(numRows, batch.numRows());
      Assertions.assertNull(ret);
      org.apache.spark.unsafe.KVIterator<UnsafeRow, UnsafeRow> iterator
              = batch.rowIterator();
      for (int i = 0; i < numRows; i++) {
        Assertions.assertTrue(iterator.next());
        UnsafeRow key1 = iterator.getKey();
        UnsafeRow value1 = iterator.getValue();
        Assertions.assertTrue(checkKey(key1, 1, "A"));
        Assertions.assertTrue(checkValue(value1, 1, 1));
      }
      Assertions.assertFalse(iterator.next());
    }
  }

  @Test
  public void failureToAllocateFirstPage() throws Exception {
    memoryManager.limit(1024);
    try (RowBasedKeyValueBatch batch = RowBasedKeyValueBatch.allocate(keySchema,
        valueSchema, taskMemoryManager, DEFAULT_CAPACITY)) {
      UnsafeRow key = makeKeyRow(1, "A");
      UnsafeRow value = makeValueRow(11, 11);
      UnsafeRow ret = appendRow(batch, key, value);
      Assertions.assertNull(ret);
      Assertions.assertFalse(batch.rowIterator().next());
    }
  }

  @Test
  public void randomizedTest() {
    try (RowBasedKeyValueBatch batch = RowBasedKeyValueBatch.allocate(keySchema,
        valueSchema, taskMemoryManager, DEFAULT_CAPACITY)) {
      int numEntry = 100;
      long[] expectedK1 = new long[numEntry];
      String[] expectedK2 = new String[numEntry];
      long[] expectedV1 = new long[numEntry];
      long[] expectedV2 = new long[numEntry];

      for (int i = 0; i < numEntry; i++) {
        long k1 = rand.nextLong();
        String k2 = getRandomString(rand.nextInt(256));
        long v1 = rand.nextLong();
        long v2 = rand.nextLong();
        appendRow(batch, makeKeyRow(k1, k2), makeValueRow(v1, v2));
        expectedK1[i] = k1;
        expectedK2[i] = k2;
        expectedV1[i] = v1;
        expectedV2[i] = v2;
      }

      for (int j = 0; j < 10000; j++) {
        int rowId = rand.nextInt(numEntry);
        if (rand.nextBoolean()) {
          UnsafeRow key = batch.getKeyRow(rowId);
          Assertions.assertTrue(checkKey(key, expectedK1[rowId], expectedK2[rowId]));
        }
        if (rand.nextBoolean()) {
          UnsafeRow value = batch.getValueRow(rowId);
          Assertions.assertTrue(checkValue(value, expectedV1[rowId], expectedV2[rowId]));
        }
      }
    }
  }
}
