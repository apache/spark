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

package org.apache.spark.util.kvstore;

import java.io.File;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.iq80.leveldb.DBIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

public class LevelDBSuite {

  private LevelDB db;
  private File dbpath;

  @AfterEach
  public void cleanup() throws Exception {
    if (db != null) {
      db.close();
    }
    if (dbpath != null) {
      FileUtils.deleteQuietly(dbpath);
    }
  }

  @BeforeEach
  public void setup() throws Exception {
    assumeFalse(SystemUtils.IS_OS_MAC_OSX && SystemUtils.OS_ARCH.equals("aarch64"));
    dbpath = File.createTempFile("test.", ".ldb");
    dbpath.delete();
    db = new LevelDB(dbpath);
  }

  @Test
  public void testReopenAndVersionCheckDb() throws Exception {
    db.close();
    db = null;
    assertTrue(dbpath.exists());

    db = new LevelDB(dbpath);
    assertEquals(LevelDB.STORE_VERSION,
      db.serializer.deserializeLong(db.db().get(LevelDB.STORE_VERSION_KEY)));
    db.db().put(LevelDB.STORE_VERSION_KEY, db.serializer.serialize(LevelDB.STORE_VERSION + 1));
    db.close();
    db = null;

    assertThrows(UnsupportedStoreVersionException.class, () -> db = new LevelDB(dbpath));
  }

  @Test
  public void testObjectWriteReadDelete() throws Exception {
    CustomType1 t = createCustomType1(1);

    assertThrows(NoSuchElementException.class, () -> db.read(CustomType1.class, t.key));

    db.write(t);
    assertEquals(t, db.read(t.getClass(), t.key));
    assertEquals(1L, db.count(t.getClass()));

    db.delete(t.getClass(), t.key);
    assertThrows(NoSuchElementException.class, () -> db.read(t.getClass(), t.key));

    // Look into the actual DB and make sure that all the keys related to the type have been
    // removed.
    assertEquals(0, countKeys(t.getClass()));
  }

  @Test
  public void testMultipleObjectWriteReadDelete() throws Exception {
    CustomType1 t1 = createCustomType1(1);
    CustomType1 t2 = createCustomType1(2);
    t2.id = t1.id;

    db.write(t1);
    db.write(t2);

    assertEquals(t1, db.read(t1.getClass(), t1.key));
    assertEquals(t2, db.read(t2.getClass(), t2.key));
    assertEquals(2L, db.count(t1.getClass()));

    // There should be one "id" index entry with two values.
    assertEquals(2, db.count(t1.getClass(), "id", t1.id));

    // Delete the first entry; now there should be 3 remaining keys, since one of the "name"
    // index entries should have been removed.
    db.delete(t1.getClass(), t1.key);

    // Make sure there's a single entry in the "id" index now.
    assertEquals(1, db.count(t2.getClass(), "id", t2.id));

    // Delete the remaining entry, make sure all data is gone.
    db.delete(t2.getClass(), t2.key);
    assertEquals(0, countKeys(t2.getClass()));
  }

  @Test
  public void testMultipleTypesWriteReadDelete() throws Exception {
    CustomType1 t1 = createCustomType1(1);

    IntKeyType t2 = new IntKeyType();
    t2.key = 2;
    t2.id = "2";
    t2.values = Arrays.asList("value1", "value2");

    ArrayKeyIndexType t3 = new ArrayKeyIndexType();
    t3.key = new int[] { 42, 84 };
    t3.id = new String[] { "id1", "id2" };

    db.write(t1);
    db.write(t2);
    db.write(t3);

    assertEquals(t1, db.read(t1.getClass(), t1.key));
    assertEquals(t2, db.read(t2.getClass(), t2.key));
    assertEquals(t3, db.read(t3.getClass(), t3.key));

    // There should be one "id" index with a single entry for each type.
    assertEquals(1, db.count(t1.getClass(), "id", t1.id));
    assertEquals(1, db.count(t2.getClass(), "id", t2.id));
    assertEquals(1, db.count(t3.getClass(), "id", t3.id));

    // Delete the first entry; this should not affect the entries for the second type.
    db.delete(t1.getClass(), t1.key);
    assertEquals(0, countKeys(t1.getClass()));
    assertEquals(1, db.count(t2.getClass(), "id", t2.id));
    assertEquals(1, db.count(t3.getClass(), "id", t3.id));

    // Delete the remaining entries, make sure all data is gone.
    db.delete(t2.getClass(), t2.key);
    assertEquals(0, countKeys(t2.getClass()));

    db.delete(t3.getClass(), t3.key);
    assertEquals(0, countKeys(t3.getClass()));
  }

  @Test
  public void testMetadata() throws Exception {
    assertNull(db.getMetadata(CustomType1.class));

    CustomType1 t = createCustomType1(1);

    db.setMetadata(t);
    assertEquals(t, db.getMetadata(CustomType1.class));

    db.setMetadata(null);
    assertNull(db.getMetadata(CustomType1.class));
  }

  @Test
  public void testUpdate() throws Exception {
    CustomType1 t = createCustomType1(1);

    db.write(t);

    t.name = "anotherName";

    db.write(t);

    assertEquals(1, db.count(t.getClass()));
    assertEquals(1, db.count(t.getClass(), "name", "anotherName"));
    assertEquals(0, db.count(t.getClass(), "name", "name"));
  }

  @Test
  public void testRemoveAll() throws Exception {
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 2; j++) {
        ArrayKeyIndexType o = new ArrayKeyIndexType();
        o.key = new int[] { i, j, 0 };
        o.id = new String[] { "things" };
        db.write(o);

        o = new ArrayKeyIndexType();
        o.key = new int[] { i, j, 1 };
        o.id = new String[] { "more things" };
        db.write(o);
      }
    }

    ArrayKeyIndexType o = new ArrayKeyIndexType();
    o.key = new int[] { 2, 2, 2 };
    o.id = new String[] { "things" };
    db.write(o);

    assertEquals(9, db.count(ArrayKeyIndexType.class));

    db.removeAllByIndexValues(
      ArrayKeyIndexType.class,
      KVIndex.NATURAL_INDEX_NAME,
      ImmutableSet.of(new int[] {0, 0, 0}, new int[] { 2, 2, 2 }));
    assertEquals(7, db.count(ArrayKeyIndexType.class));

    db.removeAllByIndexValues(
      ArrayKeyIndexType.class,
      "id",
      ImmutableSet.of(new String[] { "things" }));
    assertEquals(4, db.count(ArrayKeyIndexType.class));

    db.removeAllByIndexValues(
      ArrayKeyIndexType.class,
      "id",
      ImmutableSet.of(new String[] { "more things" }));
    assertEquals(0, db.count(ArrayKeyIndexType.class));
  }

  @Test
  public void testSkip() throws Exception {
    for (int i = 0; i < 10; i++) {
      db.write(createCustomType1(i));
    }

    try (KVStoreIterator<CustomType1> it = db.view(CustomType1.class).closeableIterator()) {
      assertTrue(it.hasNext());
      assertTrue(it.skip(5));
      assertEquals("key5", it.next().key);
      assertTrue(it.skip(3));
      assertEquals("key9", it.next().key);
      assertFalse(it.hasNext());
    }
  }

  @Test
  public void testNegativeIndexValues() throws Exception {
    List<Integer> expected = Arrays.asList(-100, -50, 0, 50, 100);

    expected.forEach(i -> {
      try {
        db.write(createCustomType1(i));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    try (KVStoreIterator<CustomType1> iterator =
      db.view(CustomType1.class).index("int").closeableIterator()) {
      List<Integer> results = StreamSupport
        .stream(Spliterators.spliteratorUnknownSize(iterator, 0), false)
        .map(e -> e.num)
        .collect(Collectors.toList());

      assertEquals(expected, results);
    }
  }

  @Test
  public void testCloseLevelDBIterator() throws Exception {
    // SPARK-31929: test when LevelDB.close() is called, related LevelDBIterators
    // are closed. And files opened by iterators are also closed.
    File dbPathForCloseTest = File
      .createTempFile(
        "test_db_close.",
        ".ldb");
    dbPathForCloseTest.delete();
    LevelDB dbForCloseTest = new LevelDB(dbPathForCloseTest);
    for (int i = 0; i < 8192; i++) {
      dbForCloseTest.write(createCustomType1(i));
    }
    String key = dbForCloseTest
      .view(CustomType1.class).iterator().next().key;
    assertEquals("key0", key);
    Iterator<CustomType1> it0 = dbForCloseTest
      .view(CustomType1.class).max(1).iterator();
    while (it0.hasNext()) {
      it0.next();
    }
    System.gc();
    Iterator<CustomType1> it1 = dbForCloseTest
      .view(CustomType1.class).iterator();
    assertEquals("key0", it1.next().key);
    try (KVStoreIterator<CustomType1> it2 = dbForCloseTest
      .view(CustomType1.class).closeableIterator()) {
      assertEquals("key0", it2.next().key);
    }
    dbForCloseTest.close();
    assertTrue(dbPathForCloseTest.exists());
    FileUtils.deleteQuietly(dbPathForCloseTest);
    assertTrue(!dbPathForCloseTest.exists());
  }

  @Test
  public void testHasNextAfterIteratorClose() throws Exception {
    db.write(createCustomType1(0));
    KVStoreIterator<CustomType1> iter =
      db.view(CustomType1.class).closeableIterator();
    // iter should be true
    assertTrue(iter.hasNext());
    // close iter
    iter.close();
    // iter.hasNext should be false after iter close
    assertFalse(iter.hasNext());
  }

  @Test
  public void testHasNextAfterDBClose() throws Exception {
    db.write(createCustomType1(0));
    KVStoreIterator<CustomType1> iter =
      db.view(CustomType1.class).closeableIterator();
    // iter should be true
    assertTrue(iter.hasNext());
    // close db
    db.close();
    // iter.hasNext should be false after db close
    assertFalse(iter.hasNext());
  }

  @Test
  public void testNextAfterIteratorClose() throws Exception {
    db.write(createCustomType1(0));
    KVStoreIterator<CustomType1> iter =
      db.view(CustomType1.class).closeableIterator();
    // iter should be true
    assertTrue(iter.hasNext());
    // close iter
    iter.close();
    // iter.next should throw NoSuchElementException after iter close
    assertThrows(NoSuchElementException.class, iter::next);
  }

  @Test
  public void testNextAfterDBClose() throws Exception {
    db.write(createCustomType1(0));
    KVStoreIterator<CustomType1> iter =
      db.view(CustomType1.class).closeableIterator();
    // iter should be true
    assertTrue(iter.hasNext());
    // close db
    iter.close();
    // iter.next should throw NoSuchElementException after db close
    assertThrows(NoSuchElementException.class, iter::next);
  }

  @Test
  public void testSkipAfterIteratorClose() throws Exception {
    db.write(createCustomType1(0));
    KVStoreIterator<CustomType1> iter =
      db.view(CustomType1.class).closeableIterator();
    // close iter
    iter.close();
    // skip should always return false after iter close
    assertFalse(iter.skip(0));
    assertFalse(iter.skip(1));
  }

  @Test
  public void testSkipAfterDBClose() throws Exception {
    db.write(createCustomType1(0));
    KVStoreIterator<CustomType1> iter =
      db.view(CustomType1.class).closeableIterator();
    // iter should be true
    assertTrue(iter.hasNext());
    // close db
    db.close();
    // skip should always return false after db close
    assertFalse(iter.skip(0));
    assertFalse(iter.skip(1));
  }

  @Test
  public void testResourceCleaner() throws Exception {
    File dbPathForCleanerTest = File.createTempFile(
      "test_db_cleaner.", ".rdb");
    dbPathForCleanerTest.delete();

    LevelDB dbForCleanerTest = new LevelDB(dbPathForCleanerTest);
    try {
      for (int i = 0; i < 8192; i++) {
        dbForCleanerTest.write(createCustomType1(i));
      }
      LevelDBIterator<CustomType1> levelDBIterator =
        (LevelDBIterator<CustomType1>) dbForCleanerTest.view(CustomType1.class).iterator();
      Reference<LevelDBIterator<?>> reference = new WeakReference<>(levelDBIterator);
      assertNotNull(reference);
      LevelDBIterator.ResourceCleaner resourceCleaner = levelDBIterator.getResourceCleaner();
      assertFalse(resourceCleaner.isCompleted());
      // Manually set levelDBIterator to null, to be GC.
      levelDBIterator = null;
      // 100 times gc, the levelDBIterator should be GCed.
      int count = 0;
      while (count < 100 && !reference.refersTo(null)) {
        System.gc();
        count++;
        Thread.sleep(100);
      }
      // check rocksDBIterator should be GCed
      assertTrue(reference.refersTo(null));
      // Verify that the Cleaner will be executed after a period of time, isAllocated is true.
      assertTrue(resourceCleaner.isCompleted());
    } finally {
      dbForCleanerTest.close();
      FileUtils.deleteQuietly(dbPathForCleanerTest);
    }
  }

  @Test
  public void testMultipleTypesWriteAll() throws Exception {

    List<CustomType1> type1List = Arrays.asList(
      createCustomType1(1),
      createCustomType1(2),
      createCustomType1(3),
      createCustomType1(4)
    );

    List<CustomType2> type2List = Arrays.asList(
      createCustomType2(10),
      createCustomType2(11),
      createCustomType2(12),
      createCustomType2(13)
    );

    List fullList = new ArrayList();
    fullList.addAll(type1List);
    fullList.addAll(type2List);

    db.writeAll(fullList);
    for (CustomType1 value : type1List) {
      assertEquals(value, db.read(value.getClass(), value.key));
    }
    for (CustomType2 value : type2List) {
      assertEquals(value, db.read(value.getClass(), value.key));
    }
  }


  private CustomType1 createCustomType1(int i) {
    CustomType1 t = new CustomType1();
    t.key = "key" + i;
    t.id = "id" + i;
    t.name = "name" + i;
    t.num = i;
    t.child = "child" + i;
    return t;
  }

  private CustomType2 createCustomType2(int i) {
    CustomType2 t = new CustomType2();
    t.key = "key" + i;
    t.id = "id" + i;
    t.parentId = "parent_id" + (i / 2);
    return t;
  }

  private int countKeys(Class<?> type) throws Exception {
    byte[] prefix = db.getTypeInfo(type).keyPrefix();
    int count = 0;

    try (DBIterator it = db.db().iterator()) {
      it.seek(prefix);

      while (it.hasNext()) {
        byte[] key = it.next().getKey();
        if (LevelDBIterator.startsWith(key, prefix)) {
          count++;
        }
      }
    }

    return count;
  }
}
