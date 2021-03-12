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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.io.FileUtils;
import org.iq80.leveldb.DBIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class LevelDBSuite {

  private LevelDB db;
  private File dbpath;

  @After
  public void cleanup() throws Exception {
    if (db != null) {
      db.close();
    }
    if (dbpath != null) {
      FileUtils.deleteQuietly(dbpath);
    }
  }

  @Before
  public void setup() throws Exception {
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

    try {
      db = new LevelDB(dbpath);
      fail("Should have failed version check.");
    } catch (UnsupportedStoreVersionException e) {
      // Expected.
    }
  }

  @Test
  public void testObjectWriteReadDelete() throws Exception {
    CustomType1 t = createCustomType1(1);

    try {
      db.read(CustomType1.class, t.key);
      fail("Expected exception for non-existent object.");
    } catch (NoSuchElementException nsee) {
      // Expected.
    }

    db.write(t);
    assertEquals(t, db.read(t.getClass(), t.key));
    assertEquals(1L, db.count(t.getClass()));

    db.delete(t.getClass(), t.key);
    try {
      db.read(t.getClass(), t.key);
      fail("Expected exception for deleted object.");
    } catch (NoSuchElementException nsee) {
      // Expected.
    }

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

    KVStoreIterator<CustomType1> it = db.view(CustomType1.class).closeableIterator();
    assertTrue(it.hasNext());
    assertTrue(it.skip(5));
    assertEquals("key5", it.next().key);
    assertTrue(it.skip(3));
    assertEquals("key9", it.next().key);
    assertFalse(it.hasNext());
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

    List<Integer> results = StreamSupport
      .stream(db.view(CustomType1.class).index("int").spliterator(), false)
      .map(e -> e.num)
      .collect(Collectors.toList());

    assertEquals(expected, results);
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

  private CustomType1 createCustomType1(int i) {
    CustomType1 t = new CustomType1();
    t.key = "key" + i;
    t.id = "id" + i;
    t.name = "name" + i;
    t.num = i;
    t.child = "child" + i;
    return t;
  }

  private int countKeys(Class<?> type) throws Exception {
    byte[] prefix = db.getTypeInfo(type).keyPrefix();
    int count = 0;

    DBIterator it = db.db().iterator();
    it.seek(prefix);

    while (it.hasNext()) {
      byte[] key = it.next().getKey();
      if (LevelDBIterator.startsWith(key, prefix)) {
        count++;
      }
    }

    return count;
  }

  public static class IntKeyType {

    @KVIndex
    public int key;

    @KVIndex("id")
    public String id;

    public List<String> values;

    @Override
    public boolean equals(Object o) {
      if (o instanceof IntKeyType) {
        IntKeyType other = (IntKeyType) o;
        return key == other.key && id.equals(other.id) && values.equals(other.values);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return id.hashCode();
    }

  }

}
