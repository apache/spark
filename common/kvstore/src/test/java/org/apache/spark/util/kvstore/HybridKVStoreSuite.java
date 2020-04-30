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

import org.apache.commons.io.FileUtils;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.*;

public class HybridKVStoreSuite {
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
  public void testObjectWriteReadDelete() throws Exception {
    HybridKVStore store = new HybridKVStore();
    store.setLevelDB(db);
    MyListener myListener = new MyListener();
    store.startBackgroundThreadToWriteToDB(myListener);

    CustomType1 t = createCustomType1(1);

    // Test when not started background thread yet
    try {
      store.read(CustomType1.class, t.key);
      fail("Expected exception for non-existent object.");
    } catch (NoSuchElementException nsee) {
      // Expected.
    }

    store.write(t);
    assertEquals(t, store.read(t.getClass(), t.key));
    assertEquals(1L, store.count(t.getClass()));

    // Switch from in memory to leveldb
    store.stopBackgroundThreadAndSwitchToLevelDB();
    assert(myListener.waitUntilDone());
    store.getWritingToLevelDBThread().join();
    assert(!store.getShouldUseMemoryStore());

    // Try to read from db
    assertEquals(t, store.read(t.getClass(), t.key));
    assertEquals(1L, store.count(t.getClass()));

    store.delete(t.getClass(), t.key);
    try {
      store.read(t.getClass(), t.key);
      fail("Expected exception for deleted object.");
    } catch (NoSuchElementException nsee) {
      // Expected.
    }
  }

  @Test
  public void testMultipleObjectWriteReadDelete() throws Exception {
    HybridKVStore store = new HybridKVStore();
    store.setLevelDB(db);
    MyListener myListener = new MyListener();
    store.startBackgroundThreadToWriteToDB(myListener);

    CustomType1 t1 = createCustomType1(1);
    CustomType1 t2 = createCustomType1(2);

    store.write(t1);
    store.write(t2);

    assertEquals(t1, store.read(t1.getClass(), t1.key));
    assertEquals(t2, store.read(t2.getClass(), t2.key));
    assertEquals(2L, store.count(t1.getClass()));

    store.stopBackgroundThreadAndSwitchToLevelDB();
    assert(myListener.waitUntilDone());
    store.getWritingToLevelDBThread().join();
    assert(!store.getShouldUseMemoryStore());

    assertEquals(t1, db.read(t1.getClass(), t1.key));
    assertEquals(t2, db.read(t2.getClass(), t2.key));

    store.delete(t1.getClass(), t1.key);
    assertEquals(t2, store.read(t2.getClass(), t2.key));
    store.delete(t2.getClass(), t2.key);
    try {
      store.read(t2.getClass(), t2.key);
      fail("Expected exception for deleted object.");
    } catch (NoSuchElementException nsee) {
      // Expected.
    }
  }

  @Test
  public void testMetadata() throws Exception {
    HybridKVStore store = new HybridKVStore();
    store.setLevelDB(db);
    MyListener myListener = new MyListener();
    store.startBackgroundThreadToWriteToDB(myListener);

    assertNull(store.getMetadata(CustomType1.class));

    CustomType1 t = createCustomType1(1);

    store.setMetadata(t);
    assertEquals(t, store.getMetadata(CustomType1.class));

    store.stopBackgroundThreadAndSwitchToLevelDB();
    assert(myListener.waitUntilDone());
    store.getWritingToLevelDBThread().join();
    assert(!store.getShouldUseMemoryStore());

    assertEquals(t, store.getMetadata(CustomType1.class));

    store.setMetadata(null);
    assertNull(store.getMetadata(CustomType1.class));
  }

  @Test
  public void testUpdate() throws Exception {
    HybridKVStore store = new HybridKVStore();
    store.setLevelDB(db);
    MyListener myListener = new MyListener();
    store.startBackgroundThreadToWriteToDB(myListener);

    CustomType1 t = createCustomType1(1);

    store.write(t);

    t.name = "anotherName";

    store.write(t);

    store.stopBackgroundThreadAndSwitchToLevelDB();
    assert(myListener.waitUntilDone());
    store.getWritingToLevelDBThread().join();
    assert(!store.getShouldUseMemoryStore());

    assertEquals(1, store.count(t.getClass()));
    assertEquals(1, store.count(t.getClass(), "name", "anotherName"));
    assertEquals(0, store.count(t.getClass(), "name", "name"));
  }

  @Test
  public void testArrayIndices() throws Exception {
    HybridKVStore store = new HybridKVStore();
    store.setLevelDB(db);
    MyListener myListener = new MyListener();
    store.startBackgroundThreadToWriteToDB(myListener);

    ArrayKeyIndexType o = new ArrayKeyIndexType();
    o.key = new int[] { 1, 2 };
    o.id = new String[] { "3", "4" };

    store.write(o);
    assertEquals(o, store.read(ArrayKeyIndexType.class, o.key));
    assertEquals(o, store.view(ArrayKeyIndexType.class).index("id").first(o.id).iterator().next());

    store.stopBackgroundThreadAndSwitchToLevelDB();
    assert(myListener.waitUntilDone());
    store.getWritingToLevelDBThread().join();
    assert(!store.getShouldUseMemoryStore());

    assertEquals(o, store.read(ArrayKeyIndexType.class, o.key));
    assertEquals(o, store.view(ArrayKeyIndexType.class).index("id").first(o.id).iterator().next());
  }

  @Test
  public void testRemoveAll() throws Exception {
    HybridKVStore store = new HybridKVStore();
    store.setLevelDB(db);
    MyListener myListener = new MyListener();
    store.startBackgroundThreadToWriteToDB(myListener);

    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 2; j++) {
        ArrayKeyIndexType o = new ArrayKeyIndexType();
        o.key = new int[] { i, j, 0 };
        o.id = new String[] { "things" };
        store.write(o);

        o = new ArrayKeyIndexType();
        o.key = new int[] { i, j, 1 };
        o.id = new String[] { "more things" };
        store.write(o);
      }
    }

    ArrayKeyIndexType o = new ArrayKeyIndexType();
    o.key = new int[] { 2, 2, 2 };
    o.id = new String[] { "things" };
    store.write(o);

    assertEquals(9, store.count(ArrayKeyIndexType.class));

    // Try removing non-existing keys
    assert(!store.removeAllByIndexValues(
      ArrayKeyIndexType.class,
      KVIndex.NATURAL_INDEX_NAME,
      ImmutableSet.of(new int[] {10, 10, 10}, new int[] { 3, 3, 3 })));
    assertEquals(9, store.count(ArrayKeyIndexType.class));

    assert(store.removeAllByIndexValues(
      ArrayKeyIndexType.class,
      KVIndex.NATURAL_INDEX_NAME,
      ImmutableSet.of(new int[] {0, 0, 0}, new int[] { 2, 2, 2 })));
    assertEquals(7, store.count(ArrayKeyIndexType.class));

    store.stopBackgroundThreadAndSwitchToLevelDB();
    assert(myListener.waitUntilDone());
    store.getWritingToLevelDBThread().join();
    assert(!store.getShouldUseMemoryStore());

    assert(store.removeAllByIndexValues(
      ArrayKeyIndexType.class,
      "id",
      ImmutableSet.of(new String [] { "things" })));
    assertEquals(4, store.count(ArrayKeyIndexType.class));

    assert(store.removeAllByIndexValues(
      ArrayKeyIndexType.class,
      "id",
      ImmutableSet.of(new String [] { "more things" })));
    assertEquals(0, store.count(ArrayKeyIndexType.class));
  }

  @Test
  public void testBasicIteration() throws Exception {
    HybridKVStore store = new HybridKVStore();
    store.setLevelDB(db);
    MyListener myListener = new MyListener();
    store.startBackgroundThreadToWriteToDB(myListener);

    CustomType1 t1 = createCustomType1(1);
    store.write(t1);

    CustomType1 t2 = createCustomType1(2);
    store.write(t2);

    store.stopBackgroundThreadAndSwitchToLevelDB();
    assert(myListener.waitUntilDone());
    store.getWritingToLevelDBThread().join();
    assert(!store.getShouldUseMemoryStore());

    assertEquals(t1.id, store.view(t1.getClass()).iterator().next().id);
    assertEquals(t2.id, store.view(t1.getClass()).skip(1).iterator().next().id);
    assertEquals(t2.id, store.view(t1.getClass()).skip(1).max(1).iterator().next().id);
    assertEquals(t1.id,
      store.view(t1.getClass()).first(t1.key).max(1).iterator().next().id);
    assertEquals(t2.id,
      store.view(t1.getClass()).first(t2.key).max(1).iterator().next().id);
  }

  @Test
  public void testSkip() throws Exception {
    HybridKVStore store = new HybridKVStore();
    store.setLevelDB(db);
    MyListener myListener = new MyListener();
    store.startBackgroundThreadToWriteToDB(myListener);

    for (int i = 0; i < 10; i++) {
      store.write(createCustomType1(i));
    }
    store.stopBackgroundThreadAndSwitchToLevelDB();
    assert(myListener.waitUntilDone());
    store.getWritingToLevelDBThread().join();
    assert(!store.getShouldUseMemoryStore());

    KVStoreIterator<CustomType1> it = store.view(CustomType1.class).closeableIterator();
    assertTrue(it.hasNext());
    assertTrue(it.skip(5));
    assertEquals("key5", it.next().key);
    assertTrue(it.skip(3));
    assertEquals("key9", it.next().key);
    assertFalse(it.hasNext());
  }

  private class MyListener implements HybridKVStore.SwitchingToLevelDBListener {
    private LinkedBlockingQueue<String> results = new LinkedBlockingQueue<>();

    @Override
    public void onSwitchingToLevelDBSuccess() {
      try {
        results.put("Succeed");
      } catch (InterruptedException ie) {
        // Ignore
      }
    }

    @Override
    public void onSwitchingToLevelDBFail(Exception e){
      try {
        results.put("Failed");
      } catch (InterruptedException ie) {
        // Ignore
      }
    }

    public boolean waitUntilDone() throws InterruptedException {
      String ret = results.take();
      return ret.equals("Succeed");
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

}
