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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.*;

public class HybridStoreSuite {
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
  public void testMultipleObjectWriteReadDelete() throws Exception {
    HybridStore store = createHybridStore();

    CustomType1 t1 = createCustomType1(1);
    CustomType1 t2 = createCustomType1(2);

    try {
      store.read(t1.getClass(), t1.key);
      fail("Expected exception for non-existent object.");
    } catch (NoSuchElementException nsee) {
      // Expected.
    }

    store.write(t1);
    store.write(t2);

    assertEquals(t1, store.read(t1.getClass(), t1.key));
    assertEquals(t2, store.read(t2.getClass(), t2.key));
    assertEquals(2L, store.count(t1.getClass()));

    store.delete(t2.getClass(), t2.key);
    assertEquals(1L, store.count(t1.getClass()));

    switchToLevelDB(store);
    assertEquals(t1, store.read(t1.getClass(), t1.key));
    try {
      store.read(t2.getClass(), t2.key);
      fail("Expected exception for deleted object.");
    } catch (NoSuchElementException nsee) {
      // Expected.
    }
  }

  @Test
  public void testUpdate() throws Exception {
    HybridStore store = createHybridStore();

    CustomType1 t = createCustomType1(1);

    store.write(t);

    t.name = "anotherName";

    store.write(t);

    switchToLevelDB(store);
    assertEquals(1, store.count(t.getClass()));
    assertEquals("anotherName", store.read(t.getClass(), t.key).name);
  }

  @Test
  public void testArrayIndices() throws Exception {
    HybridStore store = createHybridStore();

    ArrayKeyIndexType o = new ArrayKeyIndexType();
    o.key = new int[] { 1, 2 };
    o.id = new String[] { "3", "4" };

    store.write(o);

    switchToLevelDB(store);
    assertEquals(o, store.read(ArrayKeyIndexType.class, o.key));
    assertEquals(o, store.view(ArrayKeyIndexType.class).index("id").first(o.id).iterator().next());
  }

  @Test
  public void testBasicIteration() throws Exception {
    HybridStore store = createHybridStore();

    CustomType1 t1 = createCustomType1(1);
    store.write(t1);

    CustomType1 t2 = createCustomType1(2);
    store.write(t2);

    switchToLevelDB(store);
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
    HybridStore store = createHybridStore();

    for (int i = 0; i < 10; i++) {
      store.write(createCustomType1(i));
    }

    switchToLevelDB(store);
    KVStoreIterator<CustomType1> it = store.view(CustomType1.class).closeableIterator();
    assertTrue(it.hasNext());
    assertTrue(it.skip(5));
    assertEquals("key5", it.next().key);
    assertTrue(it.skip(3));
    assertEquals("key9", it.next().key);
    assertFalse(it.hasNext());
  }

  @Test
  public void testRejectWriting() throws Exception {
    HybridStore store = createHybridStore();
    switchToLevelDB(store);
    try {
      store.write(createCustomType1(1));
      fail("Expected exception for writing object after switching to LevelDB");
    } catch (RuntimeException re) {
      // Expected.
    }
  }

  private HybridStore createHybridStore() {
    HybridStore store = new HybridStore();
    store.setLevelDB(db);
    return store;
  }

  private void switchToLevelDB(HybridStore store) throws Exception {
    SwitchingListener listener = new SwitchingListener();
    store.switchingToLevelDB(listener);
    assert(listener.waitUntilDone());
  }

  private class SwitchingListener implements HybridStore.SwitchingToLevelDBListener {
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
      String result = results.take();
      return result.equals("Succeed");
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
