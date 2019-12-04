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

import java.util.NoSuchElementException;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import static org.junit.Assert.*;

public class InMemoryStoreSuite {

  @Test
  public void testObjectWriteReadDelete() throws Exception {
    KVStore store = new InMemoryStore();

    CustomType1 t = new CustomType1();
    t.key = "key";
    t.id = "id";
    t.name = "name";

    try {
      store.read(CustomType1.class, t.key);
      fail("Expected exception for non-existent object.");
    } catch (NoSuchElementException nsee) {
      // Expected.
    }

    store.write(t);
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
    KVStore store = new InMemoryStore();

    CustomType1 t1 = new CustomType1();
    t1.key = "key1";
    t1.id = "id";
    t1.name = "name1";

    CustomType1 t2 = new CustomType1();
    t2.key = "key2";
    t2.id = "id";
    t2.name = "name2";

    store.write(t1);
    store.write(t2);

    assertEquals(t1, store.read(t1.getClass(), t1.key));
    assertEquals(t2, store.read(t2.getClass(), t2.key));
    assertEquals(2L, store.count(t1.getClass()));

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
    KVStore store = new InMemoryStore();
    assertNull(store.getMetadata(CustomType1.class));

    CustomType1 t = new CustomType1();
    t.id = "id";
    t.name = "name";

    store.setMetadata(t);
    assertEquals(t, store.getMetadata(CustomType1.class));

    store.setMetadata(null);
    assertNull(store.getMetadata(CustomType1.class));
  }

  @Test
  public void testUpdate() throws Exception {
    KVStore store = new InMemoryStore();

    CustomType1 t = new CustomType1();
    t.key = "key";
    t.id = "id";
    t.name = "name";

    store.write(t);

    t.name = "anotherName";

    store.write(t);
    assertEquals(1, store.count(t.getClass()));
    assertSame(t, store.read(t.getClass(), t.key));
  }

  @Test
  public void testArrayIndices() throws Exception {
    KVStore store = new InMemoryStore();

    ArrayKeyIndexType o = new ArrayKeyIndexType();
    o.key = new int[] { 1, 2 };
    o.id = new String[] { "3", "4" };

    store.write(o);
    assertEquals(o, store.read(ArrayKeyIndexType.class, o.key));
    assertEquals(o, store.view(ArrayKeyIndexType.class).index("id").first(o.id).iterator().next());
  }

  @Test
  public void testRemoveAll() throws Exception {
    KVStore store = new InMemoryStore();

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


    store.removeAllByIndexValues(
      ArrayKeyIndexType.class,
      KVIndex.NATURAL_INDEX_NAME,
      ImmutableSet.of(new int[] {0, 0, 0}, new int[] { 2, 2, 2 }));
    assertEquals(7, store.count(ArrayKeyIndexType.class));

    store.removeAllByIndexValues(
      ArrayKeyIndexType.class,
      "id",
      ImmutableSet.of(new String [] { "things" }));
    assertEquals(4, store.count(ArrayKeyIndexType.class));

    store.removeAllByIndexValues(
      ArrayKeyIndexType.class,
      "id",
      ImmutableSet.of(new String [] { "more things" }));
    assertEquals(0, store.count(ArrayKeyIndexType.class));
  }

  @Test
  public void testBasicIteration() throws Exception {
    KVStore store = new InMemoryStore();

    CustomType1 t1 = new CustomType1();
    t1.key = "1";
    t1.id = "id1";
    t1.name = "name1";
    store.write(t1);

    CustomType1 t2 = new CustomType1();
    t2.key = "2";
    t2.id = "id2";
    t2.name = "name2";
    store.write(t2);

    assertEquals(t1.id, store.view(t1.getClass()).iterator().next().id);
    assertEquals(t2.id, store.view(t1.getClass()).skip(1).iterator().next().id);
    assertEquals(t2.id, store.view(t1.getClass()).skip(1).max(1).iterator().next().id);
    assertEquals(t1.id,
      store.view(t1.getClass()).first(t1.key).max(1).iterator().next().id);
    assertEquals(t2.id,
      store.view(t1.getClass()).first(t2.key).max(1).iterator().next().id);
    assertFalse(store.view(t1.getClass()).first(t2.id).skip(1).iterator().hasNext());
  }

}
