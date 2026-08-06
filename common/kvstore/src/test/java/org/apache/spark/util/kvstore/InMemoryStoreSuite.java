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

import java.util.Set;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class InMemoryStoreSuite {

  @Test
  public void testObjectWriteReadDelete() throws Exception {
    KVStore store = new InMemoryStore();

    CustomType1 t = new CustomType1();
    t.key = "key";
    t.id = "id";
    t.name = "name";

    assertThrows(NoSuchElementException.class, () -> store.read(CustomType1.class, t.key));

    store.write(t);
    assertEquals(t, store.read(t.getClass(), t.key));
    assertEquals(1L, store.count(t.getClass()));

    store.delete(t.getClass(), t.key);
    assertThrows(NoSuchElementException.class, () -> store.read(t.getClass(), t.key));
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
    assertThrows(NoSuchElementException.class, () -> store.read(t2.getClass(), t2.key));
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

    // Try removing non-existing keys
    assertFalse(store.removeAllByIndexValues(
      ArrayKeyIndexType.class,
      KVIndex.NATURAL_INDEX_NAME,
      Set.of(new int[] {10, 10, 10}, new int[] { 3, 3, 3 })));
    assertEquals(9, store.count(ArrayKeyIndexType.class));

    assertTrue(store.removeAllByIndexValues(
      ArrayKeyIndexType.class,
      KVIndex.NATURAL_INDEX_NAME,
      Set.of(new int[] {0, 0, 0}, new int[] { 2, 2, 2 })));
    assertEquals(7, store.count(ArrayKeyIndexType.class));

    assertTrue(store.removeAllByIndexValues(
      ArrayKeyIndexType.class,
      "id",
      Set.<String[]>of(new String [] { "things" })));
    assertEquals(4, store.count(ArrayKeyIndexType.class));

    assertTrue(store.removeAllByIndexValues(
      ArrayKeyIndexType.class,
      "id",
      Set.<String[]>of(new String [] { "more things" })));
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

  @Test
  public void testDeleteParentIndex() throws Exception {
    KVStore store = new InMemoryStore();

    CustomType2 t1 = new CustomType2();
    t1.key = "key1";
    t1.id = "id1";
    t1.parentId = "parentId1";
    store.write(t1);

    CustomType2 t2 = new CustomType2();
    t2.key = "key2";
    t2.id = "id2";
    t2.parentId = "parentId1";
    store.write(t2);

    CustomType2 t3 = new CustomType2();
    t3.key = "key3";
    t3.id = "id1";
    t3.parentId = "parentId2";
    store.write(t3);

    CustomType2 t4 = new CustomType2();
    t4.key = "key4";
    t4.id = "id2";
    t4.parentId = "parentId2";
    store.write(t4);

    assertEquals(4, store.count(CustomType2.class));

    store.delete(t1.getClass(), t1.key);
    assertEquals(3, store.count(CustomType2.class));

    store.delete(t2.getClass(), t2.key);
    assertEquals(2, store.count(CustomType2.class));

    store.delete(t3.getClass(), t3.key);
    assertEquals(1, store.count(CustomType2.class));

    store.delete(t4.getClass(), t4.key);
    assertEquals(0, store.count(CustomType2.class));
  }

  @Test
  public void testRemoveAllByNaturalParentIndex() throws Exception {
    KVStore store = new InMemoryStore();

    CustomType2 t1 = new CustomType2();
    t1.key = "key1";
    t1.id = "id1";
    t1.parentId = "parentId1";
    store.write(t1);

    CustomType2 t2 = new CustomType2();
    t2.key = "key2";
    t2.id = "id2";
    t2.parentId = "parentId1";
    store.write(t2);

    CustomType2 t3 = new CustomType2();
    t3.key = "key3";
    t3.id = "id3";
    t3.parentId = "parentId2";
    store.write(t3);

    assertEquals(3, store.count(CustomType2.class));

    // A parent value with no children removes nothing.
    assertFalse(store.removeAllByIndexValues(
      CustomType2.class, "parentId", Set.of("noSuchParent")));
    assertEquals(3, store.count(CustomType2.class));

    // Removing by a parent value deletes all of its children, and only those.
    assertTrue(store.removeAllByIndexValues(
      CustomType2.class, "parentId", Set.of("parentId1")));
    assertEquals(1, store.count(CustomType2.class));
    assertEquals("id3", store.read(CustomType2.class, "key3").id);

    assertTrue(store.removeAllByIndexValues(
      CustomType2.class, "parentId", Set.of("parentId2")));
    assertEquals(0, store.count(CustomType2.class));
  }

  @Test
  public void testViewByNaturalParentIndex() throws Exception {
    KVStore store = new InMemoryStore();

    CustomType2 t1 = new CustomType2();
    t1.key = "key1";
    t1.id = "id1";
    t1.parentId = "parentId1";
    store.write(t1);

    CustomType2 t2 = new CustomType2();
    t2.key = "key2";
    t2.id = "id2";
    t2.parentId = "parentId1";
    store.write(t2);

    CustomType2 t3 = new CustomType2();
    t3.key = "key3";
    t3.id = "id3";
    t3.parentId = "parentId2";
    store.write(t3);

    // A populated parent returns exactly its children, in natural-key order.
    try (KVStoreIterator<CustomType2> it =
        store.view(CustomType2.class).parent("parentId1").closeableIterator()) {
      assertTrue(it.hasNext());
      assertEquals("key1", it.next().key);
      assertTrue(it.hasNext());
      assertEquals("key2", it.next().key);
      assertFalse(it.hasNext());
    }

    // A parent value with no children yields an empty iterator (the early-return branch).
    try (KVStoreIterator<CustomType2> it =
        store.view(CustomType2.class).parent("noSuchParent").closeableIterator()) {
      assertFalse(it.hasNext());
    }
  }

  @Test
  public void testCountByIndexValue() throws Exception {
    KVStore store = new InMemoryStore();

    // Counting by an indexed value for a type that was never written must return 0 instead
    // of throwing, consistent with count(Class) and the LevelDB/RocksDB implementations.
    assertEquals(0, store.count(CustomType1.class, "id", "id"));

    // An unknown index name must be rejected even when the type has no rows, matching both
    // the populated-type path and the LevelDB/RocksDB stores (IllegalArgumentException),
    // rather than being silently swallowed into a 0.
    assertThrows(IllegalArgumentException.class,
      () -> store.count(CustomType1.class, "nonexistentIndex", "id"));

    CustomType1 t1 = new CustomType1();
    t1.key = "key1";
    t1.id = "id";
    t1.name = "name1";
    store.write(t1);

    CustomType1 t2 = new CustomType1();
    t2.key = "key2";
    t2.id = "id";
    t2.name = "name2";
    store.write(t2);

    assertEquals(2, store.count(CustomType1.class, "id", "id"));
    assertEquals(1, store.count(CustomType1.class, "name", "name1"));
    assertEquals(0, store.count(CustomType1.class, "name", "nonexistent"));

    // Same rejection for a populated type, confirming the absent/present paths agree.
    assertThrows(IllegalArgumentException.class,
      () -> store.count(CustomType1.class, "nonexistentIndex", "id"));
  }
}
