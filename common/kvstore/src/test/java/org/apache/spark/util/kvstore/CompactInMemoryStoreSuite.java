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

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class CompactInMemoryStoreSuite {

  public static class NumericType {
    @KVIndex(parent = "group")
    public long key;

    @KVIndex("group")
    public int group;

    @KVIndex(value = "score", parent = "group")
    public long score;

    public String payload;

    public NumericType() {}

    NumericType(long key, int group, long score, String payload) {
      this.key = key;
      this.group = group;
      this.score = score;
      this.payload = payload;
    }
  }

  private static class CountingCodec implements KVStoreRecordCodec<NumericType> {
    int decoded;
    int summaries;
    int removed;
    int compacted;

    @Override
    public Record<NumericType> encode(NumericType value) {
      long key = value.key;
      int group = value.group;
      long score = value.score;
      String payload = value.payload;
      return new Record<>() {
        @Override
        public NumericType decode() {
          decoded++;
          return new NumericType(key, group, score, payload);
        }

        @Override
        public NumericType decodeSummary() {
          summaries++;
          return new NumericType(key, group, score, "summary");
        }

        @Override
        public Object indexValue(String name) {
          return switch (name) {
            case KVIndex.NATURAL_INDEX_NAME -> key;
            case "group" -> group;
            case "score" -> score;
            default -> throw new IllegalArgumentException(name);
          };
        }

        @Override
        public void removed() {
          removed++;
        }
      };
    }

    @Override
    public void compact() {
      compacted++;
    }
  }

  @Test
  public void pagesMatchExistingStoreWithTiesAndRanges() throws Exception {
    // Exercise both bounded selection and the fallback for larger offsets.
    for (int limit : new int[] {8, 256}) {
      try (KVStore baseline = new InMemoryStore();
           KVStore compact = new CompactInMemoryStore(new KVStoreSerializer(), limit)) {
        for (int i = 199; i >= 0; i--) {
          NumericType value = new NumericType(i, i % 3, (i % 11) - 5, "payload" + i);
          baseline.write(value);
          compact.write(value);
        }
        for (boolean reversed : new boolean[] {false, true}) {
          for (int offset : new int[] {0, 1, 17, 200}) {
            KVStoreView<NumericType> expected =
              baseline.view(NumericType.class).index("score").parent(1).skip(offset).max(7);
            KVStoreView<NumericType> actual =
              compact.view(NumericType.class).index("score").parent(1).skip(offset).max(7);
            if (reversed) {
              expected.reverse().first(4L).last(-4L);
              actual.reverse().first(4L).last(-4L);
            } else {
              expected.first(-4L).last(4L);
              actual.first(-4L).last(4L);
            }
            assertEquals(keys(expected), keys(actual));
          }
        }
      }
    }
  }

  @Test
  public void onlyConsumedPageRowsAreDecoded() throws Exception {
    CountingCodec codec = new CountingCodec();
    try (CompactInMemoryStore store = new CompactInMemoryStore(new KVStoreSerializer())) {
      store.registerCodec(NumericType.class, codec);
      for (int i = 0; i < 100; i++) {
        store.write(new NumericType(i, 1, 100 - i, "payload"));
      }
      assertEquals(100, store.count(NumericType.class, "group", 1));
      assertEquals(1, store.count(NumericType.class, "score", 50L));
      try (KVStoreIterator<NumericType> it = store.view(NumericType.class)
          .index("score").parent(1).skip(10).max(5).closeableIterator()) {
        assertEquals(0, codec.decoded);
        assertTrue(it.skip(2));
        assertEquals(0, codec.decoded);
        assertEquals(87, it.next().key);
        assertEquals(1, codec.decoded);
      }
      try (KVStoreIterator<NumericType> it =
          store.viewSummaries(NumericType.class).max(2).closeableIterator()) {
        assertEquals("summary", it.next().payload);
      }
      assertEquals(1, codec.decoded);
      assertEquals(1, codec.summaries);
      assertThrows(IllegalArgumentException.class,
        () -> store.registerCodec(NumericType.class, new CountingCodec()));
    }
    assertEquals(100, codec.removed);
  }

  @Test
  public void snapshotsSurviveUpdatesDeletionCompactionAndClose() throws Exception {
    CountingCodec codec = new CountingCodec();
    CompactInMemoryStore store = new CompactInMemoryStore(new KVStoreSerializer());
    store.registerCodec(NumericType.class, codec);
    store.write(new NumericType(1, 1, 10, "old"));
    store.write(new NumericType(2, 1, 20, "deleted"));
    try (KVStoreIterator<NumericType> it = store.view(NumericType.class).closeableIterator()) {
      store.write(new NumericType(1, 2, 30, "new"));
      store.delete(NumericType.class, 2L);
      assertEquals(0, store.count(NumericType.class, "group", 1));
      assertEquals(1, store.count(NumericType.class, "group", 2));
      store.compact();
      store.close();
      assertEquals("old", it.next().payload);
      assertEquals("deleted", it.next().payload);
      assertFalse(it.hasNext());
      assertEquals(3, codec.removed);
      assertEquals(1, codec.compacted);
    }
  }

  @Test
  public void removeByNaturalParentAndSecondaryIndex() throws Exception {
    try (CompactInMemoryStore store = new CompactInMemoryStore(new KVStoreSerializer())) {
      for (int i = 0; i < 100; i++) {
        store.write(new NumericType(i, i % 3, i % 7, "payload"));
      }
      assertFalse(store.removeAllByIndexValues(NumericType.class, "group", Set.of(5)));
      assertTrue(store.removeAllByIndexValues(NumericType.class, "group", Set.of(0)));
      assertEquals(66, store.count(NumericType.class));
      assertEquals(0, store.count(NumericType.class, "group", 0));
      assertTrue(store.removeAllByIndexValues(NumericType.class, "score", Set.of(3L, 4L)));
      assertEquals(0, store.count(NumericType.class, "score", 3L));
      assertTrue(store.removeAllByIndexValues(
        NumericType.class, KVIndex.NATURAL_INDEX_NAME, Set.of(1L, 2L)));
      assertThrows(NoSuchElementException.class, () -> store.read(NumericType.class, 1L));
      assertEquals(keys(store.view(NumericType.class).parent(1)).size(),
        store.count(NumericType.class, "group", 1));
    }
  }

  @Test
  public void serializedRecordsCopyMutableValuesAndArrayIndices() throws Exception {
    try (CompactInMemoryStore store = new CompactInMemoryStore(new KVStoreSerializer())) {
      ArrayKeyIndexType value = new ArrayKeyIndexType();
      value.key = new int[] {1, 2};
      value.id = new String[] {"a", "b"};
      store.write(value);
      value.key[0] = 9;
      value.id[0] = "changed";
      ArrayKeyIndexType read = store.read(ArrayKeyIndexType.class, new int[] {1, 2});
      assertArrayEquals(new int[] {1, 2}, read.key);
      assertArrayEquals(new String[] {"a", "b"}, read.id);
      assertEquals(1, store.count(ArrayKeyIndexType.class, "id", new String[] {"a", "b"}));
      assertTrue(store.removeAllByIndexValues(
        ArrayKeyIndexType.class, "id", List.of(new String[][] {{"a", "b"}})));
      assertEquals(0, store.count(ArrayKeyIndexType.class));
    }
  }

  @Test
  public void packedStringIndicesPreserveAllUtf16CodeUnits() throws Exception {
    ArrayKeyIndexType value = new ArrayKeyIndexType();
    value.key = new int[] {1};
    value.id = new String[] {
      new String(new char[] {'a', Character.MIN_HIGH_SURROGATE}),
      new String(new char[] {0, Character.MAX_VALUE}),
      new String(Character.toChars(0x1f600))
    };
    KVStoreRecordCodec.Record<ArrayKeyIndexType> record =
      new SerializedRecordCodec<>(ArrayKeyIndexType.class, new KVStoreSerializer()).encode(value);
    assertArrayEquals(value.id, (Object[]) record.indexValue("id"));
  }

  @Test
  public void fallbackCompressionMetadataAndIndexValidation() throws Exception {
    KVStoreSerializer uncompressed = new KVStoreSerializer() {
      @Override
      public byte[] serialize(Object value) throws Exception {
        return mapper.writeValueAsBytes(value);
      }

      @Override
      public <T> T deserialize(byte[] bytes, Class<T> type) throws Exception {
        return mapper.readValue(bytes, type);
      }
    };
    try (CompactInMemoryStore store = new CompactInMemoryStore(uncompressed)) {
      assertEquals(0, store.count(NumericType.class, "group", 1));
      assertThrows(IllegalArgumentException.class,
        () -> store.count(NumericType.class, "missing", 1));
      assertThrows(IllegalArgumentException.class,
        () -> store.view(NumericType.class).index("missing").iterator());
      assertThrows(IllegalArgumentException.class,
        () -> store.view(NumericType.class).index("group").parent(1).iterator());
      NumericType value = new NumericType(Long.MAX_VALUE, 1, Long.MIN_VALUE, "x".repeat(10000));
      store.write(value);
      assertEquals(value.payload, store.read(NumericType.class, Long.MAX_VALUE).payload);
      store.setMetadata(value);
      value.payload = "mutated";
      assertEquals("x".repeat(10000), store.getMetadata(NumericType.class).payload);
      store.setMetadata(null);
      assertNull(store.getMetadata(NumericType.class));
    }
  }

  @Test
  public void iteratorCloseAndSkipDoNotDeserialize() throws Exception {
    CountingCodec codec = new CountingCodec();
    try (CompactInMemoryStore store = new CompactInMemoryStore(new KVStoreSerializer())) {
      store.registerCodec(NumericType.class, codec);
      store.write(new NumericType(1, 1, 1, "payload"));
      try (KVStoreIterator<NumericType> it = store.view(NumericType.class).closeableIterator()) {
        assertFalse(it.skip(Long.MAX_VALUE));
        assertEquals(0, codec.decoded);
        assertThrows(NoSuchElementException.class, it::next);
      }
      KVStoreIterator<NumericType> closed = store.view(NumericType.class).closeableIterator();
      closed.close();
      assertFalse(closed.hasNext());
      assertThrows(NoSuchElementException.class, closed::next);
    }
  }

  @Test
  public void concurrentQueriesKeepTheirSelectedValues() throws Exception {
    ExecutorService threads = Executors.newFixedThreadPool(2);
    try (CompactInMemoryStore store = new CompactInMemoryStore(new KVStoreSerializer(), 16)) {
      for (int i = 0; i < 64; i++) {
        store.write(new NumericType(i, i % 2, i, "initial"));
      }
      CountDownLatch start = new CountDownLatch(1);
      Future<?> writes = threads.submit(() -> {
        start.await();
        for (int i = 0; i < 512; i++) {
          long key = i % 64;
          if (i % 5 == 0) {
            store.delete(NumericType.class, key);
          }
          store.write(new NumericType(key, i % 2, 512 - i, "updated"));
        }
        return null;
      });
      Future<?> reads = threads.submit(() -> {
        start.await();
        for (int i = 0; i < 128; i++) {
          try (KVStoreIterator<NumericType> it = store.view(NumericType.class)
              .index("score").parent(i % 2).max(12).closeableIterator()) {
            long previous = Long.MIN_VALUE;
            while (it.hasNext()) {
              NumericType value = it.next();
              assertEquals(i % 2, value.group);
              assertTrue(value.score >= previous);
              previous = value.score;
            }
          }
        }
        return null;
      });
      start.countDown();
      writes.get(30, TimeUnit.SECONDS);
      reads.get(30, TimeUnit.SECONDS);
    } finally {
      threads.shutdownNow();
    }
  }

  private static List<Long> keys(KVStoreView<NumericType> view) throws Exception {
    List<Long> result = new ArrayList<>();
    try (KVStoreIterator<NumericType> it = view.closeableIterator()) {
      while (it.hasNext()) {
        result.add(it.next().key);
      }
    }
    return result;
  }
}
