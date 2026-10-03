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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

import org.apache.spark.annotation.Private;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.util.kvstore.KVStoreRecordCodec.Record;

/**
 * An opt-in, serialized in-memory store. Callers must not depend on read identity as they can
 * with {@link InMemoryStore}. Numeric natural keys use primitive maps and views sort records,
 * decoding only the values consumed by their callers. Application codecs can provide shared
 * compressed blocks and projections instead of the generic serialized representation.
 */
@Private
public class CompactInMemoryStore implements KVStore {

  private static final int DEFAULT_MAX_PAGE_CANDIDATES = 16 * 1024;

  private final KVStoreSerializer serializer;
  private final int maxPageCandidates;
  private final ConcurrentMap<Class<?>, Table<?>> tables = new ConcurrentHashMap<>();
  private byte[] metadata;

  public CompactInMemoryStore(KVStoreSerializer serializer) {
    this(serializer, DEFAULT_MAX_PAGE_CANDIDATES);
  }

  /**
   * Limits the candidates retained by a bounded page selection. Larger offsets and unbounded
   * scans sort record references; their full result cannot in general fit in a bounded heap.
   */
  public CompactInMemoryStore(KVStoreSerializer serializer, int maxPageCandidates) {
    this.serializer = Objects.requireNonNull(serializer);
    JavaUtils.checkArgument(maxPageCandidates > 0, "maxPageCandidates must be positive.");
    this.maxPageCandidates = maxPageCandidates;
  }

  /** Registers a codec before writing records of the given type. */
  public synchronized <T> void registerCodec(Class<T> type, KVStoreRecordCodec<T> codec) {
    Objects.requireNonNull(codec);
    tables.compute(type, (key, previous) -> {
      JavaUtils.checkArgument(previous == null || previous.size() == 0,
        "Cannot replace the codec of a populated type: %s", type.getName());
      return new Table<>(type, codec);
    });
  }

  /** Seals partially filled codec blocks, for example after a listener flush. */
  public synchronized void compact() {
    for (Table<?> table : tables.values()) {
      table.compact();
    }
  }

  /** A view which reconstructs list summaries when a codec supplies them. */
  public <T> KVStoreView<T> viewSummaries(Class<T> type) {
    return new CompactView<>(table(type), true);
  }

  @Override
  public synchronized <T> T getMetadata(Class<T> type) throws Exception {
    return metadata == null ? null : serializer.deserialize(metadata, type);
  }

  @Override
  public synchronized void setMetadata(Object value) throws Exception {
    metadata = value == null ? null : serializer.serialize(value);
  }

  @Override
  public <T> T read(Class<T> type, Object naturalKey) throws Exception {
    Table<T> table = existingTable(type);
    Record<T> record = table == null ? null : table.get(naturalKey);
    if (record == null) {
      throw new NoSuchElementException();
    }
    return record.decode();
  }

  @Override
  public void write(Object value) throws Exception {
    writeTyped(Objects.requireNonNull(value));
  }

  @SuppressWarnings("unchecked")
  private <T> void writeTyped(T value) throws Exception {
    table((Class<T>) value.getClass()).put(value);
  }

  @Override
  public void delete(Class<?> type, Object naturalKey) throws Exception {
    Table<?> table = existingTable(type);
    if (table != null) {
      table.delete(naturalKey);
    }
  }

  @Override
  public <T> KVStoreView<T> view(Class<T> type) {
    return new CompactView<>(table(type), false);
  }

  @Override
  public long count(Class<?> type) {
    Table<?> table = existingTable(type);
    return table == null ? 0 : table.size();
  }

  @Override
  public long count(Class<?> type, String index, Object value) throws Exception {
    Table<?> table = existingTable(type);
    if (table == null) {
      new KVTypeInfo(type).getAccessor(index);
      return 0;
    }
    return table.count(index, value);
  }

  @Override
  public <T> boolean removeAllByIndexValues(
      Class<T> type,
      String index,
      Collection<?> values) throws Exception {
    Table<T> table = existingTable(type);
    return table != null && table.removeAll(index, values);
  }

  @Override
  public synchronized void close() {
    for (Table<?> table : tables.values()) {
      table.clear();
    }
    tables.clear();
    metadata = null;
  }

  @SuppressWarnings("unchecked")
  private <T> Table<T> existingTable(Class<T> type) {
    return (Table<T>) tables.get(type);
  }

  @SuppressWarnings("unchecked")
  private <T> Table<T> table(Class<T> type) {
    return (Table<T>) tables.computeIfAbsent(type,
      key -> new Table<>(type, new SerializedRecordCodec<>(type, serializer)));
  }

  private static class Table<T> {
    private final KVTypeInfo typeInfo;
    private final Class<?> naturalKeyType;
    private final String naturalParent;
    private final KVStoreRecordCodec<T> codec;
    private CompactRecordMap<Record<T>> records;
    private final Map<Object, CompactRecordMap<Record<T>>> children = new HashMap<>();

    Table(Class<T> type, KVStoreRecordCodec<T> codec) {
      this.typeInfo = new KVTypeInfo(type);
      this.naturalKeyType = typeInfo.getAccessor(KVIndex.NATURAL_INDEX_NAME).getType();
      this.naturalParent = typeInfo.getParentIndexName(KVIndex.NATURAL_INDEX_NAME);
      this.codec = codec;
      this.records = CompactRecordMap.create(naturalKeyType);
    }

    synchronized int size() { return records.size(); }

    synchronized Record<T> get(Object key) {
      return records.get(Objects.requireNonNull(key));
    }

    synchronized void put(T value) throws Exception {
      Record<T> record = codec.encode(value);
      Object key;
      Object parent = null;
      try {
        key = Objects.requireNonNull(record.indexValue(KVIndex.NATURAL_INDEX_NAME));
        if (!naturalParent.isEmpty()) {
          parent = CompactRecordMap.asKey(record.indexValue(naturalParent));
        }
      } catch (Exception e) {
        record.removed();
        throw e;
      }
      Record<T> old = records.get(key);
      if (old != null && parent != null &&
          !parent.equals(CompactRecordMap.asKey(old.indexValue(naturalParent)))) {
        removeParent(key, old);
      }
      records.put(key, record);
      if (parent != null) {
        children.computeIfAbsent(parent, k -> CompactRecordMap.create(naturalKeyType))
          .put(key, record);
      }
      if (old != null) {
        old.removed();
      }
    }

    synchronized boolean delete(Object key) throws Exception {
      Record<T> record = records.get(key);
      if (record == null) {
        return false;
      }
      removeParent(key, record);
      records.remove(key);
      record.removed();
      return true;
    }

    private void removeParent(Object key, Record<T> record) throws Exception {
      if (!naturalParent.isEmpty()) {
        Object parent = CompactRecordMap.asKey(record.indexValue(naturalParent));
        CompactRecordMap<Record<T>> members = children.get(parent);
        if (members != null) {
          members.remove(key);
          if (members.size() == 0) {
            children.remove(parent);
          }
        }
      }
    }

    synchronized long count(String index, Object value) throws Exception {
      typeInfo.getAccessor(index);
      if (index.equals(KVIndex.NATURAL_INDEX_NAME)) {
        return records.get(value) == null ? 0 : 1;
      } else if (index.equals(naturalParent)) {
        CompactRecordMap<Record<T>> members = children.get(CompactRecordMap.asKey(value));
        return members == null ? 0 : members.size();
      }
      long count = 0;
      Object key = CompactRecordMap.asKey(value);
      for (Record<T> record : records.values()) {
        if (Objects.equals(key, CompactRecordMap.asKey(record.indexValue(index)))) {
          count++;
        }
      }
      return count;
    }

    synchronized boolean removeAll(String index, Collection<?> values) throws Exception {
      typeInfo.getAccessor(index);
      boolean removed = false;
      if (index.equals(KVIndex.NATURAL_INDEX_NAME)) {
        for (Object value : values) {
          removed |= delete(value);
        }
      } else if (index.equals(naturalParent)) {
        for (Object value : values) {
          CompactRecordMap<Record<T>> members = children.remove(CompactRecordMap.asKey(value));
          if (members != null) {
            for (Record<T> record : members.values()) {
              records.remove(record.indexValue(KVIndex.NATURAL_INDEX_NAME));
              record.removed();
              removed = true;
            }
          }
        }
      } else {
        Set<Object> keys = new HashSet<>();
        for (Object value : values) {
          keys.add(CompactRecordMap.asKey(value));
        }
        List<Record<T>> matching = new ArrayList<>();
        for (Record<T> record : records.values()) {
          if (keys.contains(CompactRecordMap.asKey(record.indexValue(index)))) {
            matching.add(record);
          }
        }
        for (Record<T> record : matching) {
          removed |= delete(record.indexValue(KVIndex.NATURAL_INDEX_NAME));
        }
      }
      return removed;
    }

    synchronized List<Record<T>> select(
        KVStoreView<T> view,
        Comparator<Record<T>> order,
        Predicate<Record<T>> matches,
        int candidateLimit) throws Exception {
      typeInfo.getAccessor(view.index);
      Iterable<Record<T>> source = records.values();
      if (view.parent != null) {
        String parentName = typeInfo.getParentIndexName(view.index);
        JavaUtils.checkArgument(!parentName.isEmpty(), "Parent filter for non-child index.");
        if (parentName.equals(naturalParent)) {
          CompactRecordMap<Record<T>> members =
            children.get(CompactRecordMap.asKey(view.parent));
          source = members == null ? Collections.emptyList() : members.values();
        }
      }
      if (candidateLimit > 0) {
        PriorityQueue<Record<T>> best =
          new PriorityQueue<>(Math.min(candidateLimit, 128), order.reversed());
        for (Record<T> record : source) {
          if (matches.test(record)) {
            if (best.size() < candidateLimit) {
              best.add(record);
            } else if (order.compare(record, best.peek()) < 0) {
              best.remove();
              best.add(record);
            }
          }
        }
        return new ArrayList<>(best);
      }
      List<Record<T>> result = new ArrayList<>();
      for (Record<T> record : source) {
        if (matches.test(record)) {
          result.add(record);
        }
      }
      return result;
    }

    synchronized void compact() {
      codec.compact();
    }

    synchronized void clear() {
      for (Record<T> record : records.values()) {
        record.removed();
      }
      records = CompactRecordMap.create(naturalKeyType);
      children.clear();
    }
  }

  private class CompactView<T> extends KVStoreView<T> {
    private final Table<T> table;
    private final boolean summaries;

    CompactView(Table<T> table, boolean summaries) {
      this.table = table;
      this.summaries = summaries;
    }

    @Override
    public Iterator<T> iterator() {
      table.typeInfo.getAccessor(index);
      int direction = ascending ? 1 : -1;
      Comparator<Record<T>> order = (a, b) -> {
        int diff = compare(indexValue(a, index), indexValue(b, index));
        if (diff == 0 && !index.equals(KVIndex.NATURAL_INDEX_NAME)) {
          diff = compare(indexValue(a, KVIndex.NATURAL_INDEX_NAME),
            indexValue(b, KVIndex.NATURAL_INDEX_NAME));
        }
        return direction * Integer.signum(diff);
      };
      String parentName = table.typeInfo.getParentIndexName(index);
      boolean scanParent = parent != null && !parentName.equals(table.naturalParent);
      Predicate<Record<T>> matches = record -> {
        if (scanParent && !parentName.isEmpty() &&
            compare(indexValue(record, parentName), parent) != 0) {
          return false;
        }
        Object value = null;
        if (first != null || last != null) {
          value = indexValue(record, index);
        }
        return (first == null || direction * Integer.signum(compare(value, first)) >= 0) &&
          (last == null || direction * Integer.signum(compare(value, last)) <= 0);
      };
      long offset = Math.max(0L, skip);
      int candidateLimit = offset < maxPageCandidates && max <= maxPageCandidates - offset
        ? (int) (offset + max) : 0;
      List<Record<T>> records;
      try {
        records = table.select(this, order, matches, candidateLimit);
      } catch (Exception e) {
        throw propagate(e);
      }
      records.sort(order);
      int from = (int) Math.min(offset, records.size());
      int length = (int) Math.min(max, records.size() - from);
      // Copy just the requested page so a long-lived iterator cannot retain skipped records.
      List<Record<T>> page = new ArrayList<>(records.subList(from, from + length));
      return new CompactIterator<>(page, summaries);
    }
  }

  private static class CompactIterator<T> implements KVStoreIterator<T> {
    private List<Record<T>> records;
    private final boolean summaries;
    private int position;

    CompactIterator(List<Record<T>> records, boolean summaries) {
      this.records = records;
      this.summaries = summaries;
    }

    @Override
    public boolean hasNext() {
      if (position >= records.size()) {
        close();
        return false;
      }
      return true;
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Record<T> record = records.set(position++, null);
      try {
        return summaries ? record.decodeSummary() : record.decode();
      } catch (Exception e) {
        close();
        throw propagate(e);
      }
    }

    @Override
    public List<T> next(int max) {
      List<T> result = new ArrayList<>();
      while (result.size() < max && hasNext()) {
        result.add(next());
      }
      return result;
    }

    @Override
    public boolean skip(long count) {
      long remaining = Math.max(0L, count);
      while (remaining > 0 && position < records.size()) {
        records.set(position++, null);
        remaining--;
      }
      return hasNext();
    }

    @Override
    public void close() {
      records = Collections.emptyList();
      position = 0;
    }
  }

  private static Object indexValue(Record<?> record, String index) {
    try {
      return record.indexValue(index);
    } catch (Exception e) {
      throw propagate(e);
    }
  }

  @SuppressWarnings("unchecked")
  private static int compare(Object first, Object second) {
    return ((Comparable<Object>) CompactRecordMap.asKey(first))
      .compareTo(CompactRecordMap.asKey(second));
  }

  private static RuntimeException propagate(Exception exception) {
    return exception instanceof RuntimeException re ? re : new RuntimeException(exception);
  }
}
