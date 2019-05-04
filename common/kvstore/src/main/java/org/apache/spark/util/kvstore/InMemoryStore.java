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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import org.apache.spark.annotation.Private;

/**
 * Implementation of KVStore that keeps data deserialized in memory. This store does not index
 * data; instead, whenever iterating over an indexed field, the stored data is copied and sorted
 * according to the index. This saves memory but makes iteration more expensive.
 */
@Private
public class InMemoryStore implements KVStore {

  private Object metadata;
  private ConcurrentMap<Class<?>, InstanceList> data = new ConcurrentHashMap<>();

  @Override
  public <T> T getMetadata(Class<T> klass) {
    return klass.cast(metadata);
  }

  @Override
  public void setMetadata(Object value) {
    this.metadata = value;
  }

  @Override
  public long count(Class<?> type) {
    InstanceList list = data.get(type);
    return list != null ? list.size() : 0;
  }

  @Override
  public long count(Class<?> type, String index, Object indexedValue) throws Exception {
    InstanceList list = data.get(type);
    int count = 0;
    Object comparable = asKey(indexedValue);
    KVTypeInfo.Accessor accessor = list.getIndexAccessor(index);
    for (Object o : view(type)) {
      if (Objects.equal(comparable, asKey(accessor.get(o)))) {
        count++;
      }
    }
    return count;
  }

  @Override
  public <T> T read(Class<T> klass, Object naturalKey) {
    InstanceList list = data.get(klass);
    Object value = list != null ? list.get(naturalKey) : null;
    if (value == null) {
      throw new NoSuchElementException();
    }
    return klass.cast(value);
  }

  @Override
  public void write(Object value) throws Exception {
    InstanceList list = data.computeIfAbsent(value.getClass(), key -> {
      try {
        return new InstanceList(key);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    });
    list.put(value);
  }

  @Override
  public void delete(Class<?> type, Object naturalKey) {
    InstanceList list = data.get(type);
    if (list != null) {
      list.delete(naturalKey);
    }
  }

  @Override
  public <T> KVStoreView<T> view(Class<T> type){
    InstanceList list = data.get(type);
    return list != null ? list.view(type)
      : new InMemoryView<>(type, Collections.emptyList(), null);
  }

  @Override
  public void close() {
    metadata = null;
    data.clear();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> boolean removeAllByKeys(Class<T> klass, String index, Collection keys) {
    InstanceList list = data.get(klass);

    if (list != null) {
      return list.countingRemoveAllByKeys(index, keys) > 0;
    } else {
      return false;
    }
  }

  @SuppressWarnings("unchecked")
  private static Comparable<Object> asKey(Object in) {
    if (in.getClass().isArray()) {
      in = ArrayWrappers.forArray(in);
    }
    return (Comparable<Object>) in;
  }

  /**
   * Encapsulates ConcurrentHashMap so that the typing in and out of the map strictly maps a
   * class of type T to an InstanceList of type T.
   */
  private static class InMemoryLists {
    private ConcurrentMap<Class<?>, InstanceList> data = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public <T> InstanceList get(Class<T> key) {
      return (InstanceList)data.get(key);
    }

    @SuppressWarnings("unchecked")
    public <T> void write(T value) throws Exception {
      InstanceList list = data.computeIfAbsent(value.getClass(), InstanceList::new);
      list.put(value);
    }

    public void clear() {
      data.clear();
    }
  }

  private static class InstanceList {

    private static class CountingRemoveIfForEach<T> implements BiConsumer<Comparable<Object>, T> {
      ConcurrentMap<Comparable<Object>, T> data;
      Predicate<? super T> filter;
      int count = 0;

      CountingRemoveIfForEach(
          ConcurrentMap<Comparable<Object>, T> data,
          Predicate<? super T> filter) {
        this.data = data;
        this.filter = filter;
      }

      public void accept(Comparable<Object> key, T value) {
        // To address https://bugs.openjdk.java.net/browse/JDK-8078645 which affects remove() on
        // all iterators of concurrent maps, and specifically makes countingRemoveIf difficult to
        // implement correctly against the values() iterator, we use forEach instead....
        if (filter.test(value)) {
          if (data.remove(key, value)) {
            count++;
          }
        }
      }
    }

    private final KVTypeInfo ti;
    private final KVTypeInfo.Accessor naturalKey;
    private final ConcurrentMap<Comparable<Object>, Object> data;

    private InstanceList(Class<?> klass) {
      this.ti = new KVTypeInfo(klass);
      this.naturalKey = ti.getAccessor(KVIndex.NATURAL_INDEX_NAME);
      this.data = new ConcurrentHashMap<>();
    }

    KVTypeInfo.Accessor getIndexAccessor(String indexName) {
      return ti.getAccessor(indexName);
    }

    public Object get(Object key) {
      return data.get(asKey(key));
    }

    // Note: removeIf returns a boolean if any element has been removed.
    // While debugging this code, it was handy to have the count of elements
    // removed, rather than an indicator of whether something has been
    // removed, and a count is no more complicated than a boolean so I've
    // retained that behavior here, although there is no current requirement.
    @SuppressWarnings("unchecked")
    int countingRemoveAllByKeys(String index, Collection keys) {
      Predicate filter = getPredicate(ti.getAccessor(index), keys);
      CountingRemoveIfForEach callback = new CountingRemoveIfForEach(data, filter);

      data.forEach(callback);
      return callback.count;
    }

    public void put(Object value) throws Exception {
      Preconditions.checkArgument(ti.type().equals(value.getClass()),
        "Unexpected type: %s", value.getClass());
      data.put(asKey(naturalKey.get(value)), value);
    }

    public void delete(Object key) {
      data.remove(asKey(key));
    }

    public int size() {
      return data.size();
    }

    @SuppressWarnings("unchecked")
    public <T> InMemoryView<T> view(Class<T> type) {
      Preconditions.checkArgument(ti.type().equals(type), "Unexpected type: %s", type);
      Collection<T> all = (Collection<T>) data.values();
      return new InMemoryView<>(type, all, ti);
    }

    @SuppressWarnings("unchecked")
    private static <T> Predicate<? super T> getPredicate(
        KVTypeInfo.Accessor getter,
        Collection keys) {
      if (Comparable.class.isAssignableFrom(getter.getType())) {
        HashSet set = new HashSet(keys);

        return (value) -> set.contains(keyFromValue(getter, value));
      } else {
        HashSet<Comparable> set = new HashSet<>(keys.size());
        for (Object key : keys) {
          set.add(asKey(key));
        }
        return (value) -> set.contains(asKey(keyFromValue(getter, value)));
      }
    }

    private static Object keyFromValue(KVTypeInfo.Accessor getter, Object value) {
      try {
        return getter.get(value);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class InMemoryView<T> extends KVStoreView<T> {

    private final Collection<T> elements;
    private final KVTypeInfo ti;
    private final KVTypeInfo.Accessor natural;

    InMemoryView(Class<T> type, Collection<T> elements, KVTypeInfo ti) {
      super(type);
      this.elements = elements;
      this.ti = ti;
      this.natural = ti != null ? ti.getAccessor(KVIndex.NATURAL_INDEX_NAME) : null;
    }

    @Override
    public Iterator<T> iterator() {
      if (elements.isEmpty()) {
        return new InMemoryIterator<>(elements.iterator());
      }

      KVTypeInfo.Accessor getter = index != null ? ti.getAccessor(index) : null;
      int modifier = ascending ? 1 : -1;

      final List<T> sorted = copyElements();
      sorted.sort((e1, e2) -> modifier * compare(e1, e2, getter));
      Stream<T> stream = sorted.stream();

      if (first != null) {
        stream = stream.filter(e -> modifier * compare(e, getter, first) >= 0);
      }

      if (last != null) {
        stream = stream.filter(e -> modifier * compare(e, getter, last) <= 0);
      }

      if (skip > 0) {
        stream = stream.skip(skip);
      }

      if (max < sorted.size()) {
        stream = stream.limit((int) max);
      }

      return new InMemoryIterator<>(stream.iterator());
    }

    /**
     * Create a copy of the input elements, filtering the values for child indices if needed.
     */
    private List<T> copyElements() {
      if (parent != null) {
        KVTypeInfo.Accessor parentGetter = ti.getParentAccessor(index);
        Preconditions.checkArgument(parentGetter != null, "Parent filter for non-child index.");

        return elements.stream()
          .filter(e -> compare(e, parentGetter, parent) == 0)
          .collect(Collectors.toList());
      } else {
        return new ArrayList<>(elements);
      }
    }

    private int compare(T e1, T e2, KVTypeInfo.Accessor getter) {
      try {
        int diff = compare(e1, getter, getter.get(e2));
        if (diff == 0 && getter != natural) {
          diff = compare(e1, natural, natural.get(e2));
        }
        return diff;
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }

    private int compare(T e1, KVTypeInfo.Accessor getter, Object v2) {
      try {
        return asKey(getter.get(e1)).compareTo(asKey(v2));
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }

  }

  private static class InMemoryIterator<T> implements KVStoreIterator<T> {

    private final Iterator<T> iter;

    InMemoryIterator(Iterator<T> iter) {
      this.iter = iter;
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public T next() {
      return iter.next();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<T> next(int max) {
      List<T> list = new ArrayList<>(max);
      while (hasNext() && list.size() < max) {
        list.add(next());
      }
      return list;
    }

    @Override
    public boolean skip(long n) {
      long skipped = 0;
      while (skipped < n) {
        if (hasNext()) {
          next();
          skipped++;
        } else {
          return false;
        }
      }

      return hasNext();
    }

    @Override
    public void close() {
      // no op.
    }

  }

}
