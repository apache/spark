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
import java.util.Iterator;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import org.apache.spark.annotation.Private;

/**
 * Implementation of KVStore that keeps data deserialized in memory. This store does not index
 * data; instead, whenever iterating over an indexed field, the stored data is copied and sorted
 * according to the index. This saves memory but makes iteration more expensive.
 */
@Private
public class InMemoryStore implements KVStore {

  private Object metadata;
  private InMemoryLists inMemoryLists = new InMemoryLists();

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
    InstanceList<?> list = inMemoryLists.get(type);
    return list != null ? list.size() : 0;
  }

  @Override
  public long count(Class<?> type, String index, Object indexedValue) throws Exception {
    InstanceList<?> list = inMemoryLists.get(type);
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
    InstanceList<T> list = inMemoryLists.get(klass);
    T value = list != null ? list.get(naturalKey) : null;
    if (value == null) {
      throw new NoSuchElementException();
    }
    return value;
  }

  @Override
  public void write(Object value) throws Exception {
    inMemoryLists.write(value);
  }

  @Override
  public void delete(Class<?> type, Object naturalKey) {
    InstanceList<?> list = inMemoryLists.get(type);
    if (list != null) {
      list.delete(naturalKey);
    }
  }

  @Override
  public <T> KVStoreView<T> view(Class<T> type){
    InstanceList<T> list = inMemoryLists.get(type);
    return list != null ? list.view() : emptyView();
  }

  @Override
  public void close() {
    metadata = null;
    inMemoryLists.clear();
  }

  @Override
  public <T> boolean removeAllByIndexValues(
      Class<T> klass,
      String index,
      Collection<?> indexValues) {
    InstanceList<T> list = inMemoryLists.get(klass);

    if (list != null) {
      return list.countingRemoveAllByIndexValues(index, indexValues) > 0;
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

  @SuppressWarnings("unchecked")
  private static <T> KVStoreView<T> emptyView() {
    return (InMemoryView<T>) InMemoryView.EMPTY_VIEW;
  }

  /**
   * Encapsulates ConcurrentHashMap so that the typing in and out of the map strictly maps a
   * class of type T to an InstanceList of type T.
   */
  private static class InMemoryLists {
    private final ConcurrentMap<Class<?>, InstanceList<?>> data = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public <T> InstanceList<T> get(Class<T> type) {
      return (InstanceList<T>) data.get(type);
    }

    @SuppressWarnings("unchecked")
    public <T> void write(T value) throws Exception {
      InstanceList<T> list =
        (InstanceList<T>) data.computeIfAbsent(value.getClass(), InstanceList::new);
      list.put(value);
    }

    public void clear() {
      data.clear();
    }
  }

  private static class InstanceList<T> {

    /**
     * A BiConsumer to control multi-entity removal.  We use this in a forEach rather than an
     * iterator because there is a bug in jdk8 which affects remove() on all concurrent map
     * iterators.  https://bugs.openjdk.java.net/browse/JDK-8078645
     */
    private static class CountingRemoveIfForEach<T> implements BiConsumer<Comparable<Object>, T> {
      private final ConcurrentMap<Comparable<Object>, T> data;
      private final Predicate<? super T> filter;

      /**
       * Keeps a count of the number of elements removed.  This count is not currently surfaced
       * to clients of KVStore as Java's generic removeAll() construct returns only a boolean,
       * but I found it handy to have the count of elements removed while debugging; a count being
       * no more complicated than a boolean, I've retained that behavior here, even though there
       * is no current requirement.
       */
      private int count = 0;

      CountingRemoveIfForEach(
          ConcurrentMap<Comparable<Object>, T> data,
          Predicate<? super T> filter) {
        this.data = data;
        this.filter = filter;
      }

      @Override
      public void accept(Comparable<Object> key, T value) {
        if (filter.test(value)) {
          if (data.remove(key, value)) {
            count++;
          }
        }
      }

      public int count() { return count; }
    }

    private final KVTypeInfo ti;
    private final KVTypeInfo.Accessor naturalKey;
    private final ConcurrentMap<Comparable<Object>, T> data;

    private InstanceList(Class<?> klass) {
      this.ti = new KVTypeInfo(klass);
      this.naturalKey = ti.getAccessor(KVIndex.NATURAL_INDEX_NAME);
      this.data = new ConcurrentHashMap<>();
    }

    KVTypeInfo.Accessor getIndexAccessor(String indexName) {
      return ti.getAccessor(indexName);
    }

    int countingRemoveAllByIndexValues(String index, Collection<?> indexValues) {
      Predicate<? super T> filter = getPredicate(ti.getAccessor(index), indexValues);
      CountingRemoveIfForEach<T> callback = new CountingRemoveIfForEach<>(data, filter);

      data.forEach(callback);
      return callback.count();
    }

    public T get(Object key) {
      return data.get(asKey(key));
    }

    public void put(T value) throws Exception {
      data.put(asKey(naturalKey.get(value)), value);
    }

    public void delete(Object key) {
      data.remove(asKey(key));
    }

    public int size() {
      return data.size();
    }

    public InMemoryView<T> view() {
      return new InMemoryView<>(data.values(), ti);
    }

    private static <T> Predicate<? super T> getPredicate(
        KVTypeInfo.Accessor getter,
        Collection<?> values) {
      if (Comparable.class.isAssignableFrom(getter.getType())) {
        HashSet<?> set = new HashSet<>(values);

        return (value) -> set.contains(indexValueForEntity(getter, value));
      } else {
        HashSet<Comparable> set = new HashSet<>(values.size());
        for (Object key : values) {
          set.add(asKey(key));
        }
        return (value) -> set.contains(asKey(indexValueForEntity(getter, value)));
      }
    }

    private static Object indexValueForEntity(KVTypeInfo.Accessor getter, Object entity) {
      try {
        return getter.get(entity);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class InMemoryView<T> extends KVStoreView<T> {
    private static final InMemoryView<?> EMPTY_VIEW =
      new InMemoryView<>(Collections.emptyList(), null);

    private final Collection<T> elements;
    private final KVTypeInfo ti;
    private final KVTypeInfo.Accessor natural;

    InMemoryView(Collection<T> elements, KVTypeInfo ti) {
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
        Comparable<?> firstKey = asKey(first);
        stream = stream.filter(e -> modifier * compare(e, getter, firstKey) >= 0);
      }

      if (last != null) {
        Comparable<?> lastKey = asKey(last);
        stream = stream.filter(e -> modifier * compare(e, getter, lastKey) <= 0);
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
        Comparable<?> parentKey = asKey(parent);

        return elements.stream()
          .filter(e -> compare(e, parentGetter, parentKey) == 0)
          .collect(Collectors.toList());
      } else {
        return new ArrayList<>(elements);
      }
    }

    private int compare(T e1, T e2, KVTypeInfo.Accessor getter) {
      try {
        int diff = compare(e1, getter, asKey(getter.get(e2)));
        if (diff == 0 && getter != natural) {
          diff = compare(e1, natural, asKey(natural.get(e2)));
        }
        return diff;
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }

    private int compare(T e1, KVTypeInfo.Accessor getter, Comparable<?> v2) {
      try {
        return asKey(getter.get(e1)).compareTo(v2);
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
