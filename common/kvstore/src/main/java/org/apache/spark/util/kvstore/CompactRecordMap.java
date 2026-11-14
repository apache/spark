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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/** Maps natural keys to records; callers provide synchronization. */
abstract class CompactRecordMap<V> {

  static <V> CompactRecordMap<V> create(Class<?> keyType) {
    if (keyType == Long.class || keyType == long.class) {
      return new LongKeys<>(Long.class);
    } else if (keyType == Integer.class || keyType == int.class) {
      return new LongKeys<>(Integer.class);
    }
    return new ObjectKeys<>();
  }

  abstract V get(Object key);

  abstract V put(Object key, V value);

  abstract V remove(Object key);

  abstract int size();

  abstract Iterable<V> values();

  private static class ObjectKeys<V> extends CompactRecordMap<V> {
    private final Map<Object, V> data = new HashMap<>();

    @Override V get(Object key) { return data.get(asKey(key)); }

    @Override V put(Object key, V value) { return data.put(asKey(key), value); }

    @Override V remove(Object key) { return data.remove(asKey(key)); }

    @Override int size() { return data.size(); }

    @Override Iterable<V> values() { return data.values(); }
  }

  /** Open addressing avoids both boxed numeric keys and a hash node for each record. */
  private static class LongKeys<V> extends CompactRecordMap<V> {
    private final Class<?> keyClass;
    private long[] keys = new long[16];
    private Object[] data = new Object[16];
    private int size;

    LongKeys(Class<?> keyClass) {
      this.keyClass = keyClass;
    }

    @Override
    @SuppressWarnings("unchecked")
    V get(Object key) {
      if (!keyClass.isInstance(key)) {
        return null;
      }
      int pos = position(((Number) key).longValue());
      return (V) data[pos];
    }

    @Override
    @SuppressWarnings("unchecked")
    V put(Object key, V value) {
      if (!keyClass.isInstance(key)) {
        throw new IllegalArgumentException("Unexpected natural key type: " + key.getClass());
      }
      long number = ((Number) key).longValue();
      int pos = position(number);
      V previous = (V) data[pos];
      if (previous == null && (size + 1) * 4L >= data.length * 3L) {
        resize(data.length * 2);
        pos = position(number);
      }
      keys[pos] = number;
      data[pos] = value;
      if (previous == null) {
        size++;
      }
      return previous;
    }

    @Override
    @SuppressWarnings("unchecked")
    V remove(Object key) {
      if (!keyClass.isInstance(key)) {
        return null;
      }
      int hole = position(((Number) key).longValue());
      V previous = (V) data[hole];
      if (previous == null) {
        return null;
      }
      int mask = data.length - 1;
      int next = (hole + 1) & mask;
      while (data[next] != null) {
        int home = hash(keys[next]) & mask;
        if (((hole - home) & mask) < ((next - home) & mask)) {
          keys[hole] = keys[next];
          data[hole] = data[next];
          hole = next;
        }
        next = (next + 1) & mask;
      }
      data[hole] = null;
      size--;
      if (data.length > 16 && size < data.length / 4) {
        resize(data.length / 2);
      }
      return previous;
    }

    private int position(long key) {
      int mask = data.length - 1;
      int pos = hash(key) & mask;
      while (data[pos] != null && keys[pos] != key) {
        pos = (pos + 1) & mask;
      }
      return pos;
    }

    private void resize(int capacity) {
      long[] oldKeys = keys;
      Object[] oldData = data;
      keys = new long[capacity];
      data = new Object[capacity];
      for (int i = 0; i < oldData.length; i++) {
        if (oldData[i] != null) {
          int pos = position(oldKeys[i]);
          keys[pos] = oldKeys[i];
          data[pos] = oldData[i];
        }
      }
    }

    private static int hash(long key) {
      long mixed = key;
      mixed = (mixed ^ (mixed >>> 33)) * 0xff51afd7ed558ccdL;
      mixed = (mixed ^ (mixed >>> 33)) * 0xc4ceb9fe1a85ec53L;
      return (int) (mixed ^ (mixed >>> 33));
    }

    @Override int size() { return size; }

    @Override
    Iterable<V> values() {
      return () -> new Iterator<>() {
        private int next;

        @Override
        public boolean hasNext() {
          while (next < data.length && data[next] == null) {
            next++;
          }
          return next < data.length;
        }

        @Override
        @SuppressWarnings("unchecked")
        public V next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          return (V) data[next++];
        }
      };
    }
  }

  static Object asKey(Object value) {
    return value.getClass().isArray() ? ArrayWrappers.forArray(value) : value;
  }
}
