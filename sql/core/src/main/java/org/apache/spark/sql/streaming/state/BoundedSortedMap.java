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
package org.apache.spark.sql.streaming.state;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class implements bounded {@link java.util.SortedMap} based on {@link java.util.TreeMap}.
 * <p/>
 * As TreeMap does, this implementation sorts elements in natural order, and cuts off
 * smaller elements to retain at most bigger N elements.
 * <p/>
 * You can provide reversed order of comparator to retain smaller elements instead.
 * <p/>
 * This class is not thread-safe, so synchronization would be needed to use this concurrently.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class BoundedSortedMap<K, V> extends TreeMap<K, V> {

  private final int limit;

  /**
   * Constructor
   *
   * @param comparator comparator instance to compare between keys
   * @param limit      bounded size
   */
  public BoundedSortedMap(Comparator<K> comparator, int limit) {
    super(comparator);
    this.limit = limit;
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map) {
    for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public V put(K key, V value) {
    // This method doesn't guarantee thread-safety, so it must be guarded with synchronization

    while (size() > limit) {
      remove(lastKey());
    }

    if (size() == limit) {
      K lk = lastKey();
      if (comparator().compare(lk, key) > 0) {
        remove(lk);
        return super.put(key, value);
      }

      // unchanged
      return null;
    }

    return super.put(key, value);
  }
}
