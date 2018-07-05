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
import java.util.SortedMap;
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
public final class BoundedSortedMap<K, V> extends TreeMap<K, V> {

  private final int limit;

  /**
   * Constructor
   *
   * @param comparator comparator instance to compare between keys
   * @param limit bounded size
   */
  public BoundedSortedMap(Comparator<K> comparator, int limit) {
    super(comparator);
    this.limit = limit;
  }

  @Override
  public V put(K key, V value) {
    // This method doesn't guarantee thread-safety, so it must be guarded with synchronization
    if (size() > limit) {
      throw new IllegalStateException("BoundedSortedMap is broken: already out of bound.");
    }

    if (size() == limit) {
      K lk = lastKey();
      int comp = comparator().compare(lk, key);
      if (comp > 0) {
        remove(lk);
        return super.put(key, value);
      } else if (comp == 0) {
        // just overwrite it without explicitly removing
        return super.put(key, value);
      }

      // unchanged
      return null;
    }

    return super.put(key, value);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map) {
    if (size() > limit) {
      throw new IllegalStateException("BoundedSortedMap is broken: already out of bound.");
    }

    if (isApplicableOfPutAll0(map)) {
      SortedMap<? extends K, ? extends V> paramMap = (SortedMap<? extends K, ? extends V>) map;
      putAll0(paramMap);
    } else {
      // fail back to put all entries one by one
      for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
        put(entry.getKey(), entry.getValue());
      }
    }
  }

  private boolean isApplicableOfPutAll0(Map<? extends K, ? extends V> map) {
    if (map instanceof SortedMap) {
      SortedMap<? extends K, ? extends V> paramMap = (SortedMap<? extends K, ? extends V>) map;

      return (paramMap.comparator() == null && comparator() == null) || (
          paramMap.comparator() != null && paramMap.comparator().equals(comparator()));
    }

    return false;
  }

  private void putAll0(SortedMap<? extends K, ? extends V> map) {
    // assuming isApplicableOfPutAll0 returns true, which means comparators in both maps
    // guarantee same ordering of key

    // if first key of this map is bigger (if it's natural ordering) than last key of parameter,
    // all of elements in this map will be evicted.
    // clear the map and put parameter's entries until reaching limit.
    K fk = firstKey();
    if (comparator().compare(fk, map.lastKey()) > 0) {
      clear();
      for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
        // safe to directly call super.put
        super.put(entry.getKey(), entry.getValue());

        if (size() == limit) {
          break;
        }
      }

      return;
    }

    for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
      if (size() == limit) {
        K lk = lastKey();

        // if the map is reached the limit and last key of this map is smaller than first key of
        // parameter map, no need to add remaining entries from parameter map.
        if (comparator().compare(lk, entry.getKey()) < 0) {
          return;
        } else {
          // remove last key to ensure free space before putting new entity
          remove(lk);
        }
      }

      // safe to directly call super.put
      super.put(entry.getKey(), entry.getValue());
    }
  }
}
