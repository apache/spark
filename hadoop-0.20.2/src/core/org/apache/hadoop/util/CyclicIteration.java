/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.util;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;

/** Provide an cyclic {@link Iterator} for a {@link NavigableMap}.
 * The {@link Iterator} navigates the entries of the map
 * according to the map's ordering.
 * If the {@link Iterator} hits the last entry of the map,
 * it will then continue from the first entry.
 */
public class CyclicIteration<K, V> implements Iterable<Map.Entry<K, V>> {
  private final NavigableMap<K, V> navigablemap;
  private final NavigableMap<K, V> tailmap;

  /** Construct an {@link Iterable} object,
   * so that an {@link Iterator} can be created  
   * for iterating the given {@link NavigableMap}.
   * The iteration begins from the starting key exclusively.
   */
  public CyclicIteration(NavigableMap<K, V> navigablemap, K startingkey) {
    if (navigablemap == null || navigablemap.isEmpty()) {
      this.navigablemap = null;
      this.tailmap = null;
    }
    else {
      this.navigablemap = navigablemap;
      this.tailmap = navigablemap.tailMap(startingkey, false); 
    }
  }

  /** {@inheritDoc} */
  public Iterator<Map.Entry<K, V>> iterator() {
    return new CyclicIterator();
  }

  /** An {@link Iterator} for {@link CyclicIteration}. */
  private class CyclicIterator implements Iterator<Map.Entry<K, V>> {
    private boolean hasnext;
    private Iterator<Map.Entry<K, V>> i;
    /** The first entry to begin. */
    private final Map.Entry<K, V> first;
    /** The next entry. */
    private Map.Entry<K, V> next;
    
    private CyclicIterator() {
      hasnext = navigablemap != null;
      if (hasnext) {
        i = tailmap.entrySet().iterator();
        first = nextEntry();
        next = first;
      }
      else {
        i = null;
        first = null;
        next = null;
      }
    }

    private Map.Entry<K, V> nextEntry() {
      if (!i.hasNext()) {
        i = navigablemap.entrySet().iterator();
      }
      return i.next();
    }

    /** {@inheritDoc} */
    public boolean hasNext() {
      return hasnext;
    }

    /** {@inheritDoc} */
    public Map.Entry<K, V> next() {
      if (!hasnext) {
        throw new NoSuchElementException();
      }

      final Map.Entry<K, V> curr = next;
      next = nextEntry();
      hasnext = !next.equals(first);
      return curr;
    }

    /** Not supported */
    public void remove() {
      throw new UnsupportedOperationException("Not supported");
    }
  }
}