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
package org.apache.hadoop.hdfs.util;

import java.util.HashMap;
import java.util.Iterator;

/**
 * A {@link GSet} implementation by {@link HashMap}.
 */
public class GSetByHashMap<K, E extends K> implements GSet<K, E> {
  private final HashMap<K, E> m;

  public GSetByHashMap(int initialCapacity, float loadFactor) {
    m = new HashMap<K, E>(initialCapacity, loadFactor);
  }

  @Override
  public int size() {
    return m.size();
  }

  @Override
  public boolean contains(K k) {
    return m.containsKey(k);
  }

  @Override
  public E get(K k) {
    return m.get(k);
  }

  @Override
  public E put(E element) {
    if (element == null) {
      throw new UnsupportedOperationException("Null element is not supported.");
    }
    return m.put(element, element);
  }

  @Override
  public E remove(K k) {
    return m.remove(k);
  }

  @Override
  public Iterator<E> iterator() {
    return m.values().iterator();
  }
}
