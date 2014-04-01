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

/**
 * A {@link GSet} is set,
 * which supports the {@link #get(Object)} operation.
 * The {@link #get(Object)} operation uses a key to lookup an element.
 * 
 * Null element is not supported.
 * 
 * @param <K> The type of the keys.
 * @param <E> The type of the elements, which must be a subclass of the keys.
 */
public interface GSet<K, E extends K> extends Iterable<E> {
  /**
   * @return The size of this set.
   */
  int size();

  /**
   * Does this set contain an element corresponding to the given key?
   * @param key The given key.
   * @return true if the given key equals to a stored element.
   *         Otherwise, return false.
   * @throws NullPointerException if key == null.
   */
  boolean contains(K key);

  /**
   * Return the stored element which is equal to the given key.
   * This operation is similar to {@link java.util.Map#get(Object)}.
   * @param key The given key.
   * @return The stored element if it exists.
   *         Otherwise, return null.
   * @throws NullPointerException if key == null.
   */
  E get(K key);

  /**
   * Add/replace an element.
   * If the element does not exist, add it to the set.
   * Otherwise, replace the existing element.
   *
   * Note that this operation
   * is similar to {@link java.util.Map#put(Object, Object)}
   * but is different from {@link java.util.Set#add(Object)}
   * which does not replace the existing element if there is any.
   *
   * @param element The element being put.
   * @return the previous stored element if there is any.
   *         Otherwise, return null.
   * @throws NullPointerException if element == null.
   */
  E put(E element);

  /**
   * Remove the element corresponding to the given key. 
   * This operation is similar to {@link java.util.Map#remove(Object)}.
   * @param key The key of the element being removed.
   * @return If such element exists, return it.
   *         Otherwise, return null. 
    * @throws NullPointerException if key == null.
  */
  E remove(K key);
}