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

package org.apache.spark.kvstore;

import java.util.Iterator;
import java.util.Map;

import com.google.common.base.Preconditions;

/**
 * A configurable view that allows iterating over values in a {@link KVStore}.
 *
 * <p>
 * The different methods can be used to configure the behavior of the iterator. Calling the same
 * method multiple times is allowed; the most recent value will be used.
 * </p>
 *
 * <p>
 * The iterators returns by this view are of type {@link KVStoreIterator}; they auto-close
 * when used in a for loop that exhausts their contents, but when used manually, they need
 * to be closed explicitly unless all elements are read.
 * </p>
 */
public abstract class KVStoreView<T> implements Iterable<T> {

  final Class<T> type;

  boolean ascending = true;
  String index = KVIndex.NATURAL_INDEX_NAME;
  Object first = null;
  long skip = 0L;

  public KVStoreView(Class<T> type) {
    this.type = type;
  }

  /**
   * Reverses the order of iteration. By default, iterates in ascending order.
   */
  public KVStoreView<T> reverse() {
    ascending = !ascending;
    return this;
  }

  /**
   * Iterates according to the given index.
   */
  public KVStoreView<T> index(String name) {
    this.index = Preconditions.checkNotNull(name);
    return this;
  }

  /**
   * Iterates starting at the given value of the chosen index.
   */
  public KVStoreView<T> first(Object value) {
    this.first = value;
    return this;
  }

  /**
   * Skips a number of elements in the resulting iterator.
   */
  public KVStoreView<T> skip(long n) {
    this.skip = n;
    return this;
  }

  /**
   * Returns an iterator for the current configuration.
   */
  public KVStoreIterator<T> closeableIterator() throws Exception {
    return (KVStoreIterator<T>) iterator();
  }

}
