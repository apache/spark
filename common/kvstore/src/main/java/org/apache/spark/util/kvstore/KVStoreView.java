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

import com.google.common.base.Preconditions;

import org.apache.spark.annotation.Private;

/**
 * A configurable view that allows iterating over values in a {@link KVStore}.
 *
 * <p>
 * The different methods can be used to configure the behavior of the iterator. Calling the same
 * method multiple times is allowed; the most recent value will be used.
 * </p>
 *
 * <p>
 * The iterators returned by this view are of type {@link KVStoreIterator}; they auto-close
 * when used in a for loop that exhausts their contents, but when used manually, they need
 * to be closed explicitly unless all elements are read.
 * </p>
 */
@Private
public abstract class KVStoreView<T> implements Iterable<T> {

  boolean ascending = true;
  String index = KVIndex.NATURAL_INDEX_NAME;
  Object first = null;
  Object last = null;
  Object parent = null;
  long skip = 0L;
  long max = Long.MAX_VALUE;

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
   * Defines the value of the parent index when iterating over a child index. Only elements that
   * match the parent index's value will be included in the iteration.
   *
   * <p>
   * Required for iterating over child indices, will generate an error if iterating over a
   * parent-less index.
   * </p>
   */
  public KVStoreView<T> parent(Object value) {
    this.parent = value;
    return this;
  }

  /**
   * Iterates starting at the given value of the chosen index (inclusive).
   */
  public KVStoreView<T> first(Object value) {
    this.first = value;
    return this;
  }

  /**
   * Stops iteration at the given value of the chosen index (inclusive).
   */
  public KVStoreView<T> last(Object value) {
    this.last = value;
    return this;
  }

  /**
   * Stops iteration after a number of elements has been retrieved.
   */
  public KVStoreView<T> max(long max) {
    Preconditions.checkArgument(max > 0L, "max must be positive.");
    this.max = max;
    return this;
  }

  /**
   * Skips a number of elements at the start of iteration. Skipped elements are not accounted
   * when using {@link #max(long)}.
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
