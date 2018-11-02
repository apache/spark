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

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.annotation.Private;

/**
 * An iterator for KVStore.
 *
 * <p>
 * Iterators may keep references to resources that need to be closed. It's recommended that users
 * explicitly close iterators after they're used.
 * </p>
 */
@Private
public interface KVStoreIterator<T> extends Iterator<T>, Closeable {

  /**
   * Retrieve multiple elements from the store.
   *
   * @param max Maximum number of elements to retrieve.
   */
  List<T> next(int max);

  /**
   * Skip in the iterator.
   *
   * @return Whether there are items left after skipping.
   */
  boolean skip(long n);

}
