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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

class LevelDBFilterItertor<T> implements KVStoreIterator<T>  {

  /** The LevelDBIterator being used */
  private final LevelDBIterator<T> iterator;
  private final Predicate<? super T> predicate;

  /** The next object in the iteration */
  private T nextObject;
  /** Whether the next object has been calculated yet */
  private boolean nextObjectSet = false;

  LevelDBFilterItertor(LevelDBIterator<T> iterator, Predicate<T> predicate) {
    this.iterator = iterator;
    this.predicate = predicate;
  }

  /**
   * Returns true if the underlying iterator contains an object that
   * matches the predicate.
   */
  @Override
  public boolean hasNext() {
    return nextObjectSet || setNextObject();
  }

  /**
   * Returns the next object that matches the predicate.
   */
  @Override
  public T next() {
    if (!nextObjectSet && !setNextObject()) {
      throw new NoSuchElementException();
    }
    nextObjectSet = false;
    return nextObject;
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
    return iterator.skip(n);
  }

  @Override
  public void close() throws IOException {
    iterator.close();
  }

  @Override
  public void remove() {
    iterator.remove();
  }

  /**
   * Set nextObject to the next object. If there are no more
   * objects then return false. Otherwise, return true.
   */
  private boolean setNextObject() {
    while (iterator.hasNext()) {
      final T object = iterator.next();
      if (predicate.test(object)) {
        nextObject = object;
        nextObjectSet = true;
        return true;
      }
    }
    return false;
  }
}
