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
package org.apache.hadoop.mapred.join;

import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * This defines an interface to a stateful Iterator that can replay elements
 * added to it directly.
 * Note that this does not extend {@link java.util.Iterator}.
 */
public interface ResetableIterator<T extends Writable> {

  public static class EMPTY<U extends Writable>
    implements ResetableIterator<U> {
    public boolean hasNext() { return false; }
    public void reset() { }
    public void close() throws IOException { }
    public void clear() { }
    public boolean next(U val) throws IOException {
      return false;
    }
    public boolean replay(U val) throws IOException {
      return false;
    }
    public void add(U item) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * True if a call to next may return a value. This is permitted false
   * positives, but not false negatives.
   */
  public boolean hasNext();

  /**
   * Assign next value to actual.
   * It is required that elements added to a ResetableIterator be returned in
   * the same order after a call to {@link #reset} (FIFO).
   *
   * Note that a call to this may fail for nested joins (i.e. more elements
   * available, but none satisfying the constraints of the join)
   */
  public boolean next(T val) throws IOException;

  /**
   * Assign last value returned to actual.
   */
  public boolean replay(T val) throws IOException;

  /**
   * Set iterator to return to the start of its range. Must be called after
   * calling {@link #add} to avoid a ConcurrentModificationException.
   */
  public void reset();

  /**
   * Add an element to the collection of elements to iterate over.
   */
  public void add(T item) throws IOException;

  /**
   * Close datasources and release resources. Calling methods on the iterator
   * after calling close has undefined behavior.
   */
  // XXX is this necessary?
  public void close() throws IOException;

  /**
   * Close datasources, but do not release internal resources. Calling this
   * method should permit the object to be reused with a different datasource.
   */
  public void clear();

}
