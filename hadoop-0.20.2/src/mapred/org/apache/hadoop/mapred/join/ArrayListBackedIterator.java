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
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * This class provides an implementation of ResetableIterator. The
 * implementation uses an {@link java.util.ArrayList} to store elements
 * added to it, replaying them as requested.
 * Prefer {@link StreamBackedIterator}.
 */
public class ArrayListBackedIterator<X extends Writable>
    implements ResetableIterator<X> {

  private Iterator<X> iter;
  private ArrayList<X> data;
  private X hold = null;

  public ArrayListBackedIterator() {
    this(new ArrayList<X>());
  }

  public ArrayListBackedIterator(ArrayList<X> data) {
    this.data = data;
    this.iter = this.data.iterator();
  }

  public boolean hasNext() {
    return iter.hasNext();
  }

  public boolean next(X val) throws IOException {
    if (iter.hasNext()) {
      WritableUtils.cloneInto(val, iter.next());
      if (null == hold) {
        hold = WritableUtils.clone(val, null);
      } else {
        WritableUtils.cloneInto(hold, val);
      }
      return true;
    }
    return false;
  }

  public boolean replay(X val) throws IOException {
    WritableUtils.cloneInto(val, hold);
    return true;
  }

  public void reset() {
    iter = data.iterator();
  }

  public void add(X item) throws IOException {
    data.add(WritableUtils.clone(item, null));
  }

  public void close() throws IOException {
    iter = null;
    data = null;
  }

  public void clear() {
    data.clear();
    reset();
  }

}
