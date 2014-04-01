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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Proxy class for a RecordReader participating in the join framework.
 * This class keeps track of the &quot;head&quot; key-value pair for the
 * provided RecordReader and keeps a store of values matching a key when
 * this source is participating in a join.
 */
public class WrappedRecordReader<K extends WritableComparable,
                          U extends Writable>
    implements ComposableRecordReader<K,U> {

  private boolean empty = false;
  private RecordReader<K,U> rr;
  private int id;  // index at which values will be inserted in collector

  private K khead; // key at the top of this RR
  private U vhead; // value assoc with khead
  private WritableComparator cmp;

  private ResetableIterator<U> vjoin;

  /**
   * For a given RecordReader rr, occupy position id in collector.
   */
  WrappedRecordReader(int id, RecordReader<K,U> rr,
      Class<? extends WritableComparator> cmpcl) throws IOException {
    this.id = id;
    this.rr = rr;
    khead = rr.createKey();
    vhead = rr.createValue();
    try {
      cmp = (null == cmpcl)
        ? WritableComparator.get(khead.getClass())
        : cmpcl.newInstance();
    } catch (InstantiationException e) {
      throw (IOException)new IOException().initCause(e);
    } catch (IllegalAccessException e) {
      throw (IOException)new IOException().initCause(e);
    }
    vjoin = new StreamBackedIterator<U>();
    next();
  }

  /** {@inheritDoc} */
  public int id() {
    return id;
  }

  /**
   * Return the key at the head of this RR.
   */
  public K key() {
    return khead;
  }

  /**
   * Clone the key at the head of this RR into the object supplied.
   */
  public void key(K qkey) throws IOException {
    WritableUtils.cloneInto(qkey, khead);
  }

  /**
   * Return true if the RR- including the k,v pair stored in this object-
   * is exhausted.
   */
  public boolean hasNext() {
    return !empty;
  }

  /**
   * Skip key-value pairs with keys less than or equal to the key provided.
   */
  public void skip(K key) throws IOException {
    if (hasNext()) {
      while (cmp.compare(khead, key) <= 0 && next());
    }
  }

  /**
   * Read the next k,v pair into the head of this object; return true iff
   * the RR and this are exhausted.
   */
  protected boolean next() throws IOException {
    empty = !rr.next(khead, vhead);
    return hasNext();
  }

  /**
   * Add an iterator to the collector at the position occupied by this
   * RecordReader over the values in this stream paired with the key
   * provided (ie register a stream of values from this source matching K
   * with a collector).
   */
                                 // JoinCollector comes from parent, which has
  @SuppressWarnings("unchecked") // no static type for the slot this sits in
  public void accept(CompositeRecordReader.JoinCollector i, K key)
      throws IOException {
    vjoin.clear();
    if (0 == cmp.compare(key, khead)) {
      do {
        vjoin.add(vhead);
      } while (next() && 0 == cmp.compare(key, khead));
    }
    i.add(id, vjoin);
  }

  /**
   * Write key-value pair at the head of this stream to the objects provided;
   * get next key-value pair from proxied RR.
   */
  public boolean next(K key, U value) throws IOException {
    if (hasNext()) {
      WritableUtils.cloneInto(key, khead);
      WritableUtils.cloneInto(value, vhead);
      next();
      return true;
    }
    return false;
  }

  /**
   * Request new key from proxied RR.
   */
  public K createKey() {
    return rr.createKey();
  }

  /**
   * Request new value from proxied RR.
   */
  public U createValue() {
    return rr.createValue();
  }

  /**
   * Request progress from proxied RR.
   */
  public float getProgress() throws IOException {
    return rr.getProgress();
  }

  /**
   * Request position from proxied RR.
   */
  public long getPos() throws IOException {
    return rr.getPos();
  }

  /**
   * Forward close request to proxied RR.
   */
  public void close() throws IOException {
    rr.close();
  }

  /**
   * Implement Comparable contract (compare key at head of proxied RR
   * with that of another).
   */
  public int compareTo(ComposableRecordReader<K,?> other) {
    return cmp.compare(key(), other.key());
  }

  /**
   * Return true iff compareTo(other) retn true.
   */
  @SuppressWarnings("unchecked") // Explicit type check prior to cast
  public boolean equals(Object other) {
    return other instanceof ComposableRecordReader
        && 0 == compareTo((ComposableRecordReader)other);
  }

  public int hashCode() {
    assert false : "hashCode not designed";
    return 42;
  }

}
