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
import java.util.PriorityQueue;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Base class for Composite join returning values derived from multiple
 * sources, but generally not tuples.
 */
public abstract class MultiFilterRecordReader<K extends WritableComparable,
                                              V extends Writable>
    extends CompositeRecordReader<K,V,V>
    implements ComposableRecordReader<K,V> {

  private Class<? extends Writable> valueclass;
  private TupleWritable ivalue;

  public MultiFilterRecordReader(int id, JobConf conf, int capacity,
      Class<? extends WritableComparator> cmpcl) throws IOException {
    super(id, capacity, cmpcl);
    setConf(conf);
  }

  /**
   * For each tuple emitted, return a value (typically one of the values
   * in the tuple).
   * Modifying the Writables in the tuple is permitted and unlikely to affect
   * join behavior in most cases, but it is not recommended. It's safer to
   * clone first.
   */
  protected abstract V emit(TupleWritable dst) throws IOException;

  /**
   * Default implementation offers {@link #emit} every Tuple from the
   * collector (the outer join of child RRs).
   */
  protected boolean combine(Object[] srcs, TupleWritable dst) {
    return true;
  }

  /** {@inheritDoc} */
  public boolean next(K key, V value) throws IOException {
    if (jc.flush(ivalue)) {
      WritableUtils.cloneInto(key, jc.key());
      WritableUtils.cloneInto(value, emit(ivalue));
      return true;
    }
    jc.clear();
    K iterkey = createKey();
    final PriorityQueue<ComposableRecordReader<K,?>> q = getRecordReaderQueue();
    while (!q.isEmpty()) {
      fillJoinCollector(iterkey);
      jc.reset(iterkey);
      if (jc.flush(ivalue)) {
        WritableUtils.cloneInto(key, jc.key());
        WritableUtils.cloneInto(value, emit(ivalue));
        return true;
      }
      jc.clear();
    }
    return false;
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked") // Explicit check for value class agreement
  public V createValue() {
    if (null == valueclass) {
      final Class<?> cls = kids[0].createValue().getClass();
      for (RecordReader<K,? extends V> rr : kids) {
        if (!cls.equals(rr.createValue().getClass())) {
          throw new ClassCastException("Child value classes fail to agree");
        }
      }
      valueclass = cls.asSubclass(Writable.class);
      ivalue = createInternalValue();
    }
    return (V) ReflectionUtils.newInstance(valueclass, null);
  }

  /**
   * Return an iterator returning a single value from the tuple.
   * @see MultiFilterDelegationIterator
   */
  protected ResetableIterator<V> getDelegate() {
    return new MultiFilterDelegationIterator();
  }

  /**
   * Proxy the JoinCollector, but include callback to emit.
   */
  protected class MultiFilterDelegationIterator
      implements ResetableIterator<V> {

    public boolean hasNext() {
      return jc.hasNext();
    }

    public boolean next(V val) throws IOException {
      boolean ret;
      if (ret = jc.flush(ivalue)) {
        WritableUtils.cloneInto(val, emit(ivalue));
      }
      return ret;
    }

    public boolean replay(V val) throws IOException {
      WritableUtils.cloneInto(val, emit(ivalue));
      return true;
    }

    public void reset() {
      jc.reset(jc.key());
    }

    public void add(V item) throws IOException {
      throw new UnsupportedOperationException();
    }

    public void close() throws IOException {
      jc.close();
    }

    public void clear() {
      jc.clear();
    }
  }

}
