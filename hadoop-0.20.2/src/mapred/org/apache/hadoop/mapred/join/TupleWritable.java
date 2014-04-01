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

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Writable type storing multiple {@link org.apache.hadoop.io.Writable}s.
 *
 * This is *not* a general-purpose tuple type. In almost all cases, users are
 * encouraged to implement their own serializable types, which can perform
 * better validation and provide more efficient encodings than this class is
 * capable. TupleWritable relies on the join framework for type safety and
 * assumes its instances will rarely be persisted, assumptions not only
 * incompatible with, but contrary to the general case.
 *
 * @see org.apache.hadoop.io.Writable
 */
public class TupleWritable implements Writable, Iterable<Writable> {

  private long written;
  private Writable[] values;

  /**
   * Create an empty tuple with no allocated storage for writables.
   */
  public TupleWritable() { }

  /**
   * Initialize tuple with storage; unknown whether any of them contain
   * &quot;written&quot; values.
   */
  public TupleWritable(Writable[] vals) {
    written = 0L;
    values = vals;
  }

  /**
   * Return true if tuple has an element at the position provided.
   */
  public boolean has(int i) {
    return 0 != ((1L << i) & written);
  }

  /**
   * Get ith Writable from Tuple.
   */
  public Writable get(int i) {
    return values[i];
  }

  /**
   * The number of children in this Tuple.
   */
  public int size() {
    return values.length;
  }

  /**
   * {@inheritDoc}
   */
  public boolean equals(Object other) {
    if (other instanceof TupleWritable) {
      TupleWritable that = (TupleWritable)other;
      if (this.size() != that.size() || this.written != that.written) {
        return false;
      }
      for (int i = 0; i < values.length; ++i) {
        if (!has(i)) continue;
        if (!values[i].equals(that.get(i))) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  public int hashCode() {
    assert false : "hashCode not designed";
    return (int)written;
  }

  /**
   * Return an iterator over the elements in this tuple.
   * Note that this doesn't flatten the tuple; one may receive tuples
   * from this iterator.
   */
  public Iterator<Writable> iterator() {
    final TupleWritable t = this;
    return new Iterator<Writable>() {
      long i = written;
      long last = 0L;
      public boolean hasNext() {
        return 0L != i;
      }
      public Writable next() {
        last = Long.lowestOneBit(i);
        if (0 == last)
          throw new NoSuchElementException();
        i ^= last;
        // numberOfTrailingZeros rtn 64 if lsb set
        return t.get(Long.numberOfTrailingZeros(last) % 64);
      }
      public void remove() {
        t.written ^= last;
        if (t.has(Long.numberOfTrailingZeros(last))) {
          throw new IllegalStateException("Attempt to remove non-existent val");
        }
      }
    };
  }

  /**
   * Convert Tuple to String as in the following.
   * <tt>[<child1>,<child2>,...,<childn>]</tt>
   */
  public String toString() {
    StringBuffer buf = new StringBuffer("[");
    for (int i = 0; i < values.length; ++i) {
      buf.append(has(i) ? values[i].toString() : "");
      buf.append(",");
    }
    if (values.length != 0)
      buf.setCharAt(buf.length() - 1, ']');
    else
      buf.append(']');
    return buf.toString();
  }

  // Writable

  /** Writes each Writable to <code>out</code>.
   * TupleWritable format:
   * {@code
   *  <count><type1><type2>...<typen><obj1><obj2>...<objn>
   * }
   */
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, values.length);
    WritableUtils.writeVLong(out, written);
    for (int i = 0; i < values.length; ++i) {
      Text.writeString(out, values[i].getClass().getName());
    }
    for (int i = 0; i < values.length; ++i) {
      if (has(i)) {
        values[i].write(out);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked") // No static typeinfo on Tuples
  public void readFields(DataInput in) throws IOException {
    int card = WritableUtils.readVInt(in);
    values = new Writable[card];
    written = WritableUtils.readVLong(in);
    Class<? extends Writable>[] cls = new Class[card];
    try {
      for (int i = 0; i < card; ++i) {
        cls[i] = Class.forName(Text.readString(in)).asSubclass(Writable.class);
      }
      for (int i = 0; i < card; ++i) {
          values[i] = cls[i].newInstance();
        if (has(i)) {
          values[i].readFields(in);
        }
      }
    } catch (ClassNotFoundException e) {
      throw (IOException)new IOException("Failed tuple init").initCause(e);
    } catch (IllegalAccessException e) {
      throw (IOException)new IOException("Failed tuple init").initCause(e);
    } catch (InstantiationException e) {
      throw (IOException)new IOException("Failed tuple init").initCause(e);
    }
  }

  /**
   * Record that the tuple contains an element at the position provided.
   */
  void setWritten(int i) {
    written |= 1L << i;
  }

  /**
   * Record that the tuple does not contain an element at the position
   * provided.
   */
  void clearWritten(int i) {
    written &= -1 ^ (1L << i);
  }

  /**
   * Clear any record of which writables have been written to, without
   * releasing storage.
   */
  void clearWritten() {
    written = 0L;
  }

}
