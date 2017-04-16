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

package org.apache.hadoop.io;

import java.io.*;

/** A WritableComparable for floats. */
public class FloatWritable implements WritableComparable {
  private float value;

  public FloatWritable() {}

  public FloatWritable(float value) { set(value); }

  /** Set the value of this FloatWritable. */
  public void set(float value) { this.value = value; }

  /** Return the value of this FloatWritable. */
  public float get() { return value; }

  public void readFields(DataInput in) throws IOException {
    value = in.readFloat();
  }

  public void write(DataOutput out) throws IOException {
    out.writeFloat(value);
  }

  /** Returns true iff <code>o</code> is a FloatWritable with the same value. */
  public boolean equals(Object o) {
    if (!(o instanceof FloatWritable))
      return false;
    FloatWritable other = (FloatWritable)o;
    return this.value == other.value;
  }

  public int hashCode() {
    return Float.floatToIntBits(value);
  }

  /** Compares two FloatWritables. */
  public int compareTo(Object o) {
    float thisValue = this.value;
    float thatValue = ((FloatWritable)o).value;
    return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
  }

  public String toString() {
    return Float.toString(value);
  }

  /** A Comparator optimized for FloatWritable. */ 
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(FloatWritable.class);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      float thisValue = readFloat(b1, s1);
      float thatValue = readFloat(b2, s2);
      return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(FloatWritable.class, new Comparator());
  }

}

