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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable for Double values.
 */
public class DoubleWritable implements WritableComparable {

  private double value = 0.0;
  
  public DoubleWritable() {
    
  }
  
  public DoubleWritable(double value) {
    set(value);
  }
  
  public void readFields(DataInput in) throws IOException {
    value = in.readDouble();
  }

  public void write(DataOutput out) throws IOException {
    out.writeDouble(value);
  }
  
  public void set(double value) { this.value = value; }
  
  public double get() { return value; }

  /**
   * Returns true iff <code>o</code> is a DoubleWritable with the same value.
   */
  public boolean equals(Object o) {
    if (!(o instanceof DoubleWritable)) {
      return false;
    }
    DoubleWritable other = (DoubleWritable)o;
    return this.value == other.value;
  }
  
  public int hashCode() {
    return (int)Double.doubleToLongBits(value);
  }
  
  public int compareTo(Object o) {
    DoubleWritable other = (DoubleWritable)o;
    return (value < other.value ? -1 : (value == other.value ? 0 : 1));
  }
  
  public String toString() {
    return Double.toString(value);
  }

  /** A Comparator optimized for DoubleWritable. */ 
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(DoubleWritable.class);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      double thisValue = readDouble(b1, s1);
      double thatValue = readDouble(b2, s2);
      return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(DoubleWritable.class, new Comparator());
  }

}

