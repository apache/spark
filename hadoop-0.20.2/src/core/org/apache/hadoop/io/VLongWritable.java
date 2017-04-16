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

/** A WritableComparable for longs in a variable-length format. Such values take
 *  between one and five bytes.  Smaller values take fewer bytes.
 *  
 *  @see org.apache.hadoop.io.WritableUtils#readVLong(DataInput)
 */
public class VLongWritable implements WritableComparable {
  private long value;

  public VLongWritable() {}

  public VLongWritable(long value) { set(value); }

  /** Set the value of this LongWritable. */
  public void set(long value) { this.value = value; }

  /** Return the value of this LongWritable. */
  public long get() { return value; }

  public void readFields(DataInput in) throws IOException {
    value = WritableUtils.readVLong(in);
  }

  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVLong(out, value);
  }

  /** Returns true iff <code>o</code> is a VLongWritable with the same value. */
  public boolean equals(Object o) {
    if (!(o instanceof VLongWritable))
      return false;
    VLongWritable other = (VLongWritable)o;
    return this.value == other.value;
  }

  public int hashCode() {
    return (int)value;
  }

  /** Compares two VLongWritables. */
  public int compareTo(Object o) {
    long thisValue = this.value;
    long thatValue = ((VLongWritable)o).value;
    return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
  }

  public String toString() {
    return Long.toString(value);
  }

}

