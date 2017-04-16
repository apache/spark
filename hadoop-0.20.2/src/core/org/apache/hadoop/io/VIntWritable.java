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

/** A WritableComparable for integer values stored in variable-length format.
 * Such values take between one and five bytes.  Smaller values take fewer bytes.
 * 
 * @see org.apache.hadoop.io.WritableUtils#readVInt(DataInput)
 */
public class VIntWritable implements WritableComparable {
  private int value;

  public VIntWritable() {}

  public VIntWritable(int value) { set(value); }

  /** Set the value of this VIntWritable. */
  public void set(int value) { this.value = value; }

  /** Return the value of this VIntWritable. */
  public int get() { return value; }

  public void readFields(DataInput in) throws IOException {
    value = WritableUtils.readVInt(in);
  }

  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, value);
  }

  /** Returns true iff <code>o</code> is a VIntWritable with the same value. */
  public boolean equals(Object o) {
    if (!(o instanceof VIntWritable))
      return false;
    VIntWritable other = (VIntWritable)o;
    return this.value == other.value;
  }

  public int hashCode() {
    return value;
  }

  /** Compares two VIntWritables. */
  public int compareTo(Object o) {
    int thisValue = this.value;
    int thatValue = ((VIntWritable)o).value;
    return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
  }

  public String toString() {
    return Integer.toString(value);
  }

}

