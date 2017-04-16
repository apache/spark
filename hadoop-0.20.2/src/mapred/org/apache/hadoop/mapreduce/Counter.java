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

package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * A named counter that tracks the progress of a map/reduce job.
 * 
 * <p><code>Counters</code> represent global counters, defined either by the 
 * Map-Reduce framework or applications. Each <code>Counter</code> is named by
 * an {@link Enum} and has a long for the value.</p>
 * 
 * <p><code>Counters</code> are bunched into Groups, each comprising of
 * counters from a particular <code>Enum</code> class. 
 */
public class Counter implements Writable {

  private String name;
  private String displayName;
  private long value = 0;
    
  protected Counter() { 
  }

  protected Counter(String name, String displayName) {
    this.name = name;
    this.displayName = displayName;
  }
  
  @Deprecated
  protected synchronized void setDisplayName(String displayName) {
    this.displayName = displayName;
  }
    
  /**
   * Read the binary representation of the counter
   */
  @Override
  public synchronized void readFields(DataInput in) throws IOException {
    name = Text.readString(in);
    if (in.readBoolean()) {
      displayName = Text.readString(in);
    } else {
      displayName = name;
    }
    value = WritableUtils.readVLong(in);
  }
    
  /**
   * Write the binary representation of the counter
   */
  @Override
  public synchronized void write(DataOutput out) throws IOException {
    Text.writeString(out, name);
    boolean distinctDisplayName = ! name.equals(displayName);
    out.writeBoolean(distinctDisplayName);
    if (distinctDisplayName) {
      Text.writeString(out, displayName);
    }
    WritableUtils.writeVLong(out, value);
  }

  public synchronized String getName() {
    return name;
  }

  /**
   * Get the name of the counter.
   * @return the user facing name of the counter
   */
  public synchronized String getDisplayName() {
    return displayName;
  }
    
  /**
   * What is the current value of this counter?
   * @return the current value
   */
  public synchronized long getValue() {
    return value;
  }
    
  /**
   * Set this counter by the given value
   * @param value the value to set
   */
  public synchronized void setValue(long value) {
    this.value = value;
  }

  /**
   * Increment this counter by the given value
   * @param incr the value to increase this counter by
   */
  public synchronized void increment(long incr) {
    value += incr;
  }

  @Override
  public synchronized boolean equals(Object genericRight) {
    if (genericRight instanceof Counter) {
      synchronized (genericRight) {
        Counter right = (Counter) genericRight;
        return name.equals(right.name) && 
               displayName.equals(right.displayName) &&
               value == right.value;
      }
    }
    return false;
  }
  
  @Override
  public synchronized int hashCode() {
    return name.hashCode() + displayName.hashCode();
  }
}
