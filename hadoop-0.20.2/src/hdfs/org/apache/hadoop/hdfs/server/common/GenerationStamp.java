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
package org.apache.hadoop.hdfs.server.common;

import java.io.*;
import org.apache.hadoop.io.*;

/****************************************************************
 * A GenerationStamp is a Hadoop FS primitive, identified by a long.
 ****************************************************************/
public class GenerationStamp implements WritableComparable<GenerationStamp> {
  public static final long WILDCARD_STAMP = 1;
  public static final long FIRST_VALID_STAMP = 1000L;

  static {                                      // register a ctor
    WritableFactories.setFactory
      (GenerationStamp.class,
       new WritableFactory() {
         public Writable newInstance() { return new GenerationStamp(0); }
       });
  }

  long genstamp;

  /**
   * Create a new instance, initialized to FIRST_VALID_STAMP.
   */
  public GenerationStamp() {this(GenerationStamp.FIRST_VALID_STAMP);}

  /**
   * Create a new instance, initialized to the specified value.
   */
  GenerationStamp(long stamp) {this.genstamp = stamp;}

  /**
   * Returns the current generation stamp
   */
  public long getStamp() {
    return this.genstamp;
  }

  /**
   * Sets the current generation stamp
   */
  public void setStamp(long stamp) {
    this.genstamp = stamp;
  }

  /**
   * First increments the counter and then returns the stamp 
   */
  public synchronized long nextStamp() {
    this.genstamp++;
    return this.genstamp;
  }

  /////////////////////////////////////
  // Writable
  /////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    out.writeLong(genstamp);
  }

  public void readFields(DataInput in) throws IOException {
    this.genstamp = in.readLong();
    if (this.genstamp < 0) {
      throw new IOException("Bad Generation Stamp: " + this.genstamp);
    }
  }

  /////////////////////////////////////
  // Comparable
  /////////////////////////////////////
  public static int compare(long x, long y) {
    return x < y? -1: x == y? 0: 1;
  }

  /** {@inheritDoc} */
  public int compareTo(GenerationStamp that) {
    return compare(this.genstamp, that.genstamp);
  }

  /** {@inheritDoc} */
  public boolean equals(Object o) {
    if (!(o instanceof GenerationStamp)) {
      return false;
    }
    return genstamp == ((GenerationStamp)o).genstamp;
  }

  public static boolean equalsWithWildcard(long x, long y) {
    return x == y || x == WILDCARD_STAMP || y == WILDCARD_STAMP;  
  }

  /** {@inheritDoc} */
  public int hashCode() {
    return 37 * 17 + (int) (genstamp^(genstamp>>>32));
  }
}
