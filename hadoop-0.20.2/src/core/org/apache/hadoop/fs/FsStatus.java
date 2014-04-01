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
package org.apache.hadoop.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/** This class is used to represent the capacity, free and used space on a
  * {@link FileSystem}.
  */
public class FsStatus implements Writable {
  private long capacity;
  private long used;
  private long remaining;

  /** Construct a FsStatus object, using the specified statistics */
  public FsStatus(long capacity, long used, long remaining) {
    this.capacity = capacity;
    this.used = used;
    this.remaining = remaining;
  }

  /** Return the capacity in bytes of the file system */
  public long getCapacity() {
    return capacity;
  }

  /** Return the number of bytes used on the file system */
  public long getUsed() {
    return used;
  }

  /** Return the number of remaining bytes on the file system */
  public long getRemaining() {
    return remaining;
  }

  //////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    out.writeLong(capacity);
    out.writeLong(used);
    out.writeLong(remaining);
  }

  public void readFields(DataInput in) throws IOException {
    capacity = in.readLong();
    used = in.readLong();
    remaining = in.readLong();
  }
}
