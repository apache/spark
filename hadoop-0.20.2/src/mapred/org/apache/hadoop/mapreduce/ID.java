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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * A general identifier, which internally stores the id
 * as an integer. This is the super class of {@link JobID}, 
 * {@link TaskID} and {@link TaskAttemptID}.
 * 
 * @see JobID
 * @see TaskID
 * @see TaskAttemptID
 */
public abstract class ID implements WritableComparable<ID> {
  protected static final char SEPARATOR = '_';
  protected int id;

  /** constructs an ID object from the given int */
  public ID(int id) {
    this.id = id;
  }

  protected ID() {
  }

  /** returns the int which represents the identifier */
  public int getId() {
    return id;
  }

  @Override
  public String toString() {
    return String.valueOf(id);
  }

  @Override
  public int hashCode() {
    return Integer.valueOf(id).hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if(o == null)
      return false;
    if (o.getClass() == this.getClass()) {
      ID that = (ID) o;
      return this.id == that.id;
    }
    else
      return false;
  }

  /** Compare IDs by associated numbers */
  public int compareTo(ID that) {
    return this.id - that.id;
  }

  public void readFields(DataInput in) throws IOException {
    this.id = in.readInt();
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(id);
  }
  
}
