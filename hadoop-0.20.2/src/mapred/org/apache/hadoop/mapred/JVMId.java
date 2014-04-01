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

package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.NumberFormat;

class JVMId extends ID {
  boolean isMap;
  JobID jobId;
  private static final String JVM = "jvm";
  private static NumberFormat idFormat = NumberFormat.getInstance();
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(6);
  }
  
  public JVMId(JobID jobId, boolean isMap, int id) {
    super(id);
    this.isMap = isMap;
    this.jobId = jobId;
  }
  
  public JVMId (String jtIdentifier, int jobId, boolean isMap, int id) {
    this(new JobID(jtIdentifier, jobId), isMap, id);
  }
    
  public JVMId() { 
    jobId = new JobID();
  }
  
  public boolean isMapJVM() {
    return isMap;
  }
  public JobID getJobId() {
    return jobId;
  }
  public boolean equals(Object o) {
    if(o == null)
      return false;
    if(o.getClass().equals(JVMId.class)) {
      JVMId that = (JVMId)o;
      return this.id==that.id
        && this.isMap == that.isMap
        && this.jobId.equals(that.jobId);
    }
    else return false;
  }

  /**Compare TaskInProgressIds by first jobIds, then by tip numbers. Reduces are 
   * defined as greater then maps.*/
  @Override
  public int compareTo(org.apache.hadoop.mapreduce.ID o) {
    JVMId that = (JVMId)o;
    int jobComp = this.jobId.compareTo(that.jobId);
    if(jobComp == 0) {
      if(this.isMap == that.isMap) {
        return this.id - that.id;
      } else {
        return this.isMap ? -1 : 1;
      }
    } else {
      return jobComp;
    }
  }
  
  @Override
  public String toString() { 
    return appendTo(new StringBuilder(JVM)).toString();
  }

  /**
   * Add the unique id to the given StringBuilder.
   * @param builder the builder to append to
   * @return the passed in builder.
   */
  protected StringBuilder appendTo(StringBuilder builder) {
    return jobId.appendTo(builder).
                 append(SEPARATOR).
                 append(isMap ? 'm' : 'r').
                 append(SEPARATOR).
                 append(idFormat.format(id));
  }
  
  @Override
  public int hashCode() {
    return jobId.hashCode() * 11 + id;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.jobId.readFields(in);
    this.isMap = in.readBoolean();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    jobId.write(out);
    out.writeBoolean(isMap);
  }
  
  /** Construct a JVMId object from given string 
   * @return constructed JVMId object or null if the given String is null
   * @throws IllegalArgumentException if the given string is malformed
   */
  public static JVMId forName(String str) 
    throws IllegalArgumentException {
    if(str == null)
      return null;
    try {
      String[] parts = str.split("_");
      if(parts.length == 5) {
        if(parts[0].equals(JVM)) {
          boolean isMap = false;
          if(parts[3].equals("m")) isMap = true;
          else if(parts[3].equals("r")) isMap = false;
          else throw new Exception();
          return new JVMId(parts[1], Integer.parseInt(parts[2]),
              isMap, Integer.parseInt(parts[4]));
        }
      }
    }catch (Exception ex) {//fall below
    }
    throw new IllegalArgumentException("TaskId string : " + str 
        + " is not properly formed");
  }

}
