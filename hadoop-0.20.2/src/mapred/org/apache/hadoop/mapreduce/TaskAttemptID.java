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

/**
 * TaskAttemptID represents the immutable and unique identifier for 
 * a task attempt. Each task attempt is one particular instance of a Map or
 * Reduce Task identified by its TaskID. 
 * 
 * TaskAttemptID consists of 2 parts. First part is the 
 * {@link TaskID}, that this TaskAttemptID belongs to.
 * Second part is the task attempt number. <br> 
 * An example TaskAttemptID is : 
 * <code>attempt_200707121733_0003_m_000005_0</code> , which represents the
 * zeroth task attempt for the fifth map task in the third job 
 * running at the jobtracker started at <code>200707121733</code>.
 * <p>
 * Applications should never construct or parse TaskAttemptID strings
 * , but rather use appropriate constructors or {@link #forName(String)} 
 * method. 
 * 
 * @see JobID
 * @see TaskID
 */
public class TaskAttemptID extends org.apache.hadoop.mapred.ID {
  protected static final String ATTEMPT = "attempt";
  private TaskID taskId;
  
  /**
   * Constructs a TaskAttemptID object from given {@link TaskID}.  
   * @param taskId TaskID that this task belongs to  
   * @param id the task attempt number
   */
  public TaskAttemptID(TaskID taskId, int id) {
    super(id);
    if(taskId == null) {
      throw new IllegalArgumentException("taskId cannot be null");
    }
    this.taskId = taskId;
  }
  
  /**
   * Constructs a TaskId object from given parts.
   * @param jtIdentifier jobTracker identifier
   * @param jobId job number 
   * @param isMap whether the tip is a map 
   * @param taskId taskId number
   * @param id the task attempt number
   */
  public TaskAttemptID(String jtIdentifier, int jobId, boolean isMap, 
                       int taskId, int id) {
    this(new TaskID(jtIdentifier, jobId, isMap, taskId), id);
  }
  
  public TaskAttemptID() { 
    taskId = new TaskID();
  }
  
  /** Returns the {@link JobID} object that this task attempt belongs to */
  public JobID getJobID() {
    return taskId.getJobID();
  }
  
  /** Returns the {@link TaskID} object that this task attempt belongs to */
  public TaskID getTaskID() {
    return taskId;
  }
  
  /**Returns whether this TaskAttemptID is a map ID */
  public boolean isMap() {
    return taskId.isMap();
  }
  
  @Override
  public boolean equals(Object o) {
    if (!super.equals(o))
      return false;

    TaskAttemptID that = (TaskAttemptID)o;
    return this.taskId.equals(that.taskId);
  }
  
  /**
   * Add the unique string to the StringBuilder
   * @param builder the builder to append ot
   * @return the builder that was passed in.
   */
  protected StringBuilder appendTo(StringBuilder builder) {
    return taskId.appendTo(builder).append(SEPARATOR).append(id);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    taskId.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    taskId.write(out);
  }

  @Override
  public int hashCode() {
    return taskId.hashCode() * 5 + id;
  }
  
  /**Compare TaskIds by first tipIds, then by task numbers. */
  @Override
  public int compareTo(ID o) {
    TaskAttemptID that = (TaskAttemptID)o;
    int tipComp = this.taskId.compareTo(that.taskId);
    if(tipComp == 0) {
      return this.id - that.id;
    }
    else return tipComp;
  }
  @Override
  public String toString() { 
    return appendTo(new StringBuilder(ATTEMPT)).toString();
  }

  /** Construct a TaskAttemptID object from given string 
   * @return constructed TaskAttemptID object or null if the given String is null
   * @throws IllegalArgumentException if the given string is malformed
   */
  public static TaskAttemptID forName(String str
                                      ) throws IllegalArgumentException {
    if(str == null)
      return null;
    try {
      String[] parts = str.split(Character.toString(SEPARATOR));
      if(parts.length == 6) {
        if(parts[0].equals(ATTEMPT)) {
          boolean isMap = false;
          if(parts[3].equals("m")) isMap = true;
          else if(parts[3].equals("r")) isMap = false;
          else throw new Exception();
          return new org.apache.hadoop.mapred.TaskAttemptID
                       (parts[1],
                        Integer.parseInt(parts[2]),
                        isMap, Integer.parseInt(parts[4]), 
                        Integer.parseInt(parts[5]));
        }
      }
    } catch (Exception ex) {
      //fall below
    }
    throw new IllegalArgumentException("TaskAttemptId string : " + str 
        + " is not properly formed");
  }

}
