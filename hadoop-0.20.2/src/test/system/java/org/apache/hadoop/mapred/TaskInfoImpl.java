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

import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;

/**
 * Concrete class to expose out the task related information to the Clients from
 * the JobTracker.
 * Look at {@link TaskInfo} for further details.
 */
class TaskInfoImpl implements TaskInfo {

  private double progress;
  private TaskID taskID;
  private int killedAttempts;
  private int failedAttempts;
  private int runningAttempts;
  private TaskStatus[] taskStatus;
  private boolean setupOrCleanup;
  private String[] taskTrackers;

  public TaskInfoImpl() {
    taskID = new TaskID();
  }

  public TaskInfoImpl(
      TaskID taskID, double progress, int runningAttempts, int killedAttempts,
      int failedAttempts, TaskStatus[] taskStatus,
      boolean setupOrCleanup, String[] taskTrackers) {
    this.progress = progress;
    this.taskID = taskID;
    this.killedAttempts = killedAttempts;
    this.failedAttempts = failedAttempts;
    this.runningAttempts = runningAttempts;
    if (taskStatus != null) {
      this.taskStatus = taskStatus;
    } else {
      if (taskID.isMap()) {
        this.taskStatus = new MapTaskStatus[] {};
      } else {
        this.taskStatus = new ReduceTaskStatus[] {};
      }
    }
    this.setupOrCleanup = setupOrCleanup;
    this.taskTrackers = taskTrackers;
  }

  @Override
  public double getProgress() {
    return progress;
  }

  @Override
  public TaskID getTaskID() {
    return taskID;
  }

  @Override
  public int numKilledAttempts() {
    return killedAttempts;
  }

  @Override
  public int numFailedAttempts() {
    return failedAttempts;
  }

  @Override
  public int numRunningAttempts() {
    return runningAttempts;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    taskID.readFields(in);
    progress = in.readDouble();
    runningAttempts = in.readInt();
    killedAttempts = in.readInt();
    failedAttempts = in.readInt();
    int size = in.readInt();
    if (taskID.isMap()) {
      taskStatus = new MapTaskStatus[size];
    }
    else {
      taskStatus = new ReduceTaskStatus[size];
    }
    for (int i = 0; i < size; i++) {
      if (taskID.isMap()) {
        taskStatus[i] = new MapTaskStatus();
      }
      else {
        taskStatus[i] = new ReduceTaskStatus();
      }
      taskStatus[i].readFields(in);
      taskStatus[i].setTaskTracker(in.readUTF());
    }
    setupOrCleanup = in.readBoolean();
    size = in.readInt();
    taskTrackers = new String[size];
    for(int i=0; i < size ; i++) {
      taskTrackers[i] = in.readUTF();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    taskID.write(out);
    out.writeDouble(progress);
    out.writeInt(runningAttempts);
    out.writeInt(killedAttempts);
    out.writeInt(failedAttempts);
    out.writeInt(taskStatus.length);
    for (TaskStatus t : taskStatus) {
      t.write(out);
      out.writeUTF(t.getTaskTracker());
    }
    out.writeBoolean(setupOrCleanup);
    out.writeInt(taskTrackers.length);
    for(String tt : taskTrackers) {
      out.writeUTF(tt);
    }
  }

  @Override
  public TaskStatus[] getTaskStatus() {
    return taskStatus;
  }

  @Override
  public boolean isSetupOrCleanup() {
    return setupOrCleanup;
  }

  @Override
  public String[] getTaskTrackers() {
    return taskTrackers;
  }
}
