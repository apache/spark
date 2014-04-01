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

import org.apache.hadoop.mapred.TaskTrackerStatus;
import org.apache.hadoop.mapreduce.test.system.TTInfo;

/**
 * Concrete implementation of the TaskTracker information which is passed to 
 * the client from JobTracker.
 * Look at {@link TTInfo}
 */

class TTInfoImpl implements TTInfo {

  private String taskTrackerName;
  private TaskTrackerStatus status;

  public TTInfoImpl() {
    taskTrackerName = "";
    status = new TaskTrackerStatus();
  }
  
  public TTInfoImpl(String taskTrackerName, TaskTrackerStatus status) {
    super();
    this.taskTrackerName = taskTrackerName;
    this.status = status;
  }

  @Override
  public String getName() {
    return taskTrackerName;
  }

  @Override
  public TaskTrackerStatus getStatus() {
    return status;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    taskTrackerName = in.readUTF();
    status.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(taskTrackerName);
    status.write(out);
  }

}
