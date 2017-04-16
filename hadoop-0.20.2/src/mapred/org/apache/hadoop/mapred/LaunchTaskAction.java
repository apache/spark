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

/**
 * Represents a directive from the {@link org.apache.hadoop.mapred.JobTracker} 
 * to the {@link org.apache.hadoop.mapred.TaskTracker} to launch a new task.
 * 
 */
class LaunchTaskAction extends TaskTrackerAction {
  private Task task;

  public LaunchTaskAction() {
    super(ActionType.LAUNCH_TASK);
  }
  
  public LaunchTaskAction(Task task) {
    super(ActionType.LAUNCH_TASK);
    this.task = task;
  }
  
  public Task getTask() {
    return task;
  }
  
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(task.isMapTask());
    task.write(out);
  }
  
  public void readFields(DataInput in) throws IOException {
    boolean isMapTask = in.readBoolean();
    if (isMapTask) {
      task = new MapTask();
    } else {
      task = new ReduceTask();
    }
    task.readFields(in);
  }

}
