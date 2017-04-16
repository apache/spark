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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * A generic directive from the {@link org.apache.hadoop.mapred.JobTracker}
 * to the {@link org.apache.hadoop.mapred.TaskTracker} to take some 'action'. 
 * 
 */
abstract class TaskTrackerAction implements Writable {
  
  /**
   * Ennumeration of various 'actions' that the {@link JobTracker}
   * directs the {@link TaskTracker} to perform periodically.
   * 
   */
  public static enum ActionType {
    /** Launch a new task. */
    LAUNCH_TASK,
    
    /** Kill a task. */
    KILL_TASK,
    
    /** Kill any tasks of this job and cleanup. */
    KILL_JOB,
    
    /** Reinitialize the tasktracker. */
    REINIT_TRACKER,

    /** Ask a task to save its output. */
    COMMIT_TASK
  };
  
  /**
   * A factory-method to create objects of given {@link ActionType}. 
   * @param actionType the {@link ActionType} of object to create.
   * @return an object of {@link ActionType}.
   */
  public static TaskTrackerAction createAction(ActionType actionType) {
    TaskTrackerAction action = null;
    
    switch (actionType) {
    case LAUNCH_TASK:
      {
        action = new LaunchTaskAction();
      }
      break;
    case KILL_TASK:
      {
        action = new KillTaskAction();
      }
      break;
    case KILL_JOB:
      {
        action = new KillJobAction();
      }
      break;
    case REINIT_TRACKER:
      {
        action = new ReinitTrackerAction();
      }
      break;
    case COMMIT_TASK:
      {
        action = new CommitTaskAction();
      }
      break;
    }

    return action;
  }
  
  private ActionType actionType;
  
  protected TaskTrackerAction(ActionType actionType) {
    this.actionType = actionType;
  }
  
  /**
   * Return the {@link ActionType}.
   * @return the {@link ActionType}.
   */
  ActionType getActionId() {
    return actionType;
  }

  public void write(DataOutput out) throws IOException {
    WritableUtils.writeEnum(out, actionType);
  }
  
  public void readFields(DataInput in) throws IOException {
    actionType = WritableUtils.readEnum(in, ActionType.class);
  }
}
