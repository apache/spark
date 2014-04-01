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
package org.apache.hadoop.tools.rumen;

import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapred.TaskStatus.State;

/**
 * {@link TaskAttemptInfo} is a collection of statistics about a particular
 * task-attempt gleaned from job-history of the job.
 */
public abstract class TaskAttemptInfo {
  protected final State state;
  protected final TaskInfo taskInfo;

  protected TaskAttemptInfo(State state, TaskInfo taskInfo) {
    if (state == State.SUCCEEDED || state == State.FAILED) {
      this.state = state;
    } else {
      throw new IllegalArgumentException("status cannot be " + state);
    }
    this.taskInfo = taskInfo;
  }

  /**
   * Get the final {@link TaskStatus.State} of the task-attempt.
   * 
   * @return the final <code>State</code> of the task-attempt
   */
  public State getRunState() {
    return state;
  }

  /**
   * Get the total runtime for the task-attempt.
   * 
   * @return the total runtime for the task-attempt
   */
  public abstract long getRuntime();

  /**
   * Get the {@link TaskInfo} for the given task-attempt.
   * 
   * @return the <code>TaskInfo</code> for the given task-attempt
   */
  public TaskInfo getTaskInfo() {
    return taskInfo;
  }
}
