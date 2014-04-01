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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;

/**
 * The context for task attempts.
 */
public class TaskAttemptContext extends JobContext implements Progressable {
  private final TaskAttemptID taskId;
  private String status = "";
  
  public TaskAttemptContext(Configuration conf, 
                            TaskAttemptID taskId) {
    super(conf, taskId.getJobID());
    this.taskId = taskId;
  }

  /**
   * Get the unique name for this task attempt.
   */
  public TaskAttemptID getTaskAttemptID() {
    return taskId;
  }

  /**
   * Set the current status of the task to the given string.
   */
  public void setStatus(String msg) throws IOException {
    status = msg;
  }

  /**
   * Get the last set status message.
   * @return the current status message
   */
  public String getStatus() {
    return status;
  }

  /**
   * Report progress. The subtypes actually do work in this method.
   */
  public void progress() { 
  }
}