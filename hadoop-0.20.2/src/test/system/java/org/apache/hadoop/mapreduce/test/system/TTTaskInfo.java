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

package org.apache.hadoop.mapreduce.test.system;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapred.TaskTracker;

/**
 * Task state information as seen by the TT.
 */
public interface TTTaskInfo extends Writable {

  /**
   * Has task occupied a slot? A task occupies a slot once it starts localizing
   * on the {@link TaskTracker} <br/>
   * 
   * @return true if task has started occupying a slot.
   */
  boolean slotTaken();

  /**
   * Has the task been killed? <br/>
   * 
   * @return true, if task has been killed.
   */
  boolean wasKilled();

  /**
   * Gets the task status associated with the particular task trackers task 
   * view.<br/>
   * 
   * @return status of the particular task
   */
  TaskStatus getTaskStatus();
  
  /**
   * Gets the configuration object of the task.
   * @return
   */
  Configuration getConf();
  
  /**
   * Gets the user of the task.
   * @return
   */
  String getUser();
  
  /**
   * Provides information as to whether the task is a cleanup of task.
   * @return true if it is a clean up of task.
   */
  boolean isTaskCleanupTask();

  /**
   * Gets the pid of the running task on the task-tracker.
   * 
   * @return pid of the task.
   */
  String getPid();
}
