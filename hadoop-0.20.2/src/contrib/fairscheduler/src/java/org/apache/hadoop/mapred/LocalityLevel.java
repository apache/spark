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

/**
 * Represents the level of data-locality at which a job in the fair scheduler
 * is allowed to launch tasks. By default, jobs are not allowed to launch
 * non-data-local tasks until they have waited a small number of seconds to
 * find a slot on a node that they have data on. If a job has waited this
 * long, it is allowed to launch rack-local tasks as well (on nodes that may
 * not have the task's input data, but share a rack with a node that does).
 * Finally, after a further wait, jobs are allowed to launch tasks anywhere
 * in the cluster.
 * 
 * This enum defines three levels - NODE, RACK and ANY (for allowing tasks
 * to be launched on any node). A map task's level can be obtained from
 * its job through {@link #fromTask(JobInProgress, Task, TaskTrackerStatus)}. In
 * addition, for any locality level, it is possible to get a "level cap" to pass
 * to {@link JobInProgress#obtainNewMapTask(TaskTrackerStatus, int, int, int)}
 * to ensure that only tasks at this level or lower are launched, through
 * the {@link #toCacheLevelCap()} method.
 */
public enum LocalityLevel {
  NODE, RACK, ANY;
  
  public static LocalityLevel fromTask(JobInProgress job, Task mapTask,
      TaskTrackerStatus tracker) {
    TaskID tipID = mapTask.getTaskID().getTaskID();
    TaskInProgress tip = job.getTaskInProgress(tipID);
    switch (job.getLocalityLevel(tip, tracker)) {
    case 0: return LocalityLevel.NODE;
    case 1: return LocalityLevel.RACK;
    default: return LocalityLevel.ANY;
    }
  }
  
  /**
   * Obtain a JobInProgress cache level cap to pass to
   * {@link JobInProgress#obtainNewMapTask(TaskTrackerStatus, int, int, int)}
   * to ensure that only tasks of this locality level and lower are launched.
   */
  public int toCacheLevelCap() {
    switch(this) {
    case NODE: return 1;
    case RACK: return 2;
    default: return Integer.MAX_VALUE;
    }
  }
}
