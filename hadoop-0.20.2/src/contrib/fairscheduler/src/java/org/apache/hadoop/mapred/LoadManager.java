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

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * A pluggable object that manages the load on each {@link TaskTracker}, telling
 * the {@link TaskScheduler} when it can launch new tasks. 
 */
public abstract class LoadManager implements Configurable {
  protected Configuration conf;
  protected TaskTrackerManager taskTrackerManager;
  protected FairSchedulerEventLog schedulingLog;
  
  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public synchronized void setTaskTrackerManager(
      TaskTrackerManager taskTrackerManager) {
    this.taskTrackerManager = taskTrackerManager;
  }

  public void setEventLog(FairSchedulerEventLog schedulingLog) {
    this.schedulingLog = schedulingLog;
  }
 
  /**
   * Lifecycle method to allow the LoadManager to start any work in separate
   * threads.
   */
  public void start() throws IOException {
    // do nothing
  }
  
  /**
   * Lifecycle method to allow the LoadManager to stop any work it is doing.
   */
  public void terminate() throws IOException {
    // do nothing
  }
  
  /**
   * Can a given {@link TaskTracker} run another map task?
   * @param tracker The machine we wish to run a new map on
   * @param totalRunnableMaps Set of running jobs in the cluster
   * @param totalMapSlots The total number of map slots in the cluster
   * @param alreadyAssigned the number of maps already assigned to
   *        this tracker during this heartbeat
   * @return true if another map can be launched on <code>tracker</code>
   */
  public abstract boolean canAssignMap(TaskTrackerStatus tracker,
      int totalRunnableMaps, int totalMapSlots, int alreadyAssigned);

  /**
   * Can a given {@link TaskTracker} run another reduce task?
   * @param tracker The machine we wish to run a new map on
   * @param totalRunnableReduces Set of running jobs in the cluster
   * @param totalReduceSlots The total number of reduce slots in the cluster
   * @param alreadyAssigned the number of reduces already assigned to
   *        this tracker during this heartbeat
   * @return true if another reduce can be launched on <code>tracker</code>
   */
  public abstract boolean canAssignReduce(TaskTrackerStatus tracker,
      int totalRunnableReduces, int totalReduceSlots, int alreadyAssigned);
}
