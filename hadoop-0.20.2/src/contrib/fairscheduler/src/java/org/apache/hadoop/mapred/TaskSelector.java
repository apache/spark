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
 * A pluggable object for selecting tasks to run from a {@link JobInProgress} on
 * a given {@link TaskTracker}, for use by the {@link TaskScheduler}. The
 * <code>TaskSelector</code> is in charge of managing both locality and
 * speculative execution. For the latter purpose, it must also provide counts of
 * how many tasks each speculative job needs to launch, so that the scheduler
 * can take this into account in its calculations.
 */
public abstract class TaskSelector implements Configurable {
  protected Configuration conf;
  protected TaskTrackerManager taskTrackerManager;
  
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
  
  /**
   * Lifecycle method to allow the TaskSelector to start any work in separate
   * threads.
   */
  public void start() throws IOException {
    // do nothing
  }
  
  /**
   * Lifecycle method to allow the TaskSelector to stop any work it is doing.
   */
  public void terminate() throws IOException {
    // do nothing
  }
  
  /**
   * How many speculative map tasks does the given job want to launch?
   * @param job The job to count speculative maps for
   * @return Number of speculative maps that can be launched for job
   */
  public abstract int neededSpeculativeMaps(JobInProgress job);

  /**
   * How many speculative reduce tasks does the given job want to launch?
   * @param job The job to count speculative reduces for
   * @return Number of speculative reduces that can be launched for job
   */
  public abstract int neededSpeculativeReduces(JobInProgress job);
  
  /**
   * Choose a map task to run from the given job on the given TaskTracker.
   * @param taskTracker {@link TaskTrackerStatus} of machine to run on
   * @param job Job to select a task for
   * @return A {@link Task} to run on the machine, or <code>null</code> if
   *         no map should be launched from this job on the task tracker.
   * @throws IOException 
   */
  public abstract Task obtainNewMapTask(TaskTrackerStatus taskTracker,
      JobInProgress job, int localityLevel) throws IOException;

  /**
   * Choose a reduce task to run from the given job on the given TaskTracker.
   * @param taskTracker {@link TaskTrackerStatus} of machine to run on
   * @param job Job to select a task for
   * @return A {@link Task} to run on the machine, or <code>null</code> if
   *         no reduce should be launched from this job on the task tracker.
   * @throws IOException 
   */
  public abstract Task obtainNewReduceTask(TaskTrackerStatus taskTracker,
      JobInProgress job) throws IOException;
}
