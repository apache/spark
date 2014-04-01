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

/**
 * A {@link TaskSelector} implementation that wraps around the default
 * {@link JobInProgress#obtainNewMapTask(TaskTrackerStatus, int)} and
 * {@link JobInProgress#obtainNewReduceTask(TaskTrackerStatus, int)} methods
 * in {@link JobInProgress}, using the default Hadoop locality and speculative
 * threshold algorithms.
 */
public class DefaultTaskSelector extends TaskSelector {

  @Override
  public int neededSpeculativeMaps(JobInProgress job) {
    int count = 0;
    long time = System.currentTimeMillis();
    double avgProgress = job.getStatus().mapProgress();
    for (TaskInProgress tip: job.maps) {
      if (tip.isRunning() && tip.hasSpeculativeTask(time, avgProgress)) {
        count++;
      }
    }
    return count;
  }

  @Override
  public int neededSpeculativeReduces(JobInProgress job) {
    int count = 0;
    long time = System.currentTimeMillis();
    double avgProgress = job.getStatus().reduceProgress();
    for (TaskInProgress tip: job.reduces) {
      if (tip.isRunning() && tip.hasSpeculativeTask(time, avgProgress)) {
        count++;
      }
    }
    return count;
  }

  @Override
  public Task obtainNewMapTask(TaskTrackerStatus taskTracker, JobInProgress job,
      int localityLevel) throws IOException {
    ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
    int numTaskTrackers = clusterStatus.getTaskTrackers();
    return job.obtainNewMapTask(taskTracker, numTaskTrackers,
        taskTrackerManager.getNumberOfUniqueHosts(), localityLevel);
  }

  @Override
  public Task obtainNewReduceTask(TaskTrackerStatus taskTracker, JobInProgress job)
      throws IOException {
    ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
    int numTaskTrackers = clusterStatus.getTaskTrackers();
    return job.obtainNewReduceTask(taskTracker, numTaskTrackers,
        taskTrackerManager.getNumberOfUniqueHosts());
  }

}
