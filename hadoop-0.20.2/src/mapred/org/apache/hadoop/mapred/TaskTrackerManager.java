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
import java.util.Collection;

/**
 * Manages information about the {@link TaskTracker}s running on a cluster.
 * This interface exits primarily to test the {@link JobTracker}, and is not
 * intended to be implemented by users.
 */
interface TaskTrackerManager {

  /**
   * @return A collection of the {@link TaskTrackerStatus} for the tasktrackers
   * being managed.
   */
  public Collection<TaskTrackerStatus> taskTrackers();
  
  /**
   * @return The number of unique hosts running tasktrackers.
   */
  public int getNumberOfUniqueHosts();
  
  /**
   * @return a summary of the cluster's status.
   */
  public ClusterStatus getClusterStatus();

  /**
   * Registers a {@link JobInProgressListener} for updates from this
   * {@link TaskTrackerManager}.
   * @param jobInProgressListener the {@link JobInProgressListener} to add
   */
  public void addJobInProgressListener(JobInProgressListener listener);

  /**
   * Unregisters a {@link JobInProgressListener} from this
   * {@link TaskTrackerManager}.
   * @param jobInProgressListener the {@link JobInProgressListener} to remove
   */
  public void removeJobInProgressListener(JobInProgressListener listener);

  /**
   * Return the {@link QueueManager} which manages the queues in this
   * {@link TaskTrackerManager}.
   *
   * @return the {@link QueueManager}
   */
  public QueueManager getQueueManager();
  
  /**
   * Return the current heartbeat interval that's used by {@link TaskTracker}s.
   *
   * @return the heartbeat interval used by {@link TaskTracker}s
   */
  public int getNextHeartbeatInterval();

  /**
   * Kill the job identified by jobid
   * 
   * @param jobid
   * @throws IOException
   */
  public void killJob(JobID jobid)
      throws IOException;

  /**
   * Obtain the job object identified by jobid
   * 
   * @param jobid
   * @return jobInProgress object
   */
  public JobInProgress getJob(JobID jobid);
  
  /**
   * Initialize the Job
   * 
   * @param job JobInProgress object
   */
  public void initJob(JobInProgress job);
  
  /**
   * Fail a job.
   * 
   * @param job JobInProgress object
   */
  public void failJob(JobInProgress job);

  /**
   * Mark the task attempt identified by taskid to be killed
   * 
   * @param taskid task to kill
   * @param shouldFail whether to count the task as failed
   * @return true if the task was found and successfully marked to kill
   */
  public boolean killTask(TaskAttemptID taskid, boolean shouldFail)
      throws IOException;
}
