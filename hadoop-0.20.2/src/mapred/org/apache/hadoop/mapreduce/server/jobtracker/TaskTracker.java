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
package org.apache.hadoop.mapreduce.server.jobtracker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.TaskTrackerStatus;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskType;

/**
 * The representation of a single <code>TaskTracker</code> as seen by 
 * the {@link JobTracker}.
 */
public class TaskTracker {
  static final Log LOG = LogFactory.getLog(TaskTracker.class);
  
  final private String trackerName;
  private TaskTrackerStatus status;

  private JobInProgress jobForFallowMapSlot;
  private JobInProgress jobForFallowReduceSlot;
  
  /**
   * Create a new {@link TaskTracker}.
   * @param trackerName Unique identifier for the <code>TaskTracker</code>
   */
  public TaskTracker(String trackerName) {
    this.trackerName = trackerName;
  }

  /**
   * Get the unique identifier for the {@link TaskTracker}
   * @return the unique identifier for the <code>TaskTracker</code>
   */
  public String getTrackerName() {
    return trackerName;
  }

  /**
   * Get the current {@link TaskTrackerStatus} of the <code>TaskTracker</code>.
   * @return the current <code>TaskTrackerStatus</code> of the 
   *         <code>TaskTracker</code>
   */
  public TaskTrackerStatus getStatus() {
    return status;
  }

  /**
   * Set the current {@link TaskTrackerStatus} of the <code>TaskTracker</code>.
   * @param status the current <code>TaskTrackerStatus</code> of the 
   *               <code>TaskTracker</code>
   */
  public void setStatus(TaskTrackerStatus status) {
    this.status = status;
  }

  /**
   * Get the number of currently available slots on this tasktracker for the 
   * given type of the task.
   * @param taskType the {@link TaskType} to check for number of available slots 
   * @return the number of currently available slots for the given 
   *         <code>taskType</code>
   */
  public int getAvailableSlots(TaskType taskType) {
    int availableSlots = 0;
    if (taskType == TaskType.MAP) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(trackerName + " getAvailSlots:" +
        		     " max(m)=" + status.getMaxMapSlots() + 
        		     " occupied(m)=" + status.countOccupiedMapSlots());
      }
      availableSlots = status.getAvailableMapSlots();
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug(trackerName + " getAvailSlots:" +
                  " max(r)=" + status.getMaxReduceSlots() + 
                  " occupied(r)=" + status.countOccupiedReduceSlots());
      }
      availableSlots = status.getAvailableReduceSlots();
    }
    return availableSlots;
  }
  
  /**
   * Get the {@link JobInProgress} for which the fallow slot(s) are held.
   * @param taskType {@link TaskType} of the task
   * @return the task for which the fallow slot(s) are held, 
   *         <code>null</code> if there are no fallow slots
   */
  public JobInProgress getJobForFallowSlot(TaskType taskType) {
    return 
      (taskType == TaskType.MAP) ? jobForFallowMapSlot : jobForFallowReduceSlot;
  }

  /**
   * Reserve specified number of slots for a given <code>job</code>.
   * @param taskType {@link TaskType} of the task
   * @param job the job for which slots on this <code>TaskTracker</code>
   *             are to be reserved
   * @param numSlots number of slots to be reserved
   */
  public void reserveSlots(TaskType taskType, JobInProgress job, int numSlots) {
    JobID jobId = job.getJobID();
    if (taskType == TaskType.MAP) {
      if (jobForFallowMapSlot != null && 
          !jobForFallowMapSlot.getJobID().equals(jobId)) {
        throw new RuntimeException(trackerName + " already has " + 
                                   "slots reserved for " + 
                                   jobForFallowMapSlot + "; being"  +
                                   " asked to reserve " + numSlots + " for " + 
                                   jobId);
      }

      jobForFallowMapSlot = job;
    } else if (taskType == TaskType.REDUCE){
      if (jobForFallowReduceSlot != null && 
          !jobForFallowReduceSlot.getJobID().equals(jobId)) {
        throw new RuntimeException(trackerName + " already has " + 
                                   "slots reserved for " + 
                                   jobForFallowReduceSlot + "; being"  +
                                   " asked to reserve " + numSlots + " for " + 
                                   jobId);
      }

      jobForFallowReduceSlot = job;
    }
    
    job.reserveTaskTracker(this, taskType, numSlots);
    LOG.info(trackerName + ": Reserved " + numSlots + " " + taskType + 
             " slots for " + jobId);
  }
  
  /**
   * Free map slots on this <code>TaskTracker</code> which were reserved for 
   * <code>taskType</code>.
   * @param taskType {@link TaskType} of the task
   * @param job job whose slots are being un-reserved
   */
  public void unreserveSlots(TaskType taskType, JobInProgress job) {
    JobID jobId = job.getJobID();
    if (taskType == TaskType.MAP) {
      if (jobForFallowMapSlot == null || 
          !jobForFallowMapSlot.getJobID().equals(jobId)) {
        throw new RuntimeException(trackerName + " already has " + 
                                   "slots reserved for " + 
                                   jobForFallowMapSlot + "; being"  +
                                   " asked to un-reserve for " + jobId);
      }

      jobForFallowMapSlot = null;
    } else {
      if (jobForFallowReduceSlot == null || 
          !jobForFallowReduceSlot.getJobID().equals(jobId)) {
        throw new RuntimeException(trackerName + " already has " + 
                                   "slots reserved for " + 
                                   jobForFallowReduceSlot + "; being"  +
                                   " asked to un-reserve for " + jobId);
      }
      
      jobForFallowReduceSlot = null;
    }
    
    job.unreserveTaskTracker(this, taskType);
    LOG.info(trackerName + ": Unreserved " + taskType + " slots for " + jobId);
  }
  
  /**
   * Cleanup when the {@link TaskTracker} is declared as 'lost/blacklisted'
   * by the JobTracker.
   * 
   * The method assumes that the lock on the {@link JobTracker} is obtained
   * by the caller.
   */
  public void cancelAllReservations() {
    // Inform jobs which have reserved slots on this tasktracker
    if (jobForFallowMapSlot != null) {
      unreserveSlots(TaskType.MAP, jobForFallowMapSlot);
    }
    if (jobForFallowReduceSlot != null) {
      unreserveSlots(TaskType.REDUCE, jobForFallowReduceSlot);
    }
  }
}
