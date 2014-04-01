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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskType;

class MemoryMatcher {

  private static final Log LOG = LogFactory.getLog(MemoryMatcher.class);
  private CapacityTaskScheduler scheduler;

  public MemoryMatcher(CapacityTaskScheduler capacityTaskScheduler) {
    this.scheduler = capacityTaskScheduler;
  }

  boolean isSchedulingBasedOnMemEnabled() {
    if (scheduler.getLimitMaxMemForMapSlot()
                                  == JobConf.DISABLED_MEMORY_LIMIT
        || scheduler.getLimitMaxMemForReduceSlot()
                                  == JobConf.DISABLED_MEMORY_LIMIT
        || scheduler.getMemSizeForMapSlot()
                                  == JobConf.DISABLED_MEMORY_LIMIT
        || scheduler.getMemSizeForReduceSlot()
                                  == JobConf.DISABLED_MEMORY_LIMIT) {
      return false;
    }
    return true;
  }

  
  /**
   * Find the memory that is already used by all the running tasks
   * residing on the given TaskTracker.
   * 
   * @param taskTracker
   * @param taskType 
   * @param availableSlots
   * @return amount of memory that is used by the residing tasks,
   *          null if memory cannot be computed for some reason.
   */
  synchronized long getMemReservedForTasks(
      TaskTrackerStatus taskTracker, TaskType taskType, int availableSlots) {
    int currentlyScheduled = 
      currentlyScheduled(taskTracker, taskType, availableSlots);
    long vmem = 0;

    for (TaskStatus task : taskTracker.getTaskReports()) {
      // the following task states are one in which the slot is
      // still occupied and hence memory of the task should be
      // accounted in used memory.
      if ((task.getRunState() == TaskStatus.State.RUNNING) ||
          (task.getRunState() == TaskStatus.State.UNASSIGNED) ||
          (task.inTaskCleanupPhase())) {
        // Get the memory "allotted" for this task based on number of slots
        long myVmem = 0;
        if (task.getIsMap() && taskType == TaskType.MAP) {
          long memSizePerMapSlot = scheduler.getMemSizeForMapSlot(); 
          myVmem = 
            memSizePerMapSlot * task.getNumSlots();
        } else if (!task.getIsMap()
            && taskType == TaskType.REDUCE) {
          long memSizePerReduceSlot = scheduler.getMemSizeForReduceSlot(); 
          myVmem = memSizePerReduceSlot * task.getNumSlots();
        }
        vmem += myVmem;
      }
    }

    long currentlyScheduledVMem = 
      currentlyScheduled * ((taskType == TaskType.MAP) ? 
          scheduler.getMemSizeForMapSlot() : 
            scheduler.getMemSizeForReduceSlot());
    return vmem + currentlyScheduledVMem; 
  }

  private int currentlyScheduled(TaskTrackerStatus taskTracker, 
                                 TaskType taskType, int availableSlots) {
    int scheduled = 0;
    if (taskType == TaskType.MAP) {
      scheduled = 
        (taskTracker.getMaxMapSlots() - taskTracker.countOccupiedMapSlots()) - 
            availableSlots;
    } else {
      scheduled = 
        (taskTracker.getMaxReduceSlots() - 
            taskTracker.countOccupiedReduceSlots()) - availableSlots;
    }
    return scheduled;
  }
  /**
   * Check if a TT has enough memory to run of task specified from this job.
   * @param job
   * @param taskType 
   * @param taskTracker
   * @param availableSlots
   * @return true if this TT has enough memory for this job. False otherwise.
   */
  boolean matchesMemoryRequirements(JobInProgress job,TaskType taskType, 
                                    TaskTrackerStatus taskTracker, 
                                    int availableSlots) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Matching memory requirements of " + job.getJobID().toString()
        + " for scheduling on " + taskTracker.trackerName);
    }

    if (!isSchedulingBasedOnMemEnabled()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Scheduling based on job's memory requirements is disabled."
          + " Ignoring any value set by job.");
      }
      return true;
    }

    long memUsedOnTT = 
      getMemReservedForTasks(taskTracker, taskType, availableSlots);
    long totalMemUsableOnTT = 0;
    long memForThisTask = 0;
    if (taskType == TaskType.MAP) {
      memForThisTask = job.getMemoryForMapTask();
      totalMemUsableOnTT =
          scheduler.getMemSizeForMapSlot() * taskTracker.getMaxMapSlots();
    } else if (taskType == TaskType.REDUCE) {
      memForThisTask = job.getMemoryForReduceTask();
      totalMemUsableOnTT =
          scheduler.getMemSizeForReduceSlot()
              * taskTracker.getMaxReduceSlots();
    }

    long freeMemOnTT = totalMemUsableOnTT - memUsedOnTT;
    if (memForThisTask > freeMemOnTT) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("memForThisTask (" + memForThisTask + ") > freeMemOnTT ("
                  + freeMemOnTT + "). A " + taskType + " task from "
                  + job.getJobID().toString() + " cannot be scheduled on TT "
                  + taskTracker.trackerName);
      }
      return false;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("memForThisTask = " + memForThisTask + ". freeMemOnTT = "
                + freeMemOnTT + ". A " + taskType.toString() + " task from "
                + job.getJobID().toString() + " matches memory requirements "
                + "on TT "+ taskTracker.trackerName);
    }
    return true;
  }
}
