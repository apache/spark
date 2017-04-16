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

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskTracker;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;
import org.apache.hadoop.util.ProcessTree;
import org.apache.hadoop.util.ProcfsBasedProcessTree;
import org.apache.hadoop.util.StringUtils;

/**
 * Manages memory usage of tasks running under this TT. Kills any task-trees
 * that overflow and over-step memory limits.
 */
class TaskMemoryManagerThread extends Thread {

  private static Log LOG = LogFactory.getLog(TaskMemoryManagerThread.class);

  private TaskTracker taskTracker;
  private long monitoringInterval;

  private long maxMemoryAllowedForAllTasks;
  private long maxRssMemoryAllowedForAllTasks;

  private Map<TaskAttemptID, ProcessTreeInfo> processTreeInfoMap;
  private Map<TaskAttemptID, ProcessTreeInfo> tasksToBeAdded;
  private List<TaskAttemptID> tasksToBeRemoved;

  private static final String MEMORY_USAGE_STRING =
    "Memory usage of ProcessTree %s for task-id %s : Virtual %d bytes, " +
      "limit : %d bytes; Physical %d bytes, limit %d bytes";
  
  public TaskMemoryManagerThread(TaskTracker taskTracker) {
    
    this(taskTracker.getTotalMemoryAllottedForTasksOnTT() * 1024 * 1024L,
      taskTracker.getJobConf().getLong(
        "mapred.tasktracker.taskmemorymanager.monitoring-interval", 
        5000L));

    this.taskTracker = taskTracker;
    long reservedRssMemory = taskTracker.getReservedPhysicalMemoryOnTT();
    long totalPhysicalMemoryOnTT = taskTracker.getTotalPhysicalMemoryOnTT();
    if (reservedRssMemory == JobConf.DISABLED_MEMORY_LIMIT ||
        totalPhysicalMemoryOnTT == JobConf.DISABLED_MEMORY_LIMIT) {
      maxRssMemoryAllowedForAllTasks = JobConf.DISABLED_MEMORY_LIMIT;
      LOG.info("Physical memory monitoring disabled");
    } else {
      maxRssMemoryAllowedForAllTasks =
                totalPhysicalMemoryOnTT - reservedRssMemory;
      if (maxRssMemoryAllowedForAllTasks < 0) {
        maxRssMemoryAllowedForAllTasks = JobConf.DISABLED_MEMORY_LIMIT;
        LOG.warn("Reserved physical memory exceeds total. Physical memory " +
            "monitoring disabled.");
      } else {
        LOG.info(String.format("Physical memory monitoring enabled. " +
            "System total: %s. Reserved: %s. Maximum: %s.",
            totalPhysicalMemoryOnTT, reservedRssMemory,
            maxRssMemoryAllowedForAllTasks));
      }
    }
  }

  // mainly for test purposes. note that the tasktracker variable is
  // not set here.
  TaskMemoryManagerThread(long maxMemoryAllowedForAllTasks,
                            long monitoringInterval) {
    setName(this.getClass().getName());

    processTreeInfoMap = new HashMap<TaskAttemptID, ProcessTreeInfo>();
    tasksToBeAdded = new HashMap<TaskAttemptID, ProcessTreeInfo>();
    tasksToBeRemoved = new ArrayList<TaskAttemptID>();

    this.maxMemoryAllowedForAllTasks = maxMemoryAllowedForAllTasks < 0 ?
        JobConf.DISABLED_MEMORY_LIMIT : maxMemoryAllowedForAllTasks;

    this.monitoringInterval = monitoringInterval;
  }

  public void addTask(TaskAttemptID tid, long memLimit, long memLimitPhysical) {
    synchronized (tasksToBeAdded) {
      LOG.debug("Tracking ProcessTree " + tid + " for the first time");
      ProcessTreeInfo ptInfo =
        new ProcessTreeInfo(tid, null, null, memLimit, memLimitPhysical);
      tasksToBeAdded.put(tid, ptInfo);
    }
  }

  public void removeTask(TaskAttemptID tid) {
    synchronized (tasksToBeRemoved) {
      tasksToBeRemoved.add(tid);
    }
  }

  private static class ProcessTreeInfo {
    private TaskAttemptID tid;
    private String pid;
    private ProcfsBasedProcessTree pTree;
    private long memLimit;
    private long memLimitPhysical;
    private String pidFile;

    public ProcessTreeInfo(TaskAttemptID tid, String pid,
        ProcfsBasedProcessTree pTree, long memLimit, long memLimitPhysical) {
      this.tid = tid;
      this.pid = pid;
      this.pTree = pTree;
      this.memLimit = memLimit;
      this.memLimitPhysical = memLimitPhysical;
    }

    public TaskAttemptID getTID() {
      return tid;
    }

    public String getPID() {
      return pid;
    }

    public void setPid(String pid) {
      this.pid = pid;
    }

    public ProcfsBasedProcessTree getProcessTree() {
      return pTree;
    }

    public void setProcessTree(ProcfsBasedProcessTree pTree) {
      this.pTree = pTree;
    }

    public long getMemLimit() {
      return memLimit;
    }

    /**
     * @return Physical memory limit for the process tree in bytes
     */
    public long getMemLimitPhysical() {
      return memLimitPhysical;
    }
  }

  @Override
  public void run() {

    LOG.info("Starting thread: " + this.getClass());

    while (true) {
      // Print the processTrees for debugging.
      if (LOG.isDebugEnabled()) {
        StringBuffer tmp = new StringBuffer("[ ");
        for (ProcessTreeInfo p : processTreeInfoMap.values()) {
          tmp.append(p.getPID());
          tmp.append(" ");
        }
        LOG.debug("Current ProcessTree list : "
            + tmp.substring(0, tmp.length()) + "]");
      }

      //Add new Tasks
      synchronized (tasksToBeAdded) {
        processTreeInfoMap.putAll(tasksToBeAdded);
        tasksToBeAdded.clear();
      }

      //Remove finished Tasks
      synchronized (tasksToBeRemoved) {
        for (TaskAttemptID tid : tasksToBeRemoved) {
          processTreeInfoMap.remove(tid);
        }
        tasksToBeRemoved.clear();
      }

      long memoryStillInUsage = 0;
      long rssMemoryStillInUsage = 0;
      // Now, check memory usage and kill any overflowing tasks
      for (Iterator<Map.Entry<TaskAttemptID, ProcessTreeInfo>> it = processTreeInfoMap
          .entrySet().iterator(); it.hasNext();) {
        Map.Entry<TaskAttemptID, ProcessTreeInfo> entry = it.next();
        TaskAttemptID tid = entry.getKey();
        ProcessTreeInfo ptInfo = entry.getValue();
        try {
          String pId = ptInfo.getPID();

          // Initialize any uninitialized processTrees
          if (pId == null) {
            // get pid from taskAttemptId
            pId = taskTracker.getPid(ptInfo.getTID());
            if (pId != null) {
              // PID will be null, either if the pid file is yet to be created
              // or if the tip is finished and we removed pidFile, but the TIP
              // itself is still retained in runningTasks till successful
              // transmission to JT

              ProcfsBasedProcessTree pt = 
                new ProcfsBasedProcessTree(pId, ProcessTree.isSetsidAvailable);
              LOG.debug("Tracking ProcessTree " + pId + " for the first time");

              ptInfo.setPid(pId);
              ptInfo.setProcessTree(pt);
            }
          }
          // End of initializing any uninitialized processTrees

          if (pId == null) {
            continue; // processTree cannot be tracked
          }

          if (taskTracker.getRunningTask(tid).wasKilled()) {
            continue; // this task has been killed already
          }

          LOG.debug("Constructing ProcessTree for : PID = " + pId + " TID = "
              + tid);
          ProcfsBasedProcessTree pTree = ptInfo.getProcessTree();
          pTree = pTree.getProcessTree(); // get the updated process-tree
          ptInfo.setProcessTree(pTree); // update ptInfo with proces-tree of
          // updated state
          long currentMemUsage = pTree.getCumulativeVmem();
          long currentRssMemUsage = pTree.getCumulativeRssmem();
          // as processes begin with an age 1, we want to see if there 
          // are processes more than 1 iteration old.
          long curMemUsageOfAgedProcesses = pTree.getCumulativeVmem(1);
          long curRssMemUsageOfAgedProcesses = pTree.getCumulativeRssmem(1);
          long limit = ptInfo.getMemLimit();
          long limitPhysical = ptInfo.getMemLimitPhysical();
          LOG.info(String.format(MEMORY_USAGE_STRING, 
                                pId, tid.toString(), currentMemUsage, limit,
                                currentRssMemUsage, limitPhysical));
          
          boolean isMemoryOverLimit = false;
          String msg = "";
          if (doCheckVirtualMemory() &&
              isProcessTreeOverLimit(tid.toString(), currentMemUsage, 
                                      curMemUsageOfAgedProcesses, limit)) {
            // Task (the root process) is still alive and overflowing memory.
            // Dump the process-tree and then clean it up.
            msg = "TaskTree [pid=" + pId + ",tipID=" + tid
                    + "] is running beyond memory-limits. Current usage : "
                    + currentMemUsage + "bytes. Limit : " + limit
                    + "bytes. Killing task. \nDump of the process-tree for "
                    + tid + " : \n" + pTree.getProcessTreeDump();
            isMemoryOverLimit = true;
          } else if (doCheckPhysicalMemory() &&
              isProcessTreeOverLimit(tid.toString(), currentRssMemUsage,
                                curRssMemUsageOfAgedProcesses, limitPhysical)) {
            // Task (the root process) is still alive and overflowing memory.
            // Dump the process-tree and then clean it up.
            msg = "TaskTree [pid=" + pId + ",tipID=" + tid
                    + "] is running beyond physical memory-limits."
                    + " Current usage : "
                    + currentRssMemUsage + "bytes. Limit : " + limitPhysical
                    + "bytes. Killing task. \nDump of the process-tree for "
                    + tid + " : \n" + pTree.getProcessTreeDump();
            isMemoryOverLimit = true;
          }

          if (isMemoryOverLimit) {
            // Virtual or physical memory over limit. Fail the task and remove
            // the corresponding process tree
            LOG.warn(msg);
            // kill the task
            TaskInProgress tip = taskTracker.getRunningTask(tid);
            if (tip != null) {
              String[] diag = msg.split("\n");
              tip.getStatus().setDiagnosticInfo(diag[0]);
            }
            taskTracker.cleanUpOverMemoryTask(tid, true, msg);

            it.remove();
            LOG.info("Removed ProcessTree with root " + pId);
          } else {
            // Accounting the total memory in usage for all tasks that are still
            // alive and within limits.
            memoryStillInUsage += currentMemUsage;
            rssMemoryStillInUsage += currentRssMemUsage;
          }
        } catch (Exception e) {
          // Log the exception and proceed to the next task.
          LOG.warn("Uncaught exception in TaskMemoryManager "
              + "while managing memory of " + tid + " : "
              + StringUtils.stringifyException(e));
        }
      }

      if (doCheckVirtualMemory() &&
          memoryStillInUsage > maxMemoryAllowedForAllTasks) {
        LOG.warn("The total memory in usage " + memoryStillInUsage
            + " is still overflowing TTs limits "
            + maxMemoryAllowedForAllTasks
            + ". Trying to kill a few tasks with the least progress.");
        killTasksWithLeastProgress(memoryStillInUsage);
      }
      
      if (doCheckPhysicalMemory() &&
          rssMemoryStillInUsage > maxRssMemoryAllowedForAllTasks) {
        LOG.warn("The total physical memory in usage " + rssMemoryStillInUsage
            + " is still overflowing TTs limits "
            + maxRssMemoryAllowedForAllTasks
            + ". Trying to kill a few tasks with the highest memory.");
        killTasksWithMaxRssMemory(rssMemoryStillInUsage);
      }
    
      // Sleep for some time before beginning next cycle
      try {
        LOG.debug(this.getClass() + " : Sleeping for " + monitoringInterval
            + " ms");
        Thread.sleep(monitoringInterval);
      } catch (InterruptedException ie) {
        LOG.warn(this.getClass()
            + " interrupted. Finishing the thread and returning.");
        return;
      }
    }
  }

  /**
   * Is the total physical memory check enabled?
   * @return true if total physical memory check is enabled.
   */
  private boolean doCheckPhysicalMemory() {
    return !(maxRssMemoryAllowedForAllTasks == JobConf.DISABLED_MEMORY_LIMIT);
  }

  /**
   * Is the total virtual memory check enabled?
   * @return true if total virtual memory check is enabled.
   */
  private boolean doCheckVirtualMemory() {
    return !(maxMemoryAllowedForAllTasks == JobConf.DISABLED_MEMORY_LIMIT);
  }

  /**
   * Check whether a task's process tree's current memory usage is over limit.
   * 
   * When a java process exec's a program, it could momentarily account for
   * double the size of it's memory, because the JVM does a fork()+exec()
   * which at fork time creates a copy of the parent's memory. If the 
   * monitoring thread detects the memory used by the task tree at the same
   * instance, it could assume it is over limit and kill the tree, for no
   * fault of the process itself.
   * 
   * We counter this problem by employing a heuristic check:
   * - if a process tree exceeds the memory limit by more than twice, 
   * it is killed immediately
   * - if a process tree has processes older than the monitoring interval
   * exceeding the memory limit by even 1 time, it is killed. Else it is given
   * the benefit of doubt to lie around for one more iteration.
   * 
   * @param tId Task Id for the task tree
   * @param currentMemUsage Memory usage of a task tree
   * @param curMemUsageOfAgedProcesses Memory usage of processes older than
   *                                    an iteration in a task tree
   * @param limit The limit specified for the task
   * @return true if the memory usage is more than twice the specified limit,
   *              or if processes in the tree, older than this thread's 
   *              monitoring interval, exceed the memory limit. False, 
   *              otherwise.
   */
  boolean isProcessTreeOverLimit(String tId, 
                                  long currentMemUsage, 
                                  long curMemUsageOfAgedProcesses, 
                                  long limit) {
    boolean isOverLimit = false;
    
    if (currentMemUsage > (2*limit)) {
      LOG.warn("Process tree for task: " + tId + " running over twice " +
                "the configured limit. Limit=" + limit + 
                ", current usage = " + currentMemUsage);
      isOverLimit = true;
    } else if (curMemUsageOfAgedProcesses > limit) {
      LOG.warn("Process tree for task: " + tId + " has processes older than 1 " +
          "iteration running over the configured limit. Limit=" + limit + 
          ", current usage = " + curMemUsageOfAgedProcesses);
      isOverLimit = true;
    }

    return isOverLimit; 
  }

  // method provided just for easy testing purposes
  boolean isProcessTreeOverLimit(ProcfsBasedProcessTree pTree, 
                                    String tId, long limit) {
    long currentMemUsage = pTree.getCumulativeVmem();
    // as processes begin with an age 1, we want to see if there are processes
    // more than 1 iteration old.
    long curMemUsageOfAgedProcesses = pTree.getCumulativeVmem(1);
    return isProcessTreeOverLimit(tId, currentMemUsage, 
                                  curMemUsageOfAgedProcesses, limit);
  }

  private void killTasksWithLeastProgress(long memoryStillInUsage) {

    List<TaskAttemptID> tasksToKill = new ArrayList<TaskAttemptID>();
    List<TaskAttemptID> tasksToExclude = new ArrayList<TaskAttemptID>();
    // Find tasks to kill so as to get memory usage under limits.
    while (memoryStillInUsage > maxMemoryAllowedForAllTasks) {
      // Exclude tasks that are already marked for killing.
      // Note that we do not need to call isKillable() here because the logic
      // is contained in taskTracker.findTaskToKill()
      TaskInProgress task = taskTracker.findTaskToKill(tasksToExclude);
      if (task == null) {
        break; // couldn't find any more tasks to kill.
      }

      TaskAttemptID tid = task.getTask().getTaskID();
      if (processTreeInfoMap.containsKey(tid)) {
        ProcessTreeInfo ptInfo = processTreeInfoMap.get(tid);
        ProcfsBasedProcessTree pTree = ptInfo.getProcessTree();
        memoryStillInUsage -= pTree.getCumulativeVmem();
        tasksToKill.add(tid);
      }
      // Exclude this task from next search because it is already
      // considered.
      tasksToExclude.add(tid);
    }

    // Now kill the tasks.
    if (!tasksToKill.isEmpty()) {
      for (TaskAttemptID tid : tasksToKill) {
        String msg =
            "Killing one of the least progress tasks - " + tid
                + ", as the cumulative memory usage of all the tasks on "
                + "the TaskTracker " + taskTracker.localHostname 
                + " exceeds virtual memory limit "
                + maxMemoryAllowedForAllTasks + ".";
        TaskInProgress tip = taskTracker.getRunningTask(tid);
        if (tip != null) {
           tip.getStatus().setDiagnosticInfo(msg);
        }
        LOG.warn(msg);
        killTask(tid, msg);
      }
    } else {
      LOG.info("The total memory usage is overflowing TTs limits. "
          + "But found no alive task to kill for freeing memory.");
    }
  }
  
  /**
   * Return the cumulative rss memory used by a task
   * @param tid the task attempt ID of the task
   * @return rss memory usage in bytes. 0 if the process tree is not available
   */
  private long getTaskCumulativeRssmem(TaskAttemptID tid) {
      ProcessTreeInfo ptInfo = processTreeInfoMap.get(tid);
      ProcfsBasedProcessTree pTree = ptInfo.getProcessTree();
      return pTree == null ? 0 : pTree.getCumulativeVmem();
  }

  /**
   * Starting from the tasks use the highest amount of RSS memory,
   * kill the tasks until the RSS memory meets the requirement
   * @param rssMemoryInUsage
   */
  private void killTasksWithMaxRssMemory(long rssMemoryInUsage) {
    
    List<TaskAttemptID> tasksToKill = new ArrayList<TaskAttemptID>();
    List<TaskAttemptID> allTasks = new ArrayList<TaskAttemptID>();
    allTasks.addAll(processTreeInfoMap.keySet());
    // Sort the tasks ascendingly according to RSS memory usage 
    Collections.sort(allTasks, new Comparator<TaskAttemptID>() {
      public int compare(TaskAttemptID tid1, TaskAttemptID tid2) {
        return  getTaskCumulativeRssmem(tid1) < getTaskCumulativeRssmem(tid2) ?
                -1 : 1;
      }});
    
    // Kill the tasks one by one until the memory requirement is met
    while (rssMemoryInUsage > maxRssMemoryAllowedForAllTasks &&
           !allTasks.isEmpty()) {
      TaskAttemptID tid = allTasks.remove(allTasks.size() - 1);
      if (!isKillable(tid)) {
        continue;
      }
      long rssmem = getTaskCumulativeRssmem(tid);
      if (rssmem == 0) {
        break; // Skip tasks without process tree information currently
      }
      tasksToKill.add(tid);
      rssMemoryInUsage -= rssmem;
    }

    // Now kill the tasks.
    if (!tasksToKill.isEmpty()) {
      for (TaskAttemptID tid : tasksToKill) {
        String msg =
            "Killing one of the memory-consuming tasks - " + tid
                + ", as the cumulative RSS memory usage of all the tasks on "
                + "the TaskTracker exceeds physical memory limit "
                + maxRssMemoryAllowedForAllTasks + ".";
        LOG.warn(msg);
        killTask(tid, msg);
      }
    } else {
      LOG.info("The total physical memory usage is overflowing TTs limits. "
          + "But found no alive task to kill for freeing memory.");
    }
  }

  /**
   * Kill the task and clean up ProcessTreeInfo
   * @param tid task attempt ID of the task to be killed.
   * @param msg diagnostics message
   */
  private void killTask(TaskAttemptID tid, String msg) {
    // Kill the task and mark it as killed.
    taskTracker.cleanUpOverMemoryTask(tid, false, msg);
    // Now destroy the ProcessTree, remove it from monitoring map.
    ProcessTreeInfo ptInfo = processTreeInfoMap.get(tid);
    ProcfsBasedProcessTree pTree = ptInfo.getProcessTree();
    pTree.destroy(true/*in the background*/);
    processTreeInfoMap.remove(tid);
    LOG.info("Removed ProcessTree with root " + ptInfo.getPID());
  }

  /**
   * Check if a task can be killed to increase free memory
   * @param tid task attempt ID
   * @return true if the task can be killed
   */
  private boolean isKillable(TaskAttemptID tid) {
      TaskInProgress tip = taskTracker.getRunningTask(tid);
      return tip != null && !tip.wasKilled() &&
             (tip.getRunState() == TaskStatus.State.RUNNING ||
              tip.getRunState() == TaskStatus.State.COMMIT_PENDING);
  }
}
