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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

/**
 * A {@link TaskScheduler} that implements the requirements in HADOOP-3421
 * and provides a HOD-less way to share large clusters. This scheduler 
 * provides the following features: 
 *  * support for queues, where a job is submitted to a queue. 
 *  * Queues are assigned a fraction of the capacity of the grid (their
 *  'capacity') in the sense that a certain capacity of resources 
 *  will be at their disposal. All jobs submitted to the queues of an Org 
 *  will have access to the capacity to the Org.
 *  * Free resources can be allocated to any queue beyond its 
 *  capacity.
 *  * Queues optionally support job priorities (disabled by default). 
 *  * Within a queue, jobs with higher priority will have access to the 
 *  queue's resources before jobs with lower priority. However, once a job 
 *  is running, it will not be preempted for a higher priority job.
 *  * In order to prevent one or more users from monopolizing its resources, 
 *  each queue enforces a limit on the percentage of resources allocated to a 
 *  user at any given time, if there is competition for them.
 *  
 */
class CapacityTaskScheduler extends TaskScheduler {


  /** quick way to get Queue object given a queue name */
  Map<String, CapacitySchedulerQueue> queueInfoMap = 
    new HashMap<String, CapacitySchedulerQueue>();
  
  /**
   * This class captures scheduling information we want to display or log.
   */
  private static class SchedulingDisplayInfo {
    private String queueName;
    CapacityTaskScheduler scheduler;
    
    SchedulingDisplayInfo(String queueName, CapacityTaskScheduler scheduler) { 
      this.queueName = queueName;
      this.scheduler = scheduler;
    }
    
    @Override
    public String toString(){
      // note that we do not call updateAllQueues() here for performance
      // reasons. This means that the data we print out may be slightly
      // stale. This data is updated whenever assignTasks() is called
      // If this doesn't happen, the data gets stale. If we see
      // this often, we may need to detect this situation and call 
      // updateAllQueues(), or just call it each time. 
      return scheduler.getDisplayInfo(queueName);
    }
  }

  // this class encapsulates the result of a task lookup
  private static class TaskLookupResult {

    static enum LookUpStatus {
      LOCAL_TASK_FOUND,
      NO_TASK_FOUND,
      TASK_FAILING_MEMORY_REQUIREMENT,
      OFF_SWITCH_TASK_FOUND
    }
    // constant TaskLookupResult objects. Should not be accessed directly.
    private static final TaskLookupResult NoTaskLookupResult = 
      new TaskLookupResult(null, null, 
          TaskLookupResult.LookUpStatus.NO_TASK_FOUND);
    private static final TaskLookupResult MemFailedLookupResult = 
      new TaskLookupResult(null, null,
          TaskLookupResult.LookUpStatus.TASK_FAILING_MEMORY_REQUIREMENT);

    private LookUpStatus lookUpStatus;
    private Task task;
    private JobInProgress job;
    
    // should not call this constructor directly. use static factory methods.
    private TaskLookupResult(Task t, JobInProgress job, LookUpStatus lUStatus) {
      this.task = t;
      this.job = job;
      this.lookUpStatus = lUStatus;
    }
    
    static TaskLookupResult getTaskFoundResult(Task t, JobInProgress job) {
      return new TaskLookupResult(t, job, LookUpStatus.LOCAL_TASK_FOUND);
    }
    static TaskLookupResult getNoTaskFoundResult() {
      return NoTaskLookupResult;
    }
    static TaskLookupResult getMemFailedResult() {
      return MemFailedLookupResult;
    }
    static TaskLookupResult getOffSwitchTaskFoundResult(Task t, 
                                                        JobInProgress job) {
      return new TaskLookupResult(t, job, LookUpStatus.OFF_SWITCH_TASK_FOUND);
    }

    Task getTask() {
      return task;
    }

    JobInProgress getJob() {
      return job;
    }
    
    LookUpStatus getLookUpStatus() {
      return lookUpStatus;
    }
  }

  /** 
   * This class handles the scheduling algorithms. 
   * The algos are the same for both Map and Reduce tasks. 
   * There may be slight variations later, in which case we can make this
   * an abstract base class and have derived classes for Map and Reduce.  
   */
  static abstract class TaskSchedulingMgr {

    /** our TaskScheduler object */
    protected CapacityTaskScheduler scheduler;
    protected TaskType type = null;

    abstract TaskLookupResult obtainNewTask(TaskTrackerStatus taskTracker, 
        JobInProgress job, boolean assignOffSwitch) throws IOException;

    int getSlotsOccupied(JobInProgress job) {
      return (getNumReservedTaskTrackers(job) + getRunningTasks(job)) * 
             getSlotsPerTask(job);
    }

    abstract int getClusterCapacity();
    abstract int getSlotsPerTask(JobInProgress job);
    abstract int getRunningTasks(JobInProgress job);
    abstract int getPendingTasks(JobInProgress job);
    abstract int getNumReservedTaskTrackers(JobInProgress job);
    
    /**
     * To check if job has a speculative task on the particular tracker.
     * 
     * @param job job to check for speculative tasks.
     * @param tts task tracker on which speculative task would run.
     * @return true if there is a speculative task to run on the tracker.
     */
    abstract boolean hasSpeculativeTask(JobInProgress job, 
        TaskTrackerStatus tts);

    /**
     * Check if the given job has sufficient reserved tasktrackers for all its
     * pending tasks.
     * 
     * @param job job to check for sufficient reserved tasktrackers 
     * @return <code>true</code> if the job has reserved tasktrackers,
     *         else <code>false</code>
     */
    boolean hasSufficientReservedTaskTrackers(JobInProgress job) {
      return getNumReservedTaskTrackers(job) >= getPendingTasks(job);
    }
    
    /**
     * List of Queues for assigning tasks.
     * Queues are ordered by a ratio of (# of running tasks)/capacity, which
     * indicates how much 'free space' the queue has, or how much it is over
     * capacity. This ordered list is iterated over, when assigning tasks.
     */  
    private List<CapacitySchedulerQueue> queuesForAssigningTasks = 
      new ArrayList<CapacitySchedulerQueue>();

    /**
     * Comparator to sort queues.
     * For maps, we need to sort on QueueSchedulingInfo.mapTSI. For 
     * reducers, we use reduceTSI. So we'll need separate comparators.  
     */ 
    private static abstract class QueueComparator 
      implements Comparator<CapacitySchedulerQueue> {
      abstract TaskType getTaskType();
      
      public int compare(CapacitySchedulerQueue q1, CapacitySchedulerQueue q2) {
        // look at how much capacity they've filled. Treat a queue with
        // capacity=0 equivalent to a queue running at capacity
        TaskType taskType = getTaskType();
        double r1 = (0 == q1.getCapacity(taskType))? 1.0f:
          (double)q1.getNumSlotsOccupied(taskType)/(double) q1.getCapacity(taskType);
        double r2 = (0 == q2.getCapacity(taskType))? 1.0f:
          (double)q2.getNumSlotsOccupied(taskType)/(double) q2.getCapacity(taskType);
        if (r1<r2) return -1;
        else if (r1>r2) return 1;
        else return 0;
      }
    }
    
    // subclass for map and reduce comparators
    private static final class MapQueueComparator extends QueueComparator {
      @Override
      TaskType getTaskType() {
        return TaskType.MAP;
      }
    }
    private static final class ReduceQueueComparator extends QueueComparator {
      @Override
      TaskType getTaskType() {
        return TaskType.REDUCE;
      }
    }
    
    // these are our comparator instances
    protected final static MapQueueComparator mapComparator = new MapQueueComparator();
    protected final static ReduceQueueComparator reduceComparator = new ReduceQueueComparator();
    // and this is the comparator to use
    protected QueueComparator queueComparator;

    // Returns queues sorted according to the QueueComparator.
    // Mainly for testing purposes.
    String[] getOrderedQueues() {
      List<String> queues = new ArrayList<String>(queuesForAssigningTasks.size());
      for (CapacitySchedulerQueue queue : queuesForAssigningTasks) {
        queues.add(queue.queueName);
      }
      return queues.toArray(new String[queues.size()]);
    }

    TaskSchedulingMgr(CapacityTaskScheduler sched) {
      scheduler = sched;
    }
    
    // let the scheduling mgr know which queues are in the system
    void initialize(Map<String, CapacitySchedulerQueue> queues) { 
      // add all the queue objects to our list and sort
      queuesForAssigningTasks.clear();
      queuesForAssigningTasks.addAll(queues.values());
      Collections.sort(queuesForAssigningTasks, queueComparator);
    }
    
    private synchronized void sortQueues() {
      Collections.sort(queuesForAssigningTasks, queueComparator);
    }

    /**
     * Ceil of result of dividing two integers.
     * 
     * This is *not* a utility method. 
     * Neither <code>a</code> or <code>b</code> should be negative.
     *  
     * @param a
     * @param b
     * @return ceil of the result of a/b
     */
    private int divideAndCeil(int a, int b) {
      if (b == 0) {
        LOG.info("divideAndCeil called with a=" + a + " b=" + b);
        return 0;
      }
      return (a + (b - 1)) / b;
    }
    
    /*
     * This is the central scheduling method. 
     * It tries to get a task from jobs in a single queue. 
     * Always return a TaskLookupResult object. Don't return null. 
     */
    private TaskLookupResult getTaskFromQueue(TaskTracker taskTracker,
                                              int availableSlots,
                                              CapacitySchedulerQueue queue,
                                              boolean assignOffSwitch)
    throws IOException {
      TaskTrackerStatus taskTrackerStatus = taskTracker.getStatus();
      // we only look at jobs in the running queues, as these are the ones
      // who have been potentially initialized

      for (JobInProgress j : queue.getRunningJobs()) {
        // only look at jobs that can be run. We ignore jobs that haven't 
        // initialized, or have completed but haven't been removed from the 
        // running queue. 
        if (j.getStatus().getRunState() != JobStatus.RUNNING) {
          continue;
        }
        
        // Check to ensure that the job/user/queue are under limits
        if (!queue.assignSlotsToJob(type, j, j.getProfile().getUser())) {
          continue;
        }

        //If this job meets memory requirements. Ask the JobInProgress for
        //a task to be scheduled on the task tracker.
        //if we find a job then we pass it on.
        if (scheduler.memoryMatcher.matchesMemoryRequirements(j, type,
                                                              taskTrackerStatus,
                                                              availableSlots)) {
          // We found a suitable job. Get task from it.
          TaskLookupResult tlr = 
            obtainNewTask(taskTrackerStatus, j, assignOffSwitch);
          //if there is a task return it immediately.
          if (tlr.getLookUpStatus() == 
                  TaskLookupResult.LookUpStatus.LOCAL_TASK_FOUND || 
              tlr.getLookUpStatus() == 
                  TaskLookupResult.LookUpStatus.OFF_SWITCH_TASK_FOUND) {
            // we're successful in getting a task
            return tlr;
          } else {
            //skip to the next job in the queue.
            if (LOG.isDebugEnabled()) {
              LOG.debug("Job " + j.getJobID().toString()
                  + " returned no tasks of type " + type);
            }
            continue;
          }
        } else {
          // if memory requirements don't match then we check if the job has
          // pending tasks and has insufficient number of 'reserved'
          // tasktrackers to cover all pending tasks. If so we reserve the
          // current tasktracker for this job so that high memory jobs are not
          // starved
          if ((getPendingTasks(j) != 0 &&
              !hasSufficientReservedTaskTrackers(j)) &&
                !(j.getNumSlotsPerTask(type) >
                 getTTMaxSlotsForType(taskTrackerStatus, type))) {
            // Reserve all available slots on this tasktracker
            LOG.info(j.getJobID() + ": Reserving "
                + taskTracker.getTrackerName()
                + " since memory-requirements don't match");
            taskTracker.reserveSlots(type, j, taskTracker
                .getAvailableSlots(type));

            // Block
            return TaskLookupResult.getMemFailedResult();
          }
        }//end of memory check block
        // if we're here, this job has no task to run. Look at the next job.
      }//end of for loop

      // found nothing for this queue, look at the next one.
      if (LOG.isDebugEnabled()) {
        String msg = "Found no task from the queue " + queue.queueName;
        LOG.debug(msg);
      }
      return TaskLookupResult.getNoTaskFoundResult();
    }

    // Always return a TaskLookupResult object. Don't return null. 
    // The caller is responsible for ensuring that the Queue objects and the 
    // collections are up-to-date.
    private TaskLookupResult assignTasks(TaskTracker taskTracker, 
                                         int availableSlots, 
                                         boolean assignOffSwitch) 
    throws IOException {
      TaskTrackerStatus taskTrackerStatus = taskTracker.getStatus();

      printQueues();

      // Check if this tasktracker has been reserved for a job...
      JobInProgress job = taskTracker.getJobForFallowSlot(type);
      if (job != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(job.getJobID() + ": Checking 'reserved' tasktracker " + 
                    taskTracker.getTrackerName() + " with " + availableSlots + 
                    " '" + type + "' slots");
        }

        if (availableSlots >= job.getNumSlotsPerTask(type)) {
          // Unreserve 
          taskTracker.unreserveSlots(type, job);
          
          // We found a suitable job. Get task from it.
          if (type == TaskType.MAP) {
            // Don't care about locality!
            job.overrideSchedulingOpportunities();
          }
          return obtainNewTask(taskTrackerStatus, job, true);
        } else {
          // Re-reserve the current tasktracker
          taskTracker.reserveSlots(type, job, availableSlots);
          
          if (LOG.isDebugEnabled()) {
            LOG.debug(job.getJobID() + ": Re-reserving " + 
                      taskTracker.getTrackerName());
          }

          return TaskLookupResult.getMemFailedResult(); 
        }
      }
      
      
      for (CapacitySchedulerQueue queue : queuesForAssigningTasks) {
        //This call is for optimization if we are already over the
        //maximum-capacity we avoid traversing the queues.
        if (!queue.assignSlotsToQueue(type, 1)) {
          continue;
        }
        
        TaskLookupResult tlr = 
          getTaskFromQueue(taskTracker, availableSlots, queue, assignOffSwitch);
        TaskLookupResult.LookUpStatus lookUpStatus = tlr.getLookUpStatus();

        if (lookUpStatus == TaskLookupResult.LookUpStatus.NO_TASK_FOUND) {
          continue; // Look in other queues.
        }

        // if we find a task, return
        if (lookUpStatus == TaskLookupResult.LookUpStatus.LOCAL_TASK_FOUND ||
            lookUpStatus == TaskLookupResult.LookUpStatus.OFF_SWITCH_TASK_FOUND) {
          return tlr;
        }
        // if there was a memory mismatch, return
        else if (lookUpStatus == 
          TaskLookupResult.LookUpStatus.TASK_FAILING_MEMORY_REQUIREMENT) {
            return tlr;
        }
      }

      // nothing to give
      return TaskLookupResult.getNoTaskFoundResult();
    }

    // for debugging.
    private void printQueues() {
      if (LOG.isDebugEnabled()) {
        StringBuffer s = new StringBuffer();
        for (CapacitySchedulerQueue queue : queuesForAssigningTasks) {
          Collection<JobInProgress> runJobs = queue.getRunningJobs();
          s.append(
            String.format(
              " Queue '%s'(%s): runningTasks=%d, "
                + "occupiedSlots=%d, capacity=%d, runJobs=%d  maxCapacity=%d ",
              queue.queueName,
              this.type, 
              Integer.valueOf(queue.getNumRunningTasks(type)), 
              Integer.valueOf(queue.getNumSlotsOccupied(type)), 
              Integer.valueOf(queue.getCapacity(type)), 
              Integer.valueOf(runJobs.size()),
              Integer.valueOf(queue.getMaxCapacity(type))));
        }
        LOG.debug(s);
      }
    }
    
    /**
     * Check if one of the tasks have a speculative task to execute on the 
     * particular task tracker.
     * 
     * @param tips tasks of a job
     * @param progress percentage progress of the job
     * @param tts task tracker status for which we are asking speculative tip
     * @return true if job has a speculative task to run on particular TT.
     */
    boolean hasSpeculativeTask(TaskInProgress[] tips, float progress, 
        TaskTrackerStatus tts) {
      long currentTime = System.currentTimeMillis();
      for(TaskInProgress tip : tips)  {
        if(tip.isRunning() 
            && !(tip.hasRunOnMachine(tts.getHost(), tts.getTrackerName())) 
            && tip.hasSpeculativeTask(currentTime, progress)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * The scheduling algorithms for map tasks. 
   */
  private static class MapSchedulingMgr extends TaskSchedulingMgr {

    MapSchedulingMgr(CapacityTaskScheduler schedulr) {
      super(schedulr);
      type = TaskType.MAP;
      queueComparator = mapComparator;
    }

    @Override
    TaskLookupResult obtainNewTask(TaskTrackerStatus taskTracker, 
                                   JobInProgress job, boolean assignOffSwitch) 
    throws IOException {
      ClusterStatus clusterStatus = 
        scheduler.taskTrackerManager.getClusterStatus();
      int numTaskTrackers = clusterStatus.getTaskTrackers();
      int numUniqueHosts = scheduler.taskTrackerManager.getNumberOfUniqueHosts();
      
      // Inform the job it is about to get a scheduling opportunity
      job.schedulingOpportunity();
      
      // First, try to get a 'local' task
      Task t = 
        job.obtainNewLocalMapTask(taskTracker, numTaskTrackers, numUniqueHosts);
      
      if (t != null) {
        return TaskLookupResult.getTaskFoundResult(t, job); 
      }
      
      // Next, try to get an 'off-switch' task if appropriate
      // Do not bother as much about locality for High-RAM jobs
      if (job.getNumSlotsPerMap() > 1 || 
          (assignOffSwitch && 
              job.scheduleOffSwitch(numTaskTrackers))) {
        t = 
          job.obtainNewNonLocalMapTask(taskTracker, numTaskTrackers, numUniqueHosts);
      }
      
      return (t != null) ? 
          TaskLookupResult.getOffSwitchTaskFoundResult(t, job) :
          TaskLookupResult.getNoTaskFoundResult();
    }

    @Override
    int getClusterCapacity() {
      return scheduler.taskTrackerManager.getClusterStatus().getMaxMapTasks();
    }

    @Override
    int getRunningTasks(JobInProgress job) {
      return job.runningMaps();
    }

    @Override
    int getPendingTasks(JobInProgress job) {
      return job.pendingMaps();
    }

    @Override
    int getSlotsPerTask(JobInProgress job) {
      return job.getNumSlotsPerTask(TaskType.MAP);
    }

    int getNumReservedTaskTrackers(JobInProgress job) {
      return job.getNumReservedTaskTrackersForMaps();
    }

    @Override
    boolean hasSpeculativeTask(JobInProgress job, TaskTrackerStatus tts) {
      //Check if job supports speculative map execution first then 
      //check if job has speculative maps.
      return (job.getMapSpeculativeExecution())&& (
          hasSpeculativeTask(job.getTasks(TaskType.MAP), 
              job.getStatus().mapProgress(), tts));
    }
  }

  /**
   * The scheduling algorithms for reduce tasks. 
   */
  private static class ReduceSchedulingMgr extends TaskSchedulingMgr {

    ReduceSchedulingMgr(CapacityTaskScheduler schedulr) {
      super(schedulr);
      type = TaskType.REDUCE;
      queueComparator = reduceComparator;
    }

    @Override
    TaskLookupResult obtainNewTask(TaskTrackerStatus taskTracker, 
                                   JobInProgress job, boolean unused) 
    throws IOException {
      ClusterStatus clusterStatus = 
        scheduler.taskTrackerManager.getClusterStatus();
      int numTaskTrackers = clusterStatus.getTaskTrackers();
      Task t = job.obtainNewReduceTask(taskTracker, numTaskTrackers, 
          scheduler.taskTrackerManager.getNumberOfUniqueHosts());
      
      return (t != null) ? TaskLookupResult.getTaskFoundResult(t, job) :
        TaskLookupResult.getNoTaskFoundResult();
    }

    @Override
    int getClusterCapacity() {
      return scheduler.taskTrackerManager.getClusterStatus()
          .getMaxReduceTasks();
    }

    @Override
    int getRunningTasks(JobInProgress job) {
      return job.runningReduces();
    }

    @Override
    int getPendingTasks(JobInProgress job) {
      return job.pendingReduces();
    }

    @Override
    int getSlotsPerTask(JobInProgress job) {
      return job.getNumSlotsPerTask(TaskType.REDUCE);    
    }

    int getNumReservedTaskTrackers(JobInProgress job) {
      return job.getNumReservedTaskTrackersForReduces();
    }

    @Override
    boolean hasSpeculativeTask(JobInProgress job, TaskTrackerStatus tts) {
      //check if the job supports reduce speculative execution first then
      //check if the job has speculative tasks.
      return (job.getReduceSpeculativeExecution()) && (
          hasSpeculativeTask(job.getTasks(TaskType.REDUCE), 
              job.getStatus().reduceProgress(), tts));
    }
  }
  
  /** the scheduling mgrs for Map and Reduce tasks */ 
  protected TaskSchedulingMgr mapScheduler = new MapSchedulingMgr(this);
  protected TaskSchedulingMgr reduceScheduler = new ReduceSchedulingMgr(this);

  MemoryMatcher memoryMatcher = new MemoryMatcher(this);

  static final Log LOG = LogFactory.getLog(CapacityTaskScheduler.class);
  protected JobQueuesManager jobQueuesManager;
  protected CapacitySchedulerConf schedConf;
  /** whether scheduler has started or not */
  private boolean started = false;

  /**
   * A clock class - can be mocked out for testing.
   */
  static class Clock {
    long getTime() {
      return System.currentTimeMillis();
    }
  }

  private Clock clock;
  private JobInitializationPoller initializationPoller;

  private long memSizeForMapSlotOnJT;
  private long memSizeForReduceSlotOnJT;
  private long limitMaxMemForMapTasks;
  private long limitMaxMemForReduceTasks;
  
  private volatile int maxTasksPerHeartbeat;
  private volatile int maxTasksToAssignAfterOffSwitch;

  public CapacityTaskScheduler() {
    this(new Clock());
  }
  
  // for testing
  public CapacityTaskScheduler(Clock clock) {
    this.jobQueuesManager = new JobQueuesManager(this);
    this.clock = clock;
  }
  
  /** mostly for testing purposes */
  public void setResourceManagerConf(CapacitySchedulerConf conf) {
    this.schedConf = conf;
  }
  
  @Override
  public synchronized void refresh() throws IOException {
    Configuration conf = new Configuration();
    CapacitySchedulerConf schedConf = new CapacitySchedulerConf();
    
    // Refresh
    QueueManager queueManager = taskTrackerManager.getQueueManager();
    Set<String> queueNames = queueManager.getQueues();
    Map<String, CapacitySchedulerQueue> newQueues =
      parseQueues(queueManager.getQueues(), schedConf);
    
    // Check to ensure no queue has been deleted
    checkForQueueDeletion(queueInfoMap, newQueues);
    
    // Re-intialize the scheduler
    initialize(queueManager, newQueues, conf, schedConf);
    
    // Inform the job-init-poller
    initializationPoller.reinit(queueNames);
    
    // Finally, reset the configuration
    setConf(conf);
    this.schedConf = schedConf;
  }

  private void 
  checkForQueueDeletion(Map<String, CapacitySchedulerQueue> currentQueues, 
      Map<String, CapacitySchedulerQueue> newQueues) 
  throws IOException {
    for (String queueName : currentQueues.keySet()) {
      if (!newQueues.containsKey(queueName)) {
        throw new IOException("Couldn't find queue '" + queueName + 
            "' during refresh!");
      }
    }
  }
  
  private void initializeMemoryRelatedConf(Configuration conf) {
    //handling @deprecated
    if (conf.get(
      CapacitySchedulerConf.DEFAULT_PERCENTAGE_OF_PMEM_IN_VMEM_PROPERTY) !=
      null) {
      LOG.warn(
        JobConf.deprecatedString(
          CapacitySchedulerConf.DEFAULT_PERCENTAGE_OF_PMEM_IN_VMEM_PROPERTY));
    }

    //handling @deprecated
    if (conf.get(CapacitySchedulerConf.UPPER_LIMIT_ON_TASK_PMEM_PROPERTY) !=
      null) {
      LOG.warn(
        JobConf.deprecatedString(
          CapacitySchedulerConf.UPPER_LIMIT_ON_TASK_PMEM_PROPERTY));
    }

    if (conf.get(JobConf.MAPRED_TASK_DEFAULT_MAXVMEM_PROPERTY) != null) {
      LOG.warn(
        JobConf.deprecatedString(
          JobConf.MAPRED_TASK_DEFAULT_MAXVMEM_PROPERTY));
    }

    memSizeForMapSlotOnJT =
        JobConf.normalizeMemoryConfigValue(conf.getLong(
            JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));
    memSizeForReduceSlotOnJT =
        JobConf.normalizeMemoryConfigValue(conf.getLong(
            JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));

    //handling @deprecated values
    if (conf.get(JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY) != null) {
      LOG.warn(
        JobConf.deprecatedString(
          JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY)+
          " instead use " +JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY+
          " and " + JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY
      );
      
      limitMaxMemForMapTasks = limitMaxMemForReduceTasks =
        JobConf.normalizeMemoryConfigValue(
          conf.getLong(
            JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));
      if (limitMaxMemForMapTasks != JobConf.DISABLED_MEMORY_LIMIT &&
        limitMaxMemForMapTasks >= 0) {
        limitMaxMemForMapTasks = limitMaxMemForReduceTasks =
          limitMaxMemForMapTasks /
            (1024 * 1024); //Converting old values in bytes to MB
      }
    } else {
      limitMaxMemForMapTasks =
        JobConf.normalizeMemoryConfigValue(
          conf.getLong(
            JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));
      limitMaxMemForReduceTasks =
        JobConf.normalizeMemoryConfigValue(
          conf.getLong(
            JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));
    }
    LOG.info(String.format("Scheduler configured with "
        + "(memSizeForMapSlotOnJT, memSizeForReduceSlotOnJT, "
        + "limitMaxMemForMapTasks, limitMaxMemForReduceTasks)"
        + " (%d,%d,%d,%d)", Long.valueOf(memSizeForMapSlotOnJT), Long
        .valueOf(memSizeForReduceSlotOnJT), Long
        .valueOf(limitMaxMemForMapTasks), Long
        .valueOf(limitMaxMemForReduceTasks)));
  }

  long getMemSizeForMapSlot() {
    return memSizeForMapSlotOnJT;
  }

  long getMemSizeForReduceSlot() {
    return memSizeForReduceSlotOnJT;
  }

  long getLimitMaxMemForMapSlot() {
    return limitMaxMemForMapTasks;
  }

  long getLimitMaxMemForReduceSlot() {
    return limitMaxMemForReduceTasks;
  }

  String[] getOrderedQueues(TaskType type) {
    if (type == TaskType.MAP) {
      return mapScheduler.getOrderedQueues();
    } else if (type == TaskType.REDUCE) {
      return reduceScheduler.getOrderedQueues();
    }
    return null;
  }

  @Override
  public synchronized void start() throws IOException {
    if (started) return;
    super.start();
    // initialize our queues from the config settings
    if (null == schedConf) {
      schedConf = new CapacitySchedulerConf();
    }

    // Initialize queues
    QueueManager queueManager = taskTrackerManager.getQueueManager();
    Set<String> queueNames = queueManager.getQueues();
    initialize(queueManager, parseQueues(queueNames, schedConf), 
        getConf(), schedConf);
    
    // listen to job changes
    taskTrackerManager.addJobInProgressListener(jobQueuesManager);

    //Start thread for initialization
    if (initializationPoller == null) {
      this.initializationPoller = new JobInitializationPoller(
          jobQueuesManager, schedConf, queueNames, taskTrackerManager);
    }
    initializationPoller.init(queueNames.size(), schedConf);
    initializationPoller.setDaemon(true);
    initializationPoller.start();

    if (taskTrackerManager instanceof JobTracker) {
      JobTracker jobTracker = (JobTracker) taskTrackerManager;
      HttpServer infoServer = jobTracker.infoServer;
      infoServer.setAttribute("scheduler", this);
      infoServer.addServlet("scheduler", "/scheduler",
          CapacitySchedulerServlet.class);
    }

    started = true;
    LOG.info("Capacity scheduler initialized " + queueNames.size() + " queues");  
  }
  
  
  void initialize(QueueManager queueManager,
      Map<String, CapacitySchedulerQueue> newQueues,
      Configuration conf, CapacitySchedulerConf schedConf) {
    // Memory related configs
    initializeMemoryRelatedConf(conf);

    // Setup queues
    for (Map.Entry<String, CapacitySchedulerQueue> e : newQueues.entrySet()) {
      String newQueueName = e.getKey();
      CapacitySchedulerQueue newQueue = e.getValue();
      CapacitySchedulerQueue currentQueue = queueInfoMap.get(newQueueName);
      if (currentQueue != null) {
        currentQueue.initializeQueue(newQueue);
        LOG.info("Updated queue configs for " + newQueueName);
      } else {
        queueInfoMap.put(newQueueName, newQueue);
        LOG.info("Added new queue: " + newQueueName);
      }
    }

    // Set SchedulingDisplayInfo
    for (String queueName : queueInfoMap.keySet()) {
      SchedulingDisplayInfo schedulingInfo = 
        new SchedulingDisplayInfo(queueName, this);
      queueManager.setSchedulerInfo(queueName, schedulingInfo);
    }

    // Inform the queue manager 
    jobQueuesManager.setQueues(queueInfoMap);
    
    // let our mgr objects know about the queues
    mapScheduler.initialize(queueInfoMap);
    reduceScheduler.initialize(queueInfoMap);
    
    // scheduling tunables
    maxTasksPerHeartbeat = schedConf.getMaxTasksPerHeartbeat();
    maxTasksToAssignAfterOffSwitch = 
      schedConf.getMaxTasksToAssignAfterOffSwitch();
  }
  
  Map<String, CapacitySchedulerQueue> 
  parseQueues(Collection<String> queueNames, CapacitySchedulerConf schedConf) 
  throws IOException {
    Map<String, CapacitySchedulerQueue> queueInfoMap = 
      new HashMap<String, CapacitySchedulerQueue>();
    
    // Sanity check: there should be at least one queue. 
    if (0 == queueNames.size()) {
      throw new IllegalStateException("System has no queue configured");
    }

    float totalCapacityPercent = 0.0f;
    for (String queueName: queueNames) {
      float capacityPercent = schedConf.getCapacity(queueName);
      if (capacityPercent == -1.0) {
        throw new IOException("Queue '" + queueName + 
            "' doesn't have configured capacity!");
      } 
      
      totalCapacityPercent += capacityPercent;

      // create our Queue and add to our hashmap
      CapacitySchedulerQueue queue = 
        new CapacitySchedulerQueue(queueName, schedConf);
      queueInfoMap.put(queueName, queue);
    }
    
    if (Math.floor(totalCapacityPercent) != 100) {
      throw new IllegalArgumentException(
        "Sum of queue capacities not 100% at "
          + totalCapacityPercent);
    }    

    return queueInfoMap;
  }
  
  /** mostly for testing purposes */
  void setInitializationPoller(JobInitializationPoller p) {
    this.initializationPoller = p;
  }
  
  @Override
  public synchronized void terminate() throws IOException {
    if (!started) return;
    if (jobQueuesManager != null) {
      taskTrackerManager.removeJobInProgressListener(
          jobQueuesManager);
    }
    started = false;
    initializationPoller.terminate();
    super.terminate();
  }
  
  @Override
  public synchronized void setConf(Configuration conf) {
    super.setConf(conf);
  }

  /**
   * provided for the test classes
   * lets you update the Queue objects and sorted collections
   */ 
  void updateQueueUsageForTests() {
    ClusterStatus c = taskTrackerManager.getClusterStatus();
    int mapClusterCapacity = c.getMaxMapTasks();
    int reduceClusterCapacity = c.getMaxReduceTasks();
    // update the Queue objects
    updateAllQueues(mapClusterCapacity, reduceClusterCapacity);
    mapScheduler.sortQueues();
    reduceScheduler.sortQueues();
    mapScheduler.printQueues();
    reduceScheduler.printQueues();
  }

  /**
   * Update all queues to reflect current usage.
   * We don't need exact information for all variables, just enough for us
   * to make scheduling decisions. For example, we don't need an exact count
   * of numRunningTasks. Once we count upto the grid capacity, any
   * number beyond that will make no difference.
   */
  private synchronized void updateAllQueues(
    int mapClusterCapacity, int reduceClusterCapacity) {
    // if # of slots have changed since last time, update.
    // First, compute whether the total number of TT slots have changed
    for (CapacitySchedulerQueue queue: queueInfoMap.values()) {
      queue.updateAll(mapClusterCapacity, reduceClusterCapacity, 
                 mapScheduler, reduceScheduler);
    }
  }

  private static final int JOBQUEUE_SCHEDULINGINFO_INITIAL_LENGTH = 175;

  static String getJobQueueSchedInfo(int numMapsRunningForThisJob, 
    int numRunningMapSlots, int numReservedMapSlotsForThisJob, 
    int numReducesRunningForThisJob, int numRunningReduceSlots, 
    int numReservedReduceSlotsForThisJob) {
    StringBuilder sb = new StringBuilder(JOBQUEUE_SCHEDULINGINFO_INITIAL_LENGTH);
    sb.append(numMapsRunningForThisJob).append(" running map tasks using ")
      .append(numRunningMapSlots).append(" map slots. ")
      .append(numReservedMapSlotsForThisJob).append(" additional slots reserved. ")
      .append(numReducesRunningForThisJob).append(" running reduce tasks using ")
      .append(numRunningReduceSlots).append(" reduce slots. ")
      .append(numReservedReduceSlotsForThisJob).append(" additional slots reserved.");
    return sb.toString();
  }

  /*
   * The grand plan for assigning a task.
   * 
   * If multiple task assignment is enabled, it tries to get one map and
   * one reduce slot depending on free slots on the TT.
   * 
   * Otherwise, we decide whether a Map or Reduce task should be given to a TT 
   * (if the TT can accept either). 
   * Either way, we first pick a queue. We only look at queues that need 
   * a slot. Among these, we first look at queues whose 
   * (# of running tasks)/capacity is the least.
   * Next, pick a job in a queue. we pick the job at the front of the queue
   * unless its user is over the user limit. 
   * Finally, given a job, pick a task from the job. 
   *  
   */
  @Override
  public synchronized List<Task> assignTasks(TaskTracker taskTracker)
  throws IOException {    
    TaskTrackerStatus taskTrackerStatus = taskTracker.getStatus();
    ClusterStatus c = taskTrackerManager.getClusterStatus();
    int mapClusterCapacity = c.getMaxMapTasks();
    int reduceClusterCapacity = c.getMaxReduceTasks();
    int maxMapSlots = taskTrackerStatus.getMaxMapSlots();
    int currentMapSlots = taskTrackerStatus.countOccupiedMapSlots();
    int maxReduceSlots = taskTrackerStatus.getMaxReduceSlots();
    int currentReduceSlots = taskTrackerStatus.countOccupiedReduceSlots();
    if (LOG.isDebugEnabled()) {
      LOG.debug("TT asking for task, max maps=" + taskTrackerStatus.getMaxMapSlots() + 
        ", run maps=" + taskTrackerStatus.countMapTasks() + ", max reds=" + 
        taskTrackerStatus.getMaxReduceSlots() + ", run reds=" + 
        taskTrackerStatus.countReduceTasks() + ", map cap=" + 
        mapClusterCapacity + ", red cap = " + 
        reduceClusterCapacity);
    }

    /* 
     * update all our queues
     * This involves updating each queue structure. This operation depends
     * on the number of running jobs in a queue, and some waiting jobs. If it
     * becomes expensive, do it once every few heartbeats only.
     */ 
    updateAllQueues(mapClusterCapacity, reduceClusterCapacity);
    
    // schedule tasks
    List<Task> result = new ArrayList<Task>();
    addMapTasks(taskTracker, result, maxMapSlots, currentMapSlots);
    addReduceTask(taskTracker, result, maxReduceSlots, currentReduceSlots);
    return result;
  }

  // Pick a reduce task and add to the list of tasks, if there's space
  // on the TT to run one.
  private void addReduceTask(TaskTracker taskTracker, List<Task> tasks,
                                  int maxReduceSlots, int currentReduceSlots) 
                    throws IOException {
    int availableSlots = maxReduceSlots - currentReduceSlots;
    if (availableSlots > 0) {
      reduceScheduler.sortQueues();
      TaskLookupResult tlr = 
        reduceScheduler.assignTasks(taskTracker, availableSlots, true);
      if (TaskLookupResult.LookUpStatus.LOCAL_TASK_FOUND == tlr.getLookUpStatus()) {
        tasks.add(tlr.getTask());
      }
    }
  }
  
  // Pick a map task and add to the list of tasks, if there's space
  // on the TT to run one.
  private void addMapTasks(TaskTracker taskTracker, List<Task> tasks, 
                              int maxMapSlots, int currentMapSlots)
                    throws IOException {
    int availableSlots = maxMapSlots - currentMapSlots;
    boolean assignOffSwitch = true;
    int tasksToAssignAfterOffSwitch = this.maxTasksToAssignAfterOffSwitch;
    while (availableSlots > 0) {
      mapScheduler.sortQueues();
      TaskLookupResult tlr = mapScheduler.assignTasks(taskTracker, 
                                                      availableSlots,
                                                      assignOffSwitch);
      if (TaskLookupResult.LookUpStatus.NO_TASK_FOUND == 
            tlr.getLookUpStatus() || 
          TaskLookupResult.LookUpStatus.TASK_FAILING_MEMORY_REQUIREMENT == 
            tlr.getLookUpStatus()) {
        break;
      }

      Task t = tlr.getTask();
      JobInProgress job = tlr.getJob();

      tasks.add(t);

      if (tasks.size() >= maxTasksPerHeartbeat) {
        return;
      }
      
      if (TaskLookupResult.LookUpStatus.OFF_SWITCH_TASK_FOUND == 
        tlr.getLookUpStatus()) {
        // Atmost 1 off-switch task per-heartbeat
        assignOffSwitch = false;
      }
      
      // Respect limits on #tasks to assign after an off-switch task is assigned
      if (!assignOffSwitch) {
        if (tasksToAssignAfterOffSwitch == 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Hit limit of max tasks after off-switch: " + 
                this.maxTasksToAssignAfterOffSwitch + " after " + 
                tasks.size() + " maps.");
          }
          return;
        }
        --tasksToAssignAfterOffSwitch;
      }
      
      // Assigned some slots
      availableSlots -= t.getNumSlotsRequired();
      
      // Update the queue
      CapacitySchedulerQueue queue = 
        queueInfoMap.get(job.getProfile().getQueueName());
      queue.update(TaskType.MAP, job,
          job.getProfile().getUser(), 1, t.getNumSlotsRequired());
    }
  }
  
  // called when a job is added
  synchronized void jobAdded(JobInProgress job) throws IOException {
    CapacitySchedulerQueue queue = 
      queueInfoMap.get(job.getProfile().getQueueName());
    
    // Inform the queue
    queue.jobAdded(job);
    
    // setup scheduler specific job information
    preInitializeJob(job);
    
    if (LOG.isDebugEnabled()) {
      String user = job.getProfile().getUser();
      LOG.debug("Job " + job.getJobID() + " is added under user " + user + 
                ", user now has " + queue.getNumJobsByUser(user) + " jobs");
    }
  }

  /**
   * Setup {@link CapacityTaskScheduler} specific information prior to
   * job initialization.
   */
  void preInitializeJob(JobInProgress job) {
    JobConf jobConf = job.getJobConf();
    
    // Compute number of slots required to run a single map/reduce task
    int slotsPerMap = 1;
    int slotsPerReduce = 1;
    if (memoryMatcher.isSchedulingBasedOnMemEnabled()) {
      slotsPerMap = jobConf.computeNumSlotsPerMap(getMemSizeForMapSlot());
     slotsPerReduce = 
       jobConf.computeNumSlotsPerReduce(getMemSizeForReduceSlot());
    }
    job.setNumSlotsPerMap(slotsPerMap);
    job.setNumSlotsPerReduce(slotsPerReduce);
  }
  
  // called when a job completes
  synchronized void jobCompleted(JobInProgress job) {
    CapacitySchedulerQueue queue = 
      queueInfoMap.get(job.getProfile().getQueueName());
    
    // Inform the queue
    queue.jobCompleted(job);
  }
  
  @Override
  public synchronized Collection<JobInProgress> getJobs(String queueName) {
    Collection<JobInProgress> jobCollection = new ArrayList<JobInProgress>();
    CapacitySchedulerQueue queue = queueInfoMap.get(queueName);
    Collection<JobInProgress> runningJobs = queue.getRunningJobs();
    jobCollection.addAll(queue.getInitializingJobs());
    if (runningJobs != null) {
      jobCollection.addAll(runningJobs);
    }
    Collection<JobInProgress> waitingJobs = queue.getWaitingJobs();
    Collection<JobInProgress> tempCollection = new ArrayList<JobInProgress>();
    if(waitingJobs != null) {
      tempCollection.addAll(waitingJobs);
    }
    tempCollection.removeAll(runningJobs);
    if(!tempCollection.isEmpty()) {
      jobCollection.addAll(tempCollection);
    }
    return jobCollection;
  }
  
  JobInitializationPoller getInitializationPoller() {
    return initializationPoller;
  }

  /**
   * @return the jobQueuesManager
   */
  JobQueuesManager getJobQueuesManager() {
    return jobQueuesManager;
  }

  Map<String, CapacitySchedulerQueue> getQueueInfoMap() {
    return queueInfoMap;
  }

  /**
   * @return the mapScheduler
   */
  TaskSchedulingMgr getMapScheduler() {
    return mapScheduler;
  }

  /**
   * @return the reduceScheduler
   */
  TaskSchedulingMgr getReduceScheduler() {
    return reduceScheduler;
  }

  synchronized String getDisplayInfo(String queueName) {
    CapacitySchedulerQueue queue = queueInfoMap.get(queueName);
    if (null == queue) { 
      return null;
    }
    return queue.toString();
  }

  private static int getTTMaxSlotsForType(TaskTrackerStatus status, TaskType type) {
      return (type == TaskType.MAP) ? status.getMaxMapSlots() : status.getMaxReduceSlots();
  }
}

