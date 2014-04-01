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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapred.CapacityTaskScheduler.TaskSchedulingMgr;
import org.apache.hadoop.mapred.JobQueueJobInProgressListener.JobSchedulingInfo;


/***********************************************************************
 * Keeping track of scheduling information for queues
 * 
 * We need to maintain scheduling information relevant to a queue (its 
 * name, capacity, etc), along with information specific to 
 * each kind of task, Map or Reduce (num of running tasks, pending 
 * tasks etc). 
 * 
 * This scheduling information is used to decide how to allocate
 * tasks, redistribute capacity, etc.
 *  
 * A QueueSchedulingInfo(QSI) object represents scheduling information for
 * a  A TaskSchedulingInfo (TSI) object represents scheduling 
 * information for a particular kind of task (Map or Reduce).
 *   
 **********************************************************************/
class CapacitySchedulerQueue {
  
  static final Log LOG = LogFactory.getLog(CapacityTaskScheduler.class);
  
  private static class SlotsUsage {
    /** 
     * the actual capacity, which depends on how many slots are available
     * in the cluster at any given time. 
     */
    private int capacity = 0;
    // number of running tasks
    int numRunningTasks = 0;
    // number of slots occupied by running tasks
    int numSlotsOccupied = 0;
  
    //the actual maximum capacity which depends on how many slots are available
    //in cluster at any given time.
    private int maxCapacity = -1;
  
    // Active users
    Set<String> users = new HashSet<String>();
    
    /**
     * for each user, we need to keep track of number of slots occupied by
     * running tasks
     */
    Map<String, Integer> numSlotsOccupiedByUser = 
      new HashMap<String, Integer>();
  
    /**
     * reset the variables associated with tasks
     */
    void reset() {
      numRunningTasks = 0;
      numSlotsOccupied = 0;
      users.clear();
      numSlotsOccupiedByUser.clear();
    }
  
  
    /**
     * Returns the actual capacity.
     * capacity.
     *
     * @return
     */
    int getCapacity() {
      return capacity;
    }
  
    /**
     * Mutator method for capacity
     *
     * @param capacity
     */
    void setCapacity(int capacity) {
        this.capacity = capacity;
    }
  
    /**
     * @return the numRunningTasks
     */
    int getNumRunningTasks() {
      return numRunningTasks;
    }
  
    /**
     * @return the numSlotsOccupied
     */
    int getNumSlotsOccupied() {
      return numSlotsOccupied;
    }
  
    /**
     * @return number of active users
     */
    int getNumActiveUsers() {
      return users.size();
    }
    
    /**
     * return information about the tasks
     */
    @Override
    public String toString() {
      float occupiedSlotsAsPercent =
          getCapacity() != 0 ?
            ((float) numSlotsOccupied * 100 / getCapacity()) : 0;
      StringBuffer sb = new StringBuffer();
      
      sb.append("Capacity: " + capacity + " slots\n");
      
      if(getMaxCapacity() >= 0) {
        sb.append("Maximum capacity: " + getMaxCapacity() +" slots\n");
      }
      sb.append(String.format("Used capacity: %d (%.1f%% of Capacity)\n",
          Integer.valueOf(numSlotsOccupied), Float
              .valueOf(occupiedSlotsAsPercent)));
      sb.append(String.format("Running tasks: %d\n", Integer
          .valueOf(numRunningTasks)));
      // include info on active users
      if (numSlotsOccupied != 0) {
        sb.append("Active users:\n");
        for (Map.Entry<String, Integer> entry : numSlotsOccupiedByUser
            .entrySet()) {
          if ((entry.getValue() == null) || (entry.getValue().intValue() <= 0)) {
            // user has no tasks running
            continue;
          }
          sb.append("User '" + entry.getKey() + "': ");
          int numSlotsOccupiedByThisUser = entry.getValue().intValue();
          float p =
              (float) numSlotsOccupiedByThisUser * 100 / numSlotsOccupied;
          sb.append(String.format("%d (%.1f%% of used capacity)\n", Long
              .valueOf(numSlotsOccupiedByThisUser), Float.valueOf(p)));
        }
      }
      return sb.toString();
    }
  
    int getMaxCapacity() {
      return maxCapacity;
    }
  
    void setMaxCapacity(int maxCapacity) {
      this.maxCapacity = maxCapacity;
    }
    
    int getNumSlotsOccupiedByUser(String user) {
      Integer slots = numSlotsOccupiedByUser.get(user);
      return (slots != null) ? slots : 0;
    }


    void updateCapacities(float capacityPercent, float maxCapacityPercent, 
                          int clusterCapacity) {
      //compute new capacity
      setCapacity((int)(capacityPercent*clusterCapacity/100));

      //compute new max map capacities
      if(maxCapacityPercent > 0) {
        setMaxCapacity((int)(maxCapacityPercent*clusterCapacity / 100));
      }
    }
    
    void updateSlotsUsage(String user, int pendingTasks, int numRunningTasks, int numSlotsOccupied) {
      this.numRunningTasks += numRunningTasks;
      this.numSlotsOccupied += numSlotsOccupied;
      Integer i = this.numSlotsOccupiedByUser.get(user);
      int slots = numSlotsOccupied + ((i == null) ? 0 : i.intValue());
      this.numSlotsOccupiedByUser.put(user, slots);
      if (pendingTasks > 0) {
        users.add(user);
      }
    }
  }

  // Queue name
  final String queueName;

  /**
   * capacity(%) is set in the config
   */
  volatile float capacityPercent = 0;
  
  
  /**
   * maxCapacityPercent(%) is set in config as
   * mapred.capacity-scheduler.<queue-name>.maximum-capacity
   * maximum-capacity percent defines a limit beyond which a queue
   * cannot expand. Remember this limit is dynamic and changes w.r.t
   * cluster size.
   */
  volatile float maxCapacityPercent = -1;
  
  /** 
   * to handle user limits, we need to know how many users have jobs in 
   * the 
   */  
  Map<String, Integer> numJobsByUser = new HashMap<String, Integer>();
    
  /**
   * min value of user limit (same for all users)
   */
  volatile int ulMin;

  /**
   * The factor of queue-capacity above which a single user can consume
   * queue resources.
   */
  volatile float ulMinFactor;
  
  /**
   * We keep a TaskSchedulingInfo object for each kind of task we support
   */
  CapacitySchedulerQueue.SlotsUsage mapSlots;
  CapacitySchedulerQueue.SlotsUsage reduceSlots;
  
  /** 
   * Whether the queue supports priorities.
   */
  final boolean supportsPriorities;
  
  /**
   * Information required to track job, user, queue limits 
   */
  
  Map<JobSchedulingInfo, JobInProgress> waitingJobs; // for waiting jobs
  Map<JobSchedulingInfo, JobInProgress> initializingJobs; // for init'ing jobs
  Map<JobSchedulingInfo, JobInProgress> runningJobs; // for running jobs
  
  /**
   *  Active tasks in the queue
   */
  int activeTasks = 0;
  
  /**
   *  Users in the queue
   */
  Map<String, UserInfo> users = new HashMap<String, UserInfo>();

  /**
   * Comparator for ordering jobs in this queue
   */
  public Comparator<JobSchedulingInfo> comparator;
  
  int maxJobsToInit;
  int maxJobsToAccept;
  int maxJobsPerUserToInit;
  int maxJobsPerUserToAccept;
  int maxActiveTasks;
  int maxActiveTasksPerUser;

  // comparator for jobs in queues that don't support priorities
  private static final Comparator<JobSchedulingInfo> STARTTIME_JOB_COMPARATOR
    = new Comparator<JobSchedulingInfo>() {
    public int compare(JobSchedulingInfo o1, JobSchedulingInfo o2) {
      // the job that started earlier wins
      if (o1.getStartTime() < o2.getStartTime()) {
        return -1;
      } else {
        return (o1.getStartTime() == o2.getStartTime() 
                ? o1.getJobID().compareTo(o2.getJobID()) 
                : 1);
      }
    }
  };

  public CapacitySchedulerQueue(String queueName, CapacitySchedulerConf conf) {
    this.queueName = queueName;

    // Do not allow changes to 'supportsPriorities'
    supportsPriorities = conf.isPrioritySupported(queueName);

    initializeQueue(conf);

    if (supportsPriorities) {
      // use the default priority-aware comparator
      comparator = JobQueueJobInProgressListener.FIFO_JOB_QUEUE_COMPARATOR;
    }
    else {
      comparator = STARTTIME_JOB_COMPARATOR;
    }
    this.waitingJobs = 
      new TreeMap<JobSchedulingInfo, JobInProgress>(comparator);
    this.initializingJobs =
      new TreeMap<JobSchedulingInfo, JobInProgress>(comparator);
    this.runningJobs = 
      new TreeMap<JobSchedulingInfo, JobInProgress>(comparator);

    this.mapSlots = new SlotsUsage();
    this.reduceSlots = new SlotsUsage();    
  }
  
  synchronized void init(float capacityPercent, float maxCapacityPercent,
      int ulMin, float ulMinFactor,
      int maxJobsToInit, int maxJobsPerUserToInit,
      int maxActiveTasks, int maxActiveTasksPerUser,
      int maxJobsToAccept, int maxJobsPerUserToAccept) {
    this.capacityPercent = capacityPercent;
    this.maxCapacityPercent = maxCapacityPercent;
    this.ulMin = ulMin;
    this.ulMinFactor = ulMinFactor;
    
    this.maxJobsToInit = maxJobsToInit;
    this.maxJobsPerUserToInit = maxJobsPerUserToInit; 
    this.maxActiveTasks = maxActiveTasks;
    this.maxActiveTasksPerUser = maxActiveTasksPerUser; 
    this.maxJobsToAccept = maxJobsToAccept;
    this.maxJobsPerUserToAccept = maxJobsPerUserToAccept;
    
    LOG.info("Initializing '" + queueName + "' queue with " +
        "cap=" + capacityPercent + ", " +
        "maxCap=" + maxCapacityPercent + ", " +
        "ulMin=" + ulMin + ", " +
        "ulMinFactor=" + ulMinFactor + ", " +
        "supportsPriorities=" + supportsPriorities + ", " +
        "maxJobsToInit=" + maxJobsToInit + ", " +
        "maxJobsToAccept=" + maxJobsToAccept + ", " +
        "maxActiveTasks=" + maxActiveTasks + ", " +
        "maxJobsPerUserToInit=" + maxJobsPerUserToInit + ", " +
        "maxJobsPerUserToAccept=" + maxJobsPerUserToAccept + ", " +
        "maxActiveTasksPerUser=" + maxActiveTasksPerUser
    );
    
    // Sanity checks
    if (maxActiveTasks < maxActiveTasksPerUser ||
        maxJobsToInit < maxJobsPerUserToInit || 
        maxJobsToAccept < maxJobsPerUserToAccept) {
      throw new IllegalArgumentException("Illegal queue configuration for " +
      		"queue '" + queueName + "'");
    }
  }
  
  synchronized void initializeQueue(CapacitySchedulerQueue other) {
    init(other.capacityPercent, other.maxCapacityPercent, 
        other.ulMin, other.ulMinFactor, 
        other.maxJobsToInit, other.maxJobsPerUserToInit, 
        other.maxActiveTasks, other.maxActiveTasksPerUser, 
        other.maxJobsToAccept, other.maxJobsPerUserToAccept);
  }
  
  synchronized void initializeQueue(CapacitySchedulerConf conf) {
    float capacityPercent = conf.getCapacity(queueName);
    float maxCapacityPercent = conf.getMaxCapacity(queueName);
    int ulMin = conf.getMinimumUserLimitPercent(queueName);
    float ulMinFactor = conf.getUserLimitFactor(queueName);
    
    int maxSystemJobs = conf.getMaxSystemJobs();
    int maxJobsToInit = (int)Math.ceil(maxSystemJobs * capacityPercent/100.0);
    int maxJobsPerUserToInit = 
      (int)Math.ceil(maxSystemJobs * capacityPercent/100.0 * ulMin/100.0);
    int maxActiveTasks = conf.getMaxInitializedActiveTasks(queueName);
    int maxActiveTasksPerUser = 
      conf.getMaxInitializedActiveTasksPerUser(queueName);

    int jobInitToAcceptFactor = conf.getInitToAcceptJobsFactor(queueName);
    int maxJobsToAccept = maxJobsToInit * jobInitToAcceptFactor;
    int maxJobsPerUserToAccept = maxJobsPerUserToInit * jobInitToAcceptFactor;
    
    init(capacityPercent, maxCapacityPercent, 
        ulMin, ulMinFactor, 
        maxJobsToInit, maxJobsPerUserToInit, 
        maxActiveTasks, maxActiveTasksPerUser, 
        maxJobsToAccept, maxJobsPerUserToAccept);
  }

  /**
   * @return the queueName
   */
  String getQueueName() {
    return queueName;
  }

  /**
   * @return the capacityPercent
   */
  float getCapacityPercent() {
    return capacityPercent;
  }

  /**
   * reset the variables associated with tasks
   */
  void resetSlotsUsage(TaskType taskType) {
    if (taskType == TaskType.MAP) {
      mapSlots.reset();
    } else if (taskType == TaskType.REDUCE) {
      reduceSlots.reset();
    } else {    
      throw new IllegalArgumentException("Illegal taskType=" + taskType);
    }
  }


  /**
   * Returns the actual capacity in terms of slots for the <code>taskType</code>.
   * @param taskType
   * @return actual capacity in terms of slots for the <code>taskType</code>
   */
  int getCapacity(TaskType taskType) {
    if (taskType == TaskType.MAP) {
      return mapSlots.getCapacity();
    } else if (taskType == TaskType.REDUCE) {
      return reduceSlots.getCapacity();
    }

    throw new IllegalArgumentException("Illegal taskType=" + taskType);
  }

  /**
   * Get the number of running tasks of the given <code>taskType</code>.
   * @param taskType
   * @return
   */
  int getNumRunningTasks(TaskType taskType) {
    if (taskType == TaskType.MAP) {
      return mapSlots.getNumRunningTasks();
    } else if (taskType == TaskType.REDUCE) {
      return reduceSlots.getNumRunningTasks();
    }
    
    throw new IllegalArgumentException("Illegal taskType=" + taskType);
  }

  /**
   * Get number of slots occupied of the <code>taskType</code>.
   * @param taskType
   * @return number of slots occupied of the <code>taskType</code>
   */
  int getNumSlotsOccupied(TaskType taskType) {
    if (taskType == TaskType.MAP) {
      return mapSlots.getNumSlotsOccupied();
    } else if (taskType == TaskType.REDUCE) {
      return reduceSlots.getNumSlotsOccupied();
    }
    
    throw new IllegalArgumentException("Illegal taskType=" + taskType);
  }

  /**
   * Get maximum number of slots for the <code>taskType</code>.
   * @param taskType
   * @return maximum number of slots for the <code>taskType</code>
   */
  int getMaxCapacity(TaskType taskType) {
    if (taskType == TaskType.MAP) {
      return mapSlots.getMaxCapacity();
    } else if (taskType == TaskType.REDUCE) {
      return reduceSlots.getMaxCapacity();
    }
    
    throw new IllegalArgumentException("Illegal taskType=" + taskType);
  }

  /**
   * Get number of slots occupied by a <code>user</code> of 
   * <code>taskType</code>.
   * @param user
   * @param taskType
   * @return number of slots occupied by a <code>user</code> of 
   *         <code>taskType</code>
   */
  int getNumSlotsOccupiedByUser(String user, TaskType taskType) {
    if (taskType == TaskType.MAP) {
      return mapSlots.getNumSlotsOccupiedByUser(user);
    } else if (taskType == TaskType.REDUCE) {
      return reduceSlots.getNumSlotsOccupiedByUser(user);
    }
    
    throw new IllegalArgumentException("Illegal taskType=" + taskType);
  }
  
  int getNumActiveUsersByTaskType(TaskType taskType) {
    if (taskType == TaskType.MAP) {
      return mapSlots.getNumActiveUsers();
    } else if (taskType == TaskType.REDUCE) {
      return reduceSlots.getNumActiveUsers();
    }
    
    throw new IllegalArgumentException("Illegal taskType=" + taskType);
  }
  
  /**
   * A new job is added to the 
   * @param job
   */
  void jobAdded(JobInProgress job) {
    // update user-specific info
    String user = job.getProfile().getUser();
    
    Integer i = numJobsByUser.get(user);
    if (null == i) {
      i = 1;
      // set the count for running tasks to 0
      mapSlots.numSlotsOccupiedByUser.put(user, 0);
      reduceSlots.numSlotsOccupiedByUser.put(user, 0);
    }
    else {
      i++;
    }
    numJobsByUser.put(user, i);
  }
  
  int getNumJobsByUser(String user) {
    Integer numJobs = numJobsByUser.get(user);
    return (numJobs != null) ? numJobs : 0;
  }
  
  /**
   * A job from the queue has completed.
   * @param job
   */
  void jobCompleted(JobInProgress job) {
    String user = job.getProfile().getUser();
    // update numJobsByUser
    if (LOG.isDebugEnabled()) {
      LOG.debug("Job to be removed for user " + user);
    }
    Integer i = numJobsByUser.get(job.getProfile().getUser());
    i--;  // i should never be null!
    if (0 == i.intValue()) {
      numJobsByUser.remove(user);
      // remove job footprint from our TSIs
      mapSlots.numSlotsOccupiedByUser.remove(user);
      reduceSlots.numSlotsOccupiedByUser.remove(user);
      if (LOG.isDebugEnabled()) {
        LOG.debug("No more jobs for user, number of users = " + 
            numJobsByUser.size());
      }
    }
    else {
      numJobsByUser.put(user, i);
      if (LOG.isDebugEnabled()) {
        LOG.debug("User still has " + i + " jobs, number of users = "
                + numJobsByUser.size());
      }
    }
  }
  
  /**
   * Update queue usage.
   * @param type
   * @param user
   * @param numRunningTasks
   * @param numSlotsOccupied
   */
  void update(TaskType type, JobInProgress job, String user, 
      int numRunningTasks, int numSlotsOccupied) {
    if (type == TaskType.MAP) {
      mapSlots.updateSlotsUsage(user, job.pendingMaps(), 
          numRunningTasks, numSlotsOccupied);
    } else if (type == TaskType.REDUCE) {
      reduceSlots.updateSlotsUsage(user, job.pendingReduces(), 
          numRunningTasks, numSlotsOccupied);
    }
  }
  
  /**
   * Update queue usage across all running jobs.
   * @param mapClusterCapacity
   * @param reduceClusterCapacity
   * @param mapScheduler
   * @param reduceScheduler
   */
  void updateAll(int mapClusterCapacity, int reduceClusterCapacity, 
      TaskSchedulingMgr mapScheduler, TaskSchedulingMgr reduceScheduler) {
   // Compute new capacities for maps and reduces
    mapSlots.updateCapacities(capacityPercent, maxCapacityPercent, 
        mapClusterCapacity);
    reduceSlots.updateCapacities(capacityPercent, maxCapacityPercent, 
        reduceClusterCapacity);

    // reset running/pending tasks, tasks per user
    resetSlotsUsage(TaskType.MAP);
    resetSlotsUsage(TaskType.REDUCE);
    
    Collection<JobInProgress> jobs = getRunningJobs(); // Safe to iterate since
                                                       // we get a copy here
    for (JobInProgress j : jobs) {
      if (j.getStatus().getRunState() != JobStatus.RUNNING) {
        continue;
      }

      int numMapsRunningForThisJob = mapScheduler.getRunningTasks(j);
      int numReducesRunningForThisJob = reduceScheduler.getRunningTasks(j);
      int numRunningMapSlots = 
        numMapsRunningForThisJob * mapScheduler.getSlotsPerTask(j);
      int numRunningReduceSlots =
        numReducesRunningForThisJob * reduceScheduler.getSlotsPerTask(j);
      int numMapSlotsForThisJob = mapScheduler.getSlotsOccupied(j);
      int numReduceSlotsForThisJob = reduceScheduler.getSlotsOccupied(j);
      int numReservedMapSlotsForThisJob = 
        (mapScheduler.getNumReservedTaskTrackers(j) * 
         mapScheduler.getSlotsPerTask(j)); 
      int numReservedReduceSlotsForThisJob = 
        (reduceScheduler.getNumReservedTaskTrackers(j) * 
         reduceScheduler.getSlotsPerTask(j)); 
      
      j.setSchedulingInfo(
          CapacityTaskScheduler.getJobQueueSchedInfo(numMapsRunningForThisJob, 
              numRunningMapSlots,
              numReservedMapSlotsForThisJob,
              numReducesRunningForThisJob, 
              numRunningReduceSlots,
              numReservedReduceSlotsForThisJob));

      update(TaskType.MAP, j, j.getProfile().getUser(), 
          numMapsRunningForThisJob, numMapSlotsForThisJob);
      update(TaskType.REDUCE, j, j.getProfile().getUser(), 
          numReducesRunningForThisJob, numReduceSlotsForThisJob);

      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format(queueName + " - updateQSI: job %s: run(m)=%d, "
            + "occupied(m)=%d, run(r)=%d, occupied(r)=%d, finished(m)=%d,"
            + " finished(r)=%d, failed(m)=%d, failed(r)=%d, "
            + "spec(m)=%d, spec(r)=%d, total(m)=%d, total(r)=%d", j
            .getJobID().toString(), Integer
            .valueOf(numMapsRunningForThisJob), Integer
            .valueOf(numMapSlotsForThisJob), Integer
            .valueOf(numReducesRunningForThisJob), Integer
            .valueOf(numReduceSlotsForThisJob), Integer.valueOf(j
            .finishedMaps()), Integer.valueOf(j.finishedReduces()), Integer
            .valueOf(j.failedMapTasks),
            Integer.valueOf(j.failedReduceTasks), Integer
                .valueOf(j.speculativeMapTasks), Integer
                .valueOf(j.speculativeReduceTasks), Integer
                .valueOf(j.numMapTasks), Integer.valueOf(j.numReduceTasks)));
      }
    }
  }
  
  boolean doesQueueSupportPriorities() {
    return supportsPriorities;
  }

  /**
   * return information about the queue
   *
   * @return a String representing the information about the 
   */
  @Override
  public String toString(){
    // We print out the queue information first, followed by info
    // on map and reduce tasks and job info
    StringBuilder sb = new StringBuilder();
    sb.append("Queue configuration\n");
    sb.append("Capacity Percentage: ");
    sb.append(capacityPercent);
    sb.append("%\n");
    sb.append("User Limit: " + ulMin + "%\n");
    sb.append("Priority Supported: " +
        (doesQueueSupportPriorities() ? "YES":"NO") + "\n");
    sb.append("-------------\n");

    sb.append("Map tasks\n");
    sb.append(mapSlots.toString());
    sb.append("-------------\n");
    sb.append("Reduce tasks\n");
    sb.append(reduceSlots.toString());
    sb.append("-------------\n");
    
    sb.append("Job info\n");
    sb.append("Number of Waiting Jobs: " + getNumWaitingJobs() + "\n");
    sb.append("Number of Initializing Jobs: " + getNumInitializingJobs() + "\n");
    sb.append("Number of users who have submitted jobs: " + 
        numJobsByUser.size() + "\n");
    return sb.toString();
  }
  
  /**
   * Functionality to deal with job initialization
   */

  
  // per-user information
  static class UserInfo {
    
    Map<JobSchedulingInfo, JobInProgress> waitingJobs; // for waiting jobs
    Map<JobSchedulingInfo, JobInProgress> initializingJobs; // for init'ing jobs
    Map<JobSchedulingInfo, JobInProgress> runningJobs; // for running jobs
    
    int activeTasks;
    
    public UserInfo(Comparator<JobSchedulingInfo> comparator) {
      waitingJobs = new TreeMap<JobSchedulingInfo, JobInProgress>(comparator);
      initializingJobs = new TreeMap<JobSchedulingInfo, JobInProgress>(comparator);
      runningJobs = new TreeMap<JobSchedulingInfo, JobInProgress>(comparator);
    }
    
    int getNumInitializingJobs() {
      return initializingJobs.size();
    }
    
    int getNumRunningJobs() {
      return runningJobs.size();
    }
    
    int getNumWaitingJobs() {
      return waitingJobs.size();
    }
    
    int getNumActiveTasks() {
      return activeTasks;
    }
    
    public void jobAdded(JobSchedulingInfo jobSchedInfo, JobInProgress job) {
      waitingJobs.put(jobSchedInfo, job); 
    }
    
    public void removeWaitingJob(JobSchedulingInfo jobSchedInfo) {
      waitingJobs.remove(jobSchedInfo);
    }
    
    public void jobInitializing(JobSchedulingInfo jobSchedInfo, 
        JobInProgress job) {
      if (!initializingJobs.containsKey(jobSchedInfo)) {
        initializingJobs.put(jobSchedInfo, job);
        activeTasks += job.desiredTasks();
      }
    }
    
    public void removeInitializingJob(JobSchedulingInfo jobSchedInfo) {
      initializingJobs.remove(jobSchedInfo);
    }
    
    public void jobInitialized(JobSchedulingInfo jobSchedInfo, 
        JobInProgress job) {
      runningJobs.put(jobSchedInfo, job);
    }
    
    public void jobCompleted(JobSchedulingInfo jobSchedInfo, 
        JobInProgress job) {
      // It is *ok* to remove from runningJobs even if the job was never RUNNING
      runningJobs.remove(jobSchedInfo);
      activeTasks -= job.desiredTasks();
    }
    
    boolean isInactive() {
      return activeTasks == 0 && runningJobs.size() == 0  && 
      waitingJobs.size() == 0 && initializingJobs.size() == 0;
    }
  }

  synchronized Collection<JobInProgress> getWaitingJobs() {
    return Collections.unmodifiableCollection(
        new LinkedList<JobInProgress>(waitingJobs.values()));
  }
  
  synchronized Collection<JobInProgress> getInitializingJobs() {
    return Collections.unmodifiableCollection(
        new LinkedList<JobInProgress>(initializingJobs.values()));
  }
  
  synchronized Collection<JobInProgress> getRunningJobs() {
    return Collections.unmodifiableCollection(
        new LinkedList<JobInProgress>(runningJobs.values())); 
  }
  
  synchronized int getNumActiveTasks() {
    return activeTasks;
  }
  
  synchronized int getNumRunningJobs() {
    return runningJobs.size();
  }
  
  synchronized int getNumInitializingJobs() {
    return initializingJobs.size();
  }
  
  synchronized int getNumInitializingJobsByUser(String user) {
    UserInfo userInfo = users.get(user);
    return (userInfo == null) ? 0 : userInfo.getNumInitializingJobs();
  }
  
  synchronized int getNumRunningJobsByUser(String user) {
    UserInfo userInfo = users.get(user);
    return (userInfo == null) ? 0 : userInfo.getNumRunningJobs();
  }

  synchronized int getNumActiveTasksByUser(String user) {
    UserInfo userInfo = users.get(user);
    return (userInfo == null) ? 0 : userInfo.getNumActiveTasks();
  }

  synchronized int getNumWaitingJobsByUser(String user) {
    UserInfo userInfo = users.get(user);
    return (userInfo == null) ? 0 : userInfo.getNumWaitingJobs();
  }

  synchronized void addInitializingJob(JobInProgress job) {
    JobSchedulingInfo jobSchedInfo = new JobSchedulingInfo(job);

    if (!waitingJobs.containsKey(jobSchedInfo)) {
      // Ideally this should have been an *assert*, but it can't be done
      // since we make copies in getWaitingJobs which is used in 
      // JobInitPoller.getJobsToInitialize
      LOG.warn("Cannot find job " + job.getJobID() + 
          " in list of waiting jobs!");
      return;
    }
    
    if (initializingJobs.containsKey(jobSchedInfo)) {
      LOG.warn("job " + job.getJobID() + " already being init'ed in queue'" +
          queueName + "'!");
      return;
    }

    // Mark the job as running
    initializingJobs.put(jobSchedInfo, job);

    addJob(jobSchedInfo, job);
    
    if (LOG.isDebugEnabled()) {
      String user = job.getProfile().getUser();
      LOG.debug("addInitializingJob:" +
          " job=" + job.getJobID() +
          " user=" + user + 
          " queue=" + queueName +
          " qWaitJobs=" +  getNumWaitingJobs() +
          " qInitJobs=" +  getNumInitializingJobs()+
          " qRunJobs=" +  getNumRunningJobs() +
          " qActiveTasks=" +  getNumActiveTasks() +
          " uWaitJobs=" +  getNumWaitingJobsByUser(user) +
          " uInitJobs=" +  getNumInitializingJobsByUser(user) +
          " uRunJobs=" +  getNumRunningJobsByUser(user) +
          " uActiveTasks=" +  getNumActiveTasksByUser(user)
      );
    }

    // Remove the job from 'waiting' jobs list
    removeWaitingJob(jobSchedInfo, JobStatus.PREP);
  }
  
  synchronized JobInProgress removeInitializingJob(
      JobSchedulingInfo jobSchedInfo, int runState) {
    JobInProgress job = initializingJobs.remove(jobSchedInfo);
    
    if (job != null) {
      String user = job.getProfile().getUser();
      UserInfo userInfo = users.get(user);
      userInfo.removeInitializingJob(jobSchedInfo);
      
      // Decrement counts if the job is killed _while_ it was selected for
      // initialization, but aborted
      // NOTE: addRunningJob calls removeInitializingJob with runState==RUNNING
      if (runState != JobStatus.RUNNING) {
        finishJob(jobSchedInfo, job);
      }
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("removeInitializingJob:" +
            " job=" + job.getJobID() +
            " user=" + user + 
            " queue=" + queueName +
            " qWaitJobs=" +  getNumWaitingJobs() +
            " qInitJobs=" +  getNumInitializingJobs()+
            " qRunJobs=" +  getNumRunningJobs() +
            " qActiveTasks=" +  getNumActiveTasks() +
            " uWaitJobs=" +  getNumWaitingJobsByUser(user) +
            " uInitJobs=" +  getNumInitializingJobsByUser(user) +
            " uRunJobs=" +  getNumRunningJobsByUser(user) +
            " uActiveTasks=" +  getNumActiveTasksByUser(user)
        );
      }
    }
    
    return job;
  }
  
  synchronized void addRunningJob(JobInProgress job) {
    JobSchedulingInfo jobSchedInfo = new JobSchedulingInfo(job);

    if (runningJobs.containsKey(jobSchedInfo)) {
      LOG.info("job " + job.getJobID() + " already running in queue'" +
          queueName + "'!");
      return;
    }

    // Mark the job as running
    runningJobs.put(jobSchedInfo,job);

    // Update user stats
    String user = job.getProfile().getUser();
    UserInfo userInfo = users.get(user);
    userInfo.jobInitialized(jobSchedInfo, job);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("addRunningJob:" +
          " job=" + job.getJobID() +
          " user=" + user + 
          " queue=" + queueName +
          " qWaitJobs=" +  getNumWaitingJobs() +
          " qInitJobs=" +  getNumInitializingJobs()+
          " qRunJobs=" +  getNumRunningJobs() +
          " qActiveTasks=" +  getNumActiveTasks() +
          " uWaitJobs=" +  getNumWaitingJobsByUser(user) +
          " uInitJobs=" +  getNumInitializingJobsByUser(user) +
          " uRunJobs=" +  getNumRunningJobsByUser(user) +
          " uActiveTasks=" +  getNumActiveTasksByUser(user)
      );
    }

    // Remove from 'initializing' list
    // Note that at this point job.status.state != RUNNING, 
    // however, logically it is a reasonable state to pass in to ensure
    // that removeInitializingJob doesn't double-decrement  
    // the relevant queue/user counters
    removeInitializingJob(jobSchedInfo, JobStatus.RUNNING);
  }

  synchronized private void addJob(JobSchedulingInfo jobSchedInfo,
      JobInProgress job) {
    // Update queue stats
    activeTasks += job.desiredTasks();
    
    // Update user stats
    String user = job.getProfile().getUser();
    UserInfo userInfo = users.get(user);
    userInfo.jobInitializing(jobSchedInfo, job);
  }
  
  synchronized private void finishJob(JobSchedulingInfo jobSchedInfo,
      JobInProgress job) {
    // Update user stats
    String user = job.getProfile().getUser();
    UserInfo userInfo = users.get(user);
    userInfo.jobCompleted(jobSchedInfo, job);
    
    if (userInfo.isInactive()) {
      users.remove(userInfo);
    }

    // Update queue stats
    activeTasks -= job.desiredTasks();
  }
  
  synchronized JobInProgress removeRunningJob(JobSchedulingInfo jobSchedInfo, 
      int runState) {
    JobInProgress job = runningJobs.remove(jobSchedInfo); 

    // We have to be careful, we might be trying to remove a job  
    // which might not have been initialized
    if (job != null) {
      String user = job.getProfile().getUser();
      finishJob(jobSchedInfo, job);
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("removeRunningJob:" +
            " job=" + job.getJobID() +
            " user=" + user + 
            " queue=" + queueName +
            " qWaitJobs=" +  getNumWaitingJobs() +
            " qInitJobs=" +  getNumInitializingJobs()+
            " qRunJobs=" +  getNumRunningJobs() +
            " qActiveTasks=" +  getNumActiveTasks() +
            " uWaitJobs=" +  getNumWaitingJobsByUser(user) +
            " uInitJobs=" +  getNumInitializingJobsByUser(user) +
            " uRunJobs=" +  getNumRunningJobsByUser(user) +
            " uActiveTasks=" +  getNumActiveTasksByUser(user)
        );
      }
    }

    return job;
  }
  
  synchronized void addWaitingJob(JobInProgress job) throws IOException {
    JobSchedulingInfo jobSchedInfo = new JobSchedulingInfo(job);
    if (waitingJobs.containsKey(jobSchedInfo)) {
      LOG.info("job " + job.getJobID() + " already waiting in queue '" + 
          queueName + "'!");
      return;
    }
    
    String user = job.getProfile().getUser();

    // Check acceptance limits
    checkJobSubmissionLimits(job, user);
    
    waitingJobs.put(jobSchedInfo, job);
    
    // Update user stats
    UserInfo userInfo = users.get(user);
    if (userInfo == null) {
      userInfo = new UserInfo(comparator);
      users.put(user, userInfo);
    }
    userInfo.jobAdded(jobSchedInfo, job);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("addWaitingJob:" +
          " job=" + job.getJobID() +
          " user=" + user + 
          " queue=" + queueName +
          " qWaitJobs=" +  getNumWaitingJobs() +
          " qInitJobs=" +  getNumInitializingJobs()+
          " qRunJobs=" +  getNumRunningJobs() +
          " qActiveTasks=" +  getNumActiveTasks() +
          " uWaitJobs=" +  getNumWaitingJobsByUser(user) +
          " uInitJobs=" +  getNumInitializingJobsByUser(user) +
          " uRunJobs=" +  getNumRunningJobsByUser(user) +
          " uActiveTasks=" +  getNumActiveTasksByUser(user)
      );
    }
  }
  
  synchronized JobInProgress removeWaitingJob(JobSchedulingInfo jobSchedInfo, 
      int unused) {
    JobInProgress job = waitingJobs.remove(jobSchedInfo);
    if (job != null) {
      String user = job.getProfile().getUser();
      UserInfo userInfo = users.get(user);
      userInfo.removeWaitingJob(jobSchedInfo);

      if (LOG.isDebugEnabled()) {
        LOG.debug("removeWaitingJob:" +
            " job=" + job.getJobID() +
            " user=" + user + 
            " queue=" + queueName +
            " qWaitJobs=" +  getNumWaitingJobs() +
            " qInitJobs=" +  getNumInitializingJobs()+
            " qRunJobs=" +  getNumRunningJobs() +
            " qActiveTasks=" +  getNumActiveTasks() +
            " uWaitJobs=" +  getNumWaitingJobsByUser(user) +
            " uInitJobs=" +  getNumInitializingJobsByUser(user) +
            " uRunJobs=" +  getNumRunningJobsByUser(user) +
            " uActiveTasks=" +  getNumActiveTasksByUser(user)
        );
      }
    }
    
    return job;
  }

  synchronized int getNumActiveUsers() {
    return users.size();
  }
  
  synchronized int getNumWaitingJobs() {
    return waitingJobs.size(); 
  } 
  
  Comparator<JobSchedulingInfo> getComparator() {
    return comparator;
  }
  
  /**
   * Functions to deal with queue-limits.
   */
  
  /**
   * Check if the queue can be assigned <code>numSlots</code> 
   * of the given <code>taskType</code> so that the queue doesn't exceed its
   * configured maximum-capacity.
   * 
   * @param taskType
   * @param numSlots
   * @return <code>true</code> if slots can be assigned
   */
  boolean assignSlotsToQueue(TaskType taskType, int numSlots) {
    // Check if the queue is running over it's maximum-capacity
    if (getMaxCapacity(taskType) > 0) {  // Check if max capacity is enabled
        if ((getNumSlotsOccupied(taskType) + numSlots) > 
             getMaxCapacity(taskType)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "Queue " + queueName + " " + "has reached its  max " + 
                taskType + " capacity");
            LOG.debug("Current running tasks " + getCapacity(taskType));
          }
          return false;
        }
      }
    
    return true;
  }
  /**
   * Check if the given <code>job</code> and <code>user</code> and 
   * queue can be assigned the requested number of slots of 
   * the given <code>taskType</code> for the .
   * 
   * This checks to ensure that queue and user are under appropriate limits.
   * 
   * @param taskType
   * @param job
   * @param user
   * @return <code>true</code> if the given job/user/queue can be assigned 
   * the requested number of slots, <code>false</code> otherwise
   */
  boolean assignSlotsToJob(TaskType taskType, JobInProgress job, String user) {
    int numSlotsRequested = job.getNumSlotsPerTask(taskType);
    
    // Check to ensure we will not go over the queue's max-capacity
    if (!assignSlotsToQueue(taskType, numSlotsRequested)) {
      return false;
    }
    
    // What is our current capacity? 
    // * It is equal to the max(numSlotsRequested queue-capacity) if
    //   we're running below capacity. The 'max' ensures that jobs in queues
    //   with miniscule capacity (< 1 slot) make progress
    // * If we're running over capacity, then its
    //   #running plus slotPerTask of the job (which is the number of extra
    //   slots we're getting).
    
    // Allow progress for queues with miniscule capacity
    int queueCapacity = Math.max(getCapacity(taskType), numSlotsRequested);
    
    int queueSlotsOccupied = getNumSlotsOccupied(taskType);
    int currentCapacity;
    if (queueSlotsOccupied < queueCapacity) {
      currentCapacity = queueCapacity;
    }
    else {
      currentCapacity = queueSlotsOccupied + numSlotsRequested;
    }
    
    // Never allow a single user to take more than the 
    // queue's configured capacity * user-limit-factor.
    // Also, the queue's configured capacity should be higher than 
    // queue-hard-limit * ulMin
    
    // All users in this queue might not need any slots of type 'taskType'
    int activeUsers = Math.max(1, getNumActiveUsersByTaskType(taskType));  
    
    int limit = 
      Math.min(
          Math.max(divideAndCeil(currentCapacity, activeUsers), 
                   divideAndCeil(ulMin*currentCapacity, 100)),
          (int)(queueCapacity * ulMinFactor)
          );

    if ((getNumSlotsOccupiedByUser(user, taskType) + numSlotsRequested) > 
        limit) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("User " + user + " is over limit for queue=" + queueName + 
            " queueCapacity=" + queueCapacity +
            " num slots occupied=" + getNumSlotsOccupiedByUser(user, taskType) + 
            " limit=" + limit +" numSlotsRequested=" + numSlotsRequested + 
            " currentCapacity=" + currentCapacity + 
            " numActiveUsers=" + getNumActiveUsersByTaskType(taskType));
      }
      return false;
    }

    return true;
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
  private static int divideAndCeil(int a, int b) {
    if (b == 0) {
      LOG.info("divideAndCeil called with a=" + a + " b=" + b);
      return 0;
    }
    return (a + (b - 1)) / b;
  }

  /**
   * Check if the given <code>job</code> can be accepted to the 
   * queue on behalf of the <code>user</code>.
   * @param job 
   * @param user
   * @return <code>true</code> if the job can be accepted, 
   *         <code>false</code> otherwise
   */
  synchronized void checkJobSubmissionLimits(JobInProgress job, String user) 
  throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("checkJobSubmissionLimits - " +
          "qWaitJobs=" + getNumWaitingJobs() + " " +
          "qInitJobs=" + getNumInitializingJobs() + " " +
          "qRunJobs=" + getNumRunningJobs() + " " +
          "maxJobsToAccept=" + maxJobsToAccept +
          "user=" + user + " " +
          "uWaitJobs=" +  getNumWaitingJobsByUser(user) + " " +
          "uRunJobs=" + getNumRunningJobsByUser(user)  + " " +
          "maxJobsPerUserToAccept=" + maxJobsPerUserToAccept + " " +
          "");
    }
    
    // Task limits - No point accepting the job if it can never be initialized
    if (job.desiredTasks() > maxActiveTasksPerUser) {
      throw new IOException(
          "Job '" + job.getJobID() + "' from user '" + user  +
          "' rejected since it has " + job.desiredTasks() + " tasks which" +
          " exceeds the limit of " + maxActiveTasksPerUser + 
          " tasks per-user which can be initialized for queue '" + 
          queueName + "'"
          );
    }
    
    // Across all jobs in queue
    int queueWaitingJobs = getNumWaitingJobs();
    int queueInitializingJobs = getNumInitializingJobs();
    int queueRunningJobs = getNumRunningJobs();
    if ((queueWaitingJobs + queueInitializingJobs + queueRunningJobs) >= 
      maxJobsToAccept) {
      throw new IOException(
          "Job '" + job.getJobID() + "' from user '" + user  + 
          "' rejected since queue '" + queueName + 
          "' already has " + queueWaitingJobs + " waiting jobs, " + 
          queueInitializingJobs + " initializing jobs and " + 
          queueRunningJobs + " running jobs - Exceeds limit of " +
          maxJobsToAccept + " jobs to accept");
    }
    
    // Across all jobs of the user
    int userWaitingJobs = getNumWaitingJobsByUser(user);
    int userInitializingJobs = getNumInitializingJobsByUser(user);
    int userRunningJobs = getNumRunningJobsByUser(user);
    if ((userWaitingJobs + userInitializingJobs + userRunningJobs) >= 
        maxJobsPerUserToAccept) {
      throw new IOException(
          "Job '" + job.getJobID() + "' rejected since user '" + user +  
          "' already has " + userWaitingJobs + " waiting jobs, " +
          userInitializingJobs + " initializing jobs and " +
          userRunningJobs + " running jobs - " +
          " Exceeds limit of " + maxJobsPerUserToAccept + " jobs to accept" +
          " in queue '" + queueName + "' per user");
    }
  }
  
  /**
   * Check if the <code>job</code> can be initialized in the queue.
   * 
   * @param job
   * @return <code>true</code> if the job can be initialized, 
   *         <code>false</code> otherwise
   */
  synchronized boolean initializeJobForQueue(JobInProgress job) {
    
    // Check if queue has sufficient number of jobs
    int runningJobs = getNumRunningJobs();
    int initializingJobs = getNumInitializingJobs();
    if ((runningJobs + initializingJobs) >= maxJobsToInit) {
      LOG.info(getQueueName() + " already has " + runningJobs + 
          " running jobs and " + initializingJobs + " initializing jobs;" +
          " cannot initialize " + job.getJobID() + 
          " since it will exceeed limit of " + maxJobsToInit + 
          " initialized jobs for this queue");
      return false;
    }
    
    // Check if queue has too many active tasks
    if ((activeTasks + job.desiredTasks()) > maxActiveTasks) {
      LOG.info("Queue '" + getQueueName() + "' has " + activeTasks + 
          " active tasks, cannot initialize job '" + job.getJobID() + 
          "' for user '" + job.getProfile().getUser() + "' with " +
          job.desiredTasks() + " tasks since it will exceed limit of " + 
          maxActiveTasks + " active tasks for this queue");
      return false;
    }
    
    return true;
  }
  
  /**
   * Check if the <code>job</code> can be initialized in the queue
   * on behalf of the <code>user</code>.
   * 
   * @param job
   * @return <code>true</code> if the job can be initialized, 
   *         <code>false</code> otherwise
   */
  synchronized boolean initializeJobForUser(JobInProgress job) {
    
    String user = job.getProfile().getUser();
    
    // Check if the user has too many jobs
    int userRunningJobs = getNumRunningJobsByUser(user);
    int userInitializingJobs = getNumInitializingJobsByUser(user);
    if ((userRunningJobs + userInitializingJobs) >= maxJobsPerUserToInit) {
      LOG.info(getQueueName() + " already has " + userRunningJobs + 
          " running jobs and " + userInitializingJobs + " initializing jobs" +
          " for user " + user + "; cannot initialize " + job.getJobID() + 
          " since it will exceeed limit of " + 
          maxJobsPerUserToInit + " initialized jobs per user for this queue");
      return false;
    }
    
    // Check if the user has too many active tasks
    int userActiveTasks = getNumActiveTasksByUser(user);
    if ((userActiveTasks + job.desiredTasks()) > maxActiveTasksPerUser) {
      LOG.info(getQueueName() + " has " + userActiveTasks + 
          " active tasks for user " + user + 
          ", cannot initialize " + job.getJobID() + " with " +
          job.desiredTasks() + " tasks since it will exceed limit of " + 
          maxActiveTasksPerUser + " active tasks per user for this queue");
      return false;
    }
    
    return true;
  }

}
