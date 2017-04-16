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
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;

/**
 * A {@link TaskScheduler} that implements fair sharing.
 */
public class FairScheduler extends TaskScheduler {
  public static final Log LOG = LogFactory.getLog(
      "org.apache.hadoop.mapred.FairScheduler");

  // How often fair shares are re-calculated
  protected long updateInterval = 2500;

  // How often to dump scheduler state to the event log
  protected long dumpInterval = 10000;
  
  // How often tasks are preempted (must be longer than a couple
  // of heartbeats to give task-kill commands a chance to act).
  protected long preemptionInterval = 15000;
  
  // Used to iterate through map and reduce task types
  private static final TaskType[] MAP_AND_REDUCE = 
    new TaskType[] {TaskType.MAP, TaskType.REDUCE};

  // Maximum locality delay when auto-computing locality delays
  private static final long MAX_AUTOCOMPUTED_LOCALITY_DELAY = 15000;
  
  protected PoolManager poolMgr;
  protected LoadManager loadMgr;
  protected TaskSelector taskSelector;
  protected WeightAdjuster weightAdjuster; // Can be null for no weight adjuster
  protected Map<JobInProgress, JobInfo> infos = // per-job scheduling variables
    new HashMap<JobInProgress, JobInfo>();
  protected long lastUpdateTime;           // Time when we last updated infos
  protected long lastPreemptionUpdateTime; // Time when we last updated preemption vars
  protected boolean initialized;  // Are we initialized?
  protected volatile boolean running; // Are we running?
  protected boolean assignMultiple; // Simultaneously assign map and reduce?
  protected int mapAssignCap = -1;    // Max maps to launch per heartbeat
  protected int reduceAssignCap = -1; // Max reduces to launch per heartbeat
  protected long localityDelay;       // Time to wait for node and rack locality
  protected boolean autoComputeLocalityDelay = false; // Compute locality delay
                                                      // from heartbeat interval
  protected boolean sizeBasedWeight; // Give larger weights to larger jobs
  protected boolean waitForMapsBeforeLaunchingReduces = true;
  protected boolean preemptionEnabled;
  protected boolean onlyLogPreemption; // Only log when tasks should be killed

  private Clock clock;

  private EagerTaskInitializationListener eagerInitListener;
  private JobListener jobListener;
  private boolean mockMode; // Used for unit tests; disables background updates
                            // and scheduler event log
  private FairSchedulerEventLog eventLog;
  protected long lastDumpTime;       // Time when we last dumped state to log
  protected long lastHeartbeatTime;  // Time we last ran assignTasks 
  private long lastPreemptCheckTime; // Time we last ran preemptTasksIfNecessary

  /**
   * A configuration property that controls the ability of submitting jobs to
   * pools not declared in the scheduler allocation file.
   */
  public final static String ALLOW_UNDECLARED_POOLS_KEY =
    "mapred.fairscheduler.allow.undeclared.pools";
  private boolean allowUndeclaredPools = false;

  private MetricsUpdater metricsUpdater; // responsible for pushing hadoop metrics

  /**
   * A class for holding per-job scheduler variables. These always contain the
   * values of the variables at the last update(), and are used along with a
   * time delta to update the map and reduce deficits before a new update().
   */
  static class JobInfo {
    boolean runnable = false;   // Can the job run given user/pool limits?
    public JobSchedulable mapSchedulable;
    public JobSchedulable reduceSchedulable;
    // Variables used for delay scheduling
    LocalityLevel lastMapLocalityLevel; // Locality level of last map launched
    long timeWaitedForLocalMap; // Time waiting for local map since last map
    boolean skippedAtLastHeartbeat;  // Was job skipped at previous assignTasks?
                                     // (used to update timeWaitedForLocalMap)
    public JobInfo(JobSchedulable mapSched, JobSchedulable reduceSched) {
      this.mapSchedulable = mapSched;
      this.reduceSchedulable = reduceSched;
      this.lastMapLocalityLevel = LocalityLevel.NODE;
    }
  }
  
  public FairScheduler() {
    this(new Clock(), false);
  }
  
  /**
   * Constructor used for tests, which can change the clock and disable updates.
   */
  protected FairScheduler(Clock clock, boolean mockMode) {
    this.clock = clock;
    this.mockMode = mockMode;
    this.jobListener = new JobListener();
  }

  @Override
  public void start() {
    try {
      Configuration conf = getConf();
      // Create scheduling log and initialize it if it is enabled
      eventLog = new FairSchedulerEventLog();
      boolean logEnabled = conf.getBoolean(
          "mapred.fairscheduler.eventlog.enabled", false);
      if (!mockMode && logEnabled) {
        String hostname = "localhost";
        if (taskTrackerManager instanceof JobTracker) {
          hostname = ((JobTracker) taskTrackerManager).getJobTrackerMachine();
        }
        eventLog.init(conf, hostname);
      }
      // Initialize other pieces of the scheduler
      taskTrackerManager.addJobInProgressListener(jobListener);
      if (!mockMode) {
        eagerInitListener = new EagerTaskInitializationListener(conf);
        eagerInitListener.setTaskTrackerManager(taskTrackerManager);
        eagerInitListener.start();
        taskTrackerManager.addJobInProgressListener(eagerInitListener);
      }

      poolMgr = new PoolManager(this);
      poolMgr.initialize();
      loadMgr = (LoadManager) ReflectionUtils.newInstance(
          conf.getClass("mapred.fairscheduler.loadmanager", 
              CapBasedLoadManager.class, LoadManager.class), conf);
      loadMgr.setTaskTrackerManager(taskTrackerManager);
      loadMgr.setEventLog(eventLog);
      loadMgr.start();
      taskSelector = (TaskSelector) ReflectionUtils.newInstance(
          conf.getClass("mapred.fairscheduler.taskselector", 
              DefaultTaskSelector.class, TaskSelector.class), conf);
      taskSelector.setTaskTrackerManager(taskTrackerManager);
      taskSelector.start();
      Class<?> weightAdjClass = conf.getClass(
          "mapred.fairscheduler.weightadjuster", null);
      if (weightAdjClass != null) {
        weightAdjuster = (WeightAdjuster) ReflectionUtils.newInstance(
            weightAdjClass, conf);
      }

      updateInterval = conf.getLong(
          "mapred.fairscheduler.update.interval", 2500);
      dumpInterval = conf.getLong(
          "mapred.fairscheduler.dump.interval", 10000);
      preemptionInterval = conf.getLong(
          "mapred.fairscheduler.preemption.interval", 15000);
      assignMultiple = conf.getBoolean(
          "mapred.fairscheduler.assignmultiple", true);
      mapAssignCap = conf.getInt(
          "mapred.fairscheduler.assignmultiple.maps", -1);
      reduceAssignCap = conf.getInt(
          "mapred.fairscheduler.assignmultiple.reduces", -1);
      sizeBasedWeight = conf.getBoolean(
          "mapred.fairscheduler.sizebasedweight", false);
      preemptionEnabled = conf.getBoolean(
          "mapred.fairscheduler.preemption", false);
      onlyLogPreemption = conf.getBoolean(
          "mapred.fairscheduler.preemption.only.log", false);
      localityDelay = conf.getLong(
          "mapred.fairscheduler.locality.delay", -1);
      allowUndeclaredPools = conf.getBoolean(ALLOW_UNDECLARED_POOLS_KEY, true);
      if (localityDelay == -1)
        autoComputeLocalityDelay = true; // Compute from heartbeat interval


      initialized = true;
      running = true;
      lastUpdateTime = clock.getTime();
      // Start a thread to update deficits every UPDATE_INTERVAL
      if (!mockMode) {
        new UpdateThread().start();
      }
      // Register servlet with JobTracker's Jetty server
      if (taskTrackerManager instanceof JobTracker) {
        JobTracker jobTracker = (JobTracker) taskTrackerManager;
        HttpServer infoServer = jobTracker.infoServer;
        infoServer.setAttribute("scheduler", this);
        infoServer.addServlet("scheduler", "/scheduler",
            FairSchedulerServlet.class);
      }
      
      initMetrics();
      
      eventLog.log("INITIALIZED");
    } catch (Exception e) {
      // Can't load one of the managers - crash the JobTracker now while it is
      // starting up so that the user notices.
      throw new RuntimeException("Failed to start FairScheduler", e);
    }
    LOG.info("Successfully configured FairScheduler");
  }

  /**
   * Register metrics for the fair scheduler, and start a thread
   * to update them periodically.
   */
  private void initMetrics() {
    MetricsContext context = MetricsUtil.getContext("fairscheduler");
    metricsUpdater = new MetricsUpdater();
    context.registerUpdater(metricsUpdater);
  }

  @Override
  public void terminate() throws IOException {
    if (eventLog != null)
      eventLog.log("SHUTDOWN");
    running = false;
    if (jobListener != null)
      taskTrackerManager.removeJobInProgressListener(jobListener);
    if (eagerInitListener != null)
      taskTrackerManager.removeJobInProgressListener(eagerInitListener);
    if (eventLog != null)
      eventLog.shutdown();
    if (metricsUpdater != null) {
      MetricsContext context = MetricsUtil.getContext("fairscheduler");
      context.unregisterUpdater(metricsUpdater);
      metricsUpdater = null;
    }
  }

  /**
   * Responsible for updating metrics when the metrics context requests it.
   */
  private class MetricsUpdater implements Updater {
    @Override
    public void doUpdates(MetricsContext context) {
      updateMetrics();
    }    
  }
  
  synchronized void updateMetrics() {
    poolMgr.updateMetrics();
  }
  
  /**
   * Used to listen for jobs added/removed by our {@link TaskTrackerManager}.
   */
  private class JobListener extends JobInProgressListener {
    @Override
    public void jobAdded(JobInProgress job) {
      synchronized (FairScheduler.this) {
        eventLog.log("JOB_ADDED", job.getJobID());
        JobInfo info = new JobInfo(new JobSchedulable(FairScheduler.this, job, TaskType.MAP),
            new JobSchedulable(FairScheduler.this, job, TaskType.REDUCE));
        infos.put(job, info);
        poolMgr.addJob(job); // Also adds job into the right PoolScheduable
        update();
      }
    }
    
    @Override
    public void jobRemoved(JobInProgress job) {
      synchronized (FairScheduler.this) {
        eventLog.log("JOB_REMOVED", job.getJobID());
        jobNoLongerRunning(job);
      }
    }
  
    @Override
    public void jobUpdated(JobChangeEvent event) {
      eventLog.log("JOB_UPDATED", event.getJobInProgress().getJobID());
    }
  }

  /**
   * A thread which calls {@link FairScheduler#update()} ever
   * <code>UPDATE_INTERVAL</code> milliseconds.
   */
  private class UpdateThread extends Thread {
    private UpdateThread() {
      super("FairScheduler update thread");
    }

    public void run() {
      while (running) {
        try {
          Thread.sleep(updateInterval);
          update();
          dumpIfNecessary();
          preemptTasksIfNecessary();
        } catch (Exception e) {
          LOG.error("Exception in fair scheduler UpdateThread", e);
        }
      }
    }
  }
  
  @Override
  public synchronized List<Task> assignTasks(TaskTracker tracker)
      throws IOException {
    if (!initialized) // Don't try to assign tasks if we haven't yet started up
      return null;
    String trackerName = tracker.getTrackerName();
    eventLog.log("HEARTBEAT", trackerName);
    long currentTime = clock.getTime();

    // Compute total runnable maps and reduces, and currently running ones
    int runnableMaps = 0;
    int runningMaps = 0;
    int runnableReduces = 0;
    int runningReduces = 0;
    for (Pool pool: poolMgr.getPools()) {
      runnableMaps += pool.getMapSchedulable().getDemand();
      runningMaps += pool.getMapSchedulable().getRunningTasks();
      runnableReduces += pool.getReduceSchedulable().getDemand();
      runningReduces += pool.getReduceSchedulable().getRunningTasks();
    }

    ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
    // Compute total map/reduce slots
    // In the future we can precompute this if the Scheduler becomes a 
    // listener of tracker join/leave events.
    int totalMapSlots = getTotalSlots(TaskType.MAP, clusterStatus);
    int totalReduceSlots = getTotalSlots(TaskType.REDUCE, clusterStatus);
    
    eventLog.log("RUNNABLE_TASKS", 
        runnableMaps, runningMaps, runnableReduces, runningReduces);

    // Update time waited for local maps for jobs skipped on last heartbeat
    updateLocalityWaitTimes(currentTime);
    
    TaskTrackerStatus tts = tracker.getStatus();

    int mapsAssigned = 0; // loop counter for map in the below while loop
    int reducesAssigned = 0; // loop counter for reduce in the below while
    int mapCapacity = maxTasksToAssign(TaskType.MAP, tts);
    int reduceCapacity = maxTasksToAssign(TaskType.REDUCE, tts);
    boolean mapRejected = false; // flag used for ending the loop
    boolean reduceRejected = false; // flag used for ending the loop

    // Keep track of which jobs were visited for map tasks and which had tasks
    // launched, so that we can later mark skipped jobs for delay scheduling
    Set<JobInProgress> visitedForMap = new HashSet<JobInProgress>();
    Set<JobInProgress> visitedForReduce = new HashSet<JobInProgress>();
    Set<JobInProgress> launchedMap = new HashSet<JobInProgress>();

    ArrayList<Task> tasks = new ArrayList<Task>();
    // Scan jobs to assign tasks until neither maps nor reduces can be assigned
    while (true) {
      // Computing the ending conditions for the loop
      // Reject a task type if one of the following condition happens
      // 1. number of assigned task reaches per heatbeat limit
      // 2. number of running tasks reaches runnable tasks
      // 3. task is rejected by the LoadManager.canAssign
      if (!mapRejected) {
        if (mapsAssigned == mapCapacity ||
            runningMaps == runnableMaps ||
            !loadMgr.canAssignMap(tts, runnableMaps,
                totalMapSlots, mapsAssigned)) {
          eventLog.log("INFO", "Can't assign another MAP to " + trackerName);
          mapRejected = true;
        }
      }
      if (!reduceRejected) {
        if (reducesAssigned == reduceCapacity ||
            runningReduces == runnableReduces ||
            !loadMgr.canAssignReduce(tts, runnableReduces,
                totalReduceSlots, reducesAssigned)) {
          eventLog.log("INFO", "Can't assign another REDUCE to " + trackerName);
          reduceRejected = true;
        }
      }
      // Exit while (true) loop if
      // 1. neither maps nor reduces can be assigned
      // 2. assignMultiple is off and we already assigned one task
      if (mapRejected && reduceRejected ||
          !assignMultiple && tasks.size() > 0) {
        break; // This is the only exit of the while (true) loop
      }

      // Determine which task type to assign this time
      // First try choosing a task type which is not rejected
      TaskType taskType;
      if (mapRejected) {
        taskType = TaskType.REDUCE;
      } else if (reduceRejected) {
        taskType = TaskType.MAP;
      } else {
        // If both types are available, choose the task type with fewer running
        // tasks on the task tracker to prevent that task type from starving
        if (tts.countMapTasks() + mapsAssigned <=
            tts.countReduceTasks() + reducesAssigned) {
          taskType = TaskType.MAP;
        } else {
          taskType = TaskType.REDUCE;
        }
      }

      // Get the map or reduce schedulables and sort them by fair sharing
      List<PoolSchedulable> scheds = getPoolSchedulables(taskType);
      Collections.sort(scheds, new SchedulingAlgorithms.FairShareComparator());
      boolean foundTask = false;
      for (Schedulable sched: scheds) { // This loop will assign only one task
        eventLog.log("INFO", "Checking for " + taskType +
            " task in " + sched.getName());
        Task task = taskType == TaskType.MAP ? 
                    sched.assignTask(tts, currentTime, visitedForMap) : 
                    sched.assignTask(tts, currentTime, visitedForReduce);
        if (task != null) {
          foundTask = true;
          JobInProgress job = taskTrackerManager.getJob(task.getJobID());
          eventLog.log("ASSIGN", trackerName, taskType,
              job.getJobID(), task.getTaskID());
          // Update running task counts, and the job's locality level
          if (taskType == TaskType.MAP) {
            launchedMap.add(job);
            mapsAssigned++;
            runningMaps++;
            updateLastMapLocalityLevel(job, task, tts);
          } else {
            reducesAssigned++;
            runningReduces++;
          }
          // Add task to the list of assignments
          tasks.add(task);
          break; // This break makes this loop assign only one task
        } // end if(task != null)
      } // end for(Schedulable sched: scheds)

      // Reject the task type if we cannot find a task
      if (!foundTask) {
        if (taskType == TaskType.MAP) {
          mapRejected = true;
        } else {
          reduceRejected = true;
        }
      }
    } // end while (true)

    // Mark any jobs that were visited for map tasks but did not launch a task
    // as skipped on this heartbeat
    for (JobInProgress job: visitedForMap) {
      if (!launchedMap.contains(job)) {
        infos.get(job).skippedAtLastHeartbeat = true;
      }
    }
    
    // If no tasks were found, return null
    return tasks.isEmpty() ? null : tasks;
  }

  /**
   * Get maximum number of tasks to assign on a TaskTracker on a heartbeat.
   * The scheduler may launch fewer than this many tasks if the LoadManager
   * says not to launch more, but it will never launch more than this number.
   */
  private int maxTasksToAssign(TaskType type, TaskTrackerStatus tts) {
    if (!assignMultiple)
      return 1;
    int cap = (type == TaskType.MAP) ? mapAssignCap : reduceAssignCap;
    if (cap == -1) // Infinite cap; use the TaskTracker's slot count
      return (type == TaskType.MAP) ?
          tts.getAvailableMapSlots(): tts.getAvailableReduceSlots();
    else
      return cap;
  }

  /**
   * Update locality wait times for jobs that were skipped at last heartbeat.
   */
  private void updateLocalityWaitTimes(long currentTime) {
    long timeSinceLastHeartbeat = 
      (lastHeartbeatTime == 0 ? 0 : currentTime - lastHeartbeatTime);
    lastHeartbeatTime = currentTime;
    for (JobInfo info: infos.values()) {
      if (info.skippedAtLastHeartbeat) {
        info.timeWaitedForLocalMap += timeSinceLastHeartbeat;
        info.skippedAtLastHeartbeat = false;
      }
    }
  }

  /**
   * Update a job's locality level and locality wait variables given that that 
   * it has just launched a map task on a given task tracker.
   */
  private void updateLastMapLocalityLevel(JobInProgress job,
      Task mapTaskLaunched, TaskTrackerStatus tracker) {
    JobInfo info = infos.get(job);
    LocalityLevel localityLevel = LocalityLevel.fromTask(
        job, mapTaskLaunched, tracker);
    info.lastMapLocalityLevel = localityLevel;
    info.timeWaitedForLocalMap = 0;
    eventLog.log("ASSIGNED_LOC_LEVEL", job.getJobID(), localityLevel);
  }

  /**
   * Get the maximum locality level at which a given job is allowed to
   * launch tasks, based on how long it has been waiting for local tasks.
   * This is used to implement the "delay scheduling" feature of the Fair
   * Scheduler for optimizing data locality.
   * If the job has no locality information (e.g. it does not use HDFS), this 
   * method returns LocalityLevel.ANY, allowing tasks at any level.
   * Otherwise, the job can only launch tasks at its current locality level
   * or lower, unless it has waited at least localityDelay milliseconds
   * (in which case it can go one level beyond) or 2 * localityDelay millis
   * (in which case it can go to any level).
   */
  protected LocalityLevel getAllowedLocalityLevel(JobInProgress job,
      long currentTime) {
    JobInfo info = infos.get(job);
    if (info == null) { // Job not in infos (shouldn't happen)
      LOG.error("getAllowedLocalityLevel called on job " + job
          + ", which does not have a JobInfo in infos");
      return LocalityLevel.ANY;
    }
    if (job.nonLocalMaps.size() > 0) { // Job doesn't have locality information
      return LocalityLevel.ANY;
    }
    // Don't wait for locality if the job's pool is starving for maps
    Pool pool = poolMgr.getPool(job);
    PoolSchedulable sched = pool.getMapSchedulable();
    long minShareTimeout = poolMgr.getMinSharePreemptionTimeout(pool.getName());
    long fairShareTimeout = poolMgr.getFairSharePreemptionTimeout();
    if (currentTime - sched.getLastTimeAtMinShare() > minShareTimeout ||
        currentTime - sched.getLastTimeAtHalfFairShare() > fairShareTimeout) {
      eventLog.log("INFO", "No delay scheduling for "
          + job.getJobID() + " because it is being starved");
      return LocalityLevel.ANY;
    }
    // In the common case, compute locality level based on time waited
    switch(info.lastMapLocalityLevel) {
    case NODE: // Last task launched was node-local
      if (info.timeWaitedForLocalMap >= 2 * localityDelay)
        return LocalityLevel.ANY;
      else if (info.timeWaitedForLocalMap >= localityDelay)
        return LocalityLevel.RACK;
      else
        return LocalityLevel.NODE;
    case RACK: // Last task launched was rack-local
      if (info.timeWaitedForLocalMap >= localityDelay)
        return LocalityLevel.ANY;
      else
        return LocalityLevel.RACK;
    default: // Last task was non-local; can launch anywhere
      return LocalityLevel.ANY;
    }
  }
  
  /**
   * Recompute the internal variables used by the scheduler - per-job weights,
   * fair shares, deficits, minimum slot allocations, and numbers of running
   * and needed tasks of each type. 
   */
  protected void update() {
    synchronized (taskTrackerManager) {
      synchronized (this) {
        ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();

        // Recompute locality delay from JobTracker heartbeat interval if enabled.
        // This will also lock the JT, so do it outside of a fair scheduler lock.
        if (autoComputeLocalityDelay) {
          JobTracker jobTracker = (JobTracker) taskTrackerManager;
          localityDelay = Math.min(MAX_AUTOCOMPUTED_LOCALITY_DELAY,
              (long) (1.5 * jobTracker.getNextHeartbeatInterval()));
        }

        // Reload allocations file if it hasn't been loaded in a while
        poolMgr.reloadAllocsIfNecessary();

        // Remove any jobs that have stopped running
        List<JobInProgress> toRemove = new ArrayList<JobInProgress>();
        for (JobInProgress job: infos.keySet()) { 
          int runState = job.getStatus().getRunState();
          if (runState == JobStatus.SUCCEEDED || runState == JobStatus.FAILED
            || runState == JobStatus.KILLED) {
              toRemove.add(job);
          }
        }
        for (JobInProgress job: toRemove) {
          jobNoLongerRunning(job);
        }

        updateRunnability(); // Set job runnability based on user/pool limits 

        // Update demands of jobs and pools
        for (Pool pool: poolMgr.getPools()) {
          pool.getMapSchedulable().updateDemand();
          pool.getReduceSchedulable().updateDemand();
        }

        // Compute fair shares based on updated demands
        List<PoolSchedulable> mapScheds = getPoolSchedulables(TaskType.MAP);
        List<PoolSchedulable> reduceScheds = getPoolSchedulables(TaskType.REDUCE);
        SchedulingAlgorithms.computeFairShares(
            mapScheds, clusterStatus.getMaxMapTasks());
        SchedulingAlgorithms.computeFairShares(
            reduceScheds, clusterStatus.getMaxReduceTasks());

        // Use the computed shares to assign shares within each pool
        for (Pool pool: poolMgr.getPools()) {
          pool.getMapSchedulable().redistributeShare();
          pool.getReduceSchedulable().redistributeShare();
        }

        if (preemptionEnabled)
          updatePreemptionVariables();
      }
    }
  }

  private void jobNoLongerRunning(JobInProgress job) {
    assert Thread.holdsLock(this);
    JobInfo info = infos.remove(job);
    if (info != null) {
      info.mapSchedulable.cleanupMetrics();
      info.reduceSchedulable.cleanupMetrics();
    }
    poolMgr.removeJob(job);
  }
  
  public List<PoolSchedulable> getPoolSchedulables(TaskType type) {
    List<PoolSchedulable> scheds = new ArrayList<PoolSchedulable>();
    for (Pool pool: poolMgr.getPools()) {
      scheds.add(pool.getSchedulable(type));
    }
    return scheds;
  }
  
  private void updateRunnability() {
    // Start by marking everything as not runnable
    for (JobInfo info: infos.values()) {
      info.runnable = false;
    }
    // Create a list of sorted jobs in order of start time and priority
    List<JobInProgress> jobs = new ArrayList<JobInProgress>(infos.keySet());
    Collections.sort(jobs, new FifoJobComparator());
    // Mark jobs as runnable in order of start time and priority, until
    // user or pool limits have been reached.
    Map<String, Integer> userJobs = new HashMap<String, Integer>();
    Map<String, Integer> poolJobs = new HashMap<String, Integer>();
    for (JobInProgress job: jobs) {
      if (job.getStatus().getRunState() == JobStatus.RUNNING) {
        String user = job.getJobConf().getUser();
        String pool = poolMgr.getPoolName(job);
        int userCount = userJobs.containsKey(user) ? userJobs.get(user) : 0;
        int poolCount = poolJobs.containsKey(pool) ? poolJobs.get(pool) : 0;
        if (userCount < poolMgr.getUserMaxJobs(user) && 
            poolCount < poolMgr.getPoolMaxJobs(pool)) {
          infos.get(job).runnable = true;
          userJobs.put(user, userCount + 1);
          poolJobs.put(pool, poolCount + 1);
        }
      }
    }
  }

  public double getJobWeight(JobInProgress job, TaskType taskType) {
    if (!isRunnable(job)) {
      // Job won't launch tasks, but don't return 0 to avoid division errors
      return 1.0;
    } else {
      double weight = 1.0;
      if (sizeBasedWeight) {
        // Set weight based on runnable tasks
        JobInfo info = infos.get(job);
        int runnableTasks = (taskType == TaskType.MAP) ?
            info.mapSchedulable.getDemand() : 
            info.reduceSchedulable.getDemand();
        weight = Math.log1p(runnableTasks) / Math.log(2);
      }
      weight *= getPriorityFactor(job.getPriority());
      if (weightAdjuster != null) {
        // Run weight through the user-supplied weightAdjuster
        weight = weightAdjuster.adjustWeight(job, taskType, weight);
      }
      return weight;
    }
  }

  private double getPriorityFactor(JobPriority priority) {
    switch (priority) {
    case VERY_HIGH: return 4.0;
    case HIGH:      return 2.0;
    case NORMAL:    return 1.0;
    case LOW:       return 0.5;
    default:        return 0.25; // priority = VERY_LOW
    }
  }
  
  public PoolManager getPoolManager() {
    return poolMgr;
  }

  private int getTotalSlots(TaskType type, ClusterStatus clusterStatus) {
    return (type == TaskType.MAP ?
      clusterStatus.getMaxMapTasks() : clusterStatus.getMaxReduceTasks());
  }

  /**
   * Update the preemption fields for all PoolScheduables, i.e. the times since
   * each pool last was at its guaranteed share and at > 1/2 of its fair share
   * for each type of task.
   * 
   * Requires locks on both the JobTracker and the FairScheduler to be held.
   */
  private void updatePreemptionVariables() {
    long now = clock.getTime();
    lastPreemptionUpdateTime = now;
    for (TaskType type: MAP_AND_REDUCE) {
      for (PoolSchedulable sched: getPoolSchedulables(type)) {
        if (!isStarvedForMinShare(sched)) {
          sched.setLastTimeAtMinShare(now);
        }
        if (!isStarvedForFairShare(sched)) {
          sched.setLastTimeAtHalfFairShare(now);
        }
        eventLog.log("PREEMPT_VARS", sched.getName(), type,
            now - sched.getLastTimeAtMinShare(),
            now - sched.getLastTimeAtHalfFairShare());
      }
    }
  }

  /**
   * Is a pool below its min share for the given task type?
   */
  boolean isStarvedForMinShare(PoolSchedulable sched) {
    int desiredShare = Math.min(sched.getMinShare(), sched.getDemand());
    return (sched.getRunningTasks() < desiredShare);
  }
  
  /**
   * Is a pool being starved for fair share for the given task type?
   * This is defined as being below half its fair share.
   */
  boolean isStarvedForFairShare(PoolSchedulable sched) {
    int desiredFairShare = (int) Math.floor(Math.min(
        sched.getFairShare() / 2, sched.getDemand()));
    return (sched.getRunningTasks() < desiredFairShare);
  }

  /**
   * Check for pools that need tasks preempted, either because they have been
   * below their guaranteed share for minSharePreemptionTimeout or they
   * have been below half their fair share for the fairSharePreemptionTimeout.
   * If such pools exist, compute how many tasks of each type need to be
   * preempted and then select the right ones using preemptTasks.
   * 
   * This method computes and logs the number of tasks we want to preempt even
   * if preemption is disabled, for debugging purposes.
   */
  protected void preemptTasksIfNecessary() {
    if (!preemptionEnabled)
      return;
    
    long curTime = clock.getTime();
    if (curTime - lastPreemptCheckTime < preemptionInterval)
      return;
    lastPreemptCheckTime = curTime;
    
    // Acquire locks on both the JobTracker (task tracker manager) and this
    // because we might need to call some JobTracker methods (killTask).
    synchronized (taskTrackerManager) {
      synchronized (this) {
        for (TaskType type: MAP_AND_REDUCE) {
          List<PoolSchedulable> scheds = getPoolSchedulables(type);
          int tasksToPreempt = 0;
          for (PoolSchedulable sched: scheds) {
            tasksToPreempt += tasksToPreempt(sched, curTime);
          }
          if (tasksToPreempt > 0) {
            eventLog.log("SHOULD_PREEMPT", type, tasksToPreempt);
            if (!onlyLogPreemption) {
              preemptTasks(scheds, tasksToPreempt);
            }
          }
        }
      }
    }
  }

  /**
   * Preempt a given number of tasks from a list of PoolSchedulables. 
   * The policy for this is to pick tasks from pools that are over their fair 
   * share, but make sure that no pool is placed below its fair share in the 
   * process. Furthermore, we want to minimize the amount of computation
   * wasted by preemption, so out of the tasks in over-scheduled pools, we
   * prefer to preempt tasks that started most recently.
   */
  private void preemptTasks(List<PoolSchedulable> scheds, int tasksToPreempt) {
    if (scheds.isEmpty() || tasksToPreempt == 0)
      return;
    
    TaskType taskType = scheds.get(0).getTaskType();
    
    // Collect running tasks of our type from over-scheduled pools
    List<TaskStatus> runningTasks = new ArrayList<TaskStatus>();
    for (PoolSchedulable sched: scheds) {
      if (sched.getRunningTasks() > sched.getFairShare())
      for (JobSchedulable js: sched.getJobSchedulables()) {
        runningTasks.addAll(getRunningTasks(js.getJob(), taskType));
      }
    }
    
    // Sort tasks into reverse order of start time
    Collections.sort(runningTasks, new Comparator<TaskStatus>() {
      public int compare(TaskStatus t1, TaskStatus t2) {
        if (t1.getStartTime() < t2.getStartTime())
          return 1;
        else if (t1.getStartTime() == t2.getStartTime())
          return 0;
        else
          return -1;
      }
    });
    
    // Maintain a count of tasks left in each pool; this is a bit
    // faster than calling runningTasks() on the pool repeatedly
    // because the latter must scan through jobs in the pool
    HashMap<Pool, Integer> tasksLeft = new HashMap<Pool, Integer>(); 
    for (Pool p: poolMgr.getPools()) {
      tasksLeft.put(p, p.getSchedulable(taskType).getRunningTasks());
    }
    
    // Scan down the sorted list of task statuses until we've killed enough
    // tasks, making sure we don't kill too many from any pool
    for (TaskStatus status: runningTasks) {
      JobID jobID = status.getTaskID().getJobID();
      JobInProgress job = taskTrackerManager.getJob(jobID);
      Pool pool = poolMgr.getPool(job);
      PoolSchedulable sched = pool.getSchedulable(taskType);
      int tasksLeftForPool = tasksLeft.get(pool);
      if (tasksLeftForPool > sched.getFairShare()) {
        eventLog.log("PREEMPT", status.getTaskID(),
            status.getTaskTracker());
        try {
          taskTrackerManager.killTask(status.getTaskID(), false);
          tasksToPreempt--;
          if (tasksToPreempt == 0)
            break;

          // reduce tasks left for pool
          tasksLeft.put(pool, --tasksLeftForPool);
        } catch (IOException e) {
          LOG.error("Failed to kill task " + status.getTaskID(), e);
        }
      }
    }
  }

  /**
   * Count how many tasks of a given type the pool needs to preempt, if any.
   * If the pool has been below its min share for at least its preemption
   * timeout, it should preempt the difference between its current share and
   * this min share. If it has been below half its fair share for at least the
   * fairSharePreemptionTimeout, it should preempt enough tasks to get up to
   * its full fair share. If both conditions hold, we preempt the max of the
   * two amounts (this shouldn't happen unless someone sets the timeouts to
   * be identical for some reason).
   */
  protected int tasksToPreempt(PoolSchedulable sched, long curTime) {
    String pool = sched.getName();
    long minShareTimeout = poolMgr.getMinSharePreemptionTimeout(pool);
    long fairShareTimeout = poolMgr.getFairSharePreemptionTimeout();
    int tasksDueToMinShare = 0;
    int tasksDueToFairShare = 0;
    if (curTime - sched.getLastTimeAtMinShare() > minShareTimeout) {
      int target = Math.min(sched.getMinShare(), sched.getDemand());
      tasksDueToMinShare = Math.max(0, target - sched.getRunningTasks());
    }
    if (curTime - sched.getLastTimeAtHalfFairShare() > fairShareTimeout) {
      int target = (int) Math.min(sched.getFairShare(), sched.getDemand());
      tasksDueToFairShare = Math.max(0, target - sched.getRunningTasks());
    }
    int tasksToPreempt = Math.max(tasksDueToMinShare, tasksDueToFairShare);
    if (tasksToPreempt > 0) {
      String message = "Should preempt " + tasksToPreempt + " " 
          + sched.getTaskType() + " tasks for pool " + sched.getName() 
          + ": tasksDueToMinShare = " + tasksDueToMinShare
          + ", tasksDueToFairShare = " + tasksDueToFairShare;
      eventLog.log("INFO", message);
      LOG.info(message);
    }
    return tasksToPreempt;
  }

  private List<TaskStatus> getRunningTasks(JobInProgress job, TaskType type) {
    // Create a list of all running TaskInProgress'es in the job
    List<TaskInProgress> tips = new ArrayList<TaskInProgress>();
    if (type == TaskType.MAP) {
      // Jobs may have both "non-local maps" which have a split with no locality
      // info (e.g. the input file is not in HDFS), and maps with locality info,
      // which are stored in the runningMapCache map from location to task list
      tips.addAll(job.nonLocalRunningMaps);
      for (Set<TaskInProgress> set: job.runningMapCache.values()) {
        tips.addAll(set);
      }
    }
    else {
      tips.addAll(job.runningReduces);
    }
    // Get the active TaskStatus'es for each TaskInProgress (there may be
    // more than one if the task has multiple copies active due to speculation)
    List<TaskStatus> statuses = new ArrayList<TaskStatus>();
    for (TaskInProgress tip: tips) {
      for (TaskAttemptID id: tip.getActiveTasks().keySet()) {
        TaskStatus stat = tip.getTaskStatus(id);
        // status is null when the task has been scheduled but not yet running
        if (stat != null) {
          statuses.add(stat);
        }
      }
    }
    return statuses;
  }



  protected boolean isRunnable(JobInProgress job) {
    JobInfo info = infos.get(job);
    if (info == null) return false;
    return info.runnable;
  }

  @Override
  public synchronized Collection<JobInProgress> getJobs(String queueName) {
    Pool myJobPool = poolMgr.getPool(queueName);
    return myJobPool.getJobs();
  }

  protected void dumpIfNecessary() {
    long now = clock.getTime();
    long timeDelta = now - lastDumpTime;
    if (timeDelta > dumpInterval && eventLog.isEnabled()) {
      dump();
      lastDumpTime = now;
    }
  }

  /**
   * Dump scheduler state to the fairscheduler log.
   */
  private synchronized void dump() {
    synchronized (eventLog) {
      eventLog.log("BEGIN_DUMP");
      // List jobs in order of submit time
      ArrayList<JobInProgress> jobs = 
        new ArrayList<JobInProgress>(infos.keySet());
      Collections.sort(jobs, new Comparator<JobInProgress>() {
        public int compare(JobInProgress j1, JobInProgress j2) {
          return (int) Math.signum(j1.getStartTime() - j2.getStartTime());
        }
      });
      // Dump info for each job
      for (JobInProgress job: jobs) {
        JobProfile profile = job.getProfile();
        JobInfo info = infos.get(job);
        Schedulable ms = info.mapSchedulable;
        Schedulable rs = info.reduceSchedulable;
        eventLog.log("JOB",
            profile.getJobID(), profile.name, profile.user,
            job.getPriority(), poolMgr.getPoolName(job),
            job.numMapTasks, ms.getRunningTasks(),
            ms.getDemand(), ms.getFairShare(), ms.getWeight(),
            job.numReduceTasks, rs.getRunningTasks(),
            rs.getDemand(), rs.getFairShare(), rs.getWeight());
      }
      // List pools in alphabetical order
      List<Pool> pools = new ArrayList<Pool>(poolMgr.getPools());
      Collections.sort(pools, new Comparator<Pool>() {
        public int compare(Pool p1, Pool p2) {
          if (p1.isDefaultPool())
            return 1;
          else if (p2.isDefaultPool())
            return -1;
          else return p1.getName().compareTo(p2.getName());
        }});
      for (Pool pool: pools) {
        int runningMaps = 0;
        int runningReduces = 0;
        for (JobInProgress job: pool.getJobs()) {
          JobInfo info = infos.get(job);
          if (info != null) {
            // TODO: Fix
            //runningMaps += info.runningMaps;
            //runningReduces += info.runningReduces;
          }
        }
        String name = pool.getName();
        eventLog.log("POOL",
            name, poolMgr.getPoolWeight(name), pool.getJobs().size(),
            poolMgr.getAllocation(name, TaskType.MAP), runningMaps,
            poolMgr.getAllocation(name, TaskType.REDUCE), runningReduces);
      }
      // Dump info for each pool
      eventLog.log("END_DUMP");
    }
  }

  public Clock getClock() {
    return clock;
  }
  
  public FairSchedulerEventLog getEventLog() {
    return eventLog;
  }

  public JobInfo getJobInfo(JobInProgress job) {
    return infos.get(job);
  }
  
  boolean isPreemptionEnabled() {
    return preemptionEnabled;
  }
  long getLastPreemptionUpdateTime() {
    return lastPreemptionUpdateTime;
  }

  /**
   * Examines the job's pool name to determine if it is a declared pool name (in
   * the scheduler allocation file).
   */
  @Override
  public void checkJobSubmission(JobInProgress job)
      throws UndeclaredPoolException {
    Set<String> declaredPools = poolMgr.getDeclaredPools();
    if (!this.allowUndeclaredPools
        && !declaredPools.contains(poolMgr.getPoolName(job)))
      throw new UndeclaredPoolException("Pool name: '"
          + poolMgr.getPoolName(job)
          + "' is invalid. Add pool name to the fair scheduler allocation "
          + "file. Valid pools are: "
          + StringUtils.join(", ", declaredPools));
  }

}
