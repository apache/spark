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
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.AuditLogger;
import org.apache.hadoop.mapred.CleanupQueue.PathDeletionContext;
import org.apache.hadoop.mapred.Counters.CountersExceededException;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.JobHistory.Values;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.DelegationTokenRenewal;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReader;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;

/*************************************************************
 * JobInProgress maintains all the info for keeping
 * a Job on the straight and narrow.  It keeps its JobProfile
 * and its latest JobStatus, plus a set of tables for 
 * doing bookkeeping of its Tasks.
 * ***********************************************************
 * 
 * This is NOT a public interface!
 */
public class JobInProgress {
  /**
   * Used when the a kill is issued to a job which is initializing.
   */
  @SuppressWarnings("serial")
  static class KillInterruptedException extends InterruptedException {
    public KillInterruptedException(String msg) {
      super(msg);
    }
  }

  static final Log LOG = LogFactory.getLog(JobInProgress.class);
    
  JobProfile profile;
  JobStatus status;
  String jobFile = null;
  Path localJobFile = null;

  TaskInProgress maps[] = new TaskInProgress[0];
  TaskInProgress reduces[] = new TaskInProgress[0];
  TaskInProgress cleanup[] = new TaskInProgress[0];
  TaskInProgress setup[] = new TaskInProgress[0];
  int numMapTasks = 0;
  int numReduceTasks = 0;
  final long memoryPerMap;
  final long memoryPerReduce;
  volatile int numSlotsPerMap = 1;
  volatile int numSlotsPerReduce = 1;
  final int maxTaskFailuresPerTracker;
  
  // Counters to track currently running/finished/failed Map/Reduce task-attempts
  int runningMapTasks = 0;
  int runningReduceTasks = 0;
  int finishedMapTasks = 0;
  int finishedReduceTasks = 0;
  int failedMapTasks = 0; 
  int failedReduceTasks = 0;
  private static long DEFAULT_REDUCE_INPUT_LIMIT = -1L;
  long reduce_input_limit = -1L;
  private static float DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART = 0.05f;
  int completedMapsForReduceSlowstart = 0;
    
  // runningMapTasks include speculative tasks, so we need to capture 
  // speculative tasks separately 
  int speculativeMapTasks = 0;
  int speculativeReduceTasks = 0;
  
  final int mapFailuresPercent;
  final int reduceFailuresPercent;
  int failedMapTIPs = 0;
  int failedReduceTIPs = 0;
  private volatile boolean launchedCleanup = false;
  private volatile boolean launchedSetup = false;
  private volatile boolean jobKilled = false;
  private volatile boolean jobFailed = false;

  JobPriority priority = JobPriority.NORMAL;
  final JobTracker jobtracker;
  
  protected Credentials tokenStorage;

  // NetworkTopology Node to the set of TIPs
  Map<Node, List<TaskInProgress>> nonRunningMapCache;
  
  // Map of NetworkTopology Node to set of running TIPs
  Map<Node, Set<TaskInProgress>> runningMapCache;

  // A list of non-local, non-running maps
  final List<TaskInProgress> nonLocalMaps;

  // Set of failed, non-running maps sorted by #failures
  final SortedSet<TaskInProgress> failedMaps;

  // A set of non-local running maps
  Set<TaskInProgress> nonLocalRunningMaps;

  // A list of non-running reduce TIPs
  Set<TaskInProgress> nonRunningReduces;

  // A set of running reduce TIPs
  Set<TaskInProgress> runningReduces;
  
  // A list of cleanup tasks for the map task attempts, to be launched
  List<TaskAttemptID> mapCleanupTasks = new LinkedList<TaskAttemptID>();
  
  // A list of cleanup tasks for the reduce task attempts, to be launched
  List<TaskAttemptID> reduceCleanupTasks = new LinkedList<TaskAttemptID>();

  // keep failedMaps, nonRunningReduces ordered by failure count to bias
  // scheduling toward failing tasks
  private static final Comparator<TaskInProgress> failComparator =
    new Comparator<TaskInProgress>() {
      @Override
      public int compare(TaskInProgress t1, TaskInProgress t2) {
        if (t1 == null) return -1;
        if (t2 == null) return 1;
        
        int failures = t2.numTaskFailures() - t1.numTaskFailures();
        return (failures == 0) ? (t1.getTIPId().getId() - t2.getTIPId().getId())
            : failures;
      }
    };

  private final int maxLevel;

  /**
   * A special value indicating that 
   * {@link #findNewMapTask(TaskTrackerStatus, int, int, int, double)} should
   * schedule any available map tasks for this job, including speculative tasks.
   */
  private final int anyCacheLevel;
  
  /**
   * Number of scheduling opportunities (heartbeats) given to this Job
   */
  private volatile long numSchedulingOpportunities;
  
  static String LOCALITY_WAIT_FACTOR = "mapreduce.job.locality.wait.factor";
  static final float DEFAULT_LOCALITY_WAIT_FACTOR = 1.0f;
  
  /**
   * Percentage of the cluster the job is willing to wait to get better locality
   */
  private float localityWaitFactor = 1.0f;
  
  /**
   * A special value indicating that 
   * {@link #findNewMapTask(TaskTrackerStatus, int, int, int, double)} should
   * schedule any only off-switch and speculative map tasks for this job.
   */
  private static final int NON_LOCAL_CACHE_LEVEL = -1;

  private int taskCompletionEventTracker = 0; 
  List<TaskCompletionEvent> taskCompletionEvents;
    
  // The maximum percentage of trackers in cluster added to the 'blacklist'.
  private static final double CLUSTER_BLACKLIST_PERCENT = 0.25;
  
  // The maximum percentage of fetch failures allowed for a map 
  private static final double MAX_ALLOWED_FETCH_FAILURES_PERCENT = 0.5;
  
  // No. of tasktrackers in the cluster
  private volatile int clusterSize = 0;
  
  // The no. of tasktrackers where >= conf.getMaxTaskFailuresPerTracker()
  // tasks have failed
  private volatile int flakyTaskTrackers = 0;
  // Map of trackerHostName -> no. of task failures
  private Map<String, Integer> trackerToFailuresMap = 
    new TreeMap<String, Integer>();
    
  //Confine estimation algorithms to an "oracle" class that JIP queries.
  private ResourceEstimator resourceEstimator; 
  
  long startTime;
  long launchTime;
  long finishTime;

  // First *task launch time
  final Map<TaskType, Long> firstTaskLaunchTimes =
      new EnumMap<TaskType, Long>(TaskType.class);
  
  // Indicates how many times the job got restarted
  private final int restartCount;

  private JobConf conf;
  volatile boolean tasksInited = false;
  private JobInitKillStatus jobInitKillStatus = new JobInitKillStatus();

  private LocalFileSystem localFs;
  private FileSystem fs;
  private JobID jobId;
  volatile private boolean hasSpeculativeMaps;
  volatile private boolean hasSpeculativeReduces;
  private long inputLength = 0;
  private String submitHostName;
  private String submitHostAddress;
  private String user;
  private String historyFile = "";
  private boolean historyFileCopied;
  
  // Per-job counters
  public static enum Counter { 
    NUM_FAILED_MAPS, 
    NUM_FAILED_REDUCES,
    TOTAL_LAUNCHED_MAPS,
    TOTAL_LAUNCHED_REDUCES,
    OTHER_LOCAL_MAPS,
    DATA_LOCAL_MAPS,
    RACK_LOCAL_MAPS,
    SLOTS_MILLIS_MAPS,
    SLOTS_MILLIS_REDUCES,
    FALLOW_SLOTS_MILLIS_MAPS,
    FALLOW_SLOTS_MILLIS_REDUCES
  }
  private Counters jobCounters = new Counters();
  
  private MetricsRecord jobMetrics;
  
  // Maximum no. of fetch-failure notifications after which
  // the map task is killed
  private static final int MAX_FETCH_FAILURES_NOTIFICATIONS = 3;
  
  // Map of mapTaskId -> no. of fetch failures
  private Map<TaskAttemptID, Integer> mapTaskIdToFetchFailuresMap =
    new TreeMap<TaskAttemptID, Integer>();

  private Object schedulingInfo;

  private static class FallowSlotInfo {
    long timestamp;
    int numSlots;

    public FallowSlotInfo(long timestamp, int numSlots) {
      this.timestamp = timestamp;
      this.numSlots = numSlots;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
    }

    public int getNumSlots() {
      return numSlots;
    }

    public void setNumSlots(int numSlots) {
      this.numSlots = numSlots;
    }
  }

  private Map<TaskTracker, FallowSlotInfo> trackersReservedForMaps = 
    new HashMap<TaskTracker, FallowSlotInfo>();
  private Map<TaskTracker, FallowSlotInfo> trackersReservedForReduces = 
    new HashMap<TaskTracker, FallowSlotInfo>();
  private Path jobSubmitDir = null;

  final private UserGroupInformation userUGI;
  
  /**
   * Create an almost empty JobInProgress, which can be used only for tests
   */
  protected JobInProgress(JobID jobid, JobConf conf, JobTracker tracker) {
    System.out.println("DEBUG3");
    this.conf = conf;
    this.jobId = jobid;
    this.numMapTasks = conf.getNumMapTasks();
    this.numReduceTasks = conf.getNumReduceTasks();
    this.maxLevel = NetworkTopology.DEFAULT_HOST_LEVEL;
    this.anyCacheLevel = this.maxLevel+1;
    this.jobtracker = tracker;
    this.restartCount = 0;
    this.status = new JobStatus(jobid, 0.0f, 0.0f, JobStatus.PREP);
    this.profile = new JobProfile(conf.getUser(), jobid, "", "", 
                                  conf.getJobName(), conf.getQueueName());
    hasSpeculativeMaps = conf.getMapSpeculativeExecution();
    hasSpeculativeReduces = conf.getReduceSpeculativeExecution();
    this.nonLocalMaps = new LinkedList<TaskInProgress>();
    this.failedMaps = new TreeSet<TaskInProgress>(failComparator);
    this.nonLocalRunningMaps = new LinkedHashSet<TaskInProgress>();
    this.runningMapCache = new IdentityHashMap<Node, Set<TaskInProgress>>();
    this.nonRunningReduces = new TreeSet<TaskInProgress>(failComparator);
    this.runningReduces = new LinkedHashSet<TaskInProgress>();
    this.resourceEstimator = new ResourceEstimator(this);
    this.status = new JobStatus(jobid, 0.0f, 0.0f, JobStatus.PREP);
    this.status.setUsername(conf.getUser());
    this.profile = new JobProfile(conf.getUser(), jobid, "", "",
                                  conf.getJobName(), conf.getQueueName());
    this.memoryPerMap = conf.getMemoryForMapTask();
    this.memoryPerReduce = conf.getMemoryForReduceTask();
    this.maxTaskFailuresPerTracker = conf.getMaxTaskFailuresPerTracker();
    this.mapFailuresPercent = conf.getMaxMapTaskFailuresPercent();
    this.reduceFailuresPercent = conf.getMaxReduceTaskFailuresPercent();

    this.taskCompletionEvents = new ArrayList<TaskCompletionEvent>
      (numMapTasks + numReduceTasks + 10);
    try {
      this.userUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie){
      throw new RuntimeException(ie);
    }
  }
  
  JobInProgress(JobTracker jobtracker, final JobConf default_conf, 
      JobInfo jobInfo, int rCount, Credentials ts) 
  throws IOException, InterruptedException {
    try {
      this.restartCount = rCount;
      this.jobId = JobID.downgrade(jobInfo.getJobID());
      String url = "http://" + jobtracker.getJobTrackerMachine() + ":" 
      + jobtracker.getInfoPort() + "/jobdetails.jsp?jobid=" + jobId;
      this.jobtracker = jobtracker;
      this.status = new JobStatus(jobId, 0.0f, 0.0f, JobStatus.PREP);
      this.status.setUsername(jobInfo.getUser().toString());
      this.jobtracker.getInstrumentation().addPrepJob(conf, jobId);
      this.startTime = jobtracker.getClock().getTime();
      status.setStartTime(startTime);
      this.localFs = jobtracker.getLocalFileSystem();

      this.tokenStorage = ts;
      // use the user supplied token to add user credentials to the conf
      jobSubmitDir = jobInfo.getJobSubmitDir();
      user = jobInfo.getUser().toString();
      userUGI = UserGroupInformation.createRemoteUser(user);
      if (ts != null) {
        for (Token<? extends TokenIdentifier> token : ts.getAllTokens()) {
          userUGI.addToken(token);
        }
      }

      fs = userUGI.doAs(new PrivilegedExceptionAction<FileSystem>() {
        public FileSystem run() throws IOException {
          return jobSubmitDir.getFileSystem(default_conf);
        }});
      
      /** check for the size of jobconf **/
      Path submitJobFile = JobSubmissionFiles.getJobConfPath(jobSubmitDir);
      FileStatus fstatus = fs.getFileStatus(submitJobFile);
      if (fstatus.getLen() > jobtracker.MAX_JOBCONF_SIZE) {
        throw new IOException("Exceeded max jobconf size: " 
            + fstatus.getLen() + " limit: " + jobtracker.MAX_JOBCONF_SIZE);
      }
      this.localJobFile = default_conf.getLocalPath(JobTracker.SUBDIR
          +"/"+jobId + ".xml");
      Path jobFilePath = JobSubmissionFiles.getJobConfPath(jobSubmitDir);
      jobFile = jobFilePath.toString();
      fs.copyToLocalFile(jobFilePath, localJobFile);
      conf = new JobConf(localJobFile);
      if (conf.getUser() == null) {
        this.conf.setUser(user);
      }
      if (!conf.getUser().equals(user)) {
        String desc = "The username " + conf.getUser() + " obtained from the " +
        "conf doesn't match the username " + user + " the user " +
        "authenticated as";
        AuditLogger.logFailure(user, Operation.SUBMIT_JOB.name(), conf.getUser(), 
            jobId.toString(), desc);
        throw new IOException(desc);
      }

      String userGroups[] = userUGI.getGroupNames();
      String primaryGroup = (userGroups.length > 0) ? userGroups[0] : null;
      if (primaryGroup != null) {
        conf.set("group.name", primaryGroup);
      }

      this.priority = conf.getJobPriority();
      this.status.setJobPriority(this.priority);
      this.profile = new JobProfile(user, jobId, 
          jobFile, url, conf.getJobName(),
          conf.getQueueName());

      this.submitHostName = conf.getJobSubmitHostName();
      this.submitHostAddress = conf.getJobSubmitHostAddress();
      this.numMapTasks = conf.getNumMapTasks();
      this.numReduceTasks = conf.getNumReduceTasks();

      this.memoryPerMap = conf.getMemoryForMapTask();
      this.memoryPerReduce = conf.getMemoryForReduceTask();

      this.taskCompletionEvents = new ArrayList<TaskCompletionEvent>
      (numMapTasks + numReduceTasks + 10);

      // Construct the jobACLs
      status.setJobACLs(jobtracker.getJobACLsManager().constructJobACLs(conf));

      this.mapFailuresPercent = conf.getMaxMapTaskFailuresPercent();
      this.reduceFailuresPercent = conf.getMaxReduceTaskFailuresPercent();

      this.maxTaskFailuresPerTracker = conf.getMaxTaskFailuresPerTracker();

      MetricsContext metricsContext = MetricsUtil.getContext("mapred");
      this.jobMetrics = MetricsUtil.createRecord(metricsContext, "job");
      this.jobMetrics.setTag("user", conf.getUser());
      this.jobMetrics.setTag("sessionId", conf.getSessionId());
      this.jobMetrics.setTag("jobName", conf.getJobName());
      this.jobMetrics.setTag("jobId", jobId.toString());
      hasSpeculativeMaps = conf.getMapSpeculativeExecution();
      hasSpeculativeReduces = conf.getReduceSpeculativeExecution();
      // a limit on the input size of the reduce.
      // we check to see if the estimated input size of 
      // of each reduce is less than this value. If not
      // we fail the job. A value of -1 just means there is no
      // limit set.
      reduce_input_limit = -1L;
      this.maxLevel = jobtracker.getNumTaskCacheLevels();
      this.anyCacheLevel = this.maxLevel+1;
      this.nonLocalMaps = new LinkedList<TaskInProgress>();
      this.failedMaps = new TreeSet<TaskInProgress>(failComparator);
      this.nonLocalRunningMaps = new LinkedHashSet<TaskInProgress>();
      this.runningMapCache = new IdentityHashMap<Node, Set<TaskInProgress>>();
      this.nonRunningReduces = new TreeSet<TaskInProgress>(failComparator);
      this.runningReduces = new LinkedHashSet<TaskInProgress>();
      this.resourceEstimator = new ResourceEstimator(this);
      this.reduce_input_limit = conf.getLong("mapreduce.reduce.input.limit", 
          DEFAULT_REDUCE_INPUT_LIMIT);
      // register job's tokens for renewal
      DelegationTokenRenewal.registerDelegationTokensForRenewal(
          jobInfo.getJobID(), ts, jobtracker.getConf());
    } finally {
      //close all FileSystems that was created above for the current user
      //At this point, this constructor is called in the context of an RPC, and
      //hence the "current user" is actually referring to the kerberos
      //authenticated user (if security is ON).
      FileSystem.closeAllForUGI(UserGroupInformation.getCurrentUser());
    }
  }

  public boolean hasSpeculativeMaps() {
    return hasSpeculativeMaps;
  }

  public boolean hasSpeculativeReduces() {
    return hasSpeculativeReduces;
  }

  /**
   * Called periodically by JobTrackerMetrics to update the metrics for
   * this job.
   */
  public void updateMetrics() {
    Counters counters = new Counters();
    boolean isFine = getCounters(counters);
    counters = (isFine? counters: new Counters());
    for (Counters.Group group : counters) {
      jobMetrics.setTag("group", group.getDisplayName());
      for (Counters.Counter counter : group) {
        jobMetrics.setTag("counter", counter.getDisplayName());
        jobMetrics.setMetric("value", (float) counter.getCounter());
        jobMetrics.update();
      }
    }
  }
    
  /**
   * Called when the job is complete
   */
  public void cleanUpMetrics() {
    // Deletes all metric data for this job (in internal table in metrics package).
    // This frees up RAM and possibly saves network bandwidth, since otherwise
    // the metrics package implementation might continue to send these job metrics
    // after the job has finished.
    jobMetrics.removeTag("group");
    jobMetrics.removeTag("counter");
    jobMetrics.remove();
  }
    
  private void printCache (Map<Node, List<TaskInProgress>> cache) {
    LOG.info("The taskcache info:");
    for (Map.Entry<Node, List<TaskInProgress>> n : cache.entrySet()) {
      List <TaskInProgress> tips = n.getValue();
      LOG.info("Cached TIPs on node: " + n.getKey());
      for (TaskInProgress tip : tips) {
        LOG.info("tip : " + tip.getTIPId());
      }
    }
  }
  
  private Map<Node, List<TaskInProgress>> createCache(
                                 TaskSplitMetaInfo[] splits, int maxLevel) {
    Map<Node, List<TaskInProgress>> cache = 
      new IdentityHashMap<Node, List<TaskInProgress>>(maxLevel);
    
    Set<String> uniqueHosts = new TreeSet<String>();
    for (int i = 0; i < splits.length; i++) {
      String[] splitLocations = splits[i].getLocations();
      if (splitLocations == null || splitLocations.length == 0) {
        nonLocalMaps.add(maps[i]);
        continue;
      }

      for(String host: splitLocations) {
        Node node = jobtracker.resolveAndAddToTopology(host);
        uniqueHosts.add(host);
        LOG.info("tip:" + maps[i].getTIPId() + " has split on node:" + node);
        for (int j = 0; j < maxLevel; j++) {
          List<TaskInProgress> hostMaps = cache.get(node);
          if (hostMaps == null) {
            hostMaps = new ArrayList<TaskInProgress>();
            cache.put(node, hostMaps);
            hostMaps.add(maps[i]);
          }
          //check whether the hostMaps already contains an entry for a TIP
          //This will be true for nodes that are racks and multiple nodes in
          //the rack contain the input for a tip. Note that if it already
          //exists in the hostMaps, it must be the last element there since
          //we process one TIP at a time sequentially in the split-size order
          if (hostMaps.get(hostMaps.size() - 1) != maps[i]) {
            hostMaps.add(maps[i]);
          }
          node = node.getParent();
        }
      }
    }
    
    // Calibrate the localityWaitFactor - Do not override user intent!
    if (localityWaitFactor == DEFAULT_LOCALITY_WAIT_FACTOR) {
      int jobNodes = uniqueHosts.size();
      int clusterNodes = jobtracker.getNumberOfUniqueHosts();
      
      if (clusterNodes > 0) {
        localityWaitFactor = 
          Math.min((float)jobNodes/clusterNodes, localityWaitFactor);
      }
      LOG.info(jobId + " LOCALITY_WAIT_FACTOR=" + localityWaitFactor);
    }
    
    return cache;
  }
  
  /**
   * Check if the job has been initialized.
   * @return <code>true</code> if the job has been initialized, 
   *         <code>false</code> otherwise
   */
  public boolean inited() {
    return tasksInited;
  }
 
  /**
   * Get the user for the job
   */
  public String getUser() {
    return user;
  }
  
  boolean hasRestarted() {
    return restartCount > 0;
  }

  boolean getMapSpeculativeExecution() {
    return hasSpeculativeMaps;
  }
  
  boolean getReduceSpeculativeExecution() {
    return hasSpeculativeReduces;
  }
  
  long getMemoryForMapTask() {
    return memoryPerMap;
  }
  
  long getMemoryForReduceTask() {
    return memoryPerReduce;
  }
  
  /**
   * Get the number of slots required to run a single map task-attempt.
   * @return the number of slots required to run a single map task-attempt
   */
  int getNumSlotsPerMap() {
    return numSlotsPerMap;
  }

  /**
   * Set the number of slots required to run a single map task-attempt.
   * This is typically set by schedulers which support high-ram jobs.
   * @param slots the number of slots required to run a single map task-attempt
   */
  void setNumSlotsPerMap(int numSlotsPerMap) {
    this.numSlotsPerMap = numSlotsPerMap;
  }

  /**
   * Get the number of slots required to run a single reduce task-attempt.
   * @return the number of slots required to run a single reduce task-attempt
   */
  int getNumSlotsPerReduce() {
    return numSlotsPerReduce;
  }

  /**
   * Set the number of slots required to run a single reduce task-attempt.
   * This is typically set by schedulers which support high-ram jobs.
   * @param slots the number of slots required to run a single reduce 
   *              task-attempt
   */
  void setNumSlotsPerReduce(int numSlotsPerReduce) {
    this.numSlotsPerReduce = numSlotsPerReduce;
  }

  /**
   * Construct the splits, etc.  This is invoked from an async
   * thread so that split-computation doesn't block anyone.
   */
  public synchronized void initTasks() 
  throws IOException, KillInterruptedException {
    if (tasksInited || isComplete()) {
      return;
    }
    synchronized(jobInitKillStatus){
      if(jobInitKillStatus.killed || jobInitKillStatus.initStarted) {
        return;
      }
      jobInitKillStatus.initStarted = true;
    }

    LOG.info("Initializing " + jobId);
    final long startTimeFinal = this.startTime;
    // log job info as the user running the job
    try {
    userUGI.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        JobHistory.JobInfo.logSubmitted(getJobID(), conf, jobFile, 
            startTimeFinal, hasRestarted());
        return null;
      }
    });
    } catch(InterruptedException ie) {
      throw new IOException(ie);
    }
    
    // log the job priority
    setPriority(this.priority);
    
    //
    // generate security keys needed by Tasks
    //
    generateAndStoreTokens();
    
    //
    // read input splits and create a map per a split
    //
    TaskSplitMetaInfo[] splits = createSplits(jobId);
    numMapTasks = splits.length;


    // if the number of splits is larger than a configured value
    // then fail the job.
    int maxTasks = jobtracker.getMaxTasksPerJob();
    if (maxTasks > 0 && numMapTasks + numReduceTasks > maxTasks) {
      throw new IOException(
                "The number of tasks for this job " + 
                (numMapTasks + numReduceTasks) +
                " exceeds the configured limit " + maxTasks);
    }

    jobtracker.getInstrumentation().addWaitingMaps(getJobID(), numMapTasks);
    jobtracker.getInstrumentation().addWaitingReduces(getJobID(), numReduceTasks);

    maps = new TaskInProgress[numMapTasks];
    for(int i=0; i < numMapTasks; ++i) {
      inputLength += splits[i].getInputDataLength();
      maps[i] = new TaskInProgress(jobId, jobFile, 
                                   splits[i], 
                                   jobtracker, conf, this, i, numSlotsPerMap);
    }
    LOG.info("Input size for job " + jobId + " = " + inputLength
        + ". Number of splits = " + splits.length);

    // Set localityWaitFactor before creating cache
    localityWaitFactor = 
      conf.getFloat(LOCALITY_WAIT_FACTOR, DEFAULT_LOCALITY_WAIT_FACTOR);
    if (numMapTasks > 0) { 
      nonRunningMapCache = createCache(splits, maxLevel);
    }
        
    // set the launch time
    this.launchTime = jobtracker.getClock().getTime();

    //
    // Create reduce tasks
    //
    this.reduces = new TaskInProgress[numReduceTasks];
    for (int i = 0; i < numReduceTasks; i++) {
      reduces[i] = new TaskInProgress(jobId, jobFile, 
                                      numMapTasks, i, 
                                      jobtracker, conf, this, numSlotsPerReduce);
      nonRunningReduces.add(reduces[i]);
    }

    // Calculate the minimum number of maps to be complete before 
    // we should start scheduling reduces
    completedMapsForReduceSlowstart = 
      (int)Math.ceil(
          (conf.getFloat("mapred.reduce.slowstart.completed.maps", 
                         DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART) * 
           numMapTasks));
    
    // ... use the same for estimating the total output of all maps
    resourceEstimator.setThreshhold(completedMapsForReduceSlowstart);
    
    // create cleanup two cleanup tips, one map and one reduce.
    cleanup = new TaskInProgress[2];

    // cleanup map tip. This map doesn't use any splits. Just assign an empty
    // split.
    TaskSplitMetaInfo emptySplit = JobSplit.EMPTY_TASK_SPLIT;
    cleanup[0] = new TaskInProgress(jobId, jobFile, emptySplit, 
            jobtracker, conf, this, numMapTasks, 1);
    cleanup[0].setJobCleanupTask();

    // cleanup reduce tip.
    cleanup[1] = new TaskInProgress(jobId, jobFile, numMapTasks,
                       numReduceTasks, jobtracker, conf, this, 1);
    cleanup[1].setJobCleanupTask();

    // create two setup tips, one map and one reduce.
    setup = new TaskInProgress[2];

    // setup map tip. This map doesn't use any split. Just assign an empty
    // split.
    setup[0] = new TaskInProgress(jobId, jobFile, emptySplit, 
            jobtracker, conf, this, numMapTasks + 1, 1);
    setup[0].setJobSetupTask();

    // setup reduce tip.
    setup[1] = new TaskInProgress(jobId, jobFile, numMapTasks,
                       numReduceTasks + 1, jobtracker, conf, this, 1);
    setup[1].setJobSetupTask();
    
    synchronized(jobInitKillStatus){
      jobInitKillStatus.initDone = true;
      if(jobInitKillStatus.killed) {
        throw new KillInterruptedException("Job " + jobId + " killed in init");
      }
    }
    
    tasksInited = true;
    JobHistory.JobInfo.logInited(profile.getJobID(), this.launchTime, 
                                 numMapTasks, numReduceTasks);
    
   // Log the number of map and reduce tasks
   LOG.info("Job " + jobId + " initialized successfully with " + numMapTasks
            + " map tasks and " + numReduceTasks + " reduce tasks.");
  }

  TaskSplitMetaInfo[] createSplits(org.apache.hadoop.mapreduce.JobID jobId)
  throws IOException {
    TaskSplitMetaInfo[] allTaskSplitMetaInfo =
      SplitMetaInfoReader.readSplitMetaInfo(jobId, fs, jobtracker.getConf(),
          jobSubmitDir);
    return allTaskSplitMetaInfo;
  }

  /////////////////////////////////////////////////////
  // Accessors for the JobInProgress
  /////////////////////////////////////////////////////
  public JobProfile getProfile() {
    return profile;
  }
  public JobStatus getStatus() {
    return status;
  }
  public synchronized long getLaunchTime() {
    return launchTime;
  }
  Map<TaskType, Long> getFirstTaskLaunchTimes() {
    return firstTaskLaunchTimes;
  }
  public long getStartTime() {
    return startTime;
  }
  public long getFinishTime() {
    return finishTime;
  }
  public int desiredMaps() {
    return numMapTasks;
  }
  public synchronized int finishedMaps() {
    return finishedMapTasks;
  }
  public int desiredReduces() {
    return numReduceTasks;
  }
  public synchronized int runningMaps() {
    return runningMapTasks;
  }
  public synchronized int runningReduces() {
    return runningReduceTasks;
  }
  public synchronized int finishedReduces() {
    return finishedReduceTasks;
  }
  public synchronized int pendingMaps() {
    return numMapTasks - runningMapTasks - failedMapTIPs - 
    finishedMapTasks + speculativeMapTasks;
  }
  public synchronized int pendingReduces() {
    return numReduceTasks - runningReduceTasks - failedReduceTIPs - 
    finishedReduceTasks + speculativeReduceTasks;
  }
  
  /**
   * Return total number of map and reduce tasks desired by the job.
   * @return total number of map and reduce tasks desired by the job
   */
  public int desiredTasks() {
    return desiredMaps() + desiredReduces();
  }
  
  public int getNumSlotsPerTask(TaskType taskType) {
    if (taskType == TaskType.MAP) {
      return numSlotsPerMap;
    } else if (taskType == TaskType.REDUCE) {
      return numSlotsPerReduce;
    } else {
      return 1;
    }
  }
  public JobPriority getPriority() {
    return this.priority;
  }
  public void setPriority(JobPriority priority) {
    if(priority == null) {
      this.priority = JobPriority.NORMAL;
    } else {
      this.priority = priority;
    }
    synchronized (this) {
      status.setJobPriority(priority);
    }
    // log and change to the job's priority
    JobHistory.JobInfo.logJobPriority(jobId, priority);
  }

  // Update the job start/launch time (upon restart) and log to history
  synchronized void updateJobInfo(long startTime, long launchTime) {
    // log and change to the job's start/launch time
    this.startTime = startTime;
    this.launchTime = launchTime;
    JobHistory.JobInfo.logJobInfo(jobId, startTime, launchTime);
  }

  /**
   * Get the number of times the job has restarted
   */
  int getNumRestarts() {
    return restartCount;
  }
  
  long getInputLength() {
    return inputLength;
  }
 
  boolean isCleanupLaunched() {
    return launchedCleanup;
  }

  boolean isSetupLaunched() {
    return launchedSetup;
  }

  /** 
   * Get all the tasks of the desired type in this job.
   * @param type {@link TaskType} of the tasks required
   * @return An array of {@link TaskInProgress} matching the given type. 
   *         Returns an empty array if no tasks are found for the given type.  
   */
  TaskInProgress[] getTasks(TaskType type) {
    TaskInProgress[] tasks = null;
    switch (type) {
      case MAP:
        {
          tasks = maps;
        }
        break;
      case REDUCE:
        {
          tasks = reduces;
        }
        break;
      case JOB_SETUP: 
        {
          tasks = setup;
        }
        break;
      case JOB_CLEANUP:
        {
          tasks = cleanup;
        }
        break;
      default:
        {
          tasks = new TaskInProgress[0];
        }
        break;
    }
    
    return tasks;
  }
  
  /**
   * Return the nonLocalRunningMaps
   * @return
   */
  Set<TaskInProgress> getNonLocalRunningMaps()
  {
    return nonLocalRunningMaps;
  }
  
  /**
   * Return the runningMapCache
   * @return
   */
  Map<Node, Set<TaskInProgress>> getRunningMapCache()
  {
    return runningMapCache;
  }
  
  /**
   * Return runningReduces
   * @return
   */
  Set<TaskInProgress> getRunningReduces()
  {
    return runningReduces;
  }
  
  /**
   * Get the job configuration
   * @return the job's configuration
   */
  JobConf getJobConf() {
    return conf;
  }
    
  /**
   * Return a vector of completed TaskInProgress objects
   */
  public synchronized Vector<TaskInProgress> reportTasksInProgress(boolean shouldBeMap,
                                                      boolean shouldBeComplete) {
    
    Vector<TaskInProgress> results = new Vector<TaskInProgress>();
    TaskInProgress tips[] = null;
    if (shouldBeMap) {
      tips = maps;
    } else {
      tips = reduces;
    }
    for (int i = 0; i < tips.length; i++) {
      if (tips[i].isComplete() == shouldBeComplete) {
        results.add(tips[i]);
      }
    }
    return results;
  }
  
  /**
   * Return a vector of cleanup TaskInProgress objects
   */
  public synchronized Vector<TaskInProgress> reportCleanupTIPs(
                                               boolean shouldBeComplete) {
    
    Vector<TaskInProgress> results = new Vector<TaskInProgress>();
    for (int i = 0; i < cleanup.length; i++) {
      if (cleanup[i].isComplete() == shouldBeComplete) {
        results.add(cleanup[i]);
      }
    }
    return results;
  }

  /**
   * Return a vector of setup TaskInProgress objects
   */
  public synchronized Vector<TaskInProgress> reportSetupTIPs(
                                               boolean shouldBeComplete) {
    
    Vector<TaskInProgress> results = new Vector<TaskInProgress>();
    for (int i = 0; i < setup.length; i++) {
      if (setup[i].isComplete() == shouldBeComplete) {
        results.add(setup[i]);
      }
    }
    return results;
  }
  

  ////////////////////////////////////////////////////
  // Status update methods
  ////////////////////////////////////////////////////

  /**
   * Assuming {@link JobTracker} is locked on entry.
   */
  public synchronized void updateTaskStatus(TaskInProgress tip, 
                                            TaskStatus status) {

    double oldProgress = tip.getProgress();   // save old progress
    boolean wasRunning = tip.isRunning();
    boolean wasComplete = tip.isComplete();
    boolean wasPending = tip.isOnlyCommitPending();
    TaskAttemptID taskid = status.getTaskID();
    boolean wasAttemptRunning = tip.isAttemptRunning(taskid);

    // If the TIP is already completed and the task reports as SUCCEEDED then 
    // mark the task as KILLED.
    // In case of task with no promotion the task tracker will mark the task 
    // as SUCCEEDED.
    // User has requested to kill the task, but TT reported SUCCEEDED, 
    // mark the task KILLED.
    if ((wasComplete || tip.wasKilled(taskid)) && 
        (status.getRunState() == TaskStatus.State.SUCCEEDED)) {
      status.setRunState(TaskStatus.State.KILLED);
    }
    
    // If the job is complete and a task has just reported its 
    // state as FAILED_UNCLEAN/KILLED_UNCLEAN, 
    // make the task's state FAILED/KILLED without launching cleanup attempt.
    // Note that if task is already a cleanup attempt, 
    // we don't change the state to make sure the task gets a killTaskAction
    if ((this.isComplete() || jobFailed || jobKilled) && 
        !tip.isCleanupAttempt(taskid)) {
      if (status.getRunState() == TaskStatus.State.FAILED_UNCLEAN) {
        status.setRunState(TaskStatus.State.FAILED);
      } else if (status.getRunState() == TaskStatus.State.KILLED_UNCLEAN) {
        status.setRunState(TaskStatus.State.KILLED);
      }
    }
    
    boolean change = tip.updateStatus(status);
    if (change) {
      TaskStatus.State state = status.getRunState();
      // get the TaskTrackerStatus where the task ran 
      TaskTracker taskTracker = 
        this.jobtracker.getTaskTracker(tip.machineWhereTaskRan(taskid));
      TaskTrackerStatus ttStatus = 
        (taskTracker == null) ? null : taskTracker.getStatus();
      String httpTaskLogLocation = null; 

      if (null != ttStatus){
        String host;
        if (NetUtils.getStaticResolution(ttStatus.getHost()) != null) {
          host = NetUtils.getStaticResolution(ttStatus.getHost());
        } else {
          host = ttStatus.getHost();
        }
        httpTaskLogLocation = "http://" + host + ":" + ttStatus.getHttpPort(); 
           //+ "/tasklog?plaintext=true&attemptid=" + status.getTaskID();
      }

      TaskCompletionEvent taskEvent = null;
      if (state == TaskStatus.State.SUCCEEDED) {
        taskEvent = new TaskCompletionEvent(
                                            taskCompletionEventTracker, 
                                            taskid,
                                            tip.idWithinJob(),
                                            status.getIsMap() &&
                                            !tip.isJobCleanupTask() &&
                                            !tip.isJobSetupTask(),
                                            TaskCompletionEvent.Status.SUCCEEDED,
                                            httpTaskLogLocation 
                                           );
        taskEvent.setTaskRunTime((int)(status.getFinishTime() 
                                       - status.getStartTime()));
        tip.setSuccessEventNumber(taskCompletionEventTracker); 
      } else if (state == TaskStatus.State.COMMIT_PENDING) {
        // If it is the first attempt reporting COMMIT_PENDING
        // ask the task to commit.
        if (!wasComplete && !wasPending) {
          tip.doCommit(taskid);
        }
        return;
      } else if (state == TaskStatus.State.FAILED_UNCLEAN ||
                 state == TaskStatus.State.KILLED_UNCLEAN) {
        tip.incompleteSubTask(taskid, this.status);
        // add this task, to be rescheduled as cleanup attempt
        if (tip.isMapTask()) {
          mapCleanupTasks.add(taskid);
        } else {
          reduceCleanupTasks.add(taskid);
        }
        // Remove the task entry from jobtracker
        jobtracker.removeTaskEntry(taskid);
      }
      //For a failed task update the JT datastructures. 
      else if (state == TaskStatus.State.FAILED ||
               state == TaskStatus.State.KILLED) {
        // Get the event number for the (possibly) previously successful
        // task. If there exists one, then set that status to OBSOLETE 
        int eventNumber;
        if ((eventNumber = tip.getSuccessEventNumber()) != -1) {
          TaskCompletionEvent t = 
            this.taskCompletionEvents.get(eventNumber);
          if (t.getTaskAttemptId().equals(taskid))
            t.setTaskStatus(TaskCompletionEvent.Status.OBSOLETE);
        }
        
        // Tell the job to fail the relevant task
        failedTask(tip, taskid, status, taskTracker,
                   wasRunning, wasComplete, wasAttemptRunning);

        // Did the task failure lead to tip failure?
        TaskCompletionEvent.Status taskCompletionStatus = 
          (state == TaskStatus.State.FAILED ) ?
              TaskCompletionEvent.Status.FAILED :
              TaskCompletionEvent.Status.KILLED;
        if (tip.isFailed()) {
          taskCompletionStatus = TaskCompletionEvent.Status.TIPFAILED;
        }
        taskEvent = new TaskCompletionEvent(taskCompletionEventTracker, 
                                            taskid,
                                            tip.idWithinJob(),
                                            status.getIsMap() &&
                                            !tip.isJobCleanupTask() &&
                                            !tip.isJobSetupTask(),
                                            taskCompletionStatus, 
                                            httpTaskLogLocation
                                           );
      }          

      // Add the 'complete' task i.e. successful/failed
      // It _is_ safe to add the TaskCompletionEvent.Status.SUCCEEDED
      // *before* calling TIP.completedTask since:
      // a. One and only one task of a TIP is declared as a SUCCESS, the
      //    other (speculative tasks) are marked KILLED by the TaskCommitThread
      // b. TIP.completedTask *does not* throw _any_ exception at all.
      if (taskEvent != null) {
        this.taskCompletionEvents.add(taskEvent);
        taskCompletionEventTracker++;
        JobTrackerStatistics.TaskTrackerStat ttStat = jobtracker.
           getStatistics().getTaskTrackerStat(tip.machineWhereTaskRan(taskid));
        if(ttStat != null) { // ttStat can be null in case of lost tracker
          ttStat.incrTotalTasks();
        }
        if (state == TaskStatus.State.SUCCEEDED) {
          completedTask(tip, status);
          if(ttStat != null) {
            ttStat.incrSucceededTasks();
          }
        }
      }
    }
        
    //
    // Update JobInProgress status
    //
    if(LOG.isDebugEnabled()) {
      LOG.debug("Taking progress for " + tip.getTIPId() + " from " + 
                 oldProgress + " to " + tip.getProgress());
    }
    
    if (!tip.isJobCleanupTask() && !tip.isJobSetupTask()) {
      double progressDelta = tip.getProgress() - oldProgress;
      if (tip.isMapTask()) {
          this.status.setMapProgress((float) (this.status.mapProgress() +
                                              progressDelta / maps.length));
      } else {
        this.status.setReduceProgress((float) (this.status.reduceProgress() + 
                                           (progressDelta / reduces.length)));
      }
    }
  }

  String getHistoryFile() {
    return historyFile;
  }

  synchronized void setHistoryFile(String file) {
    this.historyFile = file;
  }

  boolean isHistoryFileCopied() {
    return historyFileCopied;
  }

  synchronized void setHistoryFileCopied() {
    this.historyFileCopied = true;
  }
  
  /**
   * Returns the job-level counters.
   * 
   * @return the job-level counters.
   */
  public synchronized Counters getJobCounters() {
    return jobCounters;
  }
  
  /**
   *  Returns map phase counters by summing over all map tasks in progress.
   *  This method returns true if counters are within limit or false.
   */
  public synchronized boolean getMapCounters(Counters counters) {
    try {
      counters = incrementTaskCounters(counters, maps);
    } catch(CountersExceededException ce) {
      LOG.info("Counters Exceeded for Job: " + jobId, ce);
      return false;
    }
    return true;
  }
    
  /**
   *  Returns map phase counters by summing over all map tasks in progress.
   *  This method returns true if counters are within limits and false otherwise.
   */
  public synchronized boolean getReduceCounters(Counters counters) {
    try {
      counters = incrementTaskCounters(counters, reduces);
    } catch(CountersExceededException ce) {
      LOG.info("Counters Exceeded for Job: " + jobId, ce);
      return false;
    }
    return true;
  }
    
  /**
   *  Returns the total job counters, by adding together the job, 
   *  the map and the reduce counters. This method returns true if
   *  counters are within limits and false otherwise.
   */
  public synchronized boolean getCounters(Counters result) {
    try {
      result.incrAllCounters(getJobCounters());
      incrementTaskCounters(result, maps);
      incrementTaskCounters(result, reduces);
    } catch(CountersExceededException ce) {
      LOG.info("Counters Exceeded for Job: " + jobId, ce);
      return false;
    }
    return true;
  }
    
  /**
   * Increments the counters with the counters from each task.
   * @param counters the counters to increment
   * @param tips the tasks to add in to counters
   * @return counters the same object passed in as counters
   */
  private Counters incrementTaskCounters(Counters counters,
                                         TaskInProgress[] tips) {
    for (TaskInProgress tip : tips) {
      counters.incrAllCounters(tip.getCounters());
    }
    return counters;
  }

  /////////////////////////////////////////////////////
  // Create/manage tasks
  /////////////////////////////////////////////////////
  /**
   * Return a MapTask, if appropriate, to run on the given tasktracker
   */
  public synchronized Task obtainNewMapTask(TaskTrackerStatus tts, 
                                            int clusterSize, 
                                            int numUniqueHosts,
                                            int maxCacheLevel
                                           ) throws IOException {
    if (status.getRunState() != JobStatus.RUNNING) {
      LOG.info("Cannot create task split for " + profile.getJobID());
      return null;
    }
        
    int target = findNewMapTask(tts, clusterSize, numUniqueHosts, maxCacheLevel,
         getStatus().mapProgress());
    if (target == -1) {
      return null;
    }
    
    Task result = maps[target].getTaskToRun(tts.getTrackerName());
    if (result != null) {
      addRunningTaskToTIP(maps[target], result.getTaskID(), tts, true);
      if (maxCacheLevel < NON_LOCAL_CACHE_LEVEL) {
        resetSchedulingOpportunities();
        // TODO(todd) double check this logic
      }
    }

    return result;
  } 
  
  /**
   * Return a MapTask, if appropriate, to run on the given tasktracker
   */
  public synchronized Task obtainNewMapTask(TaskTrackerStatus tts, 
                                            int clusterSize, 
                                            int numUniqueHosts
                                           ) throws IOException {
    return obtainNewMapTask(tts, clusterSize, numUniqueHosts, anyCacheLevel);
  }    

  /*
   * Return task cleanup attempt if any, to run on a given tracker
   */
  public Task obtainTaskCleanupTask(TaskTrackerStatus tts, 
                                                 boolean isMapSlot)
  throws IOException {
    if (!tasksInited) {
      return null;
    }
    synchronized (this) {
      if (this.status.getRunState() != JobStatus.RUNNING || 
          jobFailed || jobKilled) {
        return null;
      }
      String taskTracker = tts.getTrackerName();
      if (!shouldRunOnTaskTracker(taskTracker)) {
        return null;
      }
      TaskAttemptID taskid = null;
      TaskInProgress tip = null;
      if (isMapSlot) {
        if (!mapCleanupTasks.isEmpty()) {
          taskid = mapCleanupTasks.remove(0);
          tip = maps[taskid.getTaskID().getId()];
        }
      } else {
        if (!reduceCleanupTasks.isEmpty()) {
          taskid = reduceCleanupTasks.remove(0);
          tip = reduces[taskid.getTaskID().getId()];
        }
      }
      if (tip != null) {
        return tip.addRunningTask(taskid, taskTracker, true);
      }
      return null;
    }
  }
  
  public synchronized Task obtainNewLocalMapTask(TaskTrackerStatus tts,
                                                     int clusterSize, 
                                                     int numUniqueHosts)
  throws IOException {
    if (!tasksInited) {
      LOG.info("Cannot create task split for " + profile.getJobID());
      return null;
    }

    return obtainNewMapTask(tts, clusterSize, numUniqueHosts, maxLevel);
  }
  
  public synchronized Task obtainNewNonLocalMapTask(TaskTrackerStatus tts,
                                                    int clusterSize, 
                                                    int numUniqueHosts)
  throws IOException {
    if (!tasksInited) {
      LOG.info("Cannot create task split for " + profile.getJobID());
      return null;
    }

    return obtainNewMapTask(tts, clusterSize, numUniqueHosts,
        NON_LOCAL_CACHE_LEVEL);
  }

  public void schedulingOpportunity() {
    ++numSchedulingOpportunities;
  }
  
  public void resetSchedulingOpportunities() {
    numSchedulingOpportunities = 0;
  }
  
  public long getNumSchedulingOpportunities() {
    return numSchedulingOpportunities;
  }

  private static final long OVERRIDE = 1000000;
  public void overrideSchedulingOpportunities() {
    numSchedulingOpportunities = OVERRIDE;
  }
  
  /**
   * Check if we can schedule an off-switch task for this job.
   * 
   * @param numTaskTrackers number of tasktrackers
   * @return <code>true</code> if we can schedule off-switch, 
   *         <code>false</code> otherwise
   * We check the number of missed opportunities for the job. 
   * If it has 'waited' long enough we go ahead and schedule.
   */
  public boolean scheduleOffSwitch(int numTaskTrackers) {
    long missedTaskTrackers = getNumSchedulingOpportunities();
    long requiredSlots = 
      Math.min((desiredMaps() - finishedMaps()), numTaskTrackers);
    
    return (requiredSlots  * localityWaitFactor) < missedTaskTrackers;
  }
  
  /**
   * Return a CleanupTask, if appropriate, to run on the given tasktracker
   * 
   */
  public Task obtainJobCleanupTask(TaskTrackerStatus tts, 
                                             int clusterSize, 
                                             int numUniqueHosts,
                                             boolean isMapSlot
                                            ) throws IOException {
    if(!tasksInited) {
      return null;
    }
    
    synchronized(this) {
      if (!canLaunchJobCleanupTask()) {
        return null;
      }
      
      String taskTracker = tts.getTrackerName();
      // Update the last-known clusterSize
      this.clusterSize = clusterSize;
      if (!shouldRunOnTaskTracker(taskTracker)) {
        return null;
      }
      
      List<TaskInProgress> cleanupTaskList = new ArrayList<TaskInProgress>();
      if (isMapSlot) {
        cleanupTaskList.add(cleanup[0]);
      } else {
        cleanupTaskList.add(cleanup[1]);
      }
      TaskInProgress tip = findTaskFromList(cleanupTaskList,
                             tts, numUniqueHosts, false);
      if (tip == null) {
        return null;
      }
      
      // Now launch the cleanupTask
      Task result = tip.getTaskToRun(tts.getTrackerName());

      if (result != null) {
        addRunningTaskToTIP(tip, result.getTaskID(), tts, true);
        if (jobFailed) {
          result.setJobCleanupTaskState
          (org.apache.hadoop.mapreduce.JobStatus.State.FAILED);
        } else if (jobKilled) {
          result.setJobCleanupTaskState
          (org.apache.hadoop.mapreduce.JobStatus.State.KILLED);
        } else {
          result.setJobCleanupTaskState
          (org.apache.hadoop.mapreduce.JobStatus.State.SUCCEEDED);
        }
      }
      return result;
    }
    
  }
  
  /**
   * Check whether cleanup task can be launched for the job.
   * 
   * Cleanup task can be launched if it is not already launched
   * or job is Killed
   * or all maps and reduces are complete
   * @return true/false
   */
  private synchronized boolean canLaunchJobCleanupTask() {
    // check if the job is running
    if (status.getRunState() != JobStatus.RUNNING &&
        status.getRunState() != JobStatus.PREP) {
      return false;
    }
    // check if cleanup task has been launched already or if setup isn't
    // launched already. The later check is useful when number of maps is
    // zero.
    if (launchedCleanup || !isSetupFinished()) {
      return false;
    }
    // check if job has failed or killed
    if (jobKilled || jobFailed) {
      return true;
    }
    // Check if all maps and reducers have finished.
    boolean launchCleanupTask = 
        ((finishedMapTasks + failedMapTIPs) == (numMapTasks));
    if (launchCleanupTask) {
      launchCleanupTask = 
        ((finishedReduceTasks + failedReduceTIPs) == numReduceTasks);
    }
    return launchCleanupTask;
  }

  /**
   * Return a SetupTask, if appropriate, to run on the given tasktracker
   * 
   */
  public Task obtainJobSetupTask(TaskTrackerStatus tts, 
                                             int clusterSize, 
                                             int numUniqueHosts,
                                             boolean isMapSlot
                                            ) throws IOException {
    if(!tasksInited) {
      return null;
    }
    
    synchronized(this) {
      if (!canLaunchSetupTask()) {
        return null;
      }
      String taskTracker = tts.getTrackerName();
      // Update the last-known clusterSize
      this.clusterSize = clusterSize;
      if (!shouldRunOnTaskTracker(taskTracker)) {
        return null;
      }
      
      List<TaskInProgress> setupTaskList = new ArrayList<TaskInProgress>();
      if (isMapSlot) {
        setupTaskList.add(setup[0]);
      } else {
        setupTaskList.add(setup[1]);
      }
      TaskInProgress tip = findTaskFromList(setupTaskList,
                             tts, numUniqueHosts, false);
      if (tip == null) {
        return null;
      }
      
      // Now launch the setupTask
      Task result = tip.getTaskToRun(tts.getTrackerName());
      if (result != null) {
        addRunningTaskToTIP(tip, result.getTaskID(), tts, true);
      }
      return result;
    }
  }
  
  public synchronized boolean scheduleReduces() {
    return finishedMapTasks >= completedMapsForReduceSlowstart;
  }
  
  /**
   * Check whether setup task can be launched for the job.
   * 
   * Setup task can be launched after the tasks are inited
   * and Job is in PREP state
   * and if it is not already launched
   * or job is not Killed/Failed
   * @return true/false
   */
  private synchronized boolean canLaunchSetupTask() {
    return (tasksInited && status.getRunState() == JobStatus.PREP && 
           !launchedSetup && !jobKilled && !jobFailed);
  }
  

  /**
   * Return a ReduceTask, if appropriate, to run on the given tasktracker.
   * We don't have cache-sensitivity for reduce tasks, as they
   *  work on temporary MapRed files.  
   */
  public synchronized Task obtainNewReduceTask(TaskTrackerStatus tts,
                                               int clusterSize,
                                               int numUniqueHosts
                                              ) throws IOException {
    if (status.getRunState() != JobStatus.RUNNING) {
      LOG.info("Cannot create task split for " + profile.getJobID());
      return null;
    }
    
    /** check to see if we have any misbehaving reducers. If the expected output
     * for reducers is huge then we just fail the job and error out. The estimated
     * size is divided by 2 since the resource estimator returns the amount of disk 
     * space the that the reduce will use (which is 2 times the input, space for merge + reduce
     * input). **/
    long estimatedReduceInputSize = resourceEstimator.getEstimatedReduceInputSize()/2;
    if (((estimatedReduceInputSize) > 
      reduce_input_limit) && (reduce_input_limit > 0L)) {
      // make sure jobtracker lock is held
      LOG.info("Exceeded limit for reduce input size: Estimated:" + 
          estimatedReduceInputSize + " Limit: " + 
          reduce_input_limit + " Failing Job " + jobId);
      status.setFailureInfo("Job exceeded Reduce Input limit " 
          + " Limit:  " + reduce_input_limit + 
          " Estimated: " + estimatedReduceInputSize);
      jobtracker.failJob(this);
      return null;
    }
    
    // Ensure we have sufficient map outputs ready to shuffle before 
    // scheduling reduces
    if (!scheduleReduces()) {
      return null;
    }

    int  target = findNewReduceTask(tts, clusterSize, numUniqueHosts, 
                                    status.reduceProgress());
    if (target == -1) {
      return null;
    }
    
    Task result = reduces[target].getTaskToRun(tts.getTrackerName());
    if (result != null) {
      addRunningTaskToTIP(reduces[target], result.getTaskID(), tts, true);
    }

    return result;
  }
  
  // returns the (cache)level at which the nodes matches
  private int getMatchingLevelForNodes(Node n1, Node n2) {
    return getMatchingLevelForNodes(n1, n2, this.maxLevel);
  }

  static int getMatchingLevelForNodes(Node n1, Node n2, int maxLevel) {
    int count = 0;

    // In the case that the two nodes are at different levels in the
    // node heirarchy, walk upwards on the deeper one until the
    // levels are equal. Each of these counts as "distance" since it
    // assumedly is going through another rack.
    int level1 = n1.getLevel(), level2 = n2.getLevel();
    while (n1 != null && level1 > level2) {
      n1 = n1.getParent();
      level1--;
      count++;
    }
    while (n2 != null && level2 > level1) {
      n2 = n2.getParent();
      level2--;
      count++;
    }

    do {
      if (n1.equals(n2) || count >= maxLevel) {
        return Math.min(count, maxLevel);
      }
      ++count;
      n1 = n1.getParent();
      n2 = n2.getParent();
    } while (n1 != null);
    return maxLevel;
  }

  /**
   * Populate the data structures as a task is scheduled.
   * 
   * Assuming {@link JobTracker} is locked on entry.
   * 
   * @param tip The tip for which the task is added
   * @param id The attempt-id for the task
   * @param tts task-tracker status
   * @param isScheduled Whether this task is scheduled from the JT or has 
   *        joined back upon restart
   */
  synchronized void addRunningTaskToTIP(TaskInProgress tip, TaskAttemptID id, 
                                        TaskTrackerStatus tts, 
                                        boolean isScheduled) {
    // Make an entry in the tip if the attempt is not scheduled i.e externally
    // added
    if (!isScheduled) {
      tip.addRunningTask(id, tts.getTrackerName());
    }
    final JobTrackerInstrumentation metrics = jobtracker.getInstrumentation();

    // keeping the earlier ordering intact
    String name;
    String splits = "";
    Enum counter = null;
    if (tip.isJobSetupTask()) {
      launchedSetup = true;
      name = Values.SETUP.name();
    } else if (tip.isJobCleanupTask()) {
      launchedCleanup = true;
      name = Values.CLEANUP.name();
    } else if (tip.isMapTask()) {
      ++runningMapTasks;
      name = Values.MAP.name();
      counter = Counter.TOTAL_LAUNCHED_MAPS;
      splits = tip.getSplitNodes();
      if (tip.getActiveTasks().size() > 1)
        speculativeMapTasks++;
      metrics.launchMap(id);
    } else {
      ++runningReduceTasks;
      name = Values.REDUCE.name();
      counter = Counter.TOTAL_LAUNCHED_REDUCES;
      if (tip.getActiveTasks().size() > 1)
        speculativeReduceTasks++;
      metrics.launchReduce(id);
    }
    // Note that the logs are for the scheduled tasks only. Tasks that join on 
    // restart has already their logs in place.
    if (tip.isFirstAttempt(id)) {
      JobHistory.Task.logStarted(tip.getTIPId(), name,
                                 tip.getExecStartTime(), splits);
      setFirstTaskLaunchTime(tip);
    }
    if (!tip.isJobSetupTask() && !tip.isJobCleanupTask()) {
      jobCounters.incrCounter(counter, 1);
    }
    
    //TODO The only problem with these counters would be on restart.
    // The jobtracker updates the counter only when the task that is scheduled
    // if from a non-running tip and is local (data, rack ...). But upon restart
    // as the reports come from the task tracker, there is no good way to infer
    // when exactly to increment the locality counters. The only solution is to 
    // increment the counters for all the tasks irrespective of 
    //    - whether the tip is running or not
    //    - whether its a speculative task or not
    //
    // So to simplify, increment the data locality counter whenever there is 
    // data locality.
    if (tip.isMapTask() && !tip.isJobSetupTask() && !tip.isJobCleanupTask()) {
      // increment the data locality counter for maps
      int level = getLocalityLevel(tip, tts);
      switch (level) {
      case 0 :
        LOG.info("Choosing data-local task " + tip.getTIPId());
        jobCounters.incrCounter(Counter.DATA_LOCAL_MAPS, 1);
        break;
      case 1:
        LOG.info("Choosing rack-local task " + tip.getTIPId());
        jobCounters.incrCounter(Counter.RACK_LOCAL_MAPS, 1);
        break;
      default :
        // check if there is any locality
        if (level != this.maxLevel) {
          LOG.info("Choosing cached task at level " + level + tip.getTIPId());
          jobCounters.incrCounter(Counter.OTHER_LOCAL_MAPS, 1);
        }
        break;
      }
    }
  }

  void setFirstTaskLaunchTime(TaskInProgress tip) {
    TaskType key = tip.getFirstTaskType();

    synchronized(firstTaskLaunchTimes) {
      // Could be optimized to do only one lookup with a little more code
      if (!firstTaskLaunchTimes.containsKey(key)) {
        firstTaskLaunchTimes.put(key, tip.getExecStartTime());
      }
    }
  }

  static String convertTrackerNameToHostName(String trackerName) {
    // Ugly!
    // Convert the trackerName to it's host name
    int indexOfColon = trackerName.indexOf(":");
    String trackerHostName = (indexOfColon == -1) ? 
      trackerName : 
      trackerName.substring(0, indexOfColon);
    return trackerHostName.substring("tracker_".length());
  }
    
  /**
   * Note that a task has failed on a given tracker and add the tracker  
   * to the blacklist iff too many trackers in the cluster i.e. 
   * (clusterSize * CLUSTER_BLACKLIST_PERCENT) haven't turned 'flaky' already.
   * 
   * @param taskTracker task-tracker on which a task failed
   */
  synchronized void addTrackerTaskFailure(String trackerName, 
                                          TaskTracker taskTracker) {
    if (flakyTaskTrackers < (clusterSize * CLUSTER_BLACKLIST_PERCENT)) { 
      String trackerHostName = convertTrackerNameToHostName(trackerName);

      Integer trackerFailures = trackerToFailuresMap.get(trackerHostName);
      if (trackerFailures == null) {
        trackerFailures = 0;
      }
      trackerToFailuresMap.put(trackerHostName, ++trackerFailures);

      // Check if this tasktracker has turned 'flaky'
      if (trackerFailures.intValue() == maxTaskFailuresPerTracker) {
        ++flakyTaskTrackers;
        
        // Cancel reservations if appropriate
        if (taskTracker != null) {
          if (trackersReservedForMaps.containsKey(taskTracker)) {
            taskTracker.unreserveSlots(TaskType.MAP, this);
          }
          if (trackersReservedForReduces.containsKey(taskTracker)) {
            taskTracker.unreserveSlots(TaskType.REDUCE, this);
          }
        }
        LOG.info("TaskTracker at '" + trackerHostName + "' turned 'flaky'");
      }
    }
  }
  
  public synchronized void reserveTaskTracker(TaskTracker taskTracker,
                                              TaskType type, int numSlots) {
    Map<TaskTracker, FallowSlotInfo> map =
      (type == TaskType.MAP) ? trackersReservedForMaps : trackersReservedForReduces;
    
    long now = jobtracker.getClock().getTime();
    
    FallowSlotInfo info = map.get(taskTracker);
    int reservedSlots = 0;
    if (info == null) {
      info = new FallowSlotInfo(now, numSlots);
      reservedSlots = numSlots;
    } else {
      // Increment metering info if the reservation is changing
      if (info.getNumSlots() != numSlots) {
        Enum<Counter> counter = 
          (type == TaskType.MAP) ? 
              Counter.FALLOW_SLOTS_MILLIS_MAPS : 
              Counter.FALLOW_SLOTS_MILLIS_REDUCES;
        long fallowSlotMillis = (now - info.getTimestamp()) * info.getNumSlots();
        jobCounters.incrCounter(counter, fallowSlotMillis);
        
        // Update 
        reservedSlots = numSlots - info.getNumSlots();
        info.setTimestamp(now);
        info.setNumSlots(numSlots);
      }
    }
    map.put(taskTracker, info);
    if (type == TaskType.MAP) {
      jobtracker.getInstrumentation().addReservedMapSlots(reservedSlots);
    }
    else {
      jobtracker.getInstrumentation().addReservedReduceSlots(reservedSlots);
    }
    jobtracker.incrementReservations(type, reservedSlots);
  }
  
  public synchronized void unreserveTaskTracker(TaskTracker taskTracker,
                                                TaskType type) {
    Map<TaskTracker, FallowSlotInfo> map =
      (type == TaskType.MAP) ? trackersReservedForMaps : 
                               trackersReservedForReduces;

    FallowSlotInfo info = map.get(taskTracker);
    if (info == null) {
      LOG.warn("Cannot find information about fallow slots for " + 
               taskTracker.getTrackerName());
      return;
    }
    
    long now = jobtracker.getClock().getTime();

    Enum<Counter> counter = 
      (type == TaskType.MAP) ? 
          Counter.FALLOW_SLOTS_MILLIS_MAPS : 
          Counter.FALLOW_SLOTS_MILLIS_REDUCES;
    long fallowSlotMillis = (now - info.getTimestamp()) * info.getNumSlots();
    jobCounters.incrCounter(counter, fallowSlotMillis);

    map.remove(taskTracker);
    if (type == TaskType.MAP) {
      jobtracker.getInstrumentation().decReservedMapSlots(info.getNumSlots());
    }
    else {
      jobtracker.getInstrumentation().decReservedReduceSlots(
        info.getNumSlots());
    }
    jobtracker.decrementReservations(type, info.getNumSlots());
  }
  
  public int getNumReservedTaskTrackersForMaps() {
    return trackersReservedForMaps.size();
  }
  
  public int getNumReservedTaskTrackersForReduces() {
    return trackersReservedForReduces.size();
  }
  
  private int getTrackerTaskFailures(String trackerName) {
    String trackerHostName = convertTrackerNameToHostName(trackerName);
    Integer failedTasks = trackerToFailuresMap.get(trackerHostName);
    return (failedTasks != null) ? failedTasks.intValue() : 0; 
  }
    
  /**
   * Get the black listed trackers for the job
   * 
   * @return List of blacklisted tracker names
   */
  List<String> getBlackListedTrackers() {
    List<String> blackListedTrackers = new ArrayList<String>();
    for (Map.Entry<String,Integer> e : trackerToFailuresMap.entrySet()) {
       if (e.getValue().intValue() >= maxTaskFailuresPerTracker) {
         blackListedTrackers.add(e.getKey());
       }
    }
    return blackListedTrackers;
  }
  
  /**
   * Get the no. of 'flaky' tasktrackers for a given job.
   * 
   * @return the no. of 'flaky' tasktrackers for a given job.
   */
  int getNoOfBlackListedTrackers() {
    return flakyTaskTrackers;
  }
    
  /**
   * Get the information on tasktrackers and no. of errors which occurred
   * on them for a given job. 
   * 
   * @return the map of tasktrackers and no. of errors which occurred
   *         on them for a given job. 
   */
  synchronized Map<String, Integer> getTaskTrackerErrors() {
    // Clone the 'trackerToFailuresMap' and return the copy
    Map<String, Integer> trackerErrors = 
      new TreeMap<String, Integer>(trackerToFailuresMap);
    return trackerErrors;
  }

  /**
   * Remove a map TIP from the lists for running maps.
   * Called when a map fails/completes (note if a map is killed,
   * it won't be present in the list since it was completed earlier)
   * @param tip the tip that needs to be retired
   */
  private synchronized void retireMap(TaskInProgress tip) {
    if (runningMapCache == null) {
      LOG.warn("Running cache for maps missing!! "
               + "Job details are missing.");
      return;
    }
    
    String[] splitLocations = tip.getSplitLocations();

    // Remove the TIP from the list for running non-local maps
    if (splitLocations == null || splitLocations.length == 0) {
      nonLocalRunningMaps.remove(tip);
      return;
    }

    // Remove from the running map caches
    for(String host: splitLocations) {
      Node node = jobtracker.getNode(host);

      for (int j = 0; j < maxLevel; ++j) {
        Set<TaskInProgress> hostMaps = runningMapCache.get(node);
        if (hostMaps != null) {
          hostMaps.remove(tip);
          if (hostMaps.size() == 0) {
            runningMapCache.remove(node);
          }
        }
        node = node.getParent();
      }
    }
  }

  /**
   * Remove a reduce TIP from the list for running-reduces
   * Called when a reduce fails/completes
   * @param tip the tip that needs to be retired
   */
  private synchronized void retireReduce(TaskInProgress tip) {
    if (runningReduces == null) {
      LOG.warn("Running list for reducers missing!! "
               + "Job details are missing.");
      return;
    }
    runningReduces.remove(tip);
  }

  /**
   * Adds a map tip to the list of running maps.
   * @param tip the tip that needs to be scheduled as running
   */
  protected synchronized void scheduleMap(TaskInProgress tip) {
    
    if (runningMapCache == null) {
      LOG.warn("Running cache for maps is missing!! " 
               + "Job details are missing.");
      return;
    }
    String[] splitLocations = tip.getSplitLocations();

    // Add the TIP to the list of non-local running TIPs
    if (splitLocations == null || splitLocations.length == 0) {
      nonLocalRunningMaps.add(tip);
      return;
    }

    for(String host: splitLocations) {
      Node node = jobtracker.getNode(host);

      for (int j = 0; j < maxLevel; ++j) {
        Set<TaskInProgress> hostMaps = runningMapCache.get(node);
        if (hostMaps == null) {
          // create a cache if needed
          hostMaps = new LinkedHashSet<TaskInProgress>();
          runningMapCache.put(node, hostMaps);
        }
        hostMaps.add(tip);
        node = node.getParent();
      }
    }
  }
  
  /**
   * Adds a reduce tip to the list of running reduces
   * @param tip the tip that needs to be scheduled as running
   */
  protected synchronized void scheduleReduce(TaskInProgress tip) {
    if (runningReduces == null) {
      LOG.warn("Running cache for reducers missing!! "
               + "Job details are missing.");
      return;
    }
    runningReduces.add(tip);
  }
  
  /**
   * Adds the failed TIP in the front of the list for non-running maps
   * @param tip the tip that needs to be failed
   */
  private synchronized void failMap(TaskInProgress tip) {
    if (failedMaps == null) {
      LOG.warn("Failed cache for maps is missing! Job details are missing.");
      return;
    }

    // Ignore locality for subsequent scheduling on this TIP. Always schedule
    // it ahead of other tasks.
    failedMaps.add(tip);
  }
  
  /**
   * Adds a failed TIP in the front of the list for non-running reduces
   * @param tip the tip that needs to be failed
   */
  private synchronized void failReduce(TaskInProgress tip) {
    if (nonRunningReduces == null) {
      LOG.warn("Failed cache for reducers missing!! "
               + "Job details are missing.");
      return;
    }
    nonRunningReduces.add(tip);
  }
  
  /**
   * Find a non-running task in the passed list of TIPs
   * @param tips a collection of TIPs
   * @param ttStatus the status of tracker that has requested a task to run
   * @param numUniqueHosts number of unique hosts that run trask trackers
   * @param removeFailedTip whether to remove the failed tips
   */
  private synchronized TaskInProgress findTaskFromList(
      Collection<TaskInProgress> tips, TaskTrackerStatus ttStatus,
      int numUniqueHosts,
      boolean removeFailedTip) {
    Iterator<TaskInProgress> iter = tips.iterator();
    while (iter.hasNext()) {
      TaskInProgress tip = iter.next();

      // Select a tip if
      //   1. runnable   : still needs to be run and is not completed
      //   2. ~running   : no other node is running it
      //   3. earlier attempt failed : has not failed on this host
      //                               and has failed on all the other hosts
      // A TIP is removed from the list if 
      // (1) this tip is scheduled
      // (2) if the passed list is a level 0 (host) cache
      // (3) when the TIP is non-schedulable (running, killed, complete)
      if (tip.isRunnable() && !tip.isRunning()) {
        // check if the tip has failed on this host
        if (!tip.hasFailedOnMachine(ttStatus.getHost()) || 
             tip.getNumberOfFailedMachines() >= numUniqueHosts) {
          // check if the tip has failed on all the nodes
          iter.remove();
          return tip;
        } else if (removeFailedTip) { 
          // the case where we want to remove a failed tip from the host cache
          // point#3 in the TIP removal logic above
          iter.remove();
        }
      } else {
        // see point#3 in the comment above for TIP removal logic
        iter.remove();
      }
    }
    return null;
  }
  
  /**
   * Find a speculative task
   * @param list a list of tips
   * @param ttStatus status of the tracker that has requested a tip
   * @param avgProgress the average progress for speculation
   * @param currentTime current time in milliseconds
   * @param shouldRemove whether to remove the tips
   * @return a tip that can be speculated on the tracker
   */
  protected synchronized TaskInProgress findSpeculativeTask(
      Collection<TaskInProgress> list, TaskTrackerStatus ttStatus,
      double avgProgress, long currentTime, boolean shouldRemove) {
    
    Iterator<TaskInProgress> iter = list.iterator();

    while (iter.hasNext()) {
      TaskInProgress tip = iter.next();
      // should never be true! (since we delete completed/failed tasks)
      if (!tip.isRunning() || !tip.isRunnable()) {
        iter.remove();
        continue;
      }

      if (!tip.hasRunOnMachine(ttStatus.getHost(), 
                               ttStatus.getTrackerName())) {
        if (tip.hasSpeculativeTask(currentTime, avgProgress)) {
          // In case of shared list we don't remove it. Since the TIP failed 
          // on this tracker can be scheduled on some other tracker.
          if (shouldRemove) {
            iter.remove(); //this tracker is never going to run it again
          }
          return tip;
        } 
      } else {
        // Check if this tip can be removed from the list.
        // If the list is shared then we should not remove.
        if (shouldRemove) {
          // This tracker will never speculate this tip
          iter.remove();
        }
      }
    }
    return null;
  }
  
  /**
   * Find new map task
   * @param tts The task tracker that is asking for a task
   * @param clusterSize The number of task trackers in the cluster
   * @param numUniqueHosts The number of hosts that run task trackers
   * @param avgProgress The average progress of this kind of task in this job
   * @param maxCacheLevel The maximum topology level until which to schedule
   *                      maps. 
   *                      A value of {@link #anyCacheLevel} implies any 
   *                      available task (node-local, rack-local, off-switch and 
   *                      speculative tasks).
   *                      A value of {@link #NON_LOCAL_CACHE_LEVEL} implies only
   *                      off-switch/speculative tasks should be scheduled.
   * @return the index in tasks of the selected task (or -1 for no task)
   */
  private synchronized int findNewMapTask(final TaskTrackerStatus tts, 
                                          final int clusterSize,
                                          final int numUniqueHosts,
                                          final int maxCacheLevel,
                                          final double avgProgress) {
    if (numMapTasks == 0) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("No maps to schedule for " + profile.getJobID());
      }
      return -1;
    }

    String taskTracker = tts.getTrackerName();
    TaskInProgress tip = null;
    
    //
    // Update the last-known clusterSize
    //
    this.clusterSize = clusterSize;

    if (!shouldRunOnTaskTracker(taskTracker)) {
      return -1;
    }

    // Check to ensure this TaskTracker has enough resources to 
    // run tasks from this job
    long outSize = resourceEstimator.getEstimatedMapOutputSize();
    long availSpace = tts.getResourceStatus().getAvailableSpace();
    if(availSpace < outSize) {
      LOG.warn("No room for map task. Node " + tts.getHost() + 
               " has " + availSpace + 
               " bytes free; but we expect map to take " + outSize);

      return -1; //see if a different TIP might work better. 
    }
    
    
    // When scheduling a map task:
    //  0) Schedule a failed task without considering locality
    //  1) Schedule non-running tasks
    //  2) Schedule speculative tasks
    //  3) Schedule tasks with no location information

    // First a look up is done on the non-running cache and on a miss, a look 
    // up is done on the running cache. The order for lookup within the cache:
    //   1. from local node to root [bottom up]
    //   2. breadth wise for all the parent nodes at max level
    // We fall to linear scan of the list ((3) above) if we have misses in the 
    // above caches

    // 0) Schedule the task with the most failures, unless failure was on this
    //    machine
    tip = findTaskFromList(failedMaps, tts, numUniqueHosts, false);
    if (tip != null) {
      // Add to the running list
      scheduleMap(tip);
      LOG.info("Choosing a failed task " + tip.getTIPId());
      return tip.getIdWithinJob();
    }

    Node node = jobtracker.getNode(tts.getHost());
    
    //
    // 1) Non-running TIP :
    // 

    // 1. check from local node to the root [bottom up cache lookup]
    //    i.e if the cache is available and the host has been resolved
    //    (node!=null)
    if (node != null) {
      Node key = node;
      int level = 0;
      // maxCacheLevel might be greater than this.maxLevel if findNewMapTask is
      // called to schedule any task (local, rack-local, off-switch or
      // speculative) tasks or it might be NON_LOCAL_CACHE_LEVEL (i.e. -1) if
      // findNewMapTask is (i.e. -1) if findNewMapTask is to only schedule
      // off-switch/speculative tasks
      int maxLevelToSchedule = Math.min(maxCacheLevel, maxLevel);
      for (level = 0;level < maxLevelToSchedule; ++level) {
        List <TaskInProgress> cacheForLevel = nonRunningMapCache.get(key);
        if (cacheForLevel != null) {
          tip = findTaskFromList(cacheForLevel, tts, 
              numUniqueHosts,level == 0);
          if (tip != null) {
            // Add to running cache
            scheduleMap(tip);

            // remove the cache if its empty
            if (cacheForLevel.size() == 0) {
              nonRunningMapCache.remove(key);
            }

            return tip.getIdWithinJob();
          }
        }
        key = key.getParent();
      }
      
      // Check if we need to only schedule a local task (node-local/rack-local)
      if (level == maxCacheLevel) {
        return -1;
      }
    }

    //2. Search breadth-wise across parents at max level for non-running 
    //   TIP if
    //     - cache exists and there is a cache miss 
    //     - node information for the tracker is missing (tracker's topology
    //       info not obtained yet)

    // collection of node at max level in the cache structure
    Collection<Node> nodesAtMaxLevel = jobtracker.getNodesAtMaxLevel();

    // get the node parent at max level
    Node nodeParentAtMaxLevel = 
      (node == null) ? null : JobTracker.getParentNode(node, maxLevel - 1);
    
    for (Node parent : nodesAtMaxLevel) {

      // skip the parent that has already been scanned
      if (parent == nodeParentAtMaxLevel) {
        continue;
      }

      List<TaskInProgress> cache = nonRunningMapCache.get(parent);
      if (cache != null) {
        tip = findTaskFromList(cache, tts, numUniqueHosts, false);
        if (tip != null) {
          // Add to the running cache
          scheduleMap(tip);

          // remove the cache if empty
          if (cache.size() == 0) {
            nonRunningMapCache.remove(parent);
          }
          LOG.info("Choosing a non-local task " + tip.getTIPId());
          return tip.getIdWithinJob();
        }
      }
    }

    // 3. Search non-local tips for a new task
    tip = findTaskFromList(nonLocalMaps, tts, numUniqueHosts, false);
    if (tip != null) {
      // Add to the running list
      scheduleMap(tip);

      LOG.info("Choosing a non-local task " + tip.getTIPId());
      return tip.getIdWithinJob();
    }

    //
    // 2) Running TIP :
    // 
 
    if (hasSpeculativeMaps) {
      long currentTime = jobtracker.getClock().getTime();

      // 1. Check bottom up for speculative tasks from the running cache
      if (node != null) {
        Node key = node;
        for (int level = 0; level < maxLevel; ++level) {
          Set<TaskInProgress> cacheForLevel = runningMapCache.get(key);
          if (cacheForLevel != null) {
            tip = findSpeculativeTask(cacheForLevel, tts, 
                                      avgProgress, currentTime, level == 0);
            if (tip != null) {
              if (cacheForLevel.size() == 0) {
                runningMapCache.remove(key);
              }
              return tip.getIdWithinJob();
            }
          }
          key = key.getParent();
        }
      }

      // 2. Check breadth-wise for speculative tasks
      
      for (Node parent : nodesAtMaxLevel) {
        // ignore the parent which is already scanned
        if (parent == nodeParentAtMaxLevel) {
          continue;
        }

        Set<TaskInProgress> cache = runningMapCache.get(parent);
        if (cache != null) {
          tip = findSpeculativeTask(cache, tts, avgProgress, 
                                    currentTime, false);
          if (tip != null) {
            // remove empty cache entries
            if (cache.size() == 0) {
              runningMapCache.remove(parent);
            }
            LOG.info("Choosing a non-local task " + tip.getTIPId() 
                     + " for speculation");
            return tip.getIdWithinJob();
          }
        }
      }

      // 3. Check non-local tips for speculation
      tip = findSpeculativeTask(nonLocalRunningMaps, tts, avgProgress, 
                                currentTime, false);
      if (tip != null) {
        LOG.info("Choosing a non-local task " + tip.getTIPId() 
                 + " for speculation");
        return tip.getIdWithinJob();
      }
    }
    
    return -1;
  }

  /**
   * Find new reduce task
   * @param tts The task tracker that is asking for a task
   * @param clusterSize The number of task trackers in the cluster
   * @param numUniqueHosts The number of hosts that run task trackers
   * @param avgProgress The average progress of this kind of task in this job
   * @return the index in tasks of the selected task (or -1 for no task)
   */
  private synchronized int findNewReduceTask(TaskTrackerStatus tts, 
                                             int clusterSize,
                                             int numUniqueHosts,
                                             double avgProgress) {
    if (numReduceTasks == 0) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("No reduces to schedule for " + profile.getJobID());
      }
      return -1;
    }

    String taskTracker = tts.getTrackerName();
    TaskInProgress tip = null;

    // Update the last-known clusterSize
    this.clusterSize = clusterSize;
    
    if (!shouldRunOnTaskTracker(taskTracker)) {
      return -1;
    }

    long outSize = resourceEstimator.getEstimatedReduceInputSize();
    long availSpace = tts.getResourceStatus().getAvailableSpace();
    if(availSpace < outSize) {
      LOG.warn("No room for reduce task. Node " + taskTracker + " has " +
                availSpace + 
               " bytes free; but we expect reduce input to take " + outSize);

      return -1; //see if a different TIP might work better. 
    }
    
    // 1. check for a never-executed reduce tip
    // reducers don't have a cache and so pass -1 to explicitly call that out
    tip = findTaskFromList(nonRunningReduces, tts, numUniqueHosts, false);
    if (tip != null) {
      scheduleReduce(tip);
      return tip.getIdWithinJob();
    }

    // 2. check for a reduce tip to be speculated
    if (hasSpeculativeReduces) {
      tip = findSpeculativeTask(runningReduces, tts, avgProgress, 
                                jobtracker.getClock().getTime(), false);
      if (tip != null) {
        scheduleReduce(tip);
        return tip.getIdWithinJob();
      }
    }

    return -1;
  }
  
  private boolean shouldRunOnTaskTracker(String taskTracker) {
    //
    // Check if too many tasks of this job have failed on this
    // tasktracker prior to assigning it a new one.
    //
    int taskTrackerFailedTasks = getTrackerTaskFailures(taskTracker);
    if ((flakyTaskTrackers < (clusterSize * CLUSTER_BLACKLIST_PERCENT)) && 
        taskTrackerFailedTasks >= maxTaskFailuresPerTracker) {
      if (LOG.isDebugEnabled()) {
        String flakyTracker = convertTrackerNameToHostName(taskTracker); 
        LOG.debug("Ignoring the black-listed tasktracker: '" + flakyTracker 
                  + "' for assigning a new task");
      }
      return false;
    }
    return true;
  }

  /**
   * Metering: Occupied Slots * (Finish - Start)
   * @param tip {@link TaskInProgress} to be metered which just completed, 
   *            cannot be <code>null</code> 
   * @param status {@link TaskStatus} of the completed task, cannot be 
   *               <code>null</code>
   */
  private void meterTaskAttempt(TaskInProgress tip, TaskStatus status) {
    Counter slotCounter = 
      (tip.isMapTask()) ? Counter.SLOTS_MILLIS_MAPS : 
                          Counter.SLOTS_MILLIS_REDUCES;
    jobCounters.incrCounter(slotCounter, 
                            tip.getNumSlotsRequired() * 
                            (status.getFinishTime() - status.getStartTime()));
  }
  
  /**
   * A taskid assigned to this JobInProgress has reported in successfully.
   */
  public synchronized boolean completedTask(TaskInProgress tip, 
                                            TaskStatus status)
  {
    TaskAttemptID taskid = status.getTaskID();
    int oldNumAttempts = tip.getActiveTasks().size();
    final JobTrackerInstrumentation metrics = jobtracker.getInstrumentation();
        
    // Metering
    meterTaskAttempt(tip, status);
    
    // Sanity check: is the TIP already complete? 
    // It _is_ safe to not decrement running{Map|Reduce}Tasks and
    // finished{Map|Reduce}Tasks variables here because one and only
    // one task-attempt of a TIP gets to completedTask. This is because
    // the TaskCommitThread in the JobTracker marks other, completed, 
    // speculative tasks as _complete_.
    if (tip.isComplete()) {
      // Mark this task as KILLED
      tip.alreadyCompletedTask(taskid);

      // Let the JobTracker cleanup this taskid if the job isn't running
      if (this.status.getRunState() != JobStatus.RUNNING) {
        jobtracker.markCompletedTaskAttempt(status.getTaskTracker(), taskid);
      }
      return false;
    } 

    LOG.info("Task '" + taskid + "' has completed " + tip.getTIPId() + 
             " successfully.");          

    // Mark the TIP as complete
    tip.completed(taskid);
    resourceEstimator.updateWithCompletedTask(status, tip);

    // Update jobhistory 
    TaskTrackerStatus ttStatus = 
      this.jobtracker.getTaskTrackerStatus(status.getTaskTracker());
    String trackerHostname = jobtracker.getNode(ttStatus.getHost()).toString();
    String taskType = getTaskType(tip);
    if (status.getIsMap()){
      JobHistory.MapAttempt.logStarted(status.getTaskID(), status.getStartTime(), 
                                       status.getTaskTracker(), 
                                       ttStatus.getHttpPort(), 
                                       taskType); 
      JobHistory.MapAttempt.logFinished(status.getTaskID(), status.getFinishTime(), 
                                        trackerHostname, taskType,
                                        status.getStateString(), 
                                        status.getCounters()); 
    }else{
      JobHistory.ReduceAttempt.logStarted( status.getTaskID(), status.getStartTime(), 
                                          status.getTaskTracker(),
                                          ttStatus.getHttpPort(), 
                                          taskType); 
      JobHistory.ReduceAttempt.logFinished(status.getTaskID(), status.getShuffleFinishTime(),
                                           status.getSortFinishTime(), status.getFinishTime(), 
                                           trackerHostname, 
                                           taskType,
                                           status.getStateString(), 
                                           status.getCounters()); 
    }
    JobHistory.Task.logFinished(tip.getTIPId(), 
                                taskType,
                                tip.getExecFinishTime(),
                                status.getCounters()); 
        
    int newNumAttempts = tip.getActiveTasks().size();
    if (tip.isJobSetupTask()) {
      // setup task has finished. kill the extra setup tip
      killSetupTip(!tip.isMapTask());
      // Job can start running now.
      this.status.setSetupProgress(1.0f);
      // move the job to running state if the job is in prep state
      if (this.status.getRunState() == JobStatus.PREP) {
        changeStateTo(JobStatus.RUNNING);
        JobHistory.JobInfo.logStarted(profile.getJobID());
      }
    } else if (tip.isJobCleanupTask()) {
      // cleanup task has finished. Kill the extra cleanup tip
      if (tip.isMapTask()) {
        // kill the reduce tip
        cleanup[1].kill();
      } else {
        cleanup[0].kill();
      }
      //
      // The Job is done
      // if the job is failed, then mark the job failed.
      if (jobFailed) {
        terminateJob(JobStatus.FAILED);
      }
      // if the job is killed, then mark the job killed.
      if (jobKilled) {
        terminateJob(JobStatus.KILLED);
      }
      else {
        jobComplete();
      }
      // The job has been killed/failed/successful
      // JobTracker should cleanup this task
      jobtracker.markCompletedTaskAttempt(status.getTaskTracker(), taskid);
    } else if (tip.isMapTask()) {
      runningMapTasks -= 1;
      // check if this was a sepculative task
      if (oldNumAttempts > 1) {
        speculativeMapTasks -= (oldNumAttempts - newNumAttempts);
      }
      finishedMapTasks += 1;
      metrics.completeMap(taskid);
      // remove the completed map from the resp running caches
      retireMap(tip);
      if ((finishedMapTasks + failedMapTIPs) == (numMapTasks)) {
        this.status.setMapProgress(1.0f);
        if (canLaunchJobCleanupTask()) {
          checkCounterLimitsAndFail();
        }
      }
    } else {
      runningReduceTasks -= 1;
      if (oldNumAttempts > 1) {
        speculativeReduceTasks -= (oldNumAttempts - newNumAttempts);
      }
      finishedReduceTasks += 1;
      metrics.completeReduce(taskid);
      // remove the completed reduces from the running reducers set
      retireReduce(tip);
      if ((finishedReduceTasks + failedReduceTIPs) == (numReduceTasks)) {
        this.status.setReduceProgress(1.0f);
        if (canLaunchJobCleanupTask()) {
          checkCounterLimitsAndFail();
        }
      }
    }
    return true;
  }
  
  /**
   * add up the counters and fail the job
   * if it exceeds the counters. Make sure we do not
   * recalculate the coutners after we fail the job. Currently
   * this is taken care by terminateJob() since it does not 
   * calculate the counters.
   */
  private void checkCounterLimitsAndFail() {
    boolean mapIsFine, reduceIsFine, jobIsFine = true;
    mapIsFine = getMapCounters(new Counters());
    reduceIsFine = getReduceCounters(new Counters());
    jobIsFine = getCounters(new Counters());
    if (!(mapIsFine && reduceIsFine && jobIsFine)) {
      status.setFailureInfo("Counters Exceeded: Limit: " + 
          Counters.MAX_COUNTER_LIMIT);
      jobtracker.failJob(this);
    }
  }
  
  /**
   * Job state change must happen thru this call
   */
  private void changeStateTo(int newState) {
    int oldState = this.status.getRunState();
    if (oldState == newState) {
      return; //old and new states are same
    }
    this.status.setRunState(newState);
    
    //update the metrics
    if (oldState == JobStatus.PREP) {
      this.jobtracker.getInstrumentation().decPrepJob(conf, jobId);
    } else if (oldState == JobStatus.RUNNING) {
      this.jobtracker.getInstrumentation().decRunningJob(conf, jobId);
    }
    
    if (newState == JobStatus.PREP) {
      this.jobtracker.getInstrumentation().addPrepJob(conf, jobId);
    } else if (newState == JobStatus.RUNNING) {
      this.jobtracker.getInstrumentation().addRunningJob(conf, jobId);
    }
    
  }

  /**
   * The job is done since all it's component tasks are either
   * successful or have failed.
   */
  private void jobComplete() {
    final JobTrackerInstrumentation metrics = jobtracker.getInstrumentation();
    //
    // All tasks are complete, then the job is done!
    //
    if (this.status.getRunState() == JobStatus.RUNNING ) {
      changeStateTo(JobStatus.SUCCEEDED);
      this.status.setCleanupProgress(1.0f);
      if (maps.length == 0) {
        this.status.setMapProgress(1.0f);
      }
      if (reduces.length == 0) {
        this.status.setReduceProgress(1.0f);
      }
     
      this.finishTime = jobtracker.getClock().getTime();
      LOG.info("Job " + this.status.getJobID() + 
      " has completed successfully.");

      // Log the job summary (this should be done prior to logging to 
      // job-history to ensure job-counters are in-sync 
      JobSummary.logJobSummary(this, jobtracker.getClusterStatus(false));
      
      Counters mapCounters = new Counters();
      boolean isFine = getMapCounters(mapCounters);
      mapCounters = (isFine ? mapCounters: new Counters());
      Counters reduceCounters = new Counters();
      isFine = getReduceCounters(reduceCounters);;
      reduceCounters = (isFine ? reduceCounters: new Counters());
      Counters jobCounters = new Counters();
      isFine = getCounters(jobCounters);
      jobCounters = (isFine? jobCounters: new Counters());
      
      // Log job-history
      JobHistory.JobInfo.logFinished(this.status.getJobID(), finishTime, 
                                     this.finishedMapTasks, 
                                     this.finishedReduceTasks, failedMapTasks, 
                                     failedReduceTasks, mapCounters,
                                     reduceCounters, jobCounters);
      
      
      // Note that finalize will close the job history handles which garbage collect
      // might try to finalize
      garbageCollect();
      
      metrics.completeJob(this.conf, this.status.getJobID());
    }
  }
  
  private synchronized void terminateJob(int jobTerminationState) {
    if ((status.getRunState() == JobStatus.RUNNING) ||
        (status.getRunState() == JobStatus.PREP)) {
      this.finishTime = jobtracker.getClock().getTime();
      this.status.setMapProgress(1.0f);
      this.status.setReduceProgress(1.0f);
      this.status.setCleanupProgress(1.0f);
      
      if (jobTerminationState == JobStatus.FAILED) {
        changeStateTo(JobStatus.FAILED);

        // Log the job summary
        JobSummary.logJobSummary(this, jobtracker.getClusterStatus(false));
        
        // Log to job-history
        JobHistory.JobInfo.logFailed(this.status.getJobID(), finishTime, 
                                     this.finishedMapTasks, 
                                     this.finishedReduceTasks);
      } else {
        changeStateTo(JobStatus.KILLED);

        // Log the job summary
        JobSummary.logJobSummary(this, jobtracker.getClusterStatus(false));
        
        // Log to job-history
        JobHistory.JobInfo.logKilled(this.status.getJobID(), finishTime, 
                                     this.finishedMapTasks, 
                                     this.finishedReduceTasks);
      }
      garbageCollect();
      
      jobtracker.getInstrumentation().terminateJob(
          this.conf, this.status.getJobID());
      if (jobTerminationState == JobStatus.FAILED) {
        jobtracker.getInstrumentation().failedJob(
            this.conf, this.status.getJobID());
      } else {
        jobtracker.getInstrumentation().killedJob(
            this.conf, this.status.getJobID());
      }
    }
  }

  /**
   * Terminate the job and all its component tasks.
   * Calling this will lead to marking the job as failed/killed. Cleanup 
   * tip will be launched. If the job has not inited, it will directly call 
   * terminateJob as there is no need to launch cleanup tip.
   * This method is reentrant.
   * @param jobTerminationState job termination state
   */
  private synchronized void terminate(int jobTerminationState) {
    if(!tasksInited) {
    	//init could not be done, we just terminate directly.
      terminateJob(jobTerminationState);
      return;
    }

    if ((status.getRunState() == JobStatus.RUNNING) ||
         (status.getRunState() == JobStatus.PREP)) {
      LOG.info("Killing job '" + this.status.getJobID() + "'");
      if (jobTerminationState == JobStatus.FAILED) {
        if(jobFailed) {//reentrant
          return;
        }
        jobFailed = true;
      } else if (jobTerminationState == JobStatus.KILLED) {
        if(jobKilled) {//reentrant
          return;
        }
        jobKilled = true;
      }
      // clear all unclean tasks
      clearUncleanTasks();
      //
      // kill all TIPs.
      //
      for (int i = 0; i < setup.length; i++) {
        setup[i].kill();
      }
      for (int i = 0; i < maps.length; i++) {
        maps[i].kill();
      }
      for (int i = 0; i < reduces.length; i++) {
        reduces[i].kill();
      }
    }
  }

  private void cancelReservedSlots() {
    // Make a copy of the set of TaskTrackers to prevent a 
    // ConcurrentModificationException ...
    Set<TaskTracker> tm = 
      new HashSet<TaskTracker>(trackersReservedForMaps.keySet());
    for (TaskTracker tt : tm) {
      tt.unreserveSlots(TaskType.MAP, this);
    }

    Set<TaskTracker> tr = 
      new HashSet<TaskTracker>(trackersReservedForReduces.keySet());
    for (TaskTracker tt : tr) {
      tt.unreserveSlots(TaskType.REDUCE, this);
    }
  }
  private void clearUncleanTasks() {
    TaskAttemptID taskid = null;
    TaskInProgress tip = null;
    while (!mapCleanupTasks.isEmpty()) {
      taskid = mapCleanupTasks.remove(0);
      tip = maps[taskid.getTaskID().getId()];
      updateTaskStatus(tip, tip.getTaskStatus(taskid));
    }
    while (!reduceCleanupTasks.isEmpty()) {
      taskid = reduceCleanupTasks.remove(0);
      tip = reduces[taskid.getTaskID().getId()];
      updateTaskStatus(tip, tip.getTaskStatus(taskid));
    }
  }

  /**
   * Kill the job and all its component tasks. This method should be called from 
   * jobtracker and should return fast as it locks the jobtracker.
   */
  public void kill() {
    boolean killNow = false;
    synchronized(jobInitKillStatus) {
      jobInitKillStatus.killed = true;
      //if not in middle of init, terminate it now
      if(!jobInitKillStatus.initStarted || jobInitKillStatus.initDone) {
        //avoiding nested locking by setting flag
        killNow = true;
      }
    }
    if(killNow) {
      terminate(JobStatus.KILLED);
    }
  }
  
  /**
   * Fails the job and all its component tasks. This should be called only from
   * {@link JobInProgress} or {@link JobTracker}. Look at 
   * {@link JobTracker#failJob(JobInProgress)} for more details.
   */
  synchronized void fail() {
    terminate(JobStatus.FAILED);
  }
  
  /**
   * A task assigned to this JobInProgress has reported in as failed.
   * Most of the time, we'll just reschedule execution.  However, after
   * many repeated failures we may instead decide to allow the entire 
   * job to fail or succeed if the user doesn't care about a few tasks failing.
   *
   * Even if a task has reported as completed in the past, it might later
   * be reported as failed.  That's because the TaskTracker that hosts a map
   * task might die before the entire job can complete.  If that happens,
   * we need to schedule reexecution so that downstream reduce tasks can 
   * obtain the map task's output.
   */
  private void failedTask(TaskInProgress tip, TaskAttemptID taskid, 
                          TaskStatus status, 
                          TaskTracker taskTracker, boolean wasRunning,
                          boolean wasComplete, boolean wasAttemptRunning) {
    final JobTrackerInstrumentation metrics = jobtracker.getInstrumentation();
    // check if the TIP is already failed
    boolean wasFailed = tip.isFailed();

    // Mark the taskid as FAILED or KILLED
    tip.incompleteSubTask(taskid, this.status);
   
    boolean isRunning = tip.isRunning();
    boolean isComplete = tip.isComplete();
    
    if (wasAttemptRunning) {
      // We are decrementing counters without looking for isRunning ,
      // because we increment the counters when we obtain
      // new map task attempt or reduce task attempt.We do not really check
      // for tip being running.
      // Whenever we obtain new task attempt following counters are incremented.
      //      ++runningMapTasks;
      //.........
      //      metrics.launchMap(id);
      // hence we are decrementing the same set.
      if (!tip.isJobCleanupTask() && !tip.isJobSetupTask()) {
        if (tip.isMapTask()) {
          runningMapTasks -= 1;
          metrics.failedMap(taskid);
        } else {
          runningReduceTasks -= 1;
          metrics.failedReduce(taskid);
        }
      }
      
      // Metering
      meterTaskAttempt(tip, status);
    }
        
    //update running  count on task failure.
    if (wasRunning && !isRunning) {
      if (tip.isJobCleanupTask()) {
        launchedCleanup = false;
      } else if (tip.isJobSetupTask()) {
        launchedSetup = false;
      } else if (tip.isMapTask()) {
        // remove from the running queue and put it in the non-running cache
        // if the tip is not complete i.e if the tip still needs to be run
        if (!isComplete) {
          retireMap(tip);
          failMap(tip);
        }
      } else {
        // remove from the running queue and put in the failed queue if the tip
        // is not complete
        if (!isComplete) {
          retireReduce(tip);
          failReduce(tip);
        }
      }
    }
        
    // The case when the map was complete but the task tracker went down.
    // However, we don't need to do any metering here...
    if (wasComplete && !isComplete) {
      if (tip.isMapTask()) {
        // Put the task back in the cache. This will help locality for cases
        // where we have a different TaskTracker from the same rack/switch
        // asking for a task. 
        // We bother about only those TIPs that were successful
        // earlier (wasComplete and !isComplete) 
        // (since they might have been removed from the cache of other 
        // racks/switches, if the input split blocks were present there too)
        failMap(tip);
        finishedMapTasks -= 1;
      }
    }
        
    // update job history
    // get taskStatus from tip
    TaskStatus taskStatus = tip.getTaskStatus(taskid);
    String taskTrackerName = taskStatus.getTaskTracker();
    String taskTrackerHostName = convertTrackerNameToHostName(taskTrackerName);
    int taskTrackerPort = -1;
    TaskTrackerStatus taskTrackerStatus = 
      (taskTracker == null) ? null : taskTracker.getStatus();
    if (taskTrackerStatus != null) {
      taskTrackerPort = taskTrackerStatus.getHttpPort();
    }
    long startTime = taskStatus.getStartTime();
    long finishTime = taskStatus.getFinishTime();
    List<String> taskDiagnosticInfo = tip.getDiagnosticInfo(taskid);
    String diagInfo = taskDiagnosticInfo == null ? "" :
      StringUtils.arrayToString(taskDiagnosticInfo.toArray(new String[0]));
    String taskType = getTaskType(tip);
    if (taskStatus.getIsMap()) {
      JobHistory.MapAttempt.logStarted(taskid, startTime, 
        taskTrackerName, taskTrackerPort, taskType);
      if (taskStatus.getRunState() == TaskStatus.State.FAILED) {
        JobHistory.MapAttempt.logFailed(taskid, finishTime,
          taskTrackerHostName, diagInfo, taskType);
      } else {
        JobHistory.MapAttempt.logKilled(taskid, finishTime,
          taskTrackerHostName, diagInfo, taskType);
      }
    } else {
      JobHistory.ReduceAttempt.logStarted(taskid, startTime, 
        taskTrackerName, taskTrackerPort, taskType);
      if (taskStatus.getRunState() == TaskStatus.State.FAILED) {
        JobHistory.ReduceAttempt.logFailed(taskid, finishTime,
          taskTrackerHostName, diagInfo, taskType);
      } else {
        JobHistory.ReduceAttempt.logKilled(taskid, finishTime,
          taskTrackerHostName, diagInfo, taskType);
      }
    }
        
    // After this, try to assign tasks with the one after this, so that
    // the failed task goes to the end of the list.
    if (!tip.isJobCleanupTask() && !tip.isJobSetupTask()) {
      if (tip.isMapTask()) {
        failedMapTasks++;
      } else {
        failedReduceTasks++; 
      }
    }
            
    //
    // Note down that a task has failed on this tasktracker 
    //
    if (status.getRunState() == TaskStatus.State.FAILED) { 
      addTrackerTaskFailure(taskTrackerName, taskTracker);
    }
        
    //
    // Let the JobTracker know that this task has failed
    //
    jobtracker.markCompletedTaskAttempt(status.getTaskTracker(), taskid);

    //
    // Check if we need to kill the job because of too many failures or 
    // if the job is complete since all component tasks have completed

    // We do it once per TIP and that too for the task that fails the TIP
    if (!wasFailed && tip.isFailed()) {
      //
      // Allow upto 'mapFailuresPercent' of map tasks to fail or
      // 'reduceFailuresPercent' of reduce tasks to fail
      //
      boolean killJob = tip.isJobCleanupTask() || tip.isJobSetupTask() ? true :
                        tip.isMapTask() ? 
            ((++failedMapTIPs*100) > (mapFailuresPercent*numMapTasks)) :
            ((++failedReduceTIPs*100) > (reduceFailuresPercent*numReduceTasks));
      
      if (killJob) {
        LOG.info("Aborting job " + profile.getJobID());
        JobHistory.Task.logFailed(tip.getTIPId(), 
                                  taskType,  
                                  finishTime, 
                                  diagInfo);
        if (tip.isJobCleanupTask()) {
          // kill the other tip
          if (tip.isMapTask()) {
            cleanup[1].kill();
          } else {
            cleanup[0].kill();
          }
          terminateJob(JobStatus.FAILED);
        } else {
          if (tip.isJobSetupTask()) {
            // kill the other tip
            killSetupTip(!tip.isMapTask());
          }
          fail();
        }
      }
      
      //
      // Update the counters
      //
      if (!tip.isJobCleanupTask() && !tip.isJobSetupTask()) {
        if (tip.isMapTask()) {
          jobCounters.incrCounter(Counter.NUM_FAILED_MAPS, 1);
        } else {
          jobCounters.incrCounter(Counter.NUM_FAILED_REDUCES, 1);
        }
      }
    }
  }

  void killSetupTip(boolean isMap) {
    if (isMap) {
      setup[0].kill();
    } else {
      setup[1].kill();
    }
  }

  boolean isSetupFinished() {
    if (setup[0].isComplete() || setup[0].isFailed() || setup[1].isComplete()
        || setup[1].isFailed()) {
      return true;
    }
    return false;
  }

  /**
   * Fail a task with a given reason, but without a status object.
   * 
   * Assuming {@link JobTracker} is locked on entry.
   * 
   * @param tip The task's tip
   * @param taskid The task id
   * @param reason The reason that the task failed
   * @param trackerName The task tracker the task failed on
   */
  public void failedTask(TaskInProgress tip, TaskAttemptID taskid, String reason, 
                         TaskStatus.Phase phase, TaskStatus.State state, 
                         String trackerName) {
    TaskStatus status = TaskStatus.createTaskStatus(tip.isMapTask(), 
                                                    taskid,
                                                    0.0f,
                                                    tip.isMapTask() ? 
                                                        numSlotsPerMap : 
                                                        numSlotsPerReduce,
                                                    state,
                                                    reason,
                                                    reason,
                                                    trackerName, phase,
                                                    new Counters());
    // update the actual start-time of the attempt
    TaskStatus oldStatus = tip.getTaskStatus(taskid); 
    long startTime = oldStatus == null
                     ? jobtracker.getClock().getTime()
                     : oldStatus.getStartTime();
    status.setStartTime(startTime);
    status.setFinishTime(jobtracker.getClock().getTime());
    boolean wasComplete = tip.isComplete();
    updateTaskStatus(tip, status);
    boolean isComplete = tip.isComplete();
    if (wasComplete && !isComplete) { // mark a successful tip as failed
      String taskType = getTaskType(tip);
      JobHistory.Task.logFailed(tip.getTIPId(), taskType, 
                                tip.getExecFinishTime(), reason, taskid);
    }
  }
       
                           
  /**
   * The job is dead.  We're now GC'ing it, getting rid of the job
   * from all tables.  Be sure to remove all of this job's tasks
   * from the various tables.
   */
  void garbageCollect() {
    synchronized(this) {
      // Cancel task tracker reservation
      cancelReservedSlots();

      // Let the JobTracker know that a job is complete
      jobtracker.getInstrumentation().decWaitingMaps(getJobID(), pendingMaps());
      jobtracker.getInstrumentation().decWaitingReduces(getJobID(), pendingReduces());
      jobtracker.storeCompletedJob(this);
      jobtracker.finalizeJob(this);

      try {
        // Definitely remove the local-disk copy of the job file
        if (localJobFile != null) {
          localFs.delete(localJobFile, true);
          localJobFile = null;
        }

        Path tempDir = jobtracker.getSystemDirectoryForJob(getJobID());
        CleanupQueue.getInstance().addToQueue(
            new PathDeletionContext(tempDir, conf)); 
      } catch (IOException e) {
        LOG.warn("Error cleaning up "+profile.getJobID()+": "+e);
      }

      cleanUpMetrics();
      // free up the memory used by the data structures
      this.failedMaps.clear();
      this.nonRunningMapCache = null;
      this.runningMapCache = null;
      this.nonRunningReduces = null;
      this.runningReduces = null;
    }
    
    // remove jobs delegation tokens
    if(conf.getBoolean(JobContext.JOB_CANCEL_DELEGATION_TOKEN, true)) {
      DelegationTokenRenewal.removeDelegationTokenRenewalForJob(jobId);
    } // else don't remove it.May be used by spawned tasks

    //close the user's FS
    try {
      FileSystem.closeAllForUGI(userUGI);
    } catch (IOException ie) {
      LOG.warn("Ignoring exception " + StringUtils.stringifyException(ie) + 
          " while closing FileSystem for " + userUGI);
    }
  }

  /**
   * Return the TaskInProgress that matches the tipid.
   */
  public synchronized TaskInProgress getTaskInProgress(TaskID tipid) {
    if (tipid.isMap()) {
      if (tipid.equals(cleanup[0].getTIPId())) { // cleanup map tip
        return cleanup[0]; 
      }
      if (tipid.equals(setup[0].getTIPId())) { //setup map tip
        return setup[0];
      }
      for (int i = 0; i < maps.length; i++) {
        if (tipid.equals(maps[i].getTIPId())){
          return maps[i];
        }
      }
    } else {
      if (tipid.equals(cleanup[1].getTIPId())) { // cleanup reduce tip
        return cleanup[1]; 
      }
      if (tipid.equals(setup[1].getTIPId())) { //setup reduce tip
        return setup[1];
      }
      for (int i = 0; i < reduces.length; i++) {
        if (tipid.equals(reduces[i].getTIPId())){
          return reduces[i];
        }
      }
    }
    return null;
  }
    
  /**
   * Find the details of someplace where a map has finished
   * @param mapId the id of the map
   * @return the task status of the completed task
   */
  public synchronized TaskStatus findFinishedMap(int mapId) {
    TaskInProgress tip = maps[mapId];
    if (tip.isComplete()) {
      TaskStatus[] statuses = tip.getTaskStatuses();
      for(int i=0; i < statuses.length; i++) {
        if (statuses[i].getRunState() == TaskStatus.State.SUCCEEDED) {
          return statuses[i];
        }
      }
    }
    return null;
  }
  
  synchronized int getNumTaskCompletionEvents() {
    return taskCompletionEvents.size();
  }
    
  synchronized public TaskCompletionEvent[] getTaskCompletionEvents(
                                                                    int fromEventId, int maxEvents) {
    TaskCompletionEvent[] events = TaskCompletionEvent.EMPTY_ARRAY;
    if (taskCompletionEvents.size() > fromEventId) {
      int actualMax = Math.min(maxEvents, 
                               (taskCompletionEvents.size() - fromEventId));
      events = taskCompletionEvents.subList(fromEventId, actualMax + fromEventId).toArray(events);        
    }
    return events; 
  }
  
  synchronized void fetchFailureNotification(TaskInProgress tip, 
                                             TaskAttemptID mapTaskId, 
                                             String mapTrackerName,
                                             TaskAttemptID reduceTaskId,
                                             String reduceTrackerName) {
    Integer fetchFailures = mapTaskIdToFetchFailuresMap.get(mapTaskId);
    fetchFailures = (fetchFailures == null) ? 1 : (fetchFailures+1);
    mapTaskIdToFetchFailuresMap.put(mapTaskId, fetchFailures);
    LOG.info("Failed fetch notification #" + fetchFailures + " for map task: "
             + mapTaskId + " running on tracker: " + mapTrackerName
             + " and reduce task: " + reduceTaskId + " running on tracker: "
             + reduceTrackerName);

    float failureRate = (float)fetchFailures / runningReduceTasks;
    // declare faulty if fetch-failures >= max-allowed-failures
    boolean isMapFaulty = failureRate >= MAX_ALLOWED_FETCH_FAILURES_PERCENT;
    if (fetchFailures >= MAX_FETCH_FAILURES_NOTIFICATIONS
        && isMapFaulty) {
      LOG.info("Too many fetch-failures for output of task: " + mapTaskId 
               + " ... killing it");
      
      failedTask(tip, mapTaskId, "Too many fetch-failures",                            
                 (tip.isMapTask() ? TaskStatus.Phase.MAP : 
                                    TaskStatus.Phase.REDUCE), 
                 TaskStatus.State.FAILED, mapTrackerName);
      
      mapTaskIdToFetchFailuresMap.remove(mapTaskId);
    }
  }
  
  /**
   * @return The JobID of this JobInProgress.
   */
  public JobID getJobID() {
    return jobId;
  }
  
  /**
   * @return submitHostName  of this JobInProgress.
   */
  public String getJobSubmitHostName() {
    return this.submitHostName;
  }
  
  /**
   * @return submitHostAddress  of this JobInProgress.
   */
  public String getJobSubmitHostAddress() {
    return this.submitHostAddress;
  }

  public synchronized Object getSchedulingInfo() {
    return this.schedulingInfo;
  }
  
  public synchronized void setSchedulingInfo(Object schedulingInfo) {
    this.schedulingInfo = schedulingInfo;
    this.status.setSchedulingInfo(schedulingInfo.toString());
  }
  
  /**
   * To keep track of kill and initTasks status of this job. initTasks() take 
   * a lock on JobInProgress object. kill should avoid waiting on 
   * JobInProgress lock since it may take a while to do initTasks().
   */
  private static class JobInitKillStatus {
    //flag to be set if kill is called
    boolean killed;
    
    boolean initStarted;
    boolean initDone;
  }

  boolean isComplete() {
    return status.isJobComplete();
  }
  
  /**
   * Get the task type for logging it to {@link JobHistory}.
   */
  private String getTaskType(TaskInProgress tip) {
    if (tip.isJobCleanupTask()) {
      return Values.CLEANUP.name();
    } else if (tip.isJobSetupTask()) {
      return Values.SETUP.name();
    } else if (tip.isMapTask()) {
      return Values.MAP.name();
    } else {
      return Values.REDUCE.name();
    }
  }

  /*
  * Get the level of locality that a given task would have if launched on
  * a particular TaskTracker. Returns 0 if the task has data on that machine,
  * 1 if it has data on the same rack, etc (depending on number of levels in
  * the network hierarchy).
  */
 int getLocalityLevel(TaskInProgress tip, TaskTrackerStatus tts) {
   Node tracker = jobtracker.getNode(tts.getHost());
   int level = this.maxLevel;
   // find the right level across split locations
   for (String local : maps[tip.getIdWithinJob()].getSplitLocations()) {
     Node datanode = jobtracker.getNode(local);
     int newLevel = this.maxLevel;
     if (tracker != null && datanode != null) {
       newLevel = getMatchingLevelForNodes(tracker, datanode);
     }
     if (newLevel < level) {
       level = newLevel;
       // an optimization
       if (level == 0) {
         break;
       }
     }
   }
   return level;
 }
  
  /**
   * Test method to set the cluster sizes
   */
  void setClusterSize(int clusterSize) {
    this.clusterSize = clusterSize;
  }

  static class JobSummary {
    static final Log LOG = LogFactory.getLog(JobSummary.class);
    
    // Escape sequences 
    static final char EQUALS = '=';
    static final char[] charsToEscape = 
      {StringUtils.COMMA, EQUALS, StringUtils.ESCAPE_CHAR};
    
    static class SummaryBuilder {
      final StringBuilder buffer = new StringBuilder();

      // A little optimization for a very common case
      SummaryBuilder add(String key, long value) {
        return _add(key, Long.toString(value));
      }

      <T> SummaryBuilder add(String key, T value) {
        return _add(key, StringUtils.escapeString(String.valueOf(value),
                    StringUtils.ESCAPE_CHAR, charsToEscape));
      }

      SummaryBuilder add(SummaryBuilder summary) {
        if (buffer.length() > 0) buffer.append(StringUtils.COMMA);
        buffer.append(summary.buffer);
        return this;
      }

      SummaryBuilder _add(String key, String value) {
        if (buffer.length() > 0) buffer.append(StringUtils.COMMA);
        buffer.append(key).append(EQUALS).append(value);
        return this;
      }

      @Override public String toString() {
        return buffer.toString();
      }
    }

    static SummaryBuilder getTaskLaunchTimesSummary(JobInProgress job) {
      SummaryBuilder summary = new SummaryBuilder();
      Map<TaskType, Long> timeMap = job.getFirstTaskLaunchTimes();

      synchronized(timeMap) {
        for (Map.Entry<TaskType, Long> e : timeMap.entrySet()) {
          summary.add("first"+ StringUtils.camelize(e.getKey().name()) +
                      "TaskLaunchTime", e.getValue().longValue());
        }
      }
      return summary;
    }

    /**
     * Log a summary of the job's runtime.
     * 
     * @param job {@link JobInProgress} whose summary is to be logged, cannot
     *            be <code>null</code>.
     * @param cluster {@link ClusterStatus} of the cluster on which the job was
     *                run, cannot be <code>null</code>
     */
    public static void logJobSummary(JobInProgress job, ClusterStatus cluster) {
      JobStatus status = job.getStatus();
      JobProfile profile = job.getProfile();
      Counters jobCounters = job.getJobCounters();
      long mapSlotSeconds = 
        (jobCounters.getCounter(Counter.SLOTS_MILLIS_MAPS) +
         jobCounters.getCounter(Counter.FALLOW_SLOTS_MILLIS_MAPS)) / 1000;
      long reduceSlotSeconds = 
        (jobCounters.getCounter(Counter.SLOTS_MILLIS_REDUCES) +
         jobCounters.getCounter(Counter.FALLOW_SLOTS_MILLIS_REDUCES)) / 1000;

      SummaryBuilder summary = new SummaryBuilder()
          .add("jobId", job.getJobID())
          .add("submitTime", job.getStartTime())
          .add("launchTime", job.getLaunchTime())
          .add(getTaskLaunchTimesSummary(job))
          .add("finishTime", job.getFinishTime())
          .add("numMaps", job.getTasks(TaskType.MAP).length)
          .add("numSlotsPerMap", job.getNumSlotsPerMap())
          .add("numReduces", job.getTasks(TaskType.REDUCE).length)
          .add("numSlotsPerReduce", job.getNumSlotsPerReduce())
          .add("user", profile.getUser())
          .add("queue", profile.getQueueName())
          .add("status", JobStatus.getJobRunState(status.getRunState()))
          .add("mapSlotSeconds", mapSlotSeconds)
          .add("reduceSlotsSeconds", reduceSlotSeconds)
          .add("clusterMapCapacity", cluster.getMaxMapTasks())
          .add("clusterReduceCapacity", cluster.getMaxReduceTasks());

      LOG.info(summary);
    }
  }

  /**
   * generate job token and save it into the file
   * @throws IOException
   */
  private void generateAndStoreTokens() throws IOException {
    Path jobDir = jobtracker.getSystemDirectoryForJob(jobId);
    Path keysFile = new Path(jobDir, TokenCache.JOB_TOKEN_HDFS_FILE);
    if (tokenStorage == null) {
      tokenStorage = new Credentials();
    }
    //create JobToken file and write token to it
    JobTokenIdentifier identifier = new JobTokenIdentifier(new Text(jobId
        .toString()));
    Token<JobTokenIdentifier> token = new Token<JobTokenIdentifier>(identifier,
        jobtracker.getJobTokenSecretManager());
    token.setService(identifier.getJobId());
    
    TokenCache.setJobToken(token, tokenStorage);
        
    // write TokenStorage out
    tokenStorage.writeTokenStorageFile(keysFile, jobtracker.getConf());
    LOG.info("jobToken generated and stored with users keys in "
        + keysFile.toUri().getPath());
  }
}
