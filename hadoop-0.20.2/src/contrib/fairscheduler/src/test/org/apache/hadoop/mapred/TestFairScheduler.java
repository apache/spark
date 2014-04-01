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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.net.Node;

import org.apache.hadoop.mapred.FairScheduler.JobInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapred.UtilsForTests.FakeClock;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.spi.NoEmitMetricsContext;
import org.apache.hadoop.metrics.spi.OutputRecord;

public class TestFairScheduler extends TestCase {
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/streaming/test/data")).getAbsolutePath();
  final static String ALLOC_FILE = new File(TEST_DIR, 
      "test-pools").getAbsolutePath();
  
  private static final String POOL_PROPERTY = "pool";
  private static final String EXPLICIT_POOL_PROPERTY = "mapred.fairscheduler.pool";
  
  private static int jobCounter;
  
  class FakeJobInProgress extends JobInProgress {
    
    private FakeTaskTrackerManager taskTrackerManager;
    private int mapCounter = 0;
    private int reduceCounter = 0;
    private final String[][] mapInputLocations; // Array of hosts for each map
    
    public FakeJobInProgress(JobConf jobConf,
        FakeTaskTrackerManager taskTrackerManager, 
        String[][] mapInputLocations,
        JobTracker jt) throws IOException {
      super(new JobID("test", ++jobCounter), jobConf, jt);
      this.taskTrackerManager = taskTrackerManager;
      this.mapInputLocations = mapInputLocations;
      this.startTime = System.currentTimeMillis();
      this.status = new JobStatus();
      this.status.setRunState(JobStatus.PREP);
      this.runningMapCache = new IdentityHashMap<Node, Set<TaskInProgress>>();
      initTasks();
    }
    
    @Override
    public synchronized void initTasks() throws IOException {
      // initTasks is needed to create non-empty cleanup and setup TIP
      // arrays, otherwise calls such as job.getTaskInProgress will fail
      JobID jobId = getJobID();
      JobConf conf = getJobConf();
      String jobFile = "";
      // create two cleanup tips, one map and one reduce.
      cleanup = new TaskInProgress[2];
      // cleanup map tip.
      cleanup[0] = new TaskInProgress(jobId, jobFile, null, 
              jobtracker, conf, this, numMapTasks, 1);
      cleanup[0].setJobCleanupTask();
      // cleanup reduce tip.
      cleanup[1] = new TaskInProgress(jobId, jobFile, numMapTasks,
                         numReduceTasks, jobtracker, conf, this, 1);
      cleanup[1].setJobCleanupTask();
      // create two setup tips, one map and one reduce.
      setup = new TaskInProgress[2];
      // setup map tip.
      setup[0] = new TaskInProgress(jobId, jobFile, null, 
              jobtracker, conf, this, numMapTasks + 1, 1);
      setup[0].setJobSetupTask();
      // setup reduce tip.
      setup[1] = new TaskInProgress(jobId, jobFile, numMapTasks,
                         numReduceTasks + 1, jobtracker, conf, this, 1);
      setup[1].setJobSetupTask();
      // create maps
      numMapTasks = conf.getNumMapTasks();
      maps = new TaskInProgress[numMapTasks];
      JobSplit.TaskSplitMetaInfo split = JobSplit.EMPTY_TASK_SPLIT;
      for (int i = 0; i < numMapTasks; i++) {
        String[] inputLocations = null;
        if (mapInputLocations != null)
          inputLocations = mapInputLocations[i];
        maps[i] = new FakeTaskInProgress(getJobID(), i,
            getJobConf(), this, inputLocations, split);
        if (mapInputLocations == null) // Job has no locality info
          nonLocalMaps.add(maps[i]);
      }
      // create reduces
      numReduceTasks = conf.getNumReduceTasks();
      reduces = new TaskInProgress[numReduceTasks];
      for (int i = 0; i < numReduceTasks; i++) {
        reduces[i] = new FakeTaskInProgress(getJobID(), i,
            getJobConf(), this);
      }
   }

    @Override
    public Task obtainNewMapTask(final TaskTrackerStatus tts, int clusterSize,
        int numUniqueHosts, int localityLevel) throws IOException {
      for (int map = 0; map < maps.length; map++) {
        FakeTaskInProgress tip = (FakeTaskInProgress) maps[map];
        if (!tip.isRunning() && !tip.isComplete() &&
            getLocalityLevel(tip, tts) < localityLevel) {
          TaskAttemptID attemptId = getTaskAttemptID(tip);
          JobSplit.TaskSplitMetaInfo split = JobSplit.EMPTY_TASK_SPLIT;
          Task task = new MapTask("", attemptId, 0, split.getSplitIndex(), 1) {
            @Override
            public String toString() {
              return String.format("%s on %s", getTaskID(), tts.getTrackerName());
            }
          };
          runningMapTasks++;
          tip.createTaskAttempt(task, tts.getTrackerName());
          nonLocalRunningMaps.add(tip);
          taskTrackerManager.startTask(tts.getTrackerName(), task, tip);
          return task;
        }
      }
      return null;
    }
    
    @Override
    public Task obtainNewReduceTask(final TaskTrackerStatus tts,
        int clusterSize, int ignored) throws IOException {
      for (int reduce = 0; reduce < reduces.length; reduce++) {
        FakeTaskInProgress tip = 
          (FakeTaskInProgress) reduces[reduce];
        if (!tip.isRunning() && !tip.isComplete()) {
          TaskAttemptID attemptId = getTaskAttemptID(tip);
          Task task = new ReduceTask("", attemptId, 0, maps.length, 1) {
            @Override
            public String toString() {
              return String.format("%s on %s", getTaskID(), tts.getTrackerName());
            }
          };
          runningReduceTasks++;
          tip.createTaskAttempt(task, tts.getTrackerName());
          runningReduces.add(tip);
          taskTrackerManager.startTask(tts.getTrackerName(), task, tip);
          return task;
        }
      }
      return null;
    }

    public void mapTaskFinished(TaskInProgress tip) {
      runningMapTasks--;
      finishedMapTasks++;
      nonLocalRunningMaps.remove(tip);
    }
    
    public void reduceTaskFinished(TaskInProgress tip) {
      runningReduceTasks--;
      finishedReduceTasks++;
      runningReduces.remove(tip);
    }    

    private TaskAttemptID getTaskAttemptID(TaskInProgress tip) {
      JobID jobId = getJobID();
      return new TaskAttemptID(jobId.getJtIdentifier(),
          jobId.getId(), tip.isMapTask(), tip.getIdWithinJob(), tip.nextTaskId++);
    }

    @Override
    int getLocalityLevel(TaskInProgress tip, TaskTrackerStatus tts) {
      FakeTaskInProgress ftip = (FakeTaskInProgress) tip;
      if (ftip.inputLocations != null) {
        // Check whether we're on the same host as an input split
        for (String location: ftip.inputLocations) {
          if (location.equals(tts.host)) {
            return 0;
          }
        }
        // Check whether we're on the same rack as an input split
        for (String location: ftip.inputLocations) {
          if (getRack(location).equals(getRack(tts.host))) {
            return 1;
          }
        }
        // Not on same rack or host
        return 2;
      } else {
        // Job has no locality info
        return -1;
      }
    }    
  }

  class FakeTaskInProgress extends TaskInProgress {
    private boolean isMap;
    private FakeJobInProgress fakeJob;
    private TreeMap<TaskAttemptID, String> activeTasks;
    private TaskStatus taskStatus;
    private boolean isComplete = false;
    private String[] inputLocations;
     
    // Constructor for map
    FakeTaskInProgress(JobID jId, int id, JobConf jobConf,
        FakeJobInProgress job, String[] inputLocations,
        JobSplit.TaskSplitMetaInfo split) {
      super(jId, "", split, job.jobtracker, jobConf, job, id, 1);
      this.isMap = true;
      this.fakeJob = job;
      this.inputLocations = inputLocations;
      activeTasks = new TreeMap<TaskAttemptID, String>();
      taskStatus = TaskStatus.createTaskStatus(isMap);
      taskStatus.setRunState(TaskStatus.State.UNASSIGNED);
    }

    // Constructor for reduce
    FakeTaskInProgress(JobID jId, int id, JobConf jobConf,
                       FakeJobInProgress job) {
      super(jId, "", jobConf.getNumMapTasks(), id, job.jobtracker, jobConf, job, 1);
      this.isMap = false;
      this.fakeJob = job;
      activeTasks = new TreeMap<TaskAttemptID, String>();
      taskStatus = TaskStatus.createTaskStatus(isMap);
      taskStatus.setRunState(TaskStatus.State.UNASSIGNED);
    }

    private void createTaskAttempt(Task task, String taskTracker) {
      activeTasks.put(task.getTaskID(), taskTracker);
      taskStatus = TaskStatus.createTaskStatus(isMap, task.getTaskID(),
                                               0.5f, 1, TaskStatus.State.RUNNING, "", "", "", 
                                               TaskStatus.Phase.STARTING, new Counters());
      taskStatus.setStartTime(clock.getTime());
    }
    
    @Override
    TreeMap<TaskAttemptID, String> getActiveTasks() {
      return activeTasks;
    }
    
    public synchronized boolean isComplete() {
      return isComplete;
    }
    
    public boolean isRunning() {
      return activeTasks.size() > 0;
    }
    
    @Override
    public TaskStatus getTaskStatus(TaskAttemptID taskid) {
      return taskStatus;
    }
    
    void killAttempt() {
      if (isMap) {
        fakeJob.mapTaskFinished(this);
      }
      else {
        fakeJob.reduceTaskFinished(this);
      }
      activeTasks.clear();
      taskStatus.setRunState(TaskStatus.State.UNASSIGNED);
    }
    
    void finishAttempt() {
      isComplete = true;
      if (isMap) {
        fakeJob.mapTaskFinished(this);
      }
      else {
        fakeJob.reduceTaskFinished(this);
      }
      activeTasks.clear();
      taskStatus.setRunState(TaskStatus.State.UNASSIGNED);
    }
  }
  
  static class FakeQueueManager extends QueueManager {
    private Set<String> queues = null;
    FakeQueueManager() {
      super(new Configuration());
    }
    void setQueues(Set<String> queues) {
      this.queues = queues;
    }
    public synchronized Set<String> getQueues() {
      return queues;
    }
  }
  
  static class FakeTaskTrackerManager implements TaskTrackerManager {
    int maps = 0;
    int reduces = 0;
    int maxMapTasksPerTracker = 2;
    int maxReduceTasksPerTracker = 2;
    List<JobInProgressListener> listeners =
      new ArrayList<JobInProgressListener>();
    Map<JobID, JobInProgress> jobs = new HashMap<JobID, JobInProgress>();
    
    private Map<String, TaskTracker> trackers =
      new HashMap<String, TaskTracker>();
    private Map<String, TaskStatus> statuses = 
      new HashMap<String, TaskStatus>();
    private Map<String, FakeTaskInProgress> tips = 
      new HashMap<String, FakeTaskInProgress>();
    private Map<String, TaskTrackerStatus> trackerForTip =
      new HashMap<String, TaskTrackerStatus>();


   public FakeTaskTrackerManager(int numRacks, int numTrackersPerRack) {
     int nextTrackerId = 1;
     for (int rack = 1; rack <= numRacks; rack++) {
       for (int node = 1; node <= numTrackersPerRack; node++) {
         int id = nextTrackerId++;
         String host = "rack" + rack + ".node" + node;
         System.out.println("Creating TaskTracker tt" + id + " on " + host);
         TaskTracker tt = new TaskTracker("tt" + id);
         tt.setStatus(new TaskTrackerStatus("tt" + id, host, 0,
             new ArrayList<TaskStatus>(), 0, 0,
             maxMapTasksPerTracker, maxReduceTasksPerTracker));
         trackers.put("tt" + id, tt);
       }
     }
   }

    public FakeTaskTrackerManager() {
      TaskTracker tt1 = new TaskTracker("tt1");
      tt1.setStatus(new TaskTrackerStatus("tt1", "tt1.host", 1,
                                          new ArrayList<TaskStatus>(), 0, 0,
                                          maxMapTasksPerTracker, 
                                          maxReduceTasksPerTracker));
      trackers.put("tt1", tt1);
      
      TaskTracker tt2 = new TaskTracker("tt2");
      tt2.setStatus(new TaskTrackerStatus("tt2", "tt2.host", 2,
                                          new ArrayList<TaskStatus>(), 0, 0,
                                          maxMapTasksPerTracker, 
                                          maxReduceTasksPerTracker));
      trackers.put("tt2", tt2);

    }
    
    @Override
    public ClusterStatus getClusterStatus() {
      int numTrackers = trackers.size();
      return new ClusterStatus(numTrackers, maps, reduces,
          numTrackers * maxMapTasksPerTracker,
          numTrackers * maxReduceTasksPerTracker,
          JobTracker.State.RUNNING);
    }

    @Override
    public QueueManager getQueueManager() {
      return null;
    }
    
    @Override
    public int getNumberOfUniqueHosts() {
      return trackers.size();
    }

    @Override
    public Collection<TaskTrackerStatus> taskTrackers() {
      List<TaskTrackerStatus> statuses = new ArrayList<TaskTrackerStatus>();
      for (TaskTracker tt : trackers.values()) {
        statuses.add(tt.getStatus());
      }
      return statuses;
    }


    @Override
    public void addJobInProgressListener(JobInProgressListener listener) {
      listeners.add(listener);
    }

    @Override
    public void removeJobInProgressListener(JobInProgressListener listener) {
      listeners.remove(listener);
    }
    
    @Override
    public int getNextHeartbeatInterval() {
      return MRConstants.HEARTBEAT_INTERVAL_MIN_DEFAULT;
    }

    @Override
    public void killJob(JobID jobid) {
      return;
    }

    @Override
    public JobInProgress getJob(JobID jobid) {
      return jobs.get(jobid);
    }

    public void initJob (JobInProgress job) {
      // do nothing
    }
    
    public void failJob (JobInProgress job) {
      // do nothing
    }
    
    // Test methods
    
    public void submitJob(JobInProgress job) throws IOException {
      jobs.put(job.getJobID(), job);
      for (JobInProgressListener listener : listeners) {
        listener.jobAdded(job);
      }
    }
    
    public TaskTracker getTaskTracker(String trackerID) {
      return trackers.get(trackerID);
    }
    
    public void startTask(String trackerName, Task t, FakeTaskInProgress tip) {
      final boolean isMap = t.isMapTask();
      if (isMap) {
        maps++;
      } else {
        reduces++;
      }
      String attemptId = t.getTaskID().toString();
      TaskStatus status = tip.getTaskStatus(t.getTaskID());
      TaskTrackerStatus trackerStatus = trackers.get(trackerName).getStatus();
      tips.put(attemptId, tip);
      statuses.put(attemptId, status);
      trackerForTip.put(attemptId, trackerStatus);
      status.setRunState(TaskStatus.State.RUNNING);
    }
    
    public void reportTaskOnTracker(String trackerName, Task t) {
      FakeTaskInProgress tip = tips.get(t.getTaskID().toString());
      TaskTrackerStatus trackerStatus = trackers.get(trackerName).getStatus();
      trackerStatus.getTaskReports().add(tip.getTaskStatus(t.getTaskID()));
    }
    
    public void finishTask(String taskTrackerName, String attemptId) {
      FakeTaskInProgress tip = tips.get(attemptId);
      if (tip.isMapTask()) {
        maps--;
      } else {
        reduces--;
      }
      tip.finishAttempt();
      TaskStatus status = statuses.get(attemptId);
      trackers.get(taskTrackerName).getStatus().getTaskReports().remove(status);
    }

    @Override
    public boolean killTask(TaskAttemptID attemptId, boolean shouldFail) {
      String attemptIdStr = attemptId.toString();
      FakeTaskInProgress tip = tips.get(attemptIdStr);
      if (tip.isMapTask()) {
        maps--;
      } else {
        reduces--;
      }
      tip.killAttempt();
      TaskStatus status = statuses.get(attemptIdStr);
      trackerForTip.get(attemptIdStr).getTaskReports().remove(status);
      return true;
    }
  }
  
  protected JobConf conf;
  protected FairScheduler scheduler;
  private FakeTaskTrackerManager taskTrackerManager;
  private FakeClock clock;

  @Override
  protected void setUp() throws Exception {
    jobCounter = 0;
    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    // Create an empty pools file (so we can add/remove pools later)
    FileWriter fileWriter = new FileWriter(ALLOC_FILE);
    fileWriter.write("<?xml version=\"1.0\"?>\n");
    fileWriter.write("<allocations />\n");
    fileWriter.close();
    setUpCluster(1, 2, false);
  }

  public String getRack(String hostname) {
    // Host names are of the form rackN.nodeM, so split at the dot.
    return hostname.split("\\.")[0];
  }

  private void setUpCluster(int numRacks, int numNodesPerRack,
      boolean assignMultiple) throws IOException {
    
    resetMetrics();
    
    conf = new JobConf();
    conf.set("mapred.fairscheduler.allocation.file", ALLOC_FILE);
    conf.set("mapred.fairscheduler.poolnameproperty", POOL_PROPERTY);
    conf.setBoolean("mapred.fairscheduler.assignmultiple", assignMultiple);
    // Manually set locality delay because we aren't using a JobTracker so
    // we can't auto-compute it from the heartbeat interval.
    conf.setLong("mapred.fairscheduler.locality.delay", 10000);
    taskTrackerManager = new FakeTaskTrackerManager(numRacks, numNodesPerRack);
    clock = new FakeClock();
    scheduler = new FairScheduler(clock, true);
    scheduler.waitForMapsBeforeLaunchingReduces = false;
    scheduler.setConf(conf);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    scheduler.start();
  }
  
  /**
   * Set up a metrics context that doesn't emit anywhere but stores the data
   * so we can verify it. Also clears it of any data so that different test
   * cases don't pollute each other.
   */
  private void resetMetrics() throws IOException {
    ContextFactory factory = ContextFactory.getFactory();
    factory.setAttribute("fairscheduler.class",
        NoEmitMetricsContext.class.getName());
    
    MetricsUtil.getContext("fairscheduler").createRecord("jobs").remove();
    MetricsUtil.getContext("fairscheduler").createRecord("pools").remove();
  }

  @Override
  protected void tearDown() throws Exception {
    if (scheduler != null) {
      scheduler.terminate();
    }
  }
  
  private JobInProgress submitJob(int state, int maps, int reduces)
      throws IOException {
    return submitJob(state, maps, reduces, null, null);
  }
  
  private JobInProgress submitJob(int state, int maps, int reduces, String pool)
      throws IOException {
    return submitJob(state, maps, reduces, pool, null);
  }
  
  private JobInProgress submitJob(int state, int maps, int reduces, String pool,
      String[][] mapInputLocations) throws IOException {
    JobConf jobConf = new JobConf(conf);
    jobConf.setNumMapTasks(maps);
    jobConf.setNumReduceTasks(reduces);
    if (pool != null)
      jobConf.set(POOL_PROPERTY, pool);
    JobInProgress job = new FakeJobInProgress(jobConf, taskTrackerManager,
        mapInputLocations,
        UtilsForTests.getJobTracker());
    job.getStatus().setRunState(state);
    taskTrackerManager.submitJob(job);
    job.startTime = clock.time;
    return job;
  }
  
  protected void submitJobs(int number, int state, int maps, int reduces)
    throws IOException {
    for (int i = 0; i < number; i++) {
      submitJob(state, maps, reduces);
    }
  }

  public void testAllocationFileParsing() throws Exception {
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>"); 
    // Give pool A a minimum of 1 map, 2 reduces
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>1</minMaps>");
    out.println("<minReduces>2</minReduces>");
    out.println("</pool>");
    // Give pool B a minimum of 2 maps, 1 reduce
    out.println("<pool name=\"poolB\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>1</minReduces>");
    out.println("</pool>");
    // Give pool C min maps but no min reduces
    out.println("<pool name=\"poolC\">");
    out.println("<minMaps>2</minMaps>");
    out.println("</pool>");
    // Give pool D a limit of 3 running jobs
    out.println("<pool name=\"poolD\">");
    out.println("<maxRunningJobs>3</maxRunningJobs>");
    out.println("</pool>");
    // Give pool E a preemption timeout of one minute
    out.println("<pool name=\"poolE\">");
    out.println("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
    out.println("</pool>");
    // Set default limit of jobs per pool to 15
    out.println("<poolMaxJobsDefault>15</poolMaxJobsDefault>");
    // Set default limit of jobs per user to 5
    out.println("<userMaxJobsDefault>5</userMaxJobsDefault>");
    // Give user1 a limit of 10 jobs
    out.println("<user name=\"user1\">");
    out.println("<maxRunningJobs>10</maxRunningJobs>");
    out.println("</user>");
    // Set default min share preemption timeout to 2 minutes
    out.println("<defaultMinSharePreemptionTimeout>120" 
        + "</defaultMinSharePreemptionTimeout>"); 
    // Set fair share preemption timeout to 5 minutes
    out.println("<fairSharePreemptionTimeout>300</fairSharePreemptionTimeout>"); 
    out.println("</allocations>"); 
    out.close();
    
    PoolManager poolManager = scheduler.getPoolManager();
    poolManager.reloadAllocs();
    
    assertEquals(6, poolManager.getPools().size()); // 5 in file + default pool
    assertEquals(0, poolManager.getAllocation(Pool.DEFAULT_POOL_NAME,
        TaskType.MAP));
    assertEquals(0, poolManager.getAllocation(Pool.DEFAULT_POOL_NAME,
        TaskType.REDUCE));
    assertEquals(1, poolManager.getAllocation("poolA", TaskType.MAP));
    assertEquals(2, poolManager.getAllocation("poolA", TaskType.REDUCE));
    assertEquals(2, poolManager.getAllocation("poolB", TaskType.MAP));
    assertEquals(1, poolManager.getAllocation("poolB", TaskType.REDUCE));
    assertEquals(2, poolManager.getAllocation("poolC", TaskType.MAP));
    assertEquals(0, poolManager.getAllocation("poolC", TaskType.REDUCE));
    assertEquals(0, poolManager.getAllocation("poolD", TaskType.MAP));
    assertEquals(0, poolManager.getAllocation("poolD", TaskType.REDUCE));
    assertEquals(0, poolManager.getAllocation("poolE", TaskType.MAP));
    assertEquals(0, poolManager.getAllocation("poolE", TaskType.REDUCE));
    assertEquals(15, poolManager.getPoolMaxJobs(Pool.DEFAULT_POOL_NAME));
    assertEquals(15, poolManager.getPoolMaxJobs("poolA"));
    assertEquals(15, poolManager.getPoolMaxJobs("poolB"));
    assertEquals(15, poolManager.getPoolMaxJobs("poolC"));
    assertEquals(3, poolManager.getPoolMaxJobs("poolD"));
    assertEquals(10, poolManager.getUserMaxJobs("user1"));
    assertEquals(5, poolManager.getUserMaxJobs("user2"));
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout(
        Pool.DEFAULT_POOL_NAME));
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout("poolA"));
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout("poolB"));
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout("poolC"));
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout("poolD"));
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout("poolA"));
    assertEquals(60000, poolManager.getMinSharePreemptionTimeout("poolE"));
    assertEquals(300000, poolManager.getFairSharePreemptionTimeout());
  }
  
  public void testTaskNotAssignedWhenNoJobsArePresent() throws IOException {
    assertNull(scheduler.assignTasks(tracker("tt1")));
  }

  public void testNonRunningJobsAreIgnored() throws IOException {
    submitJobs(1, JobStatus.PREP, 10, 10);
    submitJobs(1, JobStatus.SUCCEEDED, 10, 10);
    submitJobs(1, JobStatus.FAILED, 10, 10);
    submitJobs(1, JobStatus.KILLED, 10, 10);
    assertNull(scheduler.assignTasks(tracker("tt1")));
    advanceTime(100); // Check that we still don't assign jobs after an update
    assertNull(scheduler.assignTasks(tracker("tt1")));
  }

  /**
   * This test contains two jobs with fewer required tasks than there are slots.
   * We check that all tasks are assigned, but job 1 gets them first because it
   * was submitted earlier.
   */
  public void testSmallJobs() throws IOException {
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 2, 1);
    JobInfo info1 = scheduler.infos.get(job1);
    
    // Check scheduler variables
    assertEquals(0,    info1.mapSchedulable.getRunningTasks());
    assertEquals(0,    info1.reduceSchedulable.getRunningTasks());
    assertEquals(2,    info1.mapSchedulable.getDemand());
    assertEquals(1,    info1.reduceSchedulable.getDemand());
    assertEquals(2.0,  info1.mapSchedulable.getFairShare());
    assertEquals(1.0,  info1.reduceSchedulable.getFairShare());
    verifyMetrics();

    // Advance time before submitting another job j2, to make j1 run before j2
    // deterministically.
    advanceTime(100);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 1, 2);
    JobInfo info2 = scheduler.infos.get(job2);
    
    // Check scheduler variables
    assertEquals(0,    info1.mapSchedulable.getRunningTasks());
    assertEquals(0,    info1.reduceSchedulable.getRunningTasks());
    assertEquals(2,    info1.mapSchedulable.getDemand());
    assertEquals(1,    info1.reduceSchedulable.getDemand());
    assertEquals(2.0,  info1.mapSchedulable.getFairShare());
    assertEquals(1.0,  info1.reduceSchedulable.getFairShare());
    assertEquals(0,    info2.mapSchedulable.getRunningTasks());
    assertEquals(0,    info2.reduceSchedulable.getRunningTasks());
    assertEquals(1,    info2.mapSchedulable.getDemand());
    assertEquals(2,    info2.reduceSchedulable.getDemand());
    assertEquals(1.0,  info2.mapSchedulable.getFairShare());
    assertEquals(2.0,  info2.reduceSchedulable.getFairShare());
    verifyMetrics();
    
    // Assign tasks and check that jobs alternate in filling slots
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
    assertNull(scheduler.assignTasks(tracker("tt2")));
    
    // Check that the scheduler has started counting the tasks as running
    // as soon as it launched them.
    assertEquals(2,  info1.mapSchedulable.getRunningTasks());
    assertEquals(1,  info1.reduceSchedulable.getRunningTasks());
    assertEquals(2,  info1.mapSchedulable.getDemand());
    assertEquals(1,  info1.reduceSchedulable.getDemand());
    assertEquals(1,  info2.mapSchedulable.getRunningTasks());
    assertEquals(2,  info2.reduceSchedulable.getRunningTasks());
    assertEquals(1, info2.mapSchedulable.getDemand());
    assertEquals(2, info2.reduceSchedulable.getDemand());
    verifyMetrics();
  }
  /**
   * This test is identical to testSmallJobs but sets assignMultiple to
   * true so that multiple tasks can be assigned per heartbeat.
   */
  public void testSmallJobsWithAssignMultiple() throws IOException {
    setUpCluster(1, 2, true);
    
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 2, 1);
    JobInfo info1 = scheduler.infos.get(job1);
    
    // Check scheduler variables
    assertEquals(0,    info1.mapSchedulable.getRunningTasks());
    assertEquals(0,    info1.reduceSchedulable.getRunningTasks());
    assertEquals(2,    info1.mapSchedulable.getDemand());
    assertEquals(1,    info1.reduceSchedulable.getDemand());
    assertEquals(2.0,  info1.mapSchedulable.getFairShare());
    assertEquals(1.0,  info1.reduceSchedulable.getFairShare());
    verifyMetrics();
    
    // Advance time before submitting another job j2, to make j1 run before j2
    // deterministically.
    advanceTime(100);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 1, 2);
    JobInfo info2 = scheduler.infos.get(job2);
    
    // Check scheduler variables
    assertEquals(0,    info1.mapSchedulable.getRunningTasks());
    assertEquals(0,    info1.reduceSchedulable.getRunningTasks());
    assertEquals(2,    info1.mapSchedulable.getDemand());
    assertEquals(1,    info1.reduceSchedulable.getDemand());
    assertEquals(2.0,  info1.mapSchedulable.getFairShare());
    assertEquals(1.0,  info1.reduceSchedulable.getFairShare());
    assertEquals(0,    info2.mapSchedulable.getRunningTasks());
    assertEquals(0,    info2.reduceSchedulable.getRunningTasks());
    assertEquals(1,    info2.mapSchedulable.getDemand());
    assertEquals(2,    info2.reduceSchedulable.getDemand());
    assertEquals(1.0,  info2.mapSchedulable.getFairShare());
    assertEquals(2.0,  info2.reduceSchedulable.getFairShare());
    verifyMetrics();
    
    // Assign tasks and check that jobs alternate in filling slots
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1",
                           "attempt_test_0001_r_000000_0 on tt1",
                           "attempt_test_0002_m_000000_0 on tt1",
                           "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000001_0 on tt2",
                           "attempt_test_0002_r_000001_0 on tt2");
    assertNull(scheduler.assignTasks(tracker("tt2")));
    
    // Check that the scheduler has started counting the tasks as running
    // as soon as it launched them.
    assertEquals(2,  info1.mapSchedulable.getRunningTasks());
    assertEquals(1,  info1.reduceSchedulable.getRunningTasks());
    assertEquals(2,  info1.mapSchedulable.getDemand());
    assertEquals(1,  info1.reduceSchedulable.getDemand());
    assertEquals(1,  info2.mapSchedulable.getRunningTasks());
    assertEquals(2,  info2.reduceSchedulable.getRunningTasks());
    assertEquals(1, info2.mapSchedulable.getDemand());
    assertEquals(2, info2.reduceSchedulable.getDemand());
    verifyMetrics();
  }
  
  /**
   * This test begins by submitting two jobs with 10 maps and reduces each.
   * The first job is submitted 100ms after the second, to make it get slots
   * first deterministically. We then assign a wave of tasks and check that
   * they are given alternately to job1, job2, job1, job2, etc. We finish
   * these tasks and assign a second wave, which should continue to be
   * allocated in this manner.
   */
  public void testLargeJobs() throws IOException {
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);
    
    // Check scheduler variables
    assertEquals(0,    info1.mapSchedulable.getRunningTasks());
    assertEquals(0,    info1.reduceSchedulable.getRunningTasks());
    assertEquals(10,   info1.mapSchedulable.getDemand());
    assertEquals(10,   info1.reduceSchedulable.getDemand());
    assertEquals(4.0,  info1.mapSchedulable.getFairShare());
    assertEquals(4.0,  info1.reduceSchedulable.getFairShare());
    
    // Advance time before submitting another job j2, to make j1 run before j2
    // deterministically.
    advanceTime(100);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info2 = scheduler.infos.get(job2);
    
    // Check scheduler variables
    assertEquals(0,    info1.mapSchedulable.getRunningTasks());
    assertEquals(0,    info1.reduceSchedulable.getRunningTasks());
    assertEquals(10,   info1.mapSchedulable.getDemand());
    assertEquals(10,   info1.reduceSchedulable.getDemand());
    assertEquals(2.0,  info1.mapSchedulable.getFairShare());
    assertEquals(2.0,  info1.reduceSchedulable.getFairShare());
    assertEquals(0,    info2.mapSchedulable.getRunningTasks());
    assertEquals(0,    info2.reduceSchedulable.getRunningTasks());
    assertEquals(10,   info2.mapSchedulable.getDemand());
    assertEquals(10,   info2.reduceSchedulable.getDemand());
    assertEquals(2.0,  info2.mapSchedulable.getFairShare());
    assertEquals(2.0,  info2.reduceSchedulable.getFairShare());
    
    // Check that tasks are filled alternately by the jobs
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
    
    // Check that no new tasks can be launched once the tasktrackers are full
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
    
    // Check that the scheduler has started counting the tasks as running
    // as soon as it launched them.
    assertEquals(2,  info1.mapSchedulable.getRunningTasks());
    assertEquals(2,  info1.reduceSchedulable.getRunningTasks());
    assertEquals(10,  info1.mapSchedulable.getDemand());
    assertEquals(10,  info1.reduceSchedulable.getDemand());
    assertEquals(2,  info2.mapSchedulable.getRunningTasks());
    assertEquals(2,  info2.reduceSchedulable.getRunningTasks());
    assertEquals(10, info2.mapSchedulable.getDemand());
    assertEquals(10, info2.reduceSchedulable.getDemand());
    
    // Finish up the tasks and advance time again. Note that we must finish
    // the task since FakeJobInProgress does not properly maintain running
    // tasks, so the scheduler will always get an empty task list from
    // the JobInProgress's getMapTasks/getReduceTasks and think they finished.
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000000_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_m_000000_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_r_000000_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_r_000000_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_m_000001_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_m_000001_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_r_000001_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_r_000001_0");
    advanceTime(200);
    assertEquals(0,   info1.mapSchedulable.getRunningTasks());
    assertEquals(0,   info1.reduceSchedulable.getRunningTasks());
    assertEquals(0,   info2.mapSchedulable.getRunningTasks());
    assertEquals(0,   info2.reduceSchedulable.getRunningTasks());

    // Check that tasks are filled alternately by the jobs
    checkAssignment("tt1", "attempt_test_0001_m_000002_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000002_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_m_000002_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000002_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000003_0 on tt2");
    
    // Check scheduler variables; the demands should now be 8 because 2 tasks
    // of each type have finished in each job
    assertEquals(2,    info1.mapSchedulable.getRunningTasks());
    assertEquals(2,    info1.reduceSchedulable.getRunningTasks());
    assertEquals(8,   info1.mapSchedulable.getDemand());
    assertEquals(8,   info1.reduceSchedulable.getDemand());
    assertEquals(2.0,  info1.mapSchedulable.getFairShare());
    assertEquals(2.0,  info1.reduceSchedulable.getFairShare());
    assertEquals(2,    info2.mapSchedulable.getRunningTasks());
    assertEquals(2,    info2.reduceSchedulable.getRunningTasks());
    assertEquals(8,   info2.mapSchedulable.getDemand());
    assertEquals(8,   info2.reduceSchedulable.getDemand());
    assertEquals(2.0,  info2.mapSchedulable.getFairShare());
    assertEquals(2.0,  info2.reduceSchedulable.getFairShare());
  }
  
  /**
   * A copy of testLargeJobs that enables the assignMultiple feature to launch
   * multiple tasks per heartbeat. Results should be the same as testLargeJobs.
   */
  public void testLargeJobsWithAssignMultiple() throws IOException {
    setUpCluster(1, 2, true);
    
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);
    
    // Check scheduler variables
    assertEquals(0,    info1.mapSchedulable.getRunningTasks());
    assertEquals(0,    info1.reduceSchedulable.getRunningTasks());
    assertEquals(10,   info1.mapSchedulable.getDemand());
    assertEquals(10,   info1.reduceSchedulable.getDemand());
    assertEquals(4.0,  info1.mapSchedulable.getFairShare());
    assertEquals(4.0,  info1.reduceSchedulable.getFairShare());
    
    // Advance time before submitting another job j2, to make j1 run before j2
    // deterministically.
    advanceTime(100);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info2 = scheduler.infos.get(job2);
    
    // Check scheduler variables; the fair shares should now have been allocated
    // equally between j1 and j2, but j1 should have (4 slots)*(100 ms) deficit
    assertEquals(0,    info1.mapSchedulable.getRunningTasks());
    assertEquals(0,    info1.reduceSchedulable.getRunningTasks());
    assertEquals(10,   info1.mapSchedulable.getDemand());
    assertEquals(10,   info1.reduceSchedulable.getDemand());
    assertEquals(2.0,  info1.mapSchedulable.getFairShare());
    assertEquals(2.0,  info1.reduceSchedulable.getFairShare());
    assertEquals(0,    info2.mapSchedulable.getRunningTasks());
    assertEquals(0,    info2.reduceSchedulable.getRunningTasks());
    assertEquals(10,   info2.mapSchedulable.getDemand());
    assertEquals(10,   info2.reduceSchedulable.getDemand());
    assertEquals(2.0,  info2.mapSchedulable.getFairShare());
    assertEquals(2.0,  info2.reduceSchedulable.getFairShare());
    
    // Check that tasks are filled alternately by the jobs
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1",
                           "attempt_test_0001_r_000000_0 on tt1",
                           "attempt_test_0002_m_000000_0 on tt1",
                           "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000001_0 on tt2",
                           "attempt_test_0001_r_000001_0 on tt2",
                           "attempt_test_0002_m_000001_0 on tt2",
                           "attempt_test_0002_r_000001_0 on tt2");
    
    // Check that no new tasks can be launched once the tasktrackers are full
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
    
    // Check that the scheduler has started counting the tasks as running
    // as soon as it launched them.
    assertEquals(2,  info1.mapSchedulable.getRunningTasks());
    assertEquals(2,  info1.reduceSchedulable.getRunningTasks());
    assertEquals(10,  info1.mapSchedulable.getDemand());
    assertEquals(10,  info1.reduceSchedulable.getDemand());
    assertEquals(2,  info2.mapSchedulable.getRunningTasks());
    assertEquals(2,  info2.reduceSchedulable.getRunningTasks());
    assertEquals(10, info2.mapSchedulable.getDemand());
    assertEquals(10, info2.reduceSchedulable.getDemand());
    
    // Finish up the tasks and advance time again. Note that we must finish
    // the task since FakeJobInProgress does not properly maintain running
    // tasks, so the scheduler will always get an empty task list from
    // the JobInProgress's getTasks(TaskType.MAP)/getTasks(TaskType.REDUCE) and 
    // think they finished.
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000000_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_m_000000_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_r_000000_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_r_000000_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_m_000001_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_m_000001_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_r_000001_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_r_000001_0");
    advanceTime(200);
    assertEquals(0,   info1.mapSchedulable.getRunningTasks());
    assertEquals(0,   info1.reduceSchedulable.getRunningTasks());
    assertEquals(0,   info2.mapSchedulable.getRunningTasks());
    assertEquals(0,   info2.reduceSchedulable.getRunningTasks());

    // Check that tasks are filled alternately by the jobs
    checkAssignment("tt1", "attempt_test_0001_m_000002_0 on tt1",
                           "attempt_test_0001_r_000002_0 on tt1",
                           "attempt_test_0002_m_000002_0 on tt1",
                           "attempt_test_0002_r_000002_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2",
                           "attempt_test_0001_r_000003_0 on tt2",
                           "attempt_test_0002_m_000003_0 on tt2",
                           "attempt_test_0002_r_000003_0 on tt2");
    
    // Check scheduler variables; the demands should now be 8 because 2 tasks
    // of each type have finished in each job
    assertEquals(2,    info1.mapSchedulable.getRunningTasks());
    assertEquals(2,    info1.reduceSchedulable.getRunningTasks());
    assertEquals(8,   info1.mapSchedulable.getDemand());
    assertEquals(8,   info1.reduceSchedulable.getDemand());
    assertEquals(2.0,  info1.mapSchedulable.getFairShare());
    assertEquals(2.0,  info1.reduceSchedulable.getFairShare());
    assertEquals(2,    info2.mapSchedulable.getRunningTasks());
    assertEquals(2,    info2.reduceSchedulable.getRunningTasks());
    assertEquals(8,   info2.mapSchedulable.getDemand());
    assertEquals(8,   info2.reduceSchedulable.getDemand());
    assertEquals(2.0,  info2.mapSchedulable.getFairShare());
    assertEquals(2.0,  info2.reduceSchedulable.getFairShare());
  }

  /**
   * We submit two jobs such that one has 2x the priority of the other to 
   * a cluster of 3 nodes, wait for 100 ms, and check that the weights/shares 
   * the high-priority job gets 4 tasks while the normal-priority job gets 2.
   */
  public void testJobsWithPriorities() throws IOException {
    setUpCluster(1, 3, false);
    
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info2 = scheduler.infos.get(job2);
    job2.setPriority(JobPriority.HIGH);
    scheduler.update();
    
    // Check scheduler variables
    assertEquals(0,   info1.mapSchedulable.getRunningTasks());
    assertEquals(0,   info1.reduceSchedulable.getRunningTasks());
    assertEquals(10,  info1.mapSchedulable.getDemand());
    assertEquals(10,  info1.reduceSchedulable.getDemand());
    assertEquals(2.0, info1.mapSchedulable.getFairShare(), 0.1);
    assertEquals(2.0, info1.reduceSchedulable.getFairShare(), 0.1);
    assertEquals(0,   info2.mapSchedulable.getRunningTasks());
    assertEquals(0,   info2.reduceSchedulable.getRunningTasks());
    assertEquals(10,  info2.mapSchedulable.getDemand());
    assertEquals(10,  info2.reduceSchedulable.getDemand());
    assertEquals(4.0, info2.mapSchedulable.getFairShare(), 0.1);
    assertEquals(4.0, info2.reduceSchedulable.getFairShare(), 0.1);
    
    // Advance time
    advanceTime(100);
    
    // Assign tasks and check that j2 gets 2x more tasks than j1. In addition,
    // whenever the jobs' runningTasks/weight ratios are tied, j1 should get
    // the new task first because it started first; thus the tasks of each
    // type should be handed out alternately to 1, 2, 2, 1, 2, 2, etc.
    System.out.println("HEREEEE");
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000001_0 on tt2");
    checkAssignment("tt3", "attempt_test_0002_m_000002_0 on tt3");
    checkAssignment("tt3", "attempt_test_0002_r_000002_0 on tt3");
    checkAssignment("tt3", "attempt_test_0002_m_000003_0 on tt3");
    checkAssignment("tt3", "attempt_test_0002_r_000003_0 on tt3");
  }
  
  /**
   * This test starts by submitting three large jobs:
   * - job1 in the default pool, at time 0
   * - job2 in poolA, with an allocation of 1 map / 2 reduces, at time 200
   * - job3 in poolB, with an allocation of 2 maps / 1 reduce, at time 300
   * 
   * We then assign tasks to all slots. The maps should be assigned in the
   * order job2, job3, job 3, job1 because jobs 3 and 2 have guaranteed slots
   * (1 and 2 respectively). Job2 comes before job3 when they are both at 0
   * slots because it has an earlier start time. In a similar manner,
   * reduces should be assigned as job2, job3, job2, job1.
   */
  public void testLargeJobsWithPools() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a minimum of 1 map, 2 reduces
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>1</minMaps>");
    out.println("<minReduces>2</minReduces>");
    out.println("</pool>");
    // Give pool B a minimum of 2 maps, 1 reduce
    out.println("<pool name=\"poolB\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>1</minReduces>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    Pool defaultPool = scheduler.getPoolManager().getPool("default");
    Pool poolA = scheduler.getPoolManager().getPool("poolA");
    Pool poolB = scheduler.getPoolManager().getPool("poolB");
    
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);
    
    // Check scheduler variables
    assertEquals(0,    info1.mapSchedulable.getRunningTasks());
    assertEquals(0,    info1.reduceSchedulable.getRunningTasks());
    assertEquals(10,   info1.mapSchedulable.getDemand());
    assertEquals(10,   info1.reduceSchedulable.getDemand());
    assertEquals(4.0,  info1.mapSchedulable.getFairShare());
    assertEquals(4.0,  info1.reduceSchedulable.getFairShare());
    
    // Advance time 200ms and submit jobs 2 and 3
    advanceTime(200);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    JobInfo info2 = scheduler.infos.get(job2);
    advanceTime(100);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10, "poolB");
    JobInfo info3 = scheduler.infos.get(job3);
    
    // Check that minimum and fair shares have been allocated
    assertEquals(0,    defaultPool.getMapSchedulable().getMinShare());
    assertEquals(0,    defaultPool.getReduceSchedulable().getMinShare());
    assertEquals(1.0,  info1.mapSchedulable.getFairShare());
    assertEquals(1.0,  info1.reduceSchedulable.getFairShare());
    assertEquals(1,    poolA.getMapSchedulable().getMinShare());
    assertEquals(2,    poolA.getReduceSchedulable().getMinShare());
    assertEquals(1.0,  info2.mapSchedulable.getFairShare());
    assertEquals(2.0,  info2.reduceSchedulable.getFairShare());
    assertEquals(2,    poolB.getMapSchedulable().getMinShare());
    assertEquals(1,    poolB.getReduceSchedulable().getMinShare());
    assertEquals(2.0,  info3.mapSchedulable.getFairShare());
    assertEquals(1.0,  info3.reduceSchedulable.getFairShare());
    
    // Advance time 100ms
    advanceTime(100);
    
    // Assign tasks and check that slots are first given to needy jobs
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0003_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0003_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0003_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000000_0 on tt2");
  }

  /**
   * This test starts by submitting three large jobs:
   * - job1 in the default pool, at time 0
   * - job2 in poolA, with an allocation of 2 maps / 2 reduces, at time 200
   * - job3 in poolA, with an allocation of 2 maps / 2 reduces, at time 300
   * 
   * After this, we start assigning tasks. The first two tasks of each type
   * should be assigned to job2 and job3 since they are in a pool with an
   * allocation guarantee, but the next two slots should be assigned to job 3
   * because the pool will no longer be needy.
   */
  public void testLargeJobsWithExcessCapacity() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a minimum of 2 maps, 2 reduces
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>2</minReduces>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    Pool poolA = scheduler.getPoolManager().getPool("poolA");
    
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);
    
    // Check scheduler variables
    assertEquals(0,    info1.mapSchedulable.getRunningTasks());
    assertEquals(0,    info1.reduceSchedulable.getRunningTasks());
    assertEquals(10,   info1.mapSchedulable.getDemand());
    assertEquals(10,   info1.reduceSchedulable.getDemand());
    assertEquals(4.0,  info1.mapSchedulable.getFairShare());
    assertEquals(4.0,  info1.reduceSchedulable.getFairShare());
    
    // Advance time 200ms and submit job 2
    advanceTime(200);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    JobInfo info2 = scheduler.infos.get(job2);
    
    // Check that minimum and fair shares have been allocated
    assertEquals(2,    poolA.getMapSchedulable().getMinShare());
    assertEquals(2,    poolA.getReduceSchedulable().getMinShare());
    assertEquals(2.0,  info1.mapSchedulable.getFairShare());
    assertEquals(2.0,  info1.reduceSchedulable.getFairShare());
    assertEquals(2.0,  info2.mapSchedulable.getFairShare());
    assertEquals(2.0,  info2.reduceSchedulable.getFairShare());
    
    // Advance time 100ms and submit job 3
    advanceTime(100);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    JobInfo info3 = scheduler.infos.get(job3);
    
    // Check that minimum and fair shares have been allocated
    assertEquals(2,    poolA.getMapSchedulable().getMinShare());
    assertEquals(2,    poolA.getReduceSchedulable().getMinShare());
    assertEquals(2.0,  info1.mapSchedulable.getFairShare());
    assertEquals(2.0,  info1.reduceSchedulable.getFairShare());
    assertEquals(1.0,  info2.mapSchedulable.getFairShare());
    assertEquals(1.0,  info2.reduceSchedulable.getFairShare());
    assertEquals(1.0,  info3.mapSchedulable.getFairShare());
    assertEquals(1.0,  info3.reduceSchedulable.getFairShare());
    
    // Advance time
    advanceTime(100);
    
    // Assign tasks and check that slots are first given to needy jobs, but
    // that job 1 gets two tasks after due to having a larger share.
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0003_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0003_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000001_0 on tt2");
  }
  
  /**
   * A copy of testLargeJobsWithExcessCapacity that enables assigning multiple
   * tasks per heartbeat. Results should match testLargeJobsWithExcessCapacity.
   */
  public void testLargeJobsWithExcessCapacityAndAssignMultiple() 
      throws Exception {
    setUpCluster(1, 2, true);
    
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a minimum of 2 maps, 2 reduces
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>2</minReduces>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    Pool poolA = scheduler.getPoolManager().getPool("poolA");
    
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);
    
    // Check scheduler variables
    assertEquals(0,    info1.mapSchedulable.getRunningTasks());
    assertEquals(0,    info1.reduceSchedulable.getRunningTasks());
    assertEquals(10,   info1.mapSchedulable.getDemand());
    assertEquals(10,   info1.reduceSchedulable.getDemand());
    assertEquals(4.0,  info1.mapSchedulable.getFairShare());
    assertEquals(4.0,  info1.reduceSchedulable.getFairShare());
    
    // Advance time 200ms and submit job 2
    advanceTime(200);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    JobInfo info2 = scheduler.infos.get(job2);
    
    // Check that minimum and fair shares have been allocated
    assertEquals(2,    poolA.getMapSchedulable().getMinShare());
    assertEquals(2,    poolA.getReduceSchedulable().getMinShare());
    assertEquals(2.0,  info1.mapSchedulable.getFairShare());
    assertEquals(2.0,  info1.reduceSchedulable.getFairShare());
    assertEquals(2.0,  info2.mapSchedulable.getFairShare());
    assertEquals(2.0,  info2.reduceSchedulable.getFairShare());
    
    // Advance time 100ms and submit job 3
    advanceTime(100);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    JobInfo info3 = scheduler.infos.get(job3);
    
    // Check that minimum and fair shares have been allocated
    assertEquals(2,    poolA.getMapSchedulable().getMinShare());
    assertEquals(2,    poolA.getReduceSchedulable().getMinShare());
    assertEquals(2.0,  info1.mapSchedulable.getFairShare());
    assertEquals(2.0,  info1.reduceSchedulable.getFairShare());
    assertEquals(1.0,  info2.mapSchedulable.getFairShare());
    assertEquals(1.0,  info2.reduceSchedulable.getFairShare());
    assertEquals(1.0,  info3.mapSchedulable.getFairShare());
    assertEquals(1.0,  info3.reduceSchedulable.getFairShare());
    
    // Advance time
    advanceTime(100);
    
    // Assign tasks and check that slots are first given to needy jobs, but
    // that job 1 gets two tasks after due to having a larger share.
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1",
                           "attempt_test_0002_r_000000_0 on tt1",
                           "attempt_test_0003_m_000000_0 on tt1",
                           "attempt_test_0003_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000000_0 on tt2",
                           "attempt_test_0001_r_000000_0 on tt2",
                           "attempt_test_0001_m_000001_0 on tt2",
                           "attempt_test_0001_r_000001_0 on tt2");
  }
  
  /**
   * This test starts by submitting two jobs at time 0:
   * - job1 in the default pool
   * - job2, with 1 map and 1 reduce, in poolA, which has an alloc of 4
   *   maps and 4 reduces
   * 
   * When we assign the slots, job2 should only get 1 of each type of task.
   */
  public void testSmallJobInLargePool() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a minimum of 4 maps, 4 reduces
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>4</minMaps>");
    out.println("<minReduces>4</minReduces>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 1, 1, "poolA");
    JobInfo info2 = scheduler.infos.get(job2);
    
    // Check scheduler variables
    assertEquals(0,    info1.mapSchedulable.getRunningTasks());
    assertEquals(0,    info1.reduceSchedulable.getRunningTasks());
    assertEquals(10,   info1.mapSchedulable.getDemand());
    assertEquals(10,   info1.reduceSchedulable.getDemand());
    assertEquals(3.0,  info1.mapSchedulable.getFairShare());
    assertEquals(3.0,  info1.reduceSchedulable.getFairShare());
    assertEquals(0,    info2.mapSchedulable.getRunningTasks());
    assertEquals(0,    info2.reduceSchedulable.getRunningTasks());
    assertEquals(1,    info2.mapSchedulable.getDemand());
    assertEquals(1,    info2.reduceSchedulable.getDemand());
    assertEquals(1.0,  info2.mapSchedulable.getFairShare());
    assertEquals(1.0,  info2.reduceSchedulable.getFairShare());
    
    // Assign tasks and check that slots are first given to needy jobs
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
  }
  
  /**
   * This test starts by submitting four jobs in the default pool. However, the
   * maxRunningJobs limit for this pool has been set to two. We should see only
   * the first two jobs get scheduled, each with half the total slots.
   */
  public void testPoolMaxJobs() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"default\">");
    out.println("<maxRunningJobs>2</maxRunningJobs>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    // Submit jobs, advancing time in-between to make sure that they are
    // all submitted at distinct times.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);
    advanceTime(10);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info2 = scheduler.infos.get(job2);
    advanceTime(10);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info3 = scheduler.infos.get(job3);
    advanceTime(10);
    JobInProgress job4 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info4 = scheduler.infos.get(job4);
    
    // Check scheduler variables
    assertEquals(2.0,  info1.mapSchedulable.getFairShare());
    assertEquals(2.0,  info1.reduceSchedulable.getFairShare());
    assertEquals(2.0,  info2.mapSchedulable.getFairShare());
    assertEquals(2.0,  info2.reduceSchedulable.getFairShare());
    assertEquals(0.0,  info3.mapSchedulable.getFairShare());
    assertEquals(0.0,  info3.reduceSchedulable.getFairShare());
    assertEquals(0.0,  info4.mapSchedulable.getFairShare());
    assertEquals(0.0,  info4.reduceSchedulable.getFairShare());
    
    // Assign tasks and check that only jobs 1 and 2 get them
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0001_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
  }

  /**
   * This test starts by submitting two jobs by user "user1" to the default
   * pool, and two jobs by "user2". We set user1's job limit to 1. We should
   * see one job from user1 and two from user2. 
   */
  public void testUserMaxJobs() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<user name=\"user1\">");
    out.println("<maxRunningJobs>1</maxRunningJobs>");
    out.println("</user>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    // Submit jobs, advancing time in-between to make sure that they are
    // all submitted at distinct times.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    job1.getJobConf().set("user.name", "user1");
    JobInfo info1 = scheduler.infos.get(job1);
    advanceTime(10);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10);
    job2.getJobConf().set("user.name", "user1");
    JobInfo info2 = scheduler.infos.get(job2);
    advanceTime(10);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10);
    job3.getJobConf().set("user.name", "user2");
    JobInfo info3 = scheduler.infos.get(job3);
    advanceTime(10);
    JobInProgress job4 = submitJob(JobStatus.RUNNING, 10, 10);
    job4.getJobConf().set("user.name", "user2");
    JobInfo info4 = scheduler.infos.get(job4);
    
    // Check scheduler variables
    assertEquals(1.33,  info1.mapSchedulable.getFairShare(), 0.1);
    assertEquals(1.33,  info1.reduceSchedulable.getFairShare(), 0.1);
    assertEquals(0.0,   info2.mapSchedulable.getFairShare());
    assertEquals(0.0,   info2.reduceSchedulable.getFairShare());
    assertEquals(1.33,  info3.mapSchedulable.getFairShare(), 0.1);
    assertEquals(1.33,  info3.reduceSchedulable.getFairShare(), 0.1);
    assertEquals(1.33,  info4.mapSchedulable.getFairShare(), 0.1);
    assertEquals(1.33,  info4.reduceSchedulable.getFairShare(), 0.1);
    
    // Assign tasks and check that slots are given only to jobs 1, 3 and 4
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0003_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0003_r_000000_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0004_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0004_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000001_0 on tt2");
  }
  
  /**
   * Test a combination of pool job limits and user job limits, the latter
   * specified through both the userMaxJobsDefaults (for some users) and
   * user-specific &lt;user&gt; elements in the allocations file. 
   */
  public void testComplexJobLimits() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"poolA\">");
    out.println("<maxRunningJobs>1</maxRunningJobs>");
    out.println("</pool>");
    out.println("<user name=\"user1\">");
    out.println("<maxRunningJobs>1</maxRunningJobs>");
    out.println("</user>");
    out.println("<user name=\"user2\">");
    out.println("<maxRunningJobs>10</maxRunningJobs>");
    out.println("</user>");
    out.println("<userMaxJobsDefault>2</userMaxJobsDefault>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    // Submit jobs, advancing time in-between to make sure that they are
    // all submitted at distinct times.
    
    // Two jobs for user1; only one should get to run
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    job1.getJobConf().set("user.name", "user1");
    JobInfo info1 = scheduler.infos.get(job1);
    advanceTime(10);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10);
    job2.getJobConf().set("user.name", "user1");
    JobInfo info2 = scheduler.infos.get(job2);
    advanceTime(10);
    
    // Three jobs for user2; all should get to run
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10);
    job3.getJobConf().set("user.name", "user2");
    JobInfo info3 = scheduler.infos.get(job3);
    advanceTime(10);
    JobInProgress job4 = submitJob(JobStatus.RUNNING, 10, 10);
    job4.getJobConf().set("user.name", "user2");
    JobInfo info4 = scheduler.infos.get(job4);
    advanceTime(10);
    JobInProgress job5 = submitJob(JobStatus.RUNNING, 10, 10);
    job5.getJobConf().set("user.name", "user2");
    JobInfo info5 = scheduler.infos.get(job5);
    advanceTime(10);
    
    // Three jobs for user3; only two should get to run
    JobInProgress job6 = submitJob(JobStatus.RUNNING, 10, 10);
    job6.getJobConf().set("user.name", "user3");
    JobInfo info6 = scheduler.infos.get(job6);
    advanceTime(10);
    JobInProgress job7 = submitJob(JobStatus.RUNNING, 10, 10);
    job7.getJobConf().set("user.name", "user3");
    JobInfo info7 = scheduler.infos.get(job7);
    advanceTime(10);
    JobInProgress job8 = submitJob(JobStatus.RUNNING, 10, 10);
    job8.getJobConf().set("user.name", "user3");
    JobInfo info8 = scheduler.infos.get(job8);
    advanceTime(10);
    
    // Two jobs for user4, in poolA; only one should get to run
    JobInProgress job9 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    job9.getJobConf().set("user.name", "user4");
    JobInfo info9 = scheduler.infos.get(job9);
    advanceTime(10);
    JobInProgress job10 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    job10.getJobConf().set("user.name", "user4");
    JobInfo info10 = scheduler.infos.get(job10);
    advanceTime(10);
    
    // Check scheduler variables. The jobs in poolA should get half
    // the total share, while those in the default pool should get
    // the other half. This works out to 2 slots each for the jobs
    // in poolA and 1/3 each for the jobs in the default pool because
    // there are 2 runnable jobs in poolA and 6 jobs in the default pool.
    assertEquals(0.33,   info1.mapSchedulable.getFairShare(), 0.1);
    assertEquals(0.33,   info1.reduceSchedulable.getFairShare(), 0.1);
    assertEquals(0.0,    info2.mapSchedulable.getFairShare());
    assertEquals(0.0,    info2.reduceSchedulable.getFairShare());
    assertEquals(0.33,   info3.mapSchedulable.getFairShare(), 0.1);
    assertEquals(0.33,   info3.reduceSchedulable.getFairShare(), 0.1);
    assertEquals(0.33,   info4.mapSchedulable.getFairShare(), 0.1);
    assertEquals(0.33,   info4.reduceSchedulable.getFairShare(), 0.1);
    assertEquals(0.33,   info5.mapSchedulable.getFairShare(), 0.1);
    assertEquals(0.33,   info5.reduceSchedulable.getFairShare(), 0.1);
    assertEquals(0.33,   info6.mapSchedulable.getFairShare(), 0.1);
    assertEquals(0.33,   info6.reduceSchedulable.getFairShare(), 0.1);
    assertEquals(0.33,   info7.mapSchedulable.getFairShare(), 0.1);
    assertEquals(0.33,   info7.reduceSchedulable.getFairShare(), 0.1);
    assertEquals(0.0,    info8.mapSchedulable.getFairShare());
    assertEquals(0.0,    info8.reduceSchedulable.getFairShare());
    assertEquals(2.0,    info9.mapSchedulable.getFairShare(), 0.1);
    assertEquals(2.0,    info9.reduceSchedulable.getFairShare(), 0.1);
    assertEquals(0.0,    info10.mapSchedulable.getFairShare());
    assertEquals(0.0,    info10.reduceSchedulable.getFairShare());
  }
  
  public void testSizeBasedWeight() throws Exception {
    scheduler.sizeBasedWeight = true;
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 2, 10);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 20, 1);
    assertTrue(scheduler.infos.get(job2).mapSchedulable.getFairShare() >
               scheduler.infos.get(job1).mapSchedulable.getFairShare());
    assertTrue(scheduler.infos.get(job1).reduceSchedulable.getFairShare() >
               scheduler.infos.get(job2).reduceSchedulable.getFairShare());
  }
  

  /**
   * This test submits jobs in three pools: poolA, which has a weight
   * of 2.0; poolB, which has a weight of 0.5; and the default pool, which
   * should have a weight of 1.0. It then checks that the map and reduce
   * fair shares are given out accordingly. We then submit a second job to
   * pool B and check that each gets half of the pool (weight of 0.25).
   */
  public void testPoolWeights() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"poolA\">");
    out.println("<weight>2.0</weight>");
    out.println("</pool>");
    out.println("<pool name=\"poolB\">");
    out.println("<weight>0.5</weight>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    // Submit jobs, advancing time in-between to make sure that they are
    // all submitted at distinct times.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    JobInfo info2 = scheduler.infos.get(job2);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10, "poolB");
    JobInfo info3 = scheduler.infos.get(job3);
    advanceTime(10);
    
    assertEquals(1.14,  info1.mapSchedulable.getFairShare(), 0.01);
    assertEquals(1.14,  info1.reduceSchedulable.getFairShare(), 0.01);
    assertEquals(2.28,  info2.mapSchedulable.getFairShare(), 0.01);
    assertEquals(2.28,  info2.reduceSchedulable.getFairShare(), 0.01);
    assertEquals(0.57,  info3.mapSchedulable.getFairShare(), 0.01);
    assertEquals(0.57,  info3.reduceSchedulable.getFairShare(), 0.01);
    
    JobInProgress job4 = submitJob(JobStatus.RUNNING, 10, 10, "poolB");
    JobInfo info4 = scheduler.infos.get(job4);
    advanceTime(10);
    
    assertEquals(1.14,  info1.mapSchedulable.getFairShare(), 0.01);
    assertEquals(1.14,  info1.reduceSchedulable.getFairShare(), 0.01);
    assertEquals(2.28,  info2.mapSchedulable.getFairShare(), 0.01);
    assertEquals(2.28,  info2.reduceSchedulable.getFairShare(), 0.01);
    assertEquals(0.28,  info3.mapSchedulable.getFairShare(), 0.01);
    assertEquals(0.28,  info3.reduceSchedulable.getFairShare(), 0.01);
    assertEquals(0.28,  info4.mapSchedulable.getFairShare(), 0.01);
    assertEquals(0.28,  info4.reduceSchedulable.getFairShare(), 0.01);
    verifyMetrics();    
  }

  /**
   * This test submits jobs in two pools, poolA and poolB. None of the
   * jobs in poolA have maps, but this should not affect their reduce
   * share.
   */
  public void testPoolWeightsWhenNoMaps() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"poolA\">");
    out.println("<weight>2.0</weight>");
    out.println("</pool>");
    out.println("<pool name=\"poolB\">");
    out.println("<weight>1.0</weight>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    // Submit jobs, advancing time in-between to make sure that they are
    // all submitted at distinct times.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 0, 10, "poolA");
    JobInfo info1 = scheduler.infos.get(job1);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 0, 10, "poolA");
    JobInfo info2 = scheduler.infos.get(job2);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10, "poolB");
    JobInfo info3 = scheduler.infos.get(job3);
    advanceTime(10);
    
    /*
    assertEquals(0,     info1.mapWeight, 0.01);
    assertEquals(1.0,   info1.reduceWeight, 0.01);
    assertEquals(0,     info2.mapWeight, 0.01);
    assertEquals(1.0,   info2.reduceWeight, 0.01);
    assertEquals(1.0,   info3.mapWeight, 0.01);
    assertEquals(1.0,   info3.reduceWeight, 0.01);
    */
    
    assertEquals(0,     info1.mapSchedulable.getFairShare(), 0.01);
    assertEquals(1.33,  info1.reduceSchedulable.getFairShare(), 0.01);
    assertEquals(0,     info2.mapSchedulable.getFairShare(), 0.01);
    assertEquals(1.33,  info2.reduceSchedulable.getFairShare(), 0.01);
    assertEquals(4,     info3.mapSchedulable.getFairShare(), 0.01);
    assertEquals(1.33,  info3.reduceSchedulable.getFairShare(), 0.01);
  }

  public void testPoolMaxMapsReduces() throws Exception {
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Pool with upper bound
    out.println("<pool name=\"poolLimited\">");
    out.println("<weight>1.0</weight>");
    out.println("<maxMaps>2</maxMaps>");
    out.println("<maxReduces>1</maxReduces>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    // Create two jobs with ten maps
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 5, "poolLimited");
    advanceTime(10);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 5);
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000002_0 on tt2");

    Pool limited = scheduler.getPoolManager().getPool("poolLimited");
    assertEquals(2, limited.getSchedulable(TaskType.MAP).getRunningTasks());
    assertEquals(1, limited.getSchedulable(TaskType.REDUCE).getRunningTasks());
    Pool defaultPool = scheduler.getPoolManager().getPool("default");
    assertEquals(2, defaultPool.getSchedulable(TaskType.MAP).getRunningTasks());
    assertEquals(3, defaultPool.getSchedulable(TaskType.REDUCE)
        .getRunningTasks());
    assertEquals(2, job1.runningMapTasks);
    assertEquals(1, job1.runningReduceTasks);
    assertEquals(2, job2.runningMapTasks);
    assertEquals(3, job2.runningReduceTasks);
  }

  /**
   * Tests that max-running-tasks per node are set by assigning load
   * equally accross the cluster in CapBasedLoadManager.
   */
  public void testCapBasedLoadManager() {
    CapBasedLoadManager loadMgr = new CapBasedLoadManager();
    // Arguments to getCap: totalRunnableTasks, nodeCap, totalSlots
    // Desired behavior: return ceil(nodeCap * min(1, runnableTasks/totalSlots))
    assertEquals(1, loadMgr.getCap(1, 1, 100));
    assertEquals(1, loadMgr.getCap(1, 2, 100));
    assertEquals(1, loadMgr.getCap(1, 10, 100));
    assertEquals(1, loadMgr.getCap(200, 1, 100));
    assertEquals(1, loadMgr.getCap(1, 5, 100));
    assertEquals(3, loadMgr.getCap(50, 5, 100));
    assertEquals(5, loadMgr.getCap(100, 5, 100));
    assertEquals(5, loadMgr.getCap(200, 5, 100));
  }
  
  /**
   * This test starts by launching a job in the default pool that takes
   * all the slots in the cluster. We then submit a job in a pool with
   * min share of 2 maps and 1 reduce task. After the min share preemption
   * timeout, this pool should be allowed to preempt tasks. 
   */
  public void testMinSharePreemption() throws Exception {
    // Enable preemption in scheduler
    scheduler.preemptionEnabled = true;
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a min share of 2 maps and 1 reduce, and a preemption
    // timeout of 1 minute
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>1</minReduces>");
    out.println("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    Pool poolA = scheduler.getPoolManager().getPool("poolA");

    // Submit job 1 and assign all slots to it. Sleep a bit before assigning
    // tasks on tt1 and tt2 to ensure that the ones on tt2 get preempted first.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000003_0 on tt2");
    
    // Ten seconds later, submit job 2.
    advanceTime(10000);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    
    // Ten seconds later, check that job 2 is not able to preempt tasks.
    advanceTime(10000);
    assertEquals(0, scheduler.tasksToPreempt(poolA.getMapSchedulable(),
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(poolA.getReduceSchedulable(),
        clock.getTime()));
    
    // Advance time by 49 more seconds, putting us at 59s after the
    // submission of job 2. It should still not be able to preempt.
    advanceTime(49000);
    assertEquals(0, scheduler.tasksToPreempt(poolA.getMapSchedulable(),
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(poolA.getReduceSchedulable(),
        clock.getTime()));
    
    // Advance time by 2 seconds, putting us at 61s after the submission
    // of job 2. It should now be able to preempt 2 maps and 1 reduce.
    advanceTime(2000);
    assertEquals(2, scheduler.tasksToPreempt(poolA.getMapSchedulable(),
        clock.getTime()));
    assertEquals(1, scheduler.tasksToPreempt(poolA.getReduceSchedulable(),
        clock.getTime()));
    
    // Test that the tasks actually get preempted and we can assign new ones
    scheduler.preemptTasksIfNecessary();
    scheduler.update();
    assertEquals(2, job1.runningMaps());
    assertEquals(3, job1.runningReduces());
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000000_0 on tt2");
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
  }

  /**
   * This test starts by launching a job in the default pool that takes
   * all the slots in the cluster. We then submit a job in a pool with
   * min share of 3 maps and 3 reduce tasks, but which only actually
   * needs 1 map and 2 reduces. We check that this pool does not prempt
   * more than this many tasks despite its min share being higher. 
   */
  public void testMinSharePreemptionWithSmallJob() throws Exception {
    // Enable preemption in scheduler
    scheduler.preemptionEnabled = true;
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a min share of 2 maps and 1 reduce, and a preemption
    // timeout of 1 minute
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>3</minMaps>");
    out.println("<minReduces>3</minReduces>");
    out.println("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    Pool poolA = scheduler.getPoolManager().getPool("poolA");

    // Submit job 1 and assign all slots to it. Sleep a bit before assigning
    // tasks on tt1 and tt2 to ensure that the ones on tt2 get preempted first.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000003_0 on tt2");
    
    // Ten seconds later, submit job 2.
    advanceTime(10000);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 1, 2, "poolA");
    
    // Advance time by 59 seconds and check that no preemption occurs.
    advanceTime(59000);
    assertEquals(0, scheduler.tasksToPreempt(poolA.getMapSchedulable(),
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(poolA.getReduceSchedulable(),
        clock.getTime()));
    
    // Advance time by 2 seconds, putting us at 61s after the submission
    // of job 2. Job 2 should now preempt 1 map and 2 reduces.
    advanceTime(2000);
    assertEquals(1, scheduler.tasksToPreempt(poolA.getMapSchedulable(),
        clock.getTime()));
    assertEquals(2, scheduler.tasksToPreempt(poolA.getReduceSchedulable(),
        clock.getTime()));

    // Test that the tasks actually get preempted and we can assign new ones
    scheduler.preemptTasksIfNecessary();
    scheduler.update();
    assertEquals(3, job1.runningMaps());
    assertEquals(2, job1.runningReduces());
    checkAssignment("tt2", "attempt_test_0002_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
  }

  /**
   * This test runs on a 4-node (8-slot) cluster to allow 3 pools with fair
   * shares greater than 2 slots to coexist (which makes the half-fair-share 
   * of each pool more than 1 so that fair share preemption can kick in). 
   * 
   * The test first starts job 1, which takes 6 map slots and 6 reduce slots,
   * in pool 1.  We then submit job 2 in pool 2, which takes 2 slots of each
   * type. Finally, we submit a third job, job 3 in pool3, which gets no slots. 
   * At this point the fair share of each pool will be 8/3 ~= 2.7 slots. 
   * Pool 1 will be above its fair share, pool 2 will be below it but at half
   * fair share, and pool 3 will be below half fair share. Therefore pool 3 
   * should preempt a task (after a timeout) but pools 1 and 2 shouldn't. 
   */
  public void testFairSharePreemption() throws Exception {
    // Create a bigger cluster than normal (4 tasktrackers instead of 2)
    setUpCluster(1, 4, false);
    // Enable preemption in scheduler
    scheduler.preemptionEnabled = true;
    // Set up pools file with a fair share preemtion timeout of 1 minute
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<fairSharePreemptionTimeout>60</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    // Grab pools (they'll be created even though they're not in the alloc file)
    Pool pool1 = scheduler.getPoolManager().getPool("pool1");
    Pool pool2 = scheduler.getPoolManager().getPool("pool2");
    Pool pool3 = scheduler.getPoolManager().getPool("pool3");

    // Submit job 1. We advance time by 100 between each task tracker
    // assignment stage to ensure that the tasks from job1 on tt3 are the ones
    // that are deterministically preempted first (being the latest launched
    // tasks in an over-allocated job).
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 6, 6, "pool1");
    advanceTime(100);
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000003_0 on tt2");
    advanceTime(100);
    checkAssignment("tt3", "attempt_test_0001_m_000004_0 on tt3");
    checkAssignment("tt3", "attempt_test_0001_r_000004_0 on tt3");
    checkAssignment("tt3", "attempt_test_0001_m_000005_0 on tt3");
    checkAssignment("tt3", "attempt_test_0001_r_000005_0 on tt3");
    advanceTime(100);
    
    // Submit job 2. It should get the last 2 slots.
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "pool2");
    advanceTime(100);
    checkAssignment("tt4", "attempt_test_0002_m_000000_0 on tt4");
    checkAssignment("tt4", "attempt_test_0002_r_000000_0 on tt4");
    checkAssignment("tt4", "attempt_test_0002_m_000001_0 on tt4");
    checkAssignment("tt4", "attempt_test_0002_r_000001_0 on tt4");
    
    // Submit job 3.
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10, "pool3");
    
    // Check that after 59 seconds, neither pool can preempt
    advanceTime(59000);
    assertEquals(0, scheduler.tasksToPreempt(pool2.getMapSchedulable(),
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(pool2.getReduceSchedulable(),
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(pool3.getMapSchedulable(),
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(pool3.getReduceSchedulable(),
        clock.getTime()));
    
    // Wait 2 more seconds, so that job 3 has now been in the system for 61s.
    // Now pool 3 should be able to preempt 2 tasks (its share of 2.7 rounded
    // down to its floor), but pool 2 shouldn't.
    advanceTime(2000);
    assertEquals(0, scheduler.tasksToPreempt(pool2.getMapSchedulable(),
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(pool2.getReduceSchedulable(),
        clock.getTime()));
    assertEquals(2, scheduler.tasksToPreempt(pool3.getMapSchedulable(),
        clock.getTime()));
    assertEquals(2, scheduler.tasksToPreempt(pool3.getReduceSchedulable(),
        clock.getTime()));
    
    // Test that the tasks actually get preempted and we can assign new ones
    scheduler.preemptTasksIfNecessary();
    scheduler.update();
    assertEquals(4, job1.runningMaps());
    assertEquals(4, job1.runningReduces());
    checkAssignment("tt3", "attempt_test_0003_m_000000_0 on tt3");
    checkAssignment("tt3", "attempt_test_0003_r_000000_0 on tt3");
    checkAssignment("tt3", "attempt_test_0003_m_000001_0 on tt3");
    checkAssignment("tt3", "attempt_test_0003_r_000001_0 on tt3");
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
    assertNull(scheduler.assignTasks(tracker("tt3")));
    assertNull(scheduler.assignTasks(tracker("tt4")));
  }
  
  /**
   * This test runs on a 3-node (6-slot) cluster to allow 3 pools with fair
   * shares equal 2 slots to coexist (which makes the half-fair-share 
   * of each pool equal to 1 so that fair share preemption can kick in). 
   * 
   * The test first starts job 1, which takes 3 map slots and 0 reduce slots,
   * in pool 1.  We then submit job 2 in pool 2, which takes 3 map slots and zero
   * reduce slots. Finally, we submit a third job, job 3 in pool3, which gets no slots. 
   * At this point the fair share of each pool will be 6/3 = 2 slots. 
   * Pool 1 and 2 will be above their fair share and pool 3 will be below half fair share. 
   * Therefore pool 3 should preempt tasks from both pool 1 & 2 (after a timeout) but 
   * pools 1 and 2 shouldn't. 
   */
  public void testFairSharePreemptionFromMultiplePools() throws Exception {
	// Create a bigger cluster than normal (3 tasktrackers instead of 2)
	setUpCluster(1, 3, false);
	// Enable preemption in scheduler
	scheduler.preemptionEnabled = true;
	// Set up pools file with a fair share preemtion timeout of 1 minute
	PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
	out.println("<?xml version=\"1.0\"?>");
	out.println("<allocations>");
	out.println("<fairSharePreemptionTimeout>60</fairSharePreemptionTimeout>");
	out.println("</allocations>");
	out.close();
	scheduler.getPoolManager().reloadAllocs();
	 
	// Grab pools (they'll be created even though they're not in the alloc file)
	Pool pool1 = scheduler.getPoolManager().getPool("pool1");
	Pool pool2 = scheduler.getPoolManager().getPool("pool2");
	Pool pool3 = scheduler.getPoolManager().getPool("pool3");

	// Submit job 1. We advance time by 100 between each task tracker
	// assignment stage to ensure that the tasks from job1 on tt3 are the ones
	// that are deterministically preempted first (being the latest launched
	// tasks in an over-allocated job).
	JobInProgress job1 = submitJob(JobStatus.RUNNING, 12, 0, "pool1");
	advanceTime(100);
	checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
	checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
	advanceTime(100);
	checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
	advanceTime(100);
	    
	// Submit job 2. It should get the last 3 slots.
	JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 0, "pool2");
	advanceTime(100);
	checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
	checkAssignment("tt3", "attempt_test_0002_m_000001_0 on tt3");
	advanceTime(100);
	checkAssignment("tt3", "attempt_test_0002_m_000002_0 on tt3");

	advanceTime(100);
	    
	// Submit job 3.
	JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 0, "pool3");
	    
	// Check that after 59 seconds, neither pool can preempt
	advanceTime(59000);
	assertEquals(0, scheduler.tasksToPreempt(pool2.getMapSchedulable(),
			clock.getTime()));
	assertEquals(0, scheduler.tasksToPreempt(pool2.getReduceSchedulable(),
	        clock.getTime()));
	assertEquals(0, scheduler.tasksToPreempt(pool3.getMapSchedulable(),
	        clock.getTime()));
	assertEquals(0, scheduler.tasksToPreempt(pool3.getReduceSchedulable(),
	        clock.getTime()));
	    
	// Wait 2 more seconds, so that job 3 has now been in the system for 61s.
	// Now pool 3 should be able to preempt 2 tasks (its share of 2 rounded
	// down to its floor), but pool 1 & 2 shouldn't.
	advanceTime(2000);
	assertEquals(0, scheduler.tasksToPreempt(pool2.getMapSchedulable(),
	        clock.getTime()));
	assertEquals(0, scheduler.tasksToPreempt(pool2.getReduceSchedulable(),
	        clock.getTime()));
	assertEquals(2, scheduler.tasksToPreempt(pool3.getMapSchedulable(),
	        clock.getTime()));
	assertEquals(0, scheduler.tasksToPreempt(pool3.getReduceSchedulable(),
	        clock.getTime()));
	    
	// Test that the tasks actually get preempted and we can assign new ones.
	// This should preempt one task each from pool1 and pool2
	scheduler.preemptTasksIfNecessary();
	scheduler.update();
	assertEquals(2, job2.runningMaps());  
	assertEquals(2, job1.runningMaps());  
	checkAssignment("tt2", "attempt_test_0003_m_000000_0 on tt2");
	checkAssignment("tt3", "attempt_test_0003_m_000001_0 on tt3");
	assertNull(scheduler.assignTasks(tracker("tt1")));
	assertNull(scheduler.assignTasks(tracker("tt2")));
	assertNull(scheduler.assignTasks(tracker("tt3")));
  }
  
  
  /**
   * This test submits a job that takes all 4 slots, and then a second job in
   * a pool that has both a min share of 2 slots with a 60s timeout and a
   * fair share timeout of 60s. After 60 seconds, this pool will be starved
   * of both min share (2 slots of each type) and fair share (2 slots of each
   * type), and we test that it does not kill more than 2 tasks of each type
   * in total.
   */
  public void testMinAndFairSharePreemption() throws Exception {
    // Enable preemption in scheduler
    scheduler.preemptionEnabled = true;
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a min share of 2 maps and 1 reduce, and a preemption
    // timeout of 1 minute
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>2</minReduces>");
    out.println("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
    out.println("</pool>");
    out.println("<fairSharePreemptionTimeout>60</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    Pool poolA = scheduler.getPoolManager().getPool("poolA");

    // Submit job 1 and assign all slots to it. Sleep a bit before assigning
    // tasks on tt1 and tt2 to ensure that the ones on tt2 get preempted first.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000003_0 on tt2");
    
    // Ten seconds later, submit job 2.
    advanceTime(10000);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    
    // Ten seconds later, check that job 2 is not able to preempt tasks.
    advanceTime(10000);
    assertEquals(0, scheduler.tasksToPreempt(poolA.getMapSchedulable(),
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(poolA.getReduceSchedulable(),
        clock.getTime()));
    
    // Advance time by 49 more seconds, putting us at 59s after the
    // submission of job 2. It should still not be able to preempt.
    advanceTime(49000);
    assertEquals(0, scheduler.tasksToPreempt(poolA.getMapSchedulable(),
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(poolA.getReduceSchedulable(),
        clock.getTime()));
    
    // Advance time by 2 seconds, putting us at 61s after the submission
    // of job 2. It should now be able to preempt 2 maps and 1 reduce.
    advanceTime(2000);
    assertEquals(2, scheduler.tasksToPreempt(poolA.getMapSchedulable(),
        clock.getTime()));
    assertEquals(2, scheduler.tasksToPreempt(poolA.getReduceSchedulable(),
        clock.getTime()));

    // Test that the tasks actually get preempted and we can assign new ones
    scheduler.preemptTasksIfNecessary();
    scheduler.update();
    assertEquals(2, job1.runningMaps());
    assertEquals(2, job1.runningReduces());
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
  }
  
  /**
   * This is a copy of testMinAndFairSharePreemption that turns preemption
   * off and verifies that no tasks get killed.
   */
  public void testNoPreemptionIfDisabled() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a min share of 2 maps and 1 reduce, and a preemption
    // timeout of 1 minute
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>2</minReduces>");
    out.println("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
    out.println("</pool>");
    out.println("<fairSharePreemptionTimeout>60</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    // Submit job 1 and assign all slots to it. Sleep a bit before assigning
    // tasks on tt1 and tt2 to ensure that the ones on tt2 get preempted first.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000003_0 on tt2");
    
    // Ten seconds later, submit job 2.
    advanceTime(10000);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    
    // Advance time by 61s, putting us past the preemption timeout,
    // and check that no tasks get preempted.
    advanceTime(61000);
    scheduler.preemptTasksIfNecessary();
    scheduler.update();
    assertEquals(4, job1.runningMaps());
    assertEquals(4, job1.runningReduces());
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
  }

  /**
   * This is a copy of testMinAndFairSharePreemption that turns preemption
   * on but also turns on mapred.fairscheduler.preemption.only.log (the
   * "dry run" parameter for testing out preemption) and verifies that no
   * tasks get killed.
   */
  public void testNoPreemptionIfOnlyLogging() throws Exception {
    // Turn on preemption, but for logging only
    scheduler.preemptionEnabled = true;
    scheduler.onlyLogPreemption = true;
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a min share of 2 maps and 1 reduce, and a preemption
    // timeout of 1 minute
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>2</minReduces>");
    out.println("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
    out.println("</pool>");
    out.println("<fairSharePreemptionTimeout>60</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    // Submit job 1 and assign all slots to it. Sleep a bit before assigning
    // tasks on tt1 and tt2 to ensure that the ones on tt2 get preempted first.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000003_0 on tt2");
    
    // Ten seconds later, submit job 2.
    advanceTime(10000);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    
    // Advance time by 61s, putting us past the preemption timeout,
    // and check that no tasks get preempted.
    advanceTime(61000);
    scheduler.preemptTasksIfNecessary();
    scheduler.update();
    assertEquals(4, job1.runningMaps());
    assertEquals(4, job1.runningReduces());
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
  }

  /**
   * This test exercises delay scheduling at the node level. We submit a job
   * with data on rack1.node2 and check that it doesn't get assigned on earlier
   * nodes. A second job with no locality info should get assigned instead.
   * 
   * TaskTracker names in this test map to nodes as follows:
   * - tt1 = rack1.node1
   * - tt2 = rack1.node2
   * - tt3 = rack2.node1
   * - tt4 = rack2.node2
   */
  public void testDelaySchedulingAtNodeLevel() throws IOException {
    setUpCluster(2, 2, true);
    scheduler.assignMultiple = true;
    
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 1, 0, "pool1",
        new String[][] {
          {"rack2.node2"}
        });
    JobInfo info1 = scheduler.infos.get(job1);
    
    // Advance time before submitting another job j2, to make j1 be ahead
    // of j2 in the queue deterministically.
    advanceTime(100);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 0);
    
    // Assign tasks on nodes 1-3 and check that j2 gets them
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1", 
                           "attempt_test_0002_m_000001_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000002_0 on tt2",
                           "attempt_test_0002_m_000003_0 on tt2");
    checkAssignment("tt3", "attempt_test_0002_m_000004_0 on tt3",
                           "attempt_test_0002_m_000005_0 on tt3");
    
    // Assign a task on node 4 now and check that j1 gets it. The other slot
    // on the node should be given to j2 because j1 will be out of tasks.
    checkAssignment("tt4", "attempt_test_0001_m_000000_0 on tt4",
                           "attempt_test_0002_m_000006_0 on tt4");
    
    // Check that delay scheduling info is properly set
    assertEquals(info1.lastMapLocalityLevel, LocalityLevel.NODE);
    assertEquals(info1.timeWaitedForLocalMap, 0);
    assertEquals(info1.skippedAtLastHeartbeat, false);
  }

  /**
   * This test submits a job and causes it to exceed its node-level delay,
   * and thus to go on to launch a rack-local task. We submit one job with data
   * on rack2.node4 and check that it does not get assigned on any of the other
   * nodes until 10 seconds (the delay configured in setUpCluster) pass.
   * Finally, after some delay, we let the job assign local tasks and check
   * that it has returned to waiting for node locality.
   * 
   * TaskTracker names in this test map to nodes as follows:
   * - tt1 = rack1.node1
   * - tt2 = rack1.node2
   * - tt3 = rack2.node1
   * - tt4 = rack2.node2
   */
  public void testDelaySchedulingAtRackLevel() throws IOException {
    setUpCluster(2, 2, true);
    scheduler.assignMultiple = true;
    
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 4, 0, "pool1",
        new String[][] {
          {"rack2.node2"}, {"rack2.node2"}, {"rack2.node2"}, {"rack2.node2"}
        });
    JobInfo info1 = scheduler.infos.get(job1);
    
    // Advance time before submitting another job j2, to make j1 be ahead
    // of j2 in the queue deterministically.
    advanceTime(100);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 20, 0);
    
    // Assign tasks on nodes 1-3 and check that j2 gets them
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1", 
                           "attempt_test_0002_m_000001_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000002_0 on tt2",
                           "attempt_test_0002_m_000003_0 on tt2");
    checkAssignment("tt3", "attempt_test_0002_m_000004_0 on tt3",
                           "attempt_test_0002_m_000005_0 on tt3");
    
    // Advance time by 11 seconds to put us past the 10-second locality delay
    advanceTime(11000);
    
    // Finish some tasks on each node
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_m_000000_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_m_000002_0");
    taskTrackerManager.finishTask("tt3", "attempt_test_0002_m_000004_0");
    advanceTime(100);
    
    // Check that job 1 is only assigned on node 3 (which is rack-local)
    checkAssignment("tt1", "attempt_test_0002_m_000006_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000007_0 on tt2");
    checkAssignment("tt3", "attempt_test_0001_m_000000_0 on tt3");
    
    // Check that delay scheduling info is properly set
    assertEquals(info1.lastMapLocalityLevel, LocalityLevel.RACK);
    assertEquals(info1.timeWaitedForLocalMap, 0);
    assertEquals(info1.skippedAtLastHeartbeat, false);
    
    // Also give job 1 some tasks on node 4. Its lastMapLocalityLevel
    // should go back to 0 after it gets assigned these.
    checkAssignment("tt4", "attempt_test_0001_m_000001_0 on tt4",
                           "attempt_test_0001_m_000002_0 on tt4");
    
    // Check that delay scheduling info is properly set
    assertEquals(info1.lastMapLocalityLevel, LocalityLevel.NODE);
    assertEquals(info1.timeWaitedForLocalMap, 0);
    assertEquals(info1.skippedAtLastHeartbeat, false);
    
    // Check that job 1 no longer assigns tasks in the same rack now
    // that it has obtained a node-local task
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_m_000001_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_m_000003_0");
    taskTrackerManager.finishTask("tt3", "attempt_test_0002_m_000005_0");
    advanceTime(100);
    checkAssignment("tt1", "attempt_test_0002_m_000008_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000009_0 on tt2");
    checkAssignment("tt3", "attempt_test_0002_m_000010_0 on tt3");
    advanceTime(100);
    
    // However, job 1 should still be able to launch tasks on node 4
    taskTrackerManager.finishTask("tt4", "attempt_test_0001_m_000001_0");
    advanceTime(100);
    checkAssignment("tt4", "attempt_test_0001_m_000003_0 on tt4");
  }
  
  /**
   * This test submits a job and causes it to exceed its node-level delay,
   * then its rack-level delay. It should then launch tasks off-rack.
   * However, once the job gets a rack-local slot it should stay in-rack,
   * and once it gets a node-local slot it should stay in-node.
   * For simplicity, we don't submit a second job in this test.
   * 
   * TaskTracker names in this test map to nodes as follows:
   * - tt1 = rack1.node1
   * - tt2 = rack1.node2
   * - tt3 = rack2.node1
   * - tt4 = rack2.node2
   */
  public void testDelaySchedulingOffRack() throws IOException {
    setUpCluster(2, 2, true);
    scheduler.assignMultiple = true;
    
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 8, 0, "pool1",
        new String[][] {
          {"rack2.node2"}, {"rack2.node2"}, {"rack2.node2"}, {"rack2.node2"},
          {"rack2.node2"}, {"rack2.node2"}, {"rack2.node2"}, {"rack2.node2"},
        });
    JobInfo info1 = scheduler.infos.get(job1);
    advanceTime(100);
    
    // Check that nothing is assigned on trackers 1-3
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
    assertNull(scheduler.assignTasks(tracker("tt3")));
    
    // Advance time by 11 seconds to put us past the 10-sec node locality delay
    advanceTime(11000);

    // Check that nothing is assigned on trackers 1-2; the job would assign
    // a task on tracker 3 (rack1.node2) so we skip that one 
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
    
    // Repeat to see that receiving multiple heartbeats works
    advanceTime(100);
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
    advanceTime(100);
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));

    // Check that delay scheduling info is properly set
    assertEquals(info1.lastMapLocalityLevel, LocalityLevel.NODE);
    assertEquals(info1.timeWaitedForLocalMap, 11200);
    assertEquals(info1.skippedAtLastHeartbeat, true);
    
    // Advance time by 11 seconds to put us past the 10-sec rack locality delay
    advanceTime(11000);
    
    // Now the job should be able to assign tasks on tt1 and tt2
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1",
                           "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2",
                           "attempt_test_0001_m_000003_0 on tt2");

    // Check that delay scheduling info is properly set
    assertEquals(info1.lastMapLocalityLevel, LocalityLevel.ANY);
    assertEquals(info1.timeWaitedForLocalMap, 0);
    assertEquals(info1.skippedAtLastHeartbeat, false);
    
    // Now assign a task on tt3. This should make the job stop assigning
    // on tt1 and tt2 (checked after we finish some tasks there)
    checkAssignment("tt3", "attempt_test_0001_m_000004_0 on tt3",
                           "attempt_test_0001_m_000005_0 on tt3");

    // Check that delay scheduling info is properly set
    assertEquals(info1.lastMapLocalityLevel, LocalityLevel.RACK);
    assertEquals(info1.timeWaitedForLocalMap, 0);
    assertEquals(info1.skippedAtLastHeartbeat, false);
    
    // Check that j1 no longer assigns tasks on rack 1 now
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000001_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_m_000003_0");
    advanceTime(100);
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
    
    // However, tasks on rack 2 should still be assigned
    taskTrackerManager.finishTask("tt3", "attempt_test_0001_m_000004_0");
    advanceTime(100);
    checkAssignment("tt3", "attempt_test_0001_m_000006_0 on tt3");
    
    // Now assign a task on node 4
    checkAssignment("tt4", "attempt_test_0001_m_000007_0 on tt4");

    // Check that delay scheduling info is set so we are looking for node-local
    // tasks at this point
    assertEquals(info1.lastMapLocalityLevel, LocalityLevel.NODE);
    assertEquals(info1.timeWaitedForLocalMap, 0);
    assertEquals(info1.skippedAtLastHeartbeat, false);
  }
  
  /**
   * This test submits two jobs with 4 maps and 3 reduces in total to a
   * 4-node cluster. Although the cluster has 2 map slots and 2 reduce
   * slots per node, it should only launch one map and one reduce on each
   * node to balance the load. We check that this happens even if
   * assignMultiple is set to true so the scheduler has the opportunity
   * to launch multiple tasks per heartbeat.
   */
  public void testAssignMultipleWithUnderloadedCluster() throws IOException {
    setUpCluster(1, 4, true);
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 2, 2);
    
    // Advance to make j1 be scheduled before j2 deterministically.
    advanceTime(100);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 2, 1);
    
    // Assign tasks and check that at most one map and one reduce slot is used
    // on each node, and that no tasks are assigned on subsequent heartbeats
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1",
                           "attempt_test_0001_r_000000_0 on tt1");
    assertNull(scheduler.assignTasks(tracker("tt1")));
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2",
                           "attempt_test_0002_r_000000_0 on tt2");
    assertNull(scheduler.assignTasks(tracker("tt2")));
    checkAssignment("tt3", "attempt_test_0001_m_000001_0 on tt3",
                           "attempt_test_0001_r_000001_0 on tt3");
    assertNull(scheduler.assignTasks(tracker("tt3")));
    checkAssignment("tt4", "attempt_test_0002_m_000001_0 on tt4");
    assertNull(scheduler.assignTasks(tracker("tt4")));
  }
  
  /**
   * This test submits four jobs in the default pool, which is set to FIFO mode:
   * - job1, with 1 map and 1 reduce
   * - job2, with 3 maps and 3 reduces
   * - job3, with 1 map, 1 reduce, and priority set to HIGH
   * - job4, with 3 maps and 3 reduces
   * 
   * We check that the scheduler assigns tasks first to job3 (because it is
   * high priority), then to job1, then to job2.
   */
  public void testFifoPool() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"default\">");
    out.println("<schedulingMode>fifo</schedulingMode>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    // Submit jobs, advancing time in-between to make sure that they are
    // all submitted at distinct times.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 1, 1);
    advanceTime(10);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 3, 3);
    advanceTime(10);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 1, 1);
    job3.setPriority(JobPriority.HIGH);
    advanceTime(10);
    JobInProgress job4 = submitJob(JobStatus.RUNNING, 3, 3);
    
    // Assign tasks and check that they're given first to job3 (because it is
    // high priority), then to job1, then to job2.
    checkAssignment("tt1", "attempt_test_0003_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0003_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
  }
  
  /**
   * This test submits 2 large jobs each to 2 pools, which are both set to FIFO
   * mode through the global defaultPoolSchedulingMode setting. We check that
   * the scheduler assigns tasks only to the first job within each pool, but
   * alternates between the pools to give each pool a fair share.
   */
  public void testMultipleFifoPools() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<defaultPoolSchedulingMode>fifo</defaultPoolSchedulingMode>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    // Submit jobs, advancing time in-between to make sure that they are
    // all submitted at distinct times.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    advanceTime(10);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    advanceTime(10);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10, "poolB");
    advanceTime(10);
    JobInProgress job4 = submitJob(JobStatus.RUNNING, 10, 10, "poolB");
    
    // Assign tasks and check that they alternate between jobs 1 and 3, the
    // head-of-line jobs in their respective pools.
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0003_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0003_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0003_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0003_r_000001_0 on tt2");
  }
  
  /**
   * This test submits 2 large jobs each to 2 pools, one of which is set to FIFO
   * mode through the global defaultPoolSchedulingMode setting, and one of which
   * is set to fair mode. We check that the scheduler assigns tasks only to the
   * first job in the FIFO pool but to both jobs in the fair sharing pool.
   */
  public void testFifoAndFairPools() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<defaultPoolSchedulingMode>fifo</defaultPoolSchedulingMode>");
    out.println("<pool name=\"poolB\">");
    out.println("<schedulingMode>fair</schedulingMode>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    // Submit jobs, advancing time in-between to make sure that they are
    // all submitted at distinct times.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    advanceTime(10);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    advanceTime(10);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10, "poolB");
    advanceTime(10);
    JobInProgress job4 = submitJob(JobStatus.RUNNING, 10, 10, "poolB");
    
    // Assign tasks and check that only job 1 gets tasks in pool A, but
    // jobs 3 and 4 both get tasks in pool B.
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0003_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0003_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0004_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0004_r_000000_0 on tt2");
  }

  /**
   * This test uses the mapred.fairscheduler.pool property to assign jobs to pools.
   */
  public void testPoolAssignment() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"default\">");
    out.println("<schedulingMode>fair</schedulingMode>");
    out.println("</pool>");
    out.println("<pool name=\"poolA\">");
    out.println("<schedulingMode>fair</schedulingMode>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    Pool defaultPool = scheduler.getPoolManager().getPool("default");
    Pool poolA = scheduler.getPoolManager().getPool("poolA");
 
    // Submit a job to the default pool.  All specifications take default values.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 1, 3);

    assertEquals(1,    defaultPool.getMapSchedulable().getDemand());
    assertEquals(3,    defaultPool.getReduceSchedulable().getDemand());
    assertEquals(0,    poolA.getMapSchedulable().getDemand());
    assertEquals(0,    poolA.getReduceSchedulable().getDemand());

    // Submit a job to the default pool and move it to poolA using setPool.
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 5, 7);

    assertEquals(6,    defaultPool.getMapSchedulable().getDemand());
    assertEquals(10,   defaultPool.getReduceSchedulable().getDemand());
    assertEquals(0,    poolA.getMapSchedulable().getDemand());
    assertEquals(0,    poolA.getReduceSchedulable().getDemand());

    scheduler.getPoolManager().setPool(job2, "poolA");
    assertEquals("poolA", scheduler.getPoolManager().getPoolName(job2));

    defaultPool.getMapSchedulable().updateDemand();
    defaultPool.getReduceSchedulable().updateDemand();
    poolA.getMapSchedulable().updateDemand();
    poolA.getReduceSchedulable().updateDemand();

    assertEquals(1,    defaultPool.getMapSchedulable().getDemand());
    assertEquals(3,    defaultPool.getReduceSchedulable().getDemand());
    assertEquals(5,    poolA.getMapSchedulable().getDemand());
    assertEquals(7,    poolA.getReduceSchedulable().getDemand());

    // Submit a job to poolA by specifying mapred.fairscheduler.pool
    JobConf jobConf = new JobConf(conf);
    jobConf.setNumMapTasks(11);
    jobConf.setNumReduceTasks(13);
    jobConf.set(POOL_PROPERTY, "nonsense"); // test that this is overridden
    jobConf.set(EXPLICIT_POOL_PROPERTY, "poolA");
    JobInProgress job3 = new FakeJobInProgress(jobConf, taskTrackerManager,
        null, UtilsForTests.getJobTracker());
    job3.getStatus().setRunState(JobStatus.RUNNING);
    taskTrackerManager.submitJob(job3);

    assertEquals(1,    defaultPool.getMapSchedulable().getDemand());
    assertEquals(3,    defaultPool.getReduceSchedulable().getDemand());
    assertEquals(16,   poolA.getMapSchedulable().getDemand());
    assertEquals(20,   poolA.getReduceSchedulable().getDemand());

    // Submit a job to poolA by specifying pool and not mapred.fairscheduler.pool
    JobConf jobConf2 = new JobConf(conf);
    jobConf2.setNumMapTasks(17);
    jobConf2.setNumReduceTasks(19);
    jobConf2.set(POOL_PROPERTY, "poolA");
    JobInProgress job4 = new FakeJobInProgress(jobConf2, taskTrackerManager,
        null, UtilsForTests.getJobTracker());
    job4.getStatus().setRunState(JobStatus.RUNNING);
    taskTrackerManager.submitJob(job4);

    assertEquals(1,    defaultPool.getMapSchedulable().getDemand());
    assertEquals(3,    defaultPool.getReduceSchedulable().getDemand());
    assertEquals(33,   poolA.getMapSchedulable().getDemand());
    assertEquals(39,   poolA.getReduceSchedulable().getDemand());
  }


  /**
   * Test switching a job from one pool to another, then back to the original
   * one. This is a regression test for a bug seen during development of
   * MAPREDUCE-2323 (fair scheduler metrics).
   */
  public void testSetPoolTwice() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"default\">");
    out.println("<schedulingMode>fair</schedulingMode>");
    out.println("</pool>");
    out.println("<pool name=\"poolA\">");
    out.println("<schedulingMode>fair</schedulingMode>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    Pool defaultPool = scheduler.getPoolManager().getPool("default");
    Pool poolA = scheduler.getPoolManager().getPool("poolA");

    // Submit a job to the default pool.  All specifications take default values.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 1, 3);
    assertEquals(1,    defaultPool.getMapSchedulable().getDemand());
    assertEquals(3,    defaultPool.getReduceSchedulable().getDemand());
    assertEquals(0,    poolA.getMapSchedulable().getDemand());
    assertEquals(0,    poolA.getReduceSchedulable().getDemand());

    // Move job to poolA and make sure demand moves with it
    scheduler.getPoolManager().setPool(job1, "poolA");
    assertEquals("poolA", scheduler.getPoolManager().getPoolName(job1));

    defaultPool.getMapSchedulable().updateDemand();
    defaultPool.getReduceSchedulable().updateDemand();
    poolA.getMapSchedulable().updateDemand();
    poolA.getReduceSchedulable().updateDemand();

    assertEquals(0,    defaultPool.getMapSchedulable().getDemand());
    assertEquals(0,    defaultPool.getReduceSchedulable().getDemand());
    assertEquals(1,    poolA.getMapSchedulable().getDemand());
    assertEquals(3,    poolA.getReduceSchedulable().getDemand());

    // Move back to default pool and make sure demand goes back
    scheduler.getPoolManager().setPool(job1, "default");
    assertEquals("default", scheduler.getPoolManager().getPoolName(job1));

    defaultPool.getMapSchedulable().updateDemand();
    defaultPool.getReduceSchedulable().updateDemand();
    poolA.getMapSchedulable().updateDemand();
    poolA.getReduceSchedulable().updateDemand();

    assertEquals(1,    defaultPool.getMapSchedulable().getDemand());
    assertEquals(3,    defaultPool.getReduceSchedulable().getDemand());
    assertEquals(0,    poolA.getMapSchedulable().getDemand());
    assertEquals(0,    poolA.getReduceSchedulable().getDemand());
  }
  
  private void advanceTime(long time) {
    clock.advance(time);
    scheduler.update();
  }

  protected TaskTracker tracker(String taskTrackerName) {
    return taskTrackerManager.getTaskTracker(taskTrackerName);
  }
  
  protected void checkAssignment(String taskTrackerName,
      String... expectedTasks) throws IOException {
    List<Task> tasks = scheduler.assignTasks(tracker(taskTrackerName));
    for (Task t : tasks) {
      taskTrackerManager.reportTaskOnTracker(taskTrackerName, t);
    }
    System.out.println("Assigned tasks:");
    for (int i = 0; i < tasks.size(); i++)
      System.out.println("- " + tasks.get(i));
    assertNotNull(tasks);
    assertEquals(expectedTasks.length, tasks.size());
    for (int i = 0; i < tasks.size(); i++)
      assertEquals("assignment " + i, expectedTasks[i], tasks.get(i).toString());
  }
  
  
  /**
   * Ask scheduler to update metrics and then verify that they're all
   * correctly published to the metrics context
   */
  private void verifyMetrics() {
    scheduler.updateMetrics();
    verifyPoolMetrics();
    verifyJobMetrics();
  }
  
  /**
   * Verify that pool-level metrics match internal data
   */
  private void verifyPoolMetrics() {
    MetricsContext ctx = MetricsUtil.getContext("fairscheduler");
    Collection<OutputRecord> records = ctx.getAllRecords().get("pools");

    try {
      assertEquals(scheduler.getPoolSchedulables(TaskType.MAP).size() * 2,
          records.size());
    } catch (Error e) {
      for (OutputRecord rec : records) {
        System.err.println("record:");
        System.err.println(" name: " + rec.getTag("name"));
        System.err.println(" type: " + rec.getTag("type"));
      }

      throw e;
    }
    
    Map<String, OutputRecord> byPoolAndType =
      new HashMap<String, OutputRecord>();
    for (OutputRecord rec : records) {
      String pool = (String)rec.getTag("name");
      String type = (String)rec.getTag("taskType");
      assertNotNull(pool);
      assertNotNull(type);
      byPoolAndType.put(pool + "_" + type, rec);
    }
    
    List<PoolSchedulable> poolScheds = new ArrayList<PoolSchedulable>();
    poolScheds.addAll(scheduler.getPoolSchedulables(TaskType.MAP));
    poolScheds.addAll(scheduler.getPoolSchedulables(TaskType.REDUCE));
    
    for (PoolSchedulable pool : poolScheds) {
      String poolName = pool.getName();
      OutputRecord metrics = byPoolAndType.get(
          poolName + "_" + pool.getTaskType().toString());
      assertNotNull("Need metrics for " + pool, metrics);
      
      verifySchedulableMetrics(pool, metrics);
    }
    
  }
  
  /**
   * Verify that the job-level metrics match internal data
   */
  private void verifyJobMetrics() {
    MetricsContext ctx = MetricsUtil.getContext("fairscheduler");
    Collection<OutputRecord> records = ctx.getAllRecords().get("jobs");
    
    System.out.println("Checking job metrics...");
    Map<String, OutputRecord> byJobIdAndType =
      new HashMap<String, OutputRecord>();
    for (OutputRecord rec : records) {
      String jobId = (String)rec.getTag("name");
      String type = (String)rec.getTag("taskType");
      assertNotNull(jobId);
      assertNotNull(type);
      byJobIdAndType.put(jobId + "_" + type, rec);
      System.out.println("Got " + type + " metrics for job: " + jobId);
    }
    assertEquals(scheduler.infos.size() * 2, byJobIdAndType.size());
    
    for (Map.Entry<JobInProgress, JobInfo> entry :
            scheduler.infos.entrySet()) {
      JobInfo info = entry.getValue();
      String jobId = entry.getKey().getJobID().toString();
      
      OutputRecord mapMetrics = byJobIdAndType.get(jobId + "_MAP");
      assertNotNull("Job " + jobId + " should have map metrics", mapMetrics);
      verifySchedulableMetrics(info.mapSchedulable, mapMetrics);
      
      OutputRecord reduceMetrics = byJobIdAndType.get(jobId + "_REDUCE");
      assertNotNull("Job " + jobId + " should have reduce metrics", reduceMetrics);
      verifySchedulableMetrics(info.reduceSchedulable, reduceMetrics);
    }
  }

  /**
   * Verify that the metrics for a given Schedulable are correct
   */
  private void verifySchedulableMetrics(
      Schedulable sched, OutputRecord metrics) {
    assertEquals(sched.getRunningTasks(), metrics.getMetric("runningTasks"));
    assertEquals(sched.getDemand(), metrics.getMetric("demand"));
    assertEquals(sched.getFairShare(),
        metrics.getMetric("fairShare").doubleValue(), .001);
    assertEquals(sched.getWeight(),
        metrics.getMetric("weight").doubleValue(), .001);
  }

  /**
   * This test submits a job that takes all 2 slots in a pool has both a min
   * share of 2 slots with minshare timeout of 5s, and then a second job in
   * default pool with a fair share timeout of 5s. After 60 seconds, this pool
   * will be starved of fair share (2 slots of each type), and we test that it
   * does not kill more than 2 tasks of each type.
   */
  public void testFairSharePreemptionWithShortTimeout() throws Exception {
    // Enable preemption in scheduler
    scheduler.preemptionEnabled = true;
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<fairSharePreemptionTimeout>5</fairSharePreemptionTimeout>");
    out.println("<pool name=\"pool1\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>2</minReduces>");
    out.println("<minSharePreemptionTimeout>5</minSharePreemptionTimeout>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    Pool pool1 = scheduler.getPoolManager().getPool("pool1");
    Pool defaultPool = scheduler.getPoolManager().getPool("default");

    // Submit job 1 and assign all slots to it. Sleep a bit before assigning
    // tasks on tt1 and tt2 to ensure that the ones on tt2 get preempted first.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10, "pool1");
    JobInfo info1 = scheduler.infos.get(job1);
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000003_0 on tt2");

    advanceTime(10000);
    assertEquals(4,    info1.mapSchedulable.getRunningTasks());
    assertEquals(4,    info1.reduceSchedulable.getRunningTasks());
    assertEquals(4.0,  info1.mapSchedulable.getFairShare());
    assertEquals(4.0,  info1.reduceSchedulable.getFairShare());
    // Ten seconds later, submit job 2.
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "default");

    // Advance time by 6 seconds without update the scheduler.
    // This simulates the time gap between update and task preemption.
    clock.advance(6000);
    assertEquals(4,    info1.mapSchedulable.getRunningTasks());
    assertEquals(4,    info1.reduceSchedulable.getRunningTasks());
    assertEquals(2.0,  info1.mapSchedulable.getFairShare());
    assertEquals(2.0,  info1.reduceSchedulable.getFairShare());
    assertEquals(0, scheduler.tasksToPreempt(pool1.getMapSchedulable(),
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(pool1.getReduceSchedulable(),
        clock.getTime()));
    assertEquals(2, scheduler.tasksToPreempt(defaultPool.getMapSchedulable(),
        clock.getTime()));
    assertEquals(2, scheduler.tasksToPreempt(defaultPool.getReduceSchedulable(),
        clock.getTime()));

    // Test that the tasks actually get preempted and we can assign new ones
    scheduler.preemptTasksIfNecessary();
    scheduler.update();
    assertEquals(2, job1.runningMaps());
    assertEquals(2, job1.runningReduces());
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
  }
}
