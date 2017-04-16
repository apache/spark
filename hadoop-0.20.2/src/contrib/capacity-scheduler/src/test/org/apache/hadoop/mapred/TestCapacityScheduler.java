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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.security.authorize.AccessControlList;

public class TestCapacityScheduler extends TestCase {

  static final Log LOG =
      LogFactory.getLog(org.apache.hadoop.mapred.TestCapacityScheduler.class);

  private static int jobCounter;

  /**
   * Test class that removes the asynchronous nature of job initialization.
   * 
   * The run method is a dummy which just waits for completion. It is
   * expected that test code calls the main method, initializeJobs, directly
   * to trigger initialization.
   */
  class ControlledJobInitializer extends 
                              JobInitializationPoller.JobInitializationThread {
    
    boolean stopRunning;
    
    public ControlledJobInitializer(JobInitializationPoller p) {
      p.super();
    }
    
    @Override
    public void run() {
      while (!stopRunning) {
        try {
          synchronized(this) {
            this.wait();  
          }
        } catch (InterruptedException ie) {
          break;
        }
      }
    }
    
    void stopRunning() {
      stopRunning = true;
    }
  }
  
  /**
   * Test class that removes the asynchronous nature of job initialization.
   * 
   * The run method is a dummy which just waits for completion. It is
   * expected that test code calls the main method, selectJobsToInitialize,
   * directly to trigger initialization.
   * 
   * The class also creates the test worker thread objects of type 
   * ControlledJobInitializer instead of the objects of the actual class
   */
  class ControlledInitializationPoller extends JobInitializationPoller {
    
    private boolean stopRunning;
    private ArrayList<ControlledJobInitializer> workers;
    
    public ControlledInitializationPoller(JobQueuesManager mgr,
                                          CapacitySchedulerConf rmConf,
                                          Set<String> queues,
                                          TaskTrackerManager ttm) {
      super(mgr, rmConf, queues, ttm);
    }
    
    @Override
    public void run() {
      // don't do anything here.
      while (!stopRunning) {
        try {
          synchronized (this) {
            this.wait();
          }
        } catch (InterruptedException ie) {
          break;
        }
      }
    }
    
    @Override
    JobInitializationThread createJobInitializationThread() {
      ControlledJobInitializer t = new ControlledJobInitializer(this);
      if (workers == null) {
        workers = new ArrayList<ControlledJobInitializer>();
      }
      workers.add(t);
      return t;
    }

    @Override
    void selectJobsToInitialize() {
      super.cleanUpInitializedJobsList();
      super.selectJobsToInitialize();
      for (ControlledJobInitializer t : workers) {
        t.initializeJobs();
      }
    }
    
    void stopRunning() {
      stopRunning = true;
      for (ControlledJobInitializer t : workers) {
        t.stopRunning();
        t.interrupt();
      }
    }
  }

  private ControlledInitializationPoller controlledInitializationPoller;
  /*
   * Fake job in progress object used for testing the schedulers scheduling
   * decisions. The JobInProgress objects returns out FakeTaskInProgress
   * objects when assignTasks is called. If speculative maps and reduces
   * are configured then JobInProgress returns exactly one Speculative
   * map and reduce task.
   */
  static class FakeJobInProgress extends JobInProgress {
    
    protected FakeTaskTrackerManager taskTrackerManager;
    private int mapTaskCtr;
    private int redTaskCtr;
    private Set<TaskInProgress> mapTips = 
      new HashSet<TaskInProgress>();
    private Set<TaskInProgress> reduceTips = 
      new HashSet<TaskInProgress>();
    private int speculativeMapTaskCounter = 0;
    private int speculativeReduceTaskCounter = 0;
    public FakeJobInProgress(JobID jId, JobConf jobConf,
        FakeTaskTrackerManager taskTrackerManager, String user, 
        JobTracker jt) throws IOException {
      super(jId, jobConf, jt);
      if (user == null) {
        user = "drwho";
      }
      this.taskTrackerManager = taskTrackerManager;
      this.startTime = System.currentTimeMillis();
      this.status.setJobPriority(JobPriority.NORMAL);
      this.status.setStartTime(startTime);
      if (null == jobConf.getQueueName()) {
        this.profile = new JobProfile(user, jId, 
            null, null, null);
      }
      else {
        this.profile = new JobProfile(user, jId, 
            null, null, null, jobConf.getQueueName());
      }
      mapTaskCtr = 0;
      redTaskCtr = 0;
    }
    
    @Override
    public synchronized void initTasks() throws IOException {
      getStatus().setRunState(JobStatus.RUNNING);
    }

    @Override
    public Task obtainNewLocalMapTask(final TaskTrackerStatus tts, int clusterSize,
        int ignored) throws IOException {
      return obtainNewMapTask(tts, clusterSize, ignored);
    }
    
    @Override
    public Task obtainNewNonLocalMapTask(final TaskTrackerStatus tts, 
        int clusterSize, int ignored) throws IOException {
      return obtainNewMapTask(tts, clusterSize, ignored);
    }
    
    @Override
    public Task obtainNewMapTask(final TaskTrackerStatus tts, int clusterSize,
        int ignored) throws IOException {
      boolean areAllMapsRunning = (mapTaskCtr == numMapTasks);
      if (areAllMapsRunning){
        if(!getJobConf().getMapSpeculativeExecution() || 
            speculativeMapTasks > 0) {
          return null;
        }
      }
      TaskAttemptID attemptId = getTaskAttemptID(true, areAllMapsRunning);
      Task task = new MapTask("", attemptId, 0, new JobSplit.TaskSplitIndex(), 
                              super.numSlotsPerMap) {
        @Override
        public String toString() {
          return String.format("%s on %s", getTaskID(), tts.getTrackerName());
        }
      };
      taskTrackerManager.startTask(tts.getTrackerName(), task);
      runningMapTasks++;
      // create a fake TIP and keep track of it
      FakeTaskInProgress mapTip = new FakeTaskInProgress(getJobID(), 
          getJobConf(), task, true, this);
      mapTip.taskStatus.setRunState(TaskStatus.State.RUNNING);
      if(areAllMapsRunning) {
        speculativeMapTasks++;
        //you have scheduled a speculative map. Now set all tips in the
        //map tips not to have speculative task.
        for(TaskInProgress t : mapTips) {
          if (t instanceof FakeTaskInProgress) {
            FakeTaskInProgress mt = (FakeTaskInProgress) t;
            mt.hasSpeculativeMap = false;
          }
        }
      } else {
        //add only non-speculative tips.
        mapTips.add(mapTip);
        //add the tips to the JobInProgress TIPS
        maps = mapTips.toArray(new TaskInProgress[mapTips.size()]);
      }
      return task;
    }

    @Override
    public Task obtainNewReduceTask(final TaskTrackerStatus tts,
        int clusterSize, int ignored) throws IOException {
      boolean areAllReducesRunning = (redTaskCtr == numReduceTasks);
      if (areAllReducesRunning){
        if(!getJobConf().getReduceSpeculativeExecution() || 
            speculativeReduceTasks > 0) {
          return null;
        }
      }
      TaskAttemptID attemptId = getTaskAttemptID(false, areAllReducesRunning);
      Task task = new ReduceTask("", attemptId, 0, 10, super.numSlotsPerReduce) {
        @Override
        public String toString() {
          return String.format("%s on %s", getTaskID(), tts.getTrackerName());
        }
      };
      taskTrackerManager.startTask(tts.getTrackerName(), task);
      runningReduceTasks++;
      // create a fake TIP and keep track of it
      FakeTaskInProgress reduceTip = new FakeTaskInProgress(getJobID(), 
          getJobConf(), task, false, this);
      reduceTip.taskStatus.setRunState(TaskStatus.State.RUNNING);
      if(areAllReducesRunning) {
        speculativeReduceTasks++;
        //you have scheduled a speculative map. Now set all tips in the
        //map tips not to have speculative task.
        for(TaskInProgress t : reduceTips) {
          if (t instanceof FakeTaskInProgress) {
            FakeTaskInProgress rt = (FakeTaskInProgress) t;
            rt.hasSpeculativeReduce = false;
          }
        }
      } else {
        //add only non-speculative tips.
        reduceTips.add(reduceTip);
        //add the tips to the JobInProgress TIPS
        reduces = reduceTips.toArray(new TaskInProgress[reduceTips.size()]);
      }
      return task;
    }
    
    public void mapTaskFinished() {
      runningMapTasks--;
      finishedMapTasks++;
    }
    
    public void reduceTaskFinished() {
      runningReduceTasks--;
      finishedReduceTasks++;
    }
    
    private TaskAttemptID getTaskAttemptID(boolean isMap, boolean isSpeculative) {
      JobID jobId = getJobID();
      if (!isSpeculative) {
        return new TaskAttemptID(jobId.getJtIdentifier(), jobId.getId(), isMap,
            (isMap) ? ++mapTaskCtr : ++redTaskCtr, 0);
      } else  {
        return new TaskAttemptID(jobId.getJtIdentifier(), jobId.getId(), isMap,
            (isMap) ? mapTaskCtr : redTaskCtr, 1);
      }
    }
    
    @Override
    Set<TaskInProgress> getNonLocalRunningMaps() {
      return (Set<TaskInProgress>)mapTips;
    }
    @Override
    Set<TaskInProgress> getRunningReduces() {
      return (Set<TaskInProgress>)reduceTips;
    }
    
  }
  
  static class FakeFailingJobInProgress extends FakeJobInProgress {

    public FakeFailingJobInProgress(JobID id, JobConf jobConf,
        FakeTaskTrackerManager taskTrackerManager, String user, 
        JobTracker jt) throws IOException {
      super(id, jobConf, taskTrackerManager, user, jt);
    }
    
    @Override
    public synchronized void initTasks() throws IOException {
      throw new IOException("Failed Initalization");
    }
    
    @Override
    synchronized void fail() {
      this.status.setRunState(JobStatus.FAILED);
    }
  }
 
  static class FakeTaskInProgress extends TaskInProgress {
    private boolean isMap;
    private FakeJobInProgress fakeJob;
    private TreeMap<TaskAttemptID, String> activeTasks;
    private TaskStatus taskStatus;
    boolean hasSpeculativeMap;
    boolean hasSpeculativeReduce;
    
    FakeTaskInProgress(JobID jId, JobConf jobConf, Task t, 
        boolean isMap, FakeJobInProgress job) {
      super(jId, "", JobSplit.EMPTY_TASK_SPLIT, job.jobtracker, jobConf, job, 
            0, 1);
      this.isMap = isMap;
      this.fakeJob = job;
      activeTasks = new TreeMap<TaskAttemptID, String>();
      activeTasks.put(t.getTaskID(), "tt");
      // create a fake status for a task that is running for a bit
      this.taskStatus = TaskStatus.createTaskStatus(isMap);
      taskStatus.setProgress(0.5f);
      taskStatus.setRunState(TaskStatus.State.RUNNING);
      if (jobConf.getMapSpeculativeExecution()) {
        //resetting of the hasSpeculativeMap is done
        //when speculative map is scheduled by the job.
        hasSpeculativeMap = true;
      } 
      if (jobConf.getReduceSpeculativeExecution()) {
        //resetting of the hasSpeculativeReduce is done
        //when speculative reduce is scheduled by the job.
        hasSpeculativeReduce = true;
      }
    }
    
    @Override
    TreeMap<TaskAttemptID, String> getActiveTasks() {
      return activeTasks;
    }
    @Override
    public TaskStatus getTaskStatus(TaskAttemptID taskid) {
      // return a status for a task that has run a bit
      return taskStatus;
    }
    @Override
    boolean killTask(TaskAttemptID taskId, boolean shouldFail) {
      if (isMap) {
        fakeJob.mapTaskFinished();
      }
      else {
        fakeJob.reduceTaskFinished();
      }
      return true;
    }
    
    @Override
    /*
     *hasSpeculativeMap and hasSpeculativeReduce is reset by FakeJobInProgress
     *after the speculative tip has been scheduled.
     */
    boolean hasSpeculativeTask(long currentTime, double averageProgress) {
      if(isMap && hasSpeculativeMap) {
        return fakeJob.getJobConf().getMapSpeculativeExecution();
      } 
      if (!isMap && hasSpeculativeReduce) {
        return fakeJob.getJobConf().getReduceSpeculativeExecution();
      }
      return false;
    }
    
    @Override
    public boolean isRunning() {
      return !activeTasks.isEmpty();
    }
    
  }
  
  static class FakeQueueManager extends QueueManager {
    private static final Map<String,AccessControlList> acls =
      new HashMap<String,AccessControlList>() {
        final AccessControlList allEnabledAcl = new AccessControlList("*");
        @Override
        public AccessControlList get(Object key) {
          return allEnabledAcl;
        }
      };
    FakeQueueManager() {
      super(new Configuration());
    }
    void setQueues(Set<String> newQueues) {
      queues.clear();
      for (String qName : newQueues) {
        try {
          queues.put(qName, new Queue(qName, acls, Queue.QueueState.RUNNING));
        } catch (Throwable t) {
          throw new RuntimeException("Unable to initialize queue " + qName, t);
        }
      }
    }
  }
  
  static class FakeTaskTrackerManager implements TaskTrackerManager {
    int maps = 0;
    int reduces = 0;
    int maxMapTasksPerTracker = 2;
    int maxReduceTasksPerTracker = 1;
    List<JobInProgressListener> mylisteners =
      new ArrayList<JobInProgressListener>();
    FakeQueueManager qm = new FakeQueueManager();
    
    private Map<String, TaskTracker> trackers =
      new HashMap<String, TaskTracker>();
    private Map<String, TaskStatus> taskStatuses = 
      new HashMap<String, TaskStatus>();
    private Map<JobID, JobInProgress> jobs =
        new HashMap<JobID, JobInProgress>();

    public FakeTaskTrackerManager() {
      this(2, 2, 1);
    }

    public FakeTaskTrackerManager(int numTaskTrackers,
        int maxMapTasksPerTracker, int maxReduceTasksPerTracker) {
      this.maxMapTasksPerTracker = maxMapTasksPerTracker;
      this.maxReduceTasksPerTracker = maxReduceTasksPerTracker;
      for (int i = 1; i < numTaskTrackers + 1; i++) {
        String ttName = "tt" + i;
        TaskTracker tt = new TaskTracker(ttName);
        tt.setStatus(new TaskTrackerStatus(ttName, ttName + ".host", i,
                                           new ArrayList<TaskStatus>(), 0, 0,
                                           maxMapTasksPerTracker,
                                           maxReduceTasksPerTracker));
        trackers.put(ttName, tt);
      }
    }
    
    public void addTaskTracker(String ttName) {
      TaskTracker tt = new TaskTracker(ttName);
      tt.setStatus(new TaskTrackerStatus(ttName, ttName + ".host", 1,
                                         new ArrayList<TaskStatus>(), 0, 0,
                                         maxMapTasksPerTracker, 
                                         maxReduceTasksPerTracker));
      trackers.put(ttName, tt);
    }

    public void addTaskTracker(String ttName,
        int maxMapTasksPerTracker,
        int maxReduceTasksPerTracker) {
      TaskTracker tt = new TaskTracker(ttName);
      tt.setStatus(new TaskTrackerStatus(ttName, ttName + ".host", 1,
                                         new ArrayList<TaskStatus>(), 0, 0,
                                         maxMapTasksPerTracker,
                                         maxReduceTasksPerTracker));
      trackers.put(ttName, tt);
    }

    public ClusterStatus getClusterStatus() {
      int numTrackers = trackers.size();
      return new ClusterStatus(numTrackers, maps, reduces,
          numTrackers * maxMapTasksPerTracker,
          numTrackers * maxReduceTasksPerTracker,
          JobTracker.State.RUNNING);
    }

    public int getNumberOfUniqueHosts() {
      return 0;
    }

    public int getNextHeartbeatInterval() {
      return MRConstants.HEARTBEAT_INTERVAL_MIN_DEFAULT;
    }

    @Override
    public void killJob(JobID jobid) throws IOException {
      JobInProgress job = jobs.get(jobid);
      finalizeJob(job, JobStatus.KILLED);
      job.kill();
    }

    @Override
    public synchronized void failJob(JobInProgress job) {
      finalizeJob(job, JobStatus.FAILED);
      job.fail();
    }
    
    public void initJob(JobInProgress jip) {
      try {
        JobStatus oldStatus = (JobStatus)jip.getStatus().clone();
        jip.initTasks();
        JobStatus newStatus = (JobStatus)jip.getStatus().clone();
        JobStatusChangeEvent event = new JobStatusChangeEvent(jip, 
            EventType.RUN_STATE_CHANGED, oldStatus, newStatus);
        for (JobInProgressListener listener : mylisteners) {
          listener.jobUpdated(event);
        }
      } catch (Exception ioe) {
        failJob(jip);
      }
    }
    
    public void removeJob(JobID jobid) {
      jobs.remove(jobid);
    }
    
    @Override
    public JobInProgress getJob(JobID jobid) {
      return jobs.get(jobid);
    }

    Collection<JobInProgress> getJobs() {
      return jobs.values();
    }

    public Collection<TaskTrackerStatus> taskTrackers() {
      List<TaskTrackerStatus> statuses = new ArrayList<TaskTrackerStatus>();
      for (TaskTracker tt : trackers.values()) {
        statuses.add(tt.getStatus());
      }
      return statuses;
    }


    public void addJobInProgressListener(JobInProgressListener listener) {
      mylisteners.add(listener);
    }

    public void removeJobInProgressListener(JobInProgressListener listener) {
      mylisteners.remove(listener);
    }
    
    public void submitJob(JobInProgress job) throws IOException {
      jobs.put(job.getJobID(), job);
      for (JobInProgressListener listener : mylisteners) {
        listener.jobAdded(job);
      }
    }
    
    public TaskTracker getTaskTracker(String trackerID) {
      return trackers.get(trackerID);
    }
    
    public void startTask(String taskTrackerName, final Task t) {
      if (t.isMapTask()) {
        maps++;
      } else {
        reduces++;
      }
      TaskStatus status = new TaskStatus() {
        @Override
        public TaskAttemptID getTaskID() {
          return t.getTaskID();
        }

        @Override
        public boolean getIsMap() {
          return t.isMapTask();
        }
        
        @Override
        public int getNumSlots() {
          return t.getNumSlotsRequired();
        }
      };
      taskStatuses.put(t.getTaskID().toString(), status);
      status.setRunState(TaskStatus.State.RUNNING);
      trackers.get(taskTrackerName).getStatus().getTaskReports().add(status);
    }
    
    public void finishTask(String taskTrackerName, String tipId, 
        FakeJobInProgress j) {
      TaskStatus status = taskStatuses.get(tipId);
      if (status.getIsMap()) {
        maps--;
        j.mapTaskFinished();
      } else {
        reduces--;
        j.reduceTaskFinished();
      }
      status.setRunState(TaskStatus.State.SUCCEEDED);
    }
    
    void finalizeJob(FakeJobInProgress fjob) {
      finalizeJob(fjob, JobStatus.SUCCEEDED);
    }

    void finalizeJob(JobInProgress fjob, int state) {
      // take a snapshot of the status before changing it
      JobStatus oldStatus = (JobStatus)fjob.getStatus().clone();
      fjob.getStatus().setRunState(state);
      JobStatus newStatus = (JobStatus)fjob.getStatus().clone();
      JobStatusChangeEvent event = 
        new JobStatusChangeEvent (fjob, EventType.RUN_STATE_CHANGED, oldStatus, 
                                  newStatus);
      for (JobInProgressListener listener : mylisteners) {
        listener.jobUpdated(event);
      }
    }
    
    public void setPriority(FakeJobInProgress fjob, JobPriority priority) {
      // take a snapshot of the status before changing it
      JobStatus oldStatus = (JobStatus)fjob.getStatus().clone();
      fjob.setPriority(priority);
      JobStatus newStatus = (JobStatus)fjob.getStatus().clone();
      JobStatusChangeEvent event = 
        new JobStatusChangeEvent (fjob, EventType.PRIORITY_CHANGED, oldStatus, 
                                  newStatus);
      for (JobInProgressListener listener : mylisteners) {
        listener.jobUpdated(event);
      }
    }
    
    public void setStartTime(FakeJobInProgress fjob, long start) {
      // take a snapshot of the status before changing it
      JobStatus oldStatus = (JobStatus)fjob.getStatus().clone();
      
      fjob.startTime = start; // change the start time of the job
      fjob.status.setStartTime(start); // change the start time of the jobstatus
      
      JobStatus newStatus = (JobStatus)fjob.getStatus().clone();
      
      JobStatusChangeEvent event = 
        new JobStatusChangeEvent (fjob, EventType.START_TIME_CHANGED, oldStatus,
                                  newStatus);
      for (JobInProgressListener listener : mylisteners) {
        listener.jobUpdated(event);
      }
    }
    
    void addQueues(String[] arr) {
      Set<String> queues = new HashSet<String>();
      for (String s: arr) {
        queues.add(s);
      }
      qm.setQueues(queues);
    }
    
    public QueueManager getQueueManager() {
      return qm;
    }

    @Override
    public boolean killTask(TaskAttemptID taskid, boolean shouldFail) {
      return true;
    }
  }
  
  // represents a fake queue configuration info
  static class FakeQueueInfo {
    String queueName;
    float capacity;
    boolean supportsPrio;
    int ulMin;

    public FakeQueueInfo(String queueName, float capacity, boolean supportsPrio, int ulMin) {
      this.queueName = queueName;
      this.capacity = capacity;
      this.supportsPrio = supportsPrio;
      this.ulMin = ulMin;
    }
  }
  
  static class FakeResourceManagerConf extends CapacitySchedulerConf {
  
    // map of queue names to queue info
    private Map<String, FakeQueueInfo> queueMap = 
      new LinkedHashMap<String, FakeQueueInfo>();
    String firstQueue;
    
    
    void setFakeQueues(List<FakeQueueInfo> queues) {
      for (FakeQueueInfo q: queues) {
        queueMap.put(q.queueName, q);
      }
      firstQueue = new String(queues.get(0).queueName);
    }
    
    public synchronized Set<String> getQueues() {
      return queueMap.keySet();
    }
    
    /*public synchronized String getFirstQueue() {
      return firstQueue;
    }*/
    
    public float getCapacity(String queue) {
      if(queueMap.get(queue).capacity == -1) {
        return super.getCapacity(queue);
      }
      return queueMap.get(queue).capacity;
    }
    
    public int getMinimumUserLimitPercent(String queue) {
      return queueMap.get(queue).ulMin;
    }
    
    public boolean isPrioritySupported(String queue) {
      return queueMap.get(queue).supportsPrio;
    }
    
    @Override
    public long getSleepInterval() {
      return 1;
    }
    
    @Override
    public int getMaxWorkerThreads() {
      return 1;
    }
  }

  protected class FakeClock extends CapacityTaskScheduler.Clock {
    private long time = 0;
    
    public void advance(long millis) {
      time += millis;
    }

    @Override
    long getTime() {
      return time;
    }
  }

  
  protected JobConf conf;
  protected CapacityTaskScheduler scheduler;
  private FakeTaskTrackerManager taskTrackerManager;
  private FakeResourceManagerConf resConf;
  private FakeClock clock;

  @Override
  protected void setUp() {
    setUp(2, 2, 1);
  }

  private void setUp(int numTaskTrackers, int numMapTasksPerTracker,
      int numReduceTasksPerTracker) {
    jobCounter = 0;
    taskTrackerManager =
        new FakeTaskTrackerManager(numTaskTrackers, numMapTasksPerTracker,
            numReduceTasksPerTracker);
    clock = new FakeClock();
    scheduler = new CapacityTaskScheduler(clock);
    scheduler.setTaskTrackerManager(taskTrackerManager);

    conf = new JobConf();
    // Don't let the JobInitializationPoller come in our way.
    resConf = new FakeResourceManagerConf();
    controlledInitializationPoller = new ControlledInitializationPoller(
        scheduler.jobQueuesManager,
        resConf,
        resConf.getQueues(), taskTrackerManager);
    scheduler.setInitializationPoller(controlledInitializationPoller);
    scheduler.setConf(conf);
    //by default disable speculative execution.
    conf.setMapSpeculativeExecution(false);
    conf.setReduceSpeculativeExecution(false);
  }
  
  @Override
  protected void tearDown() throws Exception {
    if (scheduler != null) {
      scheduler.terminate();
    }
  }

  private FakeJobInProgress submitJob(int state, JobConf jobConf) throws IOException {
    FakeJobInProgress job =
        new FakeJobInProgress(new JobID("test", ++jobCounter),
            (jobConf == null ? new JobConf(conf) : jobConf), taskTrackerManager,
            jobConf.getUser(), UtilsForTests.getJobTracker());
    job.getStatus().setRunState(state);
    taskTrackerManager.submitJob(job);
    return job;
  }

  private FakeJobInProgress submitJobAndInit(int state, JobConf jobConf)
      throws IOException {
    FakeJobInProgress j = submitJob(state, jobConf);
    taskTrackerManager.initJob(j);
    return j;
  }

  private FakeJobInProgress submitJob(int state, int maps, int reduces, 
      String queue, String user) throws IOException {
    JobConf jobConf = new JobConf(conf);
    jobConf.setNumMapTasks(maps);
    jobConf.setNumReduceTasks(reduces);
    if (queue != null)
      jobConf.setQueueName(queue);
    jobConf.setUser(user);
    return submitJob(state, jobConf);
  }
  
  // Submit a job and update the listeners
  private FakeJobInProgress submitJobAndInit(int state, int maps, int reduces,
                                             String queue, String user) 
  throws IOException {
    FakeJobInProgress j = submitJob(state, maps, reduces, queue, user);
    taskTrackerManager.initJob(j);
    return j;
  }
  
  // test job run-state change
  public void testJobRunStateChange() throws IOException {
    // start the scheduler
    taskTrackerManager.addQueues(new String[] {"default"});
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 1));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();
    
    // submit the job
    FakeJobInProgress fjob1 = 
      submitJob(JobStatus.PREP, 1, 0, "default", "user");
    
    FakeJobInProgress fjob2 = 
      submitJob(JobStatus.PREP, 1, 0, "default", "user");
    
    // test if changing the job priority/start-time works as expected in the 
    // waiting queue
    testJobOrderChange(fjob1, fjob2, true);
    
    // Init the jobs
    // simulate the case where the job with a lower priority becomes running 
    // first (may be because of the setup tasks).
    
    // init the lower ranked job first
    taskTrackerManager.initJob(fjob2);
    
    // init the higher ordered job later
    taskTrackerManager.initJob(fjob1);
    
    // check if the jobs are missing from the waiting queue
    // The jobs are not removed from waiting queue until they are scheduled
    assertEquals("Waiting queue is garbled on job init", 2, 
                 scheduler.jobQueuesManager.getQueue("default").getWaitingJobs()
                          .size());
    
    // test if changing the job priority/start-time works as expected in the 
    // running queue
    testJobOrderChange(fjob1, fjob2, false);
    
    // schedule a task
    List<Task> tasks = scheduler.assignTasks(tracker("tt1"));
    
    // complete the job
    taskTrackerManager.finishTask("tt1", tasks.get(0).getTaskID().toString(), 
                                  fjob1);
    
    // mark the job as complete
    taskTrackerManager.finalizeJob(fjob1);
    
    CapacitySchedulerQueue queue = 
      scheduler.jobQueuesManager.getQueue("default"); 
    Collection<JobInProgress> rqueue = queue.getRunningJobs();
    
    // check if the job is removed from the scheduler
    assertFalse("Scheduler contains completed job", 
                rqueue.contains(fjob1));
    
    // check if the running queue size is correct
    assertEquals("Job finish garbles the queue", 
                 1, rqueue.size());

  }

  /**
   * Test the max Capacity for map and reduce
   * @throws IOException
   */
  public void testMaxCapacities() throws IOException {
    System.err.println("testMaxCapacities");
    this.setUp(4,1,1);
    taskTrackerManager.addQueues(new String[] {"default", "q2"});
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 25.0f, false, 1));
    queues.add(new FakeQueueInfo("q2", 75.0f, false, 1));
    
    resConf.setFakeQueues(queues);
    resConf.setMaxCapacity("default", 50.0f);
    resConf.setUserLimitFactor("default", 2);
    
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    //submit the Job
    FakeJobInProgress fjob1 =
      submitJobAndInit(JobStatus.PREP, 4, 4, "default", "user");

    //default queue has min capacity of 1 and max capacity of 2

    //first call of assign task should give task from default queue.
    //default uses 1 map and 1 reduce slots are used
    checkAssignments("tt1", 
        new String[] {
        "attempt_test_0001_m_000001_0 on tt1",
        "attempt_test_0001_r_000001_0 on tt1"});

    //second call of assign task
    //default uses 2 map and 2 reduce slots
    checkAssignments("tt2", 
        new String[] {
        "attempt_test_0001_m_000002_0 on tt2",
        "attempt_test_0001_r_000002_0 on tt2"});


    //Now we have reached the max capacity limit for default ,
    //no further tasks would be assigned to this queue.
    checkAssignments("tt3", new String[] {});
  }
  
  // test if the queue reflects the changes
  private void testJobOrderChange(FakeJobInProgress fjob1, 
                                  FakeJobInProgress fjob2, 
                                  boolean waiting) {
    String queueName = waiting ? "waiting" : "running";
    
    // check if the jobs in the queue are the right order
    JobInProgress[] jobs = getJobsInQueue(waiting);
    assertTrue(queueName + " queue doesnt contain job #1 in right order", 
                jobs[0].getJobID().equals(fjob1.getJobID()));
    assertTrue(queueName + " queue doesnt contain job #2 in right order", 
                jobs[1].getJobID().equals(fjob2.getJobID()));
    
    // I. Check the start-time change
    // Change job2 start-time and check if job2 bumps up in the queue 
    taskTrackerManager.setStartTime(fjob2, fjob1.startTime - 1);
    
    jobs = getJobsInQueue(waiting);
    assertTrue("Start time change didnt not work as expected for job #2 in "
               + queueName + " queue", 
                jobs[0].getJobID().equals(fjob2.getJobID()));
    assertTrue("Start time change didnt not work as expected for job #1 in"
               + queueName + " queue", 
                jobs[1].getJobID().equals(fjob1.getJobID()));
    
    // check if the queue is fine
    assertEquals("Start-time change garbled the " + queueName + " queue", 
                 2, jobs.length);
    
    // II. Change job priority change
    // Bump up job1's priority and make sure job1 bumps up in the queue
    taskTrackerManager.setPriority(fjob1, JobPriority.HIGH);
    
    // Check if the priority changes are reflected
    jobs = getJobsInQueue(waiting);
    assertTrue("Priority change didnt not work as expected for job #1 in "
               + queueName + " queue",  
                jobs[0].getJobID().equals(fjob1.getJobID()));
    assertTrue("Priority change didnt not work as expected for job #2 in "
               + queueName + " queue",  
                jobs[1].getJobID().equals(fjob2.getJobID()));
    
    // check if the queue is fine
    assertEquals("Priority change has garbled the " + queueName + " queue", 
                 2, jobs.length);
    
    // reset the queue state back to normal
    taskTrackerManager.setStartTime(fjob1, fjob2.startTime - 1);
    taskTrackerManager.setPriority(fjob1, JobPriority.NORMAL);
  }
  
  private JobInProgress[] getJobsInQueue(boolean waiting) {
    Collection<JobInProgress> queue = 
      waiting 
      ? scheduler.jobQueuesManager.getQueue("default").getWaitingJobs()
      : scheduler.jobQueuesManager.getQueue("default").getRunningJobs();
    return queue.toArray(new JobInProgress[0]);
  }
  
  // tests if tasks can be assinged when there are multiple jobs from a same
  // user
  public void testJobFinished() throws Exception {
    taskTrackerManager.addQueues(new String[] {"default", "q2"});
    
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit 2 jobs
    FakeJobInProgress j1 = submitJobAndInit(JobStatus.PREP, 3, 0, "default", "u1");
    FakeJobInProgress j2 = submitJobAndInit(JobStatus.PREP, 3, 0, "default", "u1");
    
    // I. Check multiple assignments with running tasks within job
    // ask for a task from first job
    checkAssignments("tt1", 
                     new String[] {"attempt_test_0001_m_000001_0 on tt1", 
                                   "attempt_test_0001_m_000002_0 on tt1"}
      );

    // complete tasks
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000001_0", j1);
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000002_0", j1);
    
    // II. Check multiple assignments with running tasks across jobs
    // ask for a task from first job
    checkAssignments("tt1", 
                     new String[] {"attempt_test_0001_m_000003_0 on tt1", 
                                   "attempt_test_0002_m_000001_0 on tt1"}
    );

    // complete task from job1
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000003_0", j1);
    
    // III. Check multiple assignments with completed tasks across jobs
    // ask for a task from the second job
    checkAssignment("tt1", "attempt_test_0002_m_000002_0 on tt1");
    
    // complete tasks
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_m_000002_0", j2);
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_m_000001_0", j2);
    
    // IV. Check assignment with completed job
    // finish first job
    scheduler.jobCompleted(j1);
    
    // ask for another task from the second job
    // if tasks can be assigned then the structures are properly updated 
    checkAssignment("tt1", "attempt_test_0002_m_000003_0 on tt1");
    
    // complete task
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_m_000003_0", j2);
  }

  /**
   * Tests whether a map and reduce task are assigned when there's
   * a single queue and multiple task assignment is enabled.
   * @throws Exception
   */
  public void testMultiTaskAssignmentInSingleQueue() throws Exception {
      setUp(1, 6, 2);
      // set up some queues
      String[] qs = {"default"};
      taskTrackerManager.addQueues(qs);
      ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
      queues.add(new FakeQueueInfo("default", 100.0f, true, 25));
      resConf.setFakeQueues(queues);
      scheduler.setResourceManagerConf(resConf);
      scheduler.start();

      //Submit the job with 6 maps and 2 reduces
      FakeJobInProgress j1 = submitJobAndInit(
        JobStatus.PREP, 6, 2, "default", "u1");

      List<Task> tasks = scheduler.assignTasks(tracker("tt1"));
      assertEquals(tasks.size(), 7);

      for (Task task : tasks) {
        if (task.toString().equals("attempt_test_0001_m_000001_0 on tt1")) {
          //Now finish the task
          taskTrackerManager.finishTask(
            "tt1", task.getTaskID().toString(),
            j1);
        }
      }

      // Only 1 reduce left
      tasks = scheduler.assignTasks(tracker("tt1"));
      assertEquals(tasks.size(), 1);
  }

  public void testMultiTaskAssignmentInMultipleQueues() throws Exception {
      setUp(1, 4, 2);
      // set up some queues
      String[] qs = {"q1","q2"};
      taskTrackerManager.addQueues(qs);
      ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
      queues.add(new FakeQueueInfo("q1", 50.0f, true, 25));
      queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));
      resConf.setFakeQueues(queues);
      resConf.setUserLimitFactor("q1", 4);
      resConf.setUserLimitFactor("q2", 4);
      scheduler.setResourceManagerConf(resConf);
      scheduler.start();

      System.err.println("testMultiTaskAssignmentInMultipleQueues");
      //Submit the job with 6 maps and 2 reduces
      FakeJobInProgress j1 = 
        submitJobAndInit(JobStatus.PREP, 6, 1, "q1", "u1");
      FakeJobInProgress j2 = 
        submitJobAndInit(JobStatus.PREP,2,1,"q2","u2");

      List<Task> tasks = checkAssignments("tt1", 
          new String[] {"attempt_test_0002_m_000001_0 on tt1",
                        "attempt_test_0001_m_000001_0 on tt1",
                        "attempt_test_0001_m_000002_0 on tt1",
                        "attempt_test_0002_m_000002_0 on tt1",
                        "attempt_test_0002_r_000001_0 on tt1",
                        });
      //Now finish the tasks
      for (Task task : tasks) {
        FakeJobInProgress j = 
          (task.getTaskID().getJobID().getId() == 1) ? j1 : j2;
        taskTrackerManager.finishTask("tt1", task.getTaskID().toString(), j);
      }

      checkAssignments("tt1", 
          new String[] {"attempt_test_0001_m_000003_0 on tt1",
                        "attempt_test_0001_m_000004_0 on tt1",
                        "attempt_test_0001_m_000005_0 on tt1",
                        "attempt_test_0001_m_000006_0 on tt1",
                        "attempt_test_0001_r_000001_0 on tt1",
                        });
  }

  // basic tests, should be able to submit to queues
  public void testSubmitToQueues() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit a job with no queue specified. It should be accepted
    // and given to the default queue. 
    JobInProgress j = submitJobAndInit(JobStatus.PREP, 10, 10, null, "u1");
    
    // when we ask for a task, we should get one, from the job submitted
    checkAssignments("tt1", 
        new String[] {"attempt_test_0001_m_000001_0 on tt1", 
                      "attempt_test_0001_m_000002_0 on tt1", 
                      "attempt_test_0001_r_000001_0 on tt1"});
    // submit another job, to a different queue
    j = submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u1");
    // now when we get a task, it should be from the second job
    checkAssignments("tt2", 
        new String[] {"attempt_test_0002_m_000001_0 on tt2", 
        "attempt_test_0002_m_000002_0 on tt2", 
        "attempt_test_0002_r_000001_0 on tt2"});
  }
  
  public void testGetJobs() throws Exception {
    // need only one queue
    String[] qs = { "default" };
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();
    HashMap<String, ArrayList<FakeJobInProgress>> subJobsList = 
      submitJobs(1, 4, "default");
   
    JobQueuesManager mgr = scheduler.jobQueuesManager;
    CapacitySchedulerQueue queue = mgr.getQueue("default");
    while(queue.getWaitingJobs().size() < 4){
      Thread.sleep(1);
    }
    //Raise status change events for jobs submitted.
    raiseStatusChangeEvents(mgr);
    Collection<JobInProgress> jobs = scheduler.getJobs("default");
    
    assertTrue("Number of jobs returned by scheduler is wrong" 
        ,jobs.size() == 4);
    
    assertTrue("Submitted jobs and Returned jobs are not same",
        subJobsList.get("u1").containsAll(jobs));
  }
  
  public void testCapacityAllocFailureWithLowerMaxCapacity()
    throws Exception {
    String[] qs = {"default", "q1"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 50));
    queues.add(new FakeQueueInfo("q1", 50.0f, true, 50));
    resConf.setFakeQueues(queues);
    resConf.setMaxCapacity("q1", 40.0f);
    scheduler.setResourceManagerConf(resConf);
    boolean failed = false;
    try {
      scheduler.start();
      fail("Scheduler start should fail ");
    } catch (IllegalArgumentException iae) {
      failed = true;  
    }
    assertTrue("Scheduler start didn't fail!", failed);
  }

  // Tests how capacity is computed and assignment of tasks done
  // on the basis of the capacity.
  public void testCapacityBasedAllocation() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    // set the capacity % as 10%, so that capacity will be zero initially as 
    // the cluster capacity increase slowly.
    queues.add(new FakeQueueInfo("default", 10.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 90.0f, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();
   
    // submit a job to the default queue
    submitJobAndInit(JobStatus.PREP, 10, 0, "default", "u1");
    
    // submit a job to the second queue
    submitJobAndInit(JobStatus.PREP, 10, 0, "q2", "u1");
    
    // job from q2 runs first because it has some non-zero capacity.
    checkAssignments("tt1", 
        new String[] {"attempt_test_0002_m_000001_0 on tt1", 
        "attempt_test_0002_m_000002_0 on tt1"});
    verifyCapacity("0", "default");
    verifyCapacity("3", "q2");
    
    // add another tt to increase tt slots
    taskTrackerManager.addTaskTracker("tt3");
    checkAssignments("tt2", 
        new String[] {"attempt_test_0002_m_000003_0 on tt2", 
        "attempt_test_0002_m_000004_0 on tt2"});
    verifyCapacity("0", "default");
    verifyCapacity("5", "q2");
    
    // add another tt to increase tt slots
    taskTrackerManager.addTaskTracker("tt4");
    checkAssignments("tt3", 
        new String[] {"attempt_test_0002_m_000005_0 on tt3", 
        "attempt_test_0002_m_000006_0 on tt3"});
    verifyCapacity("0", "default");
    verifyCapacity("7", "q2");
    
    // add another tt to increase tt slots
    taskTrackerManager.addTaskTracker("tt5");
    // now job from default should run, as it is furthest away
    // in terms of runningMaps / capacity.
    checkAssignments("tt4", 
        new String[] {"attempt_test_0001_m_000001_0 on tt4", 
        "attempt_test_0002_m_000007_0 on tt4"});
    verifyCapacity("1", "default");
    verifyCapacity("9", "q2");
  }
  
  private void verifyCapacity(String expectedCapacity,
                                          String queue) throws IOException {
    String schedInfo = taskTrackerManager.getQueueManager().
                          getSchedulerInfo(queue).toString();    
    assertTrue(schedInfo, schedInfo.contains("Map tasks\nCapacity: " 
        + expectedCapacity + " slots"));
  }
  
  // test capacity transfer
  public void testCapacityTransfer() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));
    resConf.setUserLimitFactor("default", 4);
    resConf.setUserLimitFactor("q2", 4);
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit a job  
    submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u1");
    // for queue 'q2', the capacity for maps is 2. Since we're the only user,
    // we should get 2 task 
    checkAssignments("tt1", 
        new String[] {"attempt_test_0001_m_000001_0 on tt1", 
        "attempt_test_0001_m_000002_0 on tt1", 
        "attempt_test_0001_r_000001_0 on tt1"});

    // Now we're at full capacity for maps. If I ask for another map task,
    // I should get a map task from the default queue's capacity. 
    checkAssignments("tt2", 
        new String[] {"attempt_test_0001_m_000003_0 on tt2", 
        "attempt_test_0001_m_000004_0 on tt2", 
        "attempt_test_0001_r_000002_0 on tt2"});
  }

  /**
   * Creates a queue with max capacity  of 50%
   * submit 1 job in the queue which is high ram(2 slots) . As 2 slots are
   * given to high ram job and are reserved , no other tasks are accepted .
   *
   * @throws IOException
   */
  public void testHighMemoryBlockingWithMaxCapacity()
      throws IOException {

    final int NUM_MAP_SLOTS = 2;
    final int NUM_REDUCE_SLOTS = 2;
    taskTrackerManager = 
      new FakeTaskTrackerManager(2, NUM_MAP_SLOTS, NUM_REDUCE_SLOTS);

    taskTrackerManager.addQueues(new String[] { "defaultXYZ", "q2" });
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("defaultXYZ", 25.0f, true, 50));
    queues.add(new FakeQueueInfo("q2", 75.0f, true, 50));
    resConf.setFakeQueues(queues);
    resConf.setUserLimitFactor("defaultXYZ", 2);

    //defaultXYZ can go up to 2 map and 2 reduce slots
    resConf.setMaxCapacity("defaultXYZ", 50.0f);

    scheduler.setTaskTrackerManager(taskTrackerManager);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY,
        2 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY,
        2 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    JobConf jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(1);
    jConf.setQueueName("defaultXYZ");
    jConf.setUser("u1");
    FakeJobInProgress job1 = submitJobAndInit(JobStatus.PREP, jConf);

    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(2 * 1024);
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(2);
    jConf.setQueueName("defaultXYZ");
    jConf.setUser("u1");
    FakeJobInProgress job2 = submitJobAndInit(JobStatus.PREP, jConf);


    //high ram map from job 1 and normal reduce task from job 1
    List<Task> tasks = checkAssignments("tt1", 
        new String[] {
        "attempt_test_0001_m_000001_0 on tt1",
        "attempt_test_0001_r_000001_0 on tt1"});

    checkOccupiedSlots("defaultXYZ", TaskType.MAP, 1, 2, 200.0f,1,0);
    checkOccupiedSlots("defaultXYZ", TaskType.REDUCE, 1, 1, 100.0f,0,2);
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 1 * 1024L, 
                                 NUM_MAP_SLOTS-2, NUM_REDUCE_SLOTS-1);

    //we have reached the maximum limit for map, so no more map tasks.
    //we have used 1 reduce already and 1 more reduce slot is left for the
    //before we reach maxcapacity for reduces.
    // But current 1 slot + 2 slots for high ram reduce would
    //mean we are crossing the maxium capacity.hence nothing would be assigned
    //in this call
    checkMultipleAssignment("tt2",null,null);

    //complete the high ram job on tt1.
    for (Task task : tasks) {
      taskTrackerManager.finishTask(
        "tt1", task.getTaskID().toString(),
        job1);
    }

    //At this point we have 1 high ram map and 1 high ram reduce.
    List<Task> t2 = checkMultipleAssignment(
      "tt2", "attempt_test_0001_m_000002_0 on tt2",
      "attempt_test_0002_r_000001_0 on tt2");

    checkOccupiedSlots("defaultXYZ", TaskType.MAP, 1, 2, 200.0f,1,0);
    checkOccupiedSlots("defaultXYZ", TaskType.REDUCE, 1, 2, 200.0f,0,2);
    checkMemReservedForTasksOnTT("tt2", 2 * 1024L, 2 * 1024L, 
                                 NUM_MAP_SLOTS-2, NUM_REDUCE_SLOTS-2);

    //complete the high ram job on tt1.
    for (Task task : t2) {
      taskTrackerManager.finishTask(
        "tt2", task.getTaskID().toString(),
        job2);
    }

    //1st map & 2nd reduce from job2
    checkMultipleAssignment(
      "tt2", "attempt_test_0002_m_000001_0 on tt2",
      "attempt_test_0002_r_000002_0 on tt2");
  }

  /**
   *   test if user limits automatically adjust to max map or reduce limit
   */
  public void testUserLimitsWithMaxCapacities() throws Exception {
    setUp(2, 2, 2);
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 50));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 50));
    resConf.setFakeQueues(queues);
    resConf.setMaxCapacity("default", 75.0f);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit a job
    FakeJobInProgress fjob1 =
      submitJobAndInit(JobStatus.PREP, 10, 10, "default", "u1");
    FakeJobInProgress fjob2 =
      submitJobAndInit(JobStatus.PREP, 10, 10, "default", "u2");

    // for queue 'default', maxCapacity for map and reduce is 3.
    // initial user limit for 50% assuming there are 2 users/queue is.
    //  1 map and 1 reduce.
    // after max capacity it is 1.5 each.

    //each job would be given 1 map task each.
    checkAssignments("tt1", 
      new String[] {"attempt_test_0001_m_000001_0 on tt1",
        "attempt_test_0002_m_000001_0 on tt1", 
        "attempt_test_0001_r_000001_0 on tt1"});

    //abt to hit max map capacity for default
    //hit user limit for reduces
    checkAssignments("tt2", 
        new String[] {"attempt_test_0001_m_000002_0 on tt2",
          "attempt_test_0002_r_000001_0 on tt2"});

    // only 1 reduce slot is remaining on tt2
    // no more maps since no map slots are available 
    checkAssignments("tt2", 
        new String[] { "attempt_test_0001_r_000002_0 on tt2"});
  }


  // test user limits
  public void testUserLimits() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit a job  
    submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u1");
    // for queue 'q2', the capacity for maps is 2. Since we're the only user,
    // we should get 2 maps and 1 reduce 
    checkAssignments("tt1", 
        new String[]{"attempt_test_0001_m_000001_0 on tt1", 
        "attempt_test_0001_m_000002_0 on tt1", 
        "attempt_test_0001_r_000001_0 on tt1"});
    
    // Submit another job, from a different user
    submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u2");
    // Now if I ask for a map task, it should come from the second job from default queue's capacity 
    checkAssignments("tt2", 
        new String[]{"attempt_test_0002_m_000001_0 on tt2", 
        "attempt_test_0002_m_000002_0 on tt2", 
        "attempt_test_0002_r_000001_0 on tt2"});
  }

  // test user limits when a 2nd job is submitted much after first job 
  public void testUserLimits2() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit a job  
    submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u1");
    // for queue 'q2', the capacity for maps is 2. Since we're the only user,
    // we should get 2 map tasks & 1 reduce 
    checkAssignments("tt1", 
        new String[] {"attempt_test_0001_m_000001_0 on tt1", 
        "attempt_test_0001_m_000002_0 on tt1", 
        "attempt_test_0001_r_000001_0 on tt1"});
    
    // Submit another job, from a different user
    submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u2");
    // Now if I ask for a map task, it should come from the second job 
    checkAssignments("tt2", 
        new String[] {"attempt_test_0002_m_000001_0 on tt2", 
        "attempt_test_0002_m_000002_0 on tt2", 
        "attempt_test_0002_r_000001_0 on tt2"});
  }

  // test user limits when a 2nd job is submitted much after first job 
  // and we need to wait for first job's task to complete
  public void testUserLimits3() throws Exception {
    System.err.println("testUserLimits3");
    
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit a job  
    FakeJobInProgress j1 = submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u1");
    // for queue 'q2', the capacity for maps is 2. Since we're the only user,
    // we should get a task 
    checkAssignments("tt1", 
        new String[] {
        "attempt_test_0001_m_000001_0 on tt1", 
        "attempt_test_0001_m_000002_0 on tt1", 
        "attempt_test_0001_r_000001_0 on tt1"});

    // No tasks assigned since u1 has hit user limits of 50% i.e. q2 capacity
    checkAssignments("tt2", new String[] {});

    // Submit another job, from a different user
    FakeJobInProgress j2 = submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u2");
    
    // Now if I ask for a map task, it should come from the second job 
    // and reduce from job2
    checkAssignments("tt2", 
        new String[] {
        "attempt_test_0002_m_000001_0 on tt2",
        "attempt_test_0002_m_000002_0 on tt2",
        "attempt_test_0002_r_000001_0 on tt2",
        });
    
    // A task from job1 finishes
    // job1 shud get the map slot since u1 has only 1 task running 
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000001_0", j1);
    checkAssignments("tt1", new String[] {"attempt_test_0001_m_000003_0 on tt1"});
    
    // now we have equal number of tasks from each job. Whichever job's
    // task finishes, that job gets a new task
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_m_000001_0", j2);
    checkAssignments("tt2", new String[] {"attempt_test_0002_m_000003_0 on tt2"});
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000002_0", j1);
    checkAssignment("tt1", "attempt_test_0001_m_000004_0 on tt1");
  }

  // test user limits with many users, more slots
  public void testUserLimits4() throws Exception {
    // set up one queue, with 10 slots
    String[] qs = {"default"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();
    // add some more TTs 
    taskTrackerManager.addTaskTracker("tt3");
    taskTrackerManager.addTaskTracker("tt4");
    taskTrackerManager.addTaskTracker("tt5");

    // u1 submits job
    FakeJobInProgress j1 = submitJobAndInit(JobStatus.PREP, 10, 10, null, "u1");
    // it gets the first 6 slots
    checkAssignments("tt1", 
        new String[] {
        "attempt_test_0001_m_000001_0 on tt1",
        "attempt_test_0001_m_000002_0 on tt1",
        "attempt_test_0001_r_000001_0 on tt1"
    });
    checkAssignments("tt2", 
        new String[] {
        "attempt_test_0001_m_000003_0 on tt2",
        "attempt_test_0001_m_000004_0 on tt2",
        "attempt_test_0001_r_000002_0 on tt2"
    });
    checkAssignments("tt3", 
        new String[] {
        "attempt_test_0001_m_000005_0 on tt3",
        "attempt_test_0001_m_000006_0 on tt3",
        "attempt_test_0001_r_000003_0 on tt3"
    });
    
    // u2 submits job with 4 slots
    FakeJobInProgress j2 = submitJobAndInit(JobStatus.PREP, 4, 4, null, "u2");
    // u2 should get next 4 slots
    checkAssignments("tt4", 
        new String[] {
        "attempt_test_0002_m_000001_0 on tt4",
        "attempt_test_0002_m_000002_0 on tt4",
        "attempt_test_0002_r_000001_0 on tt4"
    });
    checkAssignments("tt5", 
        new String[] {
        "attempt_test_0002_m_000003_0 on tt5",
        "attempt_test_0002_m_000004_0 on tt5",
        "attempt_test_0002_r_000002_0 on tt5"
    });

    // u1 finishes a task
    taskTrackerManager.finishTask("tt3", "attempt_test_0001_m_000006_0", j1);
    // u1 submits a few more jobs 
    // All the jobs are inited when submitted
    // because of addition of Eager Job Initializer all jobs in this
    //case would e initialised.
    submitJobAndInit(JobStatus.PREP, 10, 10, null, "u1");
    submitJobAndInit(JobStatus.PREP, 10, 10, null, "u1");
    submitJobAndInit(JobStatus.PREP, 10, 10, null, "u1");
    // u2 also submits a job
    submitJobAndInit(JobStatus.PREP, 10, 10, null, "u2");
    // now u3 submits a job
    submitJobAndInit(JobStatus.PREP, 2, 2, null, "u3");
    // next slot should go to u3, even though u2 has an earlier job, since
    // user limits have changed and u1/u2 are over limits
    checkAssignment("tt3", "attempt_test_0007_m_000001_0 on tt3");
    // some other task finishes and u3 gets it
    taskTrackerManager.finishTask("tt3", "attempt_test_0001_m_000005_0", j1);
    checkAssignment("tt3", "attempt_test_0007_m_000002_0 on tt3");
    // now, u2 finishes a task
    taskTrackerManager.finishTask("tt4", "attempt_test_0002_m_000002_0", j1);
    // next slot will go to u1, since u3 has nothing to run and u1's job is 
    // first in the queue
    checkAssignment("tt4", "attempt_test_0001_m_000007_0 on tt4");
  }

  /**
   * Test to verify that high memory jobs hit user limits faster than any normal
   * job.
   * 
   * @throws IOException
   */
  public void testUserLimitsForHighMemoryJobs()
      throws IOException {
    System.err.println("testUserLimitsForHighMemoryJobs");
    taskTrackerManager = new FakeTaskTrackerManager(1, 10, 10);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    String[] qs = { "default" };
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 50));
    resConf.setFakeQueues(queues);
    // enabled memory-based scheduling
    // Normal job in the cluster would be 1GB maps/reduces
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY, 2 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY, 2 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // Submit one normal job to the other queue.
    JobConf jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(6);
    jConf.setNumReduceTasks(6);
    jConf.setUser("u1");
    jConf.setQueueName("default");
    FakeJobInProgress job1 = submitJobAndInit(JobStatus.PREP, jConf);

    LOG.debug("Submit one high memory(2GB maps, 2GB reduces) job of "
        + "6 map and 6 reduce tasks");
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(2 * 1024);
    jConf.setNumMapTasks(6);
    jConf.setNumReduceTasks(6);
    jConf.setQueueName("default");
    jConf.setUser("u2");
    FakeJobInProgress job2 = submitJobAndInit(JobStatus.PREP, jConf);

    // Verify that normal job takes 3 task assignments to hit user limits,
    // and then j2 gets 4 slots
    checkAssignments("tt1", 
        new String[] {
        "attempt_test_0001_m_000001_0 on tt1",
        "attempt_test_0001_m_000002_0 on tt1",
        "attempt_test_0001_m_000003_0 on tt1",
        "attempt_test_0001_m_000004_0 on tt1",
        "attempt_test_0001_m_000005_0 on tt1",
        "attempt_test_0002_m_000001_0 on tt1",
        "attempt_test_0002_m_000002_0 on tt1",
        "attempt_test_0001_r_000001_0 on tt1",
    });

    // u1 has 5 map slots and 5 reduce slots. u2 has 4 map slots and 4 reduce
    // slots. Because of high memory tasks, giving u2 another task would
    // overflow limits. So, no more tasks should be given to anyone.
//    assertEquals(0, scheduler.assignTasks(tracker("tt1")).size());
//    assertEquals(0, scheduler.assignTasks(tracker("tt1")).size());
  }

  /*
   * Following is the testing strategy for testing scheduling information.
   * - start capacity scheduler with two queues.
   * - check the scheduling information with respect to the configuration
   * which was used to configure the queues.
   * - Submit 5 jobs to a queue.
   * - Check the waiting jobs count, it should be 5.
   * - Then run initializationPoller()
   * - Check once again the waiting queue, it should be 5 jobs again.
   * - Then raise status change events.
   * - Assign one task to a task tracker. (Map)
   * - Check waiting job count, it should be 4 now and used map (%) = 100
   * - Assign another one task (Reduce)
   * - Check waiting job count, it should be 4 now and used map (%) = 100
   * and used reduce (%) = 100
   * - finish the job and then check the used percentage it should go
   * back to zero
   * - Then pick an initialized job but not scheduled job and fail it.
   * - Run the poller
   * - Check the waiting job count should now be 3.
   * - Now fail a job which has not been initialized at all.
   * - Run the poller, so that it can clean up the job queue.
   * - Check the count, the waiting job count should be 2.
   * - Now raise status change events to move the initialized jobs which
   * should be two in count to running queue.
   * - Then schedule a map of the job in running queue.
   * - Run the poller because the poller is responsible for waiting
   * jobs count. Check the count, it should be using 100% map and one
   * waiting job
   * - fail the running job.
   * - Check the count, it should be now one waiting job and zero running
   * tasks
   */

  public void testSchedulingInformation() throws Exception {
    System.err.println("testSchedulingInformation()");
    String[] qs = {"default", "q2"};
    taskTrackerManager = new FakeTaskTrackerManager(2, 1, 1);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));
    resConf.setFakeQueues(queues);
    resConf.setMaxInitializedActiveTasksPerUser("default", 4);  // 4 tasks max
    resConf.setMaxInitializedActiveTasksPerUser("q2", 4);  // 4 tasks max
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    scheduler.assignTasks(tracker("tt1")); // heartbeat
    scheduler.assignTasks(tracker("tt2")); // heartbeat
    int totalMaps = taskTrackerManager.getClusterStatus().getMaxMapTasks();
    int totalReduces =
        taskTrackerManager.getClusterStatus().getMaxReduceTasks();
    QueueManager queueManager = scheduler.taskTrackerManager.getQueueManager();
    String schedulingInfo =
        queueManager.getJobQueueInfo("default").getSchedulingInfo();
    String schedulingInfo2 =
        queueManager.getJobQueueInfo("q2").getSchedulingInfo();
    String[] infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 19);
    assertEquals(infoStrings[0], "Queue configuration");
    assertEquals(infoStrings[1], "Capacity Percentage: 50.0%");
    assertEquals(infoStrings[2], "User Limit: 25%");
    assertEquals(infoStrings[3], "Priority Supported: YES");
    assertEquals(infoStrings[4], "-------------");
    assertEquals(infoStrings[5], "Map tasks");
    assertEquals(infoStrings[6], "Capacity: " + totalMaps * 50 / 100
        + " slots");
    assertEquals(infoStrings[7], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 0");
    assertEquals(infoStrings[9], "-------------");
    assertEquals(infoStrings[10], "Reduce tasks");
    assertEquals(infoStrings[11], "Capacity: " + totalReduces * 50 / 100
        + " slots");
    assertEquals(infoStrings[12], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[13], "Running tasks: 0");
    assertEquals(infoStrings[14], "-------------");
    assertEquals(infoStrings[15], "Job info");
    assertEquals(infoStrings[16], "Number of Waiting Jobs: 0");
    assertEquals(infoStrings[17], "Number of Initializing Jobs: 0");
    assertEquals(infoStrings[18], "Number of users who have submitted jobs: 0");
    assertEquals(schedulingInfo, schedulingInfo2);

    //Testing with actual job submission.
    ArrayList<FakeJobInProgress> userJobs =
      submitJobs(1, 5, "default").get("u1");
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");

    //waiting job should be equal to number of jobs submitted.
    assertEquals(infoStrings.length, 19);
    assertEquals(infoStrings[7], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 0");
    assertEquals(infoStrings[12], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[13], "Running tasks: 0");
    assertEquals(infoStrings[16], "Number of Waiting Jobs: 5");
    assertEquals(infoStrings[17], "Number of Initializing Jobs: 0");
    assertEquals(infoStrings[18], "Number of users who have submitted jobs: 1");

    //Initalize the jobs but don't raise events
    controlledInitializationPoller.selectJobsToInitialize();

    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 19);
    //2 jobs are now 'ready to init'
    //3 are waiting due to init task limits
    assertEquals(infoStrings[7], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 0");
    assertEquals(infoStrings[12], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[13], "Running tasks: 0");
    assertEquals(infoStrings[16], "Number of Waiting Jobs: 3");

    //Raise status change event so that jobs can move to running queue.
    raiseStatusChangeEvents(scheduler.jobQueuesManager);
    raiseStatusChangeEvents(scheduler.jobQueuesManager, "q2");
    //assign one job
    checkAssignments("tt1", 
        new String[] {
        "attempt_test_0001_m_000001_0 on tt1", 
        "attempt_test_0001_r_000001_0 on tt1"});
    
    //Initalize extra job.
    controlledInitializationPoller.selectJobsToInitialize();

    //Get scheduling information, now the number of waiting job should have
    //be 3 and initializing is 0 as 2 have been scheduled.
    // make sure we update our stats
    scheduler.updateQueueUsageForTests();
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");
    assertEquals(schedulingInfo, 23, infoStrings.length);
    assertEquals(infoStrings[7], infoStrings[7], "Used capacity: 1 (100.0% of Capacity)");
    assertEquals(infoStrings[8], infoStrings[8], "Running tasks: 1");
    assertEquals(infoStrings[9], infoStrings[9], "Active users:");
    assertEquals(infoStrings[10], infoStrings[10], "User 'u1': 1 (100.0% of used capacity)");
    assertEquals(infoStrings[14], infoStrings[14], "Used capacity: 1 (100.0% of Capacity)");
    assertEquals(infoStrings[15], infoStrings[15], "Running tasks: 1");
    assertEquals(infoStrings[18], infoStrings[20], "Number of Waiting Jobs: 3");
    assertEquals(infoStrings[18], infoStrings[21], "Number of Initializing Jobs: 0");
    
    //Complete the job and check the running tasks count
    FakeJobInProgress u1j1 = userJobs.get(0);
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000001_0", u1j1);
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_r_000001_0", u1j1);
    taskTrackerManager.finalizeJob(u1j1);

    // make sure we update our stats
    scheduler.updateQueueUsageForTests();
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 19);
    assertEquals(infoStrings[7], infoStrings[7], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[8], infoStrings[8], "Running tasks: 0");
    assertEquals(infoStrings[12], infoStrings[12], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[13], infoStrings[13], "Running tasks: 0");
    assertEquals(infoStrings[16], infoStrings[16], "Number of Waiting Jobs: 3");

    //Fail a job which is initialized but not scheduled and check the count.
    FakeJobInProgress u1j2 = userJobs.get(1);
    assertTrue("User1 job 2 not initalized ",
        u1j2.getStatus().getRunState() == JobStatus.RUNNING);
    taskTrackerManager.finalizeJob(u1j2, JobStatus.FAILED);
    //Run initializer to clean up failed jobs
    controlledInitializationPoller.selectJobsToInitialize();
    // make sure we update our stats
    scheduler.updateQueueUsageForTests();
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 19);
    //2 more jobs are now in 'initializing state', none 'initialized'
    assertEquals(infoStrings[7], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 0");
    assertEquals(infoStrings[12], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[13], "Running tasks: 0");
    assertEquals(infoStrings[16], "Number of Waiting Jobs: 1");
    assertEquals(infoStrings[17], "Number of Initializing Jobs: 2");

    //Fail a job which is not initialized but is in the waiting queue.
    FakeJobInProgress u1j5 = userJobs.get(4);
    assertFalse("User1 job 5 initalized ",
        u1j5.getStatus().getRunState() == JobStatus.RUNNING);

    taskTrackerManager.finalizeJob(u1j5, JobStatus.FAILED);
    //run initializer to clean up failed job
    controlledInitializationPoller.selectJobsToInitialize();
    // make sure we update our stats
    scheduler.updateQueueUsageForTests();
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 19);
    //no more waiting jobs
    //all jobs are initing
    assertEquals(infoStrings[7], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 0");
    assertEquals(infoStrings[12], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[13], "Running tasks: 0");
    assertEquals(infoStrings[16], "Number of Waiting Jobs: 0");
    assertEquals(infoStrings[17], "Number of Initializing Jobs: 2");

    //Raise status change events as none of the intialized jobs would be
    //in running queue as we just failed the second job which was initialized
    //and completed the first one.
    raiseStatusChangeEvents(scheduler.jobQueuesManager);
    raiseStatusChangeEvents(scheduler.jobQueuesManager, "q2");

    //Now schedule a map should be job3 of the user as job1 succeeded job2
    //failed and now job3 is running
    checkAssignments("tt1", 
        new String[] {
        "attempt_test_0003_m_000001_0 on tt1",
        "attempt_test_0003_r_000001_0 on tt1"});
    FakeJobInProgress u1j3 = userJobs.get(2);
    assertTrue("User Job 3 not running ",
        u1j3.getStatus().getRunState() == JobStatus.RUNNING);

    //now the running count of map should be one and initing jobs should be
    //one. run the poller as it is responsible for waiting count
    controlledInitializationPoller.selectJobsToInitialize();
    // make sure we update our stats
    scheduler.updateQueueUsageForTests();
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 23);
    assertEquals(infoStrings[7], infoStrings[7], "Used capacity: 1 (100.0% of Capacity)");
    assertEquals(infoStrings[8], infoStrings[8], "Running tasks: 1");
    assertEquals(infoStrings[9], infoStrings[9], "Active users:");
    assertEquals(infoStrings[10], infoStrings[10], "User 'u1': 1 (100.0% of used capacity)");
    assertEquals(infoStrings[14], infoStrings[14], "Used capacity: 1 (100.0% of Capacity)");
    assertEquals(infoStrings[15], infoStrings[15], "Running tasks: 1");
    assertEquals(infoStrings[18], infoStrings[20], "Number of Waiting Jobs: 0");
    assertEquals(infoStrings[18], infoStrings[21], "Number of Initializing Jobs: 0");

    //Fail the executing job
    taskTrackerManager.finalizeJob(u1j3, JobStatus.FAILED);
    // make sure we update our stats
    scheduler.updateQueueUsageForTests();
    //Now running counts should become zero
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 19);
    assertEquals(infoStrings[7], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 0");
    assertEquals(infoStrings[16], "Number of Waiting Jobs: 0");
    assertEquals(infoStrings[17], "Number of Initializing Jobs: 0");
  }

  /**
   * Test to verify that highMemoryJobs are scheduled like all other jobs when
   * memory-based scheduling is not enabled.
   * @throws IOException
   */
  public void testDisabledMemoryBasedScheduling()
      throws IOException {

    LOG.debug("Starting the scheduler.");
    taskTrackerManager = new FakeTaskTrackerManager(1, 1, 1);

    taskTrackerManager.addQueues(new String[] { "default" });
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    // memory-based scheduling disabled by default.
    scheduler.start();

    LOG.debug("Submit one high memory job of 1 3GB map task "
        + "and 1 1GB reduce task.");
    JobConf jConf = new JobConf();
    jConf.setMemoryForMapTask(3 * 1024L); // 3GB
    jConf.setMemoryForReduceTask(1 * 1024L); // 1 GB
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(1);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    submitJobAndInit(JobStatus.RUNNING, jConf);

    // assert that all tasks are launched even though they transgress the
    // scheduling limits.

    checkAssignments("tt1", 
        new String[] {"attempt_test_0001_m_000001_0 on tt1", 
        "attempt_test_0001_r_000001_0 on tt1"});
  }

  /**
   * Test reverting HADOOP-4979. If there is a high-mem job, we should now look
   * at reduce jobs (if map tasks are high-mem) or vice-versa.
   * 
   * @throws IOException
   */
  public void testHighMemoryBlockingAcrossTaskTypes()
      throws IOException {

    // 2 map and 1 reduce slots
    final int NUM_MAP_SLOTS = 2;
    final int NUM_REDUCE_SLOTS = 1;
    taskTrackerManager = 
      new FakeTaskTrackerManager(1, NUM_MAP_SLOTS, NUM_REDUCE_SLOTS);

    taskTrackerManager.addQueues(new String[] { "default" });
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    // enabled memory-based scheduling
    // Normal job in the cluster would be 1GB maps/reduces
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY,
        2 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY,
        1 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // The situation : Two jobs in the queue. First job with only maps and no
    // reduces and is a high memory job. Second job is a normal job with both
    // maps and reduces.
    // First job cannot run for want of memory for maps. In this case, second
    // job's reduces should run.
    
    LOG.debug("Submit one high memory(2GB maps, 0MB reduces) job of "
        + "2 map tasks");
    JobConf jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(0);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(0);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job1 = submitJobAndInit(JobStatus.PREP, jConf);

    LOG.debug("Submit another regular memory(1GB vmem maps/reduces) job of "
        + "2 map/red tasks");
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(2);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job2 = submitJobAndInit(JobStatus.PREP, jConf);
    
    // first, a map from j1 will run
    checkAssignments("tt1", 
        new String[] {"attempt_test_0001_m_000001_0 on tt1",
        "attempt_test_0002_r_000001_0 on tt1"});
    // Total 2 maps & 1 reduce slot should be accounted for.
    checkOccupiedSlots("default", TaskType.MAP, 1, 2, 100.0f);
    checkOccupiedSlots("default", TaskType.REDUCE, 1, 1,
        100.0f);
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 1 * 1024L,
                                 NUM_MAP_SLOTS-2, NUM_REDUCE_SLOTS-1);
  }

  /**
   * Tests that scheduler schedules normal jobs once high RAM jobs 
   * have been reserved to the limit.
   * 
   * The test causes the scheduler to schedule a normal job on two
   * trackers, and one task of the high RAM job on a third. Then it 
   * asserts that one of the first two trackers gets a reservation 
   * for the remaining task of the high RAM job. After this, it 
   * asserts that a normal job submitted later is allowed to run 
   * on a free slot, as all tasks of the high RAM job are either
   * scheduled or reserved.
   *  
   * @throws IOException
   */
  public void testClusterBlockingForLackOfMemory()
      throws IOException {

    LOG.debug("Starting the scheduler.");
    final int NUM_MAP_SLOTS = 2;
    final int NUM_REDUCE_SLOTS = 2;
    taskTrackerManager = 
      new FakeTaskTrackerManager(3, NUM_MAP_SLOTS, NUM_REDUCE_SLOTS);

    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 25));
    taskTrackerManager.addQueues(new String[] { "default" });
    resConf.setFakeQueues(queues);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    // enabled memory-based scheduling
    // Normal jobs 1GB maps/reduces. 2GB limit on maps/reduces
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY,
        2 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY,
        2 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    LOG.debug("Submit one normal memory(1GB maps/reduces) job of "
        + "2 map, 2 reduce tasks.");
    JobConf jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(2);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job1 = submitJobAndInit(JobStatus.PREP, jConf);

    // Fill a tt with this job's tasks.
    checkAssignments("tt1", 
        new String[] {
        "attempt_test_0001_m_000001_0 on tt1", 
        "attempt_test_0001_m_000002_0 on tt1", 
        "attempt_test_0001_r_000001_0 on tt1"});
    // Total 2 map slot and 1 reduce should be accounted for.
    checkOccupiedSlots("default", TaskType.MAP, 1, 2, 33.33f);
    assertEquals(
        CapacityTaskScheduler.getJobQueueSchedInfo(2, 2, 0, 1, 1, 0),
        (String) job1.getSchedulingInfo());
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 1 * 1024L, 
                                 NUM_MAP_SLOTS-2, NUM_REDUCE_SLOTS-1);

    // fill another TT with the rest of the tasks of the job
    checkAssignments("tt2", 
        new String[] {"attempt_test_0001_r_000002_0 on tt2"});

    LOG.debug("Submit one high memory(2GB maps/reduces) job of "
        + "2 map, 2 reduce tasks.");
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(2 * 1024);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(2);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job2 = submitJobAndInit(JobStatus.PREP, jConf);

    // Have another TT run one task of each type of the high RAM
    // job. This will fill up the TT. 
    checkAssignments("tt3", 
        new String[] {
        "attempt_test_0002_m_000001_0 on tt3",
        "attempt_test_0002_r_000001_0 on tt3"
    });
    
    checkOccupiedSlots("default", TaskType.MAP, 1, 4, 66.7f);
    assertEquals(
        CapacityTaskScheduler.getJobQueueSchedInfo(1, 2, 0, 1, 2, 0),
        (String) job2.getSchedulingInfo());
    checkMemReservedForTasksOnTT("tt3", 2 * 1024L, 2 * 1024L, 
                                 NUM_MAP_SLOTS-2, NUM_REDUCE_SLOTS-2);


    LOG.debug("Submit one normal memory(1GB maps/reduces) job of "
        + "1 map, 1 reduce tasks.");
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(1);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job3 = submitJobAndInit(JobStatus.PREP, jConf);

    // Send a TT with insufficient space for task assignment,
    // This will cause a reservation for the high RAM job.
    checkAssignments("tt2", 
        new String[] {
        "attempt_test_0002_m_000002_0 on tt2"
    });

    // reserved tasktrackers contribute to occupied slots for maps and reduces
    checkOccupiedSlots("default", TaskType.MAP, 1, 6, 100.0f);
    checkOccupiedSlots("default", TaskType.REDUCE, 1, 6, 100.0f);
    checkMemReservedForTasksOnTT("tt2", 2 * 1024L, 1 * 1024L, 
                                 0, NUM_REDUCE_SLOTS-1);
    LOG.info(job2.getSchedulingInfo());
    assertEquals(
        CapacityTaskScheduler.getJobQueueSchedInfo(2, 4, 0, 1, 2, 2),
        (String) job2.getSchedulingInfo());
    assertEquals(
        CapacityTaskScheduler.getJobQueueSchedInfo(0, 0, 0, 0, 0, 0),
        (String) job3.getSchedulingInfo());
    
    // Reservations are already done for job2. 
    // So job3 should go ahead. 
    // However, it has hit the user limit of 6 for reduces (incl. the reserved
    // slot), so we should only get a map.
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000001_0", job1);
    scheduler.updateQueueUsageForTests();
    checkAssignments("tt1", 
        new String[] {
        "attempt_test_0003_m_000001_0 on tt1"});
  }

  /**
   * Testcase to verify fix for a NPE (HADOOP-5641), when memory based
   * scheduling is enabled and jobs are retired from memory when tasks
   * are still active on some Tasktrackers.
   *  
   * @throws IOException
   */
  public void testMemoryMatchingWithRetiredJobs() throws IOException {
    // create a cluster with a single node.
    LOG.debug("Starting cluster with 1 tasktracker, 2 map and 2 reduce slots");
    final int NUM_MAP_SLOTS = 2;
    final int NUM_REDUCE_SLOTS = 2;
    taskTrackerManager = 
      new FakeTaskTrackerManager(1, NUM_MAP_SLOTS, NUM_REDUCE_SLOTS);

    // create scheduler
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));
    taskTrackerManager.addQueues(new String[] { "default" });
    resConf.setFakeQueues(queues);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    // enabled memory-based scheduling
    LOG.debug("Assume TT has 2GB for maps and 2GB for reduces");
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY,
        2 * 1024L);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 512);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY,
        2 * 1024L);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 512);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();
    
    // submit a normal job
    LOG.debug("Submitting a normal job with 1 maps and 1 reduces");
    JobConf jConf = new JobConf();
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(1);
    jConf.setMemoryForMapTask(512);
    jConf.setMemoryForReduceTask(512);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    jConf.setSpeculativeExecution(false);
    FakeJobInProgress job1 = submitJobAndInit(JobStatus.PREP, jConf);

    // 1st cycle - 1 map & 1 reduce gets assigned.
    checkAssignments("tt1", 
        new String[] {
        "attempt_test_0001_m_000001_0 on tt1",
        "attempt_test_0001_r_000001_0 on tt1"});
    // Total 1 map & 1 reduce slot should be accounted for.
    checkOccupiedSlots("default", TaskType.MAP, 1, 1, 50.0f);
    checkOccupiedSlots("default", TaskType.REDUCE, 1, 1, 50.0f);
    checkMemReservedForTasksOnTT("tt1",  512L, 512L, 
                                 NUM_MAP_SLOTS-1, NUM_REDUCE_SLOTS-1);
    
    // kill this job !
    taskTrackerManager.killJob(job1.getJobID());
    // No more map/reduce slots should be accounted for.
    checkOccupiedSlots("default", TaskType.MAP, 0, 0, 0.0f);
    checkOccupiedSlots("default", TaskType.REDUCE, 0, 0, 0.0f);
    
    // retire the job
    taskTrackerManager.removeJob(job1.getJobID());
    
    // submit another job.
    LOG.debug("Submitting another normal job with 2 maps and 2 reduces");
    jConf = new JobConf();
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(2);
    jConf.setMemoryForMapTask(512);
    jConf.setMemoryForReduceTask(512);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job2 = submitJobAndInit(JobStatus.PREP, jConf);
    
    // since with HADOOP-5964, we don't rely on a job conf to get
    // the memory occupied, scheduling should be able to work correctly.
    checkAssignments("tt1", 
        new String[] {
        "attempt_test_0002_m_000001_0 on tt1",
        "attempt_test_0002_r_000001_0 on tt1"
    });
    checkOccupiedSlots("default", TaskType.MAP, 1, 1, 50);
    checkOccupiedSlots("default", TaskType.REDUCE, 1, 1, 50);
    checkMemReservedForTasksOnTT("tt1", 1024L, 1024L, 
                                 NUM_MAP_SLOTS-2, NUM_REDUCE_SLOTS-2);
    
    // now, no more can be assigned because all the slots are blocked.
    assertEquals(0, scheduler.assignTasks(tracker("tt1")).size());

    // finish the tasks on the tracker.
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000001_0", job1);
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_r_000001_0", job1);
    
    // now new tasks can be assigned.
    checkAssignments("tt1", 
        new String[] {
        "attempt_test_0002_m_000002_0 on tt1",
        "attempt_test_0002_r_000002_0 on tt1"});
    checkOccupiedSlots("default", TaskType.MAP, 1, 2, 100.0f);
    checkOccupiedSlots("default", TaskType.REDUCE, 1, 2, 100.0f);
    checkMemReservedForTasksOnTT("tt1", 1024L, 1024L, 
                                 NUM_MAP_SLOTS-2, NUM_REDUCE_SLOTS-2);
  }

  /*
   * Test cases for Job Initialization poller.
   * 
   * This test verifies that the correct number of jobs for
   * correct number of users is initialized.
   * It also verifies that as jobs of users complete, new jobs
   * from the correct users are initialized.
   */
  public void testJobInitialization() throws Exception {
    System.err.println("testJobInitialization");
    // set up the scheduler
    String[] qs = { "default" };
    taskTrackerManager = new FakeTaskTrackerManager(2, 1, 1);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    controlledInitializationPoller.setTaskTrackerManager(taskTrackerManager);
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));
    resConf.setFakeQueues(queues);
    resConf.setMaxSystemJobs(6);  // 6 jobs max
    resConf.setMaxInitializedActiveTasksPerUser("default", 4);  // 4 tasks max
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();
  
    JobQueuesManager mgr = scheduler.jobQueuesManager;
    JobInitializationPoller initPoller = scheduler.getInitializationPoller();

    // submit 4 jobs each for 3 users.
    HashMap<String, ArrayList<FakeJobInProgress>> userJobs = submitJobs(3,
        4, "default");
        
    // get the jobs submitted.
    ArrayList<FakeJobInProgress> u1Jobs = userJobs.get("u1");
    ArrayList<FakeJobInProgress> u2Jobs = userJobs.get("u2");
    ArrayList<FakeJobInProgress> u3Jobs = userJobs.get("u3");
    
    // reference to the initializedJobs data structure
    // changes are reflected in the set as they are made by the poller
    Set<JobID> initializedJobs = initPoller.getInitializedJobList();
    
    // we should have 12 (3 x 4) jobs in the job queue
    assertEquals(mgr.getQueue("default").getWaitingJobs().size(), 12);
        
    // run one poller iteration.
    controlledInitializationPoller.selectJobsToInitialize();
    
    System.err.println("3 TTM #listeners=" + taskTrackerManager.mylisteners.size());
    
    // the poller should initialize 6 jobs
    // 3 users and 2 jobs (with 2 tasks) from each
    assertEquals(initializedJobs.size(), 6);

    assertTrue("Initialized jobs didnt contain the user1 job 1",
        initializedJobs.contains(u1Jobs.get(0).getJobID()));
    assertTrue("Initialized jobs didnt contain the user1 job 2",
        initializedJobs.contains(u1Jobs.get(1).getJobID()));
    assertTrue("Initialized jobs didnt contain the user2 job 1",
        initializedJobs.contains(u2Jobs.get(0).getJobID()));
    assertTrue("Initialized jobs didnt contain the user2 job 2",
        initializedJobs.contains(u2Jobs.get(1).getJobID()));
    assertTrue("Initialized jobs didnt contain the user3 job 1",
        initializedJobs.contains(u3Jobs.get(0).getJobID()));
    assertTrue("Initialized jobs didnt contain the user3 job 2",
        initializedJobs.contains(u3Jobs.get(1).getJobID()));
    
    // now submit one more job from another user.
    FakeJobInProgress u4j1 = 
      submitJob(JobStatus.PREP, 1, 1, "default", "u4");

    // run the poller again.
    controlledInitializationPoller.selectJobsToInitialize();
    
    // since no jobs have started running, there should be no
    // change to the initialized jobs.
    assertEquals(initializedJobs.size(), 6);
    assertFalse("Initialized jobs doesn't contain user 4 jobs",
        initializedJobs.contains(u4j1.getJobID()));
    
    // This event simulates raising the event on completion of setup task
    // and moves the job to the running list for the scheduler to pick up.
    raiseStatusChangeEvents(mgr);
    
    // get some tasks assigned.
    checkAssignments("tt1", 
        new String[] {
        "attempt_test_0001_m_000001_0 on tt1",
        "attempt_test_0001_r_000001_0 on tt1"
    });
    checkAssignments("tt2", 
        new String[] {
        "attempt_test_0002_m_000001_0 on tt2",
        "attempt_test_0002_r_000001_0 on tt2"
    });
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000001_0", u1Jobs.get(0));
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_m_000001_0", u1Jobs.get(0));
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_r_000001_0", u1Jobs.get(1));
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_r_000001_0", u1Jobs.get(1));
    
    // as max running jobs is 6, still no more jobs can be inited
    controlledInitializationPoller.selectJobsToInitialize();

    // count should be 4 since 2 jobs are running
    assertEquals(initializedJobs.size(), 4);
    
    // Job has completed
    taskTrackerManager.finalizeJob(u1Jobs.get(0));
    taskTrackerManager.finalizeJob(u1Jobs.get(1));
    
    // count should now be 4 since we haven't called the poller yet
    assertEquals(initializedJobs.size(), 4);

    // as some jobs have completed, the poller will now
    // pick up new jobs to initialize.
    controlledInitializationPoller.selectJobsToInitialize();

    // count should still be the same
    assertEquals(initializedJobs.size(), 6);

    // new jobs that have got into the list
    assertTrue(initializedJobs.contains(u1Jobs.get(2).getJobID()));
    assertTrue(initializedJobs.contains(u1Jobs.get(3).getJobID()));
    raiseStatusChangeEvents(mgr);
    
    // the first two jobs are done, no longer in the initialized list.
    assertFalse("Initialized jobs contains the user1 job 1",
        initializedJobs.contains(u1Jobs.get(0).getJobID()));
    assertFalse("Initialized jobs contains the user1 job 2",
        initializedJobs.contains(u1Jobs.get(1).getJobID()));
    
    // finish one more job
    checkAssignments("tt1", 
        new String[] {
        "attempt_test_0003_m_000001_0 on tt1",
        "attempt_test_0003_r_000001_0 on tt1"
    });
    taskTrackerManager.finishTask("tt1", "attempt_test_0003_m_000001_0", u1Jobs.get(2));
    taskTrackerManager.finishTask("tt1", "attempt_test_0003_r_000001_0", u1Jobs.get(2));

    // no new jobs should be picked up, because max running jobs is 6, job3 
    // hasn't been marked as 'complete' yet
    controlledInitializationPoller.selectJobsToInitialize();
    
    assertEquals(initializedJobs.size(), 5);
    
    // run 1 more jobs.. 
    checkAssignments("tt1", 
        new String[] {
        "attempt_test_0004_m_000001_0 on tt1",
        "attempt_test_0004_r_000001_0 on tt1"
    });
    
    // Finish job_0003
    taskTrackerManager.finalizeJob(u1Jobs.get(2));
    
    // Now initialised jobs should contain user 4's job, as
    // user 1's jobs are all done u2 and u3 already have 6 active tasks
    controlledInitializationPoller.selectJobsToInitialize();
    assertEquals(initializedJobs.size(), 5);
    assertTrue(initializedJobs.contains(u4j1.getJobID()));
    
    controlledInitializationPoller.stopRunning();
  }
  
  /**
   * This testcase test limits on job-submission per-user and per-queue.
   */
  public void testJobSubmissionLimits() throws Exception {
    System.err.println("testJobSubmissionLimits");
    
    // set up the scheduler
    String[] qs = {"default", "q2"};
    taskTrackerManager = new FakeTaskTrackerManager(2, 1, 1);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 50));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));
    resConf.setFakeQueues(queues);
    resConf.setMaxInitializedActiveTasksPerUser("default", 4);  // 4 tasks max
    resConf.setMaxInitializedActiveTasksPerUser("q2", 4);  // 4 tasks max
    resConf.setInitToAcceptJobsFactor("default", 1);
    resConf.setMaxSystemJobs(12); // max 12 running jobs in the system, hence

    // In queue 'default'
    // max (pending+running) jobs -> 12 * 1 * .5 = 6 
    // max jobs per user to init -> 12 * .5 * .5 = 2
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();
  
    JobQueuesManager mgr = scheduler.jobQueuesManager;
    JobInitializationPoller initPoller = scheduler.getInitializationPoller();

    // submit 2 jobs each for 3 users, the maximum possible to default
    HashMap<String, ArrayList<FakeJobInProgress>> userJobs = 
      submitJobs(3, 2, "default");
        
    // get the jobs submitted.
    ArrayList<FakeJobInProgress> u1Jobs = userJobs.get("u1");
    ArrayList<FakeJobInProgress> u2Jobs = userJobs.get("u2");
    ArrayList<FakeJobInProgress> u3Jobs = userJobs.get("u3");
    
    // reference to the initializedJobs data structure
    // changes are reflected in the set as they are made by the poller
    Set<JobID> initializedJobs = initPoller.getInitializedJobList();
    
    // we should have 6 jobs in the job queue
    assertEquals(6, mgr.getQueue("default").getWaitingJobs().size());
        
    // run one poller iteration.
    controlledInitializationPoller.selectJobsToInitialize();
        
    // the poller should initialize 6 jobs
    // 3 users and 2 jobs (with 2 tasks) from each
    assertEquals(initializedJobs.size(), 6);
    
    // now submit one more job from another user, should fail since default's
    // job submission capacity is full
    boolean jobSubmissionFailed = false;
    try {
      FakeJobInProgress u4j1 = 
        submitJob(JobStatus.PREP, 1, 1, "default", "u4");
    } catch (IOException ioe) {
      jobSubmissionFailed = true;
    }
    assertTrue("Job submission of 7th job to 'default' queue didn't fail!", 
        jobSubmissionFailed);

    // fail some jobs to clear up quota
    taskTrackerManager.finalizeJob(u2Jobs.get(0), JobStatus.FAILED);
    taskTrackerManager.finalizeJob(u3Jobs.get(0), JobStatus.FAILED);
    
    FakeJobInProgress u1j3 = 
      submitJob(JobStatus.PREP, 1, 1, "default", "u1");

    // run the poller again.
    controlledInitializationPoller.selectJobsToInitialize();

    // the poller should initialize 4 jobs
    // 2 from u1 and one each from u2 and u3
    assertEquals(initializedJobs.size(), 4);

    // Should fail since u1 is already at limit of 3 jobs
    jobSubmissionFailed = false;
    try {
      FakeJobInProgress u1j4 = 
        submitJob(JobStatus.PREP, 1, 1, "default", "u1");
    } catch (IOException ioe) {
      jobSubmissionFailed = true;
    }
    
    assertTrue("Job submission of 4th job of user 'u1' to queue 'default' " +
    		"didn't fail!", jobSubmissionFailed);
  }
  
  /*
   * testHighPriorityJobInitialization() shows behaviour when high priority job
   * is submitted into a queue and how initialisation happens for the same.
   */
  public void testHighPriorityJobInitialization() throws Exception {
    String[] qs = { "default"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));
    resConf.setFakeQueues(queues);
    resConf.setMaxSystemJobs(6); // 6 jobs max
    resConf.setMaxInitializedActiveTasksPerUser("default", 4);  // 4 tasks max
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    JobInitializationPoller initPoller = scheduler.getInitializationPoller();
    Set<JobID> initializedJobsList = initPoller.getInitializedJobList();

    // submit 3 jobs for 3 users, only 2 each should be inited since max active
    // tasks per user is 4 and max jobs is 6
    HashMap<String, ArrayList<FakeJobInProgress>> userJobs = 
      submitJobs(3,3,"default");
    controlledInitializationPoller.selectJobsToInitialize();
    assertEquals(initializedJobsList.size(), 6);
    
    // submit 2 job for a different user. one of them will be made high priority
    FakeJobInProgress u4j1 = submitJob(JobStatus.PREP, 1, 1, "default", "u4");
    FakeJobInProgress u4j2 = submitJob(JobStatus.PREP, 2, 2, "default", "u4");
    
    controlledInitializationPoller.selectJobsToInitialize();
    
    // shouldn't change since max jobs is 6
    assertEquals(initializedJobsList.size(), 6);
    
    assertFalse("Contains U4J1 high priority job " , 
        initializedJobsList.contains(u4j1.getJobID()));
    assertFalse("Contains U4J2 Normal priority job " , 
        initializedJobsList.contains(u4j2.getJobID()));

    // change priority of one job
    System.err.println("changing prio");
    taskTrackerManager.setPriority(u4j2, JobPriority.VERY_HIGH);
    
    // Finish one of the inited jobs
    // run 1 more jobs.. 
    checkAssignments("tt1", 
        new String[] {
        "attempt_test_0001_m_000001_0 on tt1",
        "attempt_test_0002_m_000001_0 on tt1",
        "attempt_test_0001_r_000001_0 on tt1"
    });
    
    // Finish job_0003
    taskTrackerManager.finalizeJob(userJobs.get("u1").get(0));

    controlledInitializationPoller.selectJobsToInitialize();
    
    // the high priority job should get initialized, but not the
    // low priority job from u4, as we have already exceeded the
    // limit.
    assertEquals(initializedJobsList.size(), 5);
    assertTrue("Does not contain U4J2 high priority job " , 
        initializedJobsList.contains(u4j2.getJobID()));
    assertFalse("Contains U4J1 Normal priority job " , 
        initializedJobsList.contains(u4j1.getJobID()));
    controlledInitializationPoller.stopRunning();
  }
  
  public void testJobMovement() throws Exception {
    String[] qs = { "default"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();
        
    // check proper running job movement and completion
    checkRunningJobMovementAndCompletion();

    // check failed running job movement
    checkFailedRunningJobMovement();

    // Check job movement of failed initalized job
    checkFailedInitializedJobMovement();

    // Check failed waiting job movement
    checkFailedWaitingJobMovement(); 
  }

  public void testStartWithoutDefaultQueueConfigured() throws Exception {
    //configure a single queue which is not default queue
    String[] qs = {"q1"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("q1", 100.0f, true, 100));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    //Start the scheduler.
    scheduler.start();
    //Submit a job and wait till it completes
    FakeJobInProgress job = 
      submitJob(JobStatus.PREP, 1, 1, "q1", "u1");
    controlledInitializationPoller.selectJobsToInitialize();
    raiseStatusChangeEvents(scheduler.jobQueuesManager, "q1");
    checkAssignments("tt1", 
        new String[] {"attempt_test_0001_m_000001_0 on tt1", 
        "attempt_test_0001_r_000001_0 on tt1"});
  }
  
  public void testFailedJobInitalizations() throws Exception {
    String[] qs = {"default"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    JobQueuesManager mgr = scheduler.jobQueuesManager;
    
    //Submit a job whose initialization would fail always.
    FakeJobInProgress job =
      new FakeFailingJobInProgress(new JobID("test", ++jobCounter),
          new JobConf(), taskTrackerManager,"u1", 
          UtilsForTests.getJobTracker());
    job.getStatus().setRunState(JobStatus.PREP);
    taskTrackerManager.submitJob(job);
    //check if job is present in waiting list.
    CapacitySchedulerQueue queue = mgr.getQueue("default");
    assertEquals("Waiting job list does not contain submitted job",
        1, queue.getNumWaitingJobs());
    assertTrue("Waiting job does not contain submitted job", 
        queue.getWaitingJobs().contains(job));
    //initialization should fail now.
    controlledInitializationPoller.selectJobsToInitialize();
    //Check if the job has been properly cleaned up.
    assertEquals("Waiting job list contains submitted job",
        0, queue.getNumWaitingJobs());
    assertFalse("Waiting job contains submitted job", 
        queue.getWaitingJobs().contains(job));
    assertFalse("Waiting job contains submitted job", 
        queue.getRunningJobs().contains(job));
  }
  
  private void checkRunningJobMovementAndCompletion() throws IOException {
    
    JobQueuesManager mgr = scheduler.jobQueuesManager;
    JobInitializationPoller p = scheduler.getInitializationPoller();
    // submit a job
    FakeJobInProgress job = 
      submitJob(JobStatus.PREP, 1, 1, "default", "u1");
    controlledInitializationPoller.selectJobsToInitialize();
    
    assertEquals(p.getInitializedJobList().size(), 1);

    // make it running.
    raiseStatusChangeEvents(mgr);
    
    // it should be there in both the queues.
    CapacitySchedulerQueue queue = mgr.getQueue("default");
    assertTrue("Job present in waiting Job Queue",
        !queue.getWaitingJobs().contains(job));
    assertTrue("Job present in initializing Job Queue",
        !queue.getInitializingJobs().contains(job));
    assertTrue("Job not present in Running Queue",
        queue.getRunningJobs().contains(job));
    
    // assign a task
    checkAssignments("tt1", 
        new String[] {
        "attempt_test_0001_m_000001_0 on tt1", 
        "attempt_test_0001_r_000001_0 on tt1"});
    
    controlledInitializationPoller.selectJobsToInitialize();
    
    // now this task should be removed from the initialized list.
    assertTrue(p.getInitializedJobList().isEmpty());

    // the job should also be removed from the job queue as tasks
    // are scheduled
    assertFalse("Job present in Job Queue",
        queue.getWaitingJobs().contains(job));
    
    // complete tasks and job
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000001_0", job);
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_r_000001_0", job);
    taskTrackerManager.finalizeJob(job);
    
    // make sure it is removed from the run queue
    assertFalse("Job present in running queue",
        queue.getRunningJobs().contains(job));
  }
  
  private void checkFailedRunningJobMovement() throws IOException {
    
    JobQueuesManager mgr = scheduler.jobQueuesManager;
    
    //submit a job and initalized the same
    FakeJobInProgress job = 
      submitJobAndInit(JobStatus.RUNNING, 1, 1, "default", "u1");
    
    //check if the job is present in running queue.
    CapacitySchedulerQueue queue = mgr.getQueue("default");
    assertTrue("Running jobs list does not contain submitted job",
        queue.getRunningJobs().contains(job));
    
    taskTrackerManager.finalizeJob(job, JobStatus.KILLED);
    
    //check if the job is properly removed from running queue.
    assertFalse("Running jobs list does not contain submitted job",
        queue.getRunningJobs().contains(job));
    
  }
  
  /**
   * Test case deals with normal jobs which have speculative maps and reduce.
   * Following is test executed
   * <ol>
   * <li>Submit one job with speculative maps and reduce.</li>
   * <li>Submit another job with no speculative execution.</li>
   * <li>Observe that all tasks from first job get scheduled, speculative
   * and normal tasks</li>
   * <li>Finish all the first jobs tasks second jobs tasks get scheduled.</li>
   * </ol>
   * @throws IOException
   */
  public void testSpeculativeTaskScheduling() throws IOException {
    String[] qs = {"default"};
    taskTrackerManager = new FakeTaskTrackerManager(2, 1, 1);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    JobQueuesManager mgr = scheduler.jobQueuesManager;
    JobConf conf = new JobConf();
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(1);
    conf.setMapSpeculativeExecution(true);
    conf.setReduceSpeculativeExecution(true);
    //Submit a job which would have one speculative map and one speculative
    //reduce.
    FakeJobInProgress fjob1 = submitJob(JobStatus.PREP, conf);
    
    conf = new JobConf();
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(1);
    //Submit a job which has no speculative map or reduce.
    FakeJobInProgress fjob2 = submitJob(JobStatus.PREP, conf);    

    //Ask the poller to initalize all the submitted job and raise status
    //change event.
    controlledInitializationPoller.selectJobsToInitialize();
    raiseStatusChangeEvents(mgr);

    checkAssignments("tt1", 
        new String[] {
        "attempt_test_0001_m_000001_0 on tt1" , 
        "attempt_test_0001_r_000001_0 on tt1"});
    assertTrue("Pending maps of job1 greater than zero", 
        (fjob1.pendingMaps() == 0));
    assertTrue("Pending reduces of job1 greater than zero", 
        (fjob1.pendingReduces() == 0));
    checkAssignments("tt2", 
        new String[] {
        "attempt_test_0001_m_000001_1 on tt2", 
        "attempt_test_0001_r_000001_1 on tt2"});

    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000001_0", fjob1);
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_m_000001_1", fjob1);
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_r_000001_0", fjob1);
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_r_000001_1", fjob1);
    taskTrackerManager.finalizeJob(fjob1);
    
    checkAssignments("tt1", 
        new String[] {
        "attempt_test_0002_m_000001_0 on tt1", 
        "attempt_test_0002_r_000001_0 on tt1"});
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_m_000001_0", fjob2);
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_r_000001_0", fjob2);
    taskTrackerManager.finalizeJob(fjob2);    
  }

  /**
   * Test to verify that TTs are reserved for high memory jobs, but only till a
   * TT is reserved for each of the pending task.
   * @throws IOException
   */
  public void testTTReservingWithHighMemoryJobs()
      throws IOException {
    // 3 taskTrackers, 2 map and 0 reduce slots on each TT
    taskTrackerManager = new FakeTaskTrackerManager(3, 2, 0);

    taskTrackerManager.addQueues(new String[] { "default" });
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));
    resConf.setFakeQueues(queues);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    // enabled memory-based scheduling
    // Normal job in the cluster would be 1GB maps/reduces
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY, 2 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    LOG.debug("Submit a regular memory(1GB vmem maps/reduces) job of "
        + "1 map & 0 red tasks");
    JobConf jConf = new JobConf(conf);
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(0);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job1 = submitJobAndInit(JobStatus.PREP, jConf);
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    scheduler.updateQueueUsageForTests();
    LOG.info(job1.getSchedulingInfo());
    assertEquals(
        CapacityTaskScheduler.getJobQueueSchedInfo(1, 1, 0, 0, 0, 0), 
        (String) job1.getSchedulingInfo());

    jConf = new JobConf(conf);
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(0);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job2 = submitJobAndInit(JobStatus.PREP, jConf);
    checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
    scheduler.updateQueueUsageForTests();
    LOG.info(job2.getSchedulingInfo());
    assertEquals(
        CapacityTaskScheduler.getJobQueueSchedInfo(1, 1, 0, 0, 0, 0), 
        (String) job2.getSchedulingInfo());

    jConf = new JobConf(conf);
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(0);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job3 = submitJobAndInit(JobStatus.PREP, jConf);
    checkAssignment("tt3", "attempt_test_0003_m_000001_0 on tt3");
    scheduler.updateQueueUsageForTests();
    LOG.info(job3.getSchedulingInfo());
    assertEquals(
        CapacityTaskScheduler.getJobQueueSchedInfo(1, 1, 0, 0, 0, 0), 
        (String) job3.getSchedulingInfo());

    LOG.debug("Submit one high memory(2GB maps, 0MB reduces) job of "
        + "2 map tasks");
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(0);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(0);
    jConf.setQueueName("default");
    jConf.setUser("u2");
    FakeJobInProgress job4 = submitJobAndInit(JobStatus.PREP, jConf);

    LOG.debug("Submit another regular memory(1GB vmem maps/reduces) job of "
        + "2 map/red tasks");
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(2);
    jConf.setQueueName("default");
    jConf.setUser("u3");
    FakeJobInProgress job5 = submitJobAndInit(JobStatus.PREP, jConf);

    // Job4, a high memory job cannot be accommodated on a any TT. But with each
    // trip to the scheduler, each of the TT should be reserved by job2.
    assertEquals(0, scheduler.assignTasks(tracker("tt1")).size());
    scheduler.updateQueueUsageForTests();
    LOG.info(job4.getSchedulingInfo());
    assertEquals(
        CapacityTaskScheduler.getJobQueueSchedInfo(0, 0, 2, 0, 0, 0), 
        (String) job4.getSchedulingInfo());

    assertEquals(0, scheduler.assignTasks(tracker("tt2")).size());
    scheduler.updateQueueUsageForTests();
    LOG.info(job4.getSchedulingInfo());
    assertEquals(
        CapacityTaskScheduler.getJobQueueSchedInfo(0, 0, 4, 0, 0, 0), 
        (String) job4.getSchedulingInfo());

    // Job4 has only 2 pending tasks. So no more reservations. Job5 should get
    // slots on tt3. tt1 and tt2 should not be assigned any slots with the
    // reservation stats intact.
    assertEquals(0, scheduler.assignTasks(tracker("tt1")).size());
    scheduler.updateQueueUsageForTests();
    LOG.info(job4.getSchedulingInfo());
    assertEquals(
        CapacityTaskScheduler.getJobQueueSchedInfo(0, 0, 4, 0, 0, 0), 
        (String) job4.getSchedulingInfo());

    assertEquals(0, scheduler.assignTasks(tracker("tt2")).size());
    scheduler.updateQueueUsageForTests();
    LOG.info(job4.getSchedulingInfo());
    assertEquals(
        CapacityTaskScheduler.getJobQueueSchedInfo(0, 0, 4, 0, 0, 0), 
        (String) job4.getSchedulingInfo());

    checkAssignments("tt3", 
        new String[] {
        "attempt_test_0005_m_000001_0 on tt3"});
    scheduler.updateQueueUsageForTests();
    LOG.info(job5.getSchedulingInfo());
    assertEquals(
        CapacityTaskScheduler.getJobQueueSchedInfo(1, 1, 0, 0, 0, 0), 
        (String) job5.getSchedulingInfo());

    assertEquals(
        CapacityTaskScheduler.getJobQueueSchedInfo(0, 0, 4, 0, 0, 0), 
        (String) job4.getSchedulingInfo());

    // No more tasks there in job3 also
    assertEquals(0, scheduler.assignTasks(tracker("tt3")).size());
  }

  /**
   * Test to verify that TTs are not reserved in case the required memory
   * exceeds the total availability of memory on TT.
   * @throws IOException
   */
  public void testTTReservingInHeterogenousEnvironment()
      throws IOException {
    // 2 taskTrackers, 4 map slots on one and 3 map slot on another.
    taskTrackerManager = new FakeTaskTrackerManager(1, 4, 0);
    taskTrackerManager.addTaskTracker("tt2", 3, 0);

    taskTrackerManager.addQueues(new String[] { "default" });
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));
    resConf.setFakeQueues(queues);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    // enabled memory-based scheduling
    // Normal job in the cluster would be 2GB maps/reduces
    // Max allowed map memory would be 8GB.
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY, 8 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 2 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY, 8 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 2 * 1024);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    LOG.debug("Submit a  memory(7GB vmem maps/reduces) job of "
        + "2 map & 0 red tasks");
    JobConf jConf = new JobConf(conf);

    jConf = new JobConf(conf);
    // We require 7GB maps, so thats worth 4 slots on the cluster.
    jConf.setMemoryForMapTask(7 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    // Hence, 4 + 4 slots are required totally, for two tasks.
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(0);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job = submitJobAndInit(JobStatus.PREP, jConf);
    // Heartbeating the trackers
    scheduler.assignTasks(tracker("tt1"));
    scheduler.assignTasks(tracker("tt2"));
    scheduler.updateQueueUsageForTests();
    LOG.info(job.getSchedulingInfo());
    // tt2 can at most run 3 slots while each map task of this job requires
    // at least 4 minimum slots to run.
    // tt2 should not at all be reserved, hence. Since it would be a waste of
    // slots for other jobs.
    assertEquals("Tracker tt2 got reserved unnecessarily.",
        0, scheduler.getMapScheduler().getNumReservedTaskTrackers(job));
    assertEquals(
        // Should be running only one map task worth four slots,
        // and no reservations.
        CapacityTaskScheduler.getJobQueueSchedInfo(1, 4, 0, 0, 0, 0),
        (String) job.getSchedulingInfo());
    jConf = new JobConf(conf);
    // Try submitting a 3-slot worthy job, targeting tt2
    // 5 GB should be worth 3 slots (2GB/map)
    jConf.setMemoryForMapTask(5 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    // Just one task, targetting an unreserved tt2
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(0);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    submitJobAndInit(JobStatus.PREP, jConf);
    // TT2 should get assigned.
    checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
  }

  /**
   * Test to verify that queue ordering is based on the number of slots occupied
   * and hence to verify that presence of high memory jobs is reflected properly
   * while determining used capacities of queues and hence the queue ordering.
   * 
   * @throws IOException
   */
  public void testQueueOrdering()
      throws IOException {
    taskTrackerManager = new FakeTaskTrackerManager(2, 6, 6);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    String[] qs = { "default", "q1" };
    String[] reversedQs = { qs[1], qs[0] };
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 100));
    queues.add(new FakeQueueInfo("q1", 50.0f, true, 100));
    resConf.setFakeQueues(queues);
    // enabled memory-based scheduling
    // Normal job in the cluster would be 1GB maps/reduces
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY, 2 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    LOG.debug("Submit one high memory(2GB maps, 2GB reduces) job of "
        + "6 map and 6 reduce tasks");
    JobConf jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(2 * 1024);
    jConf.setNumMapTasks(6);
    jConf.setNumReduceTasks(6);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job1 = submitJobAndInit(JobStatus.PREP, jConf);

    // Submit a normal job to the other queue.
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(6);
    jConf.setNumReduceTasks(6);
    jConf.setUser("u2");
    jConf.setQueueName("q1");
    FakeJobInProgress job2 = submitJobAndInit(JobStatus.PREP, jConf);

    // Map 1 of high memory job
    // Map 1 of normal job
    // Map 1 of normal job
    // Map 1 of normal job, since comparator won't change on equals
    // Reduce 1 of high memory job
    checkAssignments("tt1", 
        new String[] {
        "attempt_test_0001_m_000001_0 on tt1",
        "attempt_test_0002_m_000001_0 on tt1",
        "attempt_test_0002_m_000002_0 on tt1",
        "attempt_test_0002_m_000003_0 on tt1",
        "attempt_test_0001_r_000001_0 on tt1"
        });
    checkQueuesOrder(qs, scheduler
        .getOrderedQueues(TaskType.MAP));
    checkQueuesOrder(qs, scheduler
        .getOrderedQueues(TaskType.REDUCE));
    scheduler.updateQueueUsageForTests();
    assertEquals(
        CapacityTaskScheduler.getJobQueueSchedInfo(1, 2, 2, 1, 2, 0), 
        (String) job1.getSchedulingInfo());
    assertEquals(
        CapacityTaskScheduler.getJobQueueSchedInfo(3, 3, 0, 0, 0, 0), 
        (String) job2.getSchedulingInfo());


    // Reduce 1 of normal job
    checkAssignment("tt1", "attempt_test_0002_r_000001_0 on tt1");
    checkQueuesOrder(reversedQs, scheduler
        .getOrderedQueues(TaskType.REDUCE));
    scheduler.updateQueueUsageForTests();
    assertEquals(
        CapacityTaskScheduler.getJobQueueSchedInfo(1, 2, 2, 1, 2, 0), 
        (String) job1.getSchedulingInfo());
    assertEquals(
        CapacityTaskScheduler.getJobQueueSchedInfo(3, 3, 0, 1, 1, 0), 
        (String) job2.getSchedulingInfo());

    // Reduce 2 of normal job
    checkAssignment("tt1", "attempt_test_0002_r_000002_0 on tt1");
    checkQueuesOrder(reversedQs, scheduler
        .getOrderedQueues(TaskType.REDUCE));
    scheduler.updateQueueUsageForTests();
    assertEquals(
        CapacityTaskScheduler.getJobQueueSchedInfo(1, 2, 2, 1, 2, 0), 
        (String) job1.getSchedulingInfo());
    assertEquals(
        CapacityTaskScheduler.getJobQueueSchedInfo(3, 3, 0, 2, 2, 0), 
        (String) job2.getSchedulingInfo());

    // Reduce 3 of normal job
    checkAssignment("tt1", "attempt_test_0002_r_000003_0 on tt1");
    checkQueuesOrder(reversedQs, scheduler
        .getOrderedQueues(TaskType.REDUCE));
    scheduler.updateQueueUsageForTests();
    assertEquals(
        CapacityTaskScheduler.getJobQueueSchedInfo(1, 2, 2, 1, 2, 0), 
        (String) job1.getSchedulingInfo());
    assertEquals(
        CapacityTaskScheduler.getJobQueueSchedInfo(3, 3, 0, 3, 3, 0), 
        (String) job2.getSchedulingInfo());

    // Map 2 of high memory job
    // Map 4 of normal job
    // Map 4 of normal job
    // Reduce 2 of high memory job
    checkAssignments("tt2", 
        new String[] {
        "attempt_test_0002_m_000004_0 on tt2",
        "attempt_test_0002_m_000005_0 on tt2",
        "attempt_test_0001_m_000002_0 on tt2",
        "attempt_test_0002_m_000006_0 on tt2",
        "attempt_test_0001_r_000002_0 on tt2"
    });
    checkQueuesOrder(reversedQs, scheduler
        .getOrderedQueues(TaskType.MAP));
    checkQueuesOrder(qs, scheduler
        .getOrderedQueues(TaskType.REDUCE));
    scheduler.updateQueueUsageForTests();
    assertEquals( // user limit is 6
        CapacityTaskScheduler.getJobQueueSchedInfo(2, 4, 2, 2, 4, 0), 
        (String) job1.getSchedulingInfo());
    assertEquals(
        CapacityTaskScheduler.getJobQueueSchedInfo(6, 6, 0, 3, 3, 0), 
        (String) job2.getSchedulingInfo());

    // Reduce 4 of normal job
    checkAssignment("tt2", "attempt_test_0002_r_000004_0 on tt2");
    checkQueuesOrder(reversedQs, scheduler
        .getOrderedQueues(TaskType.REDUCE));
    scheduler.updateQueueUsageForTests();
    assertEquals(// user limit is 6
        CapacityTaskScheduler.getJobQueueSchedInfo(2, 4, 2, 2, 4, 0), 
        (String) job1.getSchedulingInfo());
    assertEquals(
        CapacityTaskScheduler.getJobQueueSchedInfo(6, 6, 0, 4, 4, 0), 
        (String) job2.getSchedulingInfo());
  }

  private void checkFailedInitializedJobMovement() throws IOException {
    
    JobQueuesManager mgr = scheduler.jobQueuesManager;
    JobInitializationPoller p = scheduler.getInitializationPoller();
    
    //submit a job
    FakeJobInProgress job = submitJob(JobStatus.PREP, 1, 1, "default", "u1");
    //Initialize the job
    p.selectJobsToInitialize();
    //Don't raise the status change event.
    
    CapacitySchedulerQueue queue = mgr.getQueue("default");
    
    //check in waiting and initialized jobs list.
    assertTrue("Waiting jobs list does contain the job",
        !queue.getWaitingJobs().contains(job));
    assertTrue("Initialzing jobs does not contain the job",
        !queue.getInitializingJobs().contains(job));
    
    assertTrue("Initialized job does not contain the job",
        p.getInitializedJobList().contains(job.getJobID()));
    
    //fail the initalized job
    taskTrackerManager.finalizeJob(job, JobStatus.KILLED);
    
    //Check if the job is present in waiting queue
    assertFalse("Waiting jobs list contains failed job",
        queue.getWaitingJobs().contains(job));
    
    //run the poller to do the cleanup
    p.selectJobsToInitialize();
    
    //check for failed job in the initialized job list
    assertFalse("Initialized jobs  contains failed job",
        p.getInitializedJobList().contains(job.getJobID()));
  }
  
  private void checkFailedWaitingJobMovement() throws IOException {
    JobQueuesManager mgr = scheduler.jobQueuesManager;
    // submit a job
    FakeJobInProgress job = submitJob(JobStatus.PREP, 1, 1, "default",
        "u1");
    
    // check in waiting and initialized jobs list.
    CapacitySchedulerQueue queue = mgr.getQueue("default");
    assertTrue("Waiting jobs list does not contain the job", 
        queue.getWaitingJobs().contains(job));
    // fail the waiting job
    taskTrackerManager.finalizeJob(job, JobStatus.KILLED);

    // Check if the job is present in waiting queue
    assertFalse("Waiting jobs list contains failed job", 
        queue.getWaitingJobs().contains(job));
  }
  
  private void raiseStatusChangeEvents(JobQueuesManager mgr) {
    raiseStatusChangeEvents(mgr, "default");
  }
  
  private void raiseStatusChangeEvents(JobQueuesManager mgr, String queueName) {
    Collection<JobInProgress> jips = mgr.getQueue(queueName).getInitializingJobs();
    for(JobInProgress jip : jips) {
      if(jip.getStatus().getRunState() == JobStatus.RUNNING) {
        JobStatusChangeEvent evt = new JobStatusChangeEvent(jip,
            EventType.RUN_STATE_CHANGED,jip.getStatus());
        mgr.jobUpdated(evt);
      }
    }
  }

  private HashMap<String, ArrayList<FakeJobInProgress>> submitJobs(
      int numberOfUsers, int numberOfJobsPerUser, String queue)
      throws Exception{
    HashMap<String, ArrayList<FakeJobInProgress>> userJobs = 
      new HashMap<String, ArrayList<FakeJobInProgress>>();
    for (int i = 1; i <= numberOfUsers; i++) {
      String user = String.valueOf("u" + i);
      ArrayList<FakeJobInProgress> jips = new ArrayList<FakeJobInProgress>();
      for (int j = 1; j <= numberOfJobsPerUser; j++) {
        jips.add(submitJob(JobStatus.PREP, 1, 1, queue, user));
      }
      userJobs.put(user, jips);
    }
    return userJobs;
  }

  
  protected TaskTracker tracker(String taskTrackerName) {
    return taskTrackerManager.getTaskTracker(taskTrackerName);
  }
  
  protected Task checkAssignment(String taskTrackerName,
      String expectedTaskString) throws IOException {
    List<Task> tasks = scheduler.assignTasks(tracker(taskTrackerName));
    assertNotNull(expectedTaskString, tasks);
    assertEquals(expectedTaskString, 1, tasks.size());
    assertEquals(expectedTaskString, tasks.get(0).toString());
    return tasks.get(0);
  }

  protected String getAssignedTasks(List<Task> tasks) {
    if (tasks.size() == 0) {
      return "<empty>";
    }
    StringBuffer s = new StringBuffer(tasks.get(0).toString());
    for (int i=1; i < tasks.size(); ++i) {
      s.append(", ");
      s.append(tasks.get(i).toString());
    }
    return s.toString();
  }
  
  protected List<Task> checkAssignments(String taskTrackerName,
      String[] expectedTasks) throws IOException {
    List<Task> tasks = scheduler.assignTasks(tracker(taskTrackerName));
    assertNotNull(getAssignedTasks(tasks), tasks);
    assertEquals(getAssignedTasks(tasks), expectedTasks.length, tasks.size());
    for (int i=0; i < tasks.size(); ++i) {
      assertEquals(getAssignedTasks(tasks) + " -> " + expectedTasks[i], 
                   expectedTasks[i], tasks.get(i).toString());
    }
    return tasks;
  }

  /**
   * Get the amount of memory that is reserved for tasks on the taskTracker and
   * verify that it matches what is expected.
   * 
   * @param taskTracker
   * @param expectedMemForMapsOnTT
   * @param expectedMemForReducesOnTT
   */
  private void checkMemReservedForTasksOnTT(String taskTracker,
      Long expectedMemForMapsOnTT, Long expectedMemForReducesOnTT,
      int numAvailableMapSlots, int numAvailableReduceSlots) {
    Long observedMemForMapsOnTT =
      scheduler.memoryMatcher.getMemReservedForTasks(
        tracker(taskTracker).getStatus(),
            TaskType.MAP, numAvailableMapSlots);
    Long observedMemForReducesOnTT =
      scheduler.memoryMatcher.getMemReservedForTasks(
        tracker(taskTracker).getStatus(),
            TaskType.REDUCE, numAvailableReduceSlots);
    if (expectedMemForMapsOnTT == null) {
      assertEquals(observedMemForMapsOnTT, null);
    } else {
      assertEquals(observedMemForMapsOnTT, (expectedMemForMapsOnTT));
    }
    if (expectedMemForReducesOnTT == null) {
      assertEquals(observedMemForReducesOnTT, null);
    } else {
      assertEquals(observedMemForReducesOnTT, (expectedMemForReducesOnTT));
    }
  }

  /**
   * Verify the number of slots of type 'type' from the queue 'queue'.
   * incrMapIndex and incrReduceIndex are set , when expected output string is
   * changed.these values can be set if the index of
   * "Used capacity: %d (%.1f%% of Capacity)"
   * is changed.
   * 
   * @param queue
   * @param type
   * @param numActiveUsers in the queue at present.
   * @param expectedOccupiedSlots
   * @param expectedOccupiedSlotsPercent
   * @param incrMapIndex
   * @param incrReduceIndex
   */
  private void checkOccupiedSlots(
    String queue,
    TaskType type, int numActiveUsers,
    int expectedOccupiedSlots, float expectedOccupiedSlotsPercent,int incrMapIndex
    ,int incrReduceIndex
  ) {
    scheduler.updateQueueUsageForTests();
    QueueManager queueManager = scheduler.taskTrackerManager.getQueueManager();
    String schedulingInfo =
        queueManager.getJobQueueInfo(queue).getSchedulingInfo();
    String[] infoStrings = schedulingInfo.split("\n");
    int index = -1;
    if (type.equals(TaskType.MAP)) {
      index = 7+ incrMapIndex;
    } else if (type.equals(TaskType.REDUCE)) {
      index = (numActiveUsers == 0 ? 12 : 13 + numActiveUsers)+incrReduceIndex;
    }
    LOG.info(infoStrings[index]);
    assertEquals(String.format("Used capacity: %d (%.1f%% of Capacity)",
        expectedOccupiedSlots, expectedOccupiedSlotsPercent),
        infoStrings[index]);
  }

  /**
   *
   * @param queue
   * @param type
   * @param numActiveUsers
   * @param expectedOccupiedSlots
   * @param expectedOccupiedSlotsPercent
   */
  private void checkOccupiedSlots(
    String queue,
    TaskType type, int numActiveUsers,
    int expectedOccupiedSlots, float expectedOccupiedSlotsPercent
  ) {
    checkOccupiedSlots(
      queue, type, numActiveUsers, expectedOccupiedSlots,
      expectedOccupiedSlotsPercent,0,0);
  }

  private void checkQueuesOrder(String[] expectedOrder, String[] observedOrder) {
    assertTrue("Observed and expected queues are not of same length.",
        expectedOrder.length == observedOrder.length);
    int i = 0;
    for (String expectedQ : expectedOrder) {
      assertTrue("Observed and expected queues are not in the same order. "
          + "Differ at index " + i + ". Got " + observedOrder[i]
          + " instead of " + expectedQ, expectedQ.equals(observedOrder[i]));
      i++;
    }
  }

  public void testDeprecatedMemoryValues() throws IOException {
    // 2 map and 1 reduce slots
    taskTrackerManager.addQueues(new String[] { "default" });
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 25));  
    resConf.setFakeQueues(queues);
    JobConf conf = (JobConf)(scheduler.getConf());
    conf.set(
      JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY, String.valueOf(
        1024 * 1024 * 3));
    scheduler.setTaskTrackerManager(taskTrackerManager);
    scheduler.setResourceManagerConf(resConf);    
    scheduler.start();

    assertEquals(scheduler.getLimitMaxMemForMapSlot(),3);
    assertEquals(scheduler.getLimitMaxMemForReduceSlot(),3);
  }

  /**
   * Checks for multiple assignment.
   *
   * @param taskTrackerName
   * @param mapAttempt
   * @param reduceAttempt
   * @return
   * @throws IOException
   */
  private List<Task> checkMultipleAssignment(
    String taskTrackerName, String mapAttempt, String reduceAttempt)
    throws IOException {
    List<Task> tasks = scheduler.assignTasks(tracker(taskTrackerName));
    LOG.info(
      " mapAttempt " + mapAttempt + " reduceAttempt " + reduceAttempt +
        " assignTasks result " + tasks);

    if (tasks == null || tasks.isEmpty()) {
      if (mapAttempt != null || reduceAttempt != null ) {
        fail(
          " improper attempt " + tasks + " expected attempts are  map : " +
            mapAttempt + " reduce : " + reduceAttempt);
      } else {
        return tasks;
      }
    }
    
    if (tasks.size() == 1 && (mapAttempt != null && reduceAttempt != null)) {
      fail(
        " improper attempt " + tasks + " expected attempts are  map : " +
          mapAttempt + " reduce : " + reduceAttempt);
    }
    for (Task task : tasks) {
      if (task.toString().contains("_m_")) {
        assertEquals(task.toString(), mapAttempt);
      }

      if (task.toString().contains("_r")) {
        assertEquals(task.toString(), reduceAttempt);
      }
    }
    return tasks;
  }
}
