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
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobInProgress.KillInterruptedException;
import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;

public class TestParallelInitialization extends TestCase {
  
  private static int jobCounter;
  private static final int NUM_JOBS = 3;
  IntWritable numJobsCompleted = new IntWritable();
  
  static void resetCounters() {
    jobCounter = 0;
  }
  
  class FakeJobInProgress extends JobInProgress {
   
    public FakeJobInProgress(JobConf jobConf,
        FakeTaskTrackerManager taskTrackerManager,
        JobTracker jt) throws IOException {
      super(new JobID("test", ++jobCounter), jobConf,
          jt);
      this.startTime = System.currentTimeMillis();
      this.status.setJobPriority(JobPriority.NORMAL);
      this.status.setStartTime(startTime);
    }

    @Override
    public synchronized void initTasks() throws IOException {
      try {
        int jobNumber = this.getJobID().getId();
        synchronized (numJobsCompleted) {
          while (numJobsCompleted.get() != (NUM_JOBS - jobNumber)) {
            numJobsCompleted.wait();
          }
          numJobsCompleted.set(numJobsCompleted.get() + 1);
          numJobsCompleted.notifyAll();
          LOG.info("JobNumber " + jobNumber + " succeeded");
        }
      } catch (InterruptedException ie) {};
      this.status.setRunState(JobStatus.SUCCEEDED);
    }

    @Override
    synchronized void fail() {
      this.status.setRunState(JobStatus.FAILED);
    }
  }
  
  static class FakeTaskTrackerManager implements TaskTrackerManager {
    
    int maps = 0;
    int reduces = 0;
    int maxMapTasksPerTracker = 2;
    int maxReduceTasksPerTracker = 2;
    List<JobInProgressListener> listeners =
      new ArrayList<JobInProgressListener>();
    QueueManager queueManager;
    
    private Map<String, TaskTrackerStatus> trackers =
      new HashMap<String, TaskTrackerStatus>();

    public FakeTaskTrackerManager() {
      JobConf conf = new JobConf();
      queueManager = new QueueManager(conf);
      trackers.put("tt1", new TaskTrackerStatus("tt1", "tt1.host", 1,
                   new ArrayList<TaskStatus>(), 0, 0,
                   maxMapTasksPerTracker, maxReduceTasksPerTracker));
    }
    
    public ClusterStatus getClusterStatus() {
      int numTrackers = trackers.size();
      return new ClusterStatus(numTrackers, 0, 
                               JobTracker.TASKTRACKER_EXPIRY_INTERVAL,
                               maps, reduces,
                               numTrackers * maxMapTasksPerTracker,
                               numTrackers * maxReduceTasksPerTracker,
                               JobTracker.State.RUNNING);
    }
    
    public int getNumberOfUniqueHosts() {
      return 0;
    }

    public Collection<TaskTrackerStatus> taskTrackers() {
      return trackers.values();
    }

    public void addJobInProgressListener(JobInProgressListener listener) {
      listeners.add(listener);
    }

    public void removeJobInProgressListener(JobInProgressListener listener) {
      listeners.remove(listener);
    }
    
    
    public QueueManager getQueueManager() {
      return queueManager;
    }
    
    public int getNextHeartbeatInterval() {
      return MRConstants.HEARTBEAT_INTERVAL_MIN_DEFAULT;
    }

    public void killJob(JobID jobid) {
      return;
    }

    public JobInProgress getJob(JobID jobid) {
      return null;
    }

    public void initJob(JobInProgress job) {
      try {
        JobStatus prevStatus = (JobStatus)job.getStatus().clone();
        job.initTasks();
        JobStatus newStatus = (JobStatus)job.getStatus().clone();
        if (prevStatus.getRunState() != newStatus.getRunState()) {
          JobStatusChangeEvent event = 
            new JobStatusChangeEvent(job, EventType.RUN_STATE_CHANGED, prevStatus, 
                newStatus);
          for (JobInProgressListener listener : listeners) {
            listener.jobUpdated(event);
          }
        }
      } catch (Exception ioe) {
        failJob(job);
      }
    }

    public boolean killTask(TaskAttemptID attemptId, boolean shouldFail) {
      return true;
    }

    // Test methods
    
    public synchronized void failJob(JobInProgress job) {
      JobStatus prevStatus = (JobStatus)job.getStatus().clone();
      job.fail();
      JobStatus newStatus = (JobStatus)job.getStatus().clone();
      if (prevStatus.getRunState() != newStatus.getRunState()) {
        JobStatusChangeEvent event = 
          new JobStatusChangeEvent(job, EventType.RUN_STATE_CHANGED, prevStatus, 
              newStatus);
        for (JobInProgressListener listener : listeners) {
          listener.jobUpdated(event);
        }
      }
    }
    
    public void submitJob(JobInProgress job) throws IOException {
      for (JobInProgressListener listener : listeners) {
        listener.jobAdded(job);
      }
    }
  }
  
  protected JobConf jobConf;
  protected TaskScheduler scheduler;
  private FakeTaskTrackerManager taskTrackerManager;

  @Override
  protected void setUp() throws Exception {
    resetCounters();
    jobConf = new JobConf();
    taskTrackerManager = new FakeTaskTrackerManager();
    scheduler = createTaskScheduler();
    scheduler.setConf(jobConf);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    scheduler.start();
  }
  
  @Override
  protected void tearDown() throws Exception {
    if (scheduler != null) {
      scheduler.terminate();
    }
  }
  
  protected TaskScheduler createTaskScheduler() {
    return new JobQueueTaskScheduler();
  }
  
  public void testParallelInitJobs() throws IOException {
    FakeJobInProgress[] jobs = new FakeJobInProgress[NUM_JOBS];
    
    // Submit NUM_JOBS jobs in order. The init code will ensure
    // that the jobs get inited in descending order of Job ids
    // i.e. highest job id first and the smallest last.
    // If we were not doing parallel init, the first submitted job
    // will be inited first and that will hang
    
    for (int i = 0; i < NUM_JOBS; i++) {
      jobs[i] = new FakeJobInProgress(jobConf, taskTrackerManager, 
          UtilsForTests.getJobTracker());
      jobs[i].getStatus().setRunState(JobStatus.PREP);
      taskTrackerManager.submitJob(jobs[i]);
    }
    
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ie) {}
    
    for (int i = 0; i < NUM_JOBS; i++) {
      assertTrue(jobs[i].getStatus().getRunState() == JobStatus.SUCCEEDED);
    }
  }  
}
