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

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.mapreduce.split.JobSplit;

public class TestJobQueueTaskScheduler extends TestCase {
  
  private static int jobCounter;
  private static int taskCounter;
  
  static void resetCounters() {
    jobCounter = 0;
    taskCounter = 0;
  }
  
  static class FakeJobInProgress extends JobInProgress {
    
    private FakeTaskTrackerManager taskTrackerManager;
    
    public FakeJobInProgress(JobConf jobConf,
        FakeTaskTrackerManager taskTrackerManager, JobTracker jt) 
          throws IOException {
      super(new JobID("test", ++jobCounter), jobConf, jt);
      this.taskTrackerManager = taskTrackerManager;
      this.startTime = System.currentTimeMillis();
      this.status.setJobPriority(JobPriority.NORMAL);
      this.status.setStartTime(startTime);
    }

    @Override
    public synchronized void initTasks() throws IOException {
      // do nothing
    }

    @Override
    public Task obtainNewLocalMapTask(TaskTrackerStatus tts, int clusterSize, 
                                      int ignored) 
    throws IOException {
      return obtainNewMapTask(tts, clusterSize, ignored);
    }
    
    @Override
    public Task obtainNewNonLocalMapTask(TaskTrackerStatus tts, int clusterSize, 
                                         int ignored) 
    throws IOException {
      return obtainNewMapTask(tts, clusterSize, ignored);
    }
    
    @Override
    public Task obtainNewMapTask(final TaskTrackerStatus tts, int clusterSize,
        int ignored) throws IOException {
      TaskAttemptID attemptId = getTaskAttemptID(true);
      Task task = new MapTask("", attemptId, 0, new JobSplit.TaskSplitIndex(),
          1) {
        @Override
        public String toString() {
          return String.format("%s on %s", getTaskID(), tts.getTrackerName());
        }
      };
      taskTrackerManager.update(tts.getTrackerName(), task);
      runningMapTasks++;
      return task;
    }
    
    @Override
    public Task obtainNewReduceTask(final TaskTrackerStatus tts,
        int clusterSize, int ignored) throws IOException {
      TaskAttemptID attemptId = getTaskAttemptID(false);
      Task task = new ReduceTask("", attemptId, 0, 10, 1) {
        @Override
        public String toString() {
          return String.format("%s on %s", getTaskID(), tts.getTrackerName());
        }
      };
      taskTrackerManager.update(tts.getTrackerName(), task);
      runningReduceTasks++;
      return task;
    }
    
    private TaskAttemptID getTaskAttemptID(boolean isMap) {
      JobID jobId = getJobID();
      return new TaskAttemptID(jobId.getJtIdentifier(),
          jobId.getId(), isMap, ++taskCounter, 0);
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
    
    private Map<String, TaskTracker> trackers = 
      new HashMap<String, TaskTracker>();

    public FakeTaskTrackerManager() {
      JobConf conf = new JobConf();
      queueManager = new QueueManager(conf);
      
      TaskTracker tt1 = new TaskTracker("tt1");
      tt1.setStatus(new TaskTrackerStatus("tt1", "tt1.host", 1,
                    new ArrayList<TaskStatus>(), 0, 0,
                    maxMapTasksPerTracker, maxReduceTasksPerTracker));
      trackers.put("tt1", tt1);
      
      TaskTracker tt2 = new TaskTracker("tt2");
      tt2.setStatus(new TaskTrackerStatus("tt2", "tt2.host", 2,
                    new ArrayList<TaskStatus>(), 0, 0,
                    maxMapTasksPerTracker, maxReduceTasksPerTracker));
      trackers.put("tt2", tt2);
    }
    
    @Override
    public ClusterStatus getClusterStatus() {
      int numTrackers = trackers.size();
      return new ClusterStatus(numTrackers, 0, 
                               JobTracker.TASKTRACKER_EXPIRY_INTERVAL,
                               maps, reduces,
                               numTrackers * maxMapTasksPerTracker,
                               numTrackers * maxReduceTasksPerTracker,
                               JobTracker.State.RUNNING);
    }

    @Override
    public int getNumberOfUniqueHosts() {
      return 0;
    }

    @Override
    public Collection<TaskTrackerStatus> taskTrackers() {
      List<TaskTrackerStatus> taskTrackers = new ArrayList<TaskTrackerStatus>();
      for (TaskTracker tt : trackers.values()) {
        taskTrackers.add(tt.getStatus());
      }
      return taskTrackers;
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
    public QueueManager getQueueManager() {
      return queueManager;
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
      return null;
    }

    public void initJob(JobInProgress job) {
      // do nothing
    }
    
    public void failJob(JobInProgress job) {
      // do nothing
    }

    @Override
    public boolean killTask(TaskAttemptID attemptId, boolean shouldFail) {
      return true;
    }

    
    // Test methods
    
    public void submitJob(JobInProgress job) throws IOException {
      for (JobInProgressListener listener : listeners) {
        listener.jobAdded(job);
      }
    }
    
    public TaskTracker getTaskTracker(String trackerID) {
      return trackers.get(trackerID);
    }
    
    public void update(String taskTrackerName, final Task t) {
      if (t.isMapTask()) {
        maps++;
      } else {
        reduces++;
      }
      TaskStatus status = new TaskStatus() {
        @Override
        public boolean getIsMap() {
          return t.isMapTask();
        }
      };
      status.setRunState(TaskStatus.State.RUNNING);
      trackers.get(taskTrackerName).getStatus().getTaskReports().add(status);
    }
    
  }
  
  protected JobConf jobConf;
  protected TaskScheduler scheduler;
  private FakeTaskTrackerManager taskTrackerManager;

  @Override
  protected void setUp() throws Exception {
    resetCounters();
    jobConf = new JobConf();
    jobConf.setNumMapTasks(10);
    jobConf.setNumReduceTasks(10);
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
  
  static void submitJobs(FakeTaskTrackerManager taskTrackerManager, JobConf jobConf, 
                         int numJobs, int state)
    throws IOException {
    for (int i = 0; i < numJobs; i++) {
      JobInProgress job = new FakeJobInProgress(jobConf, taskTrackerManager, 
          UtilsForTests.getJobTracker());
      job.getStatus().setRunState(state);
      taskTrackerManager.submitJob(job);
    }
  }

  public void testTaskNotAssignedWhenNoJobsArePresent() throws IOException {
    assertEquals(0, scheduler.assignTasks(tracker(taskTrackerManager, "tt1")).size());
  }

  public void testNonRunningJobsAreIgnored() throws IOException {
    submitJobs(taskTrackerManager, jobConf, 1, JobStatus.PREP);
    submitJobs(taskTrackerManager, jobConf, 1, JobStatus.SUCCEEDED);
    submitJobs(taskTrackerManager, jobConf, 1, JobStatus.FAILED);
    submitJobs(taskTrackerManager, jobConf, 1, JobStatus.KILLED);
    assertEquals(0, scheduler.assignTasks(tracker(taskTrackerManager, "tt1")).size());
  }
  
  public void testDefaultTaskAssignment() throws IOException {
    submitJobs(taskTrackerManager, jobConf, 2, JobStatus.RUNNING);
    // All slots are filled with job 1
    checkAssignment(scheduler, tracker(taskTrackerManager, "tt1"), 
                    new String[] {"attempt_test_0001_m_000001_0 on tt1", 
                                  "attempt_test_0001_m_000002_0 on tt1", 
                                  "attempt_test_0001_r_000003_0 on tt1"});
    checkAssignment(scheduler, tracker(taskTrackerManager, "tt1"), 
                    new String[] {"attempt_test_0001_r_000004_0 on tt1"});
    checkAssignment(scheduler, tracker(taskTrackerManager, "tt1"), new String[] {});
    checkAssignment(scheduler, tracker(taskTrackerManager, "tt2"), 
                    new String[] {"attempt_test_0001_m_000005_0 on tt2", 
                                         "attempt_test_0001_m_000006_0 on tt2", 
                                         "attempt_test_0001_r_000007_0 on tt2"});
    checkAssignment(scheduler, tracker(taskTrackerManager, "tt2"), 
                    new String[] {"attempt_test_0001_r_000008_0 on tt2"});
    checkAssignment(scheduler, tracker(taskTrackerManager, "tt2"), new String[] {});
    checkAssignment(scheduler, tracker(taskTrackerManager, "tt1"), new String[] {});
    checkAssignment(scheduler, tracker(taskTrackerManager, "tt2"), new String[] {});
  }

  static TaskTracker tracker(FakeTaskTrackerManager taskTrackerManager,
                                      String taskTrackerName) {
    return taskTrackerManager.getTaskTracker(taskTrackerName);
  }
  
  static void checkAssignment(TaskScheduler scheduler, TaskTracker taskTracker,
      String[] expectedTaskStrings) throws IOException {
    List<Task> tasks = scheduler.assignTasks(taskTracker);
    assertNotNull(tasks);
    assertEquals(expectedTaskStrings.length, tasks.size());
    for (int i=0; i < expectedTaskStrings.length; ++i) {
      assertEquals(expectedTaskStrings[i], tasks.get(i).toString());
    }
  }
  
}
