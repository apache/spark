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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Class to test that ClusterMetrics are being created with the right
 * counts of occupied and reserved slots.
 * 
 * The tests exercise code paths where the counts of slots are updated.
 */
public class TestClusterStatus extends TestCase {

  private static String[] trackers = new String[] { "tracker_tracker1:1000",
      "tracker_tracker2:1000", "tracker_tracker3:1000" };
  private static JobTracker jobTracker;
  private static int mapSlotsPerTracker = 4;
  private static int reduceSlotsPerTracker = 2;
  private static MiniMRCluster mr;
  private static JobClient client;
  // heartbeat responseId. increment this after sending a heartbeat
  private static short responseId = 1;
  private static FakeJobInProgress fakeJob;
  private static FakeTaskScheduler scheduler;
  
  public static Test suite() {
    TestSetup setup = new TestSetup(new TestSuite(TestClusterStatus.class)) {
      protected void setUp() throws Exception {
        JobConf conf = new JobConf();
        conf.setClass("mapred.jobtracker.taskScheduler", 
            TestClusterStatus.FakeTaskScheduler.class,
                  TaskScheduler.class);
        mr = new MiniMRCluster(0, 0, 0, "file:///", 1, null, null, null, conf);
        jobTracker = mr.getJobTrackerRunner().getJobTracker();
        for (String tracker : trackers) {
          establishFirstContact(jobTracker, tracker);
        }
        client = new JobClient(mr.createJobConf());
      }

      protected void tearDown() throws Exception {
        client.close();
        mr.shutdown();
      }
    };
    return setup;
  }

  /**
   * Fake scheduler to test reservations.
   * 
   * The reservations are updated incrementally in each
   * heartbeat to pass through the re-reservation logic,
   * until the scheduler is asked to unreserve slots.
   */
  static class FakeTaskScheduler extends JobQueueTaskScheduler {
    
    private Map<TaskTracker, Integer> reservedCounts 
      = new HashMap<TaskTracker, Integer>();
  
    // this variable can be set to trigger unreservations.
    private boolean unreserveSlots;
    
    public FakeTaskScheduler() {
      super();
      scheduler = this;
    }

    void setUnreserveSlots(boolean shouldUnreserve) {
      unreserveSlots = shouldUnreserve;
    }
    
    @Override
    public List<Task> assignTasks(TaskTracker tt) {
      if (unreserveSlots) {
        tt.unreserveSlots(TaskType.MAP, fakeJob);
        tt.unreserveSlots(TaskType.REDUCE, fakeJob);
      } else {
        int currCount = 1;
        if (reservedCounts.containsKey(tt)) {
          currCount = reservedCounts.get(tt) + 1;
        }
        reservedCounts.put(tt, currCount);
        tt.reserveSlots(TaskType.MAP, fakeJob, currCount);
        tt.reserveSlots(TaskType.REDUCE, fakeJob, currCount);
      }
      return new ArrayList<Task>();  
    }
  }

  /**
   * Fake class for JobInProgress to allow testing reservation
   * counts.
   * 
   * This class can only be used to test functionality related to
   * reservations, and not other aspects of the JobInProgress code
   * because the fields may not be initialized correctly.
   */
  static class FakeJobInProgress extends JobInProgress {
    public FakeJobInProgress(JobID jId, JobConf jobConf,
                JobTracker jt) {
      super(jId, jobConf, jt);
    }
  }
  
  static short sendHeartBeat(JobTracker jt, TaskTrackerStatus status, 
      boolean initialContact, boolean acceptNewTasks, 
      String tracker, short responseId) 
      throws IOException {
    if (status == null) {
      status = new TaskTrackerStatus(tracker, 
      JobInProgress.convertTrackerNameToHostName(tracker));
    }
    jt.heartbeat(status, false, initialContact, acceptNewTasks, responseId);
    return ++responseId ;
  }

  static void establishFirstContact(JobTracker jt, String tracker) 
      throws IOException {
    sendHeartBeat(jt, null, true, false, tracker, (short) 0);
  }

  private TaskTrackerStatus getTTStatus(String trackerName,
      List<TaskStatus> taskStatuses) {
    return new TaskTrackerStatus(trackerName, 
      JobInProgress.convertTrackerNameToHostName(trackerName), 0,
      taskStatuses, 0, 0, mapSlotsPerTracker, reduceSlotsPerTracker);
  }
  
  public void testClusterMetrics() throws IOException, InterruptedException {
    assertEquals("tasktracker count doesn't match", trackers.length,
      client.getClusterStatus().getTaskTrackers());
    
    List<TaskStatus> list = new ArrayList<TaskStatus>();

    // create a map task status, which uses 2 slots. 
    int mapSlotsPerTask = 2;
    addMapTaskAttemptToList(list, mapSlotsPerTask, TaskStatus.State.RUNNING);
    
    // create a reduce task status, which uses 1 slot.
    int reduceSlotsPerTask = 1;
    addReduceTaskAttemptToList(list, 
        reduceSlotsPerTask, TaskStatus.State.RUNNING);
    
    // create TaskTrackerStatus and send heartbeats
    sendHeartbeats(list);

    // assert ClusterMetrics
    ClusterMetrics metrics = jobTracker.getClusterMetrics();
    assertEquals("occupied map slots do not match", mapSlotsPerTask,
      metrics.getOccupiedMapSlots());
    assertEquals("occupied reduce slots do not match", reduceSlotsPerTask,
      metrics.getOccupiedReduceSlots());
    assertEquals("map slot capacities do not match",
      mapSlotsPerTracker * trackers.length,
      metrics.getMapSlotCapacity());
    assertEquals("reduce slot capacities do not match",
      reduceSlotsPerTracker * trackers.length,
      metrics.getReduceSlotCapacity());
    assertEquals("running map tasks do not match", 1,
      metrics.getRunningMaps());
    assertEquals("running reduce tasks do not match", 1,
      metrics.getRunningReduces());
    
    // assert the values in ClusterStatus also
    ClusterStatus stat = client.getClusterStatus();
    assertEquals("running map tasks do not match", 1,
      stat.getMapTasks());
    assertEquals("running reduce tasks do not match", 1,
      stat.getReduceTasks());
    assertEquals("map slot capacities do not match",
      mapSlotsPerTracker * trackers.length,
      stat.getMaxMapTasks());
    assertEquals("reduce slot capacities do not match",
      reduceSlotsPerTracker * trackers.length,
      stat.getMaxReduceTasks());
    
    // send a heartbeat finishing only a map and check
    // counts are updated.
    list.clear();
    addMapTaskAttemptToList(list, mapSlotsPerTask, TaskStatus.State.SUCCEEDED);
    addReduceTaskAttemptToList(list, 
        reduceSlotsPerTask, TaskStatus.State.RUNNING);
    sendHeartbeats(list);
    metrics = jobTracker.getClusterMetrics();
    assertEquals(0, metrics.getOccupiedMapSlots());
    assertEquals(reduceSlotsPerTask, metrics.getOccupiedReduceSlots());
    
    // send a heartbeat finishing the reduce task also.
    list.clear();
    addReduceTaskAttemptToList(list, 
        reduceSlotsPerTask, TaskStatus.State.SUCCEEDED);
    sendHeartbeats(list);
    metrics = jobTracker.getClusterMetrics();
    assertEquals(0, metrics.getOccupiedReduceSlots());
  }
  
  private void sendHeartbeats(List<TaskStatus> list) throws IOException {
    TaskTrackerStatus[] status = new TaskTrackerStatus[trackers.length];
    status[0] = getTTStatus(trackers[0], list);
    status[1] = getTTStatus(trackers[1], new ArrayList<TaskStatus>());
    status[2] = getTTStatus(trackers[2], new ArrayList<TaskStatus>());
    for (int i = 0; i< trackers.length; i++) {
      sendHeartBeat(jobTracker, status[i], false, false, 
          trackers[i], responseId);
    }
    responseId++;
  }

  private void addReduceTaskAttemptToList(List<TaskStatus> list, 
      int reduceSlotsPerTask, TaskStatus.State state) {
    TaskStatus ts = TaskStatus.createTaskStatus(false, 
      new TaskAttemptID("jt", 1, false, 0, 0), 0.0f,
      reduceSlotsPerTask,
      state, "", "", trackers[0], 
      TaskStatus.Phase.REDUCE, null);
    list.add(ts);
  }

  private void addMapTaskAttemptToList(List<TaskStatus> list, 
      int mapSlotsPerTask, TaskStatus.State state) {
    TaskStatus ts = TaskStatus.createTaskStatus(true, 
      new TaskAttemptID("jt", 1, true, 0, 0), 0.0f, mapSlotsPerTask,
      state, "", "", trackers[0], 
      TaskStatus.Phase.MAP, null);
    list.add(ts);
  }

  public void testReservedSlots() throws IOException {
    JobConf conf = mr.createJobConf();

    conf.setNumReduceTasks(1);
    conf.setSpeculativeExecution(false);
    
    //Set task tracker objects for reservation.
    TaskTracker tt1 = jobTracker.getTaskTracker(trackers[0]);
    TaskTracker tt2 = jobTracker.getTaskTracker(trackers[1]);
    TaskTrackerStatus status1 = new TaskTrackerStatus(
        trackers[0],JobInProgress.convertTrackerNameToHostName(
            trackers[0]),0,new ArrayList<TaskStatus>(), 0, 0, 2, 2);
    TaskTrackerStatus status2 = new TaskTrackerStatus(
        trackers[1],JobInProgress.convertTrackerNameToHostName(
            trackers[1]),0,new ArrayList<TaskStatus>(), 0, 0, 2, 2);
    tt1.setStatus(status1);
    tt2.setStatus(status2);
    
    fakeJob = new FakeJobInProgress(new JobID("jt", 1), new JobConf(conf),
                    jobTracker);
    
    sendHeartBeat(jobTracker, status1, false, true, trackers[0], responseId);
    sendHeartBeat(jobTracker, status2, false, true, trackers[1], responseId);
    responseId++; 
    ClusterMetrics metrics = jobTracker.getClusterMetrics();
    assertEquals("reserved map slots do not match", 
      2, metrics.getReservedMapSlots());
    assertEquals("reserved reduce slots do not match", 
      2, metrics.getReservedReduceSlots());

    // redo to test re-reservations.
    sendHeartBeat(jobTracker, status1, false, true, trackers[0], responseId);
    sendHeartBeat(jobTracker, status2, false, true, trackers[1], responseId);
    responseId++; 
    metrics = jobTracker.getClusterMetrics();
    assertEquals("reserved map slots do not match", 
        4, metrics.getReservedMapSlots());
    assertEquals("reserved reduce slots do not match", 
        4, metrics.getReservedReduceSlots());

    // undo reservations now.
    scheduler.setUnreserveSlots(true);
    sendHeartBeat(jobTracker, status1, false, true, trackers[0], responseId);
    sendHeartBeat(jobTracker, status2, false, true, trackers[1], responseId);
    responseId++;
    metrics = jobTracker.getClusterMetrics();
    assertEquals("map slots should have been unreserved",
        0, metrics.getReservedMapSlots());
    assertEquals("reduce slots should have been unreserved",
        0, metrics.getReservedReduceSlots());
  }
  
  public void testClusterStatus() throws Exception {
    ClusterStatus clusterStatus = client.getClusterStatus();
    assertEquals("JobTracker used-memory is " + clusterStatus.getUsedMemory() + 
                 ", expected " + ClusterStatus.UNINITIALIZED_MEMORY_VALUE, 
                 ClusterStatus.UNINITIALIZED_MEMORY_VALUE, clusterStatus.getUsedMemory());
    assertEquals("JobTracker max-memory is " + clusterStatus.getMaxMemory() + 
        ", expected " + ClusterStatus.UNINITIALIZED_MEMORY_VALUE, 
        ClusterStatus.UNINITIALIZED_MEMORY_VALUE, clusterStatus.getMaxMemory());
    
    clusterStatus = client.getClusterStatus(false);
    assertEquals("JobTracker used-memory is " + clusterStatus.getUsedMemory() + 
                 ", expected " + ClusterStatus.UNINITIALIZED_MEMORY_VALUE, 
                 ClusterStatus.UNINITIALIZED_MEMORY_VALUE, clusterStatus.getUsedMemory());
    assertEquals("JobTracker max-memory is " + clusterStatus.getMaxMemory() + 
                 ", expected " + ClusterStatus.UNINITIALIZED_MEMORY_VALUE, 
                 ClusterStatus.UNINITIALIZED_MEMORY_VALUE, clusterStatus.getMaxMemory());
    
    clusterStatus = client.getClusterStatus(true);
    if (ClusterStatus.UNINITIALIZED_MEMORY_VALUE == clusterStatus.getUsedMemory()) {
      assertEquals("JobTracker used-memory is " + clusterStatus.getUsedMemory(), 
                   true, false);
    }
    if (ClusterStatus.UNINITIALIZED_MEMORY_VALUE == clusterStatus.getMaxMemory()) {
      assertEquals("JobTracker max-memory is " + clusterStatus.getMaxMemory(),
                    true, false);
    }
  }
}
