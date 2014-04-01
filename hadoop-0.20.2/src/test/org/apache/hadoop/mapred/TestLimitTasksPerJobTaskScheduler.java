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

import junit.framework.TestCase;

import org.apache.hadoop.mapred.TestJobQueueTaskScheduler.FakeTaskTrackerManager;

public class TestLimitTasksPerJobTaskScheduler extends TestCase {
  protected JobConf jobConf;
  protected TaskScheduler scheduler;
  private FakeTaskTrackerManager taskTrackerManager;

  @Override
  protected void setUp() throws Exception {
    TestJobQueueTaskScheduler.resetCounters();
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
    return new LimitTasksPerJobTaskScheduler();
  }

  public void testMaxRunningTasksPerJob() throws IOException {
    jobConf.setLong(LimitTasksPerJobTaskScheduler.MAX_TASKS_PER_JOB_PROPERTY,
        4L);
    scheduler.setConf(jobConf);
    TestJobQueueTaskScheduler.submitJobs(taskTrackerManager, jobConf, 
                                         2, JobStatus.RUNNING);
    
    // First 4 slots are filled with job 1, second 4 with job 2
    TestJobQueueTaskScheduler.checkAssignment(
        scheduler, TestJobQueueTaskScheduler.tracker(taskTrackerManager, "tt1"), 
        new String[] {"attempt_test_0001_m_000001_0 on tt1"});
    TestJobQueueTaskScheduler.checkAssignment(
        scheduler, TestJobQueueTaskScheduler.tracker(taskTrackerManager, "tt1"), 
        new String[] {"attempt_test_0001_m_000002_0 on tt1"});
    TestJobQueueTaskScheduler.checkAssignment(
        scheduler, TestJobQueueTaskScheduler.tracker(taskTrackerManager, "tt1"), 
        new String[] {"attempt_test_0001_r_000003_0 on tt1"});
    TestJobQueueTaskScheduler.checkAssignment(
        scheduler, TestJobQueueTaskScheduler.tracker(taskTrackerManager, "tt1"), 
        new String[] {"attempt_test_0001_r_000004_0 on tt1"});
    TestJobQueueTaskScheduler.checkAssignment(
        scheduler, TestJobQueueTaskScheduler.tracker(taskTrackerManager, "tt2"), 
        new String[] {"attempt_test_0002_m_000005_0 on tt2"});
    TestJobQueueTaskScheduler.checkAssignment(
        scheduler, TestJobQueueTaskScheduler.tracker(taskTrackerManager, "tt2"), 
        new String[] {"attempt_test_0002_m_000006_0 on tt2"});
    TestJobQueueTaskScheduler.checkAssignment(
        scheduler, TestJobQueueTaskScheduler.tracker(taskTrackerManager, "tt2"), 
        new String[] {"attempt_test_0002_r_000007_0 on tt2"});
    TestJobQueueTaskScheduler.checkAssignment(
        scheduler, TestJobQueueTaskScheduler.tracker(taskTrackerManager, "tt2"), 
        new String[] {"attempt_test_0002_r_000008_0 on tt2"});
  }
  
  public void testMaxRunningTasksPerJobWithInterleavedTrackers()
      throws IOException {
    jobConf.setLong(LimitTasksPerJobTaskScheduler.MAX_TASKS_PER_JOB_PROPERTY,
        4L);
    scheduler.setConf(jobConf);
    TestJobQueueTaskScheduler.submitJobs(taskTrackerManager, jobConf, 2, JobStatus.RUNNING);
    
    // First 4 slots are filled with job 1, second 4 with job 2
    // even when tracker requests are interleaved
    TestJobQueueTaskScheduler.checkAssignment(
        scheduler, TestJobQueueTaskScheduler.tracker(taskTrackerManager, "tt1"), 
        new String[] {"attempt_test_0001_m_000001_0 on tt1"});
    TestJobQueueTaskScheduler.checkAssignment(
        scheduler, TestJobQueueTaskScheduler.tracker(taskTrackerManager, "tt1"), 
        new String[] {"attempt_test_0001_m_000002_0 on tt1"});
    TestJobQueueTaskScheduler.checkAssignment(
        scheduler, TestJobQueueTaskScheduler.tracker(taskTrackerManager, "tt2"), 
        new String[] {"attempt_test_0001_m_000003_0 on tt2"});
    TestJobQueueTaskScheduler.checkAssignment(
        scheduler, TestJobQueueTaskScheduler.tracker(taskTrackerManager, "tt1"), 
        new String[] {"attempt_test_0001_r_000004_0 on tt1"});
    TestJobQueueTaskScheduler.checkAssignment(
        scheduler, TestJobQueueTaskScheduler.tracker(taskTrackerManager, "tt2"), 
        new String[] {"attempt_test_0002_m_000005_0 on tt2"});
    TestJobQueueTaskScheduler.checkAssignment(
        scheduler, TestJobQueueTaskScheduler.tracker(taskTrackerManager, "tt1"), 
        new String[] {"attempt_test_0002_r_000006_0 on tt1"});
    TestJobQueueTaskScheduler.checkAssignment(
        scheduler, TestJobQueueTaskScheduler.tracker(taskTrackerManager, "tt2"), 
        new String[] {"attempt_test_0002_r_000007_0 on tt2"});
    TestJobQueueTaskScheduler.checkAssignment(
        scheduler, TestJobQueueTaskScheduler.tracker(taskTrackerManager, "tt2"), 
        new String[] {"attempt_test_0002_r_000008_0 on tt2"});
  }
  
}
