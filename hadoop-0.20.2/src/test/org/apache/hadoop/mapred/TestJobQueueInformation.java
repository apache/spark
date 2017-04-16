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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

import junit.framework.TestCase;

public class TestJobQueueInformation extends TestCase {

  private MiniMRCluster mrCluster;
  private MiniDFSCluster dfsCluster;
  private JobConf jc;
  private static final String JOB_SCHEDULING_INFO = "TESTSCHEDULINGINFO";
  private static final Path TEST_DIR = 
    new Path(System.getProperty("test.build.data","/tmp"), 
             "job-queue-info-testing");
  private static final Path IN_DIR = new Path(TEST_DIR, "input");
  private static final Path SHARE_DIR = new Path(TEST_DIR, "share");
  private static final Path OUTPUT_DIR = new Path(TEST_DIR, "output");
  
  static String getSignalFile() {
    return (new Path(SHARE_DIR, "signal")).toString();
  }

  // configure a waiting job with 2 maps
  private JobConf configureWaitingJob(JobConf conf) throws IOException {
    
    UtilsForTests.configureWaitingJobConf(conf, IN_DIR, OUTPUT_DIR, 2, 0, 
        "test-job-queue-info", getSignalFile(), getSignalFile());
    return conf;
  }

  public static class TestTaskScheduler extends LimitTasksPerJobTaskScheduler {

    @Override
    public synchronized List<Task> assignTasks(TaskTracker taskTracker)
        throws IOException {
      Collection<JobInProgress> jips = jobQueueJobInProgressListener
          .getJobQueue();
      if (jips != null && !jips.isEmpty()) {
        for (JobInProgress jip : jips) {
          jip.setSchedulingInfo(JOB_SCHEDULING_INFO);
        }
      }
      return super.assignTasks(taskTracker);
    }
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    final int taskTrackers = 4;
    Configuration conf = new Configuration();
    dfsCluster = new MiniDFSCluster(conf, 4, true, null);

    jc = new JobConf();
    jc.setClass("mapred.jobtracker.taskScheduler", TestTaskScheduler.class,
        TaskScheduler.class);
    jc.setLong("mapred.jobtracker.taskScheduler.maxRunningTasksPerJob", 10L);
    mrCluster = new MiniMRCluster(0, 0, taskTrackers, dfsCluster
        .getFileSystem().getUri().toString(), 1, null, null, null, jc);
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    mrCluster.shutdown();
    dfsCluster.shutdown();
  }

  public void testJobQueues() throws IOException {
    JobClient jc = new JobClient(mrCluster.createJobConf());
    String expectedQueueInfo = "Maximum Tasks Per Job :: 10";
    JobQueueInfo[] queueInfos = jc.getQueues();
    assertNotNull(queueInfos);
    assertEquals(1, queueInfos.length);
    assertEquals("default", queueInfos[0].getQueueName());
    assertEquals(Queue.QueueState.RUNNING.getStateName(),
                  queueInfos[0].getQueueState());
    JobConf conf = mrCluster.createJobConf();
    FileSystem fileSys = dfsCluster.getFileSystem();
    
    // configure a waiting job
    conf = configureWaitingJob(conf);
    conf.setJobName("test-job-queue-info-test");
    
    // clear the signal file if any
    fileSys.delete(SHARE_DIR, true);
    
    RunningJob rJob = jc.submitJob(conf);
    
    while (rJob.getJobState() != JobStatus.RUNNING) {
      UtilsForTests.waitFor(10);
    }
    
    int numberOfJobs = 0;

    for (JobQueueInfo queueInfo : queueInfos) {
      JobStatus[] jobStatusList = jc.getJobsFromQueue(queueInfo
          .getQueueName());
      assertNotNull(queueInfo.getQueueName());
      assertNotNull(queueInfo.getSchedulingInfo());
      assertEquals(expectedQueueInfo, queueInfo.getSchedulingInfo());
      numberOfJobs += jobStatusList.length;
      for (JobStatus status : jobStatusList) {
        assertEquals(JOB_SCHEDULING_INFO, status.getSchedulingInfo());
      }
    }
    assertEquals(1, numberOfJobs);
    
    UtilsForTests.signalTasks(dfsCluster, fileSys, getSignalFile(), 
                              getSignalFile(), 4);
  }
}
