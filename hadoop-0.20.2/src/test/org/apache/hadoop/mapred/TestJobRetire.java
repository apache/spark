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
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobTracker.RetireJobInfo;
import org.apache.hadoop.mapred.MiniMRCluster.TaskTrackerRunner;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.split.JobSplit;

/**
 * Test if the job retire works fine.
 */
public class TestJobRetire extends TestCase {
  static final Log LOG = LogFactory.getLog(TestJobRetire.class);
  static final Path testDir =
    new Path(System.getProperty("test.build.data","/tmp"),
             "job-expiry-testing");

  private MiniMRCluster startCluster(JobConf conf, int numTrackers) 
  throws IOException {
    conf.setLong("mapred.job.tracker.retiredjobs.cache.size", 1);
    conf.setLong("mapred.jobtracker.retirejob.interval", 0);
    conf.setLong("mapred.jobtracker.retirejob.check", 0);
    conf.getLong("mapred.jobtracker.completeuserjobs.maximum", 0);
    
    return new MiniMRCluster(0, 0, numTrackers, "file:///", 1, null, null, null,
                             conf, 0);
  }
  
  public void testJobRetire() throws Exception {
    MiniMRCluster mr = null;
    try {
      JobConf conf = new JobConf();
      mr = startCluster(conf, 1);
      
      JobConf jobConf = mr.createJobConf();
      JobTracker jobtracker = mr.getJobTrackerRunner().getJobTracker();

      Path inDir = new Path(testDir, "input1");
      Path outDir = new Path(testDir, "output1");

      JobID id1 = validateJobRetire(jobConf, inDir, outDir, jobtracker);

      outDir = new Path(testDir, "output2");
      JobID id2 = validateJobRetire(jobConf, inDir, outDir, jobtracker);

      assertNull("Job not removed from cache", jobtracker.getJobStatus(id1));

      assertEquals("Total job in cache not correct",
          1, jobtracker.getAllJobs().length);
    } finally {
      if (mr != null) { mr.shutdown();}
      FileUtil.fullyDelete(new File(testDir.toString()));
    }
  }

  private JobID validateJobRetire(JobConf jobConf, Path inDir, Path outDir,
      JobTracker jobtracker) throws IOException {

    RunningJob rj = UtilsForTests.runJob(jobConf, inDir, outDir, 0, 0);
    rj.waitForCompletion();
    assertTrue(rj.isSuccessful());
    JobID id = rj.getID();

    //wait for job to get retired
    waitTillRetire(id, jobtracker);
    RetireJobInfo retired = jobtracker.retireJobs.get(id);
    assertTrue("History url not set", retired.getHistoryFile() != null &&
      retired.getHistoryFile().length() > 0);
    assertNotNull("Job is not in cache", jobtracker.getJobStatus(id));

    // get the job conf filename
    String name = jobtracker.getLocalJobFilePath(id);
    File file = new File(name);

    assertFalse("JobConf file not deleted", file.exists());
    //test redirection
    URL jobUrl = new URL(rj.getTrackingURL());
    HttpURLConnection conn = (HttpURLConnection) jobUrl.openConnection();
    conn.setInstanceFollowRedirects(false);
    conn.connect();
    assertEquals(HttpURLConnection.HTTP_MOVED_TEMP, conn.getResponseCode());
    conn.disconnect();

    URL redirectedUrl = new URL(conn.getHeaderField("Location"));
    conn = (HttpURLConnection) redirectedUrl.openConnection();
    conn.connect();
    assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    conn.disconnect();

    return id;
  }
  
  // wait till the job retires
  private void waitTillRetire(JobID id, JobTracker jobtracker) {
    //wait for job to get retired
    JobInProgress job = jobtracker.getJob(id);
    for (int i = 0; i < 10 && job != null; i++) {
      UtilsForTests.waitFor(1000);
      job = jobtracker.getJob(id);
    }
    assertNull("Job did not retire", job);
  }
  
  /**
   * Custom TaskTracker which waits forever after a successful contact to 
   * the JobTracker.
   */
  class WaitingTaskTracker extends TaskTracker {
    
    WaitingTaskTracker(JobConf conf) throws InterruptedException, IOException {
      super(conf);
    }
    
    @Override
    HeartbeatResponse transmitHeartBeat(long now) throws IOException {
      HeartbeatResponse response = super.transmitHeartBeat(now);
      LOG.info("WaitingTaskTracker waiting");
      // wait forever
      UtilsForTests.waitFor(Long.MAX_VALUE);
      throw new IOException ("WaitingTaskTracker interrupted. Bailing out");
    }
  }
  
  /**
   * Test job retire with tasks that report their *first* status only after the
   * job retires.
   * Steps :
   *  - Start a mini-mr cluster with 1 task-tracker having only map slots.
   *    Note that this task-tracker will take care of setup/cleanup and the map
   *    tasks.
   *  - Submit a job with 1 map task and 1 reduce task
   *  - Wait for the job to finish the map task
   *  - Start a 2nd tracker that waits for a long time after contacting the JT.
   *  - Wait for the 2nd tracker to get stuck
   *  - Kill the job
   *  - Wait for the job to retire
   *  - Check if the tip mappings are cleaned up. 
   */
  public void testJobRetireWithUnreportedTasks() throws Exception {
    MiniMRCluster mr = null;
    try {
      JobConf conf = new JobConf();
      // set the num-map-slots to 1 so that no reduce tasks but setup/cleanup 
      // can run on it
      conf.setInt("mapred.tasktracker.map.tasks.maximum", 1);
      conf.setInt("mapred.tasktracker.reduce.tasks.maximum", 0);
      
      mr = startCluster(conf, 1);
      JobTracker jobtracker = mr.getJobTrackerRunner().getJobTracker();
      
      RunningJob job = 
        UtilsForTests.runJob(mr.createJobConf(), new Path(testDir, "in-1"), 
                             new Path(testDir, "out-1"), 1, 1);
      JobID id = JobID.downgrade(job.getID());
      JobInProgress jip = jobtracker.getJob(id);
      
      // wait 100 secs for the job to complete its map task
      for (int i = 0; i < 1000 && jip.finishedMaps() < 1; i++) {
        UtilsForTests.waitFor(100);
      }
      assertEquals(jip.finishedMaps(), 1);
      
      // start a tracker that will wait
      LOG.info("Adding a waiting tracker");
      TaskTrackerRunner testTrackerRunner = 
        mr.new TaskTrackerRunner(1, 1, null, mr.createJobConf()) {
        @Override
        TaskTracker createTaskTracker(JobConf conf) throws InterruptedException, IOException {
          return new WaitingTaskTracker(conf);
        }
      };
      mr.addTaskTracker(testTrackerRunner);
      LOG.info("Waiting tracker added");
      
      WaitingTaskTracker testTT = 
        (WaitingTaskTracker)testTrackerRunner.getTaskTracker();
      
      // wait 100 secs for the newly started task-tracker to join
      for (int i = 0; i < 1000 && jobtracker.taskTrackers().size() < 2; i++) {
        UtilsForTests.waitFor(100);
      }
      assertEquals(jobtracker.taskTrackers().size(), 2);
      LOG.info("Cluster is now up with 2 trackers");
      // stop the test-tt as its no longer required
      mr.stopTaskTracker(mr.getTaskTrackerID(testTT.getName()));
      
      // 1 reduce task should be scheduled
      assertEquals("TestTT contacted but no reduce task scheduled on it", 
                   1, jip.runningReduces());
      
      // kill the job
      LOG.info("Killing job " + id);
      job.killJob();
      
      // check if the reduce task attempt status is missing
      TaskInProgress tip = jip.getTasks(TaskType.REDUCE)[0]; 
      assertNull(tip.getTaskStatus(tip.getAllTaskAttemptIDs()[0]));
      
      // wait for the job to retire
      waitTillRetire(id, jobtracker);
      
      // check the taskidToTIPMap
      for (TaskAttemptID tid : jobtracker.taskidToTIPMap.keySet()) {
        LOG.info("TaskidToTIP : " + tid);
      }
      assertEquals("'taskid' to TIP mapping still exists", 
                   0, jobtracker.taskidToTIPMap.size());
    } finally {
      if (mr != null) { mr.shutdown();}
      // cleanup
      FileUtil.fullyDelete(new File(testDir.toString()));
    }
  }

  /**
   * (Mock)Test JobTracker.removeJobTasks() which is called only when the job 
   * retires.
   */
  public void testJobRemoval() throws Exception {
    MiniMRCluster mr = null;
    try {
      JobConf conf = new JobConf();
      mr = startCluster(conf, 0);
      JobTracker jobtracker = mr.getJobTrackerRunner().getJobTracker();
      
      // test map task removal
      testRemoveJobTasks(jobtracker, conf, TaskType.MAP);
      // test reduce task removal
      testRemoveJobTasks(jobtracker, conf, TaskType.REDUCE);
      // test job setup removal
      testRemoveJobTasks(jobtracker, conf, TaskType.JOB_SETUP);
      // test job cleanup removal
      testRemoveJobTasks(jobtracker, conf, TaskType.JOB_CLEANUP);
    } finally {
      if (mr != null) { mr.shutdown();}
      // cleanup
      FileUtil.fullyDelete(new File(testDir.toString()));
    }
  }
 
  // create a new job and add it to the jobtracker
  private JobInProgress createAndAddJob(JobTracker jobtracker, JobConf conf) {
    // submit a job in a fake manner
    // get the new job-id
    JobID id = 
      new JobID(jobtracker.getTrackerIdentifier(), jobtracker.jobs.size() + 1);
    // create a JobInProgress for this fake job
    JobInProgress jip = new JobInProgress(id, conf, jobtracker);
    
    // insert this fake completed job in the jobtracker
    jobtracker.jobs.put(id, jip);
    
    return jip;
  }
  
  // create a new TaskInProgress and make it running by adding it to jobtracker
  private TaskInProgress createAndAddTIP(JobTracker jobtracker, 
                                         JobInProgress jip, TaskType type) {
    JobConf conf = jip.getJobConf();
    JobID id = jip.getJobID();
    // now create a fake tip for this fake job
    TaskInProgress tip = null;
    if (type == TaskType.MAP) {
      tip = new TaskInProgress(id, "dummy", JobSplit.EMPTY_TASK_SPLIT, 
                               jobtracker, conf, jip, 0, 1);
      jip.maps = new TaskInProgress[] {tip};
    } else if (type == TaskType.REDUCE) {
      tip = new TaskInProgress(id, "dummy", jip.desiredMaps(), 0, 
                               jobtracker, conf, jip, 1);
      jip.reduces = new TaskInProgress[] {tip};
    } else if (type == TaskType.JOB_SETUP) {
      tip = 
        new TaskInProgress(id, "dummy", JobSplit.EMPTY_TASK_SPLIT, 
                           jobtracker, conf, jip, 0, 1);
      jip.setup = new TaskInProgress[] {tip};
    } else if (type == TaskType.JOB_CLEANUP) {
      tip = 
        new TaskInProgress(id, "dummy", JobSplit.EMPTY_TASK_SPLIT, 
                           jobtracker, conf, jip, 0, 1);
      jip.cleanup = new TaskInProgress[] {tip};
    }
    return tip;
  }
  
  // create a new Task for the given tip and make it running
  private TaskAttemptID createAndAddAttempt(TaskInProgress tip, int attemptId) {
    // create a fake attempt for this fake task
    TaskAttemptID taskid = new TaskAttemptID(tip.getTIPId(), attemptId);
    
    // insert this fake task into the jobtracker by making it running
    tip.addRunningTask(taskid, "test-tt");
    
    return taskid;
  }
  
  // Mock a job run such that the jobtracker is in a state similar to that 
  // resulting from an actual job run.
  // Steps :
  //   - generate a new job-id
  //   - create and add a JobInProgress object using the fake job-id
  //   - create and add a fake tip of the passed type 't' under the fake job
  //     Note that t can be a MAP or a REDUCE or a JOB_SETUP or a JOB_CLEANUP.
  //   - create and add a fake attempt under the fake tip
  //   - remove the job from the jobtracker
  //   - check if the fake attempt is removed from the jobtracker
  private void testRemoveJobTasks(JobTracker jobtracker, JobConf conf, 
                                  TaskType type) {
    // create and submit a job
    JobInProgress jip = createAndAddJob(jobtracker, conf);
    // create and add a tip
    TaskInProgress tip = createAndAddTIP(jobtracker, jip, type);
    // create and add an attempt
    TaskAttemptID taskid = createAndAddAttempt(tip, 0);
    
    // this fake attempt should not have any status
    assertNull(tip.getTaskStatus(taskid));
    
    // remove the job tasks for this fake job from the jobtracker
    jobtracker.removeJobTasks(jip);
    
    // check the taskidToTIPMap
    for (TaskAttemptID tid : jobtracker.taskidToTIPMap.keySet()) {
      LOG.info("TaskidToTIP : " + tid);
    }
    
    // check if the fake attempt is removed from the jobtracker
    assertEquals("'taskid' to TIP mapping still exists", 
                 0, jobtracker.taskidToTIPMap.size());
  }
}
