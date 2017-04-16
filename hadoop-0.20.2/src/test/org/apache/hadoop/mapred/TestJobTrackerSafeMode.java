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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import junit.framework.TestCase;
import java.io.*;
import java.util.HashSet;
import java.util.Set;
import org.junit.*;

/** 
 * This test checks jobtracker in safe mode. In safe mode the jobtracker upon 
 * restart doesnt schedule any new tasks and waits for the (old) trackers to 
 * join back.
 */
/**UNTIL MAPREDUCE-873 is backported, we will not run recovery manager tests
 */
@Ignore
public class TestJobTrackerSafeMode extends TestCase {
  final Path testDir = 
    new Path(System.getProperty("test.build.data", "/tmp"), "jt-safemode");
  final Path inDir = new Path(testDir, "input");
  final Path shareDir = new Path(testDir, "share");
  final Path outputDir = new Path(testDir, "output");
  final int numDir = 1;
  final int numTrackers = 2;
  
  private static final Log LOG = 
    LogFactory.getLog(TestJobTrackerSafeMode.class);
  
  private JobConf configureJob(JobConf conf, int maps, int reduces,
                               String mapSignal, String redSignal) 
  throws IOException {
    UtilsForTests.configureWaitingJobConf(conf, inDir, outputDir, 
        maps, reduces, "test-jobtracker-safemode", 
        mapSignal, redSignal);
    return conf;
  }
  
  /**
   * Tests the jobtracker's safemode. The test is as follows : 
   *   - starts a cluster with 2 trackers
   *   - submits a job with large (40) maps to make sure that all the trackers 
   *     are logged to the job history
   *   - wait for the job to be 50% done
   *   - stop the jobtracker
   *   - wait for the trackers to be done with all the tasks
   *   - kill a task tracker
   *   - start the jobtracker
   *   - start 2 more trackers
   *   - now check that while all the tracker are detected (or lost) the 
   *     scheduling window is closed
   *   - check that after all the trackers are recovered, scheduling is opened 
   */
  private void testSafeMode(MiniDFSCluster dfs, MiniMRCluster mr) 
  throws Exception {
    FileSystem fileSys = dfs.getFileSystem();
    JobConf jobConf = mr.createJobConf();
    String mapSignalFile = UtilsForTests.getMapSignalFile(shareDir);
    String redSignalFile = UtilsForTests.getReduceSignalFile(shareDir);
    JobTracker jobtracker = mr.getJobTrackerRunner().getJobTracker();
    int numTracker = jobtracker.getClusterStatus(false).getTaskTrackers();
    
    // Configure the jobs
    JobConf job = configureJob(jobConf, 40, 0, mapSignalFile, redSignalFile);
      
    fileSys.delete(shareDir, true);
    
    // Submit a master job   
    JobClient jobClient = new JobClient(job);
    RunningJob rJob = jobClient.submitJob(job);
    JobID id = rJob.getID();
    
    // wait for the job to be inited
    mr.initializeJob(id);
    
    // Make sure that the master job is 50% completed
    while (UtilsForTests.getJobStatus(jobClient, id).mapProgress() 
           < 0.5f) {
      LOG.info("Waiting for the job to be 50% done");
      UtilsForTests.waitFor(100);
    }

    // Kill the jobtracker
    mr.stopJobTracker();

    // Enable recovery on restart
    mr.getJobTrackerConf().setBoolean("mapred.jobtracker.restart.recover", 
                                      true);
    
    // Signal the maps to complete
    UtilsForTests.signalTasks(dfs, fileSys, true, mapSignalFile, redSignalFile);
    
    // Signal the reducers to complete
    UtilsForTests.signalTasks(dfs, fileSys, false, mapSignalFile, 
                              redSignalFile);
    
    // wait for the tasks to complete at the tracker
    Set<String> trackers = new HashSet<String>();
    for (int i = 0 ; i < numTracker; ++i) {
      TaskTracker t = mr.getTaskTrackerRunner(i).getTaskTracker();
      trackers.add(t.getName());
      int runningCount = t.getRunningTaskStatuses().size();
      while (runningCount != 0) {
        LOG.info("Waiting for tracker " + t.getName() + " to stabilize");
        UtilsForTests.waitFor(100);
        runningCount = 0;
        for (TaskStatus status : t.getRunningTaskStatuses()) {
          if (status.getIsMap() 
              && (status.getRunState() == TaskStatus.State.UNASSIGNED 
                  || status.getRunState() == TaskStatus.State.RUNNING)) {
            ++runningCount;
          }
        }
      }
    }

    LOG.info("Trackers have stabilized");
    
    // Kill a tasktracker
    int trackerToKill = --numTracker;
    TaskTracker t = mr.getTaskTrackerRunner(trackerToKill).getTaskTracker();
    
    trackers.remove(t.getName()); // remove this from the set to check
    
    Set<String> lostTrackers = new HashSet<String>();
    lostTrackers.add(t.getName());
    
    // get the attempt-id's to ignore
    // stop the tracker
    LOG.info("Stopping tracker : " + t.getName());
    mr.getTaskTrackerRunner(trackerToKill).getTaskTracker().shutdown();
    mr.stopTaskTracker(trackerToKill);

    // Restart the jobtracker
    mr.startJobTracker();

    // Wait for the JT to be ready
    UtilsForTests.waitForJobTracker(jobClient);

    jobtracker = mr.getJobTrackerRunner().getJobTracker();

    // Start a tracker
    LOG.info("Start a new tracker");
    mr.startTaskTracker(null, null, ++numTracker, numDir);
    
    // Start a tracker
    LOG.info("Start a new tracker");
    mr.startTaskTracker(null, null, ++numTracker, numDir);

    // Check if the jobs are still running
    
    // Wait for the tracker to be lost
    boolean shouldSchedule = jobtracker.recoveryManager.shouldSchedule();
    while (!checkTrackers(jobtracker, trackers, lostTrackers)) {
      assertFalse("JobTracker has opened up scheduling before all the" 
                  + " trackers were recovered", shouldSchedule);
      UtilsForTests.waitFor(100);
      
      // snapshot jobtracker's scheduling status
      shouldSchedule = jobtracker.recoveryManager.shouldSchedule();
    }

    assertTrue("JobTracker hasnt opened up scheduling even all the" 
               + " trackers were recovered", 
               jobtracker.recoveryManager.shouldSchedule());
    
    assertEquals("Recovery manager is in inconsistent state", 
                 0, jobtracker.recoveryManager.recoveredTrackers.size());
    
    // wait for the job to be complete
    UtilsForTests.waitTillDone(jobClient);
  }

  private boolean checkTrackers(JobTracker jobtracker, Set<String> present, 
                                Set<String> absent) {
    long jobtrackerRecoveryFinishTime = 
      jobtracker.getStartTime() + jobtracker.getRecoveryDuration();
    for (String trackerName : present) {
      TaskTrackerStatus status = jobtracker.getTaskTrackerStatus(trackerName);
      // check if the status is present and also the tracker has contacted back
      // after restart
      if (status == null 
          || status.getLastSeen() < jobtrackerRecoveryFinishTime) {
        return false;
      }
    }
    for (String trackerName : absent) {
      TaskTrackerStatus status = jobtracker.getTaskTrackerStatus(trackerName);
      // check if the status is still present
      if ( status != null) {
        return false;
      }
    }
    return true;
  }

  /**
   * Test {@link JobTracker}'s safe mode.
   */
  public void testJobTrackerSafeMode() throws Exception {
    String namenode = null;
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;

    try {
      Configuration conf = new Configuration();
      conf.setBoolean("dfs.replication.considerLoad", false);
      dfs = new MiniDFSCluster(conf, 1, true, null, null);
      dfs.waitActive();
      fileSys = dfs.getFileSystem();
      
      // clean up
      fileSys.delete(testDir, true);
      
      if (!fileSys.mkdirs(inDir)) {
        throw new IOException("Mkdirs failed to create " + inDir.toString());
      }

      // Write the input file
      UtilsForTests.writeFile(dfs.getNameNode(), conf, 
                              new Path(inDir + "/file"), (short)1);

      dfs.startDataNodes(conf, 1, true, null, null, null, null);
      dfs.waitActive();

      namenode = (dfs.getFileSystem()).getUri().getHost() + ":" 
                 + (dfs.getFileSystem()).getUri().getPort();

      // Make sure that jobhistory leads to a proper job restart
      // So keep the blocksize and the buffer size small
      JobConf jtConf = new JobConf();
      jtConf.set("mapred.jobtracker.job.history.block.size", "512");
      jtConf.set("mapred.jobtracker.job.history.buffer.size", "512");
      jtConf.setInt("mapred.tasktracker.map.tasks.maximum", 1);
      jtConf.setInt("mapred.tasktracker.reduce.tasks.maximum", 1);
      jtConf.setLong("mapred.tasktracker.expiry.interval", 5000);
      jtConf.setInt("mapred.reduce.copy.backoff", 4);
      jtConf.setLong("mapred.job.reuse.jvm.num.tasks", -1);
      
      mr = new MiniMRCluster(numTrackers, namenode, numDir, null, null, jtConf);
      
      // Test Lost tracker case
      testSafeMode(dfs, mr);
    } finally {
      if (mr != null) {
        try {
          mr.shutdown();
        } catch (Exception e) {}
      }
      if (dfs != null) {
        try {
          dfs.shutdown();
        } catch (Exception e) {}
      }
    }
  }

  public static void main(String[] args) throws Exception  {
    new TestJobTrackerSafeMode().testJobTrackerSafeMode();
  }
}
