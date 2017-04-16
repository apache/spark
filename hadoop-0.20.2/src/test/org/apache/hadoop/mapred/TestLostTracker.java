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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.mapreduce.TaskType;

import junit.framework.TestCase;
import java.io.*;

public class TestLostTracker extends TestCase {
  final Path testDir = new Path("/jt-lost-tt");
  final Path inDir = new Path(testDir, "input");
  final Path shareDir = new Path(testDir, "share");
  final Path outputDir = new Path(testDir, "output");
  
  private JobConf configureJob(JobConf conf, int maps, int reduces,
                               String mapSignal, String redSignal) 
  throws IOException {
    UtilsForTests.configureWaitingJobConf(conf, inDir, outputDir, 
        maps, reduces, "test-lost-tt", 
        mapSignal, redSignal);
    return conf;
  }
  
  public void testLostTracker(MiniDFSCluster dfs,
                              MiniMRCluster mr) 
  throws IOException {
    FileSystem fileSys = dfs.getFileSystem();
    JobConf jobConf = mr.createJobConf();
    int numMaps = 10;
    int numReds = 1;
    String mapSignalFile = UtilsForTests.getMapSignalFile(shareDir);
    String redSignalFile = UtilsForTests.getReduceSignalFile(shareDir);
    jobConf.set("user.name", UserGroupInformation.getCurrentUser().getUserName());
    // Configure the job
    JobConf job = configureJob(jobConf, numMaps, numReds, 
                               mapSignalFile, redSignalFile);
      
    fileSys.delete(shareDir, true);
    
    // Submit the job   
    JobClient jobClient = new JobClient(job);
    RunningJob rJob = jobClient.submitJob(job);
    JobID id = rJob.getID();
    
    // wait for the job to be inited
    mr.initializeJob(id);
    
    // Make sure that the master job is 50% completed
    while (UtilsForTests.getJobStatus(jobClient, id).mapProgress() 
           < 0.5f) {
      UtilsForTests.waitFor(10);
    }

    // get a completed task on 1st tracker 
    TaskAttemptID taskid = mr.getTaskTrackerRunner(0).getTaskTracker().
                              getNonRunningTasks().get(0).getTaskID();

    // Kill the 1st tasktracker
    mr.stopTaskTracker(0);

    // Signal all the maps to complete
    UtilsForTests.signalTasks(dfs, fileSys, true, mapSignalFile, redSignalFile);
    
    // Signal the reducers to complete
    UtilsForTests.signalTasks(dfs, fileSys, false, mapSignalFile, 
                              redSignalFile);
    // wait till the job is done
    UtilsForTests.waitTillDone(jobClient);

    // Check if the tasks on the lost tracker got killed and re-executed
    assertEquals(jobClient.getClusterStatus().getTaskTrackers(), 1);
    assertEquals(JobStatus.SUCCEEDED, rJob.getJobState());
    TaskInProgress tip = mr.getJobTrackerRunner().getJobTracker().
                         getTip(taskid.getTaskID());
    assertTrue(tip.isComplete());
    assertEquals(tip.numKilledTasks(), 1);
    
    // check if the task statuses for the tasks are sane
    JobTracker jt = mr.getJobTrackerRunner().getJobTracker();
    for (TaskInProgress mtip : jt.getJob(id).getTasks(TaskType.MAP)) {
      testTaskStatuses(mtip.getTaskStatuses());
    }
    
    // validate the history file
    TestJobHistory.validateJobHistoryFileFormat(id, job, "SUCCESS", true);
    TestJobHistory.validateJobHistoryFileContent(mr, rJob, job);
  }
  
  private void testTaskStatuses(TaskStatus[] tasks) {
    for (TaskStatus status : tasks) {
      assertTrue("Invalid start time " + status.getStartTime(), 
                 status.getStartTime() > 0);
      assertTrue("Invalid finish time " + status.getFinishTime(), 
                 status.getFinishTime() > 0);
      assertTrue("Start time (" + status.getStartTime() + ") is greater than " 
                 + "the finish time (" + status.getFinishTime() + ")", 
                 status.getStartTime() <= status.getFinishTime());
      assertNotNull("Task phase information is null", status.getPhase());
      assertNotNull("Task run-state information is null", status.getRunState());
      assertNotNull("TaskTracker information is null", status.getTaskTracker());
    }
  }

  public void testLostTracker() throws IOException {
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

      JobConf jtConf = new JobConf();
      jtConf.setInt("mapred.tasktracker.map.tasks.maximum", 1);
      jtConf.setInt("mapred.tasktracker.reduce.tasks.maximum", 1);
      jtConf.setLong("mapred.tasktracker.expiry.interval", 10 * 1000);
      jtConf.setInt("mapred.reduce.copy.backoff", 4);
      
      mr = new MiniMRCluster(2, namenode, 1, null, null, jtConf);
      
      // Test Lost tracker case
      testLostTracker(dfs, mr);
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

  public static void main(String[] args) throws IOException {
    new TestLostTracker().testLostTracker();
  }
}
