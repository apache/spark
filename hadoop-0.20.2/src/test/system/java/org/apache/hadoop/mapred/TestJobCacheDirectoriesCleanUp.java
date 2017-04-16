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
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.mapreduce.test.system.FinishTaskControlAction;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.mapreduce.test.system.TTClient;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapred.JobClient.NetworkedJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

import java.io.IOException;
import java.util.Random;
import java.util.HashMap;
import java.util.ArrayList;

/**
 * Verifying the non-writable cache folders and files for various jobs. 
 */
public class TestJobCacheDirectoriesCleanUp {
  private static final Log LOG = LogFactory
      .getLog(TestJobCacheDirectoriesCleanUp.class);
  private static Configuration conf = new Configuration();
  private static MRCluster cluster;
  private static JTClient jtClient;
  private static TTClient ttClient;
  private static JTProtocol rtClient;
  private static String CUSTOM_CREATION_FILE = "_custom_file_";
  private static String CUSTOM_CREATION_FOLDER = "_custom_folder_";
  private static FsPermission permission = new FsPermission(FsAction.READ, 
          FsAction.READ, FsAction.READ);

  @BeforeClass
  public static void before() throws Exception {
    conf = new Configuration();
    cluster = MRCluster.createCluster(conf);
    cluster.setUp();
    jtClient = cluster.getJTClient();
    rtClient = jtClient.getProxy();
    
  }

  @AfterClass
  public static void after() throws Exception {
    cluster.tearDown();
  }

  /**
   * Submit a job and create folders and files in work folder with 
   * non-writable permissions under task attempt id folder.
   * Wait till the job completes and verify whether the files 
   * and folders are cleaned up or not.
   * @throws IOException
   */
  @Test
  public void testJobCleanupAfterJobCompletes() throws IOException {
    HashMap<TTClient,ArrayList<String>> map = 
        new HashMap<TTClient,ArrayList<String>>();
    JobID jobId = createJobAndSubmit().getID();
    Assert.assertTrue("Job has not been started for 1 min", 
        jtClient.isJobStarted(jobId));
    TaskInfo [] taskInfos = rtClient.getTaskInfo(jobId);
    for (TaskInfo taskinfo : taskInfos) {
      if (!taskinfo.isSetupOrCleanup()) {
        Assert.assertTrue("Task has not been started for 1 min ",
            jtClient.isTaskStarted(taskinfo));
        String tasktracker = getTaskTracker(taskinfo);
        Assert.assertNotNull("TaskTracker has not been found", tasktracker);
        TTClient ttclient = getTTClient(tasktracker);
        UtilsForTests.waitFor(100);
        map.put(ttClient, getTTClientMapRedLocalDirs(ttClient, 
            taskinfo, jobId));
      }
    }

    LOG.info("Waiting till the job is completed...");
    Assert.assertTrue("Job has not been completed for 1 min",
        jtClient.isJobStopped(jobId));
    UtilsForTests.waitFor(3000);
    Assert.assertTrue("Job directories have not been cleaned up properly " + 
        "after completion of job", verifyJobDirectoryCleanup(map));
  }

  /**
   * Submit a job and create folders and files in work folder with 
   * non-writable permissions under task attempt id folder.
   * Kill the job and verify whether the files and folders
   * are cleaned up or not.
   * @throws IOException
   */
  @Test
  public void testJobCleanupAfterJobKill() throws IOException {
    HashMap<TTClient,ArrayList<String>> map = 
        new HashMap<TTClient,ArrayList<String>>();
    JobID jobId = createJobAndSubmit().getID();
    Assert.assertTrue("Job has not been started for 1 min", 
        jtClient.isJobStarted(jobId));
    TaskInfo [] taskInfos = rtClient.getTaskInfo(jobId);
    for (TaskInfo taskinfo : taskInfos) {
      if (!taskinfo.isSetupOrCleanup()) {
        Assert.assertTrue("Task has not been started for 1 min ",
            jtClient.isTaskStarted(taskinfo));
        String tasktracker = getTaskTracker(taskinfo);
        Assert.assertNotNull("TaskTracker has not been found", tasktracker);
        TTClient ttclient = getTTClient(tasktracker);
        map.put(ttClient, getTTClientMapRedLocalDirs(ttClient, 
            taskinfo, jobId));
      }
    }
    jtClient.getClient().killJob(jobId);
    LOG.info("Waiting till the job is completed...");
    Assert.assertTrue("Job has not been completed for 1 min",
        jtClient.isJobStopped(jobId));
    JobInfo jobInfo = rtClient.getJobInfo(jobId);
    Assert.assertEquals("Job has not been killed", 
            jobInfo.getStatus().getRunState(), JobStatus.KILLED);
    UtilsForTests.waitFor(3000);
    Assert.assertTrue("Job directories have not been cleaned up properly " + 
        "after completion of job", verifyJobDirectoryCleanup(map));
  }
  
  /**
   * Submit a job and create folders and files in work folder with 
   * non-writable permissions under task attempt id folder.
   * Fail the job and verify whether the files and folders
   * are cleaned up or not.
   * @throws IOException
   */
  @Test
  public void testJobCleanupAfterJobFail() throws IOException {
    HashMap<TTClient,ArrayList<String>> map = 
        new HashMap<TTClient,ArrayList<String>>();
    conf = rtClient.getDaemonConf();
    SleepJob job = new SleepJob();
    job.setConf(conf);
    JobConf jobConf = job.setupJobConf(1, 0, 10000,0, 10, 10);
    JobClient client = jtClient.getClient();
    RunningJob runJob = client.submitJob(jobConf);
    JobID jobId = runJob.getID();
    JobInfo jobInfo = rtClient.getJobInfo(jobId);
    Assert.assertTrue("Job has not been started for 1 min", 
        jtClient.isJobStarted(jobId));
    TaskInfo [] taskInfos = rtClient.getTaskInfo(jobId);
    boolean isFailTask = false;
    for (TaskInfo taskinfo : taskInfos) {
      if (!taskinfo.isSetupOrCleanup()) {        
        Assert.assertTrue("Task has not been started for 1 min ",
            jtClient.isTaskStarted(taskinfo));
        String tasktracker = getTaskTracker(taskinfo);
        Assert.assertNotNull("TaskTracker has not been found", tasktracker);
        TTClient ttclient = getTTClient(tasktracker);        
        map.put(ttClient, getTTClientMapRedLocalDirs(ttClient, 
            taskinfo, jobId));
        if (!isFailTask) {
          Assert.assertNotNull("TaskInfo is null.", taskinfo);
          TaskID taskId = TaskID.downgrade(taskinfo.getTaskID());
          TaskAttemptID taskAttID = new TaskAttemptID(taskId, 
              taskinfo.numFailedAttempts());
          int MAX_MAP_TASK_ATTEMPTS = Integer.
               parseInt(jobConf.get("mapred.map.max.attempts"));
          while(taskinfo.numFailedAttempts() < MAX_MAP_TASK_ATTEMPTS) {
            NetworkedJob networkJob = jtClient.getClient().
               new NetworkedJob(jobInfo.getStatus());
            networkJob.killTask(taskAttID, true);
            taskinfo = rtClient.getTaskInfo(taskinfo.getTaskID());
            taskAttID = new TaskAttemptID(taskId, taskinfo.numFailedAttempts());
            jobInfo = rtClient.getJobInfo(jobId);
          }
          isFailTask=true;
        }
      }
    }
    LOG.info("Waiting till the job is completed...");
    Assert.assertTrue("Job has not been completed for 1 min",
        jtClient.isJobStopped(jobId));
    jobInfo = rtClient.getJobInfo(jobId);
    Assert.assertEquals("Job has not been failed", 
            jobInfo.getStatus().getRunState(), JobStatus.FAILED);
    UtilsForTests.waitFor(3000); 
    Assert.assertTrue("Directories have not been cleaned up " + 
        "after completion of job", verifyJobDirectoryCleanup(map));
  }
  
  private static ArrayList <String> getTTClientMapRedLocalDirs(
      TTClient ttClient, TaskInfo taskinfo, JobID jobId) throws IOException {
    ArrayList <String> fileList = null;
    TaskID taskId = TaskID.downgrade(taskinfo.getTaskID());
    FinishTaskControlAction action = new FinishTaskControlAction(taskId);
    if (ttClient != null ) {
      String localDirs[] = ttClient.getMapredLocalDirs();
      TaskAttemptID taskAttID = new TaskAttemptID(taskId, 0);
      fileList = createFilesInTaskDir(localDirs, jobId, taskAttID, ttClient);
    }
    ttClient.getProxy().sendAction(action);
    return fileList;
  }
  
  private static boolean verifyJobDirectoryCleanup(HashMap<TTClient, 
    ArrayList<String>> map) throws IOException {
    boolean status = true;
    for (TTClient ttClient : map.keySet()) {
      if (map.get(ttClient) != null) {
      for(String path : map.get(ttClient)){
        FileStatus [] fs = ttClient.listStatus(path, true);
        if (fs.length > 0) {
          status = false;
        }
      }
      }
    }
    return status;
  }
  
  private static ArrayList<String> createFilesInTaskDir(String [] localDirs, 
      JobID jobId, TaskAttemptID taskAttID, TTClient ttClient) throws IOException {
    Random random = new Random(100);
    ArrayList<String> list = new ArrayList<String>();
    String customFile = CUSTOM_CREATION_FILE + random.nextInt();
    String customFolder = CUSTOM_CREATION_FOLDER + random.nextInt();
    int index = 0;
    for (String localDir : localDirs) {
      String localTaskDir = localDir + "/" + 
          TaskTracker.getLocalTaskDir(getUser(), jobId.toString(), 
          taskAttID.toString() + "/work/");
      boolean fstatus = false;
      try {
        fstatus = ttClient.getFileStatus(localTaskDir,true).isDir(); 
      } catch(Exception exp) {
        fstatus = false;
      }
      if (fstatus) {
         ttClient.createFile(localTaskDir, customFile, permission, true);
         ttClient.createFolder(localTaskDir, customFolder, permission, true);
         list.add(localTaskDir + customFile);
         list.add(localTaskDir + customFolder);
      }
     }
     
    return list;
  }
  
  private static RunningJob createJobAndSubmit() throws IOException {
    conf = rtClient.getDaemonConf();
    SleepJob job = new SleepJob();
    job.setConf(conf);
    JobConf jobConf = job.setupJobConf(3, 1, 12000, 12000, 100, 100);
    JobClient client = jtClient.getClient();
    RunningJob runJob = client.submitJob(jobConf);
    return runJob;
  }
  
  private static String getUser() throws IOException {
    JobStatus[] jobStatus = jtClient.getClient().getAllJobs();
    String userName = jobStatus[0].getUsername();
    return userName;
  }
  
  private static String getTaskTracker(TaskInfo taskInfo) 
      throws IOException {
    String taskTracker = null;
    String taskTrackers [] = taskInfo.getTaskTrackers();
    int counter = 0;
    while (counter < 30) {
      if (taskTrackers.length != 0) {
        taskTracker = taskTrackers[0];
        break;
      }
      UtilsForTests.waitFor(1000);
      taskInfo = rtClient.getTaskInfo(taskInfo.getTaskID());
      taskTrackers = taskInfo.getTaskTrackers();
      counter ++;
    }
    return taskTracker;
  }

  private static TTClient getTTClient(String taskTracker) {
    String hostName = taskTracker.split("_")[1];
    hostName = hostName.split(":")[0];
    ttClient = cluster.getTTClient(hostName);
    return ttClient;
  }
}
