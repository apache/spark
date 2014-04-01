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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;
import org.apache.hadoop.mapreduce.test.system.TTClient;
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.mapreduce.test.system.FinishTaskControlAction;
import org.apache.hadoop.mapred.JobClient.NetworkedJob;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.UtilsForTests;

/**
 * A System test for verifying the status after killing the 
 * tasks at different conditions.
 */
public class TestTaskKilling {
  private static final Log LOG = LogFactory.getLog(TestTaskKilling.class);
  private static MRCluster cluster;
  private static JobClient jobClient = null;
  private static JTClient jtClient = null;
  private static JTProtocol remoteJTClient = null;
  private static Configuration conf = new Configuration();

  @BeforeClass
  public static void before() throws Exception {    
    cluster = MRCluster.createCluster(conf);
    cluster.setUp();
    jtClient = cluster.getJTClient();
    jobClient = jtClient.getClient();
    remoteJTClient = jtClient.getProxy();
  }

  @AfterClass
  public static void after() throws Exception {
    cluster.tearDown();
  }

  /**
   * Verifying the running job status whether it succeeds or not
   * after failing some of its tasks.
   */
  @Test
  public void testFailedTaskJobStatus() throws IOException, 
          InterruptedException {
    conf = remoteJTClient.getDaemonConf();
    TaskInfo taskInfo = null;
    SleepJob job = new SleepJob();
    job.setConf(conf);
    JobConf jobConf = job.setupJobConf(1, 1, 10000, 4000, 100, 100);
    RunningJob runJob = jobClient.submitJob(jobConf);
    JobID jobId = runJob.getID();
    JobInfo jInfo = remoteJTClient.getJobInfo(jobId);
    Assert.assertTrue("Job has not been started for 1 min.", 
        jtClient.isJobStarted(jobId));
    TaskInfo[] taskInfos = remoteJTClient.getTaskInfo(jobId);
    for (TaskInfo taskinfo : taskInfos) {
      if (!taskinfo.isSetupOrCleanup() && taskinfo.getTaskID().isMap()) {
        taskInfo = taskinfo;
        break;
      }
    }
    Assert.assertTrue("Task has not been started for 1 min.", 
        jtClient.isTaskStarted(taskInfo));

    // Fail the running task.
    NetworkedJob networkJob = jobClient.new NetworkedJob(jInfo.getStatus());
    TaskID tID = TaskID.downgrade(taskInfo.getTaskID());
    TaskAttemptID taskAttID = new TaskAttemptID(tID , 0);
    networkJob.killTask(taskAttID, true);

    LOG.info("Waiting till the job is completed...");
    while (!jInfo.getStatus().isJobComplete()) {
      UtilsForTests.waitFor(100);
      jInfo = remoteJTClient.getJobInfo(jobId);
    }
    Assert.assertEquals("JobStatus", JobStatus.SUCCEEDED, 
       jInfo.getStatus().getRunState());
  }


  /**
   * Verifying whether task temporary output directory is cleaned up or not
   * after killing the task.
   */
  @Test
  public void testDirCleanupAfterTaskKilled() throws IOException, 
          InterruptedException {
    TaskInfo taskInfo = null;
    boolean isTempFolderExists = false;
    String localTaskDir = null;
    TTClient ttClient = null;
    FileStatus filesStatus [] = null;
    Path inputDir = new Path("input");
    Path outputDir = new Path("output");
    Configuration conf = new Configuration(cluster.getConf());
    JobConf jconf = new JobConf(conf);
    jconf.setJobName("Word Count");
    jconf.setJarByClass(WordCount.class);
    jconf.setMapperClass(WordCount.MapClass.class);
    jconf.setCombinerClass(WordCount.Reduce.class);
    jconf.setReducerClass(WordCount.Reduce.class);
    jconf.setNumMapTasks(1);
    jconf.setNumReduceTasks(1);
    jconf.setOutputKeyClass(Text.class);
    jconf.setOutputValueClass(IntWritable.class);

    cleanup(inputDir, conf);
    cleanup(outputDir, conf);
    createInput(inputDir, conf);
    FileInputFormat.setInputPaths(jconf, inputDir);
    FileOutputFormat.setOutputPath(jconf, outputDir);
    RunningJob runJob = jobClient.submitJob(jconf);
    JobID id = runJob.getID();
    JobInfo jInfo = remoteJTClient.getJobInfo(id);
    Assert.assertTrue("Job has not been started for 1 min.", 
       jtClient.isJobStarted(id));

    JobStatus[] jobStatus = jobClient.getAllJobs();
    String userName = jobStatus[0].getUsername();
    TaskInfo[] taskInfos = remoteJTClient.getTaskInfo(id);
    for (TaskInfo taskinfo : taskInfos) {
      if (!taskinfo.isSetupOrCleanup() && taskinfo.getTaskID().isMap()) {
        taskInfo = taskinfo;
        break;
      }
    }

    Assert.assertTrue("Task has not been started for 1 min.", 
       jtClient.isTaskStarted(taskInfo));

    TaskID tID = TaskID.downgrade(taskInfo.getTaskID());
    FinishTaskControlAction action = new FinishTaskControlAction(tID);

    String[] taskTrackers = taskInfo.getTaskTrackers();
    int counter = 0;
    TaskInfo prvTaskInfo = taskInfo;
    while (counter++ < 30) {
      if (taskTrackers.length > 0) {
        break;
      } else {
        UtilsForTests.waitFor(100);
        taskInfo = remoteJTClient.getTaskInfo(taskInfo.getTaskID());
        if (taskInfo == null) {
          taskInfo = prvTaskInfo;
        } else {
          prvTaskInfo = taskInfo;
        }
        taskTrackers = taskInfo.getTaskTrackers();
      }
    }
    Assert.assertTrue("TaskTracker is not found.", taskTrackers.length > 0);
    String hostName = taskTrackers[0].split("_")[1];
    hostName = hostName.split(":")[0];
    ttClient = cluster.getTTClient(hostName);    
    String localDirs[] = ttClient.getMapredLocalDirs();
    TaskAttemptID taskAttID = new TaskAttemptID(tID, 0);
    for (String localDir : localDirs) {
      localTaskDir = localDir + "/" 
              + TaskTracker.getLocalTaskDir(userName, 
                      id.toString(), taskAttID.toString());
      filesStatus = ttClient.listStatus(localTaskDir, true);
      if (filesStatus.length > 0) {
        isTempFolderExists = true;
        break;
      }
    }
    
    Assert.assertTrue("Task Attempt directory " + 
            taskAttID + " has not been found while task was running.", 
                    isTempFolderExists);
    
    NetworkedJob networkJob = jobClient.new NetworkedJob(jInfo.getStatus());
    networkJob.killTask(taskAttID, false);
    ttClient.getProxy().sendAction(action);
    taskInfo = remoteJTClient.getTaskInfo(tID);
    while(taskInfo.getTaskStatus()[0].getRunState() == 
       TaskStatus.State.RUNNING) {
    UtilsForTests.waitFor(1000);
    taskInfo = remoteJTClient.getTaskInfo(tID);
    }
    UtilsForTests.waitFor(1000);
    taskInfo = remoteJTClient.getTaskInfo(tID);
    Assert.assertTrue("Task status has not been changed to KILLED.", 
       (TaskStatus.State.KILLED == 
       taskInfo.getTaskStatus()[0].getRunState()
       || TaskStatus.State.KILLED_UNCLEAN == 
       taskInfo.getTaskStatus()[0].getRunState()));
    taskInfo = remoteJTClient.getTaskInfo(tID);
    counter = 0;
    while (counter++ < 60) {
      filesStatus = ttClient.listStatus(localTaskDir, true);
      if (filesStatus.length == 0) {
        break;
      } else {
        UtilsForTests.waitFor(100);
      }
    }
    Assert.assertTrue("Task attempt temporary folder has not been cleaned.", 
            isTempFolderExists && filesStatus.length == 0);
    UtilsForTests.waitFor(1000);
    jInfo = remoteJTClient.getJobInfo(id);
    LOG.info("Waiting till the job is completed...");
    while (!jInfo.getStatus().isJobComplete()) {
      UtilsForTests.waitFor(100);
      jInfo = remoteJTClient.getJobInfo(id);
    }
  }

  private void cleanup(Path dir, Configuration conf) throws 
          IOException {
    FileSystem fs = dir.getFileSystem(conf);
    fs.delete(dir, true);
  }

  private void createInput(Path inDir, Configuration conf) throws 
          IOException {
    String input = "Hadoop is framework for data intensive distributed " 
            + "applications.\n" 
            + "Hadoop enables applications to work with thousands of nodes.";
    FileSystem fs = inDir.getFileSystem(conf);
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Failed to create the input directory:" 
            + inDir.toString());
    }
    fs.setPermission(inDir, new FsPermission(FsAction.ALL, 
            FsAction.ALL, FsAction.ALL));
    DataOutputStream file = fs.create(new Path(inDir, "data.txt"));
    int i = 0;
    while(i < 1000 * 3000) {
      file.writeBytes(input);
      i++;
    }
    file.close();
  }

  /**
   * Verifying whether task temporary output directory is cleaned up or not
   * after failing the task.
   */
  @Test
  public void testDirCleanupAfterTaskFailed() throws IOException, 
          InterruptedException {
    TTClient ttClient = null;
    FileStatus filesStatus [] = null;
    String localTaskDir = null;
    TaskInfo taskInfo = null;
    TaskID tID = null;
    boolean isTempFolderExists = false;
    conf = remoteJTClient.getDaemonConf();
    SleepJob job = new SleepJob();
    job.setConf(conf);
    JobConf jobConf = job.setupJobConf(1, 0, 10000,100, 10, 10);
    RunningJob runJob = jobClient.submitJob(jobConf);
    JobID id = runJob.getID();
    JobInfo jInfo = remoteJTClient.getJobInfo(id);
    Assert.assertTrue("Job has not been started for 1 min.", 
       jtClient.isJobStarted(id));

    JobStatus[] jobStatus = jobClient.getAllJobs();
    String userName = jobStatus[0].getUsername();
    TaskInfo[] taskInfos = remoteJTClient.getTaskInfo(id);
    for (TaskInfo taskinfo : taskInfos) {
      if (!taskinfo.isSetupOrCleanup() && taskinfo.getTaskID().isMap()) {
        taskInfo = taskinfo;
        break;
      }
    }
    Assert.assertTrue("Task has not been started for 1 min.", 
       jtClient.isTaskStarted(taskInfo));
    
    tID = TaskID.downgrade(taskInfo.getTaskID());
    FinishTaskControlAction action = new FinishTaskControlAction(tID);
    String[] taskTrackers = taskInfo.getTaskTrackers();
    int counter = 0;
    TaskInfo prvTaskInfo = taskInfo;
    while (counter++ < 30) {
      if (taskTrackers.length > 0) {
        break;
      } else {
        UtilsForTests.waitFor(1000);
        taskInfo = remoteJTClient.getTaskInfo(taskInfo.getTaskID());
        if (taskInfo == null) {
          taskInfo = prvTaskInfo;
        } else {
          prvTaskInfo = taskInfo;
        }
        taskTrackers = taskInfo.getTaskTrackers();
      }
    }
    Assert.assertTrue("Task tracker not found.", taskTrackers.length > 0);
    String hostName = taskTrackers[0].split("_")[1];
    hostName = hostName.split(":")[0];
    ttClient = cluster.getTTClient(hostName);
    String localDirs[] = ttClient.getMapredLocalDirs();
    TaskAttemptID taskAttID = new TaskAttemptID(tID, 0);
    for (String localDir : localDirs) {
      localTaskDir = localDir + "/" 
              + TaskTracker.getLocalTaskDir(userName, 
                      id.toString(), taskAttID.toString());
      filesStatus = ttClient.listStatus(localTaskDir, true);
      if (filesStatus.length > 0) {
        isTempFolderExists = true;
        break;
      }
    }    
    
    Assert.assertTrue("Task Attempt directory " + 
            taskAttID + " has not been found while task was running.", 
                    isTempFolderExists);
    boolean isFailTask = false;
    JobInfo jobInfo = remoteJTClient.getJobInfo(id);
    int MAX_MAP_TASK_ATTEMPTS = Integer.parseInt(
       jobConf.get("mapred.map.max.attempts"));
    if (!isFailTask) {        
        TaskID taskId = TaskID.downgrade(taskInfo.getTaskID());
        TaskAttemptID tAttID = new TaskAttemptID(taskId, 
            taskInfo.numFailedAttempts());
        while(taskInfo.numFailedAttempts() < MAX_MAP_TASK_ATTEMPTS) {
          NetworkedJob networkJob = jtClient.getClient().
             new NetworkedJob(jobInfo.getStatus());
          networkJob.killTask(taskAttID, true);
          taskInfo = remoteJTClient.getTaskInfo(taskInfo.getTaskID());
          taskAttID = new TaskAttemptID(taskId, taskInfo.numFailedAttempts());
        }
        isFailTask=true;
      }
    
    ttClient.getProxy().sendAction(action);
    taskInfo = remoteJTClient.getTaskInfo(tID);
    Assert.assertTrue("Task status has not been changed to FAILED.", 
       TaskStatus.State.FAILED == 
       taskInfo.getTaskStatus()[0].getRunState() 
       || TaskStatus.State.FAILED_UNCLEAN ==
       taskInfo.getTaskStatus()[0].getRunState());
    UtilsForTests.waitFor(1000);
    filesStatus = ttClient.listStatus(localTaskDir, true);
    Assert.assertTrue("Temporary folder has not been cleanup.", 
            filesStatus.length == 0);
    UtilsForTests.waitFor(1000);
    jInfo = remoteJTClient.getJobInfo(id);
    LOG.info("Waiting till the job is completed...");
    while (!jInfo.getStatus().isJobComplete()) {
      UtilsForTests.waitFor(100);
      jInfo = remoteJTClient.getJobInfo(id);
    }
  }

  @Test
  /**
   * This tests verification of job killing by killing of all task 
   * attempts of a particular task
   * @param none
   * @return void
   */
  public void testAllTaskAttemptKill() throws Exception {
    Configuration conf = new Configuration(cluster.getConf());

    JobStatus[] jobStatus = null;

    SleepJob job = new SleepJob();
    job.setConf(conf);
    conf = job.setupJobConf(2, 1, 40000, 1000, 100, 100);
    JobConf jconf = new JobConf(conf);

    //Submitting the job
    RunningJob rJob = cluster.getJTClient().getClient().submitJob(jconf);

    int MAX_MAP_TASK_ATTEMPTS = Integer.
        parseInt(jconf.get("mapred.map.max.attempts"));

    LOG.info("MAX_MAP_TASK_ATTEMPTS is : " + MAX_MAP_TASK_ATTEMPTS);

    Assert.assertTrue(MAX_MAP_TASK_ATTEMPTS > 0);

    TTClient tClient = null;
    TTClient[] ttClients = null;

    JobInfo jInfo = remoteJTClient.getJobInfo(rJob.getID());

    //Assert if jobInfo is null
    Assert.assertNotNull(jInfo.getStatus().getRunState());

    //Wait for the job to start running.
    while (jInfo.getStatus().getRunState() != JobStatus.RUNNING) {
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {};
      jInfo = remoteJTClient.getJobInfo(rJob.getID());
    }

    //Temporarily store the jobid to use it later for comparision.
    JobID jobidStore = rJob.getID();
    jobidStore = JobID.downgrade(jobidStore);
    LOG.info("job id is :" + jobidStore.toString());

    TaskInfo[] taskInfos = null;

    //After making sure that the job is running,
    //the test execution has to make sure that
    //at least one task has started running before continuing.
    boolean runningCount = false;
    int count = 0;
    do {
      taskInfos = cluster.getJTClient().getProxy()
        .getTaskInfo(rJob.getID());
      runningCount = false;
      for (TaskInfo taskInfo : taskInfos) {
        TaskStatus[] taskStatuses = taskInfo.getTaskStatus();
        if (taskStatuses.length > 0){
          LOG.info("taskStatuses[0].getRunState() is :" +
            taskStatuses[0].getRunState());
          if (taskStatuses[0].getRunState() == TaskStatus.State.RUNNING){
            runningCount = true;
            break;
          } else {
            LOG.info("Sleeping 5 seconds");
            Thread.sleep(5000);
          }
        }
      }
      count++;
      //If the count goes beyond a point, then break; This is to avoid
      //infinite loop under unforeseen circumstances. Testcase will anyway
      //fail later.
      if (count > 10) {
        Assert.fail("Since the sleep count has reached beyond a point" +
          "failing at this point");
      } 
    } while (!runningCount);

    //This whole module is about getting the task Attempt id
    //of one task and killing it MAX_MAP_TASK_ATTEMPTS times,
    //whenever it re-attempts to run.
    String taskIdKilled = null;
    for (int i = 0 ; i<MAX_MAP_TASK_ATTEMPTS; i++) {
      taskInfos = cluster.getJTClient().getProxy()
          .getTaskInfo(rJob.getID());

      for (TaskInfo taskInfo : taskInfos) {
        TaskAttemptID taskAttemptID;
        if (!taskInfo.isSetupOrCleanup()) {
          //This is the task which is going to be killed continously in
          //all its task attempts.The first task is getting picked up.
          TaskID taskid = TaskID.downgrade(taskInfo.getTaskID());
          LOG.info("taskid is :" + taskid);
          if (i==0) {
            taskIdKilled = taskid.toString();
            taskAttemptID = new TaskAttemptID(taskid, i);
            LOG.info("taskAttemptid going to be killed is : " + taskAttemptID);
            (jobClient.new NetworkedJob(jInfo.getStatus())).
                killTask(taskAttemptID,true);
            checkTaskCompletionEvent(taskAttemptID, jInfo);
            break;
          } else {
            if (taskIdKilled.equals(taskid.toString())) {
              taskAttemptID = new TaskAttemptID(taskid, i);
              //Make sure that task is midway and then kill
              UtilsForTests.waitFor(20000);
              LOG.info("taskAttemptid going to be killed is : " +
                  taskAttemptID);
              (jobClient.new NetworkedJob(jInfo.getStatus())).
                  killTask(taskAttemptID,true);
              checkTaskCompletionEvent(taskAttemptID,jInfo);
              break;
            }
          }
        }
      }
    }
    //Making sure that the job is complete.
    while (jInfo != null && !jInfo.getStatus().isJobComplete()) {
      Thread.sleep(10000);
      jInfo = remoteJTClient.getJobInfo(rJob.getID());
    }

    //Making sure that the correct jobstatus is got from all the jobs
    jobStatus = jobClient.getAllJobs();
    JobStatus jobStatusFound = null;
    for (JobStatus jobStatusTmp : jobStatus) {
      if (JobID.downgrade(jobStatusTmp.getJobID()).equals(jobidStore)) {
        jobStatusFound = jobStatusTmp;
        LOG.info("jobStatus found is :" + jobStatusFound.getJobId().toString());
      }
    }

    //Making sure that the job has FAILED
    Assert.assertEquals("The job should have failed at this stage",
        JobStatus.FAILED,jobStatusFound.getRunState());
  }

  //This method checks if task Attemptid occurs in the list
  //of tasks that are completed (killed) for a job.This is
  //required because after issuing a kill comamnd, the task
  //has to be killed and appear in the taskCompletion event.
  //After this a new task attempt will start running in a
  //matter of few seconds.
  public void checkTaskCompletionEvent (TaskAttemptID taskAttemptID,
      JobInfo jInfo) throws Exception {
    boolean match = false;
    int count = 0;
    while (!match) {
      TaskCompletionEvent[] taskCompletionEvents =  jobClient.new
        NetworkedJob(jInfo.getStatus()).getTaskCompletionEvents(0);
      for (TaskCompletionEvent taskCompletionEvent : taskCompletionEvents) {
        LOG.info("taskCompletionEvent.getTaskAttemptId().toString() is : " + 
          taskCompletionEvent.getTaskAttemptId().toString());
        LOG.info("compared to taskAttemptID.toString() :" + 
          taskAttemptID.toString());
        if ((taskCompletionEvent.getTaskAttemptId().toString()).
            equals(taskAttemptID.toString())){
          match = true;
          //Sleeping for 10 seconds giving time for the next task
          //attempt to run
          Thread.sleep(10000);
          break;
        }
      }
      if (!match) {
        LOG.info("Thread is sleeping for 10 seconds");
        Thread.sleep(10000);
        count++;
      }
      //If the count goes beyond a point, then break; This is to avoid
      //infinite loop under unforeseen circumstances.Testcase will anyway
      //fail later.
      if (count > 10) {
        Assert.fail("Since the task attemptid is not appearing in the" +
            "TaskCompletionEvent, it seems this task attempt was not killed");
      }
    }
  }
}
