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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.mapreduce.test.system.TTClient;
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.mapreduce.test.system.TTProtocol;
import org.apache.hadoop.mapreduce.test.system.TTTaskInfo;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;
import org.apache.hadoop.mapreduce.test.system.FinishTaskControlAction;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Assert;
import org.junit.Test;
import java.io.IOException;
import java.io.DataOutputStream;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import testjar.GenerateTaskChildProcess;
import java.util.Hashtable;

/**
 * Submit a job which would spawn child processes and 
 * verify whether the task child processes are cleaned up 
 * or not after either job killed or task killed or task failed.
 */
public class TestChildsKillingOfMemoryExceedsTask {
  private static final Log LOG = LogFactory
      .getLog(TestChildsKillingOfMemoryExceedsTask.class);
  private static MRCluster cluster;
  private static Path inputDir = new Path("input");
  private static Path outputDir = new Path("output");
  private static Configuration conf = new Configuration();
  private static String confFile = "mapred-site.xml";

  @BeforeClass
  public static void before() throws Exception {
    Hashtable<String,Object> prop = new Hashtable<String,Object>();
    prop.put("mapred.cluster.max.map.memory.mb", 2 * 1024L);
    prop.put("mapred.cluster.map.memory.mb", 1024L);
    prop.put("mapred.cluster.max.reduce.memory.mb", 2 * 1024L);
    prop.put("mapred.cluster.reduce.memory.mb", 1024L);
    prop.put("mapred.map.max.attempts", 1L);
    prop.put("mapreduce.job.complete.cancel.delegation.tokens", false);

    String [] expExcludeList = {"java.net.ConnectException",
    "java.io.IOException"};
    cluster = MRCluster.createCluster(conf);
    cluster.setExcludeExpList(expExcludeList);
    cluster.setUp();
    cluster.restartClusterWithNewConfig(prop, confFile);
    UtilsForTests.waitFor(1000);
    conf =  cluster.getJTClient().getProxy().getDaemonConf();
    createInput(inputDir, conf);
  }

  @AfterClass
  public static void after() throws Exception {
    cleanup(inputDir, conf);
    cleanup(outputDir, conf);
    cluster.tearDown();
    cluster.restart();
  }

  /**
   * Verifying the process tree clean up of a task after fails 
   * due to memory limit and also job is killed while in progress.
   */
  @Test
  public void testProcessTreeCleanupAfterJobKilled() throws IOException {
    TaskInfo taskInfo = null;
    long PER_TASK_LIMIT = 500L;
    Matcher mat = null;
    TTTaskInfo[] ttTaskinfo = null;
    String pid = null;
    TTClient ttClientIns = null; 
    TTProtocol ttIns = null;
    TaskID tID = null;
    int counter = 0;

    String taskOverLimitPatternString = 
        "TaskTree \\[pid=[0-9]*,tipID=.*\\] is "
        + "running beyond memory-limits. "
        + "Current usage : [0-9]*bytes. Limit : %sbytes. Killing task.";
    
    Pattern taskOverLimitPattern = Pattern.compile(String.format(
        taskOverLimitPatternString, 
            String.valueOf(PER_TASK_LIMIT * 1024 * 1024L)));

    JobConf jobConf = new JobConf(conf);
    jobConf.setJobName("String Appending");
    jobConf.setJarByClass(GenerateTaskChildProcess.class);
    jobConf.setMapperClass(GenerateTaskChildProcess.StrAppendMapper.class);
    jobConf.setNumMapTasks(1);
    jobConf.setNumReduceTasks(0);
    cleanup(outputDir, conf);
    FileInputFormat.setInputPaths(jobConf, inputDir);
    FileOutputFormat.setOutputPath(jobConf, outputDir);
    jobConf.setMemoryForMapTask(PER_TASK_LIMIT);
    jobConf.setMemoryForReduceTask(PER_TASK_LIMIT);
    
    JTClient jtClient = cluster.getJTClient(); 
    JobClient client = jtClient.getClient();
    JTProtocol wovenClient = cluster.getJTClient().getProxy();
    RunningJob runJob = client.submitJob(jobConf);
    JobID id = runJob.getID();
    JobInfo jInfo = wovenClient.getJobInfo(id);
    Assert.assertNotNull("Job information is null",jInfo);

    Assert.assertTrue("Job has not been started for 1 min.", 
        jtClient.isJobStarted(id));

    TaskInfo[] taskInfos = wovenClient.getTaskInfo(id);
    for (TaskInfo taskinfo : taskInfos) {
      if (!taskinfo.isSetupOrCleanup()) {
        taskInfo = taskinfo;
        break;
      }
    }

    Assert.assertTrue("Task has not been started for 1 min.",
        jtClient.isTaskStarted(taskInfo));

    tID = TaskID.downgrade(taskInfo.getTaskID());
    TaskAttemptID tAttID = new TaskAttemptID(tID,0);
    FinishTaskControlAction action = new FinishTaskControlAction(tID);
    Collection<TTClient> ttClients = cluster.getTTClients();
    for (TTClient ttClient : ttClients) {
      TTProtocol tt = ttClient.getProxy();
      tt.sendAction(action);
      ttTaskinfo = tt.getTasks();
      for (TTTaskInfo tttInfo : ttTaskinfo) {
        if (!tttInfo.isTaskCleanupTask()) {
          pid = tttInfo.getPid();
          ttClientIns = ttClient;
          ttIns = tt;
          break;
        }
      }
      if (ttClientIns != null) {
        break;
      }
    }
    Assert.assertTrue("Map process is not alive before task fails.", 
        ttIns.isProcessTreeAlive(pid));
    
    while (ttIns.getTask(tID).getTaskStatus().getRunState() 
        == TaskStatus.State.RUNNING) {
      UtilsForTests.waitFor(1000);
      ttIns = ttClientIns.getProxy();
    }

    String[] taskDiagnostics = runJob.getTaskDiagnostics(tAttID);
    Assert.assertNotNull("Task diagnostics is null", taskDiagnostics);

    for (String strVal : taskDiagnostics) {
      mat = taskOverLimitPattern.matcher(strVal);
      Assert.assertTrue("Taskover limit error message is not matched.", 
          mat.find());
    }

    runJob.killJob();

    LOG.info("Waiting till the job is completed...");
    counter = 0;
    while (counter < 60) {
      if (jInfo.getStatus().isJobComplete()) {
        break;
      }
      UtilsForTests.waitFor(1000);
      jInfo = wovenClient.getJobInfo(id);
      counter ++;
    }
    Assert.assertTrue("Job has not been completed...", counter != 60);
    UtilsForTests.waitFor(1000);
    ttIns = ttClientIns.getProxy();
    ttIns.sendAction(action);
    UtilsForTests.waitFor(1000);
    Assert.assertTrue("Map process is still alive after task has been failed.", 
        !ttIns.isProcessTreeAlive(pid));
  }

  /**
   * Verifying the process tree clean up of a task after it fails
   * due to exceeding memory limit of mapper.
   */
  @Test
  public void testProcessTreeCleanupOfFailedTask() throws IOException {
    TaskInfo taskInfo = null;
    long PER_TASK_LIMIT = 500L;
    Matcher mat = null;
    TTTaskInfo[] ttTaskinfo = null;
    String pid = null;
    TTClient ttClientIns = null; 
    TTProtocol ttIns = null;
    TaskID tID = null;
    int counter = 0;
    
    String taskOverLimitPatternString = 
        "TaskTree \\[pid=[0-9]*,tipID=.*\\] is "
        + "running beyond memory-limits. "
        + "Current usage : [0-9]*bytes. Limit : %sbytes. Killing task.";

    Pattern taskOverLimitPattern = Pattern.compile(String.format(
        taskOverLimitPatternString, 
            String.valueOf(PER_TASK_LIMIT * 1024 * 1024L)));

    JobConf jobConf = new JobConf(conf);
    jobConf.setJobName("String Appending");
    jobConf.setJarByClass(GenerateTaskChildProcess.class);
    jobConf.setMapperClass(GenerateTaskChildProcess.StrAppendMapper.class);
    jobConf.setNumMapTasks(1);
    jobConf.setNumReduceTasks(0);
    cleanup(outputDir, conf);
    FileInputFormat.setInputPaths(jobConf, inputDir);
    FileOutputFormat.setOutputPath(jobConf, outputDir);
    jobConf.setMemoryForMapTask(PER_TASK_LIMIT);
    jobConf.setMemoryForReduceTask(PER_TASK_LIMIT);

    JTClient jtClient = cluster.getJTClient();
    JobClient client = jtClient.getClient();
    JTProtocol wovenClient = cluster.getJTClient().getProxy();
    RunningJob runJob = client.submitJob(jobConf);
    JobID id = runJob.getID();
    JobInfo jInfo = wovenClient.getJobInfo(id);
    Assert.assertNotNull("Job information is null", jInfo);

    Assert.assertTrue("Job has not been started for 1 min.", 
        jtClient.isJobStarted(id));

    TaskInfo[] taskInfos = wovenClient.getTaskInfo(id);
    for (TaskInfo taskinfo : taskInfos) {
      if (!taskinfo.isSetupOrCleanup()) {
        taskInfo = taskinfo;
        break;
      }
    }
    Assert.assertNotNull("Task information is null.", taskInfo);

    Assert.assertTrue("Task has not been started for 1 min.",
        jtClient.isTaskStarted(taskInfo));

    tID = TaskID.downgrade(taskInfo.getTaskID());
    TaskAttemptID tAttID = new TaskAttemptID(tID,0);
    FinishTaskControlAction action = new FinishTaskControlAction(tID);

    Collection<TTClient> ttClients = cluster.getTTClients();
    for (TTClient ttClient : ttClients) {
      TTProtocol tt = ttClient.getProxy();
      tt.sendAction(action);
      ttTaskinfo = tt.getTasks();
      for (TTTaskInfo tttInfo : ttTaskinfo) {
        if (!tttInfo.isTaskCleanupTask()) {
          pid = tttInfo.getPid();
          ttClientIns = ttClient;
          ttIns = tt;
          break;
        }
      }
      if (ttClientIns != null) {
        break;
      }
    }
    Assert.assertTrue("Map process is not alive before task fails.", 
        ttIns.isProcessTreeAlive(pid));

    Assert.assertTrue("Task did not stop " + tID, 
        ttClientIns.isTaskStopped(tID));

    String[] taskDiagnostics = runJob.getTaskDiagnostics(tAttID);
    Assert.assertNotNull("Task diagnostics is null.", taskDiagnostics);

    for (String strVal : taskDiagnostics) {
      mat = taskOverLimitPattern.matcher(strVal);
      Assert.assertTrue("Taskover limit error message is not matched.", 
          mat.find());
    }

    LOG.info("Waiting till the job is completed...");
    counter = 0;
    Assert.assertTrue("Job has not been completed...", 
        cluster.getJTClient().isJobStopped(id));
    ttIns = ttClientIns.getProxy();
    ttIns.sendAction(action);
    UtilsForTests.waitFor(1000);
    Assert.assertTrue("Map process is still alive after task has been failed.", 
        !ttIns.isProcessTreeAlive(pid));
  }

  private static void cleanup(Path dir, Configuration conf) throws 
      IOException {
    FileSystem fs = dir.getFileSystem(conf);
    fs.delete(dir, true);
  }

  private static void createInput(Path inDir, Configuration conf) throws 
      IOException {
    String input = "Hadoop is framework for data intensive distributed " 
        + "applications.\nHadoop enables applications to" 
        + " work with thousands of nodes.";
    FileSystem fs = inDir.getFileSystem(conf);
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Failed to create the input directory:" 
            + inDir.toString());
    }
    fs.setPermission(inDir, new FsPermission(FsAction.ALL, 
        FsAction.ALL, FsAction.ALL));
    DataOutputStream file = fs.create(new Path(inDir, "data.txt"));
    file.writeBytes(input);
    file.close();
  }
}
