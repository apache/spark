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
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;
import org.junit.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.common.RemoteExecution;

import java.util.Collection;
import java.util.Hashtable;

import org.apache.hadoop.mapreduce.test.system.FinishTaskControlAction;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.mapreduce.test.system.TTClient;
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.mapreduce.test.system.TTProtocol;
import org.apache.hadoop.mapreduce.test.system.TTTaskInfo;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;
import testjar.GenerateTaskChildProcess;

public class TestChildsKillingOfSuspendTask {
  private static final Log LOG = LogFactory
      .getLog(TestChildsKillingOfSuspendTask.class);
  private static Configuration conf = new Configuration();
  private static MRCluster cluster;
  private static Path inputDir = new Path("input");
  private static Path outputDir = new Path("output");
  private static String confFile = "mapred-site.xml"; 
  
  @BeforeClass
  public static void before() throws Exception {
    Hashtable<String,Object> prop = new Hashtable<String,Object>();
    prop.put("mapred.map.max.attempts",1L);
    prop.put("mapred.task.timeout",30000L);
    prop.put("mapreduce.job.complete.cancel.delegation.tokens", false);
    String [] expExcludeList = {"java.net.ConnectException",
    "java.io.IOException","org.apache.hadoop.metrics2.MetricsException"};
    cluster = MRCluster.createCluster(conf);
    cluster.setExcludeExpList(expExcludeList);
    cluster.setUp();
    cluster.restartClusterWithNewConfig(prop, confFile);
    UtilsForTests.waitFor(1000);
    conf = cluster.getJTClient().getProxy().getDaemonConf();
    createInput(inputDir, conf);
  }
  @AfterClass
  public static void after() throws Exception {
    cleanup(inputDir, conf);
    cleanup(outputDir, conf);
    cluster.tearDown();
   // cluster.restart();
  }
  
  /**
   * Verify the process tree clean up of a task after
   * task is suspended and wait till the task is 
   * terminated based on timeout. 
   */
  @Test
  public void testProcessTreeCleanupOfSuspendTask() throws 
      Exception {
    TaskInfo taskInfo = null;
    TaskID tID = null;
    TTTaskInfo [] ttTaskinfo = null;
    String pid = null;
    TTProtocol ttIns = null;
    TTClient ttClientIns = null;
    int counter = 0;

    JobConf jobConf = new JobConf(conf);
    jobConf.setJobName("Message Display");
    jobConf.setJarByClass(GenerateTaskChildProcess.class);
    jobConf.setMapperClass(GenerateTaskChildProcess.StrDisplayMapper.class);
    jobConf.setNumMapTasks(1);
    jobConf.setNumReduceTasks(0);
    jobConf.setMaxMapAttempts(1);
    cleanup(outputDir, conf);
    FileInputFormat.setInputPaths(jobConf, inputDir);
    FileOutputFormat.setOutputPath(jobConf, outputDir);

    JTClient jtClient = cluster.getJTClient();
    JobClient client = jtClient.getClient();
    JTProtocol wovenClient = cluster.getJTClient().getProxy();
    RunningJob runJob = client.submitJob(jobConf);
    JobID id = runJob.getID();
    JobInfo jInfo = wovenClient.getJobInfo(id);
    Assert.assertNotNull("Job information is null",jInfo);

    Assert.assertTrue("Job has not been started for 1 min.", 
	jtClient.isJobStarted(id));
    JobStatus[] jobStatus = client.getAllJobs();
    String userName = jobStatus[0].getUsername();

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
    Assert.assertTrue("Map process tree is not alive before task suspend.", 
        ttIns.isProcessTreeAlive(pid));
    LOG.info("Suspend the task of process id " + pid);
    ExecuteShellCommand execcmd = new ExecuteShellCommand(userName, 
           ttClientIns.getHostName(), "kill -SIGSTOP " + pid);
    execcmd.start();
    execcmd.join();
    UtilsForTests.waitFor(30000);
    Assert.assertTrue("Process(" + pid + ") has not been suspended", 
        execcmd.getStatus());
    ttIns = ttClientIns.getProxy();
    UtilsForTests.waitFor(1000);
    Assert.assertTrue("Map process is still alive after task has been failed.", 
        !ttIns.isProcessTreeAlive(pid));
  }

  /**
   * Verify the process tree cleanup of task after task 
   * is suspended and resumed the task before the timeout.
   */
  @Test
  public void testProcessTreeCleanupOfSuspendAndResumeTask() throws
      Exception {
    TaskInfo taskInfo = null;
    TaskID tID = null;
    TTTaskInfo [] ttTaskinfo = null;
    String pid = null;
    TTProtocol ttIns = null;
    TTClient ttClientIns = null;
    int counter = 0;

    JobConf jobConf = new JobConf(conf);
    jobConf.setJobName("Message Display");
    jobConf.setJarByClass(GenerateTaskChildProcess.class);
    jobConf.setMapperClass(GenerateTaskChildProcess.StrDisplayMapper.class);
    jobConf.setNumMapTasks(1);
    jobConf.setNumReduceTasks(0);
    jobConf.setMaxMapAttempts(1);
    cleanup(outputDir, conf);
    FileInputFormat.setInputPaths(jobConf, inputDir);
    FileOutputFormat.setOutputPath(jobConf, outputDir);

    JTClient jtClient = cluster.getJTClient();
    JobClient client = jtClient.getClient();
    JTProtocol wovenClient = cluster.getJTClient().getProxy();
    RunningJob runJob = client.submitJob(jobConf);
    JobID id = runJob.getID();
    JobInfo jInfo = wovenClient.getJobInfo(id);
    Assert.assertNotNull("Job information is null",jInfo);

    Assert.assertTrue("Job has not been started for 1 min.", 
        jtClient.isJobStarted(id));

    JobStatus[] jobStatus = client.getAllJobs();
    String userName = jobStatus[0].getUsername();

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
    Assert.assertTrue("Map process tree is not alive before task suspend.", 
        ttIns.isProcessTreeAlive(pid));
    LOG.info("Suspend the task of process id " + pid);
    ExecuteShellCommand execcmd = new ExecuteShellCommand(userName, 
           ttClientIns.getHostName(), "kill -SIGSTOP " + pid);
    execcmd.start();
    execcmd.join();

    Assert.assertTrue("Process(" + pid + ") has not been suspended", 
        execcmd.getStatus());
    Assert.assertTrue("Map process is not alive after task "
        + "has been suspended.", ttIns.isProcessTreeAlive(pid));
    UtilsForTests.waitFor(5000);
    ExecuteShellCommand execcmd1 = new ExecuteShellCommand(userName, 
           ttClientIns.getHostName(), "kill -SIGCONT " + pid);
    execcmd1.start();
    execcmd1.join();
    Assert.assertTrue("Suspended process(" + pid + ") has not been resumed", 
        execcmd1.getStatus());
    UtilsForTests.waitFor(5000);
    Assert.assertTrue("Map process tree is not alive after task is resumed.", 
        ttIns.isProcessTreeAlive(pid));
  }
  
  private static void cleanup(Path dir, Configuration conf) throws 
      IOException {
    FileSystem fs = dir.getFileSystem(conf);
    fs.delete(dir, true);
  }
  
  private static void createInput(Path inDir, Configuration conf) throws 
      IOException {
    String input = "Hadoop is framework for data intensive distributed " 
        + "applications.\n Hadoop enables applications " 
        + "to work with thousands of nodes.";
    FileSystem fs = inDir.getFileSystem(conf);
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Failed to create the input directory:" 
          + inDir.toString());
    }
    fs.setPermission(inDir, new FsPermission(FsAction.ALL, 
        FsAction.ALL, FsAction.ALL));
    DataOutputStream file = fs.create(new Path(inDir, "data.txt"));
    int i = 0;
    while(i < 10) {
      file.writeBytes(input);
      i++;
    }
    file.close();
  }

  class ExecuteShellCommand extends Thread {
    String userName;
    String cmd;
    String hostName;
    boolean exitStatus;
    public ExecuteShellCommand(String userName, String hostName, String cmd) {
      this.userName = userName;
      this.hostName = hostName;
      this.cmd = cmd;
    }
    public void run() {
      try {
        RemoteExecution.executeCommand(hostName, userName, cmd);
        exitStatus = true;
      } catch(InterruptedException iexp) {
        LOG.warn("Thread is interrupted:" + iexp.getMessage());
        exitStatus = false;
      } catch(Exception exp) {
        LOG.warn("Exception:" + exp.getMessage());
        exitStatus = false;
      }
    }
    public boolean getStatus(){
      return exitStatus;
    }
  }
}
