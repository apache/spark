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
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.streaming.StreamJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Increase memory usage beyond the memory limits of streaming job and 
 * verify whether task manager logs the process tree status 
 * before killing or not.
 */
public class TestStreamingJobProcessTree {
  private static final Log LOG = LogFactory
     .getLog(TestStreamingJobProcessTree.class);
  private static MRCluster cluster;
  private static Configuration conf = new Configuration();
  private static Path inputDir = new Path("input");
  private static Path outputDir = new Path("output");
  
  @BeforeClass
  public static void before() throws Exception {
    String [] excludeExpList = {"java.net.ConnectException",
      "java.io.IOException"};
    cluster = MRCluster.createCluster(conf);
    cluster.setExcludeExpList(excludeExpList);
    cluster.setUp();
    conf = cluster.getJTClient().getProxy().getDaemonConf();
    createInput(inputDir, conf);
  }
  @AfterClass
  public static void after() throws Exception {
   cleanup(inputDir, conf);
   cleanup(outputDir, conf);
   cluster.tearDown();
  }
  
  /**
   * Increase the memory limit for map task and verify whether the 
   * task manager logs the process tree status before killing or not.
   * @throws IOException - If an I/O error occurs.
   */
  @Test
  public void testStreamingJobProcTreeCleanOfMapTask() throws
     IOException {
    String runtimeArgs [] = {
        "-D", "mapred.job.name=ProcTreeStreamJob",
        "-D", "mapred.map.tasks=1",
        "-D", "mapred.reduce.tasks=0",
        "-D", "mapred.map.max.attempts=1",
        "-D", "mapred.cluster.max.map.memory.mb=2048",
        "-D", "mapred.cluster.reduce.memory.mb=1024",
        "-D", "mapred.cluster.max.reduce.memory.mb=2048",
        "-D", "mapred.cluster.map.memory.mb=1024",
        "-D", "mapred.job.map.memory.mb=512"
    };
    
    String [] otherArgs = new String[] {
            "-input", inputDir.toString(),
            "-output", outputDir.toString(),
            "-mapper", "ProcessTree.sh",
    };
    JobID jobId = getJobId(runtimeArgs, otherArgs);
    LOG.info("Job ID:" + jobId);
    if (jobId == null) {
      jobId = getJobId(runtimeArgs, otherArgs);
    }
    Assert.assertNotNull("Job ID not found for 1 min", jobId);
    Assert.assertTrue("Job has not been started for 1 min.", 
        cluster.getJTClient().isJobStarted(jobId));
    TaskInfo taskInfo = getTaskInfo(jobId, true);
    Assert.assertNotNull("TaskInfo is null",taskInfo);
    Assert.assertTrue("Task has not been started for 1 min.", 
        cluster.getJTClient().isTaskStarted(taskInfo));
    JTProtocol wovenClient = cluster.getJTClient().getProxy();
    int counter = 0;
    TaskInfo tempTaskInfo;
    while (counter++ < 60) {
      if (taskInfo.getTaskStatus().length == 0) {
        UtilsForTests.waitFor(1000);
        tempTaskInfo = taskInfo;
        taskInfo = wovenClient.getTaskInfo(taskInfo.getTaskID());
      }else if (taskInfo.getTaskStatus()[0].getRunState() ==
          TaskStatus.State.RUNNING) {
        UtilsForTests.waitFor(1000);
        tempTaskInfo = taskInfo;
        taskInfo = wovenClient.getTaskInfo(taskInfo.getTaskID());
      } else {
        break;
      }

      if (taskInfo == null) {
        taskInfo = tempTaskInfo;
        break;
      }
    }

    verifyProcessTreeOverLimit(taskInfo,jobId);
    JobInfo jInfo = wovenClient.getJobInfo(jobId);
    LOG.info("Waiting till the job is completed...");
    counter = 0;
    while (counter++ < 60) {
      if (jInfo == null) {
        break;
      } else if (jInfo.getStatus().isJobComplete()) {
        break;
      }
      UtilsForTests.waitFor(100);
      jInfo = wovenClient.getJobInfo(jobId);
    }
    UtilsForTests.waitFor(1000);
  }
  
  /**
   * Increase the memory limit for reduce task and verify whether the 
   * task manager logs the process tree status before killing or not.
   * @throws IOException - If an I/O error occurs.
   */
  @Test
  public void testStreamingJobProcTreeCleanOfReduceTask() throws
     IOException {
    String runtimeArgs [] = {
            "-D", "mapred.job.name=ProcTreeStreamJob",
            "-D", "mapred.reduce.tasks=1",
            "-D", "mapred.map.tasks=1",
            "-D", "mapred.reduce.max.attempts=1",
            "-D", "mapred.cluster.max.map.memory.mb=2048",
            "-D", "mapred.cluster.map.memory.mb=1024",
            "-D", "mapred.cluster.max.reduce.memory.mb=20248",
            "-D", "mapred.cluster.reduce.memory.mb=1024",
            "-D", "mapred.job.reduce.memory.mb=512"};

    String [] otherArgs = new String[] {
            "-input", inputDir.toString(),
            "-output", outputDir.toString(),
            "-mapper", "/bin/cat",
            "-reducer", "ProcessTree.sh"
    };

    cleanup(outputDir, conf);
    JobID jobId = getJobId(runtimeArgs, otherArgs);
    if (jobId == null) {
      jobId = getJobId(runtimeArgs, otherArgs);
    }
    Assert.assertNotNull("Job ID not found for 1 min", jobId);
    Assert.assertTrue("Job has not been started for 1 min.", 
        cluster.getJTClient().isJobStarted(jobId));
    TaskInfo taskInfo = getTaskInfo(jobId, false);
    Assert.assertNotNull("TaskInfo is null",taskInfo);
    Assert.assertTrue("Task has not been started for 1 min.", 
        cluster.getJTClient().isTaskStarted(taskInfo));    
    JTProtocol wovenClient = cluster.getJTClient().getProxy();
    int counter = 0;
    TaskInfo tempTaskInfo;
    while (counter++ < 60) {
      if (taskInfo.getTaskStatus().length == 0) {
        UtilsForTests.waitFor(1000);
        tempTaskInfo = taskInfo;
        taskInfo = wovenClient.getTaskInfo(taskInfo.getTaskID());
      }else if (taskInfo.getTaskStatus()[0].getRunState() == 
          TaskStatus.State.RUNNING) {
        UtilsForTests.waitFor(1000);
        tempTaskInfo = taskInfo;
        taskInfo = wovenClient.getTaskInfo(taskInfo.getTaskID());
      } else {
        break;
      }
      if (taskInfo == null) {
        taskInfo = tempTaskInfo;
        break;
      }
    }
    verifyProcessTreeOverLimit(taskInfo,jobId);
    JobInfo jInfo = wovenClient.getJobInfo(jobId); 
    LOG.info("Waiting till the job is completed...");
    counter = 0;
    while (counter++ < 60) {
      if(jInfo == null) {
        break;
      } else if (jInfo.getStatus().isJobComplete()) {
        break;
      }
      UtilsForTests.waitFor(1000);
      jInfo = wovenClient.getJobInfo(jobId);
    }
  }
  
  private void verifyProcessTreeOverLimit(TaskInfo taskInfo, JobID jobId) 
      throws IOException {
    String taskOverLimitPatternString = 
      "TaskTree \\[pid=[0-9]*,tipID=.*\\] is "
      + "running beyond memory-limits. "
      + "Current usage : [0-9]*bytes. Limit : %sbytes. Killing task.";
    Pattern taskOverLimitPattern = 
        Pattern.compile(String.format(taskOverLimitPatternString, 
        String.valueOf(512 * 1024 * 1024L)));
    LOG.info("Task OverLimit Pattern:" + taskOverLimitPattern);
    TaskID tID = TaskID.downgrade(taskInfo.getTaskID());
    TaskAttemptID taskAttID = new TaskAttemptID(tID, 0);
    JobClient jobClient = cluster.getJTClient().getClient();
    RunningJob runJob = jobClient.getJob(jobId);
    String[] taskDiagnostics = runJob.getTaskDiagnostics(taskAttID);
    Assert.assertNotNull("Task diagnostics is null.", taskDiagnostics);
    for (String strVal : taskDiagnostics) {
      Matcher mat = taskOverLimitPattern.matcher(strVal);
      Assert.assertTrue("Taskover limit error message is not matched.", 
          mat.find());
    }
  }
  
  private String[] buildArgs(String [] runtimeArgs, String[] otherArgs) {
    String shellFile = System.getProperty("user.dir") + 
        "/src/test/system/scripts/ProcessTree.sh";
    
    String fileArgs[] = new String[] {"-files", shellFile };
    int size = fileArgs.length + runtimeArgs.length + otherArgs.length;
    String args[]= new String[size];
    int index = 0;
    for (String fileArg : fileArgs) {
      args[index++] = fileArg;
    }
    for (String runtimeArg : runtimeArgs) {
      args[index++] = runtimeArg;
    }
    for (String otherArg : otherArgs) {
      args[index++] = otherArg;
    }
    return args;
  }
  
  private JobID getJobId(String [] runtimeArgs, String [] otherArgs) 
      throws IOException {
    JobID jobId = null;
    final RunStreamJob runSJ;
    StreamJob streamJob = new StreamJob();
    int counter = 0;
    JTClient jtClient = cluster.getJTClient();
    JobClient jobClient = jtClient.getClient();
    int totalJobs = jobClient.getAllJobs().length;
    String [] args = buildArgs(runtimeArgs, otherArgs);
    cleanup(outputDir, conf);
    conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);
    runSJ = new RunStreamJob(conf, streamJob, args);
    runSJ.start();
    while (counter++ < 60) {
      if (jobClient.getAllJobs().length - totalJobs == 0) {
        UtilsForTests.waitFor(1000);
      } else if (jobClient.getAllJobs()[0].getRunState() == JobStatus.RUNNING) {
        jobId = jobClient.getAllJobs()[0].getJobID();
        break;
      } else {
        UtilsForTests.waitFor(1000);
      }
    }  
    return jobId;
  }
  
  private TaskInfo getTaskInfo(JobID jobId, boolean isMap) 
      throws IOException {
    JTProtocol wovenClient = cluster.getJTClient().getProxy();
    JobInfo jInfo = wovenClient.getJobInfo(jobId);
    TaskInfo[] taskInfos = wovenClient.getTaskInfo(jobId);
    for (TaskInfo taskinfo : taskInfos) {
      if (!taskinfo.isSetupOrCleanup()) {
        if (taskinfo.getTaskID().isMap() == isMap) {
          return taskinfo;
        }
      }
    }
    return null;
  }

  private static void createInput(Path inDir, Configuration conf) 
     throws IOException {
    FileSystem fs = inDir.getFileSystem(conf);
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Failed to create the input directory:" 
          + inDir.toString());
    }
    fs.setPermission(inDir, new FsPermission(FsAction.ALL, 
    FsAction.ALL, FsAction.ALL));
    DataOutputStream file = fs.create(new Path(inDir, "data.txt"));
    String input="Process tree cleanup of Streaming job tasks.";
    file.writeBytes(input + "\n");
    file.close();
  }

  private static void cleanup(Path dir, Configuration conf) 
      throws IOException {
    FileSystem fs = dir.getFileSystem(conf);
    fs.delete(dir, true);
  }
  
  class RunStreamJob extends Thread {
    Configuration jobConf;
    Tool tool;
    String [] args;
    public RunStreamJob(Configuration jobConf, Tool tool, 
        String [] args) {
      this.jobConf = jobConf;
      this.tool = tool;
      this.args = args;
    }
    public void run() {
      try {
        ToolRunner.run(jobConf, tool, args);
      } catch(InterruptedException iexp) {
        LOG.warn("Thread is interrupted:" + iexp.getMessage());
      } catch(Exception exp) {
        LOG.warn("Exception:" + exp.getMessage());
      }
    }
  }
}
