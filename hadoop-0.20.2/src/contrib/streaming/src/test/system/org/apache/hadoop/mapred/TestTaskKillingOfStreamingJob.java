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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobClient.NetworkedJob;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.streaming.StreamJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

public class TestTaskKillingOfStreamingJob {
  private static final Log LOG = LogFactory
          .getLog(TestTaskKillingOfStreamingJob.class);
  private static MRCluster cluster;
  private static Configuration conf = new Configuration();
  private static Path inputDir = new Path("input");
  private static Path outputDir = new Path("output");
  private JTClient jtClient = null;
  private JobClient client = null;
  private JTProtocol wovenClient = null;

  @BeforeClass
  public static void before() throws Exception {
    String [] expExcludeList = {"java.net.ConnectException",
        "java.io.IOException"};
    cluster = MRCluster.createCluster(conf);
    cluster.setExcludeExpList(expExcludeList);
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
   * Set the sleep time for the tasks is 3 seconds and kill the task using sigkill.
   * Verify whether task is killed after 3 seconds or not. 
   */
  @Test
  public void testStatusOfKilledTaskWithSignalSleepTime() 
      throws IOException, Exception {
    String runtimeArgs [] = {
        "-D", "mapred.job.name=Numbers Sum",
        "-D", "mapred.map.tasks=1",
        "-D", "mapred.reduce.tasks=1",
        "-D", "mapred.tasktracker.tasks.sleeptime-before-sigkill=3000" };

    JobID jobId = getJobIdOfRunningStreamJob(runtimeArgs);    
    Assert.assertNotNull("Job ID not found for 1 min", jobId);
    Assert.assertTrue("Job has not been started for 1 min.", 
        jtClient.isJobStarted(jobId));
    
    TaskInfo taskInfo = getTaskInfoOfRunningStreamJob(jobId);
    Assert.assertNotNull("TaskInfo is null",taskInfo);
    Assert.assertTrue("Task has not been started for 1 min.", 
        jtClient.isTaskStarted(taskInfo));

    JobInfo jInfo = wovenClient.getJobInfo(jobId); 
    NetworkedJob networkJob = client.new NetworkedJob(jInfo.getStatus());
    TaskID tID = TaskID.downgrade(taskInfo.getTaskID());
    TaskAttemptID taskAttID = new TaskAttemptID(tID, 0);
    networkJob.killTask(taskAttID, false);

    int counter = 0;
    while (counter++ < 60) {
      if (taskInfo.getTaskStatus().length == 0) {
        UtilsForTests.waitFor(1000);
        taskInfo = wovenClient.getTaskInfo(taskInfo.getTaskID());
      } else if (taskInfo.getTaskStatus()[0].getRunState() == 
          TaskStatus.State.RUNNING) {
        UtilsForTests.waitFor(1000);
        taskInfo = wovenClient.getTaskInfo(taskInfo.getTaskID());
      } else if (taskInfo.getTaskStatus()[0].getRunState() == 
          TaskStatus.State.KILLED_UNCLEAN) {
        UtilsForTests.waitFor(1000);
        taskInfo = wovenClient.getTaskInfo(taskInfo.getTaskID());
      } else {
        break;
      }
    }
    Assert.assertTrue("Task has been killed before sigkill " + 
        "sleep time of 3 secs.", counter > 3 && TaskStatus.State.KILLED == 
        taskInfo.getTaskStatus()[0].getRunState());

    LOG.info("Waiting till the job is completed...");
    while (!jInfo.getStatus().isJobComplete()) {
      UtilsForTests.waitFor(100);
      jInfo = wovenClient.getJobInfo(jobId);
    }
    Assert.assertEquals("Job has not been succeeded.", 
            jInfo.getStatus().getRunState(), JobStatus.SUCCEEDED);
  }
 
  /**
   * Set the maximum attempts for the maps and reducers are one.
   * Failed the task and verify whether streaming job is failed or not.
   */
  @Test
  public void testStreamingJobStatusForFailedTask() throws IOException {
    String runtimeArgs [] = {
        "-D", "mapred.job.name=Numbers Sum",
        "-D", "mapred.map.tasks=1",
        "-D", "mapred.reduce.tasks=1",
        "-D", "mapred.map.max.attempts=1",
        "-D", "mapred.reduce.max.attempts=1"};

    JobID jobId = getJobIdOfRunningStreamJob(runtimeArgs);
    Assert.assertNotNull("Job ID not found for 1 min", jobId);
    Assert.assertTrue("Job has not been started for 1 min.", 
        jtClient.isJobStarted(jobId));

    TaskInfo taskInfo = getTaskInfoOfRunningStreamJob(jobId);
    Assert.assertNotNull("TaskInfo is null",taskInfo);
    Assert.assertTrue("Task has not been started for 1 min.", 
        jtClient.isTaskStarted(taskInfo));
    
    JobInfo jInfo = wovenClient.getJobInfo(jobId);
    NetworkedJob networkJob = client.new NetworkedJob(jInfo.getStatus());
    TaskID tID = TaskID.downgrade(taskInfo.getTaskID());
    TaskAttemptID taskAttID = new TaskAttemptID(tID, 0);
    networkJob.killTask(taskAttID, true);

    int counter = 0;
    while (counter++ < 60) {
      if (taskInfo.getTaskStatus().length == 0) {
        UtilsForTests.waitFor(1000);
        taskInfo = wovenClient.getTaskInfo(taskInfo.getTaskID());
      }else if (taskInfo.getTaskStatus()[0].getRunState() == 
          TaskStatus.State.RUNNING) {
        UtilsForTests.waitFor(1000);
        taskInfo = wovenClient.getTaskInfo(taskInfo.getTaskID());
      } else if (taskInfo.getTaskStatus()[0].getRunState() == 
          TaskStatus.State.FAILED_UNCLEAN) {
        UtilsForTests.waitFor(1000);
        taskInfo = wovenClient.getTaskInfo(taskInfo.getTaskID());
      } else {
        break;
      }
    }
    Assert.assertTrue("Task has not been Failed" , TaskStatus.State.FAILED == 
        taskInfo.getTaskStatus()[0].getRunState());

    LOG.info("Waiting till the job is completed...");
    while (!jInfo.getStatus().isJobComplete()) {
      UtilsForTests.waitFor(100);
      jInfo = wovenClient.getJobInfo(jobId);
    }
    Assert.assertEquals("Job has not been failed", 
        jInfo.getStatus().getRunState(), JobStatus.FAILED);
  }

  private TaskInfo getTaskInfoOfRunningStreamJob(JobID jobId) 
      throws IOException {
    TaskInfo taskInfo = null;
    wovenClient = cluster.getJTClient().getProxy();
    JobInfo jInfo = wovenClient.getJobInfo(jobId);
    JobStatus jobStatus = jInfo.getStatus();
    // Make sure that map is running and start progress 10%. 
    while (jobStatus.mapProgress() < 0.1f) {
      UtilsForTests.waitFor(100);
      jobStatus = wovenClient.getJobInfo(jobId).getStatus();
    }
    TaskInfo[] taskInfos = wovenClient.getTaskInfo(jobId);
    for (TaskInfo taskinfo : taskInfos) {
      if (!taskinfo.isSetupOrCleanup()) {
        taskInfo = taskinfo;
      }
    }
    return taskInfo;
  }
  
  private JobID getJobIdOfRunningStreamJob(String [] runtimeArgs) 
      throws IOException {
    JobID jobId = null;
    StreamJob streamJob = new StreamJob();
    int counter = 0;
    jtClient = cluster.getJTClient();
    client = jtClient.getClient();
    int totalJobs = client.getAllJobs().length;
    String [] streamingArgs = generateArgs(runtimeArgs);
    cleanup(outputDir, conf);
    conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);
    final RunStreamingJob streamJobThread = new RunStreamingJob(conf,
        streamJob,streamingArgs);
    streamJobThread.start();
    while (counter++ < 60) {
      if (client.getAllJobs().length - totalJobs == 0) {
        UtilsForTests.waitFor(1000);
      } else if (client.getAllJobs()[0].getRunState() == JobStatus.RUNNING) {
        jobId = client.getAllJobs()[0].getJobID();
        break;
      } else {
       UtilsForTests.waitFor(1000);
      }
    }  
    return jobId;
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
    int i = 0;
    while(i++ < 200) {
      file.writeBytes(i + "\n");
    }
    file.close();
  }
  
  private static void cleanup(Path dir, Configuration conf) 
      throws IOException {
    FileSystem fs = dir.getFileSystem(conf);
    fs.delete(dir, true);
  }

  private String[] generateArgs(String [] runtimeArgs) {
    String shellFile = System.getProperty("user.dir") + 
        "/src/test/system/scripts/StreamMapper.sh";
    String [] otherArgs = new String[] {
            "-input", inputDir.toString(),
            "-output", outputDir.toString(),
            "-mapper", "StreamMapper.sh",
            "-reducer", "/bin/cat"
    };
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
  
  class RunStreamingJob extends Thread {
    Configuration jobConf;
    Tool tool;
    String [] args;
    public RunStreamingJob(Configuration jobConf, Tool tool, String [] args) {
      this.jobConf = jobConf;
      this.tool = tool;
      this.args = args;
    }
    public void run() {
      try {
        runStreamingJob();
      } catch(InterruptedException iexp) {
        LOG.warn("Thread is interrupted:" + iexp.getMessage());
      } catch(Exception exp) {
        LOG.warn("Exception:" + exp.getMessage());
      }
    }
    private void runStreamingJob() throws Exception{
       ToolRunner.run(jobConf, tool, args);
    }
  }
}
