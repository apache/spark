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
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.mapred.JobQueueInfo;
import org.apache.hadoop.mapred.JobHistory.Keys;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import testjar.GenerateTaskChildProcess;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.security.UserGroupInformation;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
/**
 * Verifying the job-summary information at the end of a job.
 */
public class TestJobSummary {
  private static final Log LOG = LogFactory.getLog(TestJobSummary.class);
  private static MRCluster cluster;
  private static JobClient jobClient = null;
  private static JTProtocol remoteJTClient = null;
  private static JTClient jtClient;
  private static Configuration conf = new Configuration();
  private static Path inputDir = new Path("input");
  private static Path outputDir = new Path("output");
  private static String queueName = null;
  
  @BeforeClass
  public static void before() throws Exception {
    String [] expExcludeList = {"java.net.ConnectException",
                                "java.io.IOException"};
    cluster = MRCluster.createCluster(conf);
    cluster.setExcludeExpList(expExcludeList);
    cluster.setUp();
    createInput(inputDir, conf);
    jtClient = cluster.getJTClient();
    jobClient = jtClient.getClient();
    remoteJTClient = cluster.getJTClient().getProxy();
    conf = remoteJTClient.getDaemonConf();
  }

  @AfterClass
  public static void after() throws Exception {
    cleanup(inputDir, conf);
    cleanup(outputDir, conf);
    cluster.tearDown();
  }

  /**
   * Verifying the job summary information for killed job.
   */
  @Test
  public void testJobSummaryInfoOfKilledJob() throws IOException, 
          InterruptedException {
    SleepJob job = new SleepJob();
    conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);
    job.setConf(conf);
    conf = job.setupJobConf(2, 1, 4000, 4000, 100, 100);
    JobConf jobConf = new JobConf(conf);
    RunningJob runJob = jobClient.submitJob(jobConf);
    JobID jobId = runJob.getID();
    Assert.assertTrue("Job has not been started for 1 min.", 
            jtClient.isJobStarted(jobId));
    jobClient.killJob(jobId);
    LOG.info("Waiting till the job is completed...");
    Assert.assertTrue("Job has not been completed for 1 min.", 
        jtClient.isJobStopped(jobId));
    JobInfo jInfo = remoteJTClient.getJobInfo(jobId);
    Assert.assertEquals("Job has not been succeeded", 
        jInfo.getStatus().getRunState(), JobStatus.KILLED);
    verifyJobSummaryInfo(jInfo,jobId);
  }
  
  /**
   * Verifying the job summary information for failed job.
   */
  @Test
  public void testJobSummaryInfoOfFailedJob() throws IOException, 
          InterruptedException {
    conf = remoteJTClient.getDaemonConf();
    conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);
    JobConf jobConf = new JobConf(conf);
    jobConf.setJobName("Fail Job");
    jobConf.setJarByClass(GenerateTaskChildProcess.class);
    jobConf.setMapperClass(GenerateTaskChildProcess.FailMapper.class);
    jobConf.setReducerClass(IdentityReducer.class);
    jobConf.setNumMapTasks(1);
    jobConf.setNumReduceTasks(1);
    cleanup(outputDir, conf);
    FileInputFormat.setInputPaths(jobConf, inputDir);
    FileOutputFormat.setOutputPath(jobConf, outputDir);
    RunningJob runJob = jobClient.submitJob(jobConf);
    JobID jobId = runJob.getID();  
    Assert.assertTrue("Job has not been started for 1 min.", 
            jtClient.isJobStarted(jobId));
    LOG.info("Waiting till the job is completed...");
    Assert.assertTrue("Job has not been completed for 1 min.",
        jtClient.isJobStopped(jobId));
    JobInfo  jInfo = remoteJTClient.getJobInfo(jobId);
    Assert.assertEquals("Job has not been failed", 
        jInfo.getStatus().getRunState(), JobStatus.FAILED);
    verifyJobSummaryInfo(jInfo,jobId);
  }
  
  /**
   * Submit the job in different queue and verifying 
   * the job queue information in job summary 
   * after job is completed.
   */
  @Test
  public void testJobQueueInfoInJobSummary() throws IOException, 
  InterruptedException {
    SleepJob job = new SleepJob();
    job.setConf(conf);
    conf = job.setupJobConf(2, 1, 4000, 4000, 100, 100);
    conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);
    JobConf jobConf = new JobConf(conf);
    JobQueueInfo [] queues = jobClient.getQueues();
    for (JobQueueInfo queueInfo : queues ){
      if (!queueInfo.getQueueName().equals("default")) {
        queueName = queueInfo.getQueueName();
        break;
      }
    }
    Assert.assertNotNull("No multiple queues in the cluster.",queueName);
    LOG.info("queueName:" + queueName);
    jobConf.setQueueName(queueName);
    RunningJob runJob = jobClient.submitJob(jobConf);
    JobID jobId = runJob.getID();    
    Assert.assertTrue("Job has not been started for 1 min.", 
        jtClient.isJobStarted(jobId));
    LOG.info("Waiting till the job is completed...");
    Assert.assertTrue("Job has not been completed for 1 min.",
        jtClient.isJobStopped(jobId));
    JobInfo jInfo = remoteJTClient.getJobInfo(jobId);
    Assert.assertEquals("Job has not been succeeded", 
        jInfo.getStatus().getRunState(), JobStatus.SUCCEEDED);
    verifyJobSummaryInfo(jInfo,jobId);
  }

  /**
   * Verify the job summary information for high RAM jobs.
   */
  @Test
  public void testJobSummaryInfoOfHighMemoryJob() throws IOException,
      Exception {
    final HighRamJobHelper helper = new HighRamJobHelper();
    JobID jobId = helper.runHighRamJob(conf, jobClient, remoteJTClient,
        "Job did not succeed");
    JobInfo jInfo = remoteJTClient.getJobInfo(jobId);
    verifyJobSummaryInfo(jInfo,jobId);
  }

  @Test
  public void testJobSummaryInfoForDifferentUser() throws Exception {
    UserGroupInformation proxyUGI;
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    ArrayList<String> users = cluster.getHadoopMultiUsersList();
    Assert.assertTrue("proxy users are not found.", users.size() > 0);
    if (conf.get("hadoop.security.authentication").equals("simple")) {
      proxyUGI = UserGroupInformation.createRemoteUser(
          users.get(0));
    } else {
      proxyUGI = UserGroupInformation.createProxyUser(
      users.get(0), ugi);
    }
    SleepJob job = new SleepJob();
    job.setConf(conf);
    final JobConf jobConf = job.setupJobConf(2, 1, 2000, 2000, 100, 100);
    final JobClient jClient =
    	proxyUGI.doAs(new PrivilegedExceptionAction<JobClient>() {
          public JobClient run() throws IOException {
            return new JobClient(jobConf);
          }
        });
    RunningJob runJob = proxyUGI.doAs(
        new PrivilegedExceptionAction<RunningJob>() {
      public RunningJob run() throws IOException {
        return jClient.submitJob(jobConf);
      }
    });
    JobID jobId = runJob.getID();
    Assert.assertTrue("Job has not been started for 1 min.", 
        jtClient.isJobStarted(jobId));
    LOG.info("Waiting till the job is completed...");
    Assert.assertTrue("Job has not been completed for 1 min.",
        jtClient.isJobStopped(jobId));
    JobInfo jInfo = remoteJTClient.getJobInfo(jobId);
    Assert.assertEquals("Job has not been succeeded", 
        jInfo.getStatus().getRunState(), JobStatus.SUCCEEDED);
     verifyJobSummaryInfo(jInfo,jobId);  
  }
  

  private void verifyJobSummaryInfo(JobInfo jInfo, JobID id) 
      throws IOException {
    java.util.HashMap<String,String> map = jtClient.getJobSummary(id);
    Assert.assertEquals("Job id has not been matched", id.toString(),
        map.get("jobId"));
    Assert.assertEquals("User name has not been matched in JobSummary", 
        jInfo.getStatus().getUsername(), map.get("user"));    
    Assert.assertEquals("StartTime has not been  matched in JobSummary", 
        String.valueOf(jInfo.getStatus().getStartTime()), 
        map.get("startTime"));
    Assert.assertEquals("LaunchTime has not been matched in JobSummary", 
        String.valueOf(jInfo.getLaunchTime()), 
        map.get("launchTime"));
    Assert.assertEquals("FinshedTime has not been matched in JobSummary", 
        String.valueOf(jInfo.getFinishTime()), 
        map.get("finishTime"));
    Assert.assertEquals("Maps are not matched in Job summary", 
        String.valueOf(jInfo.numMaps()) , map.get("numMaps"));
    Assert.assertEquals("Reducers are not matched in Job summary", 
        String.valueOf(jInfo.numReduces()), map.get("numReduces"));
    Assert.assertEquals("Number of slots per map is not matched in Job summary", 
        String.valueOf(jInfo.getNumSlotsPerMap()), map.get("numSlotsPerMap"));
    Assert.assertEquals("Number of slots per reduce is not matched in Job summary", 
        String.valueOf(jInfo.getNumSlotsPerReduce()), map.get("numSlotsPerReduce"));
  }
  

  private static void cleanup(Path dir, Configuration conf) throws 
          IOException {
    FileSystem fs = dir.getFileSystem(conf);
    fs.delete(dir, true);
  }

  private static void createInput(Path inDir, Configuration conf) throws 
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
    while (i < 2) {
      file.writeBytes(input);
      i++;
    }
    file.close();
  }
  
}
