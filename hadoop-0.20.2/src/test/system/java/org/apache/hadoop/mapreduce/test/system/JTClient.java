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

package org.apache.hadoop.mapreduce.test.system;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapred.UtilsForTests;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.test.system.process.RemoteProcess;

/**
 * JobTracker client for system tests.
 */
public class JTClient extends MRDaemonClient<JTProtocol> {
  static final Log LOG = LogFactory.getLog(JTClient.class);
  private JobClient client;

  /**
   * Create JobTracker client to talk to {@link JobTracker} specified in the
   * configuration. <br/>
   * 
   * @param conf
   *          configuration used to create a client.
   * @param daemon
   *          the process management instance for the {@link JobTracker}
   * @throws IOException
   */
  public JTClient(Configuration conf, RemoteProcess daemon) throws 
    IOException {
    super(conf, daemon);
  }

  @Override
  public synchronized void connect() throws IOException {
    if (isConnected()) {
      return;
    }
    client = new JobClient(new JobConf(getConf()));
    setConnected(true);
  }

  @Override
  public synchronized void disconnect() throws IOException {
    client.close();
  }

  @Override
  public synchronized JTProtocol getProxy() {
    return (JTProtocol) client.getProtocol();
  }

  /**
   * Gets the {@link JobClient} which can be used for job submission. JobClient
   * which is returned would not contain the decorated API's. To be used for
   * submitting of the job.
   * 
   * @return client handle to the JobTracker
   */
  public JobClient getClient() {
    return client;
  }

  /**
   * Gets the configuration which the JobTracker is currently running.<br/>
   * 
   * @return configuration of JobTracker.
   * 
   * @throws IOException
   */
  public Configuration getJobTrackerConfig() throws IOException {
    return getProxy().getDaemonConf();
  }
  
  /**
   * Kills the job. <br/>
   * @param id of the job to be killed.
   * @throws IOException
   */
  public void killJob(JobID id) throws IOException {
    getClient().killJob(id);
  }

  /**
   * Verification API to check running jobs and running job states.
   * users have to ensure that their jobs remain running state while
   * verification is called. <br/>
   * 
   * @param id
   *          of the job to be verified.
   * 
   * @throws Exception
   */
  public void verifyRunningJob(JobID jobId) throws Exception {
  }

  private JobInfo getJobInfo(JobID jobId) throws IOException {
    JobInfo info = getProxy().getJobInfo(jobId);
    if (info == null && !getProxy().isJobRetired(jobId)) {
      Assert.fail("Job id : " + jobId + " has never been submitted to JT");
    }
    return info;
  }
  
  /**
   * Verification API to wait till job retires and verify all the retired state
   * is correct. 
   * <br/>
   * @param conf of the job used for completion
   * @return job handle
   * @throws Exception
   */
  public RunningJob submitAndVerifyJob(Configuration conf) throws Exception {
    JobConf jconf = new JobConf(conf);
    RunningJob rJob = getClient().submitJob(jconf);
    JobID jobId = rJob.getID();
    verifyRunningJob(jobId);
    verifyCompletedJob(jobId);
    return rJob;
  }
  
  /**
   * Verification API to check if the job completion state is correct. <br/>
   * 
   * @param id id of the job to be verified.
   */
  
  public void verifyCompletedJob(JobID id) throws Exception{
    RunningJob rJob = getClient().getJob(
        org.apache.hadoop.mapred.JobID.downgrade(id));
    while(!rJob.isComplete()) {
      LOG.info("waiting for job :" + id + " to retire");
      Thread.sleep(1000);
      rJob = getClient().getJob(
          org.apache.hadoop.mapred.JobID.downgrade(id));
    }
    verifyJobDetails(id);
    verifyJobHistory(id);
  }

  /**
   * Verification API to check if the job details are semantically correct.<br/>
   * 
   *  @param jobId
   *          jobID of the job
   * @param jconf
   *          configuration object of the job
   * @return true if all the job verifications are verified to be true
   * @throws Exception
   */
  public void verifyJobDetails(JobID jobId) throws Exception {
    // wait till the setup is launched and finished.
    JobInfo jobInfo = getJobInfo(jobId);
    if(jobInfo == null){
      return;
    }
    LOG.info("waiting for the setup to be finished");
    while (!jobInfo.isSetupFinished()) {
      Thread.sleep(2000);
      jobInfo = getJobInfo(jobId);
      if(jobInfo == null) {
        break;
      }
    }
    // verify job id.
    assertTrue(jobId.toString().startsWith("job_"));
    LOG.info("verified job id and is : " + jobId.toString());
    // verify the number of map/reduce tasks.
    verifyNumTasks(jobId);
    // should verify job progress.
    verifyJobProgress(jobId);
    jobInfo = getJobInfo(jobId);
    if(jobInfo == null) {
      return;
    }
    if (jobInfo.getStatus().getRunState() == JobStatus.SUCCEEDED) {
      // verify if map/reduce progress reached 1.
      jobInfo = getJobInfo(jobId);
      if (jobInfo == null) {
        return;
      }
      assertEquals(1.0, jobInfo.getStatus().mapProgress(), 0.001);
      assertEquals(1.0, jobInfo.getStatus().reduceProgress(), 0.001);
      // verify successful finish of tasks.
      verifyAllTasksSuccess(jobId);
    }
    if (jobInfo.getStatus().isJobComplete()) {
      // verify if the cleanup is launched.
      jobInfo = getJobInfo(jobId);
      if (jobInfo == null) {
        return;
      }
      assertTrue(jobInfo.isCleanupLaunched());
      LOG.info("Verified launching of cleanup");
    }
  }

  
  public void verifyAllTasksSuccess(JobID jobId) throws IOException {
    JobInfo jobInfo = getJobInfo(jobId);
    if (jobInfo == null) {
      return;
    }
    
    TaskInfo[] taskInfos = getProxy().getTaskInfo(jobId);
    
    if(taskInfos.length == 0 && getProxy().isJobRetired(jobId)) {
      LOG.info("Job has been retired from JT memory : " + jobId);
      return;
    }
    
    for (TaskInfo taskInfo : taskInfos) {
      TaskStatus[] taskStatus = taskInfo.getTaskStatus();
      if (taskStatus != null && taskStatus.length > 0) {
        int i;
        for (i = 0; i < taskStatus.length; i++) {
          if (TaskStatus.State.SUCCEEDED.equals(taskStatus[i].getRunState())) {
            break;
          }
        }
        assertFalse(i == taskStatus.length);
      }
    }
    LOG.info("verified that none of the tasks failed.");
  }
  
  public void verifyJobProgress(JobID jobId) throws IOException {
    JobInfo jobInfo;
    jobInfo = getJobInfo(jobId);
    if (jobInfo == null) {
      return;
    }
    assertTrue(jobInfo.getStatus().mapProgress() >= 0 && jobInfo.getStatus()
        .mapProgress() <= 1);
    LOG.info("verified map progress and is "
        + jobInfo.getStatus().mapProgress());    
    assertTrue(jobInfo.getStatus().reduceProgress() >= 0 && jobInfo.getStatus()
        .reduceProgress() <= 1);
    LOG.info("verified reduce progress and is "
        + jobInfo.getStatus().reduceProgress());
  }
  
  public void verifyNumTasks(JobID jobId) throws IOException {
    JobInfo jobInfo;
    jobInfo = getJobInfo(jobId);
    if (jobInfo == null) {
      return;
    }
    assertEquals(jobInfo.numMaps(), (jobInfo.runningMaps()
        + jobInfo.waitingMaps() + jobInfo.finishedMaps()));
    LOG.info("verified number of map tasks and is " + jobInfo.numMaps());
    
    assertEquals(jobInfo.numReduces(),  (jobInfo.runningReduces()
        + jobInfo.waitingReduces() + jobInfo.finishedReduces()));
    LOG.info("verified number of reduce tasks and is "
        + jobInfo.numReduces());
  }

  /**
   * Verification API to check if the job history file is semantically correct.
   * <br/>
   * 
   * 
   * @param id
   *          of the job to be verified.
   * @throws IOException
   */
  public void verifyJobHistory(JobID jobId) throws IOException {
    JobInfo info = getJobInfo(jobId);
    String url ="";
    if(info == null) {
      LOG.info("Job has been retired from JT memory : " + jobId);
      url = getProxy().getJobHistoryLocationForRetiredJob(jobId);
    } else {
      url = info.getHistoryUrl();
    }
    Path p = new Path(url);
    if (p.toUri().getScheme().equals("file:/")) {
      FileStatus st = getFileStatus(url, true);
      Assert.assertNotNull("Job History file for " + jobId + " not present " +
          "when job is completed" , st);
    } else {
      FileStatus st = getFileStatus(url, false);
      Assert.assertNotNull("Job History file for " + jobId + " not present " +
          "when job is completed" , st);
    }
    LOG.info("Verified the job history for the jobId : " + jobId);
  }

  /**
   * The method provides the information on the job has stopped or not
   * @return indicates true if the job has stopped false otherwise.
   * @param job id has the information of the running job.
   * @throw IOException is thrown if the job info cannot be fetched.   
   */
  public boolean isJobStopped(JobID id) throws IOException{
    int counter = 0;
    JobInfo jInfo = getProxy().getJobInfo(id);
    if(jInfo != null ) {
      while (counter < 60) {
        if (jInfo.getStatus().isJobComplete()) {
          break;
        }
        UtilsForTests.waitFor(1000);
        jInfo = getProxy().getJobInfo(id);
        counter ++;
      }
    }
    return (counter != 60)? true : false;
  }

  /**
   * It uses to check whether job is started or not.
   * @param id job id
   * @return true if job is running.
   * @throws IOException if an I/O error occurs.
   */
  public boolean isJobStarted(JobID id) throws IOException {
    JobInfo jInfo = getJobInfo(id);
    int counter = 0;
    while (counter < 60) {
      if (jInfo.getStatus().getRunState() == JobStatus.RUNNING) {
        break;
      } else {
        UtilsForTests.waitFor(1000);
        jInfo = getJobInfo(jInfo.getID());
        Assert.assertNotNull("Job information is null",jInfo);
      }
      counter++;
    }
    return (counter != 60)? true : false ;
  }

  /**
   * It uses to check whether task is started or not.
   * @param taskInfo task information
   * @return true if task is running.
   * @throws IOException if an I/O error occurs.
   */
  public boolean isTaskStarted(TaskInfo taskInfo) throws IOException { 
    JTProtocol wovenClient = getProxy();
    int counter = 0;
    while (counter < 60) {
      if (taskInfo.getTaskStatus().length > 0) {
        if (taskInfo.getTaskStatus()[0].getRunState() == 
            TaskStatus.State.RUNNING) {
          break;
        }
      }
      UtilsForTests.waitFor(1000);
      taskInfo = wovenClient.getTaskInfo(taskInfo.getTaskID());
      counter++;
    }
    return (counter != 60)? true : false;
  }
  /**
   * Get the jobtracker log files as pattern.
   * @return String - Jobtracker log file pattern.
   * @throws IOException - if I/O error occurs.
   */
  public String getJobTrackerLogFilePattern() throws IOException  {
    return getProxy().getFilePattern();
  }

  /**
   * It uses to get the job summary details of given job id. .
   * @param jobID - job id
   * @return HashMap -the job summary details as map.
   * @throws IOException if any I/O error occurs.
   */
  public HashMap<String,String> getJobSummary(JobID jobID)
      throws IOException {
    String output = getProxy().getJobSummaryInfo(jobID);
    StringTokenizer strToken = new StringTokenizer(output,",");
    HashMap<String,String> mapcollect = new HashMap<String,String>();
    while(strToken.hasMoreTokens()) {
      String keypair = strToken.nextToken();
      mapcollect.put(keypair.split("=")[0], keypair.split("=")[1]);
    }
    return mapcollect;
  }
}
