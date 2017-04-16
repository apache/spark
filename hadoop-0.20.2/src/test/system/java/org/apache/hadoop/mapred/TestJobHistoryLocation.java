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

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.TTClient;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;
import org.apache.hadoop.mapreduce.test.system.MRCluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.UtilsForTests;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.examples.SleepJob;
import java.io.DataOutputStream;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

/**
 * Verify the retired job history Location.
 */

public class TestJobHistoryLocation {

  private static MRCluster cluster = null;
  private static FileSystem dfs = null;
  private static JobClient jobClient = null;
  private static String jobHistoryDonePathString = null;
  private static int count = 0;
  private static int fileCount = 0;
  private static boolean jobFileFound = false;
  private static int retiredJobInterval = 0;
  private static Configuration conf = null;

  static final Log LOG = LogFactory.
      getLog(TestJobHistoryLocation.class);

  public TestJobHistoryLocation() throws Exception {
  }

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = MRCluster.createCluster(new Configuration());
    String [] expExcludeList = {"java.net.ConnectException",
        "java.io.IOException", "org.apache.hadoop.metrics2.MetricsException"};
    cluster.setExcludeExpList(expExcludeList);

    conf = new Configuration(cluster.getConf());
    cluster.setUp();
    jobClient = cluster.getJTClient().getClient();
    dfs = jobClient.getFs();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    cluster.tearDown();
  }

  /**
   * This tests when successful / failed jobs are retired, the location
   * of the retired jobs are as according to 
   * mapred.job.tracker.history.completed.location.
   * This tests when there are 100 files in the done directory,
   * still the retired jobs are as according to
   * mapred.job.tracker.history.completed.location.
   * @param none
   * @return void
   */
  @Test
  public void testRetiredJobsHistoryLocation() throws Exception {
    JTProtocol remoteJTClient = cluster.getJTClient().getProxy();
    int testIterationLoop = 0;

    do {
      SleepJob job = null;
      testIterationLoop++;
      job = new SleepJob();
      job.setConf(conf);
      conf = job.setupJobConf(5, 1, 100, 100, 100, 100);
      //Get the value of mapred.jobtracker.retirejob.check. If not
      //found then use 60000 milliseconds, which is the application default.
      retiredJobInterval = 
        conf.getInt("mapred.jobtracker.retirejob.check", 60000);
      //Assert if retiredJobInterval is 0
      if ( retiredJobInterval == 0 ) {
        Assert.fail("mapred.jobtracker.retirejob.check is 0");
      }

      conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);
      jobFileFound = false;

      JobConf jconf = new JobConf(conf);
      jobHistoryDonePathString = null;
      jobHistoryDonePathString = jconf.
          get("mapred.job.tracker.history.completed.location");
      //Assert if jobHistoryDonePathString is null
      Assert.assertNotNull("mapred.job.tracker.history.completed.location " +
          "is null", jobHistoryDonePathString); 

      LOG.info("jobHistoryDonePath location is :" + jobHistoryDonePathString);

      FileStatus[] jobHistoryDoneFileStatuses = dfs.
          listStatus(new Path (jobHistoryDonePathString));
      String jobHistoryPathString = jconf.get("hadoop.job.history.location");

      //Submitting the job
      RunningJob rJob = cluster.getJTClient().getClient().submitJob(jconf);

      JobID jobID = rJob.getID();
      JobInfo jInfo = remoteJTClient.getJobInfo(jobID);
      String jobIDString = jobID.toString();
      LOG.info("jobIDString is :" + jobIDString);

      //Assert if jobInfo is null
      Assert.assertNotNull("jobInfo is null", jInfo);

      waitTillRunState(jInfo, jobID, remoteJTClient);

      if (jobHistoryPathString != null) {
        FileStatus[] jobHistoryFileStatuses = dfs.
          listStatus(new Path (jobHistoryPathString));
        jobFileFound = false;
        for (FileStatus jobHistoryFileStatus : jobHistoryFileStatuses) {
          if ((jobHistoryFileStatus.getPath().toString()).
              matches(jobIDString)) {
            jobFileFound = true;
            break;
          }
        }
        Assert.assertTrue("jobFileFound is false", jobFileFound);
      }

      TaskInfo[] taskInfos = cluster.getJTClient().getProxy()
          .getTaskInfo(rJob.getID());

      //Killing this job will happen only in the second iteration.
      if (testIterationLoop == 2) {
        //Killing the job because all the verification needed
        //for this testcase is completed.
        rJob.killJob();
      }

      //Making sure that the job is complete.
      count = 0;
      while (jInfo != null && !jInfo.getStatus().isJobComplete()) {
        UtilsForTests.waitFor(10000);
        count++;
        jInfo = remoteJTClient.getJobInfo(rJob.getID());
        //If the count goes beyond 100 seconds, then break; This is to avoid
        //infinite loop.
        if (count > 10) {
          Assert.fail("job has not reached running state for more than" +
              "100 seconds. Failing at this point");
        }
      }

      //After checking for Job Completion, waiting for 4 times of 
      //retiredJobInterval seconds for the job to go to retired state 
      UtilsForTests.waitFor(retiredJobInterval * 4);


      jobHistoryDoneFileStatuses = dfs.
          listStatus(new Path (jobHistoryDonePathString));

      checkJobHistoryFileInformation( jobHistoryDoneFileStatuses, jobIDString);  
      Assert.assertTrue("jobFileFound is false. Job History " +
        "File is not found in the done directory",
          jobFileFound);

      Assert.assertEquals("Both the job related files are not found",
        fileCount, 2);

    } while ( testIterationLoop < 2 );
  }

  /**
   * This tests when multiple instances of successful / failed jobs are 
   * retired, the location of the retired jobs are as according to 
   * mapred.job.tracker.history.completed.location 
   * @param none
   * @return void
   */
  @Test
  public void testRetiredMultipleJobsHistoryLocation() throws Exception {
    Configuration conf = new Configuration(cluster.getConf());
    JTProtocol remoteJTClient = cluster.getJTClient().getProxy();
    int testIterationLoop = 0;
    FileStatus[] jobHistoryDoneFileStatuses;
    RunningJob[] rJobCollection = new RunningJob[4];
    JobID[] rJobIDCollection = new JobID[4];
    String jobHistoryDonePathString = null;
    JobInfo jInfo = null;
    for ( int noOfJobs = 0; noOfJobs < 4; noOfJobs++ ) {
      SleepJob job = null;
      testIterationLoop++;
      job = new SleepJob();
      job.setConf(conf);
      conf = job.setupJobConf(5, 1, 100, 100, 100, 100);
      conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", 
        false);
      JobConf jconf = new JobConf(conf);

      jobHistoryDonePathString = null;
      jobHistoryDonePathString = jconf.
          get("mapred.job.tracker.history.completed.location");
      //Assert if jobHistoryDonePathString is null
      Assert.assertNotNull("mapred.job.tracker.history.completed.location "
          + "is null", jobHistoryDonePathString);

      LOG.info("jobHistoryDonePath location is :" + 
          jobHistoryDonePathString);

      //Submitting the job
      RunningJob rJob = cluster.getJTClient().getClient().submitJob(jconf);
      JobID jobID = rJob.getID();
     
      rJobCollection[noOfJobs] = rJob;
      rJobIDCollection[noOfJobs] = jobID;

      jInfo = remoteJTClient.getJobInfo(jobID);
      LOG.info("jobIDString is :" + jobID.toString());
      //Assert if jobInfo is null
      Assert.assertNotNull("jobInfo is null", jInfo);
    }

    //Wait for the jobs to start running.
    for (int noOfJobs = 0; noOfJobs < 4; noOfJobs++) {
      waitTillRunState(jInfo, rJobIDCollection[noOfJobs], remoteJTClient);
    }

    //Killing two jobs 
    (rJobCollection[0]).killJob();
    (rJobCollection[3]).killJob();

    //Making sure that the jobs are complete.
    for (int noOfJobs = 0; noOfJobs < 4; noOfJobs++) {
      count = 0;
      while (remoteJTClient.getJobInfo(rJobIDCollection[noOfJobs]) != null && 
          !(remoteJTClient.getJobInfo(rJobIDCollection[noOfJobs])).
          getStatus().isJobComplete()) {
        UtilsForTests.waitFor(10000);
        count++;
        //If the count goes beyond 100 seconds, then break; This is to avoid
        //infinite loop.
        if (count > 20) {
          Assert.fail("job has not reached completed state for more than" +
              "200 seconds. Failing at this point");
        }
      }
    }

    //After checking for Job Completion, waiting for 4 times 
    // of retiredJobInterval seconds for the job to go to retired state 
    UtilsForTests.waitFor(retiredJobInterval * 4);

    jobHistoryDoneFileStatuses = dfs.
        listStatus(new Path (jobHistoryDonePathString));

    for (int noOfJobs = 0; noOfJobs < 4; noOfJobs++) {
      checkJobHistoryFileInformation( jobHistoryDoneFileStatuses, 
          (rJobIDCollection[noOfJobs]).toString());
    Assert.assertTrue("jobFileFound is false. Job History " +
        "File is not found in the done directory",
          jobFileFound);
    Assert.assertEquals("Both the job related files are not found",
        fileCount, 2);

    }
  }

  //Waiting till job starts running
  private void waitTillRunState(JobInfo jInfo, JobID jobID, 
      JTProtocol remoteJTClient) throws Exception { 
    int count = 0;
    while (jInfo != null && jInfo.getStatus().getRunState()
        != JobStatus.RUNNING) {
      UtilsForTests.waitFor(10000);
      count++;
      jInfo = remoteJTClient.getJobInfo(jobID);
      //If the count goes beyond 100 seconds, then break; This is to avoid
      //infinite loop.
      if (count > 10) {
        Assert.fail("job has not reached running state for more than" +
            "100 seconds. Failing at this point");
      }
    }
  }
 
  //Checking for job file information in done directory
  //Since done directory has sub directories search under all 
  //the sub directories.
  private void checkJobHistoryFileInformation( FileStatus[] 
      jobHistoryDoneFileStatuses, String jobIDString ) throws Exception {
    fileCount = 0;
    jobFileFound = false;
    for (FileStatus jobHistoryDoneFileStatus : jobHistoryDoneFileStatuses) {
      FileStatus[] jobHistoryDoneFileStatuses1 = dfs.
          listStatus(jobHistoryDoneFileStatus.getPath());
      for (FileStatus jobHistoryDoneFileStatus1 : jobHistoryDoneFileStatuses1) {
        FileStatus[] jobHistoryDoneFileStatuses2 = dfs.
          listStatus(jobHistoryDoneFileStatus1.getPath());
        for (FileStatus jobHistoryDoneFileStatus2 : 
          jobHistoryDoneFileStatuses2) {

          FileStatus[] jobHistoryDoneFileStatuses3 = dfs.
            listStatus(jobHistoryDoneFileStatus2.getPath());
          for (FileStatus jobHistoryDoneFileStatus3 : 
            jobHistoryDoneFileStatuses3) {

            FileStatus[] jobHistoryDoneFileStatuses4 = dfs.
              listStatus(jobHistoryDoneFileStatus3.getPath());
            for (FileStatus jobHistoryDoneFileStatus4 : 
              jobHistoryDoneFileStatuses4) {

              FileStatus[] jobHistoryDoneFileStatuses5 = dfs.
                listStatus(jobHistoryDoneFileStatus4.getPath());
              for (FileStatus jobHistoryDoneFileStatus5 : 
                jobHistoryDoneFileStatuses5) {
      
                FileStatus[] jobHistoryDoneFileStatuses6 = dfs.
                  listStatus(jobHistoryDoneFileStatus5.getPath());
                for (FileStatus jobHistoryDoneFileStatus6 : 
                  jobHistoryDoneFileStatuses6) {

                  if ( (jobHistoryDoneFileStatus6.getPath().toString()).
                      indexOf(jobIDString) != -1 ) {
                    jobFileFound = true;
                    fileCount++;
                    //Both the conf file and the job file has to be present
                    if (fileCount == 2) {
                      break;
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
