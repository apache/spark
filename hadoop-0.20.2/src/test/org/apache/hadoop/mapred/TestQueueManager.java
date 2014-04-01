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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Set;
import java.util.TreeSet;

import java.io.File;
import java.util.Properties;

import javax.security.auth.login.LoginException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.QueueManager.QueueACL;
import org.apache.hadoop.security.UserGroupInformation;

public class TestQueueManager extends TestCase {

  private static final Log LOG = LogFactory.getLog(TestQueueManager.class);

  String submitAcl = QueueACL.SUBMIT_JOB.getAclName();
  String adminAcl  = QueueACL.ADMINISTER_JOBS.getAclName();

  MiniDFSCluster miniDFSCluster;
  MiniMRCluster miniMRCluster = null;
  
  /**
   * For some tests it is necessary to sandbox them in a doAs with a fake user
   * due to bug HADOOP-6527, which wipes out real group mappings. It's also
   * necessary to then add the real user running the test to the fake users
   * so that child processes can write to the DFS.
   */
  UserGroupInformation createNecessaryUsers() throws IOException {
    // Add real user to fake groups mapping so that child processes (tasks)
    // will have permissions on the dfs
    String j = UserGroupInformation.getCurrentUser().getShortUserName();
    UserGroupInformation.createUserForTesting(j, new String [] { "myGroup"});
    
    // Create a fake user for all processes to execute within
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting("Zork",
                                                 new String [] {"ZorkGroup"});
    return ugi;
  }
  
  public void testDefaultQueueConfiguration() {
    JobConf conf = new JobConf();
    QueueManager qMgr = new QueueManager(conf);
    Set<String> expQueues = new TreeSet<String>();
    expQueues.add("default");
    verifyQueues(expQueues, qMgr.getQueues());
    // pass true so it will fail if the key is not found.
    assertFalse(conf.getBoolean(JobConf.MR_ACLS_ENABLED, true));
  }
  
  public void testMultipleQueues() {
    JobConf conf = new JobConf();
    conf.set("mapred.queue.names", "q1,q2,Q3");
    QueueManager qMgr = new QueueManager(conf);
    Set<String> expQueues = new TreeSet<String>();
    expQueues.add("q1");
    expQueues.add("q2");
    expQueues.add("Q3");
    verifyQueues(expQueues, qMgr.getQueues());
  }

  public void testSchedulerInfo() {
    JobConf conf = new JobConf();
    conf.set("mapred.queue.names", "qq1,qq2");
    QueueManager qMgr = new QueueManager(conf);
    qMgr.setSchedulerInfo("qq1", "queueInfoForqq1");
    qMgr.setSchedulerInfo("qq2", "queueInfoForqq2");
    assertEquals(qMgr.getSchedulerInfo("qq2"), "queueInfoForqq2");
    assertEquals(qMgr.getSchedulerInfo("qq1"), "queueInfoForqq1");
  }
  
  public void testAllEnabledACLForJobSubmission() 
  throws IOException, InterruptedException {
    try {
      JobConf conf = setupConf(QueueManager.toFullPropertyName(
                                                               "default", submitAcl), "*");
      UserGroupInformation ugi = createNecessaryUsers();
      String[] groups = ugi.getGroupNames();
      verifyJobSubmissionToDefaultQueue(conf, true,
                                        ugi.getShortUserName() + "," + groups[groups.length-1]);
    } finally {
      tearDownCluster();
    }
  }
  
  public void testAllDisabledACLForJobSubmission() 
  throws IOException, InterruptedException {
    try {
      createNecessaryUsers();
      JobConf conf = setupConf(QueueManager.toFullPropertyName(
                                                               "default", submitAcl), " ");
      String userName = "user1";
      String groupName = "group1";
      verifyJobSubmissionToDefaultQueue(conf, false, userName + "," + groupName);
    
      // Check if admins can submit job
      String user2 = "user2";
      String group2 = "group2";
      conf.set(JobConf.MR_ADMINS, user2 + " " + groupName);
      tearDownCluster();
      verifyJobSubmissionToDefaultQueue(conf, true, userName + "," + groupName);
      verifyJobSubmissionToDefaultQueue(conf, true, user2 + "," + group2);
    
      // Check if MROwner(user who started the mapreduce cluster) can submit job
      UserGroupInformation mrOwner = UserGroupInformation.getCurrentUser();
      userName = mrOwner.getShortUserName();
      String[] groups = mrOwner.getGroupNames();
      groupName = groups[groups.length - 1];
      verifyJobSubmissionToDefaultQueue(conf, true, userName + "," + groupName);
    } finally {
      tearDownCluster();
    }
  }
  
  public void testUserDisabledACLForJobSubmission() 
  throws IOException, InterruptedException {
    try {
      JobConf conf = setupConf(QueueManager.toFullPropertyName(
                                                               "default", submitAcl), "3698-non-existent-user");
      verifyJobSubmissionToDefaultQueue(conf, false, "user1,group1");
    } finally {
      tearDownCluster();
    }
  }

  public void testSubmissionToInvalidQueue() 
  throws IOException, InterruptedException{
    try {
    JobConf conf = new JobConf();
    conf.set("mapred.queue.names","default");
    setUpCluster(conf);
    String queueName = "q1";
    try {
      submitSleepJob(1, 1, 100, 100, true, null, queueName);
    } catch (IOException ioe) {      
       assertTrue(ioe.getMessage().contains("Queue \"" + queueName + "\" does not exist"));
       return;
    } finally {
      tearDownCluster();
    }
    fail("Job submission to invalid queue job shouldnot complete , it should fail with proper exception ");  
    } finally {
      tearDownCluster();
    } 
  }

  public void testUserEnabledACLForJobSubmission() 
  throws IOException, LoginException, InterruptedException {
    try {
      String userName = "user1";
      JobConf conf
        = setupConf(QueueManager.toFullPropertyName
                    ("default", submitAcl), "3698-junk-user," + userName 
                    + " 3698-junk-group1,3698-junk-group2");
      verifyJobSubmissionToDefaultQueue(conf, true, userName+",group1");
    } finally {
      tearDownCluster();
    }
  }

  /**
   * Test to verify refreshing of queue properties by using MRAdmin tool.
   *
   * @throws Exception
   */
  public void testStateRefresh() throws Exception {
    String queueConfigPath =
        System.getProperty("test.build.extraconf", "build/test/extraconf");
    File queueConfigFile =
        new File(queueConfigPath, QueueManager.QUEUE_ACLS_FILE_NAME);
    try {
      //Setting up default mapred-site.xml
      Properties queueConfProps = new Properties();
      //these properties should be retained.
      queueConfProps.put("mapred.queue.names", "default,qu1");
      queueConfProps.put("mapred.acls.enabled", "true");
      //These property should always be overridden
      queueConfProps.put("mapred.queue.default.state", "RUNNING");
      queueConfProps.put("mapred.queue.qu1.state", "STOPPED");
      UtilsForTests.setUpConfigFile(queueConfProps, queueConfigFile);

      //Create a new configuration to be used with QueueManager
      JobConf conf = new JobConf();
      setUpCluster(conf);
      QueueManager queueManager =
        miniMRCluster.getJobTrackerRunner().getJobTracker().getQueueManager();

      RunningJob job = submitSleepJob(1, 1, 100, 100, true,null, "default" );
      assertTrue(job.isSuccessful());

      try {
        submitSleepJob(1, 1, 100, 100, true,null, "qu1" );
        fail("submit job in default queue should be failed ");
      } catch (Exception e) {
        assertTrue(e.getMessage().contains(
              "Queue \"" + "qu1" + "\" is not running"));
      }

      // verify state of queues before refresh
      JobQueueInfo queueInfo = queueManager.getJobQueueInfo("default");
      assertEquals(Queue.QueueState.RUNNING.getStateName(),
                    queueInfo.getQueueState());
      queueInfo = queueManager.getJobQueueInfo("qu1");
      assertEquals(Queue.QueueState.STOPPED.getStateName(),
                    queueInfo.getQueueState());

      queueConfProps.put("mapred.queue.default.state", "STOPPED");
      queueConfProps.put("mapred.queue.qu1.state", "RUNNING");
      UtilsForTests.setUpConfigFile(queueConfProps, queueConfigFile);

      //refresh configuration
      queueManager.refreshQueues(conf);

      //Job Submission should pass now because ugi to be used is set to blank.
      try {
        submitSleepJob(1, 1, 100, 100, true,null,"qu1");
      } catch (Exception e) {
        fail("submit job in qu1 queue should be sucessful ");
      }

      try {
        submitSleepJob(1, 1, 100, 100, true,null, "default" );
        fail("submit job in default queue should be failed ");
      } catch (Exception e){
        assertTrue(e.getMessage().contains(
              "Queue \"" + "default" + "\" is not running"));
      }

      // verify state of queues after refresh
      queueInfo = queueManager.getJobQueueInfo("default");
      assertEquals(Queue.QueueState.STOPPED.getStateName(),
                    queueInfo.getQueueState());
      queueInfo = queueManager.getJobQueueInfo("qu1");
      assertEquals(Queue.QueueState.RUNNING.getStateName(),
                    queueInfo.getQueueState());
    } finally{
      if(queueConfigFile.exists()) {
        queueConfigFile.delete();
      }
      this.tearDownCluster();
    }
  }

  JobConf setupConf(String aclName, String aclValue) {
    JobConf conf = new JobConf();
    conf.setBoolean(JobConf.MR_ACLS_ENABLED, true);
    conf.set(aclName, aclValue);
    return conf;
  }
  
  void verifyQueues(Set<String> expectedQueues, 
                                          Set<String> actualQueues) {
    assertEquals(expectedQueues.size(), actualQueues.size());
    for (String queue : expectedQueues) {
      assertTrue(actualQueues.contains(queue));
    }
  }
  
  /**
   *  Verify job submission as given user to the default queue
   */
  void verifyJobSubmissionToDefaultQueue(JobConf conf, boolean shouldSucceed,
		  String userInfo) throws IOException, InterruptedException {
    verifyJobSubmission(conf, shouldSucceed, userInfo, "default");
  }

  /**
   * Verify job submission as given user to the given queue
   */
  void verifyJobSubmission(JobConf conf, boolean shouldSucceed, 
      String userInfo, String queue) throws IOException, InterruptedException {
    setUpCluster(conf);
    try {
      runAndVerifySubmission(conf, shouldSucceed, queue, userInfo);
    } finally {
      // tearDownCluster();
    }
  }

  /**
   * Verify if submission of job to the given queue will succeed or not
   */
  void runAndVerifySubmission(JobConf conf, boolean shouldSucceed,
      String queue, String userInfo)
      throws IOException, InterruptedException {
    try {
      RunningJob rjob = submitSleepJob(1, 1, 100, 100, true, userInfo, queue);
      if (shouldSucceed) {
        assertTrue(rjob.isSuccessful());
      } else {
        fail("Job submission should have failed.");
      }
    } catch (IOException ioe) {
      if (shouldSucceed) {
        throw ioe;
      } else {
        LOG.info("exception while submitting job: " + ioe.getMessage());
        assertTrue(ioe.getMessage().
            contains("cannot perform operation " +
            "SUBMIT_JOB on queue " + queue));
        // check if the system directory gets cleaned up or not
        JobTracker jobtracker = miniMRCluster.getJobTrackerRunner().getJobTracker();
        Path sysDir = new Path(jobtracker.getSystemDir());
        FileSystem fs = sysDir.getFileSystem(conf);
        int size = fs.listStatus(sysDir).length;
        while (size > 1) { // ignore the jobtracker.info file
          System.out.println("Waiting for the job files in sys directory to be cleaned up");
          UtilsForTests.waitFor(100);
          size = fs.listStatus(sysDir).length;
        }
      }
    } finally {
      // tearDownCluster();
    }
  }

  /**
   * Submit job as current user and kill the job as user of ugi.
   * @param ugi {@link UserGroupInformation} of user who tries to kill the job
   * @param conf JobConf for the job
   * @param shouldSucceed Should the killing of job be succeeded ?
   * @throws IOException
   * @throws InterruptedException
   */
  void verifyJobKill(UserGroupInformation ugi, JobConf conf,
		  boolean shouldSucceed) throws IOException, InterruptedException {
    setUpCluster(conf);
    try {
      RunningJob rjob = submitSleepJob(1, 1, 1000, 1000, false);
      assertFalse(rjob.isComplete());
      while(rjob.mapProgress() == 0.0f) {
        try {
          Thread.sleep(10);  
        } catch (InterruptedException ie) {
          break;
        }
      }
      conf.set("mapred.job.tracker", "localhost:"
              + miniMRCluster.getJobTrackerPort());
      final String jobId = rjob.getJobID();
      ugi.doAs(new PrivilegedExceptionAction<Object>() {

        @Override
        public Object run() throws Exception {
          RunningJob runningJob =
        	  new JobClient(miniMRCluster.createJobConf()).getJob(jobId);
          runningJob.killJob();
          return null;
        }
      });

      while(rjob.cleanupProgress() == 0.0f) {
        try {
          Thread.sleep(10);  
        } catch (InterruptedException ie) {
          break;
        }
      }
      if (shouldSucceed) {
        assertTrue(rjob.isComplete());
      } else {
        fail("Job kill should have failed.");
      }
    } catch (IOException ioe) {
      if (shouldSucceed) {
        throw ioe;
      } else {
        LOG.info("exception while submitting/killing job: " + ioe.getMessage());
        assertTrue(ioe.getMessage().
            contains(" cannot perform operation KILL_JOB on "));
      }
    } finally {
      // tearDownCluster();
    }
  }

  
  void verifyJobKillAsOtherUser(JobConf conf, boolean shouldSucceed,
                                        String otherUserInfo) 
                        throws IOException, InterruptedException {
    setUpCluster(conf);
    try {
      // submit a job as another user.
      String userInfo = otherUserInfo;
      RunningJob rjob = submitSleepJob(1, 1, 1000, 1000, false, userInfo);
      assertFalse(rjob.isComplete());

      //try to kill as self
      try {
        conf.set("mapred.job.tracker", "localhost:"
            + miniMRCluster.getJobTrackerPort());
        JobClient client = new JobClient(miniMRCluster.createJobConf());
        client.getJob(rjob.getID()).killJob();
        if (!shouldSucceed) {
          fail("should fail kill operation");  
        }
      } catch (IOException ioe) {
        if (shouldSucceed) {
          throw ioe;
        }
        //verify it fails
        LOG.info("exception while killing job: " + ioe.getMessage());
        assertTrue(ioe.getMessage().
                        contains("cannot perform operation " +
                                    "KILL_JOB on queue default"));
      }
      //wait for job to complete on its own
      while (!rjob.isComplete()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          break;
        }
      }
    } finally {
      // tearDownCluster();
    }
  }
  
  /**
   * Submit job as current user and try to change priority of that job as
   * another user.
   * @param otherUGI user who will try to change priority of job
   * @param conf jobConf for the job
   * @param shouldSucceed Should the changing of priority of job be succeeded ?
   * @throws IOException
   * @throws InterruptedException
   */
  void verifyJobPriorityChangeAsOtherUser(UserGroupInformation otherUGI,
      JobConf conf, final boolean shouldSucceed)
      throws IOException, InterruptedException {
    setUpCluster(conf);
    try {
      // submit job as current user.
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      String[] groups = ugi.getGroupNames();
      String userInfo = ugi.getShortUserName() + "," +
                        groups[groups.length - 1];
      final RunningJob rjob = submitSleepJob(1, 1, 1000, 1000, false, userInfo);
      assertFalse(rjob.isComplete());
      
      conf.set("mapred.job.tracker", "localhost:"
	            + miniMRCluster.getJobTrackerPort());
      // try to change priority as other user
      otherUGI.doAs(new PrivilegedExceptionAction<Object>() {

        @Override
        public Object run() throws Exception {
      	  try {
            JobClient client = new JobClient(miniMRCluster.createJobConf());
            client.getJob(rjob.getID()).setJobPriority("VERY_LOW");
             if (!shouldSucceed) {
              fail("changing priority should fail.");
             }
          } catch (IOException ioe) {
            //verify it fails
            LOG.info("exception while changing priority of job: " +
                     ioe.getMessage());
            assertTrue(ioe.getMessage().
                contains(" cannot perform operation SET_JOB_PRIORITY on "));
          }
          return null;
        }
      });
      //wait for job to complete on its own
      while (!rjob.isComplete()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          break;
        }
      }
    } finally {
      // tearDownCluster();
    }
  }
  
  void setUpCluster(JobConf conf) throws IOException {
    if(miniMRCluster == null) {
      miniDFSCluster = new MiniDFSCluster(conf, 1, true, null);
      FileSystem fileSys = miniDFSCluster.getFileSystem();
      TestMiniMRWithDFSWithDistinctUsers.mkdir(fileSys, "/user");
      TestMiniMRWithDFSWithDistinctUsers.mkdir
        (fileSys, conf.get("mapreduce.jobtracker.staging.root.dir",
                           "/tmp/hadoop/mapred/staging"));
      String namenode = fileSys.getUri().toString();
      miniMRCluster = new MiniMRCluster(1, namenode, 3, 
                                        null, null, conf);
    }
  }
  
  void tearDownCluster() throws IOException {
    if (miniMRCluster != null) {
      long mrTeardownStart = new java.util.Date().getTime();
      if (miniMRCluster != null) { miniMRCluster.shutdown(); }
      long mrTeardownEnd = new java.util.Date().getTime();
      if (miniDFSCluster != null) { miniDFSCluster.shutdown(); }
      long dfsTeardownEnd = new java.util.Date().getTime();
      miniMRCluster = null;
      miniDFSCluster = null;
      System.err.println("An MR teardown took "
                         + (mrTeardownEnd - mrTeardownStart)
                         + " milliseconds.  A DFS teardown took "
                         + ( dfsTeardownEnd - mrTeardownEnd )
                         + " milliseconds.");
    }
  }

  RunningJob submitSleepJob(int numMappers, int numReducers, 
                            long mapSleepTime, long reduceSleepTime,
                            boolean shouldComplete) 
                              throws IOException, InterruptedException {
    return submitSleepJob(numMappers, numReducers, mapSleepTime,
                          reduceSleepTime, shouldComplete, null);
  }
  
  RunningJob submitSleepJob(int numMappers, int numReducers, 
                                      long mapSleepTime, long reduceSleepTime,
                                      boolean shouldComplete, String userInfo) 
                                     throws IOException, InterruptedException {
    return submitSleepJob(numMappers, numReducers, mapSleepTime, 
                          reduceSleepTime, shouldComplete, userInfo, null);
  }

  RunningJob submitSleepJob(final int numMappers, final int numReducers, 
      final long mapSleepTime,
      final long reduceSleepTime, final boolean shouldComplete, String userInfo,
                                    String queueName) 
                                      throws IOException, InterruptedException {
    JobConf clientConf = new JobConf();
    clientConf.set("mapred.job.tracker", "localhost:"
        + miniMRCluster.getJobTrackerPort());
    UserGroupInformation ugi;
    SleepJob job = new SleepJob();
    job.setConf(clientConf);
    clientConf = job.setupJobConf(numMappers, numReducers, 
        mapSleepTime, (int)mapSleepTime/100,
        reduceSleepTime, (int)reduceSleepTime/100);
    if (queueName != null) {
      clientConf.setQueueName(queueName);
    }
    final JobConf jc = new JobConf(clientConf);
    if (userInfo != null) {
      String[] splits = userInfo.split(",");
      String[] groups = new String[splits.length - 1];
      System.arraycopy(splits, 1, groups, 0, splits.length - 1);
      ugi = UserGroupInformation.createUserForTesting(splits[0], groups);
    } else {
      ugi = UserGroupInformation.getCurrentUser();
    }
    RunningJob rJob = ugi.doAs(new PrivilegedExceptionAction<RunningJob>() {
      public RunningJob run() throws IOException {
        if (shouldComplete) {
          return JobClient.runJob(jc);  
        } else {
          // Job should be submitted as 'userInfo'. So both the client as well as
          // the configuration should point to the same UGI.
          return new JobClient(jc).submitJob(jc);
        }
      }
    });
    return rJob;
  }

}
