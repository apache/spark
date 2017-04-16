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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.QueueManager.QueueACL;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

/**
 * Verify the job-ACLs
 * 
 */
public class TestJobACLs {

  static final Log LOG = LogFactory.getLog(TestJobACLs.class);

  private MiniMRCluster mr = null;

  private static final Path TEST_DIR =
      new Path(System.getProperty("test.build.data", "/tmp"),
          TestJobACLs.class.getCanonicalName() + Path.SEPARATOR
              + "completed-job-store");

  private String jobSubmitter = "jobSubmitter";
  private String viewColleague = "viewColleague";
  private String modifyColleague = "modifyColleague";
  private String qAdmin = "qAdmin";

  /**
   * Start the cluster before running the actual test.
   * 
   * @throws IOException
   */
  @Before
  public void setup() throws IOException {
    // Start the cluster
    startCluster(false);
  }

  private void startCluster(boolean reStart) throws IOException {
    UserGroupInformation MR_UGI = UserGroupInformation.getLoginUser();
    JobConf conf = new JobConf();

    // Enable queue and job level authorization
    conf.setBoolean(JobConf.MR_ACLS_ENABLED, true);
    // qAdmin is a queue administrator for default queue
    conf.set(QueueManager.toFullPropertyName(
        "default", QueueACL.ADMINISTER_JOBS.getAclName()), qAdmin);
    conf.set(QueueManager.toFullPropertyName(
        "default", QueueACL.SUBMIT_JOB.getAclName()), jobSubmitter);

    // Enable CompletedJobStore
    FileSystem fs = FileSystem.getLocal(conf);
    if (!reStart) {
      fs.delete(TEST_DIR, true);
    }
    conf.set("mapred.job.tracker.persist.jobstatus.dir",
        fs.makeQualified(TEST_DIR).toString());
    conf.setBoolean("mapred.job.tracker.persist.jobstatus.active", true);
    conf.set("mapred.job.tracker.persist.jobstatus.hours", "1");

    // Let us have enough slots sothat there won't be contention for slots
    // for launching JOB_CLEANUP tasks
    conf.set("mapred.tasktracker.map.tasks.maximum", "4");

    mr = new MiniMRCluster(0, 0, 2, "file:///", 1, null, null, MR_UGI, conf);
  }

  /**
   * Kill the cluster after the test is done.
   */
  @After
  public void tearDown() {
    if (mr != null) {
      mr.shutdown();
    }
  }

  /**
   * Test view-job-acl, modify-job-acl and acl persistence to the
   * completed-jobs-store.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  @Test
  public void testACLS() throws IOException, InterruptedException,
      ClassNotFoundException {
    verifyACLViewJob();
    verifyACLModifyJob(modifyColleague);
    verifyACLModifyJob(qAdmin);
    verifyACLPersistence();
  }

  /**
   * Verify JobContext.JOB_ACL_VIEW_JOB
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  private void verifyACLViewJob() throws IOException, InterruptedException {

    // Set the job up.
    final JobConf myConf = mr.createJobConf();
    myConf.set(JobContext.JOB_ACL_VIEW_JOB, viewColleague);

    // Submit the job as user1
    RunningJob job = submitJobAsUser(myConf, jobSubmitter);

    final JobID jobId = job.getID();

    // Try operations as an unauthorized user.
    verifyViewJobAsUnauthorizedUser(myConf, jobId, modifyColleague);

    // Try operations as an authorized user, who is part of view-job-acl.
    verifyViewJobAsAuthorizedUser(myConf, jobId, viewColleague);

    // Try operations as an authorized user, who is a queue administrator.
    verifyViewJobAsAuthorizedUser(myConf, jobId, qAdmin);

    // Clean up the job
    job.killJob();
  }

  /**
   * Submits a sleep job with 1 map task that runs for a long time(2000 sec)
   * @param clusterConf
   * @param user the jobOwner
   * @return RunningJob that is started
   * @throws IOException
   * @throws InterruptedException
   */
  private RunningJob submitJobAsUser(final JobConf clusterConf, String user)
      throws IOException, InterruptedException {
    UserGroupInformation ugi =
        UserGroupInformation.createUserForTesting(user, new String[] {});
    RunningJob job = (RunningJob) ugi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        JobClient jobClient = new JobClient(clusterConf);
        SleepJob sleepJob = new SleepJob();
        sleepJob.setConf(clusterConf);
        JobConf jobConf = sleepJob.setupJobConf(1, 0, 2000, 1000, 1000, 1000);
        RunningJob runningJob = jobClient.submitJob(jobConf);
        return runningJob;
      }
    });
    return job;
  }

  private void verifyViewJobAsAuthorizedUser(final JobConf myConf,
      final JobID jobId, String authorizedUser) throws IOException,
      InterruptedException {
    UserGroupInformation authorizedUGI =
        UserGroupInformation.createUserForTesting(authorizedUser,
            new String[] {});
    authorizedUGI.doAs(new PrivilegedExceptionAction<Object>() {
      @SuppressWarnings("null")
      @Override
      public Object run() throws Exception {
        RunningJob myJob = null;
        JobClient client = null;
        try {
          client = new JobClient(myConf);
          myJob = client.getJob(jobId);
        } catch (Exception e) {
          fail("Exception .." + e);
        }

        assertNotNull("Job " + jobId + " is not known to the JobTracker!",
            myJob);

        // Tests authorization with getCounters
        try {
          myJob.getCounters();
        } catch (IOException ioe) {
          fail("Unexpected.. exception.. " + ioe);
        }

        // Tests authorization  with getTaskReports
        try {
          client.getCleanupTaskReports(jobId);
        } catch (IOException ioe) {
          fail("Unexpected.. exception.. " + ioe);
        }

        return null;
      }
    });
  }

  private void verifyViewJobAsUnauthorizedUser(final JobConf myConf,
      final JobID jobId, String unauthorizedUser) throws IOException,
      InterruptedException {
    UserGroupInformation unauthorizedUGI =
        UserGroupInformation.createUserForTesting(unauthorizedUser,
            new String[] {});
    unauthorizedUGI.doAs(new PrivilegedExceptionAction<Object>() {
      @SuppressWarnings("null")
      @Override
      public Object run() {
        RunningJob myJob = null;
        JobClient client = null;
        try {
          client = new JobClient(myConf);
          myJob = client.getJob(jobId);
        } catch (Exception e) {
          fail("Exception .." + e);
        }

        assertNotNull("Job " + jobId + " is not known to the JobTracker!",
            myJob);

        // Tests authorization failure with getCounters
        try {
          myJob.getCounters();
          fail("AccessControlException expected..");
        } catch (IOException ioe) {
          assertTrue(ioe.getMessage().contains("AccessControlException"));
        }

        // Tests authorization failure with getTaskReports
        try {
          client.getSetupTaskReports(jobId);
          fail("AccessControlException expected..");
        } catch (IOException ioe) {
          assertTrue(ioe.getMessage().contains("AccessControlException"));
        }

        return null;
      }
    });
  }

  /**
   * Verify JobContext.Job_ACL_MODIFY_JOB
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  private void verifyACLModifyJob(String authorizedUser) throws IOException,
      InterruptedException, ClassNotFoundException {

    // Set the job up.
    final JobConf myConf = mr.createJobConf();
    myConf.set(JobContext.JOB_ACL_MODIFY_JOB, modifyColleague);

    // Submit the job as user1
    RunningJob job = submitJobAsUser(myConf, jobSubmitter);

    final JobID jobId = job.getID();

    // Try operations as an unauthorized user.
    verifyModifyJobAsUnauthorizedUser(myConf, jobId, viewColleague);

    // Try operations as an authorized user.
    verifyModifyJobAsAuthorizedUser(myConf, jobId, authorizedUser);
  }

  private void verifyModifyJobAsAuthorizedUser(
      final JobConf clusterConf, final JobID jobId,
      String authorizedUser) throws IOException, InterruptedException {
    UserGroupInformation authorizedUGI =
        UserGroupInformation.createUserForTesting(authorizedUser,
            new String[] {});
    authorizedUGI.doAs(new PrivilegedExceptionAction<Object>() {
      @SuppressWarnings("null")
      @Override
      public Object run() throws Exception {
        RunningJob myJob = null;
        try {
          JobClient client = new JobClient(clusterConf);
          myJob = client.getJob(jobId);
        } catch (Exception e) {
          fail("Exception .." + e);
        }

        assertNotNull("Job " + jobId + " is not known to the JobTracker!",
            myJob);

        // Test authorization success with setJobPriority
        try {
          myJob.setJobPriority(JobPriority.HIGH.toString());
        } catch (IOException ioe) {
          fail("Unexpected.. exception.. " + ioe);
        }

        // Test authorization success with killJob
        try {
          myJob.killJob();
        } catch (IOException ioe) {
          fail("Unexpected.. exception.. " + ioe);
        }

        return null;
      }
    });
  }

  private void verifyModifyJobAsUnauthorizedUser(
      final JobConf clusterConf, final JobID jobId,
      String unauthorizedUser) throws IOException, InterruptedException {
    UserGroupInformation unauthorizedUGI =
        UserGroupInformation.createUserForTesting(unauthorizedUser,
            new String[] {});
    unauthorizedUGI.doAs(new PrivilegedExceptionAction<Object>() {
      @SuppressWarnings("null")
      @Override
      public Object run() {
        RunningJob myJob = null;
        try {
          JobClient client = new JobClient(clusterConf);
          myJob = client.getJob(jobId);
        } catch (Exception e) {
          fail("Exception .." + e);
        }

        assertNotNull("Job " + jobId + " is not known to the JobTracker!",
            myJob);

        // Tests authorization failure with killJob
        try {
          myJob.killJob();
          fail("AccessControlException expected..");
        } catch (IOException ioe) {
          assertTrue(ioe.getMessage().contains("AccessControlException"));
        }


        // Tests authorization failure with setJobPriority
        try {
          myJob.setJobPriority(JobPriority.HIGH.toString());
          fail("AccessControlException expected..");
        } catch (IOException ioe) {
          assertTrue(ioe.getMessage().contains("AccessControlException"));
        }

        return null;
      }
    });
  }

  private void verifyACLPersistence() throws IOException,
      InterruptedException {

    // Set the job up.
    final JobConf myConf = mr.createJobConf();
    myConf.set(JobContext.JOB_ACL_VIEW_JOB, viewColleague + " group2");

    // Submit the job as user1
    RunningJob job = submitJobAsUser(myConf, jobSubmitter);

    final JobID jobId = job.getID();

    // Kill the job and wait till it is actually killed so that it is written to
    // CompletedJobStore
    job.killJob();
    while (job.getJobState() != JobStatus.KILLED) {
      LOG.info("Waiting for the job to be killed successfully..");
      Thread.sleep(200);
    }

    // Now kill the cluster, so that the job is 'forgotten'
    tearDown();

    // Re-start the cluster
    startCluster(true);

    final JobConf myNewJobConf = mr.createJobConf();
    // Now verify view-job works off CompletedJobStore
    verifyViewJobAsAuthorizedUser(myNewJobConf, jobId, viewColleague);
    verifyViewJobAsAuthorizedUser(myNewJobConf, jobId, qAdmin);

    // Only JobCounters is persisted on the JobStore. So test counters only.
    UserGroupInformation unauthorizedUGI =
        UserGroupInformation.createUserForTesting(
            modifyColleague, new String[] {});

    unauthorizedUGI.doAs(new PrivilegedExceptionAction<Object>() {
      @SuppressWarnings("null")
      @Override
      public Object run() {
        RunningJob myJob = null;
        try {
          JobClient client = new JobClient(myNewJobConf);
          myJob = client.getJob(jobId);
        } catch (Exception e) {
          fail("Exception .." + e);
        }

        assertNotNull("Job " + jobId + " is not known to the JobTracker!",
            myJob);

        // Tests authorization failure with getCounters
        try {
          myJob.getCounters();
          fail("AccessControlException expected..");
        } catch (IOException ioe) {
          assertTrue(ioe.getMessage().contains("AccessControlException"));
        }

        return null;
      }
    });

  }
}
