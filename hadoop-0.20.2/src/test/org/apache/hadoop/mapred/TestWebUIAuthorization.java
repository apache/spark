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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.net.HttpURLConnection;
import java.net.URLEncoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.http.TestHttpServer.DummyFilterInitializer;
import org.apache.hadoop.mapred.JobHistory.Keys;
import org.apache.hadoop.mapred.JobHistory.TaskAttempt;
import org.apache.hadoop.mapred.QueueManager.QueueACL;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TestWebUIAuthorization extends ClusterMapReduceTestCase {

  private static final Log LOG = LogFactory.getLog(
      TestWebUIAuthorization.class);

  // users who submit the jobs
  private static final String jobSubmitter = "user1";
  private static final String jobSubmitter1 = "user11";
  private static final String jobSubmitter2 = "user12";
  private static final String jobSubmitter3 = "user13";

  // mrOwner starts the cluster
  private static String mrOwner = null;
  // admin of "default" queue
  private static final String qAdmin = "user3";
  // mrAdmin
  private static final String mrAdminUser = "user4";
  // Group for mrAdmins
  private static final String mrAdminGroup = "admingroup";
  // member of mrAdminGroup
  private static final String mrAdminGroupMember = "user2";
  // "colleague1" is there in job-view-acls config
  private static final String viewColleague = "colleague1";
  // "colleague2" is there in job-modify-acls config
  private static final String modifyColleague = "colleague2";
  // "colleague3" is there in both job-view-acls and job-modify-acls
  private static final String viewAndModifyColleague = "colleague3";
  // "evilJohn" is not having view/modify access on the jobs
  private static final String unauthorizedUser = "evilJohn";

  protected void setUp() throws Exception {
    // do not do anything
  };

  /** access a url, ignoring some IOException such as the page does not exist */
  static int getHttpStatusCode(String urlstring, String userName,
      String method) throws IOException {
    LOG.info("Accessing " + urlstring + " as user " + userName);
    URL url = new URL(urlstring + "&user.name=" + userName);
    HttpURLConnection connection = (HttpURLConnection)url.openConnection();
    connection.setRequestMethod(method);
    if (method.equals("POST")) {
      String encodedData = "action=kill&user.name=" + userName;      
      connection.setRequestProperty("Content-Type",
                                    "application/x-www-form-urlencoded");
      connection.setRequestProperty("Content-Length",
                                    Integer.toString(encodedData.length()));
      connection.setDoOutput(true);

      OutputStream os = connection.getOutputStream();
      os.write(encodedData.getBytes());
    }
    connection.connect();

    return connection.getResponseCode();
  }

  public static class MyGroupsProvider extends ShellBasedUnixGroupsMapping {
    static Map<String, List<String>> mapping = new HashMap<String, List<String>>();

    @Override
    public List<String> getGroups(String user) throws IOException {
      return mapping.get(user);
    }
  }

  /**
   * Validates the given jsp/servlet against different user names who
   * can(or cannot) view the job.
   * (1) jobSubmitter can view the job
   * (2) mrAdmin can view any job
   * (3) mrOwner can view any job
   * (4) qAdmins of the queue to which job is submitted to can view any job in
   *     that queue.
   * (5) user mentioned in job-view-acl should be able to view the
   *     job irrespective of job-modify-acl.
   * (6) user mentioned in job-modify-acl but not in job-view-acl
   *     cannot view the job
   * (7) other unauthorized users cannot view the job
   */
  private void validateViewJob(String url, String method)
      throws IOException {
    assertEquals("Incorrect return code for job submitter " + jobSubmitter,
        HttpURLConnection.HTTP_OK, getHttpStatusCode(url, jobSubmitter,
            method));
    assertEquals("Incorrect return code for admin user " +
        mrAdminUser, HttpURLConnection.HTTP_OK,
        getHttpStatusCode(url, mrAdminUser, method));
    assertEquals("Incorrect return code for admingroup-member " +
        mrAdminGroupMember, HttpURLConnection.HTTP_OK,
        getHttpStatusCode(url, mrAdminGroupMember, method));
    assertEquals("Incorrect return code for MR-owner " + mrOwner,
        HttpURLConnection.HTTP_OK, getHttpStatusCode(url, mrOwner, method));
    assertEquals("Incorrect return code for queue admin " + qAdmin,
        HttpURLConnection.HTTP_OK, getHttpStatusCode(url, qAdmin, method));
    assertEquals("Incorrect return code for user in job-view-acl " +
        viewColleague, HttpURLConnection.HTTP_OK,
        getHttpStatusCode(url, viewColleague, method));
    assertEquals("Incorrect return code for user in job-view-acl and " +
        "job-modify-acl " + viewAndModifyColleague, HttpURLConnection.HTTP_OK,
        getHttpStatusCode(url, viewAndModifyColleague, method));
    assertEquals("Incorrect return code for user in job-modify-acl " +
        modifyColleague, HttpURLConnection.HTTP_UNAUTHORIZED,
        getHttpStatusCode(url, modifyColleague, method));
    assertEquals("Incorrect return code for unauthorizedUser " +
        unauthorizedUser, HttpURLConnection.HTTP_UNAUTHORIZED,
        getHttpStatusCode(url, unauthorizedUser, method));
  }

  /**
   * Validates the given jsp/servlet against different user names who
   * can(or cannot) modify the job.
   * (1) jobSubmitter, mrOwner, qAdmin and mrAdmin can modify the job.
   *     But we are not validating this in this method. Let the caller
   *     explicitly validate this, if needed.
   * (2) user mentioned in job-view-acl but not in job-modify-acl cannot
   *     modify the job
   * (3) user mentioned in job-modify-acl (irrespective of job-view-acl)
   *     can modify the job
   * (4) other unauthorized users cannot modify the job
   */
  private void validateModifyJob(String url, String method) throws IOException {
    assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED,
        getHttpStatusCode(url, viewColleague, method));
    assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED,
        getHttpStatusCode(url, unauthorizedUser, method));
    assertEquals(HttpURLConnection.HTTP_OK,
        getHttpStatusCode(url, modifyColleague, method));
  }

  // starts a sleep job with 1 map task that runs for a long time
  private RunningJob startSleepJobAsUser(String user, final JobConf conf,
      MiniMRCluster cluster) throws Exception {
	final SleepJob sleepJob = new SleepJob();
    sleepJob.setConf(conf);
    UserGroupInformation jobSubmitterUGI = 
        UserGroupInformation.createRemoteUser(user);
    RunningJob job =
        jobSubmitterUGI.doAs(new PrivilegedExceptionAction<RunningJob>() {
        public RunningJob run() throws Exception {
          JobClient jobClient = new JobClient(conf);
          SleepJob sleepJob = new SleepJob();
          sleepJob.setConf(conf);
          JobConf jobConf =
              sleepJob.setupJobConf(1, 0, 900000, 1, 1000, 1000);
          RunningJob runningJob = jobClient.submitJob(jobConf);
          return runningJob;
        }
      });
    JobID jobId = job.getID();
    getTIPId(cluster, jobId);//wait till the map task starts running

    return job;
  }

  // Waits till the map task gets started and gets its tipId from map reports
  // and returns the tipId
  private TaskID getTIPId(MiniMRCluster cluster,
      org.apache.hadoop.mapreduce.JobID jobid) throws Exception {
    JobClient client = new JobClient(cluster.createJobConf());
    JobID jobId = (JobID) jobid;
    TaskReport[] mapReports = null;

    TaskID tipId = null;
    do { // make sure that the map task is running
      Thread.sleep(200);
      mapReports = client.getMapTaskReports(jobId);
    } while (mapReports.length == 0);

    for (TaskReport r : mapReports) {
      tipId = r.getTaskID();
      break;// because we have only one map
    }

    JobTracker jt = cluster.getJobTrackerRunner().getJobTracker();
    // wait till we see an attempt actually running
    while (jt.getTaskStatuses(tipId).length == 0) {
      try {
        Thread.sleep(100);
      } catch(InterruptedException e) {
        LOG.warn("Interrupted while waiting for map attempt to be started");
      }
    }

    return tipId;
  }

  /**
   * Make sure that the given user can do killJob using jobdetails.jsp url
   * @param cluster
   * @param conf
   * @param jtURL
   * @param jobTrackerJSP
   * @param user
   * @throws Exception
   */
  private void confirmJobDetailsJSPKillJobAsUser(MiniMRCluster cluster,
      JobConf conf, String jtURL, String jobTrackerJSP, String user)
      throws Exception {
    RunningJob job = startSleepJobAsUser(jobSubmitter, conf, cluster);
    org.apache.hadoop.mapreduce.JobID jobid = job.getID();
    // jobDetailsJSP killJob url
    String url = jtURL + "/jobdetails.jsp?" +
        "action=kill&jobid="+ jobid.toString();
    try {
      assertEquals(HttpURLConnection.HTTP_OK,
          getHttpStatusCode(url, user, "POST"));
      waitForKillJobToFinish(job);
      assertTrue("killJob failed for a job for which user has "
          + "job-modify permission", job.isComplete());
    } finally {
      if (!job.isComplete()) {
        LOG.info("Killing job " + jobid + " from finally block");
        assertEquals(HttpURLConnection.HTTP_OK,
            getHttpStatusCode(jobTrackerJSP + "&killJobs=true&jobCheckBox=" +
            jobid.toString(), jobSubmitter, "GET"));
      }
    }
  }

  static void setupGroupsProvider() throws IOException {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        MyGroupsProvider.class.getName());
    Groups.getUserToGroupsMappingService(conf);
    MyGroupsProvider.mapping.put(jobSubmitter, Arrays.asList("group1"));
    MyGroupsProvider.mapping.put(viewColleague, Arrays.asList("group2"));
    MyGroupsProvider.mapping.put(modifyColleague, Arrays.asList("group1"));
    MyGroupsProvider.mapping.put(unauthorizedUser, Arrays.asList("evilSociety"));
    MyGroupsProvider.mapping.put(mrAdminGroupMember, Arrays.asList(mrAdminGroup));
    MyGroupsProvider.mapping.put(viewAndModifyColleague, Arrays.asList("group3"));
    MyGroupsProvider.mapping.put(qAdmin, Arrays.asList("group4"));

    mrOwner = UserGroupInformation.getCurrentUser().getShortUserName();
    MyGroupsProvider.mapping.put(mrOwner, Arrays.asList(
        new String[] { "group5", "group6" }));
    
    MyGroupsProvider.mapping.put(jobSubmitter1, Arrays.asList("group7"));
    MyGroupsProvider.mapping.put(jobSubmitter2, Arrays.asList("group7"));
    MyGroupsProvider.mapping.put(jobSubmitter3, Arrays.asList("group7"));

    MyGroupsProvider.mapping.put(mrAdminUser, Arrays.asList("group8"));
  }

  public void testAuthorizationForJobHistoryPages() throws Exception {
    setupGroupsProvider();
    Properties props = new Properties();
    props.setProperty("hadoop.http.filter.initializers",
        DummyFilterInitializer.class.getName());

    props.setProperty(JobConf.MR_ACLS_ENABLED, String.valueOf(true));
    props.setProperty(QueueManager.toFullPropertyName(
        "default", QueueACL.ADMINISTER_JOBS.getAclName()), qAdmin);
    props.setProperty(QueueManager.toFullPropertyName(
        "default", QueueACL.SUBMIT_JOB.getAclName()), jobSubmitter);

    props.setProperty("dfs.permissions", "false");

    // Let us have history files on HDFS
    props.setProperty("mapred.job.tracker.history.completed.location",
        "historyDoneFolderOnHDFS");

    props.setProperty(JobConf.MR_ADMINS, mrAdminUser + " " + mrAdminGroup);


    startCluster(true, props);
    MiniMRCluster cluster = getMRCluster();
    int infoPort = cluster.getJobTrackerRunner().getJobTrackerInfoPort();

    JobConf conf = new JobConf(cluster.createJobConf());
    conf.set(JobContext.JOB_ACL_VIEW_JOB, viewColleague + " group3");

    // Let us add group1 and group3 to modify-job-acl. So modifyColleague and
    // viewAndModifyColleague will be able to modify the job
    conf.set(JobContext.JOB_ACL_MODIFY_JOB, " group1,group3");

    final SleepJob sleepJob = new SleepJob();
    final JobConf jobConf = new JobConf(conf);
    sleepJob.setConf(jobConf);
    UserGroupInformation jobSubmitterUGI =
        UserGroupInformation.createRemoteUser(jobSubmitter);
    RunningJob job =
        jobSubmitterUGI.doAs(new PrivilegedExceptionAction<RunningJob>() {
          public RunningJob run() throws Exception {
            JobClient jobClient = new JobClient(jobConf);
            SleepJob sleepJob = new SleepJob();
            sleepJob.setConf(jobConf);
            JobConf jobConf =
                sleepJob.setupJobConf(1, 0, 1000, 1, 1000, 1000);
            RunningJob runningJob = jobClient.runJob(jobConf);
            return runningJob;
          }
        });

    JobID jobid = job.getID();

    JobTracker jobTracker = getMRCluster().getJobTrackerRunner().getJobTracker();
    JobInProgress jip = jobTracker.getJob(jobid);
    JobConf finalJobConf = jip.getJobConf();
    Path doneDir = JobHistory.getCompletedJobHistoryLocation();

    // Wait for history file to be written
    while(TestJobHistory.getDoneFile(finalJobConf, jobid, doneDir) == null) {
      Thread.sleep(2000);
    }

    Path historyFilePath = new Path(doneDir,
        JobHistory.JobInfo.getDoneJobHistoryFileName(finalJobConf, jobid));

    String urlEncodedHistoryFileName = URLEncoder.encode(historyFilePath.toString());
    String jtURL = "http://localhost:" + infoPort;

    // validate access of jobdetails_history.jsp
    String jobDetailsJSP =
        jtURL + "/jobdetailshistory.jsp?logFile=" + urlEncodedHistoryFileName;
    validateViewJob(jobDetailsJSP, "GET");

    // validate accesses of jobtaskshistory.jsp
    String jobTasksJSP =
        jtURL + "/jobtaskshistory.jsp?logFile=" + urlEncodedHistoryFileName;
    String[] taskTypes =
        new String[] { "JOb_SETUP", "MAP", "REDUCE", "JOB_CLEANUP" };
    String[] states =
        new String[] { "all", "SUCCEEDED", "FAILED", "KILLED" };
    for (String taskType : taskTypes) {
      for (String state : states) {
        validateViewJob(jobTasksJSP + "&taskType=" + taskType + "&status="
            + state, "GET");
      }
    }

    JobHistory.JobInfo jobInfo = new JobHistory.JobInfo(jobid.toString());

    DefaultJobHistoryParser.JobTasksParseListener l =
                   new DefaultJobHistoryParser.JobTasksParseListener(jobInfo);
    JobHistory.parseHistoryFromFS(historyFilePath.toString().substring(5),
        l, historyFilePath.getFileSystem(conf));

    Map<String, org.apache.hadoop.mapred.JobHistory.Task> tipsMap =
    	jobInfo.getAllTasks();

    for (String tip : tipsMap.keySet()) {
      // validate access of taskdetailshistory.jsp
      validateViewJob(jtURL + "/taskdetailshistory.jsp?logFile="
          + urlEncodedHistoryFileName + "&tipid=" + tip.toString(), "GET");

      Map<String, TaskAttempt> attemptsMap =
          tipsMap.get(tip).getTaskAttempts();
      for (String attempt : attemptsMap.keySet()) {

        // validate access to taskstatshistory.jsp
        validateViewJob(jtURL + "/taskstatshistory.jsp?attemptid="
            + attempt.toString() + "&logFile=" + urlEncodedHistoryFileName, "GET");

        // validate access to tasklogs
        String taskLogURL = TaskLogServlet.getTaskLogUrl("localhost",
            attemptsMap.get(attempt).get(Keys.HTTP_PORT), attempt.toString());
        validateViewJob(taskLogURL, "GET");
      }
    }

    // For each tip, let us test the effect of deletion of job-acls.xml file and
    // deletion of task log dir for each of the attempts of the tip.
    
    // delete job-acls.xml file from the job userlog dir and verify
    // if unauthorized users can view task logs of each attempt.
    Path jobACLsFilePath = new Path(TaskLog.getJobDir(jobid).toString(),
        TaskTracker.jobACLsFile);
    new File(jobACLsFilePath.toUri().getPath()).delete();

    for (String tip : tipsMap.keySet()) {

      Map<String, TaskAttempt> attemptsMap =
        tipsMap.get(tip).getTaskAttempts();
      for (String attempt : attemptsMap.keySet()) {

        String taskLogURL = TaskLogServlet.getTaskLogUrl("localhost",
            attemptsMap.get(attempt).get(Keys.HTTP_PORT), attempt.toString());

        // unauthorized users can view task logs of each attempt because
        // job-acls.xml file is deleted.
        assertEquals("Incorrect return code for " + unauthorizedUser,
            HttpURLConnection.HTTP_OK, getHttpStatusCode(taskLogURL,
                unauthorizedUser, "GET"));

        // delete the whole task log dir of attempt and verify that we get
        // correct response code (i.e. HTTP_GONE) when task logs are accessed.
        File attemptLogDir = TaskLog.getAttemptDir(TaskAttemptID
            .forName(attempt), false);
        FileUtil.fullyDelete(attemptLogDir);
        assertEquals("Incorrect return code for " + jobSubmitter,
            HttpURLConnection.HTTP_GONE, getHttpStatusCode(taskLogURL,
                jobSubmitter, "GET"));
      }
    }

    // validate access to analysejobhistory.jsp
    String analyseJobHistoryJSP =
        jtURL + "/analysejobhistory.jsp?logFile=" + urlEncodedHistoryFileName;
    validateViewJob(analyseJobHistoryJSP, "GET");

    // validate access of jobconf_history.jsp
    String jobConfJSP =
        jtURL + "/jobconf_history.jsp?logFile=" + urlEncodedHistoryFileName;
    validateViewJob(jobConfJSP, "GET");
  }

  /**
   * Starts a sleep job and tries to kill the job using jobdetails.jsp as
   * (1) viewColleague (2) unauthorizedUser (3) modifyColleague
   * (4) viewAndModifyColleague (5) mrOwner (6) mrAdmin and
   * (7) jobSubmitter
   *
   * Validates the given jsp/servlet against different user names who
   * can(or cannot) do both view and modify on the job.
   * (1) jobSubmitter, mrOwner and mrAdmin can do both view and modify
   *     on the job. But we are not validating this in this method. Let the
   *     caller explicitly validate this, if needed.
   * (2) user mentioned in job-view-acls and job-modify-acls can do this
   * (3) user mentioned in job-view-acls but not in job-modify-acls cannot
   *     do this
   * (4) user mentioned in job-modify-acls but not in job-view-acls cannot
   *     do this
   * (5) qAdmin cannot do this because he doesn't have view access to the job
   * (6) other unauthorized users cannot do this
   *
   * @throws Exception
   */
  private void validateJobDetailsJSPKillJob(MiniMRCluster cluster,
      JobConf clusterConf, String jtURL) throws Exception {

    JobConf conf = new JobConf(cluster.createJobConf());
    conf.set(JobContext.JOB_ACL_VIEW_JOB, viewColleague + " group3");

    // Let us add group1 and group3 to modify-job-acl. So modifyColleague and
    // viewAndModifyColleague will be able to modify the job
    conf.set(JobContext.JOB_ACL_MODIFY_JOB, " group1,group3");

    String jobTrackerJSP =  jtURL + "/jobtracker.jsp?a=b";
    RunningJob job = startSleepJobAsUser(jobSubmitter, conf, cluster);
    org.apache.hadoop.mapreduce.JobID jobid = job.getID();
    // jobDetailsJSPKillJobAction url
    String url = jtURL + "/jobdetails.jsp?" +
        "action=kill&jobid="+ jobid.toString();
    try {
      assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED,
          getHttpStatusCode(url, viewColleague, "POST"));
      assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED,
          getHttpStatusCode(url, unauthorizedUser, "POST"));
      assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED,
          getHttpStatusCode(url, modifyColleague, "POST"));

      assertEquals(HttpURLConnection.HTTP_OK,
          getHttpStatusCode(url, viewAndModifyColleague, "POST"));
      waitForKillJobToFinish(job);
      assertTrue("killJob using jobdetails.jsp failed for a job for which "
          + "user has job-view and job-modify permissions", job.isComplete());
    } finally {
      if (!job.isComplete()) {
        LOG.info("Killing job " + jobid + " from finally block");
        assertEquals(HttpURLConnection.HTTP_OK,
            getHttpStatusCode(jobTrackerJSP + "&killJobs=true&jobCheckBox=" +
            jobid.toString(), jobSubmitter, "GET"));
      }
    }

    // check if jobSubmitter, mrOwner and mrAdmin can do
    // killJob using jobdetails.jsp url
    confirmJobDetailsJSPKillJobAsUser(cluster, conf, jtURL, jobTrackerJSP,
                                      jobSubmitter);
    confirmJobDetailsJSPKillJobAsUser(cluster, conf, jtURL, jobTrackerJSP,
                                      mrOwner);
    confirmJobDetailsJSPKillJobAsUser(cluster, conf, jtURL, jobTrackerJSP,
                                      mrAdminUser);
    confirmJobDetailsJSPKillJobAsUser(cluster, conf, jtURL, jobTrackerJSP,
                                      mrAdminGroupMember);
    confirmJobDetailsJSPKillJobAsUser(cluster, conf, jtURL, jobTrackerJSP,
                                      qAdmin);
  }

  /**
   * Make sure that the given user can do killJob using jobtracker.jsp url
   * @param cluster
   * @param conf
   * @param jtURL
   * @param user
   * @throws Exception
   */
  private void confirmJobTrackerJSPKillJobAsUser(MiniMRCluster cluster,
      JobConf conf, String jtURL, String user)
      throws Exception {
    String jobTrackerJSP =  jtURL + "/jobtracker.jsp?a=b";
    RunningJob job = startSleepJobAsUser(jobSubmitter, conf, cluster);
    org.apache.hadoop.mapreduce.JobID jobid = job.getID();
    // jobTrackerJSP killJob url
    String url = jobTrackerJSP +
        "&killJobs=true&jobCheckBox=" + jobid.toString();
    try {
      assertEquals(HttpURLConnection.HTTP_OK,
          getHttpStatusCode(url, user, "POST"));
      waitForKillJobToFinish(job);
      assertTrue("killJob failed for a job for which user has "
          + "job-modify permission", job.isComplete());
    } finally {
      if (!job.isComplete()) {
        LOG.info("Killing job " + jobid + " from finally block");
        assertEquals(HttpURLConnection.HTTP_OK,
            getHttpStatusCode(jobTrackerJSP + "&killJobs=true&jobCheckBox=" +
            jobid.toString(), jobSubmitter, "GET"));
      }
    }
  }

  /**
   * Waits for the job to become completed as we know that killJob is called
   * @param job
   * @throws IOException
   */
  void waitForKillJobToFinish(RunningJob job) throws IOException {
    while (!job.isComplete()) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e){
        LOG.warn("Interrupted while waiting for killJob() to finish for "
                 + job.getID());
      }
    }
  }

  /**
   * Make sure that multiple jobs get killed using jobtracker.jsp url when
   * user has modify access on only some of those jobs.
   * @param cluster
   * @param conf
   * @param jtURL
   * @param user
   * @throws Exception
   */
  private void validateKillMultipleJobs(MiniMRCluster cluster,
      JobConf conf, String jtURL) throws Exception {
    String jobTrackerJSP =  jtURL + "/jobtracker.jsp?a=b";
    // jobTrackerJSP killJob url
    String url = jobTrackerJSP + "&killJobs=true";
    // view-job-acl doesn't matter for killJob from jobtracker jsp page
    conf.set(JobContext.JOB_ACL_VIEW_JOB, " ");
    
    // Let us start 4 jobs as 4 different users(none of these 4 users is
    // mrOwner and none of these users is a member of mrAdmin and none of
    // these 4 users is a queue admin for the default queue). So only
    // based on the config JobContext.JOB_ACL_MODIFY_JOB being set here and the
    // job-submitter, killJob on each of the jobs will be succeeded.

    // start 1st job.
    // Out of these 4 users, only jobSubmitter can do killJob on 1st job
    conf.set(JobContext.JOB_ACL_MODIFY_JOB, " ");
    RunningJob job1 = startSleepJobAsUser(jobSubmitter, conf, cluster);
    org.apache.hadoop.mapreduce.JobID jobid = job1.getID();
    url = url.concat("&jobCheckBox=" + jobid.toString());

    // start 2nd job.
    // Out of these 4 users, only jobSubmitter1 can do killJob on 2nd job
    RunningJob job2 = startSleepJobAsUser(jobSubmitter1, conf, cluster);
    jobid = job2.getID();
    url = url.concat("&jobCheckBox=" + jobid.toString());

    // start 3rd job.
    // Out of these 4 users, only jobSubmitter2 can do killJob on 3rd job
    RunningJob job3 = startSleepJobAsUser(jobSubmitter2, conf, cluster);
    jobid = job3.getID();
    url = url.concat("&jobCheckBox=" + jobid.toString());

    // start 4rd job.
    // Out of these 4 users, jobSubmitter1 and jobSubmitter3
    // can do killJob on 4th job
    conf.set(JobContext.JOB_ACL_MODIFY_JOB, jobSubmitter1);
    RunningJob job4 = startSleepJobAsUser(jobSubmitter3,
                                          conf, cluster);
    jobid = job4.getID();
    url = url.concat("&jobCheckBox=" + jobid.toString());

    try {
      // Try killing all the 4 jobs as user viewColleague who can kill only
      // 2nd and 4th jobs. Check if 1st and 3rd jobs are not killed and
      // 2nd and 4th jobs get killed
      assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED,
          getHttpStatusCode(url, jobSubmitter1, "POST"));
      
      waitForKillJobToFinish(job2);
      assertTrue("killJob failed for a job for which user has "
          + "job-modify permission", job2.isComplete());
      waitForKillJobToFinish(job4);
      assertTrue("killJob failed for a job for which user has "
          + "job-modify permission", job4.isComplete());
      assertFalse("killJob succeeded for a job for which user doesnot "
          + " have job-modify permission", job1.isComplete());
      assertFalse("killJob succeeded for a job for which user doesnot "
          + " have job-modify permission", job3.isComplete());
    } finally {
      // Kill all 4 jobs as user mrOwner(even though some of them
      // were already killed)
      assertEquals(HttpURLConnection.HTTP_OK,
          getHttpStatusCode(url, mrOwner, "GET"));
    }
  }

  /**
   * Run a job and validate if JSPs/Servlets are going through authentication
   * and authorization.
   * @throws Exception 
   */
  @Test
  public void testWebUIAuthorization() throws Exception {
    setupGroupsProvider();
    Properties props = new Properties();
    props.setProperty("hadoop.http.filter.initializers",
        DummyFilterInitializer.class.getName());

    props.setProperty(JobConf.MR_ACLS_ENABLED, String.valueOf(true));
    props.setProperty(QueueManager.toFullPropertyName(
        "default", QueueACL.ADMINISTER_JOBS.getAclName()), qAdmin);
    props.setProperty(QueueManager.toFullPropertyName(
        "default", QueueACL.SUBMIT_JOB.getAclName()),
        jobSubmitter + "," + jobSubmitter1 + "," + jobSubmitter2 + "," +
        jobSubmitter3);

    props.setProperty("dfs.permissions", "false");
    // let us have enough map slots so that there won't be waiting for slots
    props.setProperty("mapred.tasktracker.map.tasks.maximum", "6");

    props.setProperty(JSPUtil.PRIVATE_ACTIONS_KEY, "true");
    props.setProperty(JobConf.MR_ADMINS, mrAdminUser + " " + mrAdminGroup);
    startCluster(true, props);
    MiniMRCluster cluster = getMRCluster();
    int infoPort = cluster.getJobTrackerRunner().getJobTrackerInfoPort();

    JobConf clusterConf = cluster.createJobConf();
    JobConf conf = new JobConf(clusterConf);
    conf.set(JobContext.JOB_ACL_VIEW_JOB, viewColleague + " group3");

    // Let us add group1 and group3 to modify-job-acl. So modifyColleague and
    // viewAndModifyColleague will be able to modify the job
    conf.set(JobContext.JOB_ACL_MODIFY_JOB, " group1,group3");

    RunningJob job = startSleepJobAsUser(jobSubmitter, conf, cluster);
    org.apache.hadoop.mapreduce.JobID jobid = job.getID();

    String jtURL = "http://localhost:" + infoPort;

    String jobTrackerJSP =  jtURL + "/jobtracker.jsp?a=b";
    try {
      // Currently, authorization is not done for jobtracker page. So allow
      // everyone to view it.
      validateJobTrackerJSPAccess(jtURL);
      validateJobDetailsJSPAccess(jobid, jtURL);
      validateTaskGraphServletAccess(jobid, jtURL);
      validateJobTasksJSPAccess(jobid, jtURL);
      validateJobConfJSPAccess(jobid, jtURL);
      validateJobFailuresJSPAccess(jobid, jtURL);
      valiateJobBlacklistedTrackerJSPAccess(jobid, jtURL);
      validateJobTrackerJSPSetPriorityAction(jobid, jtURL);

      // Wait for the tip to start so as to test task related JSP
      TaskID tipId = getTIPId(cluster, jobid);
      validateTaskStatsJSPAccess(jobid, jtURL, tipId);
      validateTaskDetailsJSPAccess(jobid, jtURL, tipId);
      validateJobTrackerJSPKillJobAction(job, jtURL);
    } finally {
      if (!job.isComplete()) { // kill the job(as jobSubmitter) if needed
        LOG.info("Killing job " + jobid + " from finally block");
        assertEquals(HttpURLConnection.HTTP_OK,
            getHttpStatusCode(jobTrackerJSP + "&killJobs=true&jobCheckBox=" +
            jobid.toString(), jobSubmitter, "GET"));
      }
    }

    // validate killJob of jobdetails.jsp
    validateJobDetailsJSPKillJob(cluster, clusterConf, jtURL);

    // validate killJob of jobtracker.jsp as users viewAndModifyColleague,
    // jobSubmitter, mrOwner and mrAdmin
    confirmJobTrackerJSPKillJobAsUser(cluster, conf, jtURL,
        viewAndModifyColleague);
    confirmJobTrackerJSPKillJobAsUser(cluster, conf, jtURL, jobSubmitter);
    confirmJobTrackerJSPKillJobAsUser(cluster, conf, jtURL, mrOwner);
    confirmJobTrackerJSPKillJobAsUser(cluster, conf, jtURL, mrAdminUser);
    confirmJobTrackerJSPKillJobAsUser(cluster, conf, jtURL, mrAdminGroupMember);
    confirmJobTrackerJSPKillJobAsUser(cluster, conf, jtURL, qAdmin);

    // validate killing of multiple jobs using jobtracker jsp and check
    // if all the jobs which can be killed by user are actually the ones that
    // got killed
    validateKillMultipleJobs(cluster, conf, jtURL);
  }

  public void testWebUIAuthorizationForCommonServlets() throws Exception {
    setupGroupsProvider();
    Properties props = new Properties();
    props.setProperty("hadoop.http.filter.initializers",
        DummyFilterInitializer.class.getName());
    props.setProperty(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, "true");
    props.setProperty(JobConf.MR_ADMINS, mrAdminUser + " " + mrAdminGroup);

    startCluster(true, props);
    validateCommonServlets(getMRCluster());
  }

  private void validateCommonServlets(MiniMRCluster cluster) throws IOException {
    int infoPort = cluster.getJobTrackerRunner().getJobTrackerInfoPort();
    String jtURL = "http://localhost:" + infoPort;
    for (String servlet : new String[] { "logs", "stacks", "logLevel" }) {
      String url = jtURL + "/" + servlet;
      checkAccessToCommonServlet(url);
    }
    // validate access to common servlets for TaskTracker.
    String ttURL = "http://localhost:"
        + cluster.getTaskTrackerRunner(0).getTaskTracker().getHttpPort();
    for (String servlet : new String[] { "logs", "stacks", "logLevel" }) {
      String url = ttURL + "/" + servlet;
      checkAccessToCommonServlet(url);
    }
  }

  private void checkAccessToCommonServlet(String url) throws IOException {
    url = url + "?a=b";
    assertEquals(HttpURLConnection.HTTP_OK, getHttpStatusCode(url, mrAdminUser,
        "GET"));
    assertEquals(HttpURLConnection.HTTP_OK, getHttpStatusCode(url,
        mrAdminGroupMember, "GET"));
    assertEquals(HttpURLConnection.HTTP_OK, getHttpStatusCode(url,
        mrOwner, "GET"));
    // no access for any other user
    assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, getHttpStatusCode(url,
        jobSubmitter, "GET"));
  }

  // validate killJob of jobtracker.jsp
  private void validateJobTrackerJSPKillJobAction(RunningJob job, String jtURL)
      throws IOException {
    org.apache.hadoop.mapreduce.JobID jobid = job.getID();
    String jobTrackerJSP =  jtURL + "/jobtracker.jsp?a=b";
    String jobTrackerJSPKillJobAction = jobTrackerJSP +
        "&killJobs=true&jobCheckBox="+ jobid.toString();
    validateModifyJob(jobTrackerJSPKillJobAction, "GET");
    waitForKillJobToFinish(job);
    assertTrue("killJob failed for a job for which user has "
        + "job-modify permission", job.isComplete());
  }

  // validate viewing of job of taskdetails.jsp
  private void validateTaskDetailsJSPAccess(
      org.apache.hadoop.mapreduce.JobID jobid, String jtURL, TaskID tipId)
      throws IOException {
    String taskDetailsJSP =  jtURL + "/taskdetails.jsp?jobid=" +
        jobid.toString() + "&tipid=" + tipId;
    validateViewJob(taskDetailsJSP, "GET");
  }

  // validate taskstats.jsp
  private void validateTaskStatsJSPAccess(
      org.apache.hadoop.mapreduce.JobID jobid, String jtURL, TaskID tipId)
      throws IOException {
    String taskStatsJSP =  jtURL + "/taskstats.jsp?jobid=" +
        jobid.toString() + "&tipid=" + tipId;
    validateViewJob(taskStatsJSP, "GET");
  }

  // validate setJobPriority
  private void validateJobTrackerJSPSetPriorityAction(
      org.apache.hadoop.mapreduce.JobID jobid, String jtURL)
      throws IOException {
    String jobTrackerJSP =  jtURL + "/jobtracker.jsp?a=b";
    String jobTrackerJSPSetJobPriorityAction = jobTrackerJSP +
        "&changeJobPriority=true&setJobPriority="+"HIGH"+"&jobCheckBox=" +
        jobid.toString();
    validateModifyJob(jobTrackerJSPSetJobPriorityAction, "GET");
    // jobSubmitter, mrOwner, qAdmin and mrAdmin are not validated for
    // job-modify permission in validateModifyJob(). So let us do it
    // explicitly here
    assertEquals(HttpURLConnection.HTTP_OK, getHttpStatusCode(
        jobTrackerJSPSetJobPriorityAction, jobSubmitter, "GET"));
    assertEquals(HttpURLConnection.HTTP_OK, getHttpStatusCode(
        jobTrackerJSPSetJobPriorityAction, mrAdminUser, "GET"));
    assertEquals(HttpURLConnection.HTTP_OK, getHttpStatusCode(
        jobTrackerJSPSetJobPriorityAction, mrAdminGroupMember, "GET"));
    assertEquals(HttpURLConnection.HTTP_OK, getHttpStatusCode(
        jobTrackerJSPSetJobPriorityAction, qAdmin, "GET"));
    assertEquals(HttpURLConnection.HTTP_OK, getHttpStatusCode(
        jobTrackerJSPSetJobPriorityAction, mrOwner, "GET"));
  }

  // validate access of jobblacklistedtrackers.jsp
  private void valiateJobBlacklistedTrackerJSPAccess(
      org.apache.hadoop.mapreduce.JobID jobid, String jtURL)
      throws IOException {
    String jobBlacklistedTrackersJSP =  jtURL +
        "/jobblacklistedtrackers.jsp?jobid="+jobid.toString();
    validateViewJob(jobBlacklistedTrackersJSP, "GET");
  }

  // validate access of jobfailures.jsp
  private void validateJobFailuresJSPAccess(
      org.apache.hadoop.mapreduce.JobID jobid, String jtURL)
      throws IOException {
    String jobFailuresJSP =  jtURL + "/jobfailures.jsp?jobid="+jobid.toString();
    validateViewJob(jobFailuresJSP, "GET");
  }

  // validate access of jobconf.jsp
  private void validateJobConfJSPAccess(
      org.apache.hadoop.mapreduce.JobID jobid, String jtURL)
      throws IOException {
    String jobConfJSP =  jtURL + "/jobconf.jsp?jobid="+jobid.toString();
    validateViewJob(jobConfJSP, "GET");
  }

  // validate access of jobtasks.jsp
  private void validateJobTasksJSPAccess(
      org.apache.hadoop.mapreduce.JobID jobid, String jtURL)
      throws IOException {
    String jobTasksJSP =  jtURL + "/jobtasks.jsp?jobid="
        + jobid.toString() + "&type=map&pagenum=1&state=running";
    validateViewJob(jobTasksJSP, "GET");
  }

  // validate access of TaskGraphServlet
  private void validateTaskGraphServletAccess(
      org.apache.hadoop.mapreduce.JobID jobid, String jtURL)
      throws IOException {
    String taskGraphServlet = jtURL + "/taskgraph?type=map&jobid="
        + jobid.toString();
    validateViewJob(taskGraphServlet, "GET");
    taskGraphServlet = jtURL + "/taskgraph?type=reduce&jobid="
        + jobid.toString();
    validateViewJob(taskGraphServlet, "GET");
  }

  // validate access of jobdetails.jsp
  private void validateJobDetailsJSPAccess(
      org.apache.hadoop.mapreduce.JobID jobid, String jtURL)
      throws IOException {
    String jobDetailsJSP =  jtURL + "/jobdetails.jsp?jobid="
        + jobid.toString();
    validateViewJob(jobDetailsJSP, "GET");
  }

  // validate access of jobtracker.jsp
  private void validateJobTrackerJSPAccess(String jtURL)
      throws IOException {
    String jobTrackerJSP =  jtURL + "/jobtracker.jsp?a=b";
    assertEquals(HttpURLConnection.HTTP_OK,
        getHttpStatusCode(jobTrackerJSP, jobSubmitter, "GET"));
    assertEquals(HttpURLConnection.HTTP_OK,
        getHttpStatusCode(jobTrackerJSP, viewColleague, "GET"));
    assertEquals(HttpURLConnection.HTTP_OK,
        getHttpStatusCode(jobTrackerJSP, unauthorizedUser, "GET"));
    assertEquals(HttpURLConnection.HTTP_OK,
        getHttpStatusCode(jobTrackerJSP, modifyColleague, "GET"));
    assertEquals(HttpURLConnection.HTTP_OK,
        getHttpStatusCode(jobTrackerJSP, viewAndModifyColleague, "GET"));
    assertEquals(HttpURLConnection.HTTP_OK,
        getHttpStatusCode(jobTrackerJSP, mrOwner, "GET"));
    assertEquals(HttpURLConnection.HTTP_OK,
        getHttpStatusCode(jobTrackerJSP, qAdmin, "GET"));
    assertEquals(HttpURLConnection.HTTP_OK,
        getHttpStatusCode(jobTrackerJSP, mrAdminUser, "GET"));
    assertEquals(HttpURLConnection.HTTP_OK,
        getHttpStatusCode(jobTrackerJSP, mrAdminGroupMember, "GET"));
  }
}
