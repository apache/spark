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
import java.net.URI;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

import junit.framework.TestCase;

public class TestSubmitJob extends TestCase {
  static final Log LOG = LogFactory.getLog(TestSubmitJob.class);
  private MiniMRCluster mrCluster;

  private MiniDFSCluster dfsCluster;
  private JobTracker jt;
  private FileSystem fs;
  private static Path TEST_DIR =
    new Path(System.getProperty("test.build.data","/tmp"),
             "job-submission-testing");
  private static int numSlaves = 1;

  private void startCluster() throws Exception {
    super.setUp();
    Configuration conf = new Configuration();
    dfsCluster = new MiniDFSCluster(conf, numSlaves, true, null);
    JobConf jConf = new JobConf(conf);
    jConf.setLong("mapred.job.submission.expiry.interval", 6 * 1000);
    mrCluster = new MiniMRCluster(0, 0, numSlaves,
        dfsCluster.getFileSystem().getUri().toString(), 1, null, null, null,
        jConf);
    jt = mrCluster.getJobTrackerRunner().getJobTracker();
    fs = FileSystem.get(mrCluster.createJobConf());
  }

  private void stopCluster() throws Exception {
    mrCluster.shutdown();
    mrCluster = null;
    dfsCluster.shutdown();
    dfsCluster = null;
    jt = null;
    fs = null;
  }
  
  /**
   * Test to verify that jobs with invalid memory requirements are killed at the
   * JT.
   * 
   * @throws Exception
   */
  public void testJobWithInvalidMemoryReqs()
      throws Exception {
    JobConf jtConf = new JobConf();
    jtConf
        .setLong(JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 1 * 1024L);
    jtConf.setLong(JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY,
        2 * 1024L);
    jtConf.setLong(JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY,
        3 * 1024L);
    jtConf.setLong(JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY,
        4 * 1024L);

    mrCluster = new MiniMRCluster(0, "file:///", 0, null, null, jtConf);

    JobConf clusterConf = mrCluster.createJobConf();

    // No map-memory configuration
    JobConf jobConf = new JobConf(clusterConf);
    jobConf.setMemoryForReduceTask(1 * 1024L);
    runJobAndVerifyFailure(jobConf, JobConf.DISABLED_MEMORY_LIMIT, 1 * 1024L,
        "Invalid job requirements.");

    // No reduce-memory configuration
    jobConf = new JobConf(clusterConf);
    jobConf.setMemoryForMapTask(1 * 1024L);
    runJobAndVerifyFailure(jobConf, 1 * 1024L, JobConf.DISABLED_MEMORY_LIMIT,
        "Invalid job requirements.");

    // Invalid map-memory configuration
    jobConf = new JobConf(clusterConf);
    jobConf.setMemoryForMapTask(4 * 1024L);
    jobConf.setMemoryForReduceTask(1 * 1024L);
    runJobAndVerifyFailure(jobConf, 4 * 1024L, 1 * 1024L,
        "Exceeds the cluster's max-memory-limit.");

    // No reduce-memory configuration
    jobConf = new JobConf(clusterConf);
    jobConf.setMemoryForMapTask(1 * 1024L);
    jobConf.setMemoryForReduceTask(5 * 1024L);
    runJobAndVerifyFailure(jobConf, 1 * 1024L, 5 * 1024L,
        "Exceeds the cluster's max-memory-limit.");
    mrCluster.shutdown();
    mrCluster = null;
  }
  
  /** check for large jobconfs **/
  public void testJobWithInvalidDiskReqs()
      throws Exception {
    JobConf jtConf = new JobConf();
    jtConf
        .setLong(JobTracker.MAX_USER_JOBCONF_SIZE_KEY, 1 * 1024L);
 
    mrCluster = new MiniMRCluster(0, "file:///", 0, null, null, jtConf);

    JobConf clusterConf = mrCluster.createJobConf();

    // No map-memory configuration
    JobConf jobConf = new JobConf(clusterConf);
    String[] args = { "-m", "0", "-r", "0", "-mt", "0", "-rt", "0" };
    String msg = null;
    try {
      ToolRunner.run(jobConf, new SleepJob(), args);
      assertTrue(false);
    } catch (RemoteException re) {
      System.out.println("Exception " + StringUtils.stringifyException(re));
    }

    mrCluster.shutdown();
    mrCluster = null;
  }
  
  private void runJobAndVerifyFailure(JobConf jobConf, long memForMapTasks,
      long memForReduceTasks, String expectedMsg)
      throws Exception,
      IOException {
    String[] args = { "-m", "0", "-r", "0", "-mt", "0", "-rt", "0" };
    boolean throwsException = false;
    String msg = null;
    try {
      ToolRunner.run(jobConf, new SleepJob(), args);
    } catch (RemoteException re) {
      throwsException = true;
      msg = re.unwrapRemoteException().getMessage();
    }
    assertTrue(throwsException);
    assertNotNull(msg);

    String overallExpectedMsg =
        "(" + memForMapTasks + " memForMapTasks " + memForReduceTasks
            + " memForReduceTasks): " + expectedMsg;
    assertTrue("Observed message - " + msg
        + " - doesn't contain expected message - " + overallExpectedMsg, msg
        .contains(overallExpectedMsg));
  }
  static JobSubmissionProtocol getJobSubmitClient(JobConf conf,
                                            UserGroupInformation ugi)
   throws IOException {
    return (JobSubmissionProtocol) RPC.getProxy(JobSubmissionProtocol.class,
        JobSubmissionProtocol.versionID, JobTracker.getAddress(conf), ugi,
         conf, NetUtils.getSocketFactory(conf, JobSubmissionProtocol.class));
   }
 
  static org.apache.hadoop.hdfs.protocol.ClientProtocol getDFSClient(
        Configuration conf, UserGroupInformation ugi)
    throws IOException {
     return (org.apache.hadoop.hdfs.protocol.ClientProtocol)
        RPC.getProxy(org.apache.hadoop.hdfs.protocol.ClientProtocol.class,
           org.apache.hadoop.hdfs.protocol.ClientProtocol.versionID,
           NameNode.getAddress(conf), ugi,
           conf,
           NetUtils.getSocketFactory(conf,
               org.apache.hadoop.hdfs.protocol.ClientProtocol.class));
  }
   /**
    * Submit a job and check if the files are accessible to other users.
    */
  public void testSecureJobExecution() throws Exception {
    LOG.info("Testing secure job submission/execution");
    MiniMRCluster mr = null;
    Configuration conf = new Configuration();
    final MiniDFSCluster dfs = new MiniDFSCluster(conf, 1, true, null);
    try {
      FileSystem fs =
        TestMiniMRWithDFSWithDistinctUsers.
        DFS_UGI.doAs(new PrivilegedExceptionAction<FileSystem>() {
          public FileSystem run() throws IOException {
            return dfs.getFileSystem();
         }
        });
      TestMiniMRWithDFSWithDistinctUsers.mkdir(fs, "/user");
      TestMiniMRWithDFSWithDistinctUsers.mkdir(fs, "/mapred");
      TestMiniMRWithDFSWithDistinctUsers.mkdir(fs,
          conf.get("mapreduce.jobtracker.staging.root.dir",
              "/tmp/hadoop/mapred/staging"));
      UserGroupInformation MR_UGI = UserGroupInformation.getLoginUser();
      mr = new MiniMRCluster(0, 0, 1, dfs.getFileSystem().getUri().toString(),
          1, null, null, MR_UGI);
      JobTracker jt = mr.getJobTrackerRunner().getJobTracker();
      String jobTrackerName = "localhost:" + mr.getJobTrackerPort();
      // cleanup
      dfs.getFileSystem().delete(TEST_DIR, true);

      final Path mapSignalFile = new Path(TEST_DIR, "map-signal");
      final Path reduceSignalFile = new Path(TEST_DIR, "reduce-signal");

      // create a ugi for user 1
      UserGroupInformation user1 =
        TestMiniMRWithDFSWithDistinctUsers.createUGI("user1", false);
      Path inDir = new Path("/user/input");
      Path outDir = new Path("/user/output");
      final JobConf job = mr.createJobConf();

      UtilsForTests.configureWaitingJobConf(job, inDir, outDir, 2, 0,
          "test-submit-job", mapSignalFile.toString(),
          reduceSignalFile.toString());
      job.set(UtilsForTests.getTaskSignalParameter(true),
          mapSignalFile.toString());
      job.set(UtilsForTests.getTaskSignalParameter(false),
          reduceSignalFile.toString());
      LOG.info("Submit job as the actual user (" + user1.getUserName() + ")");
      final JobClient jClient =
        user1.doAs(new PrivilegedExceptionAction<JobClient>() {
          public JobClient run() throws IOException {
            return new JobClient(job);
          }
        });
      RunningJob rJob = user1.doAs(new PrivilegedExceptionAction<RunningJob>() {
        public RunningJob run() throws IOException {
          return jClient.submitJob(job);
        }
      });
      JobID id = rJob.getID();
      LOG.info("Running job " + id);

      // create user2
      UserGroupInformation user2 =
        TestMiniMRWithDFSWithDistinctUsers.createUGI("user2", false);
      JobConf conf_other = mr.createJobConf();
      org.apache.hadoop.hdfs.protocol.ClientProtocol client =
        getDFSClient(conf_other, user2);

      // try accessing mapred.system.dir/jobid/*
      try {
        String path = new URI(jt.getSystemDir()).getPath();
        LOG.info("Try listing the mapred-system-dir as the user ("
            + user2.getUserName() + ")");
        client.getListing(path, HdfsFileStatus.EMPTY_NAME);
        fail("JobTracker system dir is accessible to others");
      } catch (IOException ioe) {
        assertTrue(ioe.toString(),
          ioe.toString().contains("Permission denied"));
      }
      // try accessing ~/.staging/jobid/*
      JobInProgress jip = jt.getJob(id);
      Path jobSubmitDirpath =
        new Path(jip.getJobConf().get("mapreduce.job.dir"));
      try {
        LOG.info("Try accessing the job folder for job " + id + " as the user ("
            + user2.getUserName() + ")");
        client.getListing(jobSubmitDirpath.toUri().getPath(), HdfsFileStatus.EMPTY_NAME);
        fail("User's staging folder is accessible to others");
      } catch (IOException ioe) {
        assertTrue(ioe.toString(),
          ioe.toString().contains("Permission denied"));
      }
      UtilsForTests.signalTasks(dfs, fs, true, mapSignalFile.toString(),
          reduceSignalFile.toString());
      // wait for job to be done
      UtilsForTests.waitTillDone(jClient);

      // check if the staging area is cleaned up
      LOG.info("Check if job submit dir is cleanup or not");
      assertFalse(fs.exists(jobSubmitDirpath));
    } finally {
      if (mr != null) {
        mr.shutdown();
      }
      if (dfs != null) {
        dfs.shutdown();
      }
    }
  }
}
