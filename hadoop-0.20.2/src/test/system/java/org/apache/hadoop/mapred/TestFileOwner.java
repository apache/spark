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
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.test.system.FinishTaskControlAction;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.mapreduce.test.system.TTClient;
import org.apache.hadoop.mapreduce.test.system.TTInfo;
import org.apache.hadoop.mapreduce.test.system.TTTaskInfo;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFileOwner {
  public static MRCluster cluster;

  private StringBuffer jobIdDir = new StringBuffer();
  private JTProtocol wovenClient = null;
  private static final Log LOG = LogFactory.getLog(TestFileOwner.class);
  private String taskController = null;
  private final FsPermission PERM_777 = new FsPermission("777");
  private final FsPermission PERM_755 = new FsPermission("755");
  private final FsPermission PERM_644 = new FsPermission("644");

  @BeforeClass
  public static void setUp() throws java.lang.Exception {
    cluster = MRCluster.createCluster(new Configuration());
    cluster.setUp();
  }

  /*
   * The test is used to check the file permission of local files
   * in mapred.local.dir. The job control is used which will make the
   * tasks wait for completion until it is signaled
   *
   * @throws Exception in case of test errors
   */
  @Test
  public void testFilePermission() throws Exception {
    wovenClient = cluster.getJTClient().getProxy();
    Configuration conf = new Configuration(cluster.getConf());
    FinishTaskControlAction.configureControlActionForJob(conf);
    SleepJob job = new SleepJob();
    job.setConf(conf);
    conf = job.setupJobConf(1, 0, 100, 100, 100, 100);
    JobConf jconf = new JobConf(conf);
    RunningJob rJob = cluster.getJTClient().getClient().submitJob(jconf);
    taskController = conf.get("mapred.task.tracker.task-controller");
    // get the job info so we can get the env variables from the daemon.
    // Now wait for the task to be in the running state, only then the
    // directories will be created
    JobInfo info = wovenClient.getJobInfo(rJob.getID());
    Assert.assertNotNull("JobInfo is null",info);
    JobID id = rJob.getID();
    while (info.runningMaps() != 1) {
      Thread.sleep(1000);
      info = wovenClient.getJobInfo(id);
    }
    TaskInfo[] myTaskInfos = wovenClient.getTaskInfo(id);
    for (TaskInfo tInfo : myTaskInfos) {
      if (!tInfo.isSetupOrCleanup()) {
        String[] taskTrackers = tInfo.getTaskTrackers();
        for (String taskTracker : taskTrackers) {
          TTInfo ttInfo = wovenClient.getTTInfo(taskTracker);
          TTClient ttCli = cluster.getTTClient(ttInfo.getStatus().getHost());
          Assert.assertNotNull("TTClient instance is null",ttCli);
          TTTaskInfo ttTaskInfo = ttCli.getProxy().getTask(tInfo.getTaskID());
          Assert.assertNotNull("TTTaskInfo is null",ttTaskInfo);
          while (ttTaskInfo.getTaskStatus().getRunState() !=
                 TaskStatus.State.RUNNING) {
            Thread.sleep(100);
            ttTaskInfo = ttCli.getProxy().getTask(tInfo.getTaskID());
          }
          testPermissionWithTaskController(ttCli, conf, info);
          FinishTaskControlAction action = new FinishTaskControlAction(TaskID
              .downgrade(tInfo.getTaskID()));
          for (TTClient cli : cluster.getTTClients()) {
            cli.getProxy().sendAction(action);
          }
        }
      }
    }
    JobInfo jInfo = wovenClient.getJobInfo(id);
    jInfo = cluster.getJTClient().getProxy().getJobInfo(id);
    while (!jInfo.getStatus().isJobComplete()) {
      Thread.sleep(100);
      jInfo = cluster.getJTClient().getProxy().getJobInfo(id);
    }
  }

  private void testPermissionWithTaskController(TTClient tClient,
      Configuration conf,
      JobInfo info) {
    Assert.assertNotNull("TTclient is null",tClient);
    FsPermission fsPerm = null;
    String[] pathInfo = conf.getStrings("mapred.local.dir");
    for (int i = 0; i < pathInfo.length; i++) {
      // First verify the jobid directory exists
      jobIdDir = new StringBuffer();
      String userName = null;
      try {
        JobStatus[] jobStatus = cluster.getJTClient().getClient().getAllJobs();
        userName = jobStatus[0].getUsername();
      } catch(Exception ex) {
        LOG.error("Failed to get user name");
        boolean status = false;
        Assert.assertTrue("Failed to get the userName", status);
      }
      jobIdDir.append(pathInfo[i]).append(Path.SEPARATOR);
      jobIdDir.append(TaskTracker.getLocalJobDir(userName,
                                   info.getID().toString()));
      FileStatus[] fs = null;
      try {
        fs = tClient.listStatus(jobIdDir.toString(), true);
      } catch (Exception ex) {
        LOG.error("Failed to get the jobIdDir files " + ex);
      }
      Assert.assertEquals("Filestatus length is zero",fs.length != 0, true);
      for (FileStatus file : fs) {
        try {
          String filename = file.getPath().getName();
          if (filename.equals(TaskTracker.JOBFILE)) {
            if (taskController == DefaultTaskController.class.getName()) {
              fsPerm = file.getPermission();
              Assert.assertTrue("FilePermission failed for "+filename,
                  fsPerm.equals(PERM_777));
            }
          }
          if (filename.startsWith("attempt")) {
            StringBuffer attemptDir = new StringBuffer(jobIdDir);
            attemptDir.append(Path.SEPARATOR).append(filename);
            if (tClient.getFileStatus(attemptDir.toString(), true) != null) {
              FileStatus[] attemptFs = tClient.listStatus(
                  attemptDir.toString(), true, true);
              for (FileStatus attemptfz : attemptFs) {
                Assert.assertNotNull("FileStatus is null",attemptfz);
                fsPerm = attemptfz.getPermission();
                Assert.assertNotNull("FsPermission is null",fsPerm);
                if (taskController == DefaultTaskController.class.getName()) {
                  if (!attemptfz.isDir()) {
                    Assert.assertTrue("FilePermission failed for "+filename,
                        fsPerm.equals(PERM_777));
                  } else {
                    Assert.assertTrue("FilePermission failed for "+filename,
                        fsPerm.equals(PERM_755));
                  }
                }
              }
            }
          }
          if (filename.equals(TaskTracker.TASKJARDIR)) {
            StringBuffer jarsDir = new StringBuffer(jobIdDir);
            jarsDir.append(Path.SEPARATOR).append(filename);
            FileStatus[] jarsFs = tClient.listStatus(jarsDir.toString(), true,
                true);
            for (FileStatus jarsfz : jarsFs) {
              Assert.assertNotNull("FileStatus is null",jarsfz);
              fsPerm = jarsfz.getPermission();
              Assert.assertNotNull("File permission is null",fsPerm);
              if (taskController == DefaultTaskController.class.getName()) {
                if (!jarsfz.isDir()) {
                  if (jarsfz.getPath().getName().equals("job.jar")) {
                    Assert.assertTrue("FilePermission failed for "+filename,
                        fsPerm.equals(PERM_777));
                  } else {
                    Assert.assertTrue("FilePermission failed for "+filename,
                        fsPerm.equals(PERM_644));
                  }
                } else {
                  Assert.assertTrue("FilePermission failed for "+filename,
                      fsPerm.equals(PERM_755));
                }
              }
            }
          }
        } catch (Exception ex) {
          LOG.error("The exception occurred while searching for nonexsistent"
              + "file, ignoring and continuing. " + ex);
        }
      }// for loop ends
    }// for loop ends
  }

  @AfterClass
  public static void tearDown() throws java.lang.Exception {
    cluster.tearDown();
  }
}
