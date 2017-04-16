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

import java.security.PrivilegedExceptionAction;
import java.util.Collection;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.TaskStatus.State;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.test.system.FinishTaskControlAction;
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.mapreduce.test.system.TTClient;
import org.apache.hadoop.mapreduce.test.system.TTInfo;
import org.apache.hadoop.mapreduce.test.system.TTTaskInfo;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.system.AbstractDaemonClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCluster {

  private static final Log LOG = LogFactory.getLog(TestCluster.class);

  private static MRCluster cluster;

  public TestCluster() throws Exception {
    
  }

  @BeforeClass
  public static void before() throws Exception {
    String [] expExcludeList = new String[2];
    expExcludeList[0] = "java.net.ConnectException";
    expExcludeList[1] = "java.io.IOException";
    cluster = MRCluster.createCluster(new Configuration());
    cluster.setExcludeExpList(expExcludeList);
    cluster.setUp();
  }

  @AfterClass
  public static void after() throws Exception {
    cluster.tearDown();
  }

  @Test
  public void testProcessInfo() throws Exception {
    LOG.info("Process info of JobTracker is : "
        + cluster.getJTClient().getProcessInfo());
    Assert.assertNotNull(cluster.getJTClient().getProcessInfo());
    Collection<TTClient> tts = cluster.getTTClients();
    for (TTClient tt : tts) {
      LOG.info("Process info of TaskTracker is : " + tt.getProcessInfo());
      Assert.assertNotNull(tt.getProcessInfo());
    }
  }
  
  @Test
  public void testJobSubmission() throws Exception {
    Configuration conf = new Configuration(cluster.getConf());
    SleepJob job = new SleepJob();
    job.setConf(conf);
    conf = job.setupJobConf(1, 1, 100, 100, 100, 100);
    RunningJob rJob = cluster.getJTClient().submitAndVerifyJob(conf);
    cluster.getJTClient().verifyJobHistory(rJob.getID());
  }

  //@Test
  public void testFileStatus() throws Exception {
    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(cluster
            .getJTClient().getProxy().getDaemonUser());
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        MRCluster myCluster = null;
        try {
          myCluster = MRCluster.createCluster(cluster.getConf());
          myCluster.connect();
          JTClient jt = myCluster.getJTClient();
          String dir = ".";
          checkFileStatus(jt.getFileStatus(dir, true));
          checkFileStatus(jt.listStatus(dir, false, true), dir);
          for (TTClient tt : myCluster.getTTClients()) {
            String[] localDirs = tt.getMapredLocalDirs();
            for (String localDir : localDirs) {
              checkFileStatus(tt.listStatus(localDir, true, false), localDir);
              checkFileStatus(tt.listStatus(localDir, true, true), localDir);
            }
          }
          String systemDir = jt.getClient().getSystemDir().toString();
          checkFileStatus(jt.listStatus(systemDir, false, true), systemDir);
          checkFileStatus(jt.listStatus(jt.getLogDir(), true, true), jt
              .getLogDir());
        } finally {
          if (myCluster != null) {
            myCluster.disconnect();
          }
        }
        return null;
      }
    });
  }

  private void checkFileStatus(FileStatus[] fs, String path) {
    Assert.assertNotNull(fs);
    LOG.info("-----Listing for " + path + "  " + fs.length);
    for (FileStatus fz : fs) {
      checkFileStatus(fz);
    }
  }

  private void checkFileStatus(FileStatus fz) {
    Assert.assertNotNull(fz);
    LOG.info("FileStatus is " + fz.getPath() 
        + "  " + fz.getPermission()
        +"  " + fz.getOwner()
        +"  " + fz.getGroup()
        +"  " + fz.getClass());
  }

  /**
   * Test to verify the common properties of tasks.
   * @throws Exception
   */
  @Test
  public void testTaskDetails() throws Exception {
    Configuration conf = new Configuration(cluster.getConf());
    JTProtocol wovenClient = cluster.getJTClient().getProxy();
    FinishTaskControlAction.configureControlActionForJob(conf);
    SleepJob job = new SleepJob();
    job.setConf(conf);

    conf = job.setupJobConf(1, 1, 100, 100, 100, 100);
    JobClient client = cluster.getJTClient().getClient();

    RunningJob rJob = client.submitJob(new JobConf(conf));
    JobID id = rJob.getID();

    JobInfo jInfo = wovenClient.getJobInfo(id);

    while (jInfo.getStatus().getRunState() != JobStatus.RUNNING) {
      Thread.sleep(1000);
      jInfo = wovenClient.getJobInfo(id);
    }

    LOG.info("Waiting till job starts running one map");

    TaskInfo[] myTaskInfos = wovenClient.getTaskInfo(id);
    boolean isOneTaskStored = false;
    String sometaskpid = null;
    org.apache.hadoop.mapreduce.TaskAttemptID sometaskId = null;
    TTClient myCli = null;
    for(TaskInfo info : myTaskInfos) {
      if(!info.isSetupOrCleanup()) {
        String[] taskTrackers = info.getTaskTrackers();
        for(String taskTracker : taskTrackers) {
          TTInfo ttInfo = wovenClient.getTTInfo(taskTracker);
          TTClient ttCli =  cluster.getTTClient(ttInfo.getStatus().getHost());
          TaskID taskId = info.getTaskID();
          TTTaskInfo ttTaskInfo = ttCli.getProxy().getTask(taskId);
          Assert.assertNotNull(ttTaskInfo);
          Assert.assertNotNull(ttTaskInfo.getConf());
          Assert.assertNotNull(ttTaskInfo.getUser());
          Assert.assertTrue(ttTaskInfo.getTaskStatus().getProgress() >= 0.0);
          Assert.assertTrue(ttTaskInfo.getTaskStatus().getProgress() <= 1.0);
          //Get the pid of the task attempt. The task need not have 
          //reported the pid of the task by the time we are checking
          //the pid. So perform null check.
          String pid = ttTaskInfo.getPid();
          int i = 1;
          while(pid.isEmpty()) {
            Thread.sleep(1000);
            LOG.info("Waiting for task to report its pid back");
            ttTaskInfo = ttCli.getProxy().getTask(taskId);
            pid = ttTaskInfo.getPid();
            if(i == 40) {
              Assert.fail("The task pid not reported for 40 seconds.");
            }
            i++;
          }
          if(!isOneTaskStored) {
            sometaskpid = pid;
            sometaskId = ttTaskInfo.getTaskStatus().getTaskID();
            myCli = ttCli;
            isOneTaskStored = true;
          }
          LOG.info("verified task progress to be between 0 and 1");
          State state = ttTaskInfo.getTaskStatus().getRunState();
          if (ttTaskInfo.getTaskStatus().getProgress() < 1.0 &&
              ttTaskInfo.getTaskStatus().getProgress() >0.0) {
            Assert.assertEquals(TaskStatus.State.RUNNING, state);
            LOG.info("verified run state as " + state);
          }
          FinishTaskControlAction action = new FinishTaskControlAction(
              org.apache.hadoop.mapred.TaskID.downgrade(info.getTaskID()));
          ttCli.getProxy().sendAction(action);
        }
      }
    }
    rJob.killJob();
    int i = 1;
    while (!rJob.isComplete()) {
      Thread.sleep(1000);
      if (i == 40) {
        Assert
            .fail("The job not completed within 40 seconds after killing it.");
      }
      i++;
    }
    TTTaskInfo myTaskInfo = myCli.getProxy().getTask(sometaskId.getTaskID());
    i = 0;
    while (myTaskInfo != null && !myTaskInfo.getPid().isEmpty()) {
      LOG.info("sleeping till task is retired from TT memory");
      Thread.sleep(1000);
      myTaskInfo = myCli.getProxy().getTask(sometaskId.getTaskID());
      if (i == 40) {
        Assert
            .fail("Task not retired from TT memory within 40 seconds of job completeing");
      }
      i++;
    }
    Assert.assertFalse(myCli.getProxy().isProcessTreeAlive(sometaskpid));
  }
  
  @Test
  public void testClusterRestart() throws Exception {
    cluster.stop();
    // Give the cluster time to stop the whole cluster.
    AbstractDaemonClient cli = cluster.getJTClient();
    int i = 1;
    while (i < 40) {
      try {
        cli.ping();
        Thread.sleep(1000);
        i++;
      } catch (Exception e) {
        break;
      }
    }
    if (i >= 40) {
      Assert.fail("JT on " + cli.getHostName() + " Should have been down.");
    }
    i = 1;
    for (AbstractDaemonClient tcli : cluster.getTTClients()) {
      i = 1;
      while (i < 40) {
        try {
          tcli.ping();
          Thread.sleep(1000);
          i++;
        } catch (Exception e) {
          break;
        }
      }
      if (i >= 40) {
        Assert.fail("TT on " + tcli.getHostName() + " Should have been down.");
      }
    }
    cluster.start();
    cli = cluster.getJTClient();
    i = 1;
    while (i < 40) {
      try {
        cli.ping();
        break;
      } catch (Exception e) {
        i++;
        Thread.sleep(1000);
        LOG.info("Waiting for Jobtracker on host : "
            + cli.getHostName() + " to come up.");
      }
    }
    if (i >= 40) {
      Assert.fail("JT on " + cli.getHostName() + " Should have been up.");
    }
    for (AbstractDaemonClient tcli : cluster.getTTClients()) {
      i = 1;
      while (i < 40) {
        try {
          tcli.ping();
          break;
        } catch (Exception e) {
          i++;
          Thread.sleep(1000);
          LOG.info("Waiting for Tasktracker on host : "
              + tcli.getHostName() + " to come up.");
        }
      }
      if (i >= 40) {
        Assert.fail("TT on " + tcli.getHostName() + " Should have been Up.");
      }
    }
  } 
}
