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
import java.util.List;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.TTClient;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.UtilsForTests;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.mapred.StatisticsCollectionHandler;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

/**
 * Verify the Task Tracker Info functionality.
 */

public class TestTaskTrackerInfoTTProcess {

  private static MRCluster cluster = null;
  private static JobClient client = null;
  static final Log LOG = LogFactory.
                           getLog(TestTaskTrackerInfoTTProcess.class);
  private static Configuration conf = null;
  private static JTProtocol remoteJTClient = null;
  private static String confFile = "mapred-site.xml";
  int taskTrackerHeartBeatInterval;
  StatisticsCollectionHandler statisticsCollectionHandler = null;

  public TestTaskTrackerInfoTTProcess() throws Exception {
  }

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = MRCluster.createCluster(new Configuration());
    cluster.setUp();
    conf = new Configuration(cluster.getConf());
    conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);
    remoteJTClient = cluster.getJTClient().getProxy();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    cluster.tearDown();
  }

  @Test
  /**
   * This tests Task tracker info when a job with 0 maps and 0 reduces run 
   * summary information for
   * since start - total tasks, successful tasks
   * Last Hour - total tasks, successful tasks
   * Last Day - total tasks, successful tasks
   * It is checked for multiple job submissions. 
   * @param none
   * @return void
   */
  public void testTaskTrackerInfoZeroMapsZeroReduces() throws Exception {
    
    TaskInfo taskInfo = null;

    String jobTrackerUserName = remoteJTClient.getDaemonUser();

    LOG.info("jobTrackerUserName is :" + jobTrackerUserName);

    int count = 0;

    SleepJob job = new SleepJob();
    job.setConf(conf);
    int totalMapTasks = 0;
    int totalReduceTasks = 0;
    conf = job.setupJobConf(totalMapTasks, totalReduceTasks, 
        100, 100, 100, 100);
    JobConf jconf = new JobConf(conf);

    count = 0;
    //The last hour and last day are given 60 seconds and 120 seconds
    //recreate values rate, replacing one hour and 1 day. Waiting for 
    //them to be ona  just created stage when testacse starts.
    while (remoteJTClient.getInfoFromAllClients("last_day","total_tasks") 
        != 0) {
      count++;
      UtilsForTests.waitFor(1000);
      //If the count goes beyond a point, then break; This is to avoid
      //infinite loop under unforeseen circumstances. Testcase will
      //anyway fail later.
      if (count > 140) {
        Assert.fail("Since this value has not reached 0" +
          "in more than 140 seconds. Failing at this point");
      }
    }

    statisticsCollectionHandler = remoteJTClient.
        getInfoFromAllClientsForAllTaskType();

    int totalTasksSinceStartBeforeJob = statisticsCollectionHandler.
        getSinceStartTotalTasks();
    int succeededTasksSinceStartBeforeJob = statisticsCollectionHandler.
        getSinceStartSucceededTasks();
    int totalTasksLastHourBeforeJob = statisticsCollectionHandler.
        getLastHourTotalTasks();
    int succeededTasksLastHourBeforeJob = statisticsCollectionHandler.
        getLastHourSucceededTasks();
    int totalTasksLastDayBeforeJob = statisticsCollectionHandler.
        getLastDayTotalTasks();
    int succeededTasksLastDayBeforeJob = statisticsCollectionHandler.
        getLastDaySucceededTasks();
 
    //Submitting the job
    RunningJob rJob = cluster.getJTClient().getClient().
        submitJob(jconf);

    JobInfo jInfo = remoteJTClient.getJobInfo(rJob.getID());
    LOG.info("jInfo is :" + jInfo);

    count = 0;
    while (count < 60) {
      if (jInfo.getStatus().getRunState() == JobStatus.RUNNING) {
        break;
      } else {
        UtilsForTests.waitFor(1000);
        jInfo = remoteJTClient.getJobInfo(rJob.getID());
      }
      count++;
    }
    Assert.assertTrue("Job has not been started for 1 min.", 
      count != 60);

    //Assert if jobInfo is null
    Assert.assertNotNull("jobInfo is null", jInfo);

    count = 0;
    LOG.info("Waiting till the job is completed...");
    while (!jInfo.getStatus().isJobComplete()) {
      UtilsForTests.waitFor(1000);
      count++;
      jInfo = remoteJTClient.getJobInfo(rJob.getID());
      //If the count goes beyond a point, then break; This is to avoid
      //infinite loop under unforeseen circumstances. Testcase will 
      //anyway fail later.
      if (count > 40) {
        Assert.fail("job has not reached completed state for more" +
          " than 400 seconds. Failing at this point");
      }
    }

    //Waiting for 20 seconds to make sure that all the completed tasks 
    //are reflected in their corresponding Tasktracker boxes. 
    taskTrackerHeartBeatInterval =  remoteJTClient.
        getTaskTrackerHeartbeatInterval();

    //Waiting for 6 times the Task tracker heart beat interval to
    //account for network slowness, job tracker processing time
    //after receiving the tasktracker updates etc.
    UtilsForTests.waitFor(taskTrackerHeartBeatInterval * 6);

    statisticsCollectionHandler = null;
    statisticsCollectionHandler =
      remoteJTClient.getInfoFromAllClientsForAllTaskType();
    int totalTasksSinceStartAfterJob = statisticsCollectionHandler.
        getSinceStartTotalTasks();
    int succeededTasksSinceStartAfterJob = statisticsCollectionHandler.
        getSinceStartSucceededTasks();
    int totalTasksLastHourAfterJob = statisticsCollectionHandler.
        getLastHourTotalTasks();
    int succeededTasksLastHourAfterJob = statisticsCollectionHandler.
        getLastHourSucceededTasks();
    int totalTasksLastDayAfterJob = statisticsCollectionHandler.
        getLastDayTotalTasks();
    int succeededTasksLastDayAfterJob = statisticsCollectionHandler.
        getLastDaySucceededTasks();

    int totalTasksForJob = (totalMapTasks + totalReduceTasks + 2); 

    Assert.assertEquals("The number of total tasks, since start" +
         " dont match", 
        (totalTasksSinceStartBeforeJob + totalTasksForJob), 
          totalTasksSinceStartAfterJob );
    Assert.assertEquals("The number of succeeded tasks, " +
        "since start dont match", 
        (succeededTasksSinceStartBeforeJob + totalTasksForJob), 
        succeededTasksSinceStartAfterJob); 

    Assert.assertEquals("The number of total tasks, last hour" +
        " dont match", 
        (totalTasksLastHourBeforeJob + totalTasksForJob), 
        totalTasksLastHourAfterJob);
    Assert.assertEquals("The number of succeeded tasks, " +
        "last hour dont match", 
        (succeededTasksLastHourBeforeJob + totalTasksForJob),  
        succeededTasksLastHourAfterJob); 

    Assert.assertEquals("The number of total tasks, last day" +
        " dont match", 
        (totalTasksLastDayBeforeJob + totalTasksForJob), 
        totalTasksLastDayAfterJob);
    Assert.assertEquals("The number of succeeded tasks, " +
        "since start dont match", 
        (succeededTasksLastDayBeforeJob + totalTasksForJob), 
        succeededTasksLastDayAfterJob); 
  }

  @Test
  /**
   * This tests Task tracker info when tasktracker is suspended/killed 
   * and then the process comes alive again 
   * summary information for
   * since start - total tasks, successful tasks
   * Last Hour - total tasks, successful tasks
   * Last Day - total tasks, successful tasks
   * It is checked for multiple job submissions. 
   * @param none
   * @return void
   */
  public void testTaskTrackerInfoTaskTrackerSuspend() throws Exception {
    
    TaskInfo taskInfo = null;

    String jobTrackerUserName = remoteJTClient.getDaemonUser();

    LOG.info("jobTrackerUserName is :" + jobTrackerUserName);

    int count = 0;

    SleepJob job = new SleepJob();
    job.setConf(conf);
    int totalMapTasks = 5;
    int totalReduceTasks = 1;
    conf = job.setupJobConf(totalMapTasks, totalReduceTasks, 
        100, 100, 100, 100);
    JobConf jconf = new JobConf(conf);

    //The last hour and last day are given 60 seconds and 120 seconds
    //recreate values rate, replacing one hour and 1 day. Waiting for 
    //them to be ona  just created stage when testacse starts.
    while (remoteJTClient.getInfoFromAllClients("last_day","total_tasks") 
        != 0) {
      count++;
      UtilsForTests.waitFor(1000);
      //If the count goes beyond a point, then break; This is to avoid
      //infinite loop under unforeseen circumstances. Testcase will
      //anyway fail later.
      if (count > 140) {
        Assert.fail("Since this value has not reached 0" +
          "in more than 140 seconds. Failing at this point");
      }
    }

    statisticsCollectionHandler = remoteJTClient.
        getInfoFromAllClientsForAllTaskType();

    int totalTasksSinceStartBeforeJob = statisticsCollectionHandler.
        getSinceStartTotalTasks();
    int succeededTasksSinceStartBeforeJob = statisticsCollectionHandler.
        getSinceStartSucceededTasks();
    int totalTasksLastHourBeforeJob = statisticsCollectionHandler.
        getLastHourTotalTasks();
    int succeededTasksLastHourBeforeJob = statisticsCollectionHandler.
        getLastHourSucceededTasks();
    int totalTasksLastDayBeforeJob = statisticsCollectionHandler.
        getLastDayTotalTasks();
    int succeededTasksLastDayBeforeJob = statisticsCollectionHandler.
        getLastDaySucceededTasks();

    //Submitting the job
    RunningJob rJob = cluster.getJTClient().getClient().
        submitJob(jconf);

    JobInfo jInfo = remoteJTClient.getJobInfo(rJob.getID());
    LOG.info("jInfo is :" + jInfo);

    count = 0;
    while (count < 60) {
      if (jInfo.getStatus().getRunState() == JobStatus.RUNNING) {
        break;
      } else {
        UtilsForTests.waitFor(1000);
        jInfo = remoteJTClient.getJobInfo(rJob.getID());
      }
      count++;
    }
    Assert.assertTrue("Job has not been started for 1 min.", 
      count != 60);

    //Assert if jobInfo is null
    Assert.assertNotNull("jobInfo is null", jInfo);

    TaskInfo[] taskInfos = remoteJTClient.getTaskInfo(rJob.getID());
    for (TaskInfo taskinfo : taskInfos) {
      if (!taskinfo.isSetupOrCleanup()) {
        taskInfo = taskinfo;
        break;
      }
    }

    TTClient ttClient = cluster.getTTClientInstance(taskInfo);
    String pid = null;
    ttClient.kill();
    ttClient.waitForTTStop();
    ttClient.start();
    ttClient.waitForTTStart();

    count = 0;
    LOG.info("Waiting till the job is completed...");
    while (!jInfo.getStatus().isJobComplete()) {
      UtilsForTests.waitFor(1000);
      count++;
      jInfo = remoteJTClient.getJobInfo(rJob.getID());
      //If the count goes beyond a point, then break; This is to avoid
      //infinite loop under unforeseen circumstances. Testcase will 
      //anyway fail later.
      if (count > 40) {
        Assert.fail("job has not reached completed state for more" +
          " than 400 seconds. Failing at this point");
      }
    }

    //Waiting for 20 seconds to make sure that all the completed tasks
    //are reflected in their corresponding Tasktracker boxes.
    taskTrackerHeartBeatInterval =  remoteJTClient.
        getTaskTrackerHeartbeatInterval();

    //Waiting for 6 times the Task tracker heart beat interval to
    //account for network slowness, job tracker processing time
    //after receiving the tasktracker updates etc.
    UtilsForTests.waitFor(taskTrackerHeartBeatInterval * 6);

    statisticsCollectionHandler = null;
    statisticsCollectionHandler =
      remoteJTClient.getInfoFromAllClientsForAllTaskType();
    int totalTasksSinceStartAfterJob = statisticsCollectionHandler.
        getSinceStartTotalTasks();
    int succeededTasksSinceStartAfterJob = statisticsCollectionHandler.
        getSinceStartSucceededTasks();
    int totalTasksLastHourAfterJob = statisticsCollectionHandler.
        getLastHourTotalTasks();
    int succeededTasksLastHourAfterJob = statisticsCollectionHandler.
        getLastHourSucceededTasks();
    int totalTasksLastDayAfterJob = statisticsCollectionHandler.
        getLastDayTotalTasks();
    int succeededTasksLastDayAfterJob = statisticsCollectionHandler.
        getLastDaySucceededTasks();

    int totalTasksForJob = (totalMapTasks + totalReduceTasks + 2); 

    Assert.assertTrue("The number of total tasks, since start" +
         " dont match",  
        (totalTasksSinceStartAfterJob >= succeededTasksSinceStartBeforeJob 
        + totalTasksForJob));

    Assert.assertTrue("The number of succeeded tasks, " +
        "since start dont match",  
        (succeededTasksSinceStartAfterJob >= succeededTasksSinceStartBeforeJob
        + totalTasksForJob)); 
      
    Assert.assertTrue("The number of total tasks, last hour" +
        " dont match", 
        ( totalTasksLastHourAfterJob >= totalTasksLastHourBeforeJob + 
        totalTasksForJob)); 

    Assert.assertTrue("The number of succeeded tasks, " +
        "last hour dont match", 
        (succeededTasksLastHourAfterJob >= succeededTasksLastHourBeforeJob + 
        totalTasksForJob));  

    Assert.assertTrue("The number of total tasks, last day" +
        " dont match", 
        (totalTasksLastDayAfterJob >= totalTasksLastDayBeforeJob + 
        totalTasksForJob));

    Assert.assertTrue("The number of succeeded tasks, " +
        "since start dont match", 
        (succeededTasksLastDayAfterJob >= succeededTasksLastDayBeforeJob + 
        totalTasksForJob));
   }
}
