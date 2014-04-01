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

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.UtilsForTests;
import org.apache.hadoop.mapred.JobClient.NetworkedJob;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.Path;
import testjar.GenerateTaskChildProcess;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

/**
 * Verify the Task Tracker Info functionality.
 */

public class TestTaskTrackerInfoSuccessfulFailedJobs {

  private static MRCluster cluster = null;
  private static JobClient client = null;
  static final Log LOG = LogFactory.
                          getLog(TestTaskTrackerInfoSuccessfulFailedJobs.class);
  private static Configuration conf = null;
  private static JTProtocol remoteJTClient = null;

  StatisticsCollectionHandler statisticsCollectionHandler = null;
  int taskTrackerHeartBeatInterval = 0;

  public TestTaskTrackerInfoSuccessfulFailedJobs() throws Exception {
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
   * This tests Task tracker summary information for
   * since start - total tasks, successful tasks
   * Last Hour - total tasks, successful tasks
   * Last Day - total tasks, successful tasks
   * It is checked for multiple job submissions. 
   * @param none
   * @return void
   */
  public void testTaskTrackerInfoAll() throws Exception {
    
    //This boolean will decide whether to run job again
    boolean continueLoop = true;

    //counter for job Loop
    int countLoop = 0;

    String jobTrackerUserName = remoteJTClient.getDaemonUser();

    LOG.info("jobTrackerUserName is :" + jobTrackerUserName);

    //This counter will check for count of a loop,
    //which might become infinite.
    int count = 0;

    SleepJob job = new SleepJob();
    job.setConf(conf);
    int totalMapTasks = 5;
    int totalReduceTasks = 1;
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
      if (count > 180) {
        Assert.fail("Since this value has not reached 0" +
          "in more than 180 seconds. Failing at this point");
      }
    }

    statisticsCollectionHandler = null;
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
    RunningJob rJob = cluster.getJTClient().getClient().submitJob(jconf);

    JobInfo jInfo = remoteJTClient.getJobInfo(rJob.getID());
    LOG.info("jInfo is :" + jInfo);

    //Assert if jobInfo is null
    Assert.assertNotNull("jobInfo is null", jInfo);

    count = 0;
    LOG.info("Waiting till the job is completed...");
    while (jInfo != null && !jInfo.getStatus().isJobComplete()) {
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
        totalTasksSinceStartAfterJob);
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
   * This tests Task tracker task killed 
   * summary information for
   * since start - total tasks, successful tasks
   * Last Hour - total tasks, successful tasks
   * Last Day - total tasks, successful tasks
   * It is checked for multiple job submissions. 
   * @param none
   * @return void
   */
  public void testTaskTrackerInfoKilled() throws Exception {
    
    //This boolean will decide whether to run job again
    boolean continueLoop = true;

    //counter for job Loop
    int countLoop = 0;

    TaskInfo taskInfo = null;

    String jobTrackerUserName = remoteJTClient.getDaemonUser();

    LOG.info("jobTrackerUserName is :" + jobTrackerUserName);

    //This counter will check for count of a loop,
    //which might become infinite.
    int count = 0;

    SleepJob job = new SleepJob();
    job.setConf(conf);
    int totalMapTasks = 5;
    int totalReduceTasks = 1;
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

    statisticsCollectionHandler = null;
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
      }
    }

    count = 0;
    taskInfo = remoteJTClient.getTaskInfo(taskInfo.getTaskID());
    while (count < 60) {
      if (taskInfo.getTaskStatus().length > 0) {
        if (taskInfo.getTaskStatus()[0].getRunState()
              == TaskStatus.State.RUNNING) {
          break;
        }
      }
      UtilsForTests.waitFor(1000);
      taskInfo = remoteJTClient.getTaskInfo(taskInfo.getTaskID());
      count++;
    }

    Assert.assertTrue("Task has not been started for 1 min.", 
      count != 60);

    NetworkedJob networkJob = (cluster.getJTClient().getClient()).new 
      NetworkedJob(jInfo.getStatus());
    TaskID tID = TaskID.downgrade(taskInfo.getTaskID());
    TaskAttemptID taskAttID = new TaskAttemptID(tID , 0);
    networkJob.killTask(taskAttID, false);

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

    //Total tasks expected is setup, Cleanup + totalMapTasks
    //+ totalReduceTasks
    int totalTasksForJob = (totalMapTasks + totalReduceTasks + 2); 

    //The total tasks will be equal to the totalTasksSinceStartBeforeJob
    // + totalTasksFor present Job + 1 more task which was killed.
    //This kiled task will be re-attempted by the job tracker and would have
    //rerun in another tasktracker and would have completed successfully,
    //which is captured in totalTasksForJob
    Assert.assertEquals("The number of total tasks, since start" +
         " dont match", 
        (totalTasksSinceStartBeforeJob + totalTasksForJob + 1), 
        totalTasksSinceStartAfterJob );
    Assert.assertEquals("The number of succeeded tasks, " +
         "since start dont match", 
        (succeededTasksSinceStartBeforeJob + totalTasksForJob), 
             succeededTasksSinceStartAfterJob); 

    Assert.assertEquals("The number of total tasks, last hour" +
        " dont match", 
        (totalTasksLastHourBeforeJob + totalTasksForJob + 1), 
        totalTasksLastHourAfterJob);
    Assert.assertEquals("The number of succeeded tasks, " +
        "last hour dont match", 
        (succeededTasksLastHourBeforeJob + totalTasksForJob),  
        succeededTasksLastHourAfterJob); 

    Assert.assertEquals("The number of total tasks, last day" +
        " dont match", 
        (totalTasksLastDayBeforeJob + totalTasksForJob + 1), 
        totalTasksLastDayAfterJob);
    Assert.assertEquals("The number of succeeded tasks, " +
        "since start dont match", 
        (succeededTasksLastDayBeforeJob + totalTasksForJob), 
        succeededTasksLastDayAfterJob); 

  }

  @Test
  /**
   * This tests Task tracker task failure 
   * summary information for
   * since start - total tasks, successful tasks
   * Last Hour - total tasks, successful tasks
   * Last Day - total tasks, successful tasks
   * @param none
   * @return void
   */
  public void testTaskTrackerInfoTaskFailure() throws Exception {
    
    //This boolean will decide whether to run job again
    boolean continueLoop = true;

    //counter for job Loop
    int countLoop = 0;

    TaskInfo taskInfo = null;

    String jobTrackerUserName = remoteJTClient.getDaemonUser();

    LOG.info("jobTrackerUserName is :" + jobTrackerUserName);

    //This counter will check for count of a loop,
    //which might become infinite.
    int count = 0;

    Configuration conf = new Configuration(cluster.getConf());
    conf.setBoolean("mapreduce.map.output.compress", false); 
    conf.set("mapred.map.output.compression.codec", 
        "org.apache.hadoop.io.compress.DefaultCodec");
    JobConf jconf = new JobConf(conf);
    Path inputDir = new Path("input");
    Path outputDir = new Path("output");
    cleanup(inputDir, conf);
    cleanup(outputDir, conf);

    createInput(inputDir, conf);
    jconf.setJobName("Task Failed job");
    jconf.setJarByClass(UtilsForTests.class);
    jconf.setMapperClass(GenerateTaskChildProcess.FailedMapper.class);
    jconf.setNumMapTasks(1);
    jconf.setNumReduceTasks(0);
    jconf.setMaxMapAttempts(1);
    FileInputFormat.setInputPaths(jconf, inputDir);
    FileOutputFormat.setOutputPath(jconf, outputDir);

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

    statisticsCollectionHandler = null;
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

    RunningJob rJob = cluster.getJTClient().getClient().submitJob(jconf);
    JobID id = rJob.getID();
    JobInfo jInfo = remoteJTClient.getJobInfo(id);

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
    while ( jInfo != null && (!jInfo.getStatus().isJobComplete())) {
      UtilsForTests.waitFor(1000);
      count++;
      jInfo = remoteJTClient.getJobInfo(id);
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

    //1 map running 4 times before failure, plus sometimes two failures 
    //which are not captured in Job summary, but caught in 
    //tasktracker summary. 
    //0 reduces, setup and cleanup
    int totalTasksForJob = 4; 

    Assert.assertTrue("The number of total tasks, since start" +
         " dont match", (totalTasksSinceStartAfterJob >= 
         totalTasksSinceStartBeforeJob + totalTasksForJob)); 
        
    Assert.assertTrue("The number of succeeded tasks, " +
        "since start dont match", 
        (succeededTasksSinceStartAfterJob  >= 
        succeededTasksSinceStartBeforeJob));
      
    Assert.assertTrue("The number of total tasks, last hour" +
        " dont match", (totalTasksLastHourAfterJob >= 
         totalTasksLastHourBeforeJob + totalTasksForJob));
    Assert.assertTrue("The number of succeeded tasks, " +
        "last hour dont match", (succeededTasksLastHourAfterJob >= 
        succeededTasksLastHourBeforeJob));  
    
    Assert.assertTrue("The number of total tasks, last day" +
        " dont match", totalTasksLastDayAfterJob >= 
        totalTasksLastDayBeforeJob + totalTasksForJob); 
    Assert.assertTrue("The number of succeeded tasks, " +
        "since start dont match", succeededTasksLastDayAfterJob >= 
        succeededTasksLastDayBeforeJob);
  }

  //This creates the input directories in the dfs
  private void createInput(Path inDir, Configuration conf) throws
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
    while(i < 1000 * 3000) {
      file.writeBytes(input);
      i++;
    }
    file.close();
  }

  //This cleans up the specified directories in the dfs
  private void cleanup(Path dir, Configuration conf) throws
          IOException {
    FileSystem fs = dir.getFileSystem(conf);
    fs.delete(dir, true);
  }
}
