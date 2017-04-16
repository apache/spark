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
import java.net.URI;
import java.util.Collection;
import java.util.ArrayList;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.TTClient;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;
import org.apache.hadoop.mapreduce.test.system.MRCluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.UtilsForTests;

import org.apache.hadoop.mapreduce.test.system.FinishTaskControlAction;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.examples.SleepJob;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

/**
 * Verify the Distributed Cache functionality.
 * This test scenario is for a distributed cache file behaviour
 * when it is modified before and after being
 * accessed by maximum two jobs. Once a job uses a distributed cache file
 * that file is stored in the mapred.local.dir. If the next job
 * uses the same file, but with differnt timestamp, then that 
 * file is stored again. So, if two jobs choose
 * the same tasktracker for their job execution
 * then, the distributed cache file should be found twice.
 *
 * This testcase runs a job with a distributed cache file. All the
 * tasks' corresponding tasktracker's handle is got and checked for
 * the presence of distributed cache with proper permissions in the
 * proper directory. Next when job 
 * runs again and if any of its tasks hits the same tasktracker, which
 * ran one of the task of the previous job, then that
 * file should be uploaded again and task should not use the old file. 
 * This is verified.
*/

public class TestDistributedCacheModifiedFile {

  private static MRCluster cluster = null;
  private static FileSystem dfs = null;
  private static FileSystem ttFs = null;
  private static JobClient client = null;
  private static FsPermission permission = new FsPermission((short)00777);

  private static String uriPath = "hdfs:///tmp/test.txt";
  private static final Path URIPATH = new Path(uriPath);
  private String distributedFileName = "test.txt";

  static final Log LOG = LogFactory.
                           getLog(TestDistributedCacheModifiedFile.class);

  public TestDistributedCacheModifiedFile() throws Exception {
  }

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = MRCluster.createCluster(new Configuration());
    String [] expExcludeList = {"java.net.ConnectException",
        "java.io.IOException","org.apache.hadoop.metrics2.MetricsException"};
    cluster.setExcludeExpList(expExcludeList);

    cluster.setUp();
    client = cluster.getJTClient().getClient();
    dfs = client.getFs();
    //Deleting the file if it already exists
    dfs.delete(URIPATH, true);

    Collection<TTClient> tts = cluster.getTTClients();
    //Stopping all TTs
    for (TTClient tt : tts) {
      tt.kill();
      tt.waitForTTStop();
    }
    //Starting all TTs
    for (TTClient tt : tts) {
      tt.start();
      tt.waitForTTStart();
    }
    //Waiting for 5 seconds to make sure tasktrackers are ready 
    Thread.sleep(5000);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    dfs.delete(URIPATH, true);
    
    Collection<TTClient> tts = cluster.getTTClients();
    //Stopping all TTs
    for (TTClient tt : tts) {
      tt.kill();
      tt.waitForTTStop();
    }
    //Starting all TTs
    for (TTClient tt : tts) {
      tt.start();
      tt.waitForTTStart();
    }
    cluster.tearDown();
  }

  @Test
  /**
   * This tests Distributed Cache for modified file
   * @param none
   * @return void
   */
  public void testDistributedCache() throws Exception {
    Configuration conf = new Configuration(cluster.getConf());
    JTProtocol wovenClient = cluster.getJTClient().getProxy();

    //This counter will check for count of a loop, 
    //which might become infinite.
    int count = 0;
    //This boolean will decide whether to run job again
    boolean continueLoop = true;
    //counter for job Loop
    int countLoop = 0;
    //This counter increases with all the tasktrackers in which tasks ran
    int taskTrackerCounter = 0;
    //This will store all the tasktrackers in which tasks ran
    ArrayList<String> taskTrackerCollection = new ArrayList<String>();
    //This boolean tells if two tasks ran onteh same tasktracker or not
    boolean taskTrackerFound = false;

    do {
      SleepJob job = new SleepJob();
      job.setConf(conf);
      conf = job.setupJobConf(5, 1, 1000, 1000, 100, 100);
      conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);

      //Before starting, Modify the file
      String input = "This will be the content of\n" + "distributed cache\n";
      //Creating the path with the file
      DataOutputStream file =
          UtilsForTests.createTmpFileDFS(dfs, URIPATH, permission, input);

      DistributedCache.createSymlink(conf);
      URI uri = URI.create(uriPath);
      DistributedCache.addCacheFile(uri, conf);
      JobConf jconf = new JobConf(conf);

      //Controls the job till all verification is done 
      FinishTaskControlAction.configureControlActionForJob(conf);

      //Submitting the job
      RunningJob rJob = cluster.getJTClient().getClient().submitJob(jconf);

      //counter for job Loop
      countLoop++;

      TTClient tClient = null;
      JobInfo jInfo = wovenClient.getJobInfo(rJob.getID());
      LOG.info("jInfo is :" + jInfo);

      //Assert if jobInfo is null
      Assert.assertNotNull("jobInfo is null", jInfo);

      //Wait for the job to start running.
      count = 0;
      while (jInfo.getStatus().getRunState() != JobStatus.RUNNING) {
        UtilsForTests.waitFor(10000);
        count++;
        jInfo = wovenClient.getJobInfo(rJob.getID());
        //If the count goes beyond a point, then break; This is to avoid
        //infinite loop under unforeseen circumstances. Testcase will anyway
        //fail later.
        if (count > 10) {
          Assert.fail("job has not reached running state for more than" +
            "100 seconds. Failing at this point");
        }
      }

      LOG.info("job id is :" + rJob.getID().toString());

      TaskInfo[] taskInfos = cluster.getJTClient().getProxy()
             .getTaskInfo(rJob.getID());

      boolean distCacheFileIsFound;

      for (TaskInfo taskInfo : taskInfos) {
        distCacheFileIsFound = false;
        String[] taskTrackers = taskInfo.getTaskTrackers();
        for (String taskTracker : taskTrackers) {
          //Formatting tasktracker to get just its FQDN 
          taskTracker = UtilsForTests.getFQDNofTT(taskTracker);
          LOG.info("taskTracker is :" + taskTracker);

          //The tasktrackerFound variable is initialized
          taskTrackerFound = false;

          //This will be entered from the second job onwards
          if (countLoop > 1) {
            if (taskTracker != null) {
              continueLoop = taskTrackerCollection.contains(taskTracker);
            }
            if (continueLoop) {
              taskTrackerFound = true;
            }
          }
          //Collecting the tasktrackers
          if (taskTracker != null)
            taskTrackerCollection.add(taskTracker);

          //we have loopped through two times to look for task
          //getting submitted on same tasktrackers.The same tasktracker 
          //for subsequent jobs was not hit maybe because of many number 
          //of tasktrackers. So, testcase has to stop here.
          if (countLoop > 1) {
            continueLoop = false;
          }

          tClient = cluster.getTTClient(taskTracker);

          //tClient maybe null because the task is already dead. Ex: setup
          if (tClient == null) {
            continue;
          }

          String[] localDirs = tClient.getMapredLocalDirs();
          int distributedFileCount = 0;
          //Go to every single path
          for (String localDir : localDirs) {
            //Public Distributed cache will always be stored under
            //mapre.local.dir/tasktracker/archive
            localDir = localDir + Path.SEPARATOR + 
                   TaskTracker.getPublicDistributedCacheDir();
            LOG.info("localDir is : " + localDir);

            //Get file status of all the directories 
            //and files under that path.
            FileStatus[] fileStatuses = tClient.listStatus(localDir, 
                true, true);
            for (FileStatus  fileStatus : fileStatuses) {
              Path path = fileStatus.getPath();
              LOG.info("path is :" + path.toString());
              //Checking if the received path ends with 
              //the distributed filename
              distCacheFileIsFound = (path.toString()).
                  endsWith(distributedFileName);
              //If file is found, check for its permission. 
              //Since the file is found break out of loop
              if (distCacheFileIsFound){
                LOG.info("PATH found is :" + path.toString());
                distributedFileCount++;
                String filename = path.getName();
                FsPermission fsPerm = fileStatus.getPermission();
                Assert.assertTrue("File Permission is not 777",
                    fsPerm.equals(new FsPermission("777")));
              }
            }
          }

          LOG.debug("The distributed FileCount is :" + distributedFileCount);
          LOG.debug("The taskTrackerFound is :" + taskTrackerFound);

          // If distributed cache is modified in dfs
          // between two job runs, it can be present more than once
          // in any of the task tracker, in which job ran.
          if (distributedFileCount != 2 && taskTrackerFound) {
            Assert.fail("The distributed cache file has to be two. " +
            		"But found was " + distributedFileCount);
          } else if (distributedFileCount < 1)
            Assert.fail("The distributed cache file is less than one. " +
            		"But found was " + distributedFileCount);
          if (!distCacheFileIsFound) {
            Assert.assertEquals("The distributed cache file does not exist",
                distCacheFileIsFound, false);
          }
        }
      }
      //Allow the job to continue through MR control job.
      cluster.signalAllTasks(rJob.getID());

      //Killing the job because all the verification needed
      //for this testcase is completed.
      rJob.killJob();

      //Waiting for 3 seconds for cleanup to start
      Thread.sleep(3000);

      //Getting the last cleanup task's tasktracker also, as
      //distributed cache gets uploaded even during cleanup.
      TaskInfo[] myTaskInfos = wovenClient.getTaskInfo(rJob.getID());
      if (myTaskInfos != null) {
        for(TaskInfo info : myTaskInfos) {
          if(info.isSetupOrCleanup()) {
            String[] taskTrackers = info.getTaskTrackers();
            for(String taskTracker : taskTrackers) {
              //Formatting tasktracker to get just its FQDN
              taskTracker = UtilsForTests.getFQDNofTT(taskTracker);
              LOG.info("taskTracker is :" + taskTracker);
              //Collecting the tasktrackers
              if (taskTracker != null)
                taskTrackerCollection.add(taskTracker);
            }    
          }
        }
      }

      //Making sure that the job is complete.
      while (jInfo != null && !jInfo.getStatus().isJobComplete()) {
        Thread.sleep(10000);
        jInfo = wovenClient.getJobInfo(rJob.getID());
      }

    } while (continueLoop);
  }
}
