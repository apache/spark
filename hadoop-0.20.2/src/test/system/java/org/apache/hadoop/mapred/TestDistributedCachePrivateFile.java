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
 * when the file is private. Once a job uses a distributed 
 * cache file with private permissions that file is stored in the
 * mapred.local.dir, under the directory which has the same name 
 * as job submitter's username. The directory has 700 permission 
 * and the file under it, should have 777 permissions. 
*/

public class TestDistributedCachePrivateFile {

  private static MRCluster cluster = null;
  private static FileSystem dfs = null;
  private static JobClient client = null;
  private static FsPermission permission = new FsPermission((short)00770);

  private static String uriPath = "hdfs:///tmp/test.txt";
  private static final Path URIPATH = new Path(uriPath);
  private String distributedFileName = "test.txt";

  static final Log LOG = LogFactory.
                           getLog(TestDistributedCachePrivateFile.class);

  public TestDistributedCachePrivateFile() throws Exception {
  }

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = MRCluster.createCluster(new Configuration());
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

    String input = "This will be the content of\n" + "distributed cache\n";
    //Creating the path with the file
    DataOutputStream file = 
        UtilsForTests.createTmpFileDFS(dfs, URIPATH, permission, input);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    cluster.tearDown();
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
  }

  @Test
  /**
   * This tests Distributed Cache for private file
   * @param none
   * @return void
   */
  public void testDistributedCache() throws Exception {
    Configuration conf = new Configuration(cluster.getConf());
    JTProtocol wovenClient = cluster.getJTClient().getProxy();

    String jobTrackerUserName = wovenClient.getDaemonUser();

    LOG.info("jobTrackerUserName is :" + jobTrackerUserName);

    //This counter will check for count of a loop,
    //which might become infinite.
    int count = 0;

    SleepJob job = new SleepJob();
    job.setConf(conf);
    conf = job.setupJobConf(5, 1, 1000, 1000, 100, 100);

    DistributedCache.createSymlink(conf);
    URI uri = URI.create(uriPath);
    DistributedCache.addCacheFile(uri, conf);
    JobConf jconf = new JobConf(conf);

    //Controls the job till all verification is done 
    FinishTaskControlAction.configureControlActionForJob(conf);

    //Submitting the job
    RunningJob rJob = cluster.getJTClient().getClient().submitJob(jconf);

    JobStatus[] jobStatus = client.getAllJobs();
    String userName = jobStatus[0].getUsername();

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
      //If the count goes beyond a point, then Assert; This is to avoid
      //infinite loop under unforeseen circumstances.
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

      for(String taskTracker : taskTrackers) {
        //Getting the exact FQDN of the tasktracker from
        //the tasktracker string.
        taskTracker = UtilsForTests.getFQDNofTT(taskTracker);
        tClient =  cluster.getTTClient(taskTracker);
        String[] localDirs = tClient.getMapredLocalDirs();
        int distributedFileCount = 0;
        String localDirOnly = null;

        boolean FileNotPresentForThisDirectoryPath = false;

        //Go to every single path
        for (String localDir : localDirs) {
          FileNotPresentForThisDirectoryPath = false;
          localDirOnly = localDir;

          //Public Distributed cache will always be stored under
          //mapred.local.dir/tasktracker/archive
          localDirOnly = localDir + Path.SEPARATOR + TaskTracker.SUBDIR + 
              Path.SEPARATOR +  userName;

          //Private Distributed cache will always be stored under
          //mapre.local.dir/taskTracker/<username>/distcache
          //Checking for username directory to check if it has the
          //proper permissions
          localDir = localDir + Path.SEPARATOR +
                  TaskTracker.getPrivateDistributedCacheDir(userName);

          FileStatus fileStatusMapredLocalDirUserName = null;

          try {
            fileStatusMapredLocalDirUserName = tClient.
                            getFileStatus(localDirOnly, true);
          } catch (Exception e) {
            LOG.info("LocalDirOnly :" + localDirOnly + " not found");
            FileNotPresentForThisDirectoryPath = true;
          }

          //File will only be stored under one of the mapred.lcoal.dir
          //If other paths were hit, just continue  
          if (FileNotPresentForThisDirectoryPath)
            continue;

          Path pathMapredLocalDirUserName = 
              fileStatusMapredLocalDirUserName.getPath();
          FsPermission fsPermMapredLocalDirUserName =
              fileStatusMapredLocalDirUserName.getPermission();
          //If userName of Jobtracker is same as username
          //of jobSubmission, then the permissions are 770.
          //Otherwise 570
          if ( userName.compareTo(jobTrackerUserName) == 0 ) {
            Assert.assertTrue("Directory Permission is not 770",
              fsPermMapredLocalDirUserName.equals(new FsPermission("770")));
          } else {
            Assert.assertTrue("Directory Permission is not 570",
              fsPermMapredLocalDirUserName.equals(new FsPermission("570")));
          }

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
              //If userName of Jobtracker is same as username
              //of jobSubmission, then the permissions are 770.
              //Otherwise 570
              if ( userName.compareTo(jobTrackerUserName) == 0 ) {
                Assert.assertTrue("File Permission is not 770",
                  fsPerm.equals(new FsPermission("770")));
              } else {
                Assert.assertTrue("File Permission is not 570",
                  fsPerm.equals(new FsPermission("570")));
              }
            }
          }
        }

        LOG.info("Distributed File count is :" + distributedFileCount);

        if (distributedFileCount > 1) {
          Assert.fail("The distributed cache file is more than one");
        } else if (distributedFileCount < 1)
          Assert.fail("The distributed cache file is less than one");
        if (!distCacheFileIsFound) {
          Assert.assertEquals("The distributed cache file does not exist", 
              distCacheFileIsFound, false);
        }
      }

      //Allow the job to continue through MR control job.
      for (TaskInfo taskInfoRemaining : taskInfos) {
        FinishTaskControlAction action = new FinishTaskControlAction(TaskID
           .downgrade(taskInfoRemaining.getTaskID()));
        Collection<TTClient> tts = cluster.getTTClients();
        for (TTClient cli : tts) {
          cli.getProxy().sendAction(action);
        }
      }

      //Killing the job because all the verification needed
      //for this testcase is completed.
      rJob.killJob();
    }
  }
}
