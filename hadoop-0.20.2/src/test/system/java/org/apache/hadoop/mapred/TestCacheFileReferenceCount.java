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
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.mapreduce.test.system.TTClient;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import java.net.URI;
import java.io.DataOutputStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Assert;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Verify the job cache files localizations.
 */
public class TestCacheFileReferenceCount {
  private static final Log LOG = LogFactory
          .getLog(TestCacheFileReferenceCount.class);
  private static Configuration conf = new Configuration();
  private static MRCluster cluster;
  private static Path tmpFolderPath = null;
  private static String cacheFile1 = "cache1.txt";
  private static String cacheFile2 = "cache2.txt";
  private static String cacheFile3 = "cache3.txt";
  private static String cacheFile4 = "cache4.txt";
  private static JTProtocol wovenClient = null;
  private static JobClient jobClient = null;
  private static JTClient jtClient = null;
  private static URI cacheFileURI1;
  private static URI cacheFileURI2;
  private static URI cacheFileURI3;
  private static URI cacheFileURI4;
  
  @BeforeClass
  public static void before() throws Exception {
    cluster = MRCluster.createCluster(conf);
    cluster.setUp();
    tmpFolderPath = new Path("hdfs:///tmp");
    jtClient = cluster.getJTClient();
    jobClient = jtClient.getClient();
    wovenClient = cluster.getJTClient().getProxy();
    cacheFileURI1 = createCacheFile(tmpFolderPath, cacheFile1);
    cacheFileURI2 = createCacheFile(tmpFolderPath, cacheFile2);
  }
  
  @AfterClass
  public static void after() throws Exception {
    deleteCacheFile(new Path(tmpFolderPath, cacheFile1));
    deleteCacheFile(new Path(tmpFolderPath, cacheFile2));
    deleteCacheFile(new Path(tmpFolderPath, cacheFile4));
    cluster.tearDown();
  }
  
  /**
   * Run the job with two distributed cache files and verify
   * whether job is succeeded or not.
   * @throws Exception
   */
  @Test
  public void testCacheFilesLocalization() throws Exception {
    conf = wovenClient.getDaemonConf();
    SleepJob job = new SleepJob();
    job.setConf(conf);
    JobConf jobConf = job.setupJobConf(4, 1, 4000, 4000, 1000, 1000);
    DistributedCache.createSymlink(jobConf);
    DistributedCache.addCacheFile(cacheFileURI1, jobConf);
    DistributedCache.addCacheFile(cacheFileURI2, jobConf);
    RunningJob runJob = jobClient.submitJob(jobConf);
    JobID jobId = runJob.getID();

    Assert.assertTrue("Job has not been started for 1 min.", 
        jtClient.isJobStarted(jobId));
    TaskInfo[] taskInfos = wovenClient.getTaskInfo(jobId);
    Assert.assertTrue("Cache File1 has not been localize",
        checkLocalization(taskInfos,cacheFile1));
    Assert.assertTrue("Cache File2 has not been localize",
            checkLocalization(taskInfos,cacheFile2));
    JobInfo jInfo = wovenClient.getJobInfo(jobId);
    LOG.info("Waiting till the job is completed...");
    while (!jInfo.getStatus().isJobComplete()) {
      UtilsForTests.waitFor(100);
      jInfo = wovenClient.getJobInfo(jobId);
    }
    Assert.assertEquals("Job has not been succeeded", 
        jInfo.getStatus().getRunState(), JobStatus.SUCCEEDED);
  }
  
  /**
   * Run the job with distributed cache files and remove one cache
   * file from the DFS when it is localized.verify whether the job
   * is failed or not.
   * @throws Exception
   */
  @Test
  public void testDeleteCacheFileInDFSAfterLocalized() throws Exception {
    conf = wovenClient.getDaemonConf();
    SleepJob job = new SleepJob();
    job.setConf(conf);
    JobConf jobConf = job.setupJobConf(4, 1, 4000, 4000, 1000, 1000);
    cacheFileURI3 = createCacheFile(tmpFolderPath, cacheFile3);
    DistributedCache.createSymlink(jobConf);
    DistributedCache.addCacheFile(cacheFileURI3, jobConf);
    RunningJob runJob = jobClient.submitJob(jobConf);
    JobID jobId = runJob.getID();
    Assert.assertTrue("Job has not been started for 1 min.", 
        jtClient.isJobStarted(jobId));
    TaskInfo[] taskInfos = wovenClient.getTaskInfo(jobId);
    boolean iscacheFileLocalized = checkLocalization(taskInfos,cacheFile3);
    Assert.assertTrue("CacheFile has not been localized", 
        iscacheFileLocalized);
    deleteCacheFile(new Path(tmpFolderPath, cacheFile3));
    JobInfo jInfo = wovenClient.getJobInfo(jobId);
    LOG.info("Waiting till the job is completed...");
    while (!jInfo.getStatus().isJobComplete()) {
      UtilsForTests.waitFor(100);
      jInfo = wovenClient.getJobInfo(jobId);
    }
    Assert.assertEquals("Job has not been failed", 
        jInfo.getStatus().getRunState(), JobStatus.FAILED);
  }
  
  /**
   * Run the job with two distribute cache files and the size of
   * one file should be larger than local.cache.size.Verify 
   * whether job is succeeded or not.
   * @throws Exception
   */
  @Test
  public void testCacheSizeExceeds() throws Exception {
    conf = wovenClient.getDaemonConf();
    SleepJob job = new SleepJob();
    String jobArgs []= {"-D","local.cache.size=1024", 
                        "-m", "4", 
                        "-r", "2", 
                        "-mt", "2000", 
                        "-rt", "2000",
                        "-recordt","100"};
    JobConf jobConf = new JobConf(conf);
    cacheFileURI4 = createCacheFile(tmpFolderPath, cacheFile4);
    DistributedCache.createSymlink(jobConf);
    DistributedCache.addCacheFile(cacheFileURI4, jobConf);
    int countBeforeJS = jtClient.getClient().getAllJobs().length;
    JobID prvJobId = jtClient.getClient().getAllJobs()[0].getJobID();
    int exitCode = ToolRunner.run(jobConf,job,jobArgs);
    Assert.assertEquals("Exit Code:", 0, exitCode);
    int countAfterJS = jtClient.getClient().getAllJobs().length;
    int counter = 0;
    while (counter++ < 30 ) {
      if (countBeforeJS == countAfterJS) {
        UtilsForTests.waitFor(1000);
        countAfterJS = jtClient.getClient().getAllJobs().length;
      } else {
        break;
      } 
    }
    JobID jobId = jtClient.getClient().getAllJobs()[0].getJobID();
    counter = 0;
    while (counter++ < 30) {
      if (jobId.toString().equals(prvJobId.toString())) {
        UtilsForTests.waitFor(1000); 
        jobId = jtClient.getClient().getAllJobs()[0].getJobID();
      } else { 
        break;
      }
    }
    JobInfo jInfo = wovenClient.getJobInfo(jobId);
    Assert.assertEquals("Job has not been succeeded", 
          jInfo.getStatus().getRunState(), JobStatus.SUCCEEDED);
  }
  
  private boolean checkLocalization(TaskInfo[] taskInfos, String cacheFile) 
      throws Exception {
    boolean iscacheFileLocalized = false;
    for (TaskInfo taskinfo : taskInfos) {
      if (!taskinfo.isSetupOrCleanup()) {
        String[] taskTrackers = taskinfo.getTaskTrackers();
        List<TTClient> ttList = getTTClients(taskTrackers);
        for (TTClient ttClient : ttList) {
          iscacheFileLocalized = checkCacheFile(ttClient,cacheFile);
          if(iscacheFileLocalized) {
            return true;
          }
        } 
      }
    }
    return false;
  }
  
  private List<TTClient> getTTClients(String[] taskTrackers) 
      throws Exception {
    List<TTClient> ttClientList= new ArrayList<TTClient>();
    for (String taskTracker: taskTrackers) {
      taskTracker = UtilsForTests.getFQDNofTT(taskTracker);
      TTClient ttClient = cluster.getTTClient(taskTracker);
      if (ttClient != null) {
        ttClientList.add(ttClient);
      }
    }
    return ttClientList;
  }
  
  private boolean checkCacheFile(TTClient ttClient, String cacheFile) 
      throws IOException {
    String[] localDirs = ttClient.getMapredLocalDirs();
    for (String localDir : localDirs) {
      localDir = localDir + Path.SEPARATOR + 
          TaskTracker.getPublicDistributedCacheDir();
      FileStatus[] fileStatuses = ttClient.listStatus(localDir, 
          true, true);
      for (FileStatus  fileStatus : fileStatuses) {
        Path path = fileStatus.getPath();
        if ((path.toString()).endsWith(cacheFile)) {
          return true;
        }
      }
    }
    return false;
  }
  
  private static void deleteCacheFile(Path cacheFilePath) 
      throws IOException {
    FileSystem dfs = jobClient.getFs();
    dfs.delete(cacheFilePath, true);
  }

  private static URI createCacheFile(Path tmpFolderPath, String cacheFile) 
      throws IOException {
    String input = "distribute cache content...";
    FileSystem dfs = jobClient.getFs();
    conf = wovenClient.getDaemonConf();
    FileSystem fs = tmpFolderPath.getFileSystem(conf);
    if (!fs.mkdirs(tmpFolderPath)) {
        throw new IOException("Failed to create the temp directory:" 
            + tmpFolderPath.toString());
      }
    deleteCacheFile(new Path(tmpFolderPath, cacheFile));
    DataOutputStream file = fs.create(new Path(tmpFolderPath, cacheFile));
    int i = 0;
    while(i++ < 100) {
      file.writeBytes(input);
    }
    file.close();
    dfs.setPermission(new Path(tmpFolderPath, cacheFile), new FsPermission(FsAction.ALL, 
        FsAction.ALL, FsAction.ALL));
    URI uri = URI.create(new Path(tmpFolderPath, cacheFile).toString());
    return uri;
  }
}
