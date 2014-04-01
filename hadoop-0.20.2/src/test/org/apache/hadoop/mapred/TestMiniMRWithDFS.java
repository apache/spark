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

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.security.auth.login.LoginException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

/**
 * A JUnit test to test Mini Map-Reduce Cluster with Mini-DFS.
 */
public class TestMiniMRWithDFS extends TestCase {
  private static final Log LOG =
    LogFactory.getLog(TestMiniMRWithDFS.class.getName());
  
  static final int NUM_MAPS = 10;
  static final int NUM_SAMPLES = 100000;
  
  public static class TestResult {
    public String output;
    public RunningJob job;
    TestResult(RunningJob job, String output) {
      this.job = job;
      this.output = output;
    }
  }
  public static TestResult launchWordCount(JobConf conf,
                                           Path inDir,
                                           Path outDir,
                                           String input,
                                           int numMaps,
                                           int numReduces) throws IOException {
    FileSystem inFs = inDir.getFileSystem(conf);
    FileSystem outFs = outDir.getFileSystem(conf);
    outFs.delete(outDir, true);
    if (!inFs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    {
      DataOutputStream file = inFs.create(new Path(inDir, "part-0"));
      file.writeBytes(input);
      file.close();
    }
    conf.setJobName("wordcount");
    conf.setInputFormat(TextInputFormat.class);
    
    // the keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    conf.setOutputValueClass(IntWritable.class);
    
    conf.setMapperClass(WordCount.MapClass.class);        
    conf.setCombinerClass(WordCount.Reduce.class);
    conf.setReducerClass(WordCount.Reduce.class);
    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.setNumMapTasks(numMaps);
    conf.setNumReduceTasks(numReduces);
    RunningJob job = JobClient.runJob(conf);
    return new TestResult(job, MapReduceTestUtil.readOutput(outDir, conf));
  }

  /**
   * Make sure that there are exactly the directories that we expect to find.
   * 
   * <br/>
   * <br/>
   * 
   * For e.g., if we want to check the existence of *only* the directories for
   * user1's tasks job1-attempt1, job1-attempt2, job2-attempt1, we pass user1 as
   * user, {job1, job1, job2, job3} as jobIds and {attempt1, attempt2, attempt1,
   * attempt3} as taskDirs.
   * 
   * @param mr the map-reduce cluster
   * @param user the name of the job-owner
   * @param jobIds the list of jobs
   * @param taskDirs the task ids that should be present
   */
  static void checkTaskDirectories(MiniMRCluster mr, String user,
      String[] jobIds, String[] taskDirs) {

    mr.waitUntilIdle();
    int trackers = mr.getNumTaskTrackers();

    List<String> observedJobDirs = new ArrayList<String>();
    List<String> observedFilesInsideJobDir = new ArrayList<String>();

    for (int i = 0; i < trackers; ++i) {

      // Verify that mapred-local-dir and it's direct contents are valid
      File localDir = new File(mr.getTaskTrackerLocalDir(i));
      assertTrue("Local dir " + localDir + " does not exist.", localDir
          .isDirectory());
      LOG.info("Verifying contents of mapred.local.dir "
          + localDir.getAbsolutePath());
      // Verify contents(user-dir) of tracker-sub-dir
      File trackerSubDir = new File(localDir, TaskTracker.SUBDIR);
      if (trackerSubDir.isDirectory()) {

        // Verify contents of user-dir and populate the job-dirs/attempt-dirs
        // lists
        File userDir = new File(trackerSubDir, user);
        if (userDir.isDirectory()) {
          LOG.info("Verifying contents of user-dir "
              + userDir.getAbsolutePath());
          verifyContents(new String[] { TaskTracker.JOBCACHE,
              TaskTracker.DISTCACHEDIR }, userDir.list());

          File jobCacheDir =
              new File(localDir, TaskTracker.getJobCacheSubdir(user));
          String[] jobDirs = jobCacheDir.list();
          observedJobDirs.addAll(Arrays.asList(jobDirs));

          for (String jobDir : jobDirs) {
            String[] attemptDirs = new File(jobCacheDir, jobDir).list();
            observedFilesInsideJobDir.addAll(Arrays.asList(attemptDirs));
          }
        }
      }
    }

    // Now verify that only expected job-dirs and attempt-dirs are present.
    LOG.info("Verifying the list of job directories");
    verifyContents(jobIds, observedJobDirs.toArray(new String[observedJobDirs
        .size()]));
    LOG.info("Verifying the list of task directories");
    // All taskDirs should be present in the observed list. Other files like
    // job.xml etc may be present too, we are not checking them here.
    for (int j = 0; j < taskDirs.length; j++) {
      assertTrue(
          "Expected task-directory " + taskDirs[j] + " is not present!",
          observedFilesInsideJobDir.contains(taskDirs[j]));
    }
  }

  /**
   * Check the list of expectedFiles against the list of observedFiles and make
   * sure they both are the same. Duplicates can be present in either of the
   * lists and all duplicate entries are treated as a single entity.
   * 
   * @param expectedFiles
   * @param observedFiles
   */
  private static void verifyContents(String[] expectedFiles,
      String[] observedFiles) {
    boolean[] foundExpectedFiles = new boolean[expectedFiles.length];
    boolean[] validObservedFiles = new boolean[observedFiles.length];
    for (int j = 0; j < observedFiles.length; ++j) {
      for (int k = 0; k < expectedFiles.length; ++k) {
        if (expectedFiles[k].equals(observedFiles[j])) {
          foundExpectedFiles[k] = true;
          validObservedFiles[j] = true;
        }
      }
    }
    for (int j = 0; j < foundExpectedFiles.length; j++) {
      assertTrue("Expected file " + expectedFiles[j] + " not found",
          foundExpectedFiles[j]);
    }
    for (int j = 0; j < validObservedFiles.length; j++) {
      assertTrue("Unexpected file " + observedFiles[j] + " found",
          validObservedFiles[j]);
    }
  }

  public static void runPI(MiniMRCluster mr, JobConf jobconf) throws IOException {
    LOG.info("runPI");
    double estimate = org.apache.hadoop.examples.PiEstimator.estimate(
        NUM_MAPS, NUM_SAMPLES, jobconf).doubleValue();
    double error = Math.abs(Math.PI - estimate);
    assertTrue("Error in PI estimation "+error+" exceeds 0.01", (error < 0.01));
    String userName = UserGroupInformation.getLoginUser().getUserName();
    checkTaskDirectories(mr, userName, new String[] {}, new String[] {});
  }

  public static void runWordCount(MiniMRCluster mr, JobConf jobConf) 
  throws IOException {
    LOG.info("runWordCount");
    // Run a word count example
    // Keeping tasks that match this pattern
    String pattern = 
      TaskAttemptID.getTaskAttemptIDsPattern(null, null, true, 1, null);
    jobConf.setKeepTaskFilesPattern(pattern);
    TestResult result;
    final Path inDir = new Path("./wc/input");
    final Path outDir = new Path("./wc/output");
    String input = "The quick brown fox\nhas many silly\nred fox sox\n";
    result = launchWordCount(jobConf, inDir, outDir, input, 3, 1);
    assertEquals("The\t1\nbrown\t1\nfox\t2\nhas\t1\nmany\t1\n" +
                 "quick\t1\nred\t1\nsilly\t1\nsox\t1\n", result.output);
    JobID jobid = result.job.getID();
    TaskAttemptID taskid = new TaskAttemptID(
        new TaskID(jobid, true, 1),0);
    String userName = UserGroupInformation.getLoginUser().getUserName();
    checkTaskDirectories(mr, userName, new String[] { jobid.toString() },
        new String[] { taskid.toString() });
    // test with maps=0
    jobConf = mr.createJobConf();
    input = "owen is oom";
    result = launchWordCount(jobConf, inDir, outDir, input, 0, 1);
    assertEquals("is\t1\noom\t1\nowen\t1\n", result.output);
    Counters counters = result.job.getCounters();
    long hdfsRead = 
      counters.findCounter(Task.FILESYSTEM_COUNTER_GROUP, 
          Task.getFileSystemCounterNames("hdfs")[0]).getCounter();
    long hdfsWrite = 
      counters.findCounter(Task.FILESYSTEM_COUNTER_GROUP, 
          Task.getFileSystemCounterNames("hdfs")[1]).getCounter();
    long rawSplitBytesRead =
      counters.findCounter(Task.Counter.SPLIT_RAW_BYTES).getCounter();
    assertEquals(result.output.length(), hdfsWrite);
    assertEquals(input.length() + rawSplitBytesRead, hdfsRead);

    // Run a job with input and output going to localfs even though the 
    // default fs is hdfs.
    {
      FileSystem localfs = FileSystem.getLocal(jobConf);
      String TEST_ROOT_DIR =
        new File(System.getProperty("test.build.data","/tmp"))
        .toString().replace(' ', '+');
      Path localIn = localfs.makeQualified
                        (new Path(TEST_ROOT_DIR + "/local/in"));
      Path localOut = localfs.makeQualified
                        (new Path(TEST_ROOT_DIR + "/local/out"));
      result = launchWordCount(jobConf, localIn, localOut,
                               "all your base belong to us", 1, 1);
      assertEquals("all\t1\nbase\t1\nbelong\t1\nto\t1\nus\t1\nyour\t1\n", 
                   result.output);
      assertTrue("outputs on localfs", localfs.exists(localOut));

    }
  }

  public void testWithDFS() throws IOException {
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;
    try {
      final int taskTrackers = 4;

      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster(conf, 4, true, null);
      fileSys = dfs.getFileSystem();
      mr = new MiniMRCluster(taskTrackers, fileSys.getUri().toString(), 1);
      // make cleanup inline sothat validation of existence of these directories
      // can be done
      mr.setInlineCleanupThreads();

      runPI(mr, mr.createJobConf());
      runWordCount(mr, mr.createJobConf());
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown();
      }
    }
  }
  
  public void testWithDFSWithDefaultPort() throws IOException {
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;
    try {
      final int taskTrackers = 4;

      Configuration conf = new Configuration();
      // start a dfs with the default port number
      dfs = new MiniDFSCluster(
          NameNode.DEFAULT_PORT, conf, 4, true, true, null, null);
      fileSys = dfs.getFileSystem();
      mr = new MiniMRCluster(taskTrackers, fileSys.getUri().toString(), 1);

      JobConf jobConf = mr.createJobConf();
      TestResult result;
      final Path inDir = new Path("./wc/input");
      final Path outDir = new Path("hdfs://" +
          dfs.getNameNode().getNameNodeAddress().getHostName() +
          ":" + NameNode.DEFAULT_PORT +"/./wc/output");
      String input = "The quick brown fox\nhas many silly\nred fox sox\n";
      result = launchWordCount(jobConf, inDir, outDir, input, 3, 1);
      assertEquals("The\t1\nbrown\t1\nfox\t2\nhas\t1\nmany\t1\n" +
                   "quick\t1\nred\t1\nsilly\t1\nsox\t1\n", result.output);
      final Path outDir2 = new Path("hdfs:/test/wc/output2");
      jobConf.set("fs.default.name", "hdfs://localhost:" + NameNode.DEFAULT_PORT);
      result = launchWordCount(jobConf, inDir, outDir2, input, 3, 1);
      assertEquals("The\t1\nbrown\t1\nfox\t2\nhas\t1\nmany\t1\n" +
                   "quick\t1\nred\t1\nsilly\t1\nsox\t1\n", result.output);
    } catch (java.net.BindException be) {
      LOG.info("Skip the test this time because can not start namenode on port "
          + NameNode.DEFAULT_PORT, be);
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown();
      }
    }
  }
}
