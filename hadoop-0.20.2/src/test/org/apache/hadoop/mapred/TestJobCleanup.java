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
import java.io.File;
import java.io.IOException;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;

/**
 * A JUnit test to test Map-Reduce job cleanup.
 */
public class TestJobCleanup extends TestCase {
  private static String TEST_ROOT_DIR =
      new File(System.getProperty("test.build.data", "/tmp") + "/" 
               + "test-job-cleanup").toString();
  private static final String CUSTOM_CLEANUP_FILE_NAME = 
    "_custom_cleanup";
  private static final String ABORT_KILLED_FILE_NAME = 
    "_custom_abort_killed";
  private static final String ABORT_FAILED_FILE_NAME = 
    "_custom_abort_failed";
  private static FileSystem fileSys = null;
  private static MiniMRCluster mr = null;
  private static Path inDir = null;
  private static Path emptyInDir = null;
  private static int outDirs = 0;
  
  public static Test suite() {
    TestSetup setup = new TestSetup(new TestSuite(TestJobCleanup.class)) {
      protected void setUp() throws Exception {
        JobConf conf = new JobConf();
        fileSys = FileSystem.get(conf);
        fileSys.delete(new Path(TEST_ROOT_DIR), true);
        conf.set("mapred.job.tracker.handler.count", "1");
        conf.set("mapred.job.tracker", "127.0.0.1:0");
        conf.set("mapred.job.tracker.http.address", "127.0.0.1:0");
        conf.set("mapred.task.tracker.http.address", "127.0.0.1:0");

        mr = new MiniMRCluster(1, "file:///", 1, null, null, conf);
        inDir = new Path(TEST_ROOT_DIR, "test-input");
        String input = "The quick brown fox\n" + "has many silly\n"
                       + "red fox sox\n";
        DataOutputStream file = fileSys.create(new Path(inDir, "part-" + 0));
        file.writeBytes(input);
        file.close();
        emptyInDir = new Path(TEST_ROOT_DIR, "empty-input");
        fileSys.mkdirs(emptyInDir);
      }
      
      protected void tearDown() throws Exception {
        if (fileSys != null) {
          fileSys.close();
        }
        if (mr != null) {
          mr.shutdown();
        }
      }
    };
    return setup;
  }
  
  /** 
   * Committer with deprecated {@link FileOutputCommitter#cleanupJob(JobContext)}
   * making a _failed/_killed in the output folder
   */
  static class CommitterWithCustomDeprecatedCleanup extends FileOutputCommitter {
    @Override
    public void cleanupJob(JobContext context) throws IOException {
      System.err.println("---- HERE ----");
      JobConf conf = context.getJobConf();
      Path outputPath = FileOutputFormat.getOutputPath(conf);
      FileSystem fs = outputPath.getFileSystem(conf);
      fs.create(new Path(outputPath, CUSTOM_CLEANUP_FILE_NAME)).close();
    }
  }
  
  /** 
   * Committer with abort making a _failed/_killed in the output folder
   */
  static class CommitterWithCustomAbort extends FileOutputCommitter {
    @Override
    public void abortJob(JobContext context, int state) 
    throws IOException {
      JobConf conf = context.getJobConf();
      Path outputPath = FileOutputFormat.getOutputPath(conf);
      FileSystem fs = outputPath.getFileSystem(conf);
      String fileName = (state == JobStatus.FAILED) 
                        ? TestJobCleanup.ABORT_FAILED_FILE_NAME 
                        : TestJobCleanup.ABORT_KILLED_FILE_NAME;
      fs.create(new Path(outputPath, fileName)).close();
    }
  }
  
  private Path getNewOutputDir() {
    return new Path(TEST_ROOT_DIR, "output-" + outDirs++);
  }
  
  private void configureJob(JobConf jc, String jobName, int maps, int reds, 
                            Path outDir) {
    jc.setJobName(jobName);
    jc.setInputFormat(TextInputFormat.class);
    jc.setOutputKeyClass(LongWritable.class);
    jc.setOutputValueClass(Text.class);
    FileInputFormat.setInputPaths(jc, inDir);
    FileOutputFormat.setOutputPath(jc, outDir);
    jc.setMapperClass(IdentityMapper.class);
    jc.setReducerClass(IdentityReducer.class);
    jc.setNumMapTasks(maps);
    jc.setNumReduceTasks(reds);
    jc.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", true);
  }
  
  // run a job with 1 map and let it run to completion
  private void testSuccessfulJob(String filename, 
    Class<? extends OutputCommitter> committer, String[] exclude) 
  throws IOException {
    JobConf jc = mr.createJobConf();
    Path outDir = getNewOutputDir();
    configureJob(jc, "job with cleanup()", 1, 0, outDir);
    jc.setOutputCommitter(committer);
    
    JobClient jobClient = new JobClient(jc);
    RunningJob job = jobClient.submitJob(jc);
    JobID id = job.getID();
    job.waitForCompletion();
    
    Path testFile = new Path(outDir, filename);
    assertTrue("Done file missing for job " + id, fileSys.exists(testFile));
    
    // check if the files from the missing set exists
    for (String ex : exclude) {
      Path file = new Path(outDir, ex);
      assertFalse("File " + file + " should not be present for successful job " 
                  + id, fileSys.exists(file));
    }
  }
  
  // run a job for which all the attempts simply fail.
  private void testFailedJob(String fileName, 
    Class<? extends OutputCommitter> committer, String[] exclude) 
  throws IOException {
    JobConf jc = mr.createJobConf();
    Path outDir = getNewOutputDir();
    configureJob(jc, "fail job with abort()", 1, 0, outDir);
    jc.setMaxMapAttempts(1);
    // set the job to fail
    jc.setMapperClass(UtilsForTests.FailMapper.class);
    jc.setOutputCommitter(committer);

    JobClient jobClient = new JobClient(jc);
    RunningJob job = jobClient.submitJob(jc);
    JobID id = job.getID();
    job.waitForCompletion();
    
    if (fileName != null) {
      Path testFile = new Path(outDir, fileName);
      assertTrue("File " + testFile + " missing for failed job " + id, 
                 fileSys.exists(testFile));
    }
    
    // check if the files from the missing set exists
    for (String ex : exclude) {
      Path file = new Path(outDir, ex);
      assertFalse("File " + file + " should not be present for failed job "
                  + id, fileSys.exists(file));
    }
  }
  
  // run a job which gets stuck in mapper and kill it.
  private void testKilledJob(String fileName,
    Class<? extends OutputCommitter> committer, String[] exclude) 
  throws IOException {
    JobConf jc = mr.createJobConf();
    Path outDir = getNewOutputDir();
    configureJob(jc, "kill job with abort()", 1, 0, outDir);
    // set the job to wait for long
    jc.setMapperClass(UtilsForTests.KillMapper.class);
    jc.setOutputCommitter(committer);
    
    JobClient jobClient = new JobClient(jc);
    RunningJob job = jobClient.submitJob(jc);
    JobID id = job.getID();
    JobInProgress jip = 
      mr.getJobTrackerRunner().getJobTracker().getJob(job.getID());
    
    // wait for the map to be launched
    while (true) {
      if (jip.runningMaps() == 1) {
        break;
      }
      UtilsForTests.waitFor(100);
    }
    
    job.killJob(); // kill the job
    
    job.waitForCompletion(); // wait for the job to complete
    
    if (fileName != null) {
      Path testFile = new Path(outDir, fileName);
      assertTrue("File " + testFile + " missing for job " + id, 
                 fileSys.exists(testFile));
    }
    
    // check if the files from the missing set exists
    for (String ex : exclude) {
      Path file = new Path(outDir, ex);
      assertFalse("File " + file + " should not be present for killed job "
                  + id, fileSys.exists(file));
    }
  }
  
  /**
   * Test default cleanup/abort behavior
   * 
   * @throws IOException
   */
  public void testDefaultCleanupAndAbort() throws IOException {
    // check with a successful job
    testSuccessfulJob(FileOutputCommitter.SUCCEEDED_FILE_NAME,
                      FileOutputCommitter.class,
                      new String[] {});
    
    // check with a failed job
    testFailedJob(null, 
                  FileOutputCommitter.class, 
                  new String[] {FileOutputCommitter.SUCCEEDED_FILE_NAME});
    
    // check default abort job kill
    testKilledJob(null, 
                  FileOutputCommitter.class, 
                  new String[] {FileOutputCommitter.SUCCEEDED_FILE_NAME});
  }
  
  /**
   * Test if a failed job with custom committer runs the abort code.
   * 
   * @throws IOException
   */
  public void testCustomAbort() throws IOException {
    // check with a successful job
    testSuccessfulJob(FileOutputCommitter.SUCCEEDED_FILE_NAME, 
                      CommitterWithCustomAbort.class,
                      new String[] {ABORT_FAILED_FILE_NAME, 
                                    ABORT_KILLED_FILE_NAME});
    
    // check with a failed job
    testFailedJob(ABORT_FAILED_FILE_NAME, CommitterWithCustomAbort.class, 
                  new String[] {FileOutputCommitter.SUCCEEDED_FILE_NAME,
                                ABORT_KILLED_FILE_NAME});
    
    // check with a killed job
    testKilledJob(ABORT_KILLED_FILE_NAME, CommitterWithCustomAbort.class, 
                  new String[] {FileOutputCommitter.SUCCEEDED_FILE_NAME,
                                ABORT_FAILED_FILE_NAME});
  }

  /**
   * Test if a failed job with custom committer runs the deprecated
   * {@link FileOutputCommitter#cleanupJob(JobContext)} code for api 
   * compatibility testing.
   */
  public void testCustomCleanup() throws IOException {
    // check with a successful job
    testSuccessfulJob(CUSTOM_CLEANUP_FILE_NAME, 
                      CommitterWithCustomDeprecatedCleanup.class,
                      new String[] {});
    
    // check with a failed job
    testFailedJob(CUSTOM_CLEANUP_FILE_NAME, 
                  CommitterWithCustomDeprecatedCleanup.class, 
                  new String[] {FileOutputCommitter.SUCCEEDED_FILE_NAME});
    
    // check with a killed job
    testKilledJob(TestJobCleanup.CUSTOM_CLEANUP_FILE_NAME, 
                  CommitterWithCustomDeprecatedCleanup.class, 
                  new String[] {FileOutputCommitter.SUCCEEDED_FILE_NAME});
  }
}
