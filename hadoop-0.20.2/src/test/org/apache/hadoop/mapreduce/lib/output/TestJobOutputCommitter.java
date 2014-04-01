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

package org.apache.hadoop.mapreduce.lib.output;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.mapred.UtilsForTests;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * A JUnit test to test Map-Reduce job committer.
 */
public class TestJobOutputCommitter extends HadoopTestCase {

  public TestJobOutputCommitter() throws IOException {
    super(CLUSTER_MR, LOCAL_FS, 1, 1);
  }

  private static String TEST_ROOT_DIR = new File(System.getProperty(
      "test.build.data", "/tmp")
      + "/" + "test-job-cleanup").toString();
  private static final String CUSTOM_CLEANUP_FILE_NAME = "_custom_cleanup";
  private static final String ABORT_KILLED_FILE_NAME = "_custom_abort_killed";
  private static final String ABORT_FAILED_FILE_NAME = "_custom_abort_failed";
  private static Path inDir = new Path(TEST_ROOT_DIR, "test-input");
  private static int outDirs = 0;
  private FileSystem fs;
  private Configuration conf = null;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    conf = createJobConf();
    conf.setBoolean(FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true);
    fs = getFileSystem();
  }

  @Override
  protected void tearDown() throws Exception {
    fs.delete(new Path(TEST_ROOT_DIR), true);
    super.tearDown();
  }

  /** 
   * Committer with deprecated {@link FileOutputCommitter#cleanupJob(JobContext)}
   * making a _failed/_killed in the output folder
   */
  static class CommitterWithCustomDeprecatedCleanup extends FileOutputCommitter {
    public CommitterWithCustomDeprecatedCleanup(Path outputPath,
        TaskAttemptContext context) throws IOException {
      super(outputPath, context);
    }

    @Override
    public void cleanupJob(JobContext context) throws IOException {
      System.err.println("---- HERE ----");
      Path outputPath = FileOutputFormat.getOutputPath(context);
      FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
      fs.create(new Path(outputPath, CUSTOM_CLEANUP_FILE_NAME)).close();
    }
  }
  
  /**
   * Committer with abort making a _failed/_killed in the output folder
   */
  static class CommitterWithCustomAbort extends FileOutputCommitter {
    public CommitterWithCustomAbort(Path outputPath, TaskAttemptContext context)
        throws IOException {
      super(outputPath, context);
    }

    @Override
    public void abortJob(JobContext context, JobStatus.State state)
        throws IOException {
      Path outputPath = FileOutputFormat.getOutputPath(context);
      FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
      String fileName = 
        (state.equals(JobStatus.State.FAILED)) ? ABORT_FAILED_FILE_NAME
          : ABORT_KILLED_FILE_NAME;
      fs.create(new Path(outputPath, fileName)).close();
    }
  }

  private Path getNewOutputDir() {
    return new Path(TEST_ROOT_DIR, "output-" + outDirs++);
  }

  static class MyOutputFormatWithCustomAbort<K, V> 
  extends TextOutputFormat<K, V> {
    private OutputCommitter committer = null;

    public synchronized OutputCommitter getOutputCommitter(
        TaskAttemptContext context) throws IOException {
      if (committer == null) {
        Path output = getOutputPath(context);
        committer = new CommitterWithCustomAbort(output, context);
      }
      return committer;
    }
  }

  static class MyOutputFormatWithCustomCleanup<K, V> 
  extends TextOutputFormat<K, V> {
    private OutputCommitter committer = null;

    public synchronized OutputCommitter getOutputCommitter(
        TaskAttemptContext context) throws IOException {
      if (committer == null) {
        Path output = getOutputPath(context);
        committer = new CommitterWithCustomDeprecatedCleanup(output, context);
      }
      return committer;
    }
  }

  // run a job with 1 map and let it run to completion
  private void testSuccessfulJob(String filename,
      Class<? extends OutputFormat> output, String[] exclude) throws Exception {
    Path outDir = getNewOutputDir();
    Job job = MapReduceTestUtil.createJob(conf, inDir, outDir, 1, 0);
    job.setOutputFormatClass(output);

    assertTrue("Job failed!", job.waitForCompletion(true));

    Path testFile = new Path(outDir, filename);
    assertTrue("Done file missing for job ", fs.exists(testFile));

    // check if the files from the missing set exists
    for (String ex : exclude) {
      Path file = new Path(outDir, ex);
      assertFalse(
          "File " + file + " should not be present for successful job ", fs
              .exists(file));
    }
  }

  // run a job for which all the attempts simply fail.
  private void testFailedJob(String fileName,
      Class<? extends OutputFormat> output, String[] exclude) throws Exception {
    Path outDir = getNewOutputDir();
    Job job = MapReduceTestUtil.createFailJob(conf, outDir, inDir);
    job.setOutputFormatClass(output);

    assertFalse("Job did not fail!", job.waitForCompletion(true));

    if (fileName != null) {
      Path testFile = new Path(outDir, fileName);
      assertTrue("File " + testFile + " missing for failed job ", fs
          .exists(testFile));
    }

    // check if the files from the missing set exists
    for (String ex : exclude) {
      Path file = new Path(outDir, ex);
      assertFalse("File " + file + " should not be present for failed job ", fs
          .exists(file));
    }
  }

  // run a job which gets stuck in mapper and kill it.
  private void testKilledJob(String fileName,
      Class<? extends OutputFormat> output, String[] exclude) throws Exception {
    Path outDir = getNewOutputDir();
    Job job = MapReduceTestUtil.createKillJob(conf, outDir, inDir);
    job.setOutputFormatClass(output);

    job.submit();

    // wait for the setup to be completed
    while (job.setupProgress() != 1.0f) {
      UtilsForTests.waitFor(100);
    }

    job.killJob(); // kill the job

    assertFalse("Job did not get kill", job.waitForCompletion(true));

    if (fileName != null) {
      Path testFile = new Path(outDir, fileName);
      assertTrue("File " + testFile + " missing for job ", fs.exists(testFile));
    }

    // check if the files from the missing set exists
    for (String ex : exclude) {
      Path file = new Path(outDir, ex);
      assertFalse("File " + file + " should not be present for killed job ", fs
          .exists(file));
    }
  }

  /**
   * Test default cleanup/abort behavior
   * 
   * @throws Exception
   */
  public void testDefaultCleanupAndAbort() throws Exception {
    // check with a successful job
    testSuccessfulJob(FileOutputCommitter.SUCCEEDED_FILE_NAME,
                      TextOutputFormat.class, new String[] {});

    // check with a failed job
    testFailedJob(null, TextOutputFormat.class,
                  new String[] { FileOutputCommitter.SUCCEEDED_FILE_NAME });

    // check default abort job kill
    testKilledJob(null, TextOutputFormat.class,
                  new String[] { FileOutputCommitter.SUCCEEDED_FILE_NAME });
  }

  /**
   * Test if a failed job with custom committer runs the abort code.
   * 
   * @throws Exception
   */
  public void testCustomAbort() throws Exception {
    // check with a successful job
    testSuccessfulJob(FileOutputCommitter.SUCCEEDED_FILE_NAME,
                      MyOutputFormatWithCustomAbort.class, 
                      new String[] {ABORT_FAILED_FILE_NAME, 
                                    ABORT_KILLED_FILE_NAME});

    // check with a failed job
    testFailedJob(ABORT_FAILED_FILE_NAME,  
                  MyOutputFormatWithCustomAbort.class, 
                  new String[] {FileOutputCommitter.SUCCEEDED_FILE_NAME, 
                                ABORT_KILLED_FILE_NAME});

    // check with a killed job
    testKilledJob(ABORT_KILLED_FILE_NAME, 
                  MyOutputFormatWithCustomAbort.class, 
                  new String[] {FileOutputCommitter.SUCCEEDED_FILE_NAME, 
                                ABORT_FAILED_FILE_NAME});
  }

  /**
   * Test if a failed job with custom committer runs the deprecated
   * {@link FileOutputCommitter#cleanupJob(JobContext)} code for api 
   * compatibility testing.
   * @throws Exception 
   */
  public void testCustomCleanup() throws Exception {
    
    // check with a successful job
    testSuccessfulJob(CUSTOM_CLEANUP_FILE_NAME, 
                      MyOutputFormatWithCustomCleanup.class, 
                      new String[] {});

    // check with a failed job
    testFailedJob(CUSTOM_CLEANUP_FILE_NAME, 
                  MyOutputFormatWithCustomCleanup.class,
                  new String[] {FileOutputCommitter.SUCCEEDED_FILE_NAME});

    // check with a killed job
    testKilledJob(CUSTOM_CLEANUP_FILE_NAME,  
                  MyOutputFormatWithCustomCleanup.class,
                  new String[] {FileOutputCommitter.SUCCEEDED_FILE_NAME});
  }
}
