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

package org.apache.hadoop.streaming;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapred.TestMiniMRMapRedDebugScript;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;

/**
  * Tests if mapper/reducer with empty/nonempty input works properly if
  * reporting is done using lines like "reporter:status:" and
  * "reporter:counter:" before map()/reduce() method is called.
  * Validates the task's log of STDERR if messages are written to stderr before
  * map()/reduce() is called.
  * Also validates job output.
  * Uses MiniMR since the local jobtracker doesn't track task status. 
 */
public class TestStreamingStatus {
   protected static String TEST_ROOT_DIR =
     new File(System.getProperty("test.build.data","/tmp"),
     TestStreamingStatus.class.getSimpleName())
    .toURI().toString().replace(' ', '+');
  protected String INPUT_FILE = TEST_ROOT_DIR + "/input.txt";
  protected String OUTPUT_DIR = TEST_ROOT_DIR + "/out";
  protected String input = "roses.are.red\nviolets.are.blue\nbunnies.are.pink\n";
  protected String map = null;
  protected String reduce = null;
    
  protected String scriptFile = TEST_ROOT_DIR + "/perlScript.pl";
  protected String scriptFileName = new Path(scriptFile).toUri().getPath();


  String expectedStderr = "my error msg before consuming input\n" +
      "my error msg after consuming input\n";
  String expectedOutput = null;// inited in setUp()
  String expectedStatus = "before consuming input";

  // This script does the following
  // (a) setting task status before reading input
  // (b) writing to stderr before reading input and after reading input
  // (c) writing to stdout before reading input
  // (d) incrementing user counter before reading input and after reading input
  // Write lines to stdout before reading input{(c) above} is to validate
  // the hanging task issue when input to task is empty(because of not starting
  // output thread).
  protected String script =
    "#!/usr/bin/perl\n" +
    "print STDERR \"reporter:status:" + expectedStatus + "\\n\";\n" +
    "print STDERR \"reporter:counter:myOwnCounterGroup,myOwnCounter,1\\n\";\n" +
    "print STDERR \"my error msg before consuming input\\n\";\n" +
    "for($count = 1500; $count >= 1; $count--) {print STDOUT \"$count \";}" +
    "while(<STDIN>) {chomp;}\n" +
    "print STDERR \"my error msg after consuming input\\n\";\n" +
    "print STDERR \"reporter:counter:myOwnCounterGroup,myOwnCounter,1\\n\";\n";

  MiniMRCluster mr = null;
  FileSystem fs = null;
  JobConf conf = null;

  /**
   * Start the cluster and create input file before running the actual test.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    conf = new JobConf();

    mr = new MiniMRCluster(1, "file:///", 3, null , null, conf);

    Path inFile = new Path(INPUT_FILE);
    fs = inFile.getFileSystem(mr.createJobConf());
    clean(fs);

    buildExpectedJobOutput();
  }

  /**
   * Kill the cluster after the test is done.
   */
  @After
  public void tearDown() {
    if (fs != null) { clean(fs); }
    if (mr != null) { mr.shutdown(); }
  }

  // Updates expectedOutput to have the expected job output as a string
  void buildExpectedJobOutput() {
    if (expectedOutput == null) {
      expectedOutput = "";
      for(int i = 1500; i >= 1; i--) {
        expectedOutput = expectedOutput.concat(Integer.toString(i) + " ");
      }
      expectedOutput = expectedOutput.trim();
    }
  }

  // Create empty/nonempty input file.
  // Create script file with the specified content.
  protected void createInputAndScript(boolean isEmptyInput,
      String script) throws IOException {
    makeInput(fs, isEmptyInput ? "" : input);

    // create script file
    DataOutputStream file = fs.create(new Path(scriptFileName));
    file.writeBytes(script);
    file.close();
  }

  protected String[] genArgs(int jobtrackerPort, String mapper, String reducer)
  {
    return new String[] {
      "-input", INPUT_FILE,
      "-output", OUTPUT_DIR,
      "-mapper", mapper,
      "-reducer", reducer,
      "-jobconf", "mapred.map.tasks=1",
      "-jobconf", "mapred.reduce.tasks=1",
      "-jobconf", "keep.failed.task.files=true",
      "-jobconf", "stream.tmpdir=" + new Path(TEST_ROOT_DIR).toUri().getPath(),
      "-jobconf", "mapred.job.tracker=localhost:"+jobtrackerPort,
      "-jobconf", "fs.default.name=file:///"
    };
  }

  // create input file with the given content
  public void makeInput(FileSystem fs, String input) throws IOException {
    Path inFile = new Path(INPUT_FILE);
    DataOutputStream file = fs.create(inFile);
    file.writeBytes(input);
    file.close();
  }

  // Delete output directory
  protected void deleteOutDir(FileSystem fs) {
    try {
      Path outDir = new Path(OUTPUT_DIR);
      fs.delete(outDir, true);
    } catch (Exception e) {}
  }

  // Delete input file, script file and output directory
  public void clean(FileSystem fs) {
    deleteOutDir(fs);
    try {
      Path file = new Path(INPUT_FILE);
      if (fs.exists(file)) {
        fs.delete(file, false);
      }
      file = new Path(scriptFile);
      if (fs.exists(file)) {
        fs.delete(file, false);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Check if mapper/reducer with empty/nonempty input works properly if
   * reporting is done using lines like "reporter:status:" and
   * "reporter:counter:" before map()/reduce() method is called.
   * Validate the task's log of STDERR if messages are written
   * to stderr before map()/reduce() is called.
   * Also validate job output.
   *
   * @throws IOException
   */
  @Test
  public void testReporting() throws Exception {
    testStreamJob(false);// nonempty input
    testStreamJob(true);// empty input
  }

  /**
   * Run a streaming job with the given script as mapper and validate.
   * Run another streaming job with the given script as reducer and validate.
   *
   * @param isEmptyInput Should the input to the script be empty ?
   * @param script The content of the script that will run as the streaming task
   */
  private void testStreamJob(boolean isEmptyInput)
      throws IOException {

      createInputAndScript(isEmptyInput, script);

      // Check if streaming mapper works as expected
      map = scriptFileName;
      reduce = "/bin/cat";
      runStreamJob(TaskType.MAP, isEmptyInput);
      deleteOutDir(fs);

      // Check if streaming reducer works as expected.
      map = "/bin/cat";
      reduce = scriptFileName;
      runStreamJob(TaskType.REDUCE, isEmptyInput);
      clean(fs);
  }

  // Run streaming job for the specified input file, mapper and reducer and
  // (1) Validate if the job succeeds.
  // (2) Validate if user counter is incremented properly for the cases of
  //   (a) nonempty input to map
  //   (b) empty input to map and
  //   (c) nonempty input to reduce
  // (3) Validate task status for the cases of (2)(a),(2)(b),(2)(c).
  //     Because empty input to reduce task => reporter is dummy and ignores
  //     all "reporter:status" and "reporter:counter" lines. 
  // (4) Validate stderr of task of given task type.
  // (5) Validate job output
  void runStreamJob(TaskType type, boolean isEmptyInput) throws IOException {
    boolean mayExit = false;
    StreamJob job = new StreamJob(genArgs(
        mr.getJobTrackerPort(), map, reduce), mayExit);
    int returnValue = job.go();
    assertEquals(0, returnValue);

    // If input to reducer is empty, dummy reporter(which ignores all
    // reporting lines) is set for MRErrorThread in waitOutputThreads(). So
    // expectedCounterValue is 0 for empty-input-to-reducer case.
    // Output of reducer is also empty for empty-input-to-reducer case.
    int expectedCounterValue = 0;
    if (type == TaskType.MAP || !isEmptyInput) {
      validateTaskStatus(job, type);
      // output is from "print STDOUT" statements in perl script
      validateJobOutput(job.getConf());
      expectedCounterValue = 2;
    }
    validateUserCounter(job, expectedCounterValue);
    validateTaskStderr(job, type);

    deleteOutDir(fs);
  }

  // validate task status of task of given type(validates 1st task of that type)
  void validateTaskStatus(StreamJob job, TaskType type) throws IOException {
    // Map Task has 1 phase: map (note that in 0.21 onwards it has a sort phase too)
    // Reduce Task has 3 phases: copy, sort, reduce
    String finalPhaseInTask = null;
    TaskReport[] reports;
    if (type == TaskType.MAP) {
      reports = job.jc_.getMapTaskReports(job.jobId_);
    } else {// reduce task
      reports = job.jc_.getReduceTaskReports(job.jobId_);
      finalPhaseInTask = "reduce";
    }
    assertEquals(1, reports.length);
    assertEquals(expectedStatus +
        (finalPhaseInTask == null ? "" : " > " + finalPhaseInTask),
        reports[0].getState());
  }

  // Validate the job output
  void validateJobOutput(Configuration conf)
      throws IOException {

    String output = MapReduceTestUtil.readOutput(
        new Path(OUTPUT_DIR), conf).trim();

    assertTrue(output.equals(expectedOutput));
  }

  // Validate stderr task log of given task type(validates 1st
  // task of that type).
  void validateTaskStderr(StreamJob job, TaskType type)
      throws IOException {
    TaskAttemptID attemptId =
        new TaskAttemptID(new TaskID(job.jobId_, type == TaskType.MAP, 0), 0);

    String log = TestMiniMRMapRedDebugScript.readTaskLog(TaskLog.LogName.STDERR,
        attemptId, false);

    // trim() is called on expectedStderr here because the method
    // MapReduceTestUtil.readTaskLog() returns trimmed String.
    assertTrue(log.equals(expectedStderr.trim()));
  }

  // Validate if user counter is incremented properly
  void validateUserCounter(StreamJob job, int expectedCounterValue)
      throws IOException {
    Counters counters = job.running_.getCounters();
    assertEquals(expectedCounterValue, counters.findCounter(
        "myOwnCounterGroup", "myOwnCounter").getValue());
  }
}
