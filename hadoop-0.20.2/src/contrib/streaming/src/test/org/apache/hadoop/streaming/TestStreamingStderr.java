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

import junit.framework.TestCase;
import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Test that streaming consumes stderr from the streaming process
 * (before, during, and after the main processing of mapred input),
 * and that stderr messages count as task progress.
 */
public class TestStreamingStderr extends TestCase
{
  public TestStreamingStderr() throws IOException {
    UtilTest utilTest = new UtilTest(getClass().getName());
    utilTest.checkUserDir();
    utilTest.redirectIfAntJunit();
  }

  protected String[] genArgs(File input, File output, int preLines, int duringLines, int postLines) {
    return new String[] {
      "-input", input.getAbsolutePath(),
      "-output", output.getAbsolutePath(),
      "-mapper", StreamUtil.makeJavaCommand(StderrApp.class,
                                            new String[]{Integer.toString(preLines),
                                                         Integer.toString(duringLines),
                                                         Integer.toString(postLines)}),
      "-reducer", StreamJob.REDUCE_NONE,
      "-jobconf", "keep.failed.task.files=true",
      "-jobconf", "mapred.task.timeout=5000",
      "-jobconf", "stream.tmpdir="+System.getProperty("test.build.data","/tmp")
    };
  }

  protected File setupInput(String base, boolean hasInput) throws IOException {
    File input = new File(base + "-input.txt");
    UtilTest.recursiveDelete(input);
    FileOutputStream in = new FileOutputStream(input.getAbsoluteFile());
    if (hasInput) {
      in.write("hello\n".getBytes());      
    }
    in.close();
    return input;
  }
  
  protected File setupOutput(String base) throws IOException {
    File output = new File(base + "-out");
    UtilTest.recursiveDelete(output);
    return output;
  }

  public void runStreamJob(String baseName, boolean hasInput,
                           int preLines, int duringLines, int postLines)
    throws Exception {
    File input = setupInput(baseName, hasInput);
    File output = setupOutput(baseName);
    boolean mayExit = false;
    int returnStatus = 0;

    StreamJob job = new StreamJob(genArgs(input, output, preLines, duringLines, postLines), mayExit);
    returnStatus = job.go();
    assertEquals("StreamJob success", 0, returnStatus);
  }

  // This test will fail by blocking forever if the stderr isn't
  // consumed by Hadoop for tasks that don't have any input.
  public void testStderrNoInput() throws Exception {
    runStreamJob("stderr-pre", false, 10000, 0, 0);
  }

  // Streaming should continue to read stderr even after all input has
  // been consumed.
  public void testStderrAfterOutput() throws Exception {
    runStreamJob("stderr-post", false, 0, 0, 10000);
  }

  // This test should produce a task timeout if stderr lines aren't
  // counted as progress. This won't actually work until
  // LocalJobRunner supports timeouts.
  public void testStderrCountsAsProgress() throws Exception {
    runStreamJob("stderr-progress", true, 10, 1000, 0);
  }
  
}
