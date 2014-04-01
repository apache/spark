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
import org.apache.hadoop.util.StringUtils;

/**
 * This class tests if hadoopStreaming fails a job when the mapper or
 * reducers have non-zero exit status and the
 * stream.non.zero.exit.status.is.failure jobconf is set.
 */
public class TestStreamingExitStatus extends TestCase
{
  protected File TEST_DIR =
    new File("TestStreamingExitStatus").getAbsoluteFile();

  protected File INPUT_FILE = new File(TEST_DIR, "input.txt");
  protected File OUTPUT_DIR = new File(TEST_DIR, "out");  

  protected String failingTask = StreamUtil.makeJavaCommand(FailApp.class, new String[]{"true"});
  protected String echoTask = StreamUtil.makeJavaCommand(FailApp.class, new String[]{"false"});

  public TestStreamingExitStatus() throws IOException {
    UtilTest utilTest = new UtilTest(getClass().getName());
    utilTest.checkUserDir();
  }

  protected String[] genArgs(boolean exitStatusIsFailure, boolean failMap) {
    return new String[] {
      "-input", INPUT_FILE.getAbsolutePath(),
      "-output", OUTPUT_DIR.getAbsolutePath(),
      "-mapper", (failMap ? failingTask : echoTask),
      "-reducer", (failMap ? echoTask : failingTask),
      "-jobconf", "keep.failed.task.files=true",
      "-jobconf", "stream.non.zero.exit.is.failure=" + exitStatusIsFailure,
      "-jobconf", "stream.tmpdir="+System.getProperty("test.build.data","/tmp"),
      "-jobconf", "io.sort.mb=10"
    };
  }

  public void setUp() throws IOException {
    UtilTest.recursiveDelete(TEST_DIR);
    assertTrue(TEST_DIR.mkdirs());

    FileOutputStream out = new FileOutputStream(INPUT_FILE.getAbsoluteFile());
    out.write("hello\n".getBytes());
    out.close();
  }

  public void runStreamJob(boolean exitStatusIsFailure, boolean failMap) throws Exception {
    boolean mayExit = false;
    int returnStatus = 0;
    String args[] = genArgs(exitStatusIsFailure, failMap);
    System.err.println("Testing streaming command line:\n" +
               StringUtils.join(" ", Arrays.asList(args)));
    StreamJob job = new StreamJob(genArgs(exitStatusIsFailure, failMap), mayExit);
    returnStatus = job.go();
    
    if (exitStatusIsFailure) {
      assertEquals("Streaming Job failure code expected", /*job not successful:*/1, returnStatus);
    } else {
      assertEquals("Streaming Job expected to succeed", 0, returnStatus);
    }
  }
  
  public void testMapFailOk() throws Exception {
    runStreamJob(false, true);
  }
  
  public void testMapFailNotOk() throws Exception {
    runStreamJob(true, true);
  }
  
  public void testReduceFailOk() throws Exception {
    runStreamJob(false, false);
  }
  
  public void testReduceFailNotOk() throws Exception {
    runStreamJob(true, false);
  }  
  
}
