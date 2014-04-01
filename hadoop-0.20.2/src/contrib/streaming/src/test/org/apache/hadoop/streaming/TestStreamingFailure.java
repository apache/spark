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
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

/**
 * This class tests if hadoopStreaming returns Exception 
 * on failure when submitted an invalid/failed job
 * The test case provides an invalid input file for map/reduce job as
 * a unit test case
 */
public class TestStreamingFailure extends TestStreaming
{

  protected File INVALID_INPUT_FILE;// = new File("invalid_input.txt");
  private StreamJob job;

  public TestStreamingFailure() throws IOException
  {
    INVALID_INPUT_FILE = new File("invalid_input.txt");
  }

  protected String[] genArgs() {
    return new String[] {
      "-input", INVALID_INPUT_FILE.getAbsolutePath(),
      "-output", OUTPUT_DIR.getAbsolutePath(),
      "-mapper", map,
      "-reducer", reduce,
      //"-verbose",
      //"-jobconf", "stream.debug=set"
      "-jobconf", "keep.failed.task.files=true",
      "-jobconf", "stream.tmpdir="+System.getProperty("test.build.data","/tmp")
    };
  }

  public void testCommandLine()
  {
    try {
      try {
        FileUtil.fullyDelete(OUTPUT_DIR.getAbsoluteFile());
      } catch (Exception e) {
      }

      boolean mayExit = false;
      int returnStatus = 0;

      // During tests, the default Configuration will use a local mapred
      // So don't specify -config or -cluster
      job = new StreamJob(genArgs(), mayExit);      
      returnStatus = job.go();
      assertEquals("Streaming Job Failure code expected", 5, returnStatus);
    } catch(Exception e) {
      // Expecting an exception
    } finally {
      try {
        FileUtil.fullyDelete(OUTPUT_DIR.getAbsoluteFile());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[]args) throws Exception
  {
      new TestStreamingFailure().testCommandLine();
  }
}
