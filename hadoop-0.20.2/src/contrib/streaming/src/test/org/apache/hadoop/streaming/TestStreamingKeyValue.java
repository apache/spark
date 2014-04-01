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

import org.apache.hadoop.fs.FileUtil;

/**
 * This class tests hadoopStreaming in MapReduce local mode.
 * This testcase looks at different cases of tab position in input. 
 */
public class TestStreamingKeyValue extends TestCase
{
  protected File INPUT_FILE = new File("input.txt");
  protected File OUTPUT_DIR = new File("stream_out");
  // First line of input has 'key' 'tab' 'value'
  // Second line of input starts with a tab character. 
  // So, it has empty key and the whole line as value.
  // Third line of input does not have any tab character.
  // So, the whole line is the key and value is empty.
  protected String input = 
    "roses are \tred\t\n\tviolets are blue\nbunnies are pink\n" +
    "this is for testing a big\tinput line\n" +
    "small input\n";
  private final static String outputWithoutKey = 
    "\tviolets are blue\nbunnies are pink\t\n" + 
    "roses are \tred\t\n" +
    "small input\t\n" +
    "this is for testing a big\tinput line\n";
  private final static String outputWithKey = 
    "0\troses are \tred\t\n" +  
    "16\t\tviolets are blue\n" +
    "34\tbunnies are pink\n" +
    "51\tthis is for testing a big\tinput line\n" +
    "88\tsmall input\n";

  private StreamJob job;

  public TestStreamingKeyValue() throws IOException
  {
    UtilTest utilTest = new UtilTest(getClass().getName());
    utilTest.checkUserDir();
    utilTest.redirectIfAntJunit();
  }

  protected void createInput() throws IOException
  {
    DataOutputStream out = new DataOutputStream(
       new FileOutputStream(INPUT_FILE.getAbsoluteFile()));
    out.write(input.getBytes("UTF-8"));
    out.close();
  }

  protected String[] genArgs(boolean ignoreKey) {
    return new String[] {
      "-input", INPUT_FILE.getAbsolutePath(),
      "-output", OUTPUT_DIR.getAbsolutePath(),
      "-mapper", "cat",
      "-jobconf", "keep.failed.task.files=true",
      "-jobconf", "stream.non.zero.exit.is.failure=true",
      "-jobconf", "stream.tmpdir="+System.getProperty("test.build.data","/tmp"),
      "-jobconf", "stream.map.input.ignoreKey="+ignoreKey,
    };
  }
  
  public void runStreamJob(final String outputExpect, boolean ignoreKey)
      throws Exception {
    String outFileName = "part-00000";
    File outFile = null;
    try {
      try {
        FileUtil.fullyDelete(OUTPUT_DIR.getAbsoluteFile());
      } catch (Exception e) {
      }

      createInput();
      boolean mayExit = false;

      // During tests, the default Configuration will use a local mapred
      // So don't specify -config or -cluster
      job = new StreamJob(genArgs(ignoreKey), mayExit);      
      job.go();
      outFile = new File(OUTPUT_DIR, outFileName).getAbsoluteFile();
      String output = StreamUtil.slurp(outFile);
      System.err.println("outEx1=" + outputExpect);
      System.err.println("  out1=" + output);
      assertEquals(outputExpect, output);
    } finally {
      INPUT_FILE.delete();
      FileUtil.fullyDelete(OUTPUT_DIR.getAbsoluteFile());
    }
  }

  /**
   * Run the job with the indicating the input format key should be emitted. 
   */
  public void testCommandLineWithKey() throws Exception
  {
    runStreamJob(outputWithKey, false);
  }

  /**
   * Run the job the default way (the input format key is not emitted).
   */
  public void testCommandLineWithoutKey() throws Exception
  {
      runStreamJob(outputWithoutKey, true);
  }
  
  public static void main(String[]args) throws Exception
  {    
    new TestStreamingKeyValue().testCommandLineWithKey();
    new TestStreamingKeyValue().testCommandLineWithoutKey();
  }
}
