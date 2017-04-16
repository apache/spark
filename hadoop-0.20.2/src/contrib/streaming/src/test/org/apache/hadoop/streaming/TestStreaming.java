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
 */
public class TestStreaming extends TestCase
{

  // "map" command: grep -E (red|green|blue)
  // reduce command: uniq
  protected File TEST_DIR;
  protected File INPUT_FILE;
  protected File OUTPUT_DIR;
  protected String input = "roses.are.red\nviolets.are.blue\nbunnies.are.pink\n";
  // map behaves like "/usr/bin/tr . \\n"; (split words into lines)
  protected String map = StreamUtil.makeJavaCommand(TrApp.class, new String[]{".", "\\n"});
  // reduce behave like /usr/bin/uniq. But also prepend lines with R.
  // command-line combiner does not have any effect any more.
  protected String reduce = StreamUtil.makeJavaCommand(UniqApp.class, new String[]{"R"});
  protected String outputExpect = "Rare\t\nRblue\t\nRbunnies\t\nRpink\t\nRred\t\nRroses\t\nRviolets\t\n";

  protected StreamJob job;

  public TestStreaming() throws IOException
  {
    UtilTest utilTest = new UtilTest(getClass().getName());
    utilTest.checkUserDir();
    TEST_DIR = new File(getClass().getName()).getAbsoluteFile();
    OUTPUT_DIR = new File(TEST_DIR, "out");
    INPUT_FILE = new File(TEST_DIR, "input.txt");
  }

  protected void createInput() throws IOException
  {
    if (!TEST_DIR.exists()) {
      assertTrue("Creating " + TEST_DIR, TEST_DIR.mkdirs());
    }
    DataOutputStream out = new DataOutputStream(
                                                new FileOutputStream(INPUT_FILE.getAbsoluteFile()));
    out.write(input.getBytes("UTF-8"));
    out.close();
  }

  protected String[] genArgs() {
    return new String[] {
      "-input", INPUT_FILE.getAbsolutePath(),
      "-output", OUTPUT_DIR.getAbsolutePath(),
      "-mapper", map,
      "-reducer", reduce,
      //"-verbose",
      //"-jobconf", "stream.debug=set"
      "-jobconf", "keep.failed.task.files=true",
      "-jobconf", "stream.tmpdir="+System.getProperty("test.build.data","/tmp")
    };
  }
  
  public void testCommandLine() throws Exception
  {
    try {
      try {
        FileUtil.fullyDelete(OUTPUT_DIR.getAbsoluteFile());
      } catch (Exception e) {
      }

      createInput();
      boolean mayExit = false;

      // During tests, the default Configuration will use a local mapred
      // So don't specify -config or -cluster
      job = new StreamJob(genArgs(), mayExit);      
      job.go();
      File outFile = new File(OUTPUT_DIR, "part-00000").getAbsoluteFile();
      String output = StreamUtil.slurp(outFile);
      outFile.delete();
      System.err.println("outEx1=" + outputExpect);
      System.err.println("  out1=" + output);
      assertEquals(outputExpect, output);
    } finally {
      INPUT_FILE.delete();
      FileUtil.fullyDelete(OUTPUT_DIR.getAbsoluteFile());
    }
  }

  public static void main(String[]args) throws Exception
  {
    new TestStreaming().testCommandLine();
  }

}
