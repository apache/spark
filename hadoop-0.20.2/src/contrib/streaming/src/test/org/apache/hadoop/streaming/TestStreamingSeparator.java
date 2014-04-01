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
 * This class tests hadoopStreaming with customized separator in MapReduce local mode.
 */
public class TestStreamingSeparator extends TestCase
{

  // "map" command: grep -E (red|green|blue)
  // reduce command: uniq
  protected File INPUT_FILE = new File("TestStreamingSeparator.input.txt");
  protected File OUTPUT_DIR = new File("TestStreamingSeparator.out");
  protected String input = "roses1are.red\nviolets1are.blue\nbunnies1are.pink\n";
  // key.value.separator.in.input.line reads 1 as separator
  // stream.map.input.field.separator uses 2 as separator
  // map behaves like "/usr/bin/tr 2 3"; (translate 2 to 3)
  protected String map = StreamUtil.makeJavaCommand(TrApp.class, new String[]{"2", "3"});
  // stream.map.output.field.separator recognize 3 as separator
  // stream.reduce.input.field.separator recognize 3 as separator
  // reduce behaves like "/usr/bin/tr 3 4"; (translate 3 to 4)
  protected String reduce = StreamUtil.makeJavaCommand(TrAppReduce.class, new String[]{"3", "4"});
  // stream.reduce.output.field.separator recognize 4 as separator
  // mapred.textoutputformat.separator outputs 5 as separator
  protected String outputExpect = "bunnies5are.pink\nroses5are.red\nviolets5are.blue\n";

  private StreamJob job;

  public TestStreamingSeparator() throws IOException
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

  protected String[] genArgs() {
    return new String[] {
      "-input", INPUT_FILE.getAbsolutePath(),
      "-output", OUTPUT_DIR.getAbsolutePath(),
      "-mapper", map,
      "-reducer", reduce,
      //"-verbose",
      //"-jobconf", "stream.debug=set"
      "-jobconf", "keep.failed.task.files=true",
      "-jobconf", "stream.tmpdir="+System.getProperty("test.build.data","/tmp"),
      "-inputformat", "KeyValueTextInputFormat",
      "-jobconf", "key.value.separator.in.input.line=1",
      "-jobconf", "stream.map.input.field.separator=2",
      "-jobconf", "stream.map.output.field.separator=3",
      "-jobconf", "stream.reduce.input.field.separator=3",
      "-jobconf", "stream.reduce.output.field.separator=4",
      "-jobconf", "mapred.textoutputformat.separator=5",
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
    new TestStreamingSeparator().testCommandLine();
  }

}
