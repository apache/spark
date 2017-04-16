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
import org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner;

/**
 * This class tests hadoopStreaming in MapReduce local mode.
 */
public class TestStreamDataProtocol extends TestCase
{

  // "map" command: grep -E (red|green|blue)
  // reduce command: uniq
  protected File INPUT_FILE = new File("input_for_data_protocol_test.txt");
  protected File OUTPUT_DIR = new File("out_for_data_protocol_test");
  protected String input = "roses.smell.good\nroses.look.good\nroses.need.care\nroses.attract.bees\nroses.are.red\nroses.are.not.blue\nbunnies.are.pink\nbunnies.run.fast\nbunnies.have.short.tail\nbunnies.have.long.ears\n";
  // map behaves like "/usr/bin/cat"; 
  protected String map = StreamUtil.makeJavaCommand(TrApp.class, new String[]{".", "."});
  // reduce counts the number of values for each key
  protected String reduce = "org.apache.hadoop.streaming.ValueCountReduce";
  protected String outputExpect = "bunnies.are\t1\nbunnies.have\t2\nbunnies.run\t1\nroses.are\t2\nroses.attract\t1\nroses.look\t1\nroses.need\t1\nroses.smell\t1\n";

  private StreamJob job;

  public TestStreamDataProtocol() throws IOException
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
      "-partitioner", KeyFieldBasedPartitioner.class.getCanonicalName(),
      //"-verbose",
      "-jobconf", "stream.map.output.field.separator=.",
      "-jobconf", "stream.num.map.output.key.fields=2",
      "-jobconf", "map.output.key.field.separator=.",
      "-jobconf", "num.key.fields.for.partition=1",
      "-jobconf", "mapred.reduce.tasks=2",
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
      System.err.println("  equals=" + outputExpect.compareTo(output));
      assertEquals(outputExpect, output);
    } finally {
      INPUT_FILE.delete();
      FileUtil.fullyDelete(OUTPUT_DIR.getAbsoluteFile());
    }
  }

  public static void main(String[]args) throws Exception
  {
    new TestStreamDataProtocol().testCommandLine();
  }

}
