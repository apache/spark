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
 * This class tests hadoopStreaming in MapReduce local mode by giving
 * empty input to mapper and the mapper generates nonempty output. Since map()
 * is not called at all, output thread was not getting created and the mapper
 * was hanging forever. Now this issue is solved. Similarly reducer is also
 * checked for task completion with empty input and nonempty output.
 */
public class TestStreamingEmptyInpNonemptyOut extends TestCase
{

  protected File INPUT_FILE = new File("emptyInputFile.txt");
  protected File OUTPUT_DIR = new File("out");
  protected File SCRIPT_FILE = new File("perlScript.pl");

  protected String map = "perlScript.pl";
  protected String reduce = "org.apache.hadoop.mapred.lib.IdentityReducer";
  protected String script = "#!/usr/bin/perl\nfor($count = 1500; $count >= 1; $count--) {print \"$count \";}";

  private StreamJob job;

  public TestStreamingEmptyInpNonemptyOut() throws IOException
  {
    UtilTest utilTest = new UtilTest(getClass().getName());
    utilTest.checkUserDir();
    utilTest.redirectIfAntJunit();
  }

  protected void createInputAndScript() throws IOException
  {
    DataOutputStream out = new DataOutputStream(
                           new FileOutputStream(INPUT_FILE.getAbsoluteFile()));
    out.close();

    out = new DataOutputStream(
          new FileOutputStream(SCRIPT_FILE.getAbsoluteFile()));
    out.write(script.getBytes("UTF-8"));
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
  
  public void testEmptyInputNonemptyOutput() throws IOException
  {
    try {
      try {
        FileUtil.fullyDelete(OUTPUT_DIR.getAbsoluteFile());
      } catch (Exception e) {
      }

      createInputAndScript();
      boolean mayExit = false;

      // During tests, the default Configuration will use a local mapred
      // So don't specify -config or -cluster.
      // First let us test if mapper doesn't hang for empty i/p and nonempty o/p
      job = new StreamJob(genArgs(), mayExit);      
      job.go();
      File outFile = new File(OUTPUT_DIR, "part-00000").getAbsoluteFile();
      outFile.delete();

      // Now let us test if reducer doesn't hang for empty i/p and nonempty o/p
      map = "org.apache.hadoop.mapred.lib.IdentityMapper";
      reduce = "perlScript.pl";
      job = new StreamJob(genArgs(), mayExit);      
      job.go();
      outFile = new File(OUTPUT_DIR, "part-00000").getAbsoluteFile();
      outFile.delete();
    } finally {
      try {
        INPUT_FILE.delete();
        SCRIPT_FILE.delete();
        FileUtil.fullyDelete(OUTPUT_DIR.getAbsoluteFile());
      } catch(Exception e) {
        e.printStackTrace();
      }
   }
  }

  public static void main(String[]args) throws Exception
  {
    new TestStreaming().testCommandLine();
  }

}
