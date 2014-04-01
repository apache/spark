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

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.util.StringUtils;

import junit.framework.TestCase;

/**
 * This tests the setting of memory limit for streaming processes.
 * This will launch a streaming app which will allocate 10MB memory.
 * First, program is launched with sufficient memory. And test expects
 * it to succeed. Then program is launched with insufficient memory and 
 * is expected to be a failure.  
 */
public class TestUlimit extends TestCase {
  String input = "the dummy input";
  Path inputPath = new Path("/testing/in");
  Path outputPath = new Path("/testing/out");
  String map = null;
  MiniDFSCluster dfs = null;
  MiniMRCluster mr = null;
  FileSystem fs = null;
  private static String SET_MEMORY_LIMIT = "786432"; // 768MB

  String[] genArgs(String memLimit) {
    return new String[] {
      "-input", inputPath.toString(),
      "-output", outputPath.toString(),
      "-mapper", map,
      "-reducer", "org.apache.hadoop.mapred.lib.IdentityReducer",
      "-numReduceTasks", "0",
      "-jobconf", "mapred.map.tasks=1",
      "-jobconf", JobConf.MAPRED_MAP_TASK_ULIMIT + "=" + memLimit,
      "-jobconf", "mapred.job.tracker=" + "localhost:" +
                                           mr.getJobTrackerPort(),
      "-jobconf", "fs.default.name=" + "hdfs://localhost:" 
                   + dfs.getNameNodePort(),
      "-jobconf", "stream.tmpdir=" + 
                   System.getProperty("test.build.data","/tmp")
    };
  }

  /**
   * This tests the setting of memory limit for streaming processes.
   * This will launch a streaming app which will allocate 10MB memory.
   * First, program is launched with sufficient memory. And test expects
   * it to succeed. Then program is launched with insufficient memory and 
   * is expected to be a failure.  
   */
  public void testCommandLine() {
    if (StreamUtil.isCygwin()) {
      return;
    }
    try {
      final int numSlaves = 2;
      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster(conf, numSlaves, true, null);
      fs = dfs.getFileSystem();
      
      mr = new MiniMRCluster(numSlaves, fs.getUri().toString(), 1);
      writeInputFile(fs, inputPath);
      map = StreamUtil.makeJavaCommand(UlimitApp.class, new String[]{});  
      runProgram(SET_MEMORY_LIMIT);
      fs.delete(outputPath, true);
      assertFalse("output not cleaned up", fs.exists(outputPath));
      mr.waitUntilIdle();
    } catch(IOException e) {
      fail(StringUtils.stringifyException(e));
    } finally {
      if (mr != null) {
        mr.shutdown();
      }
      if (dfs != null) {
        dfs.shutdown();
      }
    }
  }

  private void writeInputFile(FileSystem fs, Path dir) throws IOException {
    DataOutputStream out = fs.create(new Path(dir, "part0"));
    out.writeBytes(input);
    out.close();
  }

  /**
   * Runs the streaming program. and asserts the result of the program.
   * @param memLimit memory limit to set for mapred child.
   * @param result Expected result
   * @throws IOException
   */
  private void runProgram(String memLimit) throws IOException {
    boolean mayExit = false;
    StreamJob job = new StreamJob(genArgs(memLimit), mayExit);
    job.go();
    String output = MapReduceTestUtil.readOutput(outputPath,
                                        mr.createJobConf());
    assertEquals("output is wrong", SET_MEMORY_LIMIT,
                                    output.trim());
  }
  
  public static void main(String[]args) throws Exception
  {
    new TestUlimit().testCommandLine();
  }

}
