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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.util.Shell;

import junit.framework.TestCase;

/**
 * This tests the environment set by TT for the child of task jvm.
 * This will launch a streaming job with a shell script as mapper.
 */
public class TestStreamingTaskLog extends TestCase {
  String input = "the dummy input";
  Path inputPath = new Path("inDir");
  Path outputPath = new Path("outDir");
  String map = null;
  MiniMRCluster mr = null;
  FileSystem fs = null;
  final long USERLOG_LIMIT_KB = 5;//consider 5kb as logSize

  String[] genArgs() {
    return new String[] {
      "-input", inputPath.toString(),
      "-output", outputPath.toString(),
      "-mapper", map,
      "-reducer", StreamJob.REDUCE_NONE,
      "-jobconf", "mapred.job.tracker=" + "localhost:" + mr.getJobTrackerPort(),
      "-jobconf", "fs.default.name=" + fs.getUri().toString(),
      "-jobconf", "mapred.map.tasks=1",
      "-jobconf", "keep.failed.task.files=true",
      "-jobconf", "mapred.userlog.limit.kb=" + USERLOG_LIMIT_KB,
      "-jobconf", "stream.tmpdir="+System.getProperty("test.build.data","/tmp")
    };
  }

  /**
   * This test validates the setting of HADOOP_ROOT_LOGGER to 'INFO,TLA' and the
   * dependent properties
   *  (a) hadoop.tasklog.taskid and
   *  (b) hadoop.tasklog.totalLogFileSize
   * for the children of java tasks in streaming jobs.
   */
  public void testStreamingTaskLogWithHadoopCmd() {
    try {
      final int numSlaves = 1;
      Configuration conf = new Configuration();

      fs = FileSystem.getLocal(conf);
      Path testDir = new Path(System.getProperty("test.build.data","/tmp"));
      if (fs.exists(testDir)) {
        fs.delete(testDir, true);
      }
      fs.mkdirs(testDir);
      File scriptFile = createScript(
          testDir.toString() + "/testTaskLog.sh");
      mr = new MiniMRCluster(numSlaves, fs.getUri().toString(), 1);
      
      writeInputFile(fs, inputPath);
      map = scriptFile.getAbsolutePath();
      
      runStreamJobAndValidateEnv();
      
      fs.delete(outputPath, true);
      assertFalse("output not cleaned up", fs.exists(outputPath));
      mr.waitUntilIdle();
    } catch(IOException e) {
      fail(e.toString());
    } finally {
      if (mr != null) {
        mr.shutdown();
      }
    }
  }

  private File createScript(String script) throws IOException {
    File scriptFile = new File(script);
    UtilTest.recursiveDelete(scriptFile);
    FileOutputStream in = new FileOutputStream(scriptFile);
    in.write(("cat > /dev/null 2>&1\n" +
              "echo $HADOOP_ROOT_LOGGER $HADOOP_CLIENT_OPTS").getBytes());
    in.close();
    
    Shell.execCommand(new String[]{"chmod", "+x",
                                   scriptFile.getAbsolutePath()});
    return scriptFile;
  }
  
  private void writeInputFile(FileSystem fs, Path dir) throws IOException {
    DataOutputStream out = fs.create(new Path(dir, "part0"));
    out.writeBytes(input);
    out.close();
  }

  /**
   * Runs the streaming job and validates the output.
   * @throws IOException
   */
  private void runStreamJobAndValidateEnv() throws IOException {
    int returnStatus = -1;
    boolean mayExit = false;
    StreamJob job = new StreamJob(genArgs(), mayExit);
    returnStatus = job.go();
    assertEquals("StreamJob failed.", 0, returnStatus);
    
    // validate environment variables set for the child(script) of java process
    String env = MapReduceTestUtil.readOutput(outputPath, mr.createJobConf());
    long logSize = USERLOG_LIMIT_KB * 1024;
    assertTrue("environment set for child is wrong", env.contains("INFO,TLA")
               && env.contains("-Dhadoop.tasklog.taskid=attempt_")
               && env.contains("-Dhadoop.tasklog.totalLogFileSize=" + logSize)
               && env.contains("-Dhadoop.tasklog.iscleanup=false"));
  }
}
