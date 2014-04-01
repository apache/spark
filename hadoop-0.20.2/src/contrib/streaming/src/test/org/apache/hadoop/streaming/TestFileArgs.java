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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.zip.GZIPOutputStream;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.MiniMRCluster;

/**
 * This class tests that the '-file' argument to streaming results
 * in files being unpacked in the job working directory.
 */
public class TestFileArgs extends TestStreaming
{
  private MiniMRCluster mr = null;
  private FileSystem fileSys = null;
  private String strJobTracker = null;
  private Configuration conf = null;
  private MiniDFSCluster dfs = null;
  private String strNamenode = null;
  private String namenode = null;

  private File SIDE_FILE;
  
  private static final String LS_PATH = "/bin/ls";
  
  public TestFileArgs() throws IOException {
    super();
    outputExpect = "job.jar\t\nsidefile\t\ntmp\t\n";

    conf = new Configuration();
    dfs = new MiniDFSCluster(conf, 1, true, null);
    fileSys = dfs.getFileSystem();
    namenode = fileSys.getUri().getAuthority();
    mr  = new MiniMRCluster(1, namenode, 1);
    strJobTracker = "mapred.job.tracker=" + "localhost:" + mr.getJobTrackerPort();
    strNamenode = "fs.default.name=" + namenode;
  }

  @Override
  protected void createInput() throws IOException {
    // Since ls doesn't read stdin, we don't want to write anything
    // to it, or else we risk Broken Pipe exceptions.
    input = "";
    super.createInput();

    SIDE_FILE = new File(TEST_DIR, "sidefile");
    DataOutputStream dos = new DataOutputStream(
      new FileOutputStream(SIDE_FILE.getAbsoluteFile()));
    dos.write("hello world\n".getBytes("UTF-8"));
    dos.close();
  }

  @Override
  protected String[] genArgs() {
    return new String[] {
      "-input", "file://" + INPUT_FILE.getAbsolutePath(),
      "-output", "file://" + OUTPUT_DIR.getAbsolutePath(),
      "-file", SIDE_FILE.getAbsolutePath(),
      "-mapper", LS_PATH,
      "-numReduceTasks", "0",
      "-jobconf", strNamenode,
      "-jobconf", strJobTracker,
      "-jobconf", "stream.tmpdir=" + System.getProperty("test.build.data","/tmp")
    };
  }


  public static void main(String[]args) throws Exception
  {
    new TestFileArgs().testCommandLine();
  }

}
