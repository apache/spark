/* Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.hadoop.mapred;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class TestCustomOutputCommitter extends TestCase {
  static final Path input = new Path("/test/input/");
  static final Path output = new Path("/test/output");
  
  public void testCommitter() throws Exception {
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fs = null;
    Path testFile = new Path(input, "testfile");
    try {
      Configuration conf = new Configuration();

      //start the mini mr and dfs cluster.
      dfs = new MiniDFSCluster(conf, 2 , true, null);
      fs = dfs.getFileSystem();
      FSDataOutputStream stream = fs.create(testFile);
      stream.write("teststring".getBytes());
      stream.close();

      mr = new MiniMRCluster(2, fs.getUri().toString(), 1);

      String[] args = new String[6];
      args[0] = "-libjars";
      // the testjob.jar as a temporary jar file 
      // holding custom output committer
      args[1] = "build/test/testjar/testjob.jar";
      args[2] = "-D";
      args[3] = "mapred.output.committer.class=testjar.CustomOutputCommitter";
      args[4] = input.toString();
      args[5] = output.toString();
      JobConf jobConf = mr.createJobConf();
      int ret = ToolRunner.run(jobConf, new WordCount(), args);

      assertTrue("not failed ", ret == 0);
    } finally {
      if (dfs != null) {dfs.shutdown();};
      if (mr != null) {mr.shutdown();};
    }
  }
}
