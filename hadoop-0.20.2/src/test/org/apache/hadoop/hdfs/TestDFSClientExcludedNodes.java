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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.io.OutputStream;

import org.junit.*;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * These tests make sure that DFSClient retries fetching data from DFS
 * properly in case of errors.
 */
public class TestDFSClientExcludedNodes {

  @Test
  public void testExcludedNodes() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 3, true, null);
    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testExcludedNodes");

    // kill a datanode
    cluster.stopDataNode(AppendTestUtil.nextInt(3));
    OutputStream out = fs.create(filePath, true, 4096);
    out.write(20);

    try {
      out.close();
    } catch (Exception e) {
      fail("DataNode failure should not result in a block abort: \n" + e.getMessage());
    }
  }
  
}
