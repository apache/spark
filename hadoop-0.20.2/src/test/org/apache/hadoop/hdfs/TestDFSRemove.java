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

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;

public class TestDFSRemove extends junit.framework.TestCase {
  static int countLease(MiniDFSCluster cluster) {
    return cluster.getNameNode().namesystem.leaseManager.countLease();
  }
  
  final Path dir = new Path("/test/remove/");

  void list(FileSystem fs, String name) throws IOException {
    FileSystem.LOG.info("\n\n" + name);
    for(FileStatus s : fs.listStatus(dir)) {
      FileSystem.LOG.info("" + s.getPath());
    }
  }

  static void createFile(FileSystem fs, Path f) throws IOException {
    DataOutputStream a_out = fs.create(f);
    a_out.writeBytes("something");
    a_out.close();
  }
  
  static long getTotalDfsUsed(MiniDFSCluster cluster) throws IOException {
    long total = 0;
    for(DataNode node : cluster.getDataNodes()) {
      total += node.getFSDataset().getDfsUsed();
    }
    return total;
  }
  
  public void testRemove() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    try {
      FileSystem fs = cluster.getFileSystem();
      assertTrue(fs.mkdirs(dir));
      
      long dfsUsedStart = getTotalDfsUsed(cluster);
      {
        // Create 100 files
        final int fileCount = 100;
        for (int i = 0; i < fileCount; i++) {
          Path a = new Path(dir, "a" + i);
          createFile(fs, a);
        }
        long dfsUsedMax = getTotalDfsUsed(cluster);
        // Remove 100 files
        for (int i = 0; i < fileCount; i++) {
          Path a = new Path(dir, "a" + i);
          fs.delete(a, false);
        }
        // wait 5 heartbeat intervals, so that all blocks are deleted.
        Thread.sleep(5 * FSConstants.HEARTBEAT_INTERVAL * 1000);
        // all blocks should be gone now.
        long dfsUsedFinal = getTotalDfsUsed(cluster);
        assertEquals("All blocks should be gone. start=" + dfsUsedStart
            + " max=" + dfsUsedMax + " final=" + dfsUsedFinal, dfsUsedStart, dfsUsedFinal);
      }

      fs.delete(dir, true);
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
}
