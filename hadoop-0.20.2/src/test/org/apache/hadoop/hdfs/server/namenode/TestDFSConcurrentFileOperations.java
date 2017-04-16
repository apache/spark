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
package org.apache.hadoop.hdfs.server.namenode;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import java.io.IOException;

public class TestDFSConcurrentFileOperations extends TestCase {

  MiniDFSCluster cluster;
  FileSystem fs;
  private int writeSize;
  private long blockSize;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    writeSize = 64 * 1024;
    blockSize = 2 * writeSize;
  }

  private void init() throws IOException {
    init(new Configuration());
  }

  private void init(Configuration conf) throws IOException {
    cluster = new MiniDFSCluster(conf, 3, true, new String[]{"/rack1", "/rack2", "/rack1"});
    cluster.waitClusterUp();
    fs = cluster.getFileSystem();
  }

  @Override
  protected void tearDown() throws Exception {
    fs.close();
    cluster.shutdown();
    super.tearDown();
  }

  /*
   * test case: 
   * 1. file is opened
   * 2. file is moved while being written to (including move to trash on delete)
   * 3. blocks complete and are finalized
   * 4. close fails
   * 5. lease recovery tries to finalize blocks and should succeed
   */
  public void testLeaseRecoveryOnTrashedFile() throws Exception {
    Configuration conf = new Configuration();
    
    conf.setLong("dfs.block.size", blockSize);
    conf.setBoolean("dfs.support.broken.append", true);

    init(conf);
    
    String src = "/file-1";
    String dst = "/file-2";
    Path srcPath = new Path(src);
    Path dstPath = new Path(dst);
    FSDataOutputStream fos = fs.create(srcPath);
   
    AppendTestUtil.write(fos, 0, writeSize);
    fos.sync();
    
    // renaming a file out from under a client will cause close to fail
    // and result in the lease remaining while the blocks are finalized on
    // the DNs
    fs.rename(srcPath, dstPath);

    try {
      fos.close();
      fail("expected IOException");
    } catch (IOException e) {
      //expected
    }

    FileSystem fs2 = AppendTestUtil.createHdfsWithDifferentUsername(conf);
    AppendTestUtil.recoverFile(cluster, fs2, dstPath);
    AppendTestUtil.check(fs2, dstPath, writeSize);
  }
}
