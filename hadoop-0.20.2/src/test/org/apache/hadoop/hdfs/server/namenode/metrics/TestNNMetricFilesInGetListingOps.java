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
 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.metrics;

import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

/**
 * Test case for FilesInGetListingOps metric in Namenode
 */
public class TestNNMetricFilesInGetListingOps extends TestCase {
  private static final Configuration CONF = new Configuration();
  static {
    CONF.setLong("dfs.block.size", 100);
    CONF.setInt("io.bytes.per.checksum", 1);
    CONF.setLong("dfs.heartbeat.interval", 1L);
    CONF.setInt("dfs.replication.interval", 1);
  }
     
  private MiniDFSCluster cluster;
  private NameNodeMetrics nnMetrics;
  private DistributedFileSystem fs;
  private Random rand = new Random();

  @Override
  protected void setUp() throws Exception {
    cluster = new MiniDFSCluster(CONF, 1, true, null);
    cluster.waitActive();
    cluster.getNameNode();
	nnMetrics = NameNode.getNameNodeMetrics();
    fs = (DistributedFileSystem) cluster.getFileSystem();
  }

  @Override
  protected void tearDown() throws Exception {
    cluster.shutdown();
  }

  /** create a file with a length of <code>fileLen</code> */
  private void createFile(String fileName, long fileLen, short replicas) throws IOException {
    Path filePath = new Path(fileName);
    DFSTestUtil.createFile(fs, filePath, fileLen, replicas, rand.nextLong());
  }
     

  public void testFilesInGetListingOps() throws Exception {
    createFile("/tmp1/t1", 3200, (short)3);
    createFile("/tmp1/t2", 3200, (short)3);
    createFile("/tmp2/t1", 3200, (short)3);
    createFile("/tmp2/t2", 3200, (short)3);
    cluster.getNameNode().getListing("/tmp1", HdfsFileStatus.EMPTY_NAME) ;
    assertEquals(2,nnMetrics.numFilesInGetListingOps.getCurrentIntervalValue());
    cluster.getNameNode().getListing("/tmp2", HdfsFileStatus.EMPTY_NAME) ;
    assertEquals(4,nnMetrics.numFilesInGetListingOps.getCurrentIntervalValue());
    // test non-existent path
    cluster.getNameNode().getListing("/tmp", HdfsFileStatus.EMPTY_NAME) ;
    assertEquals(4,nnMetrics.numFilesInGetListingOps.getCurrentIntervalValue());
    // test listing a file
    cluster.getNameNode().getListing("/tmp1/t1", HdfsFileStatus.EMPTY_NAME) ;
    assertEquals(5,nnMetrics.numFilesInGetListingOps.getCurrentIntervalValue());
  }
}

