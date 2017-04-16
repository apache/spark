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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.thirdparty.guava.common.io.NullOutputStream;

public class TestDataTransferKeepalive {
  Configuration conf = new Configuration();
  private MiniDFSCluster cluster;
  private FileSystem fs;
  private InetSocketAddress dnAddr;
  private DataNode dn;
  private DFSClient dfsClient;
  private static Path TEST_FILE = new Path("/test");
  
  private static final int KEEPALIVE_TIMEOUT = 1000;
  private static final int WRITE_TIMEOUT = 3000;
  
  @Before
  public void setup() throws Exception {
    conf.setInt(DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_KEY,
        KEEPALIVE_TIMEOUT);
    conf.setInt(DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY,
        0);

    cluster = new MiniDFSCluster(conf, 1, true, null);
    fs = cluster.getFileSystem();
    dfsClient = ((DistributedFileSystem)fs).dfs;

    dn = cluster.getDataNodes().get(0);
    DatanodeID dnReg = dfsClient.datanodeReport(DatanodeReportType.LIVE)[0];
    dnAddr = NetUtils.createSocketAddr(dnReg.getName());
  }
  
  @After
  public void teardown() {
    cluster.shutdown();
  }
  
  /**
   * Regression test for HDFS-3357. Check that the datanode is respecting
   * its configured keepalive timeout.
   */
  @Test(timeout=30000)
  public void testKeepaliveTimeouts() throws Exception {
    DFSTestUtil.createFile(fs, TEST_FILE, 1L, (short)1, 0L);

    // Clients that write aren't currently re-used.
    assertEquals(0, dfsClient.socketCache.size());
    assertXceiverCount(0);

    // Reads the file, so we should get a
    // cached socket, and should have an xceiver on the other side.
    DFSTestUtil.readFile(fs, TEST_FILE);
    assertEquals(1, dfsClient.socketCache.size());
    assertXceiverCount(1);

    // Sleep for a bit longer than the keepalive timeout
    // and make sure the xceiver died.
    Thread.sleep(KEEPALIVE_TIMEOUT * 2);
    assertXceiverCount(0);
    
    // The socket is still in the cache, because we don't
    // notice that it's closed until we try to read
    // from it again.
    assertEquals(1, dfsClient.socketCache.size());
    
    // Take it out of the cache - reading should
    // give an EOF.
    Socket s = dfsClient.socketCache.get(dnAddr);
    assertNotNull(s);
    assertEquals(-1, NetUtils.getInputStream(s).read());
  }

  /**
   * Test for the case where the client beings to read a long block, but doesn't
   * read bytes off the stream quickly. The datanode should time out sending the
   * chunks and the transceiver should die, even if it has a long keepalive.
   */
  @Test(timeout=30000)
  public void testSlowReader() throws Exception {
    // Restart the DN with a shorter write timeout.
    DataNodeProperties props = cluster.stopDataNode(0);
    props.conf.setInt(DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY,
        WRITE_TIMEOUT);
    props.conf.setInt(DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_KEY,
        120000);
    assertTrue(cluster.restartDataNode(props, true));
    dn = cluster.getDataNodes().get(0);
    
    DFSTestUtil.createFile(fs, TEST_FILE, 1024*1024*8L, (short)1, 0L);
    FSDataInputStream stm = fs.open(TEST_FILE);
    try {
      stm.read();
      assertXceiverCount(1);

      Thread.sleep(WRITE_TIMEOUT + 1000);
      // DN should time out in sendChunks, and this should force
      // the xceiver to exit.
      assertXceiverCount(0);
    } finally {
      IOUtils.closeStream(stm);
    }
  }

  @Test(timeout=30000)
  public void testManyClosedSocketsInCache() throws Exception {
    // Make a small file
    DFSTestUtil.createFile(fs, TEST_FILE, 1L, (short)1, 0L);

    // Insert a bunch of dead sockets in the cache, by opening
    // many streams concurrently, reading all of the data,
    // and then closing them.
    InputStream[] stms = new InputStream[5];
    try {
      for (int i = 0; i < stms.length; i++) {
        stms[i] = fs.open(TEST_FILE);
      }
      for (InputStream stm : stms) {
        IOUtils.copyBytes(stm, new NullOutputStream(), 1024, false);
      }
    } finally {
      IOUtils.cleanup(null, stms);
    }
    
    DFSClient client = ((DistributedFileSystem)fs).dfs;
    assertEquals(5, client.socketCache.size());
    
    // Let all the xceivers timeout
    Thread.sleep(1500);
    assertXceiverCount(0);

    // Client side still has the sockets cached
    assertEquals(5, client.socketCache.size());

    // Reading should not throw an exception.
    DFSTestUtil.readFile(fs, TEST_FILE);
  }

  private void assertXceiverCount(int expected) {
    // Subtract 1, since the DataXceiverServer
    // counts as one
    int count = dn.getXceiverCount() - 1;
    if (count != expected) {
      ReflectionUtils.printThreadInfo(
          new PrintWriter(System.err),
          "Thread dumps");
      fail("Expected " + expected + " xceivers, found " +
          count);
    }
  }
}
