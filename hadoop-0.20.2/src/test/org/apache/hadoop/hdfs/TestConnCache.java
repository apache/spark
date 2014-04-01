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

import java.net.InetSocketAddress;
import java.net.Socket;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSClient.RemoteBlockReader;
import org.apache.hadoop.hdfs.DFSClient.DFSInputStream;
import org.apache.hadoop.hdfs.SocketCache;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.DataNode;

import org.apache.hadoop.security.token.Token;
import org.junit.Test;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import static org.junit.Assert.*;

import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.mockito.invocation.InvocationOnMock;
import static org.mockito.Mockito.spy;

/**
 * This class tests the client connection caching in a single node
 * mini-cluster.
 */
public class TestConnCache {
  static final Log LOG = LogFactory.getLog(TestConnCache.class);

  static final int BLOCK_SIZE = 4096;
  static final int FILE_SIZE = 3 * BLOCK_SIZE;

  static Configuration conf = null;
  static MiniDFSCluster cluster = null;
  static FileSystem fs = null;

  static final Path testFile = new Path("/testConnCache.dat");
  static byte authenticData[] = null;

  static BlockReaderTestUtil util = null;


  /**
   * A mock Answer to remember the BlockReader used.
   *
   * It verifies that all invocation to DFSInputStream.getBlockReader()
   * use the same socket.
   */
  private class MockGetBlockReader implements Answer<BlockReader> {
    public RemoteBlockReader reader = null;
    private Socket sock = null;
    private final boolean shouldBeAllTheSame;

    public MockGetBlockReader(boolean shouldBeAllTheSame) {
      this.shouldBeAllTheSame = shouldBeAllTheSame;
    }

    public BlockReader answer(InvocationOnMock invocation) throws Throwable {
      RemoteBlockReader prevReader = reader;
      reader = (RemoteBlockReader) invocation.callRealMethod();
      if (sock == null) {
        sock = reader.dnSock;
      } else if (prevReader != null &&
          (shouldBeAllTheSame || prevReader.hasSentStatusCode())) {
        // Can't reuse socket if the previous BlockReader didn't read till EOS.
        assertSame("DFSInputStream should use the same socket",
                   sock, reader.dnSock);
      } return reader;
    }
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    final int REPLICATION_FACTOR = 1;

    util = new BlockReaderTestUtil(REPLICATION_FACTOR);
    cluster = util.getCluster();
    conf = util.getConf();
    fs = cluster.getFileSystem();

    authenticData = util.writeFile(testFile, FILE_SIZE / 1024);
  }


  /**
   * (Optionally) seek to position, read and verify data.
   *
   * Seek to specified position if pos is non-negative.
   */
  private void seekAndRead(DFSInputStream in,
                     long pos,
                     byte[] buffer,
                     int offset,
                     int length)
      throws IOException {
    assertTrue("Test buffer too small", buffer.length >= offset + length);

    if (pos >= 0)
      in.seek(pos);

    LOG.info("Reading from file of size " + in.getFileLength() +
             " at offset " + in.getPos());

    while (length > 0) {
      int cnt = in.read(buffer, offset, length);
      assertTrue("Error in read", cnt > 0);
      offset += cnt;
      length -= cnt;
    }

    // Verify
    for (int i = 0; i < length; ++i) {
      byte actual = buffer[i];
      byte expect = authenticData[(int)pos + i];
      assertEquals("Read data mismatch at file offset " + (pos + i) +
                   ". Expects " + expect + "; got " + actual,
                   actual, expect);
    }
  }
  
  private void pread(DFSInputStream in,
                     long pos,
                     byte[] buffer,
                     int offset,
                     int length)
      throws IOException {
    assertTrue("Test buffer too small", buffer.length >= offset + length);

    LOG.info("PReading from file of size " + in.getFileLength() +
             " at offset " + in.getPos());

    while (length > 0) {
      int cnt = in.read(pos, buffer, offset, length);
      assertTrue("Error in read", cnt > 0);
      offset += cnt;
      length -= cnt;
      pos += cnt;
    }

    // Verify
    for (int i = 0; i < length; ++i) {
      byte actual = buffer[i];
      byte expect = authenticData[(int)pos + i];
      assertEquals("Read data mismatch at file offset " + (pos + i) +
                   ". Expects " + expect + "; got " + actual,
                   actual, expect);
    }
  }
  

  /**
   * Test the SocketCache itself.
   */
  @Test
  public void testSocketCache() throws IOException {
    final int CACHE_SIZE = 4;
    SocketCache cache = new SocketCache(CACHE_SIZE);

    // Make a client
    InetSocketAddress nnAddr =
        new InetSocketAddress("localhost", cluster.getNameNodePort());
    DFSClient client = new DFSClient(nnAddr, conf);

    // Find out the DN addr
    LocatedBlock block =
        client.namenode.getBlockLocations(
            testFile.toString(), 0, FILE_SIZE)
        .getLocatedBlocks().get(0);
    DataNode dn = util.getDataNode(block);
    InetSocketAddress dnAddr = dn.getSelfAddr();

    // Make some sockets to the DN
    Socket[] dnSockets = new Socket[CACHE_SIZE];
    for (int i = 0; i < dnSockets.length; ++i) {
      dnSockets[i] = client.socketFactory.createSocket(
          dnAddr.getAddress(), dnAddr.getPort());
    }

    // Insert a socket to the NN
    Socket nnSock = new Socket(nnAddr.getAddress(), nnAddr.getPort());
    cache.put(nnSock);
    assertSame("Read the write", nnSock, cache.get(nnAddr));
    cache.put(nnSock);

    // Insert DN socks
    for (Socket dnSock : dnSockets) {
      cache.put(dnSock);
    }

    assertEquals("NN socket evicted", null, cache.get(nnAddr));
    assertTrue("Evicted socket closed", nnSock.isClosed());

    // Lookup the DN socks
    for (Socket dnSock : dnSockets) {
      assertEquals("Retrieve cached sockets", dnSock, cache.get(dnAddr));
      dnSock.close();
    }

    assertEquals("Cache is empty", 0, cache.size());
  }

  /**
   * Read a file served entirely from one DN. Seek around and read from
   * different offsets. And verify that they all use the same socket.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testReadFromOneDN() throws IOException {
    doOneDNTest(false);
  }
  
  @Test
  public void testPreadFromOneDN() throws IOException {
    doOneDNTest(true);
  }

  
  private void doOneDNTest(boolean usePread) throws IOException {
    LOG.info("Starting testReadFromOneDN()");
    DFSClient client = new DFSClient(
        new InetSocketAddress("localhost", cluster.getNameNodePort()), conf);
    DFSInputStream in = spy(client.open(testFile.toString()));
    LOG.info("opened " + testFile.toString());

    byte[] dataBuf = new byte[BLOCK_SIZE];

    MockGetBlockReader answer = new MockGetBlockReader(usePread);
    Mockito.doAnswer(answer).when(in).getBlockReader(
      Matchers.<InetSocketAddress>anyObject(), // addr
      Matchers.anyString(), // file
      Matchers.anyLong(), // id
      Matchers.<Token<BlockTokenIdentifier>>anyObject(), // accessToken
      Matchers.anyLong(), //genStamp
      Matchers.anyLong(), //startOffset
      Matchers.anyLong(), // len
      Matchers.anyInt(), // bufferSize
      Matchers.anyBoolean(), // verifyChecksum
      Matchers.anyString()); //clientName

    if (usePread) {
      // Initial read
      pread(in, 0, dataBuf, 0, dataBuf.length);
      // Read again and verify that the socket is the same
      pread(in, FILE_SIZE - dataBuf.length, dataBuf, 0, dataBuf.length);
      pread(in, 1024, dataBuf, 0, dataBuf.length);
      pread(in, 64, dataBuf, 0, dataBuf.length / 2);
    } else {
      // Seek and read
      seekAndRead(in, 0, dataBuf, 0, dataBuf.length);
      // Read again and verify that the socket is the same
      seekAndRead(in, FILE_SIZE - dataBuf.length, dataBuf, 0, dataBuf.length);
      seekAndRead(in, 1024, dataBuf, 0, dataBuf.length);
      seekAndRead(in, -1, dataBuf, 0, dataBuf.length);            // No seek; just read
      seekAndRead(in, 64, dataBuf, 0, dataBuf.length / 2);
    }

    in.close();
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    util.shutdown();
  }
}
