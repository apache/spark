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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSClient.RemoteBlockReader;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.*;
import org.apache.hadoop.hdfs.server.balancer.TestBalancer;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.token.*;
import org.apache.log4j.Level;

import junit.framework.TestCase;

public class TestBlockTokenWithDFS extends TestCase {

  private static final int BLOCK_SIZE = 1024;
  private static final int FILE_SIZE = 2 * BLOCK_SIZE;
  private static final String FILE_TO_READ = "/fileToRead.dat";
  private static final String FILE_TO_WRITE = "/fileToWrite.dat";
  private static final String FILE_TO_APPEND = "/fileToAppend.dat";
  private final byte[] rawData = new byte[FILE_SIZE];

  {
    ((Log4JLogger) DFSClient.LOG).getLogger().setLevel(Level.ALL);
    Random r = new Random();
    r.nextBytes(rawData);
  }

  private void createFile(FileSystem fs, Path filename) throws IOException {
    FSDataOutputStream out = fs.create(filename);
    out.write(rawData);
    out.close();
  }

  // read a file using blockSeekTo()
  private boolean checkFile1(FSDataInputStream in) {
    byte[] toRead = new byte[FILE_SIZE];
    int totalRead = 0;
    int nRead = 0;
    try {
      while ((nRead = in.read(toRead, totalRead, toRead.length - totalRead)) > 0) {
        totalRead += nRead;
      }
    } catch (IOException e) {
      return false;
    }
    assertEquals("Cannot read file.", toRead.length, totalRead);
    return checkFile(toRead);
  }

  // read a file using fetchBlockByteRange()
  private boolean checkFile2(FSDataInputStream in) {
    byte[] toRead = new byte[FILE_SIZE];
    try {
      assertEquals("Cannot read file", toRead.length, in.read(0, toRead, 0,
          toRead.length));
    } catch (IOException e) {
      return false;
    }
    return checkFile(toRead);
  }

  private boolean checkFile(byte[] fileToCheck) {
    if (fileToCheck.length != rawData.length) {
      return false;
    }
    for (int i = 0; i < fileToCheck.length; i++) {
      if (fileToCheck[i] != rawData[i]) {
        return false;
      }
    }
    return true;
  }

  // creates a file and returns a descriptor for writing to it
  private static FSDataOutputStream writeFile(FileSystem fileSys, Path name,
      short repl, long blockSize) throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true, fileSys.getConf()
        .getInt("io.file.buffer.size", 4096), repl, blockSize);
    return stm;
  }

  // try reading a block using a BlockReader directly
  private static void tryRead(Configuration conf, LocatedBlock lblock,
      boolean shouldSucceed) {
    InetSocketAddress targetAddr = null;
    Socket s = null;
    BlockReader blockReader = null;
    Block block = lblock.getBlock();
    try {
      DatanodeInfo[] nodes = lblock.getLocations();
      targetAddr = NetUtils.createSocketAddr(nodes[0].getName());
      s = new Socket();
      s.connect(targetAddr, HdfsConstants.READ_TIMEOUT);
      s.setSoTimeout(HdfsConstants.READ_TIMEOUT);

      blockReader = RemoteBlockReader.newBlockReader(s, targetAddr
          .toString()
          + ":" + block.getBlockId(), block.getBlockId(), lblock
          .getBlockToken(), block.getGenerationStamp(), 0, -1, conf.getInt(
          "io.file.buffer.size", 4096));

    } catch (IOException ex) {
      if (ex instanceof InvalidBlockTokenException) {
        assertFalse("OP_READ_BLOCK: access token is invalid, "
            + "when it is expected to be valid", shouldSucceed);
        return;
      }
      fail("OP_READ_BLOCK failed due to reasons other than access token");
    } finally {
      if (s != null) {
        try {
          s.close();
        } catch (IOException iex) {
        } finally {
          s = null;
        }
      }
    }
    if (blockReader == null) {
      fail("OP_READ_BLOCK failed due to reasons other than access token");
    }
    assertTrue("OP_READ_BLOCK: access token is valid, "
        + "when it is expected to be invalid", shouldSucceed);
  }

  // get a conf for testing
  private static Configuration getConf(int numDataNodes) throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setLong("dfs.block.size", BLOCK_SIZE);
    conf.setInt("io.bytes.per.checksum", BLOCK_SIZE);
    conf.setInt("dfs.heartbeat.interval", 1);
    conf.setInt("dfs.replication", numDataNodes);
    conf.setInt("ipc.client.connect.max.retries", 0);
    conf.setBoolean("dfs.support.broken.append", true);
    return conf;
  }

  /*
   * testing that APPEND operation can handle token expiration when
   * re-establishing pipeline is needed
   */
  public void testAppend() throws Exception {
    MiniDFSCluster cluster = null;
    int numDataNodes = 2;
    Configuration conf = getConf(numDataNodes);

    try {
      cluster = new MiniDFSCluster(conf, numDataNodes, true, null);
      cluster.waitActive();
      assertEquals(numDataNodes, cluster.getDataNodes().size());
      // set a short token lifetime (1 second)
      SecurityTestUtil.setBlockTokenLifetime(
          cluster.getNameNode().getNamesystem().accessTokenHandler, 1000L);
      Path fileToAppend = new Path(FILE_TO_APPEND);
      FileSystem fs = cluster.getFileSystem();

      // write a one-byte file
      FSDataOutputStream stm = writeFile(fs, fileToAppend,
          (short) numDataNodes, BLOCK_SIZE);
      stm.write(rawData, 0, 1);
      stm.close();
      // open the file again for append
      stm = fs.append(fileToAppend);
      int mid = rawData.length - 1;
      stm.write(rawData, 1, mid - 1);
      stm.sync();

      /*
       * wait till token used in stm expires
       */
      Token<BlockTokenIdentifier> token = DFSTestUtil.getAccessToken(stm);
      while (!SecurityTestUtil.isBlockTokenExpired(token)) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException ignored) {
        }
      }

      // remove a datanode to force re-establishing pipeline
      cluster.stopDataNode(0);
      // append the rest of the file
      stm.write(rawData, mid, rawData.length - mid);
      stm.close();
      // check if append is successful
      FSDataInputStream in5 = fs.open(fileToAppend);
      assertTrue(checkFile1(in5));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
   * testing that WRITE operation can handle token expiration when
   * re-establishing pipeline is needed
   */
  public void testWrite() throws Exception {
    MiniDFSCluster cluster = null;
    int numDataNodes = 2;
    Configuration conf = getConf(numDataNodes);

    try {
      cluster = new MiniDFSCluster(conf, numDataNodes, true, null);
      cluster.waitActive();
      assertEquals(numDataNodes, cluster.getDataNodes().size());
      // set a short token lifetime (1 second)
      SecurityTestUtil.setBlockTokenLifetime(
          cluster.getNameNode().getNamesystem().accessTokenHandler, 1000L);
      Path fileToWrite = new Path(FILE_TO_WRITE);
      FileSystem fs = cluster.getFileSystem();

      FSDataOutputStream stm = writeFile(fs, fileToWrite, (short) numDataNodes,
          BLOCK_SIZE);
      // write a partial block
      int mid = rawData.length - 1;
      stm.write(rawData, 0, mid);
      stm.sync();

      /*
       * wait till token used in stm expires
       */
      Token<BlockTokenIdentifier> token = DFSTestUtil.getAccessToken(stm);
      while (!SecurityTestUtil.isBlockTokenExpired(token)) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException ignored) {
        }
      }

      // remove a datanode to force re-establishing pipeline
      cluster.stopDataNode(0);
      // write the rest of the file
      stm.write(rawData, mid, rawData.length - mid);
      stm.close();
      // check if write is successful
      FSDataInputStream in4 = fs.open(fileToWrite);
      assertTrue(checkFile1(in4));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  public void testRead() throws Exception {
    MiniDFSCluster cluster = null;
    int numDataNodes = 2;
    Configuration conf = getConf(numDataNodes);

    try {
      cluster = new MiniDFSCluster(conf, numDataNodes, true, null);
      cluster.waitActive();
      assertEquals(numDataNodes, cluster.getDataNodes().size());
      // set a short token lifetime (1 second) initially
      SecurityTestUtil.setBlockTokenLifetime(
          cluster.getNameNode().getNamesystem().accessTokenHandler, 1000L);
      Path fileToRead = new Path(FILE_TO_READ);
      FileSystem fs = cluster.getFileSystem();
      createFile(fs, fileToRead);

      /*
       * setup for testing expiration handling of cached tokens
       */

      // read using blockSeekTo(). Acquired tokens are cached in in1
      FSDataInputStream in1 = fs.open(fileToRead);
      assertTrue(checkFile1(in1));
      // read using blockSeekTo(). Acquired tokens are cached in in2
      FSDataInputStream in2 = fs.open(fileToRead);
      assertTrue(checkFile1(in2));
      // read using fetchBlockByteRange(). Acquired tokens are cached in in3
      FSDataInputStream in3 = fs.open(fileToRead);
      assertTrue(checkFile2(in3));

      /*
       * testing READ interface on DN using a BlockReader
       */

      DFSClient dfsclient = new DFSClient(new InetSocketAddress("localhost",
          cluster.getNameNodePort()), conf);
      List<LocatedBlock> locatedBlocks = cluster.getNameNode().getBlockLocations(
          FILE_TO_READ, 0, FILE_SIZE).getLocatedBlocks();
      LocatedBlock lblock = locatedBlocks.get(0); // first block
      Token<BlockTokenIdentifier> myToken = lblock.getBlockToken();
      // verify token is not expired
      assertFalse(SecurityTestUtil.isBlockTokenExpired(myToken));
      // read with valid token, should succeed
      tryRead(conf, lblock, true);

      /*
       * wait till myToken and all cached tokens in in1, in2 and in3 expire
       */

      while (!SecurityTestUtil.isBlockTokenExpired(myToken)) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException ignored) {
        }
      }

      /*
       * continue testing READ interface on DN using a BlockReader
       */

      // verify token is expired
      assertTrue(SecurityTestUtil.isBlockTokenExpired(myToken));
      // read should fail
      tryRead(conf, lblock, false);
      // use a valid new token
      lblock.setBlockToken(cluster.getNameNode().getNamesystem()
          .accessTokenHandler.generateToken(lblock.getBlock(),
              EnumSet.of(BlockTokenSecretManager.AccessMode.READ)));
      // read should succeed
      tryRead(conf, lblock, true);
      // use a token with wrong blockID
      Block wrongBlock = new Block(lblock.getBlock().getBlockId() + 1);
      lblock.setBlockToken(cluster.getNameNode().getNamesystem()
          .accessTokenHandler.generateToken(wrongBlock,
              EnumSet.of(BlockTokenSecretManager.AccessMode.READ)));
      // read should fail
      tryRead(conf, lblock, false);
      // use a token with wrong access modes
      lblock.setBlockToken(cluster.getNameNode().getNamesystem()
          .accessTokenHandler.generateToken(lblock.getBlock(), EnumSet.of(
              BlockTokenSecretManager.AccessMode.WRITE,
              BlockTokenSecretManager.AccessMode.COPY,
              BlockTokenSecretManager.AccessMode.REPLACE)));
      // read should fail
      tryRead(conf, lblock, false);

      // set a long token lifetime for future tokens
      SecurityTestUtil.setBlockTokenLifetime(
          cluster.getNameNode().getNamesystem().accessTokenHandler, 600 * 1000L);

      /*
       * testing that when cached tokens are expired, DFSClient will re-fetch
       * tokens transparently for READ.
       */

      // confirm all tokens cached in in1 are expired by now
      List<LocatedBlock> lblocks = DFSTestUtil.getAllBlocks(in1);
      for (LocatedBlock blk : lblocks) {
        assertTrue(SecurityTestUtil.isBlockTokenExpired(blk.getBlockToken()));
      }
      // verify blockSeekTo() is able to re-fetch token transparently
      in1.seek(0);
      assertTrue(checkFile1(in1));

      // confirm all tokens cached in in2 are expired by now
      List<LocatedBlock> lblocks2 = DFSTestUtil.getAllBlocks(in2);
      for (LocatedBlock blk : lblocks2) {
        assertTrue(SecurityTestUtil.isBlockTokenExpired(blk.getBlockToken()));
      }
      // verify blockSeekTo() is able to re-fetch token transparently (testing
      // via another interface method)
      assertTrue(in2.seekToNewSource(0));
      assertTrue(checkFile1(in2));

      // confirm all tokens cached in in3 are expired by now
      List<LocatedBlock> lblocks3 = DFSTestUtil.getAllBlocks(in3);
      for (LocatedBlock blk : lblocks3) {
        assertTrue(SecurityTestUtil.isBlockTokenExpired(blk.getBlockToken()));
      }
      // verify fetchBlockByteRange() is able to re-fetch token transparently
      assertTrue(checkFile2(in3));

      /*
       * testing that after datanodes are restarted on the same ports, cached
       * tokens should still work and there is no need to fetch new tokens from
       * namenode. This test should run while namenode is down (to make sure no
       * new tokens can be fetched from namenode).
       */

      // restart datanodes on the same ports that they currently use
      assertTrue(cluster.restartDataNodes(true));
      cluster.waitActive();
      assertEquals(numDataNodes, cluster.getDataNodes().size());
      cluster.shutdownNameNode();

      // confirm tokens cached in in1 are still valid
      lblocks = DFSTestUtil.getAllBlocks(in1);
      for (LocatedBlock blk : lblocks) {
        assertFalse(SecurityTestUtil.isBlockTokenExpired(blk.getBlockToken()));
      }
      // verify blockSeekTo() still works (forced to use cached tokens)
      in1.seek(0);
      assertTrue(checkFile1(in1));

      // confirm tokens cached in in2 are still valid
      lblocks2 = DFSTestUtil.getAllBlocks(in2);
      for (LocatedBlock blk : lblocks2) {
        assertFalse(SecurityTestUtil.isBlockTokenExpired(blk.getBlockToken()));
      }
      // verify blockSeekTo() still works (forced to use cached tokens)
      in2.seekToNewSource(0);
      assertTrue(checkFile1(in2));

      // confirm tokens cached in in3 are still valid
      lblocks3 = DFSTestUtil.getAllBlocks(in3);
      for (LocatedBlock blk : lblocks3) {
        assertFalse(SecurityTestUtil.isBlockTokenExpired(blk.getBlockToken()));
      }
      // verify fetchBlockByteRange() still works (forced to use cached tokens)
      assertTrue(checkFile2(in3));

      /*
       * testing that when namenode is restarted, cached tokens should still
       * work and there is no need to fetch new tokens from namenode. Like the
       * previous test, this test should also run while namenode is down. The
       * setup for this test depends on the previous test.
       */

      // restart the namenode and then shut it down for test
      cluster.restartNameNode();
      cluster.shutdownNameNode();

      // verify blockSeekTo() still works (forced to use cached tokens)
      in1.seek(0);
      assertTrue(checkFile1(in1));
      // verify again blockSeekTo() still works (forced to use cached tokens)
      in2.seekToNewSource(0);
      assertTrue(checkFile1(in2));
      // verify fetchBlockByteRange() still works (forced to use cached tokens)
      assertTrue(checkFile2(in3));

      /*
       * testing that after both namenode and datanodes got restarted (namenode
       * first, followed by datanodes), DFSClient can't access DN without
       * re-fetching tokens and is able to re-fetch tokens transparently. The
       * setup of this test depends on the previous test.
       */

      // restore the cluster and restart the datanodes for test
      cluster.restartNameNode();
      assertTrue(cluster.restartDataNodes(true));
      cluster.waitActive();
      assertEquals(numDataNodes, cluster.getDataNodes().size());

      // shutdown namenode so that DFSClient can't get new tokens from namenode
      cluster.shutdownNameNode();

      // verify blockSeekTo() fails (cached tokens become invalid)
      in1.seek(0);
      assertFalse(checkFile1(in1));
      // verify fetchBlockByteRange() fails (cached tokens become invalid)
      assertFalse(checkFile2(in3));

      // restart the namenode to allow DFSClient to re-fetch tokens
      cluster.restartNameNode();
      // verify blockSeekTo() works again (by transparently re-fetching
      // tokens from namenode)
      in1.seek(0);
      assertTrue(checkFile1(in1));
      in2.seekToNewSource(0);
      assertTrue(checkFile1(in2));
      // verify fetchBlockByteRange() works again (by transparently
      // re-fetching tokens from namenode)
      assertTrue(checkFile2(in3));

      /*
       * testing that when datanodes are restarted on different ports, DFSClient
       * is able to re-fetch tokens transparently to connect to them
       */

      // restart datanodes on newly assigned ports
      assertTrue(cluster.restartDataNodes(false));
      cluster.waitActive();
      assertEquals(numDataNodes, cluster.getDataNodes().size());
      // verify blockSeekTo() is able to re-fetch token transparently
      in1.seek(0);
      assertTrue(checkFile1(in1));
      // verify blockSeekTo() is able to re-fetch token transparently
      in2.seekToNewSource(0);
      assertTrue(checkFile1(in2));
      // verify fetchBlockByteRange() is able to re-fetch token transparently
      assertTrue(checkFile2(in3));

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
   * Integration testing of access token, involving NN, DN, and Balancer
   */
  public void testEnd2End() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    new TestBalancer().integrationTest(conf);
  }
}
