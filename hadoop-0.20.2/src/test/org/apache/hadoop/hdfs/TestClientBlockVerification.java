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

import java.net.Socket;
import java.net.InetSocketAddress;
import java.io.DataOutputStream;
import java.util.Random;
import java.util.List;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient.RemoteBlockReader;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.net.NetUtils;

import org.junit.Test;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;

import static org.junit.Assert.*;

public class TestClientBlockVerification {
  static MiniDFSCluster cluster = null;
  static Configuration conf = null;
  static FileSystem fs = null;
  static final Path TEST_FILE = new Path("/test.file");
  static final int FILE_SIZE_K = 256;
  static LocatedBlock testBlock = null;

  @BeforeClass
  public static void setupCluster() throws Exception {
    conf = new Configuration();
    int numDataNodes = 1;
    conf.setInt("dfs.replication", numDataNodes);
    cluster = new MiniDFSCluster(conf, numDataNodes, true, null);
    cluster.waitActive();
    fs = cluster.getFileSystem();

    // Write a file with 256K of data
    DataOutputStream os = fs.create(TEST_FILE);
    byte data[] = new byte[1024];
    new Random().nextBytes(data);
    for (int i = 0; i < FILE_SIZE_K; i++) {
      os.write(data);
    }
    os.close();

    // Locate the block we just wrote
    DFSClient dfsclient = new DFSClient(
      new InetSocketAddress("localhost",
                            cluster.getNameNodePort()), conf);
    List<LocatedBlock> locatedBlocks = dfsclient.namenode.getBlockLocations(
      TEST_FILE.toString(), 0, FILE_SIZE_K * 1024).getLocatedBlocks();
    testBlock = locatedBlocks.get(0); // first block
  }

  private RemoteBlockReader getBlockReader(
    int offset, int lenToRead) throws IOException {
    InetSocketAddress targetAddr = null;
    Socket s = null;
    Block block = testBlock.getBlock();
    DatanodeInfo[] nodes = testBlock.getLocations();
    targetAddr = NetUtils.createSocketAddr(nodes[0].getName());
    s = new Socket();
    s.connect(targetAddr, HdfsConstants.READ_TIMEOUT);
    s.setSoTimeout(HdfsConstants.READ_TIMEOUT);

    return (RemoteBlockReader)RemoteBlockReader.newBlockReader(
      s, targetAddr.toString()+ ":" + block.getBlockId(),
      block.getBlockId(),
      testBlock.getBlockToken(),
      block.getGenerationStamp(),
      offset, lenToRead,
      conf.getInt("io.file.buffer.size", 4096));
  }

  /**
   * Verify that if we read an entire block, we send checksumOk
   */
  @Test
  public void testBlockVerification() throws Exception {
    RemoteBlockReader reader = spy(getBlockReader(0, FILE_SIZE_K * 1024));
    slurpReader(reader, FILE_SIZE_K * 1024, true);
    verify(reader).sendReadResult(
      reader.dnSock,
      (short)DataTransferProtocol.OP_STATUS_CHECKSUM_OK);
    reader.close();
  }

  /**
   * Test that if we do an incomplete read, we don't call checksumOk
   */
  @Test
  public void testIncompleteRead() throws Exception {
    RemoteBlockReader reader = spy(getBlockReader(0, FILE_SIZE_K * 1024));
    slurpReader(reader, FILE_SIZE_K / 2 * 1024, false);

    // We asked the blockreader for the whole file, and only read
    // half of it, so no CHECKSUM_OK
    verify(reader, never()).sendReadResult(
      reader.dnSock,
      (short)DataTransferProtocol.OP_STATUS_CHECKSUM_OK);
    reader.close();
  }

  /**
   * Test that if we ask for a half block, and read it all, we *do*
   * send CHECKSUM_OK. The DN takes care of knowing whether it was
   * the whole block or not.
   */
  @Test
  public void testCompletePartialRead() throws Exception {
    // Ask for half the file
    RemoteBlockReader reader = spy(getBlockReader(0, FILE_SIZE_K * 1024 / 2));
    // And read half the file
    slurpReader(reader, FILE_SIZE_K * 1024 / 2, true);
    verify(reader).sendReadResult(reader.dnSock,
      (short)DataTransferProtocol.OP_STATUS_CHECKSUM_OK);

    reader.close();
  }

  /**
   * Test various unaligned reads to make sure that we properly
   * account even when we don't start or end on a checksum boundary
   */
  @Test
  public void testUnalignedReads() throws Exception {
    int startOffsets[] = new int[] { 0, 3, 129 };
    int lengths[] = new int[] { 30, 300, 512, 513, 1025 };
    for (int startOffset : startOffsets) {
      for (int length : lengths) {
        DFSClient.LOG.info("Testing startOffset = " + startOffset + " and " +
                           " len=" + length);
        RemoteBlockReader reader = spy(getBlockReader(startOffset, length));
        slurpReader(reader, length, true);
        verify(reader).sendReadResult(
          reader.dnSock,
          (short)DataTransferProtocol.OP_STATUS_CHECKSUM_OK);
        reader.close();
      }
    }
  }


  /**
   * Read the given length from the given block reader.
   *
   * @param expectEOF if true, will expect an eof response when done
   */
  private void slurpReader(BlockReader reader, int length, boolean expectEof)
    throws IOException {
    byte buf[] = new byte[1024];
    int nRead = 0;
    while (nRead < length) {
      DFSClient.LOG.info("So far read " + nRead + " - going to read more.");
      int n = reader.read(buf, 0, buf.length);
      assertTrue(n > 0);
      nRead += n;
    }
    DFSClient.LOG.info("Done reading, expect EOF for next read.");
    if (expectEof) {
      try {
        assertEquals(-1, reader.read(buf, 0, buf.length));
      } catch (IOException ioe) {
        // IOException is thrown for BlockReader EOF on 0.20, since the
        // DFSInputStream generally disallows any read *past* the EOF.
        if (!"BlockRead: already got EOS or an error".equals(ioe.getMessage())) {
          throw ioe;
        }
      }
    }
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

}
