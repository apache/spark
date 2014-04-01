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
import java.util.List;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient.RemoteBlockReader;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.net.NetUtils;

import static org.junit.Assert.*;

/**
 * A helper class to setup the cluster, and get to BlockReader and DataNode for a block.
 */
public class BlockReaderTestUtil {

  private Configuration conf = null;
  private MiniDFSCluster cluster = null;

  /**
   * Setup the cluster
   */
  public BlockReaderTestUtil(int replicationFactor) throws Exception {
    conf = new Configuration();
    conf.setInt("dfs.replication", replicationFactor);
    cluster = new MiniDFSCluster(conf, replicationFactor, true, null);
    cluster.waitActive();
  }

  /**
   * Shutdown cluster
   */
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  public MiniDFSCluster getCluster() {
    return cluster;
  }

  public Configuration getConf() {
    return conf;
  }

  /**
   * Create a file of the given size filled with random data.
   * @return  File data.
   */
  public byte[] writeFile(Path filepath, int sizeKB)
      throws IOException {
    FileSystem fs = cluster.getFileSystem();

    // Write a file with 256K of data
    DataOutputStream os = fs.create(filepath);
    byte data[] = new byte[1024 * sizeKB];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte)(i&0xff);
    }
    // new Random().nextBytes(data);
    os.write(data);
    os.close();
    return data;
  }

  /**
   * Get the list of Blocks for a file.
   */
  public List<LocatedBlock> getFileBlocks(Path filepath, int sizeKB)
      throws IOException {
    // Return the blocks we just wrote
    DFSClient dfsclient = getDFSClient();
    return dfsclient.namenode.getBlockLocations(
      filepath.toString(), 0, sizeKB * 1024).getLocatedBlocks();
  }

  /**
   * Get the DFSClient.
   */
  public DFSClient getDFSClient() throws IOException {
    InetSocketAddress nnAddr = new InetSocketAddress("localhost", cluster.getNameNodePort());
    return new DFSClient(nnAddr, conf);
  }
    
  /**
   * Exercise the BlockReader and read length bytes.
   *
   * It does not verify the bytes read.
   */
  public void readCasually(BlockReader reader, int length, boolean expectEof)
      throws IOException {
    byte buf[] = new byte[1024];
    int nRead = 0;
    while (nRead < length) {
      DFSClient.LOG.info("So far read " + nRead + " - going to read more.");
      int n = reader.read(buf, 0, buf.length);
      assertTrue(n > 0);
      nRead += n;
    }

    if (expectEof) {
      DFSClient.LOG.info("Done reading, expect EOF for next read.");
      assertEquals(-1, reader.read(buf, 0, buf.length));
    }
  }

  /**
   * Get a BlockReader for the given block.
   */
  public BlockReader getBlockReader(LocatedBlock testBlock, int offset, int lenToRead)
      throws IOException {
    InetSocketAddress targetAddr = null;
    Socket sock = null;
    BlockReader blockReader = null;
    Block block = testBlock.getBlock();
    DatanodeInfo[] nodes = testBlock.getLocations();
    targetAddr = NetUtils.createSocketAddr(nodes[0].getName());
    sock = new Socket();
    sock.connect(targetAddr, HdfsConstants.READ_TIMEOUT);
    sock.setSoTimeout(HdfsConstants.READ_TIMEOUT);

    return RemoteBlockReader.newBlockReader(
      sock, targetAddr.toString()+ ":" + block.getBlockId(), block.getBlockId(),
      testBlock.getBlockToken(),
      block.getGenerationStamp(),
      offset, lenToRead,
      conf.getInt("io.file.buffer.size", 4096));
  }

  /**
   * Get a DataNode that serves our testBlock.
   */
  public DataNode getDataNode(LocatedBlock testBlock) {
    DatanodeInfo[] nodes = testBlock.getLocations();
    int ipcport = nodes[0].ipcPort;
    return cluster.getDataNode(ipcport);
  }

}
