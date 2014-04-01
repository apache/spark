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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.datanode.BlockTransferThrottler;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
/**
 * This class tests if block replacement request to data nodes work correctly.
 */
public class TestBlockReplacement extends TestCase {
  private static final Log LOG = LogFactory.getLog(
  "org.apache.hadoop.hdfs.TestBlockReplacement");

  MiniDFSCluster cluster;
  public void testThrottler() throws IOException {
    Configuration conf = new Configuration();
    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    long bandwidthPerSec = 1024*1024L;
    final long TOTAL_BYTES =6*bandwidthPerSec; 
    long bytesToSend = TOTAL_BYTES; 
    long start = Util.now();
    BlockTransferThrottler throttler = new BlockTransferThrottler(bandwidthPerSec);
    long totalBytes = 0L;
    long bytesSent = 1024*512L; // 0.5MB
    throttler.throttle(bytesSent);
    bytesToSend -= bytesSent;
    bytesSent = 1024*768L; // 0.75MB
    throttler.throttle(bytesSent);
    bytesToSend -= bytesSent;
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ignored) {}
    throttler.throttle(bytesToSend);
    long end = Util.now();
    assertTrue(totalBytes*1000/(end-start)<=bandwidthPerSec);
  }
  
  public void testBlockReplacement() throws IOException {
    final Configuration CONF = new Configuration();
    final String[] INITIAL_RACKS = {"/RACK0", "/RACK1", "/RACK2"};
    final String[] NEW_RACKS = {"/RACK2"};

    final short REPLICATION_FACTOR = (short)3;
    final int DEFAULT_BLOCK_SIZE = 1024;
    final Random r = new Random();
    
    CONF.setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    CONF.setInt("io.bytes.per.checksum", DEFAULT_BLOCK_SIZE/2);
    CONF.setLong("dfs.blockreport.intervalMsec",500);
    cluster = new MiniDFSCluster(
          CONF, REPLICATION_FACTOR, true, INITIAL_RACKS );
    try {
      cluster.waitActive();
      
      FileSystem fs = cluster.getFileSystem();
      Path fileName = new Path("/tmp.txt");
      
      // create a file with one block
      DFSTestUtil.createFile(fs, fileName,
          DEFAULT_BLOCK_SIZE, REPLICATION_FACTOR, r.nextLong());
      DFSTestUtil.waitReplication(fs,fileName, REPLICATION_FACTOR);
      
      // get all datanodes
      InetSocketAddress addr = new InetSocketAddress("localhost",
          cluster.getNameNodePort());
      DFSClient client = new DFSClient(addr, CONF);
      List<LocatedBlock> locatedBlocks = client.namenode.
        getBlockLocations("/tmp.txt", 0, DEFAULT_BLOCK_SIZE).getLocatedBlocks();
      assertEquals(1, locatedBlocks.size());
      LocatedBlock block = locatedBlocks.get(0);
      DatanodeInfo[]  oldNodes = block.getLocations();
      assertEquals(oldNodes.length, 3);
      Block b = block.getBlock();
      
      // add a new datanode to the cluster
      cluster.startDataNodes(CONF, 1, true, null, NEW_RACKS);
      cluster.waitActive();
      
      DatanodeInfo[] datanodes = client.datanodeReport(DatanodeReportType.ALL);

      // find out the new node
      DatanodeInfo newNode=null;
      for(DatanodeInfo node:datanodes) {
        Boolean isNewNode = true;
        for(DatanodeInfo oldNode:oldNodes) {
          if(node.equals(oldNode)) {
            isNewNode = false;
            break;
          }
        }
        if(isNewNode) {
          newNode = node;
          break;
        }
      }
      
      assertTrue(newNode!=null);
      DatanodeInfo source=null;
      ArrayList<DatanodeInfo> proxies = new ArrayList<DatanodeInfo>(2);
      for(DatanodeInfo node:datanodes) {
        if(node != newNode) {
          if( node.getNetworkLocation().equals(newNode.getNetworkLocation())) {
            source = node;
          } else {
            proxies.add( node );
          }
        }
      }
      assertTrue(source!=null && proxies.size()==2);
      
      // start to replace the block
      // case 1: proxySource does not contain the block
      LOG.info("Testcase 1: Proxy " + newNode.getName() 
          + " does not contain the block " + b.getBlockName() );
      assertFalse(replaceBlock(b, source, newNode, proxies.get(0)));
      // case 2: destination contains the block
      LOG.info("Testcase 2: Destination " + proxies.get(1).getName() 
          + " contains the block " + b.getBlockName() );
      assertFalse(replaceBlock(b, source, proxies.get(0), proxies.get(1)));
      // case 3: correct case
      LOG.info("Testcase 3: Proxy=" + source.getName() + " source=" + 
          proxies.get(0).getName() + " destination=" + newNode.getName() );
      assertTrue(replaceBlock(b, source, proxies.get(0), newNode));
      // block locations should contain two proxies and newNode
      checkBlocks(new DatanodeInfo[]{newNode, proxies.get(0), proxies.get(1)},
          fileName.toString(), 
          DEFAULT_BLOCK_SIZE, REPLICATION_FACTOR, client);
      // case 4: proxies.get(0) is not a valid del hint
      LOG.info("Testcase 4: invalid del hint " + proxies.get(0).getName() );
      assertTrue(replaceBlock(b, proxies.get(1), proxies.get(0), source));
      /* block locations should contain two proxies,
       * and either of source or newNode
       */
      checkBlocks(proxies.toArray(new DatanodeInfo[proxies.size()]), 
          fileName.toString(), 
          DEFAULT_BLOCK_SIZE, REPLICATION_FACTOR, client);
    } finally {
      cluster.shutdown();
    }
  }
  
  /* check if file's blocks exist at includeNodes */
  private void checkBlocks(DatanodeInfo[] includeNodes, String fileName, 
      long fileLen, short replFactor, DFSClient client) throws IOException {
    Boolean notDone;
    do {
      try {
        Thread.sleep(100);
      } catch(InterruptedException e) {
      }
      List<LocatedBlock> blocks = client.namenode.
      getBlockLocations(fileName, 0, fileLen).getLocatedBlocks();
      assertEquals(1, blocks.size());
      DatanodeInfo[] nodes = blocks.get(0).getLocations();
      notDone = (nodes.length != replFactor);
      if (notDone) {
        LOG.info("Expected replication factor is " + replFactor +
            " but the real replication factor is " + nodes.length );
      } else {
        List<DatanodeInfo> nodeLocations = Arrays.asList(nodes);
        for (DatanodeInfo node : includeNodes) {
          if (!nodeLocations.contains(node) ) {
            notDone=true; 
            LOG.info("Block is not located at " + node.getName() );
            break;
          }
        }
      }
    } while(notDone);
  }

  /* Copy a block from sourceProxy to destination. If the block becomes
   * over-replicated, preferably remove it from source.
   * 
   * Return true if a block is successfully copied; otherwise false.
   */
  private boolean replaceBlock( Block block, DatanodeInfo source,
      DatanodeInfo sourceProxy, DatanodeInfo destination) throws IOException {
    Socket sock = new Socket();
    sock.connect(NetUtils.createSocketAddr(
        destination.getName()), HdfsConstants.READ_TIMEOUT);
    sock.setKeepAlive(true);
    // sendRequest
    DataOutputStream out = new DataOutputStream(sock.getOutputStream());
    out.writeShort(DataTransferProtocol.DATA_TRANSFER_VERSION);
    out.writeByte(DataTransferProtocol.OP_REPLACE_BLOCK);
    out.writeLong(block.getBlockId());
    out.writeLong(block.getGenerationStamp());
    Text.writeString(out, source.getStorageID());
    sourceProxy.write(out);
    BlockTokenSecretManager.DUMMY_TOKEN.write(out);
    out.flush();
    // receiveResponse
    DataInputStream reply = new DataInputStream(sock.getInputStream());

    short status = reply.readShort();
    if(status == DataTransferProtocol.OP_STATUS_SUCCESS) {
      return true;
    }
    return false;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    (new TestBlockReplacement()).testBlockReplacement();
  }

}
