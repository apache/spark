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
import java.net.InetSocketAddress;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import junit.framework.TestCase;
/**
 * This class tests if block replacement request to data nodes work correctly.
 */
public class TestGetBlocks extends TestCase {
  /** test getBlocks */
  public void testGetBlocks() throws Exception {
    final Configuration CONF = new Configuration();

    final short REPLICATION_FACTOR = (short)2;
    final int DEFAULT_BLOCK_SIZE = 1024;
    final Random r = new Random();
    
    CONF.setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    MiniDFSCluster cluster = new MiniDFSCluster(
          CONF, REPLICATION_FACTOR, true, null );
    try {
      cluster.waitActive();
      
      // create a file with two blocks
      FileSystem fs = cluster.getFileSystem();
      FSDataOutputStream out = fs.create(new Path("/tmp.txt"),
          REPLICATION_FACTOR);
      byte [] data = new byte[1024];
      long fileLen = 2*DEFAULT_BLOCK_SIZE;
      long bytesToWrite = fileLen;
      while( bytesToWrite > 0 ) {
        r.nextBytes(data);
        int bytesToWriteNext = (1024<bytesToWrite)?1024:(int)bytesToWrite;
        out.write(data, 0, bytesToWriteNext);
        bytesToWrite -= bytesToWriteNext;
      }
      out.close();

      // get blocks & data nodes
      List<LocatedBlock> locatedBlocks;
      DatanodeInfo[] dataNodes=null;
      boolean notWritten;
      do {
        final DFSClient dfsclient = new DFSClient(NameNode.getAddress(CONF), CONF);
        locatedBlocks = dfsclient.namenode.
          getBlockLocations("/tmp.txt", 0, fileLen).getLocatedBlocks();
        assertEquals(2, locatedBlocks.size());
        notWritten = false;
        for(int i=0; i<2; i++) {
          dataNodes = locatedBlocks.get(i).getLocations();
          if(dataNodes.length != REPLICATION_FACTOR) {
            notWritten = true;
            try {
              Thread.sleep(10);
            } catch(InterruptedException e) {
            }
            break;
          }
        }
      } while(notWritten);
      
      // get RPC client to namenode
      InetSocketAddress addr = new InetSocketAddress("localhost",
          cluster.getNameNodePort());
      NamenodeProtocol namenode = (NamenodeProtocol) RPC.getProxy(
          NamenodeProtocol.class, NamenodeProtocol.versionID, addr,
          UserGroupInformation.getCurrentUser(), CONF,
          NetUtils.getDefaultSocketFactory(CONF));

      // get blocks of size fileLen from dataNodes[0]
      BlockWithLocations[] locs;
      locs = namenode.getBlocks(dataNodes[0], fileLen).getBlocks();
      assertEquals(locs.length, 2);
      assertEquals(locs[0].getDatanodes().length, 2);
      assertEquals(locs[1].getDatanodes().length, 2);

      // get blocks of size BlockSize from dataNodes[0]
      locs = namenode.getBlocks(dataNodes[0], DEFAULT_BLOCK_SIZE).getBlocks();
      assertEquals(locs.length, 1);
      assertEquals(locs[0].getDatanodes().length, 2);

      // get blocks of size 1 from dataNodes[0]
      locs = namenode.getBlocks(dataNodes[0], 1).getBlocks();
      assertEquals(locs.length, 1);
      assertEquals(locs[0].getDatanodes().length, 2);

      // get blocks of size 0 from dataNodes[0]
      getBlocksWithException(namenode, dataNodes[0], 0);     

      // get blocks of size -1 from dataNodes[0]
      getBlocksWithException(namenode, dataNodes[0], -1);

      // get blocks of size BlockSize from a non-existent datanode
      getBlocksWithException(namenode, new DatanodeInfo(), 2);
    } finally {
      cluster.shutdown();
    }
  }

  private void getBlocksWithException(NamenodeProtocol namenode,
                                      DatanodeInfo datanode,
                                      long size) throws IOException {
    boolean getException = false;
    try {
        namenode.getBlocks(new DatanodeInfo(), 2);
    } catch(RemoteException e) {
      getException = true;
      assertTrue(e.getMessage().contains("IllegalArgumentException"));
    }
    assertTrue(getException);
  }
 
  public void testGenerationStampWildCard() {
    Map<Block, Long> map = new HashMap<Block, Long>();
    final Random RAN = new Random();
    final long seed = RAN.nextLong();
    System.out.println("seed=" +  seed);
    RAN.setSeed(seed);

    long[] blkids = new long[10]; 
    for(int i = 0; i < blkids.length; i++) {
      blkids[i] = 1000L + RAN.nextInt(100000);
      map.put(new Block(blkids[i], 0, blkids[i]), blkids[i]);
    }
    System.out.println("map=" + map.toString().replace(",", "\n  "));
    
    for(int i = 0; i < blkids.length; i++) {
      Block b = new Block(blkids[i], 0, GenerationStamp.WILDCARD_STAMP);
      Long v = map.get(b);
      System.out.println(b + " => " + v);
      assertEquals(blkids[i], v.longValue());
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    (new TestGetBlocks()).testGetBlocks();
  }

}
