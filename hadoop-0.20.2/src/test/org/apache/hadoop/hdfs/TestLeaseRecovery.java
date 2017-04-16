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
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.TestInterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetTestUtil;

import org.apache.hadoop.hdfs.server.protocol.BlockMetaDataInfo;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;

public class TestLeaseRecovery extends junit.framework.TestCase {
  static final int BLOCK_SIZE = 1024;
  static final short REPLICATION_NUM = (short)3;

  static void checkMetaInfo(Block b, InterDatanodeProtocol idp
      ) throws IOException {
    TestInterDatanodeProtocol.checkMetaInfo(b, idp, null);
  }
  
  static int min(Integer... x) {
    int m = x[0];
    for(int i = 1; i < x.length; i++) {
      if (x[i] < m) {
        m = x[i];
      }
    }
    return m;
  }

  public void testBlockSynchronization() throws Exception {
    runTestBlockSynchronization(false);
  }
  public void testBlockSynchronizationWithZeroBlock() throws Exception {
    runTestBlockSynchronization(true);
  }


  /**
   * The following test first creates a file with a few blocks.
   * It randomly truncates the replica of the last block stored in each datanode.
   * Finally, it triggers block synchronization to synchronize all stored block.
   * @param forceOneBlockToZero if true, will truncate one block to 0 length
   */
  public void runTestBlockSynchronization(boolean forceOneBlockToZero)
  throws Exception {
    final int ORG_FILE_SIZE = 3000; 
    Configuration conf = new Configuration();
    conf.setLong("dfs.block.size", BLOCK_SIZE);
    conf.setBoolean("dfs.support.broken.append", true);
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster(conf, 5, true, null);
      cluster.waitActive();

      //create a file
      DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();
      String filestr = "/foo";
      Path filepath = new Path(filestr);
      DFSTestUtil.createFile(dfs, filepath, ORG_FILE_SIZE, REPLICATION_NUM, 0L);
      assertTrue(dfs.dfs.exists(filestr));
      DFSTestUtil.waitReplication(dfs, filepath, REPLICATION_NUM);

      //get block info for the last block
      LocatedBlock locatedblock = TestInterDatanodeProtocol.getLastLocatedBlock(
          dfs.dfs.namenode, filestr);
      DatanodeInfo[] datanodeinfos = locatedblock.getLocations();
      assertEquals(REPLICATION_NUM, datanodeinfos.length);

      //connect to data nodes
      InterDatanodeProtocol[] idps = new InterDatanodeProtocol[REPLICATION_NUM];
      DataNode[] datanodes = new DataNode[REPLICATION_NUM];
      for(int i = 0; i < REPLICATION_NUM; i++) {
        idps[i] = DataNode.createInterDataNodeProtocolProxy(datanodeinfos[i], conf, 0, false);
        datanodes[i] = cluster.getDataNode(datanodeinfos[i].getIpcPort());
        assertTrue(datanodes[i] != null);
      }
      
      //verify BlockMetaDataInfo
      Block lastblock = locatedblock.getBlock();
      DataNode.LOG.info("newblocks=" + lastblock);
      for(int i = 0; i < REPLICATION_NUM; i++) {
        checkMetaInfo(lastblock, idps[i]);
      }

      //setup random block sizes 
      int lastblocksize = ORG_FILE_SIZE % BLOCK_SIZE;
      Integer[] newblocksizes = new Integer[REPLICATION_NUM];
      for(int i = 0; i < REPLICATION_NUM; i++) {
        newblocksizes[i] = AppendTestUtil.nextInt(lastblocksize);
      }
      if (forceOneBlockToZero) {
        newblocksizes[0] = 0;
      }
      DataNode.LOG.info("newblocksizes = " + Arrays.asList(newblocksizes)); 

      //update blocks with random block sizes
      Block[] newblocks = new Block[REPLICATION_NUM];
      for(int i = 0; i < REPLICATION_NUM; i++) {
        DataNode dn = datanodes[i];
        FSDatasetTestUtil.truncateBlock(dn, lastblock, newblocksizes[i]);
        newblocks[i] = new Block(lastblock.getBlockId(), newblocksizes[i],
            lastblock.getGenerationStamp());
        checkMetaInfo(newblocks[i], idps[i]);
      }

      DataNode.LOG.info("dfs.dfs.clientName=" + dfs.dfs.clientName);
      cluster.getNameNode().append(filestr, dfs.dfs.clientName);

      //block synchronization
      final int primarydatanodeindex = AppendTestUtil.nextInt(datanodes.length);
      DataNode.LOG.info("primarydatanodeindex  =" + primarydatanodeindex);
      DataNode primary = datanodes[primarydatanodeindex];
      DataNode.LOG.info("primary.dnRegistration=" + primary.dnRegistration);
      primary.recoverBlocks(new Block[]{lastblock}, new DatanodeInfo[][]{datanodeinfos}).join();

      BlockMetaDataInfo[] updatedmetainfo = new BlockMetaDataInfo[REPLICATION_NUM];
      int minsize = min(newblocksizes);
      long currentGS = cluster.getNameNode().namesystem.getGenerationStamp();
      lastblock.setGenerationStamp(currentGS);
      for(int i = 0; i < REPLICATION_NUM; i++) {
        updatedmetainfo[i] = idps[i].getBlockMetaDataInfo(lastblock);
        assertEquals(lastblock.getBlockId(), updatedmetainfo[i].getBlockId());
        assertEquals(minsize, updatedmetainfo[i].getNumBytes());
        assertEquals(currentGS, updatedmetainfo[i].getGenerationStamp());
      }
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
}
