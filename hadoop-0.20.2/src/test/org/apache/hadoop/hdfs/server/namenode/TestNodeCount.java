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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.NumberReplicas;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeActivtyMBean;

import junit.framework.TestCase;

/**
 * Test if live nodes count per node is correct 
 * so NN makes right decision for under/over-replicated blocks
 */
public class TestNodeCount extends TestCase {
  static final Log LOG = LogFactory.getLog(TestNodeCount.class);

  public void testInvalidateMultipleReplicas() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster =
      new MiniDFSCluster(conf, 5, true, null);
    final int FILE_LEN = 123;
    final String pathStr = "/testInvalidateMultipleReplicas";
    try {
      FileSystem fs = cluster.getFileSystem();
      Path path = new Path(pathStr);
      cluster.waitActive();
      // create a small file on 3 nodes
      DFSTestUtil.createFile(fs, path, 123, (short)3, 0);
      DFSTestUtil.waitReplication(fs, path, (short)3);
      NameNode nn = cluster.getNameNode();
      LocatedBlocks located = nn.getBlockLocations(pathStr, 0, FILE_LEN);
      
      // Get the original block locations
      List<LocatedBlock> blocks = located.getLocatedBlocks();
      LocatedBlock firstBlock = blocks.get(0);
      
      DatanodeInfo[] locations = firstBlock.getLocations();
      assertEquals("Should have 3 good blocks", 3, locations.length);
      nn.getNamesystem().stallReplicationWork();
      
      DatanodeInfo[] badLocations = new DatanodeInfo[2];
      badLocations[0] = locations[0];
      badLocations[1] = locations[1];
      
      // Report some blocks corrupt
      LocatedBlock badLBlock = new LocatedBlock(
          firstBlock.getBlock(), badLocations);
      
      nn.reportBadBlocks(new LocatedBlock[] {badLBlock});
      
      nn.getNamesystem().restartReplicationWork();
      
      DFSTestUtil.waitReplication(fs, path, (short)3);
      NumberReplicas num = nn.getNamesystem().countNodes(
          firstBlock.getBlock());
      assertEquals(0, num.corruptReplicas());

    } finally {
      cluster.shutdown();
    }
  }
  
  public void testNodeCount() throws Exception {
    // start a mini dfs cluster of 2 nodes
    final Configuration conf = new Configuration();
    final short REPLICATION_FACTOR = (short)2;
    final MiniDFSCluster cluster = 
      new MiniDFSCluster(conf, REPLICATION_FACTOR, true, null);
    try {
      final FSNamesystem namesystem = cluster.getNameNode().namesystem;
      final FileSystem fs = cluster.getFileSystem();
      
      // populate the cluster with a one block file
      final Path FILE_PATH = new Path("/testfile");
      DFSTestUtil.createFile(fs, FILE_PATH, 1L, REPLICATION_FACTOR, 1L);
      DFSTestUtil.waitReplication(fs, FILE_PATH, REPLICATION_FACTOR);
      Block block = DFSTestUtil.getFirstBlock(fs, FILE_PATH);

      // keep a copy of all datanode descriptor
      DatanodeDescriptor[] datanodes = (DatanodeDescriptor[])
         namesystem.heartbeats.toArray(new DatanodeDescriptor[REPLICATION_FACTOR]);
      
      // start two new nodes
      cluster.startDataNodes(conf, 2, true, null, null);
      cluster.waitActive(false);
      
      LOG.info("Bringing down first DN");
      // bring down first datanode
      DatanodeDescriptor datanode = datanodes[0];
      DataNodeProperties dnprop = cluster.stopDataNode(datanode.getName());
      // make sure that NN detects that the datanode is down
      synchronized (namesystem.heartbeats) {
        datanode.setLastUpdate(0); // mark it dead
        namesystem.heartbeatCheck();
      }

      LOG.info("Waiting for block to be replicated");
      // the block will be replicated
      DFSTestUtil.waitReplication(fs, FILE_PATH, REPLICATION_FACTOR);

      LOG.info("Restarting first datanode");
      // restart the first datanode
      cluster.restartDataNode(dnprop);
      cluster.waitActive(false);

      LOG.info("Waiting for excess replicas to be detected");

      // check if excessive replica is detected
      waitForExcessReplicasToChangeTo(namesystem, block, 1);

      LOG.info("Finding a non-excess node");
      // find out a non-excess node
      Iterator<DatanodeDescriptor> iter = namesystem.blocksMap.nodeIterator(block);
      DatanodeDescriptor nonExcessDN = null;
      while (iter.hasNext()) {
        DatanodeDescriptor dn = iter.next();
        Collection<Block> blocks = namesystem.excessReplicateMap.get(dn.getStorageID());
        if (blocks == null || !blocks.contains(block) ) {
          nonExcessDN = dn;
          break;
        }
      }
      assertTrue(nonExcessDN!=null);

      LOG.info("Stopping non-excess node: " + nonExcessDN);
      // bring down non excessive datanode
      dnprop = cluster.stopDataNode(nonExcessDN.getName());
      // make sure that NN detects that the datanode is down
      synchronized (namesystem.heartbeats) {
        nonExcessDN.setLastUpdate(0); // mark it dead
        namesystem.heartbeatCheck();
      }

      LOG.info("Waiting for live replicas to hit repl factor");
      // The block should be replicated
      NumberReplicas num;
      do {
        num = namesystem.countNodes(block);
      } while (num.liveReplicas() != REPLICATION_FACTOR);

      LOG.info("Restarting first DN");
      // restart the first datanode
      cluster.restartDataNode(dnprop);
      cluster.waitActive(false);

      Thread.sleep(3000);

      LOG.info("Waiting for excess replicas to be detected");
      // check if excessive replica is detected
      waitForExcessReplicasToChangeTo(namesystem, block, 2);
    } finally {
      cluster.shutdown();
    }
  }

  private void waitForExcessReplicasToChangeTo(
    FSNamesystem namesystem,
    Block block,
    int waitForReplicas) throws Exception
  {
    NumberReplicas num;
    long startChecking = System.currentTimeMillis();
    do {
      synchronized (namesystem) {
        num = namesystem.countNodes(block);
      }
      LOG.info("Waiting for excess replicas == " + waitForReplicas +
       " - current: " + num);
      Thread.sleep(200);
      if (System.currentTimeMillis() - startChecking > 30000) {
        namesystem.metaSave("TestNodeCount.meta");
        LOG.warn("Dumping meta into log directory");
        fail("Timed out waiting for excess replicas to change");
      }

    } while (num.excessReplicas() != waitForReplicas);
  }
    
}
