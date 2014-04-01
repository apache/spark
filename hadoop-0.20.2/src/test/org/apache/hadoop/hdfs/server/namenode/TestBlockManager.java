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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

public class TestBlockManager {
  {
    ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
  }

  private final List<DatanodeDescriptor> nodes = Arrays.asList(
      new DatanodeDescriptor[] {
      new DatanodeDescriptor(new DatanodeID("h1:5020"), "/rackA"),
      new DatanodeDescriptor(new DatanodeID("h2:5020"), "/rackA"),
      new DatanodeDescriptor(new DatanodeID("h3:5020"), "/rackA"),
      new DatanodeDescriptor(new DatanodeID("h4:5020"), "/rackB"),
      new DatanodeDescriptor(new DatanodeID("h5:5020"), "/rackB"),
      new DatanodeDescriptor(new DatanodeID("h6:5020"), "/rackB")
      });
  private final List<DatanodeDescriptor> rackA = nodes.subList(0, 3);
  private final List<DatanodeDescriptor> rackB = nodes.subList(3, 6);
  
  /**
   * Some of these tests exercise code which has some randomness involved -
   * ie even if there's a bug, they may pass because the random node selection
   * chooses the correct result.
   * 
   * Since they're true unit tests and run quickly, we loop them a number
   * of times trying to trigger the incorrect behavior.
   */
  private static final int NUM_TEST_ITERS = 30;
  
  private static final int BLOCK_SIZE = 64*1024;
  
  private Configuration conf;
  private FSNamesystem fsn;
  private MiniDFSCluster cluster;

  @Before
  public void setupMockCluster() throws IOException {
    conf = new Configuration();
    cluster = new MiniDFSCluster(conf, 0, true, null);
    fsn = cluster.getNameNode().getNamesystem();
  }
  
  @After
  public void tearDownCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  private void addNodes(Iterable<DatanodeDescriptor> nodesToAdd) throws IOException {
    NetworkTopology cluster = fsn.clusterMap;
    // construct network topology
    for (DatanodeDescriptor dn : nodesToAdd) {
      cluster.add(dn);
      dn.updateHeartbeat(
          2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0, 0);
      fsn.unprotectedAddDatanode(dn);
      fsn.unprotectedRegisterInHeartbeatMap(dn);
      assertNotNull(fsn.getDatanode(dn));
    }
  }


  /**
   * Test that replication of under-replicated blocks is detected
   * and basically works
   */
  @Test
  public void testBasicReplication() throws Exception {
    addNodes(nodes);
    for (int i = 0; i < NUM_TEST_ITERS; i++) {
      doBasicTest(i);
    }
  }
  
  /**
   * Regression test for HDFS-1480
   * - Cluster has 2 racks, A and B, each with three nodes.
   * - Block initially written on A1, A2, B1
   * - Admin decommissions two of these nodes (let's say A1 and A2 but it doesn't matter)
   * - Re-replication should respect rack policy
   */
  @Test
  public void testTwoOfThreeNodesDecomissioned() throws Exception {
    addNodes(nodes);
    for (int i = 0; i < NUM_TEST_ITERS; i++) {
      doTestTwoOfThreeNodesDecomissioned(i);
    }
  }
  
  @Test
  public void testAllNodesHoldingReplicasDecomissioned() throws Exception {
    addNodes(nodes);
    for (int i = 0; i < NUM_TEST_ITERS; i++) {
      doTestAllNodesHoldingReplicasDecomissioned(i);
    }
  }

  @Test
  public void testOneOfTwoRacksDecomissioned() throws Exception {
    addNodes(nodes);
    for (int i = 0; i < NUM_TEST_ITERS; i++) {
      doTestOneOfTwoRacksDecomissioned(i);
    }
  }


  /**
   * Unit test version of testSufficientlyReplBlocksUsesNewRack
   * 
   * This test is currently ignored, since 0.20 doesn't check replication
   * policy on sufficiently replicated blocks. If an additional rack is
   * added to a 1-rack cluster, the replication level needs to be boosted
   * and brought back down to attain the proper policy.
   **/
  @Test
  @Ignore
  public void testSufficientlyReplBlocksUsesNewRack() throws Exception {
    addNodes(nodes);
    for (int i = 0; i < NUM_TEST_ITERS; i++) {
      doTestSufficientlyReplBlocksUsesNewRack(i);
    }
  }

  private void doTestSufficientlyReplBlocksUsesNewRack(int testIndex) {
    // Originally on only nodes in rack A.
    List<DatanodeDescriptor> origNodes = rackA;
    BlockInfo blockInfo = addBlockOnNodes((long)testIndex, origNodes);
    DatanodeInfo[] pipeline = scheduleSingleReplication(blockInfo);
    
    assertEquals(2, pipeline.length); // single new copy
    assertTrue("Source of replication should be one of the nodes the block " +
        "was on. Was: " + pipeline[0],
        nodeInList(pipeline[0], origNodes));
    assertTrue("Destination of replication should be on the other rack. " +
        "Was: " + pipeline[1],
        nodeInList(pipeline[1], rackB));
  }
  
  private void doBasicTest(int testIndex) {
    List<DatanodeDescriptor> origNodes = nodesAtIndexes(0, 1);
    BlockInfo blockInfo = addBlockOnNodes((long)testIndex, origNodes);

    DatanodeInfo[] pipeline = scheduleSingleReplication(blockInfo);
    assertEquals(2, pipeline.length);
    assertTrue("Source of replication should be one of the nodes the block " +
    		"was on. Was: " + pipeline[0],
    		nodeInList(pipeline[0], origNodes));
    assertTrue("Destination of replication should be on the other rack. " +
        "Was: " + pipeline[1],
        nodeInList(pipeline[1], rackB));
  }
  
  private void doTestTwoOfThreeNodesDecomissioned(int testIndex) throws Exception {
    // Block originally on A1, A2, B1
    List<DatanodeDescriptor> origNodes = nodesAtIndexes(0, 1, 3);
    BlockInfo blockInfo = addBlockOnNodes(testIndex, origNodes);
    
    // Decommission two of the nodes (A1, A2)
    List<DatanodeDescriptor> decomNodes = startDecommission(0, 1);
    
    DatanodeInfo[] pipeline = scheduleSingleReplication(blockInfo);
    assertTrue("Source of replication should be one of the nodes the block " +
        "was on. Was: " + pipeline[0],
        nodeInList(pipeline[0], origNodes));
    assertEquals("Should have two targets", 3, pipeline.length);
    
    boolean foundOneOnRackA = false;
    for (int i = 1; i < pipeline.length; i++) {
      DatanodeInfo target = pipeline[i];
      if (nodeInList(target, rackA)) {
        foundOneOnRackA = true;
      }
      assertFalse(nodeInList(target, decomNodes));
      assertFalse(nodeInList(target, origNodes));
    }
    
    assertTrue("Should have at least one target on rack A. Pipeline: " +
        StringUtils.joinObjects(",", Arrays.asList(pipeline)),
        foundOneOnRackA);
  }
  
  private boolean nodeInList(DatanodeInfo node,
      List<DatanodeDescriptor> nodeList) {
    for (DatanodeDescriptor candidate : nodeList) {
      if (node.getName().equals(candidate.getName())) {
        return true;
      }
    }
    return false;
  }

  private void doTestAllNodesHoldingReplicasDecomissioned(int testIndex) throws Exception {
    // Block originally on A1, A2, B1
    List<DatanodeDescriptor> origNodes = nodesAtIndexes(0, 1, 3);
    BlockInfo blockInfo = addBlockOnNodes(testIndex, origNodes);
    
    // Decommission all of the nodes
    List<DatanodeDescriptor> decomNodes = startDecommission(0, 1, 3);
    
    DatanodeInfo[] pipeline = scheduleSingleReplication(blockInfo);
    assertTrue("Source of replication should be one of the nodes the block " +
        "was on. Was: " + pipeline[0],
        nodeInList(pipeline[0], origNodes));
    assertEquals("Should have three targets", 4, pipeline.length);
    
    boolean foundOneOnRackA = false;
    boolean foundOneOnRackB = false;
    for (int i = 1; i < pipeline.length; i++) {
      DatanodeInfo target = pipeline[i];
      if (nodeInList(target, rackA)) {
        foundOneOnRackA = true;
      } else if (nodeInList(target, rackB)) {
        foundOneOnRackB = true;
      }
      assertFalse(nodeInList(target, decomNodes));
      assertFalse(nodeInList(target, origNodes));
    }
    
    assertTrue("Should have at least one target on rack A. Pipeline: " +
        StringUtils.joinObjects(",", Arrays.asList(pipeline)),
        foundOneOnRackA);
    assertTrue("Should have at least one target on rack B. Pipeline: " +
        StringUtils.joinObjects(",", Arrays.asList(pipeline)),
        foundOneOnRackB);
  }
  
  private void doTestOneOfTwoRacksDecomissioned(int testIndex) throws Exception {
    System.out.println("Begin iter " + testIndex);
    // Block originally on A1, A2, B1
    List<DatanodeDescriptor> origNodes = nodesAtIndexes(0, 1, 3);
    BlockInfo blockInfo = addBlockOnNodes(testIndex, origNodes);
    
    // Decommission all of the nodes in rack A
    List<DatanodeDescriptor> decomNodes = startDecommission(0, 1, 2);
    
    DatanodeInfo[] pipeline = scheduleSingleReplication(blockInfo);
    assertTrue("Source of replication should be one of the nodes the block " +
        "was on. Was: " + pipeline[0],
        nodeInList(pipeline[0], origNodes));
    assertEquals("Should have 2 targets", 3, pipeline.length);
    
    boolean foundOneOnRackB = false;
    for (int i = 1; i < pipeline.length; i++) {
      DatanodeInfo target = pipeline[i];
      if (nodeInList(target, rackB)) {
        foundOneOnRackB = true;
      }
      assertFalse(nodeInList(target, decomNodes));
      assertFalse(nodeInList(target, origNodes));
    }
    
    assertTrue("Should have at least one target on rack B. Pipeline: " +
        StringUtils.joinObjects(",", Arrays.asList(pipeline)),
        foundOneOnRackB);    
  }

  private List<DatanodeDescriptor> nodesAtIndexes(int ... indexes) {
    List<DatanodeDescriptor> ret = new ArrayList<DatanodeDescriptor>();
    for (int idx : indexes) {
      ret.add(nodes.get(idx));
    }
    return ret;
  }
  
  private List<DatanodeDescriptor> startDecommission(int ... indexes) {
    List<DatanodeDescriptor> nodes = nodesAtIndexes(indexes);
    for (DatanodeDescriptor node : nodes) {
      node.startDecommission();
    }
    return nodes;
  }
  
  private BlockInfo addBlockOnNodes(long blockId, List<DatanodeDescriptor> nodes) {
    INodeFile iNode = Mockito.mock(INodeFile.class);
    Mockito.doReturn((short)3).when(iNode).getReplication();
    Block block = new Block(blockId);

    BlockInfo blockInfo = fsn.blocksMap.addINode(block, iNode);

    // Block originally on A1, A2, B1
    for (DatanodeDescriptor dn : nodes) {
      blockInfo.addNode(dn);
    }

    return blockInfo;
  }
  
  private DatanodeInfo[] scheduleSingleReplication(Block block) {
    assertEquals("Block not initially pending replication",
        0, fsn.pendingReplications.getNumReplicas(block));
    assertTrue("computeReplicationWork should indicate replication is needed",
        fsn.computeReplicationWorkForBlock(block, 1));
    assertTrue("replication is pending after work is computed",
        fsn.pendingReplications.getNumReplicas(block) > 0);
    
    List<PendingReplPipeline> pipelines = getAllPendingReplications();
      
    assertEquals(1, pipelines.size());
    assertEquals(block, pipelines.get(0).block);
    return pipelines.get(0).pipeline;
  }

  private List<PendingReplPipeline> getAllPendingReplications() {
    List<PendingReplPipeline> pendingPipelines = new ArrayList<PendingReplPipeline>();
    
    for (DatanodeDescriptor dn : nodes) {
      BlockCommand replCommand = dn.getReplicationCommand(10);
      if (replCommand == null) continue;

      Block[] blocks = replCommand.getBlocks();
      DatanodeInfo[][] allTargets = replCommand.getTargets();
      
      for (int i = 0; i < blocks.length; i++) {
        DatanodeInfo[] targets = allTargets[i];
        Block block = blocks[i];
                
        DatanodeInfo[] pipeline = new DatanodeInfo[1 + targets.length];
        pipeline[0] = dn;
        System.arraycopy(targets, 0, pipeline, 1, targets.length);
        pendingPipelines.add(new PendingReplPipeline(block, pipeline));
      }
    }
    return pendingPipelines;
  }
  
  private static class PendingReplPipeline {
    final Block block;
    final DatanodeInfo[] pipeline;
    public PendingReplPipeline(Block block, DatanodeInfo[] pipeline) {
      super();
      this.block = block;
      this.pipeline = pipeline;
    }
  }
}
