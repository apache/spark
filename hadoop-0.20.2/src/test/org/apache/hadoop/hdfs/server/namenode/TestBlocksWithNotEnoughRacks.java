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

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;

import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Tests that rack policy is properly maintained during various operations.
 * 
 * NOTE: many of the test cases here are Ignored in CDH but not in trunk.
 * The reasoning is that trunk has HDFS-15, which causes rack policy to
 * get checked on every replication decision. This is a potential performance
 * issue that has not seen enough scale testing in trunk yet, so it
 * has not been backported. Thus, in any case where replication has been
 * reached the desired number of replicas, new replicas will not be made
 * just to satisfy the rack policy. If HDFS-15 is backported, these tests
 * may be re-enabled.
 */
public class TestBlocksWithNotEnoughRacks {
  public static final Log LOG = LogFactory.getLog(TestBlocksWithNotEnoughRacks.class);
  static {
    ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);        
  }

  /*
   * Wait up to 10s for the given block to be replicated across 
   * the requested number of racks, with the requested number of 
   * replicas, and the requested number of replicas still needed. 
   */
  private void waitForReplication(FSNamesystem ns, Block b, int racks, 
      int replicas, int neededReplicas) throws Exception {
    int curRacks = ns.getNumberOfRacks(b);
    int curReplicas = ns.countNodes(b).liveReplicas();
    int curNeededReplicas = ns.neededReplications.size();
    int count = 0;

    while (curRacks < racks || curReplicas < replicas || 
           curNeededReplicas > neededReplicas) {
      if (++count == 10) { 
        break;
      }
      Thread.sleep(1000);
      curRacks = ns.getNumberOfRacks(b);
      curReplicas = ns.countNodes(b).liveReplicas();
      curNeededReplicas = ns.neededReplications.size();
    }
    assertEquals(racks, curRacks);
    assertEquals(replicas, curReplicas);
    assertEquals(neededReplicas, curNeededReplicas);
  }

  /*
   * Wait for the given DN (host:port) to be decommissioned.
   */
  private void waitForDecommission(FileSystem fs, String name) 
      throws Exception {
    DatanodeInfo dn = null;
    while (dn == null || dn.isDecommissionInProgress() || !dn.isDecommissioned()) {
      Thread.sleep(1000);
      DistributedFileSystem dfs = (DistributedFileSystem)fs;
      for (DatanodeInfo info : dfs.getDataNodeStats()) {
        if (name.equals(info.getName())) {
          dn = info;
        }
      }      
    }
  }

  /*
   * Return a configuration object with low timeouts for testing and 
   * a topology script set (which enables rack awareness).  
   */
  private Configuration getConf() {
    Configuration conf = new Configuration();
    conf.setLong("dfs.heartbeat.interval", 1L);
    conf.setInt("dfs.replication.interval", 1);
    conf.setInt("dfs.replication.pending.timeout.sec", 1);
    conf.setLong("dfs.blockreport.intervalMsec", 1000L);
    conf.set("topology.script.file.name", "xyz");
    return conf;
  }

  /*
   * Write the given string to the given file.
   */
  private void writeFile(FileSystem fs, Path p, String s) throws Exception {
    if (fs.exists(p)) {
      fs.delete(p, true);
    }
    FSDataOutputStream stm = fs.create(p);
    stm.writeBytes(s);
    stm.writeBytes("\n");
    stm.close();
  }

  /*
   * Creates a block with all datanodes on the same rack, though the block
   * is sufficiently replicated. Adds an additional datanode on a new rack. 
   * The block should be replicated to the new rack.
   */
  @Test
  @Ignore("See javadoc at top of class")
  public void testSufficientlyReplBlocksUsesNewRack() throws Exception {
    Configuration conf = getConf();
    final short REPLICATION_FACTOR = 3;
    final Path filePath = new Path("/testFile");
    // All datanodes are on the same rack
    String racks[] = {"/rack1", "/rack1", "/rack1"};
    MiniDFSCluster cluster = new MiniDFSCluster(conf, racks.length, true, racks);
    final FSNamesystem ns = cluster.getNameNode().getNamesystem();

    try {
      // Create a file with one block with a replication factor of 3
      final FileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, filePath, 1L, REPLICATION_FACTOR, 1L);
      Block b = DFSTestUtil.getFirstBlock(fs, filePath);
      waitForReplication(ns, b, 1, REPLICATION_FACTOR, 1);      

      // Add a new datanode on a different rack
      String newRacks[] = {"/rack2"};
      cluster.startDataNodes(conf, 1, true, null, newRacks);
      cluster.waitActive();

      waitForReplication(ns, b, 2, REPLICATION_FACTOR, 0);
    } finally {
      cluster.shutdown();
    }
  }

  /*
   * Creates a block with all datanodes on the same rack. Add additional
   * datanodes on a different rack and increase the replication factor, 
   * making sure there are enough replicas across racks. If the previous
   * test passes this one should too, however this test may pass when
   * the previous one fails because the replication code is explicitly
   * triggered by setting the replication factor.
   */
  @Test
  @Ignore("See javadoc at top of class")
  public void testUnderReplicatedUsesNewRacks() throws Exception {
    Configuration conf = getConf();
    short REPLICATION_FACTOR = 3;
    final Path filePath = new Path("/testFile");
    // All datanodes are on the same rack
    String racks[] = {"/rack1", "/rack1", "/rack1", "/rack1", "/rack1"};
    MiniDFSCluster cluster = new MiniDFSCluster(conf, racks.length, true, racks);
    final FSNamesystem ns = cluster.getNameNode().getNamesystem();

    try {
      // Create a file with one block
      final FileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, filePath, 1L, REPLICATION_FACTOR, 1L);
      Block b = DFSTestUtil.getFirstBlock(fs, filePath);
      waitForReplication(ns, b, 1, REPLICATION_FACTOR, 1);
      
      // Add new datanodes on a different rack and increase the
      // replication factor so the block is underreplicated and make
      // sure at least one of the hosts on the new rack is used. 
      String newRacks[] = {"/rack2", "/rack2"};
      cluster.startDataNodes(conf, 2, true, null, newRacks);
      REPLICATION_FACTOR = 5;
      ns.setReplication("/testFile", REPLICATION_FACTOR); 

      waitForReplication(ns, b, 2, REPLICATION_FACTOR, 0);
    } finally {
      cluster.shutdown();
    }
  }

  /*
   * Mark a block as corrupt, test that when it is re-replicated that it
   * is still replicated across racks.
   */
  @Test
  @Ignore("See class JavaDoc")
  public void testCorruptBlockRereplicatedAcrossRacks() throws Exception {
    Configuration conf = getConf();
    short REPLICATION_FACTOR = 2;
    final Path filePath = new Path("/testFile");
    // Last datanode is on a different rack
    String racks[] = {"/rack1", "/rack1", "/rack2"};
    MiniDFSCluster cluster = new MiniDFSCluster(conf, racks.length, true, racks);
    final FSNamesystem ns = cluster.getNameNode().getNamesystem();

    try {
      // Create a file with one block with a replication factor of 2
      final FileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, filePath, 1L, REPLICATION_FACTOR, 1L);
      Block b = DFSTestUtil.getFirstBlock(fs, filePath);
      waitForReplication(ns, b, 2, REPLICATION_FACTOR, 0);

      // 3rd datanode reports that the block is corrupt
      ArrayList<DataNode> datanodes = cluster.getDataNodes();
      assertEquals(3, datanodes.size());
      DataNode dataNode = datanodes.get(2);
      cluster.getNameNode().namesystem.markBlockAsCorrupt(b, 
          new DatanodeInfo(dataNode.dnRegistration));

      // The rack policy is still respected (the 3rd datanode got
      // the new replica even though it reported the corrupt block).
      waitForReplication(ns, b, 2, REPLICATION_FACTOR, 0);
    } finally {
      cluster.shutdown();
    }
  }

  /*
   * Reduce the replication factor of a file, making sure that the only
   * block that is across racks is not removed when deleting replicas.
   */
  @Test
  public void testReduceReplFactorRespectsRackPolicy() throws Exception {
    Configuration conf = getConf();
    short REPLICATION_FACTOR = 3;
    final Path filePath = new Path("/testFile");
    String racks[] = {"/rack1", "/rack1", "/rack2", "/rack2"};
    MiniDFSCluster cluster = new MiniDFSCluster(conf, racks.length, true, racks);
    final FSNamesystem ns = cluster.getNameNode().getNamesystem();

    try {
      // Create a file with one block
      final FileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, filePath, 1L, REPLICATION_FACTOR, 1L);
      Block b = DFSTestUtil.getFirstBlock(fs, filePath);
      waitForReplication(ns, b, 2, REPLICATION_FACTOR, 0);

      // Decrease the replication factor, make sure the deleted replica
      // was not the one that lived on the rack with only one replica,
      // ie we should still have 2 racks after reducing the repl factor.
      REPLICATION_FACTOR = 2;
      ns.setReplication("/testFile", REPLICATION_FACTOR); 

      waitForReplication(ns, b, 2, REPLICATION_FACTOR, 0);
    } finally {
      cluster.shutdown();
    }
  }

  /*
   * Test that when a block is replicated because a replica is lost due
   * to host failure the the rack policy is preserved.
   */
  @Test
  public void testReplDueToNodeFailRespectsRackPolicy() throws Exception {
    Configuration conf = getConf();
    short REPLICATION_FACTOR = 3;
    final Path filePath = new Path("/testFile");
    // Last datanode is on a different rack
    String racks[] = {"/rack1", "/rack1", "/rack1", "/rack2", "/rack2"};
    MiniDFSCluster cluster = new MiniDFSCluster(conf, racks.length, true, racks);
    final FSNamesystem ns = cluster.getNameNode().getNamesystem();

    try {
      // Create a file with one block with a replication factor of 2
      final FileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, filePath, 1L, REPLICATION_FACTOR, 1L);
      Block b = DFSTestUtil.getFirstBlock(fs, filePath);
      waitForReplication(ns, b, 2, REPLICATION_FACTOR, 0);

      // Make the last datanode look like it failed to heartbeat by 
      // calling removeDatanode and stopping it.
      ArrayList<DataNode> datanodes = cluster.getDataNodes();
      int idx = datanodes.size() - 1;
      DataNode dataNode = datanodes.get(idx);
      cluster.stopDataNode(idx);
      ns.removeDatanode(dataNode.dnRegistration);

      // The block should still have sufficient # replicas, across racks.
      // The last node may not have contained a replica, but if it did
      // it should have been replicated within the same rack.
      waitForReplication(ns, b, 2, REPLICATION_FACTOR, 0);
      
      // Fail the last datanode again, it's also on rack2 so there is
      // only 1 rack for all the replicas
      datanodes = cluster.getDataNodes();
      idx = datanodes.size() - 1;
      dataNode = datanodes.get(idx);
      cluster.stopDataNode(idx);
      ns.removeDatanode(dataNode.dnRegistration);

      
      // Make sure we have enough live replicas even though we are
      // short one rack and therefore need one replica
      /**
       * In trunk, the assertion here is the following:
       * waitForReplication(ns, b, 1, REPLICATION_FACTOR, 1);
       * 
       * i.e. that the block is on 1 rack with 2 replicas, and wants one
       * more replica, since it isn't achieving the right rack policy.
       * In 0.20, if there is only one live rack, then this block
       * is not considered under-replicated, since rack policy is
       * unachievable (see class JavaDoc above).
       */
      waitForReplication(ns, b, 1, REPLICATION_FACTOR, 0);
    } finally {
      cluster.shutdown();
    }
  }
  
  /*
   * Test that when the execss replicas of a block are reduced due to a 
   * node re-joining the cluster the rack policy is not violated.
   */
  @Test
  @Ignore("See javadoc at top of class")
  public void testReduceReplFactorDueToRejoinRespectsRackPolicy() throws Exception {
    Configuration conf = getConf();
    short REPLICATION_FACTOR = 2;
    final Path filePath = new Path("/testFile");
    // Last datanode is on a different rack
    String racks[] = {"/rack1", "/rack1", "/rack2"};
    MiniDFSCluster cluster = new MiniDFSCluster(conf, racks.length, true, racks);
    final FSNamesystem ns = cluster.getNameNode().getNamesystem();

    try {
      // Create a file with one block
      final FileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, filePath, 1L, REPLICATION_FACTOR, 1L);
      Block b = DFSTestUtil.getFirstBlock(fs, filePath);
      waitForReplication(ns, b, 2, REPLICATION_FACTOR, 0);

      // Make the last (cross rack) datanode look like it failed
      // to heartbeat by stopping it and calling removeDatanode.
      ArrayList<DataNode> datanodes = cluster.getDataNodes();
      assertEquals(3, datanodes.size());
      DataNode dataNode = datanodes.get(2);
      cluster.stopDataNode(2);
      ns.removeDatanode(dataNode.dnRegistration);

      // The block gets re-replicated to another datanode so it has a 
      // sufficient # replicas, but not across racks, so there should
      // be 1 rack, and 1 needed replica (even though there are 2 hosts 
      // available and only 2 replicas required).
      waitForReplication(ns, b, 1, REPLICATION_FACTOR, 1);

      // Start the "failed" datanode, which has a replica so the block is
      // now over-replicated and therefore a replica should be removed but
      // not on the restarted datanode as that would violate the rack policy.
      String rack2[] = {"/rack2"};
      cluster.startDataNodes(conf, 1, true, null, rack2);
      cluster.waitActive();      
      
      // The block now has sufficient # replicas, across racks
      waitForReplication(ns, b, 2, REPLICATION_FACTOR, 0);
    } finally {
      cluster.shutdown();
    }
  }

  /*
   * Test that rack policy is still respected when blocks are replicated
   * due to node decommissioning.
   */
  @Test
  public void testNodeDecomissionRespectsRackPolicy() throws Exception {
    Configuration conf = getConf();
    short REPLICATION_FACTOR = 2;
    final Path filePath = new Path("/testFile");

    // Configure an excludes file
    FileSystem localFileSys = FileSystem.getLocal(conf);
    Path workingDir = localFileSys.getWorkingDirectory();
    Path dir = new Path(workingDir, "build/test/data/temp/decommission");
    Path excludeFile = new Path(dir, "exclude");
    assertTrue(localFileSys.mkdirs(dir));
    writeFile(localFileSys, excludeFile, "");
    conf.set("dfs.hosts.exclude", excludeFile.toUri().getPath());

    // Two blocks and four racks
    String racks[] = {"/rack1", "/rack1", "/rack2", "/rack2"};
    MiniDFSCluster cluster = new MiniDFSCluster(conf, racks.length, true, racks);
    final FSNamesystem ns = cluster.getNameNode().getNamesystem();

    try {
      // Create a file with one block
      final FileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, filePath, 1L, REPLICATION_FACTOR, 1L);
      Block b = DFSTestUtil.getFirstBlock(fs, filePath);
      waitForReplication(ns, b, 2, REPLICATION_FACTOR, 0);

      // Decommission one of the hosts with the block, this should cause 
      // the block to get replicated to another host on the same rack,
      // otherwise the rack policy is violated.
      BlockLocation locs[] = fs.getFileBlockLocations(
          fs.getFileStatus(filePath), 0, Long.MAX_VALUE);
      String name = locs[0].getNames()[0];
      writeFile(localFileSys, excludeFile, name);
      ns.refreshNodes(conf);
      waitForDecommission(fs, name);

      // Check the block still has sufficient # replicas across racks
      waitForReplication(ns, b, 2, REPLICATION_FACTOR, 0);
    } finally {
      cluster.shutdown();
    }
  }

  /*
   * Test that rack policy is still respected when blocks are replicated
   * due to node decommissioning, when the blocks are over-replicated.
   */
  @Test
  public void testNodeDecomissionWithOverreplicationRespectsRackPolicy() 
      throws Exception {
    Configuration conf = getConf();
    short REPLICATION_FACTOR = 5;
    final Path filePath = new Path("/testFile");

    // Configure an excludes file
    FileSystem localFileSys = FileSystem.getLocal(conf);
    Path workingDir = localFileSys.getWorkingDirectory();
    Path dir = new Path(workingDir, "build/test/data/temp/decommission");
    Path excludeFile = new Path(dir, "exclude");
    assertTrue(localFileSys.mkdirs(dir));
    writeFile(localFileSys, excludeFile, "");
    conf.set("dfs.hosts.exclude", excludeFile.toUri().getPath());

    // All hosts are on two racks, only one host on /rack2
    String racks[] = {"/rack1", "/rack2", "/rack1", "/rack1", "/rack1"};
    MiniDFSCluster cluster = new MiniDFSCluster(conf, racks.length, true, racks);
    final FSNamesystem ns = cluster.getNameNode().getNamesystem();

    try {
      final FileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, filePath, 1L, REPLICATION_FACTOR, 1L);
      Block b = DFSTestUtil.getFirstBlock(fs, filePath);
      waitForReplication(ns, b, 2, REPLICATION_FACTOR, 0);

      // Lower the replication factor so the blocks are over replicated
      REPLICATION_FACTOR = 2;
      fs.setReplication(filePath, REPLICATION_FACTOR);
      
      // Decommission one of the hosts with the block that is not on
      // the lone host on rack2 (if we decomission that host it would
      // be impossible to respect the rack policy).
      BlockLocation locs[] = fs.getFileBlockLocations(
          fs.getFileStatus(filePath), 0, Long.MAX_VALUE);
      String[] tops = locs[0].getTopologyPaths();
      for (String top : tops) {
        if (!top.startsWith("/rack2")) {
          String name = top.substring("/rack1".length()+1);
          writeFile(localFileSys, excludeFile, name);
          ns.refreshNodes(conf);
          waitForDecommission(fs, name);
          break;
        }
      }

      // Check the block still has sufficient # replicas across racks,
      // ie we didn't remove the replica on the host on /rack1.
      waitForReplication(ns, b, 2, REPLICATION_FACTOR, 0);
    } finally {
      cluster.shutdown();
    }
  }
}
