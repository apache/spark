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
package org.apache.hadoop.hdfs.server.namenode.metrics;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.mortbay.log.Log;

/**
 * Test for metrics published by the Namenode
 */
public class TestNameNodeMetrics extends TestCase {
  private static final Configuration CONF = new Configuration();
  private static final int DFS_REPLICATION_INTERVAL = 1;
  private static final Path TEST_ROOT_DIR_PATH = 
    new Path(System.getProperty("test.build.data", "build/test/data"));
  
  // Number of datanodes in the cluster
  private static final int DATANODE_COUNT = 3; 
  static {
    CONF.setLong("dfs.block.size", 100);
    CONF.setInt("io.bytes.per.checksum", 1);
    CONF.setLong("dfs.heartbeat.interval", DFS_REPLICATION_INTERVAL);
    CONF.setInt("dfs.replication.interval", DFS_REPLICATION_INTERVAL);
  }
  
  private MiniDFSCluster cluster;
  private FSNamesystemMetrics metrics;
  private DistributedFileSystem fs;
  private Random rand = new Random();
  private FSNamesystem namesystem;
  private NameNodeMetrics nnMetrics;
  private NameNode nn;

  private static Path getTestPath(String fileName) {
    return new Path(TEST_ROOT_DIR_PATH, fileName);
  }
  
  @Override
  protected void setUp() throws Exception {
    cluster = new MiniDFSCluster(CONF, DATANODE_COUNT, true, null);
    cluster.waitActive();
    namesystem = cluster.getNameNode().getNamesystem();
    fs = (DistributedFileSystem) cluster.getFileSystem();
    metrics = namesystem.getFSNamesystemMetrics();
    nn = cluster.getNameNode();
    nnMetrics = nn.getNameNodeMetrics();
  }
  
  @Override
  protected void tearDown() throws Exception {
    cluster.shutdown();
  }
  
  /** create a file with a length of <code>fileLen</code> */
  private void createFile(Path file, long fileLen, short replicas) throws IOException {
    DFSTestUtil.createFile(fs, file, fileLen, replicas, rand.nextLong());
  }

  private void updateMetrics() throws Exception {
    // Wait for metrics update (corresponds to dfs.replication.interval
    // for some block related metrics to get updated)
    Thread.sleep(1000);
    metrics.doUpdates(null);
  }

  private void updateNNMetrics() throws Exception {
    //Wait for nnmetrics update
    Thread.sleep(1000);
    nnMetrics.doUpdates(null);
  }
  
  private void readFile(FileSystem fileSys,Path name) throws IOException {
    //Just read file so that getNumBlockLocations are incremented
    DataInputStream stm = fileSys.open(name);
    byte [] buffer = new byte[4];
    int bytesRead =  stm.read(buffer,0,4);
    stm.close();
  }

  /** Test metrics associated with addition of a file */
  public void testFileAdd() throws Exception {
    // Add files with 100 blocks
    final Path file = getTestPath("testFileAdd");
    createFile(file, 3200, (short)3);
    final int blockCount = 32;
    int blockCapacity = namesystem.getBlockCapacity();
    updateMetrics();
    assertEquals(blockCapacity, metrics.blockCapacity.get());

    // Blocks are stored in a hashmap. Compute its capacity, which
    // doubles every time the number of entries reach the threshold.
    int threshold = (int)(blockCapacity * FSNamesystem.DEFAULT_MAP_LOAD_FACTOR);
    while (threshold < blockCount) {
      blockCapacity <<= 1;
    }
    updateMetrics();
    int filesTotal = file.depth() + 1; // Add 1 for root
    assertEquals(filesTotal, metrics.filesTotal.get());
    assertEquals(blockCount, metrics.blocksTotal.get());
    assertEquals(blockCapacity, metrics.blockCapacity.get());
    fs.delete(file, true);
    filesTotal--; // reduce the filecount for deleted file
    
    // Wait for more than DATANODE_COUNT replication intervals to ensure all 
    // the blocks pending deletion are sent for deletion to the datanodes.
    Thread.sleep(DFS_REPLICATION_INTERVAL * (DATANODE_COUNT + 1) * 1000);
    updateMetrics();
    assertEquals(filesTotal, metrics.filesTotal.get());
    assertEquals(0, metrics.pendingDeletionBlocks.get());
  }
  
  /** Corrupt a block and ensure metrics reflects it */
  public void testCorruptBlock() throws Exception {
    // Create a file with single block with two replicas
    final Path file = getTestPath("testCorruptBlock");
    createFile(file, 100, (short)2);
    
    // Corrupt first replica of the block
    LocatedBlock block = namesystem.getBlockLocations(file.toString(), 0, 1).get(0);
    namesystem.markBlockAsCorrupt(block.getBlock(), block.getLocations()[0]);
    updateMetrics();
    assertEquals(1, metrics.corruptBlocks.get());
    assertEquals(1, metrics.pendingReplicationBlocks.get());
    assertEquals(1, metrics.scheduledReplicationBlocks.get());
    fs.delete(file, true);
    updateMetrics();
    assertEquals(0, metrics.corruptBlocks.get());
    assertEquals(0, metrics.pendingReplicationBlocks.get());
    assertEquals(0, metrics.scheduledReplicationBlocks.get());
  }
  
  /** Create excess blocks by reducing the replication factor for
   * for a file and ensure metrics reflects it
   */
  public void testExcessBlocks() throws Exception {
    Path file = getTestPath("testExcessBlocks");
    createFile(file, 100, (short)2);
    int totalBlocks = 1;
    namesystem.setReplication(file.toString(), (short)1);
    updateMetrics();
    assertEquals(totalBlocks, metrics.excessBlocks.get());
    fs.delete(file, true);
  }
  
  /** Test to ensure metrics reflects missing blocks */
  public void testMissingBlock() throws Exception {
    // Create a file with single block with two replicas
    Path file = getTestPath("testMissingBlocks");
    createFile(file, 100, (short)1);
    
    // Corrupt the only replica of the block to result in a missing block
    LocatedBlock block = namesystem.getBlockLocations(file.toString(), 0, 1).get(0);
    namesystem.markBlockAsCorrupt(block.getBlock(), block.getLocations()[0]);
    updateMetrics();
    assertEquals(1, metrics.underReplicatedBlocks.get());
    assertEquals(1, metrics.missingBlocks.get());
    fs.delete(file, true);
    updateMetrics();
    assertEquals(0, metrics.underReplicatedBlocks.get());
  }
  
  /**
   * Test numGetBlockLocations metric   
   * 
   * Test initiates and performs file operations (create,read,close,open file )
   * which results in metrics changes. These metrics changes are updated and 
   * tested for correctness.
   * 
   *  create file operation does not increment numGetBlockLocation
   *  one read file operation increments numGetBlockLocation by 1
   *    
   * @throws IOException in case of an error
   */
  public void testGetBlockLocationMetric() throws Exception{
    final String METHOD_NAME = "TestGetBlockLocationMetric";
    Log.info("Running test "+METHOD_NAME);
  
    Path file1_Path = new Path(TEST_ROOT_DIR_PATH, "file1.dat");

    // When cluster starts first time there are no file  (read,create,open)
    // operations so metric numGetBlockLocations should be 0.
    // Verify that numGetBlockLocations for current interval 
    // and previous interval are 0
    assertEquals("numGetBlockLocations for previous interval is incorrect",
    0,nnMetrics.numGetBlockLocations.getPreviousIntervalValue());
    assertEquals("numGetBlockLocations for current interval is incorrect",
    0,nnMetrics.numGetBlockLocations.getCurrentIntervalValue());

    //Perform create file operation
    createFile(file1_Path,100,(short)2);
    // Update NameNode metrics
    updateNNMetrics();
  
    //Create file does not change numGetBlockLocations metric
    //expect numGetBlockLocations = 0 for previous and current interval 
    assertEquals("numGetBlockLocations for previous interval is incorrect",
    0,nnMetrics.numGetBlockLocations.getPreviousIntervalValue());
    // Verify numGetBlockLocations for current interval is 0
    assertEquals("numGetBlockLocations for current interval is incorrect",
    0,nnMetrics.numGetBlockLocations.getCurrentIntervalValue());
  
    // Open and read file operation increments numGetBlockLocations
    // Perform read file operation on earlier created file
    readFile(fs, file1_Path);
    // Update NameNode metrics
    updateNNMetrics();
    // Verify read file operation has incremented numGetBlockLocations by 1
    assertEquals("numGetBlockLocations for previous interval is incorrect",
    1,nnMetrics.numGetBlockLocations.getPreviousIntervalValue());
    // Verify numGetBlockLocations for current interval is 0
    assertEquals("numGetBlockLocations for current interval is incorrect",
    0,nnMetrics.numGetBlockLocations.getCurrentIntervalValue());

    // opening and reading file  twice will increment numGetBlockLocations by 2
    readFile(fs, file1_Path);
    readFile(fs, file1_Path);
    updateNNMetrics();
    assertEquals("numGetBlockLocations for previous interval is incorrect",
    2,nnMetrics.numGetBlockLocations.getPreviousIntervalValue());
    // Verify numGetBlockLocations for current interval is 0
    assertEquals("numGetBlockLocations for current interval is incorrect",
    0,nnMetrics.numGetBlockLocations.getCurrentIntervalValue());
  
    // Verify total load metrics, total load = Data Node started.
    updateMetrics();
    assertEquals("Metrics TotalLoad is incorrect"
    ,DATANODE_COUNT,metrics.totalLoad.get());
  }
}
