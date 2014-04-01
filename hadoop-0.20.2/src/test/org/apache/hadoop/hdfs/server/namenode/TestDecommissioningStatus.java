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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;

/**
 * This class tests the decommissioning of nodes.
 */
public class TestDecommissioningStatus {
  private static final long seed = 0xDEADBEEFL;
  private static final int blockSize = 8192;
  private static final int fileSize = 16384;
  private static final int numDatanodes = 2;
  private static MiniDFSCluster cluster;
  private static FileSystem fileSys;
  private static Path excludeFile;
  private static FileSystem localFileSys;
  private static Configuration conf;
  private static Path dir;

  ArrayList<String> decommissionedNodes = new ArrayList<String>(numDatanodes);
  
  @BeforeClass
  public static void setUp() throws Exception {
    conf = new Configuration();
    conf.setBoolean("dfs.replication.considerLoad", false);

    // Set up the hosts/exclude files.
    localFileSys = FileSystem.getLocal(conf);
    Path workingDir = localFileSys.getWorkingDirectory();
    dir = new Path(workingDir, "build/test/data/work-dir/decommission");
    assertTrue(localFileSys.mkdirs(dir));
    excludeFile = new Path(dir, "exclude");
    conf.set("dfs.hosts.exclude", excludeFile.toUri().getPath());
    conf.setInt("heartbeat.recheck.interval", 2000);
    conf.setInt("dfs.heartbeat.interval", 1);
    conf.setInt("dfs.replication.pending.timeout.sec", 4);
    conf.setInt("dfs.replication.interval", 1000);
    conf.setInt("dfs.namenode.decommission.interval", 1);
    writeConfigFile(localFileSys, excludeFile, null);

    cluster = new MiniDFSCluster(conf, numDatanodes, true, null);
    cluster.waitActive();
    fileSys = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if(fileSys != null) fileSys.close();
    if(cluster != null) cluster.shutdown();
  }

  private static void writeConfigFile(FileSystem fs, Path name,
      ArrayList<String> nodes) throws IOException {

    // delete if it already exists
    if (fs.exists(name)) {
      fs.delete(name, true);
    }

    FSDataOutputStream stm = fs.create(name);

    if (nodes != null) {
      for (Iterator<String> it = nodes.iterator(); it.hasNext();) {
        String node = it.next();
        stm.writeBytes(node);
        stm.writeBytes("\n");
      }
    }
    stm.close();
  }

  private void writeFile(FileSystem fileSys, Path name, short repl)
      throws IOException {
    // create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name, true, fileSys.getConf()
        .getInt("io.file.buffer.size", 4096), repl, (long) blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }
 
  private FSDataOutputStream writeIncompleteFile(FileSystem fileSys, Path name,
      short repl) throws IOException {
    // create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name, true, fileSys.getConf()
        .getInt("io.file.buffer.size", 4096), repl, (long) blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    // Do not close stream, return it
    // so that it is not garbage collected
    return stm;
  }
  
  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }

  /*
   * Decommissions the node at the given index
   */
  private String decommissionNode(FSNamesystem namesystem, Configuration conf,
      DFSClient client, FileSystem localFileSys, int nodeIndex)
      throws IOException {
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);

    String nodename = info[nodeIndex].getName();
    System.out.println("Decommissioning node: " + nodename);

    // write nodename into the exclude file.
    ArrayList<String> nodes = new ArrayList<String>(decommissionedNodes);
    nodes.add(nodename);
    writeConfigFile(localFileSys, excludeFile, nodes);
    namesystem.refreshNodes(conf);
    return nodename;
  }

  private void checkDecommissionStatus(DatanodeDescriptor decommNode,
      int expectedUnderRep, int expectedDecommissionOnly,
      int expectedUnderRepInOpenFiles) {
    assertEquals(decommNode.decommissioningStatus.getUnderReplicatedBlocks(),
        expectedUnderRep);
    assertEquals(
        decommNode.decommissioningStatus.getDecommissionOnlyReplicas(),
        expectedDecommissionOnly);
    assertEquals(decommNode.decommissioningStatus
        .getUnderReplicatedInOpenFiles(), expectedUnderRepInOpenFiles);
  }
  
  /**
   * Tests Decommissioning Status in DFS.
   */

  @Test
  public void testDecommissionStatus() throws IOException, InterruptedException {
    InetSocketAddress addr = new InetSocketAddress("localhost", cluster
        .getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    assertEquals("Number of Datanodes ", 2, info.length);
    FileSystem fileSys = cluster.getFileSystem();

    short replicas = 2;
    //
    // Decommission one node. Verify the decommission status
    // 
    Path file1 = new Path("decommission.dat");
    writeFile(fileSys, file1, replicas);

    Path file2 = new Path("decommission1.dat");
    FSDataOutputStream st1 = writeIncompleteFile(fileSys, file2, replicas);
    Thread.sleep(5000);

    FSNamesystem fsn = cluster.getNameNode().getNamesystem();
    for (int iteration = 0; iteration < numDatanodes; iteration++) {
      String downnode = decommissionNode(fsn, conf, client, localFileSys,
          iteration);
      decommissionedNodes.add(downnode);
      Thread.sleep(5000);
      ArrayList<DatanodeDescriptor> decommissioningNodes = fsn
          .getDecommissioningNodes();
      if (iteration == 0) {
        assertEquals(decommissioningNodes.size(), 1);
        DatanodeDescriptor decommNode = decommissioningNodes.get(0);
        checkDecommissionStatus(decommNode, 4, 0, 2);
      } else {
        assertEquals(decommissioningNodes.size(), 2);
        DatanodeDescriptor decommNode1 = decommissioningNodes.get(0);
        DatanodeDescriptor decommNode2 = decommissioningNodes.get(1);
        checkDecommissionStatus(decommNode1, 4, 4, 2);
        checkDecommissionStatus(decommNode2, 4, 4, 2);
      }
    }
    // Call refreshNodes on FSNamesystem with empty exclude file.
    // This will remove the datanodes from decommissioning list and
    // make them available again.
    writeConfigFile(localFileSys, excludeFile, null);
    fsn.refreshNodes(conf);
    st1.close();
    cleanupFile(fileSys, file1);
    cleanupFile(fileSys, file2);
    cleanupFile(localFileSys, dir);
  }
}
