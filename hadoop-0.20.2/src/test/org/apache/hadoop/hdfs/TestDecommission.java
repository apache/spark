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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

/**
 * This class tests the decommissioning of nodes.
 */
public class TestDecommission extends TestCase {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int fileSize = 16384;
  static final int numDatanodes = 6;


  Random myrand = new Random();
  Path hostsFile;
  Path excludeFile;

  ArrayList<String> decommissionedNodes = new ArrayList<String>(numDatanodes);

  private enum NodeState {NORMAL, DECOMMISSION_INPROGRESS, DECOMMISSIONED; }

  private void writeConfigFile(FileSystem fs, Path name, ArrayList<String> nodes) 
    throws IOException {

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

  private void writeFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    // create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name, true, 
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }
  
  
  private void checkFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    DFSTestUtil.waitReplication(fileSys, name, (short) repl);
  }

  private void printFileLocations(FileSystem fileSys, Path name)
  throws IOException {
    BlockLocation[] locations = fileSys.getFileBlockLocations(
        fileSys.getFileStatus(name), 0, fileSize);
    for (int idx = 0; idx < locations.length; idx++) {
      String[] loc = locations[idx].getHosts();
      System.out.print("Block[" + idx + "] : ");
      for (int j = 0; j < loc.length; j++) {
        System.out.print(loc[j] + " ");
      }
      System.out.println("");
    }
  }

  /**
   * For blocks that reside on the nodes that are down, verify that their
   * replication factor is 1 more than the specified one.
   */
  private void checkFile(FileSystem fileSys, Path name, int repl,
                         String downnode) throws IOException {
    //
    // sleep an additional 10 seconds for the blockreports from the datanodes
    // to arrive. 
    //
    // need a raw stream
    assertTrue("Not HDFS:"+fileSys.getUri(), fileSys instanceof DistributedFileSystem);
        
    DFSClient.DFSDataInputStream dis = (DFSClient.DFSDataInputStream) 
      ((DistributedFileSystem)fileSys).open(name);
    Collection<LocatedBlock> dinfo = dis.getAllBlocks();

    for (LocatedBlock blk : dinfo) { // for each block
      int hasdown = 0;
      DatanodeInfo[] nodes = blk.getLocations();
      for (int j = 0; j < nodes.length; j++) {     // for each replica
        if (nodes[j].getName().equals(downnode)) {
          hasdown++;
          System.out.println("Block " + blk.getBlock() + " replica " +
                             nodes[j].getName() + " is decommissioned.");
        }
      }
      System.out.println("Block " + blk.getBlock() + " has " + hasdown +
                         " decommissioned replica.");
      assertEquals("Number of replicas for block" + blk.getBlock(),
                   Math.min(numDatanodes, repl+hasdown), nodes.length);  
    }
  }
  
  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }

  private void printDatanodeReport(DatanodeInfo[] info) {
    System.out.println("-------------------------------------------------");
    for (int i = 0; i < info.length; i++) {
      System.out.println(info[i].getDatanodeReport());
      System.out.println();
    }
  }

  /*
   * decommission one random node.
   */
  private String decommissionNode(NameNode namenode,
                                  Configuration conf,
                                  DFSClient client, 
                                  FileSystem localFileSys)
    throws IOException {
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);

    //
    // pick one datanode randomly.
    //
    int index = 0;
    boolean found = false;
    while (!found) {
      index = myrand.nextInt(info.length);
      if (!info[index].isDecommissioned()) {
        found = true;
      }
    }
    String nodename = info[index].getName();
    System.out.println("Decommissioning node: " + nodename);

    // write nodename into the exclude file.
    ArrayList<String> nodes = new ArrayList<String>(decommissionedNodes);
    nodes.add(nodename);
    writeConfigFile(localFileSys, excludeFile, nodes);
    namenode.namesystem.refreshNodes(conf);
    return nodename;
  }

  /*
   * Check if node is in the requested state.
   */
  private boolean checkNodeState(FileSystem filesys, 
                                 String node, 
                                 NodeState state) throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem) filesys;
    boolean done = false;
    boolean foundNode = false;
    DatanodeInfo[] datanodes = dfs.getDataNodeStats();
    for (int i = 0; i < datanodes.length; i++) {
      DatanodeInfo dn = datanodes[i];
      if (dn.getName().equals(node)) {
        if (state == NodeState.DECOMMISSIONED) {
          done = dn.isDecommissioned();
        } else if (state == NodeState.DECOMMISSION_INPROGRESS) {
          done = dn.isDecommissionInProgress();
        } else {
          done = (!dn.isDecommissionInProgress() && !dn.isDecommissioned());
        }
        System.out.println(dn.getDatanodeReport());
        foundNode = true;
      }
    }
    if (!foundNode) {
      throw new IOException("Could not find node: " + node);
    }
    return done;
  }

  /* 
   * Wait till node is fully decommissioned.
   */
  private void waitNodeState(FileSystem filesys,
                             String node,
                             NodeState state) throws IOException {
    boolean done = checkNodeState(filesys, node, state);
    while (!done) {
      System.out.println("Waiting for node " + node +
                         " to change state to " + state);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // nothing
      }
      done = checkNodeState(filesys, node, state);
    }
  }
  
  /**
   * Tests Decommission in DFS.
   */
  public void testDecommission() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean("dfs.replication.considerLoad", false);

    // Set up the hosts/exclude files.
    FileSystem localFileSys = FileSystem.getLocal(conf);
    Path workingDir = localFileSys.getWorkingDirectory();
    Path dir = new Path(workingDir, "build/test/data/work-dir/decommission");
    assertTrue(localFileSys.mkdirs(dir));
    hostsFile = new Path(dir, "hosts");
    excludeFile = new Path(dir, "exclude");
    conf.set("dfs.hosts.exclude", excludeFile.toUri().getPath());
    conf.setInt("heartbeat.recheck.interval", 2000);
    conf.setInt("dfs.heartbeat.interval", 1);
    conf.setInt("dfs.replication.pending.timeout.sec", 4);
    writeConfigFile(localFileSys, excludeFile, null);

    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDatanodes, true, null);
    cluster.waitActive();
    InetSocketAddress addr = new InetSocketAddress("localhost", 
                                                   cluster.getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    assertEquals("Number of Datanodes ", numDatanodes, info.length);
    FileSystem fileSys = cluster.getFileSystem();

    try {
      for (int iteration = 0; iteration < numDatanodes - 1; iteration++) {
        int replicas = numDatanodes - iteration - 1;
        //
        // Decommission one node. Verify that node is decommissioned.
        // 
        Path file1 = new Path("decommission.dat");
        writeFile(fileSys, file1, replicas);
        System.out.println("Created file decommission.dat with " +
                           replicas + " replicas.");
        checkFile(fileSys, file1, replicas);
        printFileLocations(fileSys, file1);
        String downnode = decommissionNode(cluster.getNameNode(), conf,
                                           client, localFileSys);
        decommissionedNodes.add(downnode);
        waitNodeState(fileSys, downnode, NodeState.DECOMMISSIONED);
        checkFile(fileSys, file1, replicas, downnode);
        cleanupFile(fileSys, file1);
        cleanupFile(localFileSys, dir);
      }
    } catch (IOException e) {
      info = client.datanodeReport(DatanodeReportType.ALL);
      printDatanodeReport(info);
      throw e;
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }
}
