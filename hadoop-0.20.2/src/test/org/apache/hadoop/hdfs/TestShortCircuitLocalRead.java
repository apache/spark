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

import static org.junit.Assert.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test for short circuit read functionality using {@link BlockReaderLocal}.
 * When a block is being read by a client is on the local datanode, instead of
 * using {@link DataTransferProtocol} and connect to datanode, the short circuit
 * read allows reading the file directly from the files on the local file
 * system.
 */
public class TestShortCircuitLocalRead {
  static final String DIR = "/" + TestShortCircuitLocalRead.class.getSimpleName() + "/";

  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 5120;
  boolean simulatedStorage = false;
  
  // creates a file but does not close it
  static FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl)
      throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)blockSize);
    return stm;
  }

  static private void checkData(byte[] actual, int from, byte[] expected,
      String message) {
    checkData(actual, from, expected, actual.length, message);
  }
  
  static private void checkData(byte[] actual, int from, byte[] expected,
      int len, String message) {
    for (int idx = 0; idx < len; idx++) {
      if (expected[from + idx] != actual[idx]) {
        Assert.fail(message + " byte " + (from + idx) + " differs. expected "
            + expected[from + idx] + " actual " + actual[idx]);
      }
    }
  }

  static void checkFileContent(FileSystem fs, Path name, byte[] expected,
      int readOffset) throws IOException {
    FSDataInputStream stm = fs.open(name);
    byte[] actual = new byte[expected.length-readOffset];
    stm.readFully(readOffset, actual);
    checkData(actual, readOffset, expected, "Read 2");
    stm.close();
    // Now read using a different API.
    actual = new byte[expected.length-readOffset];
    stm = fs.open(name);
    long skipped = stm.skip(readOffset);
    Assert.assertEquals(skipped, readOffset);
    //Read a small number of bytes first.
    int nread = stm.read(actual, 0, 3);
    nread += stm.read(actual, nread, 2);
    //Read across chunk boundary
    nread += stm.read(actual, nread, 517);
    checkData(actual, readOffset, expected, nread, "A few bytes");
    //Now read rest of it
    while (nread < actual.length) {
      int nbytes = stm.read(actual, nread, actual.length - nread);
      if (nbytes < 0) {
        throw new EOFException("End of file reached before reading fully.");
      }
      nread += nbytes;
    }
    checkData(actual, readOffset, expected, "Read 3");
    stm.close();
  }

  /**
   * Test that file data can be read by reading the block file
   * directly from the local store.
   */
  public void doTestShortCircuitRead(boolean ignoreChecksum, int size,
      int readOffset) throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY,
        ignoreChecksum);
    conf.set(DFSConfigKeys.DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY,
        UserGroupInformation.getCurrentUser().getShortUserName());
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    try {
      // check that / exists
      Path path = new Path("/"); 
      assertTrue("/ should be a directory", 
                 fs.getFileStatus(path).isDir() == true);
      
      byte[] fileData = AppendTestUtil.randomBytes(seed, size);
      // create a new file in home directory. Do not close it.
      Path file1 = new Path("filelocal.dat");
      FSDataOutputStream stm = createFile(fs, file1, 1);

      // write to file
      stm.write(fileData);
      stm.close();
      checkFileContent(fs, file1, fileData, readOffset);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  @Test
  public void testFileLocalReadNoChecksum() throws IOException {
    doTestShortCircuitRead(true, 3*blockSize+100, 0);
  }

  @Test
  public void testFileLocalReadChecksum() throws IOException {
    doTestShortCircuitRead(false, 3*blockSize+100, 0);
  }
  
  @Test
  public void testSmallFileLocalRead() throws IOException {
    doTestShortCircuitRead(false, 13, 0);
    doTestShortCircuitRead(false, 13, 5);
    doTestShortCircuitRead(true, 13, 0);
    doTestShortCircuitRead(true, 13, 5);
  }
  
  @Test
  public void testReadFromAnOffset() throws IOException {
    doTestShortCircuitRead(false, 3*blockSize+100, 777);
    doTestShortCircuitRead(true, 3*blockSize+100, 777);
  }

  @Test
  public void testLongFile() throws IOException {
    doTestShortCircuitRead(false, 10*blockSize+100, 777);
    doTestShortCircuitRead(true, 10*blockSize+100, 777);
  }
  
  @Test
  public void testGetBlockLocalPathInfo() throws IOException, InterruptedException {
    final Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY, "alloweduser");
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    cluster.waitActive();
    final DataNode dn = cluster.getDataNodes().get(0);
    FileSystem fs = cluster.getFileSystem();
    try {
      DFSTestUtil.createFile(fs, new Path("/tmp/x"), 16, (short) 1, 23);
      UserGroupInformation aUgi = UserGroupInformation
          .createRemoteUser("alloweduser");
      LocatedBlocks lb = cluster.getNameNode().getBlockLocations("/tmp/x", 0,
          16);
      // Create a new block object, because the block inside LocatedBlock at
      // namenode is of type BlockInfo.
      Block blk = new Block(lb.get(0).getBlock());
      Token<BlockTokenIdentifier> token = lb.get(0).getBlockToken();
      final DatanodeInfo dnInfo = lb.get(0).getLocations()[0];
      ClientDatanodeProtocol proxy = aUgi
          .doAs(new PrivilegedExceptionAction<ClientDatanodeProtocol>() {
            @Override
            public ClientDatanodeProtocol run() throws Exception {
              return DFSClient.createClientDatanodeProtocolProxy(
                  dnInfo, conf, 60000, false);
            }
          });
      
      //This should succeed
      BlockLocalPathInfo blpi = proxy.getBlockLocalPathInfo(blk, token);
      Assert.assertEquals(dn.data.getBlockLocalPathInfo(blk).getBlockPath(),
          blpi.getBlockPath());
      RPC.stopProxy(proxy);

      // Now try with a not allowed user.
      UserGroupInformation bUgi = UserGroupInformation
          .createRemoteUser("notalloweduser");
      proxy = bUgi
          .doAs(new PrivilegedExceptionAction<ClientDatanodeProtocol>() {
            @Override
            public ClientDatanodeProtocol run() throws Exception {
              return DFSClient.createClientDatanodeProtocolProxy(
                  dnInfo, conf, 60000, false);
            }
          });
      try {
        proxy.getBlockLocalPathInfo(blk, token);
        Assert.fail("The call should have failed as " + bUgi.getShortUserName()
            + " is not allowed to call getBlockLocalPathInfo");
      } catch (IOException ex) {
        Assert.assertTrue(ex.getMessage().contains(
            "not allowed to call getBlockLocalPathInfo"));
      } finally {
        RPC.stopProxy(proxy);
      }
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }
  
  /**
   * Test to run benchmarks between shortcircuit read vs regular read with
   * specified number of threads simultaneously reading.
   * <br>
   * Run this using the following command:
   * bin/hadoop --config confdir \
   * org.apache.hadoop.hdfs.TestShortCircuitLocalRead \
   * <shortcircuit on?> <checsum on?> <Number of threads>
   */
  public static void main(String[] args) throws Exception {
    ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.INFO);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.INFO);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.INFO);
    
    if (args.length != 3) {
      System.out.println("Usage: test shortcircuit checksum threadCount");
      System.exit(1);
    }
    boolean shortcircuit = Boolean.valueOf(args[0]);
    boolean checksum = Boolean.valueOf(args[1]);
    int threadCount = Integer.valueOf(args[2]);

    // Setup create a file
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY, shortcircuit);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY,
        checksum);
    
    //Override fileSize and DATA_TO_WRITE to much larger values for benchmark test
    int fileSize = 1000 * blockSize + 100; // File with 1000 blocks
    final byte [] dataToWrite = AppendTestUtil.randomBytes(seed, fileSize);
    
    // create a new file in home directory. Do not close it.
    final Path file1 = new Path("filelocal.dat");
    final FileSystem fs = FileSystem.get(conf);
    FSDataOutputStream stm = createFile(fs, file1, 1);
    
    stm.write(dataToWrite);
    stm.close();

    long start = System.currentTimeMillis();
    final int iteration = 20;
    Thread[] threads = new Thread[threadCount];
    for (int i = 0; i < threadCount; i++) {
      threads[i] = new Thread() {
        public void run() {
          for (int i = 0; i < iteration; i++) {
            try {
              checkFileContent(fs, file1, dataToWrite, 0);
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        }
      };
    }
    for (int i = 0; i < threadCount; i++) {
      threads[i].start();
    }
    for (int i = 0; i < threadCount; i++) {
      threads[i].join();
    }
    long end = System.currentTimeMillis();
    System.out.println("Iteration " + iteration + " took " + (end - start));
    fs.delete(file1, false);
  }
}
