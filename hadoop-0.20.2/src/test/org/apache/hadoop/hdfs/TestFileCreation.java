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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Level;

import static org.junit.Assume.assumeTrue;

/**
 * This class tests that a file need not be closed before its
 * data can be read by another client.
 */
public class TestFileCreation extends junit.framework.TestCase {
  static final String DIR = "/" + TestFileCreation.class.getSimpleName() + "/";

  {
    //((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
  }

  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int numBlocks = 2;
  static final int fileSize = numBlocks * blockSize + 1;
  boolean simulatedStorage = false;

  // The test file is 2 times the blocksize plus one. This means that when the
  // entire file is written, the first two blocks definitely get flushed to
  // the datanodes.

  // creates a file but does not close it
  static FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    System.out.println("createFile: Created " + name + " with " + repl + " replica.");
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)blockSize);
    return stm;
  }

  //
  // writes to file but does not close it
  //
  static void writeFile(FSDataOutputStream stm) throws IOException {
    writeFile(stm, fileSize);
  }

  //
  // writes specified bytes to file.
  //
  static void writeFile(FSDataOutputStream stm, int size) throws IOException {
    byte[] buffer = AppendTestUtil.randomBytes(seed, size);
    stm.write(buffer, 0, size);
  }
  
  //
  // verify that the data written to the full blocks are sane
  // 
  private void checkFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    boolean done = false;

    // wait till all full blocks are confirmed by the datanodes.
    while (!done) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}
      done = true;
      BlockLocation[] locations = fileSys.getFileBlockLocations(
          fileSys.getFileStatus(name), 0, fileSize);
      if (locations.length < numBlocks) {
        done = false;
        continue;
      }
      for (int idx = 0; idx < locations.length; idx++) {
        if (locations[idx].getHosts().length < repl) {
          done = false;
          break;
        }
      }
    }
    FSDataInputStream stm = fileSys.open(name);
    final byte[] expected;
    if (simulatedStorage) {
      expected = new byte[numBlocks * blockSize];
      for (int i= 0; i < expected.length; i++) {  
        expected[i] = SimulatedFSDataset.DEFAULT_DATABYTE;
      }
    } else {
      expected = AppendTestUtil.randomBytes(seed, numBlocks*blockSize);
    }
    // do a sanity check. Read the file
    byte[] actual = new byte[numBlocks * blockSize];
    stm.readFully(0, actual);
    stm.close();
    checkData(actual, 0, expected, "Read 1");
  }

  static private void checkData(byte[] actual, int from, byte[] expected, String message) {
    for (int idx = 0; idx < actual.length; idx++) {
      assertEquals(message+" byte "+(from+idx)+" differs. expected "+
                   expected[from+idx]+" actual "+actual[idx],
                   expected[from+idx], actual[idx]);
      actual[idx] = 0;
    }
  }

  static void checkFullFile(FileSystem fs, Path name) throws IOException {
    FileStatus stat = fs.getFileStatus(name);
    BlockLocation[] locations = fs.getFileBlockLocations(stat, 0, 
                                                         fileSize);
    for (int idx = 0; idx < locations.length; idx++) {
      String[] hosts = locations[idx].getNames();
      for (int i = 0; i < hosts.length; i++) {
        System.out.print( hosts[i] + " ");
      }
      System.out.println(" off " + locations[idx].getOffset() +
                         " len " + locations[idx].getLength());
    }

    byte[] expected = AppendTestUtil.randomBytes(seed, fileSize);
    FSDataInputStream stm = fs.open(name);
    byte[] actual = new byte[fileSize];
    stm.readFully(0, actual);
    checkData(actual, 0, expected, "Read 2");
    stm.close();
  }

  public void testFileCreation() throws IOException {
    checkFileCreation(null, null);
  }

  /** Same test but the client should use DN hostname instead of IPs */
  public void testFileCreationByHostname() throws IOException {
    assumeTrue(System.getProperty("os.name").startsWith("Linux"));

    // Since the mini cluster only listens on the loopback we have to
    // ensure the hostname used to access DNs maps to the loopback. We
    // do this by telling the DN to advertise localhost as its hostname
    // instead of the default hostname.
    checkFileCreation("localhost", null);
  }

  /** Same test but the client should bind to a local interface */
  public void testFileCreationSetLocalInterface() throws IOException {
    assumeTrue(System.getProperty("os.name").startsWith("Linux"));

    // The mini cluster listens on the loopback so we can use it here
    checkFileCreation(null, "lo");

    try {
      checkFileCreation(null, "bogus-interface");
      fail("Able to specify a bogus interface");
    } catch (UnknownHostException e) {
      assertEquals("Unknown interface bogus-interface", e.getMessage());
    }
  }

  /**
   * Test that file data becomes available before file is closed.
   * @param hostname the hostname, if any, clients should use to access DNs
   * @param netIf the local interface, if any, clients should use to access DNs
   */
  public void checkFileCreation(String hostname, String netIf) throws IOException {
    Configuration conf = new Configuration();

    if (hostname != null) {
      conf.setBoolean(DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME, true);
      conf.set("slave.host.name", hostname);
    }
    if (netIf != null) {
      conf.set(DFSConfigKeys.DFS_CLIENT_LOCAL_INTERFACES, netIf);
    }
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    try {

      //
      // check that / exists
      //
      Path path = new Path("/");
      System.out.println("Path : \"" + path.toString() + "\"");
      System.out.println(fs.getFileStatus(path).isDir()); 
      assertTrue("/ should be a directory", 
                 fs.getFileStatus(path).isDir() == true);

      //
      // Create a directory inside /, then try to overwrite it
      //
      Path dir1 = new Path("/test_dir");
      fs.mkdirs(dir1);
      System.out.println("createFile: Creating " + dir1.getName() + 
        " for overwrite of existing directory.");
      try {
        fs.create(dir1, true); // Create path, overwrite=true
        fs.close();
        assertTrue("Did not prevent directory from being overwritten.", false);
      } catch (IOException ie) {
        if (!ie.getMessage().contains("already exists as a directory."))
          throw ie;
      }
      
      // create a new file in home directory. Do not close it.
      //
      Path file1 = new Path("filestatus.dat");
      FSDataOutputStream stm = createFile(fs, file1, 1);

      // verify that file exists in FS namespace
      assertTrue(file1 + " should be a file", 
                  fs.getFileStatus(file1).isDir() == false);
      System.out.println("Path : \"" + file1 + "\"");

      // write to file
      writeFile(stm);

      // Make sure a client can read it before it is closed.
      checkFile(fs, file1, 1);

      // verify that file size has changed
      long len = fs.getFileStatus(file1).getLen();
      assertTrue(file1 + " should be of size " + (numBlocks * blockSize) +
                 " but found to be of size " + len, 
                  len == numBlocks * blockSize);

      stm.close();

      // verify that file size has changed to the full size
      len = fs.getFileStatus(file1).getLen();
      assertTrue(file1 + " should be of size " + fileSize +
                 " but found to be of size " + len, 
                  len == fileSize);
      
      
      // Check storage usage 
      // can't check capacities for real storage since the OS file system may be changing under us.
      if (simulatedStorage) {
        DataNode dn = cluster.getDataNodes().get(0);
        assertEquals(fileSize, dn.getFSDataset().getDfsUsed());
        assertEquals(SimulatedFSDataset.DEFAULT_CAPACITY-fileSize, dn.getFSDataset().getRemaining());
      }
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test deleteOnExit
   */
  public void testDeleteOnExit() throws IOException {
    Configuration conf = new Configuration();
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    FileSystem localfs = FileSystem.getLocal(conf);

    try {

      // Creates files in HDFS and local file system.
      //
      Path file1 = new Path("filestatus.dat");
      Path file2 = new Path("filestatus2.dat");
      Path file3 = new Path("filestatus3.dat");
      FSDataOutputStream stm1 = createFile(fs, file1, 1);
      FSDataOutputStream stm2 = createFile(fs, file2, 1);
      FSDataOutputStream stm3 = createFile(localfs, file3, 1);
      System.out.println("DeleteOnExit: Created files.");

      // write to files and close. Purposely, do not close file2.
      writeFile(stm1);
      writeFile(stm3);
      stm1.close();
      stm2.close();
      stm3.close();

      // set delete on exit flag on files.
      fs.deleteOnExit(file1);
      fs.deleteOnExit(file2);
      localfs.deleteOnExit(file3);

      // close the file system. This should make the above files
      // disappear.
      fs.close();
      localfs.close();
      fs = null;
      localfs = null;

      // reopen file system and verify that file does not exist.
      fs = cluster.getFileSystem();
      localfs = FileSystem.getLocal(conf);

      assertTrue(file1 + " still exists inspite of deletOnExit set.",
                 !fs.exists(file1));
      assertTrue(file2 + " still exists inspite of deletOnExit set.",
                 !fs.exists(file2));
      assertTrue(file3 + " still exists inspite of deletOnExit set.",
                 !localfs.exists(file3));
      System.out.println("DeleteOnExit successful.");

    } finally {
      IOUtils.closeStream(fs);
      IOUtils.closeStream(localfs);
      cluster.shutdown();
    }
  }

  /**
   * Test file creation using createNonRecursive().
   */
  public void testFileCreationNonRecursive() throws IOException {
    Configuration conf = new Configuration();
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    final Path path = new Path("/" + System.currentTimeMillis()
        + "-testFileCreationNonRecursive");
    FSDataOutputStream out = null;

    try {
      IOException expectedException = null;
      final String nonExistDir = "/non-exist-" + System.currentTimeMillis();

      fs.delete(new Path(nonExistDir), true);
      // Create a new file in root dir, should succeed
      out = createNonRecursive(fs, path, 1, false);
      out.close();
      // Create a file when parent dir exists as file, should fail
      expectedException = null;
      try {
        createNonRecursive(fs, new Path(path, "Create"), 1, false);
      } catch (IOException e) {
        expectedException = e;
      }
      assertTrue("Create a file when parent directory exists as a file"
          + " should throw FileAlreadyExistsException ",
          expectedException != null
              && expectedException instanceof FileAlreadyExistsException);
      fs.delete(path, true);
      // Create a file in a non-exist directory, should fail
      final Path path2 = new Path(nonExistDir + "/testCreateNonRecursive");
      expectedException = null;
      try {
        createNonRecursive(fs, path2, 1, false);
      } catch (IOException e) {
        expectedException = e;
      }
      assertTrue("Create a file in a non-exist dir using"
          + " createNonRecursive() should throw FileNotFoundException ",
          expectedException != null
              && expectedException instanceof FileNotFoundException);

      // Overwrite a file in root dir, should succeed
      out = createNonRecursive(fs, path, 1, true);
      out.close();
      // Overwrite a file when parent dir exists as file, should fail
      expectedException = null;
      try {
        createNonRecursive(fs, new Path(path, "Overwrite"), 1, true);
      } catch (IOException e) {
        expectedException = e;
      }
      assertTrue("Overwrite a file when parent directory exists as a file"
          + " should throw FileAlreadyExistsException ",
          expectedException != null
              && expectedException instanceof FileAlreadyExistsException);
      fs.delete(path, true);
      // Overwrite a file in a non-exist directory, should fail
      final Path path3 = new Path(nonExistDir + "/testOverwriteNonRecursive");
      expectedException = null;
      try {
        createNonRecursive(fs, path3, 1, true);
      } catch (IOException e) {
        expectedException = e;
      }
      assertTrue("Overwrite a file in a non-exist dir using"
          + " createNonRecursive() should throw FileNotFoundException ",
          expectedException != null
              && expectedException instanceof FileNotFoundException);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  // creates a file using DistributedFileSystem.createNonRecursive()
  static FSDataOutputStream createNonRecursive(FileSystem fs, Path name,
      int repl, boolean overwrite) throws IOException {
    System.out.println("createNonRecursive: Created " + name + " with " + repl
        + " replica.");
    FSDataOutputStream stm = ((DistributedFileSystem) fs).createNonRecursive(
        name, FsPermission.getDefault(), overwrite, fs.getConf().getInt(
            "io.file.buffer.size", 4096), (short) repl, (long) blockSize, null);

    return stm;
  }
  
  /**
   * Test that file data does not become corrupted even in the face of errors.
   */
  public void testFileCreationError1() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt("heartbeat.recheck.interval", 1000);
    conf.setInt("dfs.heartbeat.interval", 1);
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    cluster.waitActive();
    InetSocketAddress addr = new InetSocketAddress("localhost",
                                                   cluster.getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);

    try {

      // create a new file.
      //
      Path file1 = new Path("/filestatus.dat");
      FSDataOutputStream stm = createFile(fs, file1, 1);

      // verify that file exists in FS namespace
      assertTrue(file1 + " should be a file", 
                  fs.getFileStatus(file1).isDir() == false);
      System.out.println("Path : \"" + file1 + "\"");

      // kill the datanode
      cluster.shutdownDataNodes();

      // wait for the datanode to be declared dead
      while (true) {
        DatanodeInfo[] info = client.datanodeReport(
            FSConstants.DatanodeReportType.LIVE);
        if (info.length == 0) {
          break;
        }
        System.out.println("testFileCreationError1: waiting for datanode " +
                           " to die.");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
      }

      // write 1 byte to file. 
      // This should fail because all datanodes are dead.
      byte[] buffer = AppendTestUtil.randomBytes(seed, 1);
      try {
        stm.write(buffer);
        stm.close();
      } catch (Exception e) {
        System.out.println("Encountered expected exception");
      }

      // verify that no blocks are associated with this file
      // bad block allocations were cleaned up earlier.
      LocatedBlocks locations = client.namenode.getBlockLocations(
                                  file1.toString(), 0, Long.MAX_VALUE);
      System.out.println("locations = " + locations.locatedBlockCount());
      assertTrue("Error blocks were not cleaned up",
                 locations.locatedBlockCount() == 0);
    } finally {
      cluster.shutdown();
      client.close();
    }
  }

  /**
   * Test that the filesystem removes the last block from a file if its
   * lease expires.
   */
  public void testFileCreationError2() throws IOException {
    long leasePeriod = 1000;
    System.out.println("testFileCreationError2 start");
    Configuration conf = new Configuration();
    conf.setInt("heartbeat.recheck.interval", 1000);
    conf.setInt("dfs.heartbeat.interval", 1);
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    DistributedFileSystem dfs = null;
    try {
      cluster.waitActive();
      dfs = (DistributedFileSystem)cluster.getFileSystem();
      DFSClient client = dfs.dfs;

      // create a new file.
      //
      Path file1 = new Path("/filestatus.dat");
      createFile(dfs, file1, 1);
      System.out.println("testFileCreationError2: "
                         + "Created file filestatus.dat with one replicas.");

      LocatedBlocks locations = client.namenode.getBlockLocations(
                                  file1.toString(), 0, Long.MAX_VALUE);
      System.out.println("testFileCreationError2: "
          + "The file has " + locations.locatedBlockCount() + " blocks.");

      // add another block to the file
      LocatedBlock location = client.namenode.addBlock(file1.toString(), 
          client.clientName);
      System.out.println("testFileCreationError2: "
          + "Added block " + location.getBlock());

      locations = client.namenode.getBlockLocations(file1.toString(), 
                                                    0, Long.MAX_VALUE);
      int count = locations.locatedBlockCount();
      System.out.println("testFileCreationError2: "
          + "The file now has " + count + " blocks.");
      
      // set the soft and hard limit to be 1 second so that the
      // namenode triggers lease recovery
      cluster.setLeasePeriod(leasePeriod, leasePeriod);

      // wait for the lease to expire
      try {
        Thread.sleep(5 * leasePeriod);
      } catch (InterruptedException e) {
      }

      // verify that the last block was synchronized.
      locations = client.namenode.getBlockLocations(file1.toString(), 
                                                    0, Long.MAX_VALUE);
      System.out.println("testFileCreationError2: "
          + "locations = " + locations.locatedBlockCount());
      assertEquals(0, locations.locatedBlockCount());
      System.out.println("testFileCreationError2 successful");
    } finally {
      IOUtils.closeStream(dfs);
      cluster.shutdown();
    }
  }

  /**
   * Test that file leases are persisted across namenode restarts.
   * This test is currently not triggered because more HDFS work is 
   * is needed to handle persistent leases.
   */
  public void xxxtestFileCreationNamenodeRestart() throws IOException {
    Configuration conf = new Configuration();
    final int MAX_IDLE_TIME = 2000; // 2s
    conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
    conf.setInt("heartbeat.recheck.interval", 1000);
    conf.setInt("dfs.heartbeat.interval", 1);
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = null;
    try {
      cluster.waitActive();
      fs = cluster.getFileSystem();
      final int nnport = cluster.getNameNodePort();

      // create a new file.
      Path file1 = new Path("/filestatus.dat");
      FSDataOutputStream stm = createFile(fs, file1, 1);
      System.out.println("testFileCreationNamenodeRestart: "
                         + "Created file " + file1);

      // write two full blocks.
      int remainingPiece = blockSize/2;
      int blocksMinusPiece = numBlocks * blockSize - remainingPiece;
      writeFile(stm, blocksMinusPiece);
      stm.sync();
      int actualRepl = ((DFSClient.DFSOutputStream)(stm.getWrappedStream())).
                        getNumCurrentReplicas();
      // if we sync on a block boundary, actualRepl will be 0
      assertTrue(file1 + " should be replicated to 1 datanodes, not " + actualRepl,
                 actualRepl == 1);
      writeFile(stm, remainingPiece);
      stm.sync();

      // rename file wile keeping it open.
      Path fileRenamed = new Path("/filestatusRenamed.dat");
      fs.rename(file1, fileRenamed);
      System.out.println("testFileCreationNamenodeRestart: "
                         + "Renamed file " + file1 + " to " +
                         fileRenamed);
      file1 = fileRenamed;

      // create another new file.
      //
      Path file2 = new Path("/filestatus2.dat");
      FSDataOutputStream stm2 = createFile(fs, file2, 1);
      System.out.println("testFileCreationNamenodeRestart: "
                         + "Created file " + file2);

      // create yet another new file with full path name. 
      // rename it while open
      //
      Path file3 = new Path("/user/home/fullpath.dat");
      FSDataOutputStream stm3 = createFile(fs, file3, 1);
      System.out.println("testFileCreationNamenodeRestart: "
                         + "Created file " + file3);
      Path file4 = new Path("/user/home/fullpath4.dat");
      FSDataOutputStream stm4 = createFile(fs, file4, 1);
      System.out.println("testFileCreationNamenodeRestart: "
                         + "Created file " + file4);

      fs.mkdirs(new Path("/bin"));
      fs.rename(new Path("/user/home"), new Path("/bin"));
      Path file3new = new Path("/bin/home/fullpath.dat");
      System.out.println("testFileCreationNamenodeRestart: "
                         + "Renamed file " + file3 + " to " +
                         file3new);
      Path file4new = new Path("/bin/home/fullpath4.dat");
      System.out.println("testFileCreationNamenodeRestart: "
                         + "Renamed file " + file4 + " to " +
                         file4new);

      // restart cluster with the same namenode port as before.
      // This ensures that leases are persisted in fsimage.
      cluster.shutdown();
      try {
        Thread.sleep(2*MAX_IDLE_TIME);
      } catch (InterruptedException e) {
      }
      cluster = new MiniDFSCluster(nnport, conf, 1, false, true, 
                                   null, null, null);
      cluster.waitActive();

      // restart cluster yet again. This triggers the code to read in
      // persistent leases from fsimage.
      cluster.shutdown();
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
      }
      cluster = new MiniDFSCluster(nnport, conf, 1, false, true, 
                                   null, null, null);
      cluster.waitActive();
      fs = cluster.getFileSystem();

      // instruct the dfsclient to use a new filename when it requests
      // new blocks for files that were renamed.
      DFSClient.DFSOutputStream dfstream = (DFSClient.DFSOutputStream)
                                                 (stm.getWrappedStream());
      dfstream.setTestFilename(file1.toString());
      dfstream = (DFSClient.DFSOutputStream) (stm3.getWrappedStream());
      dfstream.setTestFilename(file3new.toString());
      dfstream = (DFSClient.DFSOutputStream) (stm4.getWrappedStream());
      dfstream.setTestFilename(file4new.toString());

      // write 1 byte to file.  This should succeed because the 
      // namenode should have persisted leases.
      byte[] buffer = AppendTestUtil.randomBytes(seed, 1);
      stm.write(buffer);
      stm.close();
      stm2.write(buffer);
      stm2.close();
      stm3.close();
      stm4.close();

      // verify that new block is associated with this file
      DFSClient client = ((DistributedFileSystem)fs).dfs;
      LocatedBlocks locations = client.namenode.getBlockLocations(
                                  file1.toString(), 0, Long.MAX_VALUE);
      System.out.println("locations = " + locations.locatedBlockCount());
      assertTrue("Error blocks were not cleaned up for file " + file1,
                 locations.locatedBlockCount() == 3);

      // verify filestatus2.dat
      locations = client.namenode.getBlockLocations(
                                  file2.toString(), 0, Long.MAX_VALUE);
      System.out.println("locations = " + locations.locatedBlockCount());
      assertTrue("Error blocks were not cleaned up for file " + file2,
                 locations.locatedBlockCount() == 1);
    } finally {
      IOUtils.closeStream(fs);
      cluster.shutdown();
    }
  }

  /**
   * Test that all open files are closed when client dies abnormally.
   */
  public void testDFSClientDeath() throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    System.out.println("Testing adbornal client death.");
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    DFSClient dfsclient = dfs.dfs;
    try {

      // create a new file in home directory. Do not close it.
      //
      Path file1 = new Path("/clienttest.dat");
      FSDataOutputStream stm = createFile(fs, file1, 1);
      System.out.println("Created file clienttest.dat");

      // write to file
      writeFile(stm);

      // close the dfsclient before closing the output stream.
      // This should close all existing file.
      dfsclient.close();

      // reopen file system and verify that file exists.
      assertTrue(file1 + " does not exist.", 
          AppendTestUtil.createHdfsWithDifferentUsername(conf).exists(file1));
    } finally {
      cluster.shutdown();
    }
  }

/**
 * Test that file data becomes available before file is closed.
 */
  public void testFileCreationSimulated() throws IOException {
    simulatedStorage = true;
    testFileCreation();
    simulatedStorage = false;
  }

  /**
   * Test creating two files at the same time. 
   */
  public void testConcurrentFileCreation() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);

    try {
      FileSystem fs = cluster.getFileSystem();
      
      Path[] p = {new Path("/foo"), new Path("/bar")};
      
      //write 2 files at the same time
      FSDataOutputStream[] out = {fs.create(p[0]), fs.create(p[1])};
      int i = 0;
      for(; i < 100; i++) {
        out[0].write(i);
        out[1].write(i);
      }
      out[0].close();
      for(; i < 200; i++) {out[1].write(i);}
      out[1].close();

      //verify
      FSDataInputStream[] in = {fs.open(p[0]), fs.open(p[1])};  
      for(i = 0; i < 100; i++) {assertEquals(i, in[0].read());}
      for(i = 0; i < 200; i++) {assertEquals(i, in[1].read());}
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  /**
   * Create a file, write something, fsync but not close.
   * Then change lease period and wait for lease recovery.
   * Finally, read the block directly from each Datanode and verify the content.
   */
  public void testLeaseExpireHardLimit() throws Exception {
    System.out.println("testLeaseExpireHardLimit start");
    final long leasePeriod = 1000;
    final int DATANODE_NUM = 3;

    Configuration conf = new Configuration();
    conf.setInt("heartbeat.recheck.interval", 1000);
    conf.setInt("dfs.heartbeat.interval", 1);

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster(conf, DATANODE_NUM, true, null);
    DistributedFileSystem dfs = null;
    try {
      cluster.waitActive();
      dfs = (DistributedFileSystem)cluster.getFileSystem();

      // create a new file.
      final String f = DIR + "foo";
      final Path fpath = new Path(f);
      FSDataOutputStream out = TestFileCreation.createFile(dfs, fpath, DATANODE_NUM);
      out.write("something".getBytes());
      out.sync();
      int actualRepl = ((DFSClient.DFSOutputStream)(out.getWrappedStream())).
                        getNumCurrentReplicas();
      assertTrue(f + " should be replicated to " + DATANODE_NUM + " datanodes.",
                 actualRepl == DATANODE_NUM);

      // set the soft and hard limit to be 1 second so that the
      // namenode triggers lease recovery
      cluster.setLeasePeriod(leasePeriod, leasePeriod);
      // wait for the lease to expire
      try {Thread.sleep(5 * leasePeriod);} catch (InterruptedException e) {}

      LocatedBlocks locations = dfs.dfs.namenode.getBlockLocations(
          f, 0, Long.MAX_VALUE);
      assertEquals(1, locations.locatedBlockCount());
      LocatedBlock locatedblock = locations.getLocatedBlocks().get(0);
      int successcount = 0;
      for(DatanodeInfo datanodeinfo: locatedblock.getLocations()) {
        DataNode datanode = cluster.getDataNode(datanodeinfo.ipcPort);
        FSDataset dataset = (FSDataset)datanode.data;
        Block b = dataset.getStoredBlock(locatedblock.getBlock().getBlockId());
        File blockfile = dataset.findBlockFile(b.getBlockId());
        System.out.println("blockfile=" + blockfile);
        if (blockfile != null) {
          BufferedReader in = new BufferedReader(new FileReader(blockfile));
          assertEquals("something", in.readLine());
          in.close();
          successcount++;
        }
      }
      System.out.println("successcount=" + successcount);
      assertTrue(successcount > 0); 
    } finally {
      IOUtils.closeStream(dfs);
      cluster.shutdown();
    }

    System.out.println("testLeaseExpireHardLimit successful");
  }

  // test closing file system before all file handles are closed.
  public void testFsClose() throws Exception {
    System.out.println("test file system close start");
    final int DATANODE_NUM = 3;

    Configuration conf = new Configuration();

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster(conf, DATANODE_NUM, true, null);
    DistributedFileSystem dfs = null;
    try {
      cluster.waitActive();
      dfs = (DistributedFileSystem)cluster.getFileSystem();

      // create a new file.
      final String f = DIR + "foofs";
      final Path fpath = new Path(f);
      FSDataOutputStream out = TestFileCreation.createFile(dfs, fpath, DATANODE_NUM);
      out.write("something".getBytes());

      // close file system without closing file
      dfs.close();
    } finally {
      System.out.println("testFsClose successful");
    }
  }
}
