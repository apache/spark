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

import junit.framework.TestCase;


import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient.DFSOutputStream;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetTestUtil;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSImageAdapter;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import static org.apache.hadoop.hdfs.AppendTestUtil.loseLeases;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;

/* File Append tests for HDFS-200 & HDFS-142, specifically focused on:
 *  using append()/sync() to recover block information
 */
public class TestFileAppend4 extends TestCase {
  static final Log LOG = LogFactory.getLog(TestFileAppend4.class);
  static final long BLOCK_SIZE = 1024;
  static final long BBW_SIZE = 500; // don't align on bytes/checksum

  static final Object [] NO_ARGS = new Object []{};

  Configuration conf;
  MiniDFSCluster cluster;
  Path file1;
  FSDataOutputStream stm;
  boolean simulatedStorage = false;

  {
    ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
  }

  @Override
  public void setUp() throws Exception {
    this.conf = new Configuration();
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    conf.setBoolean("dfs.support.broken.append", true);

    // lower heartbeat interval for fast recognition of DN death
    conf.setInt("heartbeat.recheck.interval", 1000);
    conf.setInt("dfs.heartbeat.interval", 1);
    conf.setInt("dfs.socket.timeout", 5000);
    // handle under-replicated blocks quickly (for replication asserts)
//    conf.set("dfs.replication.pending.timeout.sec", Integer.toString(5));
    conf.setInt("dfs.replication.pending.timeout.sec", 5);
    conf.setInt("dfs.replication.interval", 1);
    // handle failures in the DFSClient pipeline quickly
    // (for cluster.shutdown(); fs.close() idiom)
    conf.setInt("ipc.client.connect.max.retries", 1);
    conf.setInt("dfs.client.block.recovery.retries", 1);
    // Delay blockReceived calls from DNs to be more similar to a real
    // cluster. 10ms is enough so that client often gets there first.
    conf.setInt("dfs.datanode.artificialBlockReceivedDelay", 10);
  }

  @Override
  public void tearDown() throws Exception {
    
  }
  
  private void createFile(FileSystem whichfs, String filename, 
                  int rep, long fileSize) throws Exception {
    file1 = new Path(filename);
    stm = whichfs.create(file1, true, (int)fileSize+1, (short)rep, BLOCK_SIZE);
    LOG.info("Created file " + filename);
    LOG.info("Writing " + fileSize + " bytes to " + file1);
    AppendTestUtil.write(stm, 0, (int)fileSize);
  }
  
  private void assertFileSize(FileSystem whichfs, long expectedSize) throws Exception {
    LOG.info("reading length of " + file1.getName() + " on namenode");
    long realSize = whichfs.getFileStatus(file1).getLen();
    assertTrue("unexpected file size! received=" + realSize 
                                + " , expected=" + expectedSize, 
               realSize == expectedSize);
  }
  
  private void assertNumCurrentReplicas(short rep) throws Exception {
    OutputStream hdfs_out = stm.getWrappedStream();
    Method r = hdfs_out.getClass().getMethod("getNumCurrentReplicas",
                                             new Class<?> []{});
    r.setAccessible(true);
    int actualRepl = ((Integer)r.invoke(hdfs_out, NO_ARGS)).intValue();
    assertTrue(file1 + " should be replicated to " + rep + " datanodes, not " +
               actualRepl + ".", actualRepl == rep);
  }

  /*
   * Recover file.
   * Try and open file in append mode.
   * Doing this, we get a hold of the file that crashed writer
   * was writing to.  Once we have it, close it.  This will
   * allow subsequent reader to see up to last sync.
   * NOTE: This is the same algorithm that HBase uses for file recovery
   * @param fs
   * @throws Exception
   */
  private void recoverFile(final FileSystem fs) throws Exception {
    LOG.info("Recovering File Lease");
    AppendTestUtil.recoverFile(cluster, fs, file1);
    LOG.info("Past out lease recovery");
  }
  
  // Waits for all of the blocks to have expected replication
  private void waitForBlockReplication(FileSystem whichfs, String filename, 
                                       int expected, long maxWaitSec) 
                                       throws IOException {
    long start = System.currentTimeMillis();
    
    //wait for all the blocks to be replicated;
    LOG.info("Checking for block replication for " + filename);
    int iters = 0;
    while (true) {
      boolean replOk = true;

      BlockLocation[] bl = whichfs.getFileBlockLocations(
          whichfs.getFileStatus(file1), 0, BLOCK_SIZE);
      if(bl.length == 0) {
        replOk = false;
      }
      for (BlockLocation b : bl) {
        
        int actual = b.getNames().length;
        if ( actual < expected ) {
          LOG.info("Not enough replicas for " + b +
              " yet. Expecting " + expected + ", got " + 
              actual + ".");
          replOk = false;
          break;
        }
      }
      
      if (replOk) {
        return;
      }
      
      iters++;
      
      if (maxWaitSec > 0 && 
          (System.currentTimeMillis() - start) > (maxWaitSec * 1000)) {
        throw new IOException("Timedout while waiting for all blocks to " +
                              " be replicated for " + filename);
      }
      
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignored) {}
    }
  }

  private void checkFile(FileSystem whichfs, long fileSize) throws Exception {
    LOG.info("validating content from datanodes...");
    AppendTestUtil.check(whichfs, file1, fileSize);
  }
  
  enum CorruptionType {
    CORRUPT_LAST_CHUNK,
    TRUNCATE_BLOCK_TO_ZERO,
    TRUNCATE_BLOCK_HALF;
  }
  
  /**
   * Corrupt all of the blocks in the blocksBeingWritten dir
   * for the specified datanode number. The corruption is
   * specifically the last checksum chunk of the file being
   * modified by writing random data into it.
   */
  private void corruptDataNode(int dnNumber, CorruptionType type) throws Exception {
    // get the FS data of the specified datanode
    File data_dir = new File(System.getProperty("test.build.data"),
                             "dfs/data/data" + 
                             Integer.toString(dnNumber*2 + 1) + 
                             "/blocksBeingWritten");
    int corrupted = 0;
    for (File block : data_dir.listFiles()) {
      // only touch the actual data, not the metadata (with CRC)
      if (block.getName().startsWith("blk_") && 
         !block.getName().endsWith("meta")) {
        if (type == CorruptionType.CORRUPT_LAST_CHUNK) {
          RandomAccessFile file = new RandomAccessFile(block, "rw");
          FileChannel channel = file.getChannel();
          Random r = new Random();
          long lastBlockSize = channel.size() % 512;
          long position = channel.size() - lastBlockSize;
          int length = r.nextInt((int)(channel.size() - position + 1));
          byte[] buffer = new byte[length];
          r.nextBytes(buffer);


          channel.write(ByteBuffer.wrap(buffer), position);
          System.out.println("Deliberately corrupting file " + block.getName() + 
                             " at offset " + position +
                             " length " + length);
          file.close();

        } else if (type == CorruptionType.TRUNCATE_BLOCK_TO_ZERO) {
          LOG.info("Truncating block file at " + block);
          RandomAccessFile blockFile = new RandomAccessFile(block, "rw");
          blockFile.setLength(0);
          blockFile.close();
          
          RandomAccessFile metaFile = new RandomAccessFile(
              FSDataset.findMetaFile(block), "rw");
          metaFile.setLength(0);
          metaFile.close();
        } else if (type == CorruptionType.TRUNCATE_BLOCK_HALF) {
          FSDatasetTestUtil.truncateBlockFile(block, block.length() / 2);
        } else {
          assert false;
        }
        ++corrupted;
      }
    }
    assertTrue("Should have some data in bbw to corrupt", corrupted > 0);
  }

  // test [1 bbw, 0 HDFS block]
  public void testAppendSyncBbw() throws Exception {
    LOG.info("START");
    cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs1 = cluster.getFileSystem();;
    FileSystem fs2 = AppendTestUtil.createHdfsWithDifferentUsername(fs1.getConf());
    try {
      createFile(fs1, "/bbw.test", 1, BBW_SIZE);
      stm.sync();
      // empty before close()
      assertFileSize(fs1, 0); 
      loseLeases(fs1);
      recoverFile(fs2);
      // close() should write recovered bbw to HDFS block
      assertFileSize(fs2, BBW_SIZE); 
      checkFile(fs2, BBW_SIZE);
    } finally {
      fs2.close();
      fs1.close();
      cluster.shutdown();
    }
    LOG.info("STOP");
  }

  // test [1 bbw, 0 HDFS block] with cluster restart
  public void testAppendSyncBbwClusterRestart() throws Exception {
    LOG.info("START");
    cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs1 = cluster.getFileSystem();
    FileSystem fs2 = null;
    try {
      createFile(fs1, "/bbwRestart.test", 1, BBW_SIZE);
      stm.sync();
      // empty before close()
      assertFileSize(fs1, 0); 

      cluster.shutdown();
      fs1.close(); // same as: loseLeases()
      LOG.info("STOPPED first instance of the cluster");

      cluster = new MiniDFSCluster(conf, 1, false, null);
      cluster.waitActive();
      LOG.info("START second instance.");

      fs2 = cluster.getFileSystem();

      recoverFile(fs2);
      
      // close() should write recovered bbw to HDFS block
      assertFileSize(fs2, BBW_SIZE); 
      checkFile(fs2, BBW_SIZE);

    } finally {
      if(fs2 != null) {
        fs2.close();
      }
      fs1.close();
      cluster.shutdown();
    }
    LOG.info("STOP");
  }


  // test [3 bbw, 0 HDFS block] with cluster restart
  // ** previous HDFS-142 patches hit an problem with multiple outstanding bbw on a single disk**
  public void testAppendSync2XBbwClusterRestart() throws Exception {
    LOG.info("START");
    cluster = new MiniDFSCluster(conf, 1, true, null);
    // assumption: this MiniDFS starts up 1 datanode with 2 dirs to load balance
    assertTrue(cluster.getDataNodes().get(0).getConf().get("dfs.data.dir").matches("[^,]+,[^,]*"));
    FileSystem fs1 = cluster.getFileSystem();
    FileSystem fs2 = null;
    try {
      // create 3 bbw files [so at least one dir has 2 files]
      int[] files = new int[]{0,1,2};
      Path[] paths = new Path[files.length];
      FSDataOutputStream[] stms = new FSDataOutputStream[files.length];
      for (int i : files ) {
        createFile(fs1, "/bbwRestart" + i + ".test", 1, BBW_SIZE);
        stm.sync();
        assertFileSize(fs1, 0); 
        paths[i] = file1;
        stms[i] = stm;
      }

      cluster.shutdown();
      fs1.close(); // same as: loseLeases()
      LOG.info("STOPPED first instance of the cluster");

      cluster = new MiniDFSCluster(conf, 1, false, null);
      cluster.waitActive();
      LOG.info("START second instance.");

      fs2 = cluster.getFileSystem();
      
      // recover 3 bbw files
      for (int i : files) {
        file1 = paths[i];
        recoverFile(fs2);
        assertFileSize(fs2, BBW_SIZE); 
        checkFile(fs2, BBW_SIZE);
      }
    } finally {
      if(fs2 != null) {
        fs2.close();
      }
      fs1.close();
      cluster.shutdown();
    }
    LOG.info("STOP");
  }  
  // test [1 bbw, 1 HDFS block]
  public void testAppendSyncBlockPlusBbw() throws Exception {
    LOG.info("START");
    cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs1 = cluster.getFileSystem();;
    FileSystem fs2 = AppendTestUtil.createHdfsWithDifferentUsername(fs1.getConf());
    try {
      createFile(fs1, "/blockPlusBbw.test", 1, BLOCK_SIZE + BBW_SIZE);
      // 0 before sync()
      assertFileSize(fs1, 0); 
      stm.sync();
      // BLOCK_SIZE after sync()
      assertFileSize(fs1, BLOCK_SIZE); 
      loseLeases(fs1);
      recoverFile(fs2);
      // close() should write recovered bbw to HDFS block
      assertFileSize(fs2, BLOCK_SIZE + BBW_SIZE); 
      checkFile(fs2, BLOCK_SIZE + BBW_SIZE);
    } finally {
      stm = null;
      fs2.close();
      fs1.close();
      cluster.shutdown();
    }
    LOG.info("STOP");
  }

  // we test different datanodes restarting to exercise 
  // the start, middle, & end of the DFSOutputStream pipeline
  public void testAppendSyncReplication0() throws Exception {
    replicationTest(0);
  }
  public void testAppendSyncReplication1() throws Exception {
    replicationTest(1);
  }
  public void testAppendSyncReplication2() throws Exception {
    replicationTest(2);
  }
  
  void replicationTest(int badDN) throws Exception {
    LOG.info("START");
    cluster = new MiniDFSCluster(conf, 3, true, null);
    FileSystem fs1 = cluster.getFileSystem();
    try {
      int halfBlock = (int)BLOCK_SIZE/2;
      short rep = 3; // replication
      assertTrue(BLOCK_SIZE%4 == 0);

      file1 = new Path("/appendWithReplication.dat");

      // write 1/2 block & sync
      stm = fs1.create(file1, true, (int)BLOCK_SIZE*2, rep, BLOCK_SIZE);
      AppendTestUtil.write(stm, 0, halfBlock);
      stm.sync();
      assertNumCurrentReplicas(rep);
      
      // close one of the datanodes
      cluster.stopDataNode(badDN);
      
      // write 1/4 block & sync
      AppendTestUtil.write(stm, halfBlock, (int)BLOCK_SIZE/4);
      stm.sync();
      assertNumCurrentReplicas((short)(rep - 1));
      
      // restart the cluster
      /* 
       * we put the namenode in safe mode first so he doesn't process 
       * recoverBlock() commands from the remaining DFSClient as datanodes 
       * are serially shutdown
       */
      cluster.getNameNode().setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      cluster.shutdown();
      fs1.close();
      LOG.info("STOPPED first instance of the cluster");
      cluster = new MiniDFSCluster(conf, 3, false, null);
      cluster.getNameNode().getNamesystem().stallReplicationWork();
      cluster.waitActive();
      fs1 = cluster.getFileSystem();
      LOG.info("START second instance.");

      recoverFile(fs1);
      LOG.info("Recovered file");
      
      // the 2 DNs with the larger sequence number should win
      BlockLocation[] bl = fs1.getFileBlockLocations(
          fs1.getFileStatus(file1), 0, BLOCK_SIZE);
      LOG.info("Checking blocks");
      assertTrue("Should have one block", bl.length == 1);
      
      // Wait up to 1 second for block replication - we may have
      // only replication 1 for a brief moment after close, since
      // closing only waits for fs.replcation.min replicas, and
      // it may take some millis before the other DN reports block
      waitForBlockReplication(fs1, file1.toString(), 2, 1);

      assertFileSize(fs1, BLOCK_SIZE*3/4);
      checkFile(fs1, BLOCK_SIZE*3/4);

      LOG.info("Checking replication");
      // verify that, over time, the block has been replicated to 3 DN
      cluster.getNameNode().getNamesystem().restartReplicationWork();
      waitForBlockReplication(fs1, file1.toString(), 3, 20);
    } finally {
      fs1.close();
      cluster.shutdown();
    }
  }

  // we test different datanodes restarting to exercise 
  // the start, middle, & end of the DFSOutputStream pipeline
  public void testAppendSyncChecksum0() throws Exception {
    checksumTest(0);
  }
  public void testAppendSyncChecksum1() throws Exception {
    checksumTest(1);
  }
  public void testAppendSyncChecksum2() throws Exception {
    checksumTest(2);
  }

  void checksumTest(int goodDN) throws Exception {
    int deadDN = (goodDN + 1) % 3;
    int corruptDN  = (goodDN + 2) % 3;
    
    LOG.info("START");
    cluster = new MiniDFSCluster(conf, 3, true, null);
    FileSystem fs1 = cluster.getFileSystem();
    try {
      int halfBlock = (int)BLOCK_SIZE/2;
      short rep = 3; // replication
      assertTrue(BLOCK_SIZE%8 == 0);

      file1 = new Path("/appendWithReplication.dat");

      // write 1/2 block & sync
      stm = fs1.create(file1, true, (int)BLOCK_SIZE*2, rep, BLOCK_SIZE);
      AppendTestUtil.write(stm, 0, halfBlock);
      stm.sync();
      assertNumCurrentReplicas(rep);
      
      // close one of the datanodes
      cluster.stopDataNode(deadDN);
      
      // write 1/4 block & sync
      AppendTestUtil.write(stm, halfBlock, (int)BLOCK_SIZE/4);
      stm.sync();
      assertNumCurrentReplicas((short)(rep - 1));
      
      // stop the cluster
      cluster.getNameNode().setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      cluster.shutdown();
      fs1.close();
      LOG.info("STOPPED first instance of the cluster");

      // give the second datanode a bad CRC
      corruptDataNode(corruptDN, CorruptionType.CORRUPT_LAST_CHUNK);
      
      // restart the cluster
      cluster = new MiniDFSCluster(conf, 3, false, null);
      cluster.getNameNode().getNamesystem().stallReplicationWork();
      cluster.waitActive();
      fs1 = cluster.getFileSystem();
      LOG.info("START second instance.");

      // verify that only the good datanode's file is used
      recoverFile(fs1);

      BlockLocation[] bl = fs1.getFileBlockLocations(
          fs1.getFileStatus(file1), 0, BLOCK_SIZE);
      assertTrue("Should have one block", bl.length == 1);
      assertTrue("Should have 1 replica for that block, not " + 
          bl[0].getNames().length, bl[0].getNames().length == 1);  

      assertTrue("The replica should be the datanode with the correct CRC",
                 cluster.getDataNodes().get(goodDN).getSelfAddr().toString()
                   .endsWith(bl[0].getNames()[0]) );
      assertFileSize(fs1, BLOCK_SIZE*3/4);

      // should fail checkFile() if data with the bad CRC was used
      checkFile(fs1, BLOCK_SIZE*3/4);

      // ensure proper re-replication
      cluster.getNameNode().getNamesystem().restartReplicationWork();
      waitForBlockReplication(fs1, file1.toString(), 3, 20);
    } finally {
      cluster.shutdown();
      fs1.close();
    }
  }
  
  // we test different datanodes dying and not coming back
  public void testDnDeath0() throws Exception {
    dnDeathTest(0);
  }
  public void testDnDeath1() throws Exception {
    dnDeathTest(1);
  }
  public void testDnDeath2() throws Exception {
    dnDeathTest(2);
  }

  /**
   * Test case that writes and completes a file, and then
   * tries to recover the file after the old primary
   * DN has failed.
   */
  void dnDeathTest(int badDN) throws Exception {
    LOG.info("START");
    cluster = new MiniDFSCluster(conf, 3, true, null);
    FileSystem fs1 = cluster.getFileSystem();
    try {
      int halfBlock = (int)BLOCK_SIZE/2;
      short rep = 3; // replication
      assertTrue(BLOCK_SIZE%4 == 0);

      file1 = new Path("/dnDeath.dat");

      // write 1/2 block & close
      stm = fs1.create(file1, true, (int)BLOCK_SIZE*2, rep, BLOCK_SIZE);
      AppendTestUtil.write(stm, 0, halfBlock);
      stm.close();
      
      // close one of the datanodes
      cluster.stopDataNode(badDN);

      // Recover the lease
      recoverFile(fs1);
      checkFile(fs1, halfBlock);
    } finally {
      fs1.close();
      cluster.shutdown();
    }
  }

  /**
   * Test case that stops a writer after finalizing a block but
   * before calling completeFile, and then tries to recover
   * the lease.
   */
  public void testRecoverFinalizedBlock() throws Throwable {
    cluster = new MiniDFSCluster(conf, 3, true, null);

    try {
      cluster.waitActive();
      NameNode preSpyNN = cluster.getNameNode();
      NameNode spyNN = spy(preSpyNN);

      // Delay completeFile
      DelayAnswer delayer = new DelayAnswer();
      doAnswer(delayer).when(spyNN).complete(anyString(), anyString());

      DFSClient client = new DFSClient(null, spyNN, conf, null);
      file1 = new Path("/testRecoverFinalized");
      final OutputStream stm = client.create("/testRecoverFinalized", true);

      // write 1/2 block
      AppendTestUtil.write(stm, 0, 4096);
      final AtomicReference<Throwable> err = new AtomicReference<Throwable>();
      Thread t = new Thread() { 
          public void run() {
            try {
              stm.close();
            } catch (Throwable t) {
              err.set(t);
            }
          }};
      t.start();
      LOG.info("Waiting for close to get to latch...");
      delayer.waitForCall();

      // At this point, the block is finalized on the DNs, but the file
      // has not been completed in the NN.
      // Lose the leases
      LOG.info("Killing lease checker");
      client.leasechecker.interruptAndJoin();

      FileSystem fs1 = cluster.getFileSystem();
      FileSystem fs2 = AppendTestUtil.createHdfsWithDifferentUsername(
        fs1.getConf());

      LOG.info("Recovering file");
      recoverFile(fs2);

      LOG.info("Telling close to proceed.");
      delayer.proceed();
      LOG.info("Waiting for close to finish.");
      t.join();
      LOG.info("Close finished.");

      // We expect that close will get a "Could not complete file"
      // error.
      Throwable thrownByClose = err.get();
      assertNotNull(thrownByClose);
      assertTrue(thrownByClose instanceof IOException);
      if (!thrownByClose.getMessage().contains(
        "File is not open for writing.")) {
        throw thrownByClose;
      }
    } finally {
      cluster.shutdown();
    }
  }


  /**
   * Test case that stops a writer after finalizing a block but
   * before calling completeFile, recovers a file from another writer,
   * starts writing from that writer, and then has the old lease holder
   * call completeFile
   */
  public void testCompleteOtherLeaseHoldersFile() throws Throwable {
    cluster = new MiniDFSCluster(conf, 3, true, null);

    try {
      cluster.waitActive();
      NameNode preSpyNN = cluster.getNameNode();
      NameNode spyNN = spy(preSpyNN);

      // Delay completeFile
      DelayAnswer delayer = new DelayAnswer();
      doAnswer(delayer).when(spyNN).complete(anyString(), anyString());

      DFSClient client = new DFSClient(null, spyNN, conf, null);
      file1 = new Path("/testRecoverFinalized");
      final OutputStream stm = client.create("/testRecoverFinalized", true);

      // write 1/2 block
      AppendTestUtil.write(stm, 0, 4096);
      final AtomicReference<Throwable> err = new AtomicReference<Throwable>();
      Thread t = new Thread() { 
          public void run() {
            try {
              stm.close();
            } catch (Throwable t) {
              err.set(t);
            }
          }};
      t.start();
      LOG.info("Waiting for close to get to latch...");
      delayer.waitForCall();

      // At this point, the block is finalized on the DNs, but the file
      // has not been completed in the NN.
      // Lose the leases
      LOG.info("Killing lease checker");
      client.leasechecker.interruptAndJoin();

      FileSystem fs1 = cluster.getFileSystem();
      FileSystem fs2 = AppendTestUtil.createHdfsWithDifferentUsername(
        fs1.getConf());

      LOG.info("Recovering file");
      recoverFile(fs2);

      LOG.info("Opening file for append from new fs");
      FSDataOutputStream appenderStream = fs2.append(file1);
      
      LOG.info("Writing some data from new appender");
      AppendTestUtil.write(appenderStream, 0, 4096);
      
      LOG.info("Telling old close to proceed.");
      delayer.proceed();
      LOG.info("Waiting for close to finish.");
      t.join();
      LOG.info("Close finished.");

      // We expect that close will get a "Lease mismatch"
      // error.
      Throwable thrownByClose = err.get();
      assertNotNull(thrownByClose);
      assertTrue(thrownByClose instanceof IOException);
      if (!thrownByClose.getMessage().contains(
            "Lease mismatch"))
        throw thrownByClose;
      
      // The appender should be able to close properly
      appenderStream.close();
    } finally {
      cluster.shutdown();
    }
  }  
  
  /**
   * Test for an intermittent failure of commitBlockSynchronization.
   * This could happen if the DN crashed between calling updateBlocks
   * and commitBlockSynchronization.
   */
  public void testDatanodeFailsToCommit() throws Throwable {
    LOG.info("START");
    cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs1 = cluster.getFileSystem();;
    FileSystem fs2 = AppendTestUtil.createHdfsWithDifferentUsername(fs1.getConf());
    try {
      createFile(fs1, "/datanodeFailsCommit.test", 1, BBW_SIZE);
      stm.sync();
      loseLeases(fs1);

      // Make the NN fail to commitBlockSynchronization one time
      NameNode nn = cluster.getNameNode();
      nn.namesystem = spy(nn.namesystem);
      doAnswer(new ThrowNTimesAnswer(IOException.class, 1)).
        when(nn.namesystem).
        commitBlockSynchronization((Block)anyObject(), anyInt(), anyInt(),
                                   anyBoolean(), anyBoolean(),
                                   (DatanodeID[])anyObject());

      recoverFile(fs2);
      // close() should write recovered bbw to HDFS block
      assertFileSize(fs2, BBW_SIZE); 
      checkFile(fs2, BBW_SIZE);
    } finally {
      fs2.close();
      fs1.close();
      cluster.shutdown();
    }
    LOG.info("STOP");
  }

  
  /**
   * Test that when a DN starts up with bbws from a file that got
   * removed or finalized when it was down, the block gets deleted.
   */
  public void testBBWCleanupOnStartup() throws Throwable {
    LOG.info("START");
    cluster = new MiniDFSCluster(conf, 3, true, null);
    FileSystem fs1 = cluster.getFileSystem();
    try {
      int halfBlock = (int) BLOCK_SIZE / 2;
      short rep = 3; // replication
      assertTrue(BLOCK_SIZE % 4 == 0);

      file1 = new Path("/bbwCleanupOnStartup.dat");

      // write 1/2 block & sync
      stm = fs1.create(file1, true, (int) BLOCK_SIZE * 2, rep, BLOCK_SIZE);
      AppendTestUtil.write(stm, 0, halfBlock);
      stm.sync();

      String dataDirs = cluster.getDataNodes().get(0).getConf().get("dfs.data.dir");
      // close one of the datanodes
      MiniDFSCluster.DataNodeProperties dnprops = cluster.stopDataNode(0);

      stm.close();

      List<File> bbwFilesAfterShutdown = getBBWFiles(dataDirs);
      assertEquals(1, bbwFilesAfterShutdown.size());

      assertTrue(cluster.restartDataNode(dnprops));

      List<File> bbwFilesAfterRestart = null;
      // Wait up to 10 heartbeats for the files to get removed - it should
      // really happen after just a couple.
      for (int i = 0; i < 10; i++) {
        LOG.info("Waiting for heartbeat #" + i + " after DN restart");
        cluster.waitForDNHeartbeat(0, 10000);

        // Check if it has been deleted
        bbwFilesAfterRestart = getBBWFiles(dataDirs);
        if (bbwFilesAfterRestart.size() == 0) {
          break;
        }
      }

      assertEquals(0, bbwFilesAfterRestart.size());

    } finally {
      fs1.close();
      cluster.shutdown();
    }
  }

  private List<File> getBBWFiles(String dfsDataDirs) {
    ArrayList<File> files = new ArrayList<File>();
    for (String dirString : dfsDataDirs.split(",")) {
      File dir = new File(dirString);
      assertTrue("data dir " + dir + " should exist",
        dir.exists());
      File bbwDir = new File(dir, "blocksBeingWritten");
      assertTrue("bbw dir " + bbwDir + " should eixst",
        bbwDir.exists());
      for (File blockFile : bbwDir.listFiles()) {
        if (!blockFile.getName().endsWith(".meta")) {
          files.add(blockFile);
        }
      }
    }
    return files;
  }

  /**
   * Test for following sequence:
   * 1. Client finishes writing a block, but does not allocate next one
   * 2. Client loses lease
   * 3. Recovery process starts, but commitBlockSynchronization not called yet
   * 4. Client calls addBlock and continues writing
   * 5. commitBlockSynchronization proceeds
   * 6. Original client tries to write/close
   */
  public void testRecoveryOnBlockBoundary() throws Throwable {
    LOG.info("START");
    cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs1 = cluster.getFileSystem();
    ;
    final FileSystem fs2 = AppendTestUtil.createHdfsWithDifferentUsername(fs1.getConf());

    // Allow us to delay commitBlockSynchronization
    DelayAnswer delayer = new DelayAnswer();
    NameNode nn = cluster.getNameNode();
    nn.namesystem = spy(nn.namesystem);
    doAnswer(delayer).
      when(nn.namesystem).
      commitBlockSynchronization((Block) anyObject(), anyInt(), anyInt(),
        anyBoolean(), anyBoolean(),
        (DatanodeID[]) anyObject());

    try {
      file1 = new Path("/testWritingDuringRecovery.test");
      stm = fs1.create(file1, true, (int) BLOCK_SIZE * 2, (short) 3, BLOCK_SIZE);
      AppendTestUtil.write(stm, 0, (int) (BLOCK_SIZE));
      stm.sync();

      LOG.info("Losing lease");
      loseLeases(fs1);


      LOG.info("Triggering recovery in another thread");

      final AtomicReference<Throwable> err = new AtomicReference<Throwable>();
      Thread recoverThread = new Thread() {
        public void run() {
          try {
            recoverFile(fs2);
          } catch (Throwable t) {
            err.set(t);
          }
        }
      };
      recoverThread.start();

      LOG.info("Waiting for recovery about to call commitBlockSynchronization");
      delayer.waitForCall();

      LOG.info("Continuing to write to stream");
      AppendTestUtil.write(stm, 0, (int) (BLOCK_SIZE));
      try {
        stm.sync();
        fail("Sync was allowed after recovery started");
      } catch (IOException ioe) {
        LOG.info("Got expected IOE trying to write to a file from the writer " +
          "that lost its lease", ioe);
      }

      LOG.info("Written more to stream, allowing commit to proceed");
      delayer.proceed();

      LOG.info("Joining on recovery thread");
      recoverThread.join();
      if (err.get() != null) {
        throw err.get();
      }

      LOG.info("Now that recovery has finished, still expect further writes to fail.");
      try {
        AppendTestUtil.write(stm, 0, (int) (BLOCK_SIZE));
        stm.sync();
        fail("Further writes after recovery finished did not fail!");
      } catch (IOException ioe) {
        LOG.info("Got expected exception", ioe);
      }


      LOG.info("Checking that file looks good");

      // close() should write recovered only the first successful
      // writes
      assertFileSize(fs2, BLOCK_SIZE);
      checkFile(fs2, BLOCK_SIZE);
    } finally {
      try {
        fs2.close();
        fs1.close();
        cluster.shutdown();
      } catch (Throwable t) {
        LOG.warn("Didn't close down cleanly", t);
      }
    }
    LOG.info("STOP");
  }

  /**
   * Test for a race in appendFile where the file might get removed in between
   * the two synchronized sections.
   */
  public void testAppendFileRace() throws Throwable {
    LOG.info("START");
    cluster = new MiniDFSCluster(conf, 1, true, null);
    final FileSystem fs1 = cluster.getFileSystem();;

    try {
      createFile(fs1, "/testAppendFileRace", 1, BBW_SIZE);
      stm.close();

      NameNode nn = cluster.getNameNode();
      FSEditLog editLogSpy = FSImageAdapter.injectEditLogSpy(nn.getNamesystem());
      DelayAnswer  delayer = new DelayAnswer();
      doAnswer(delayer).when(editLogSpy).logSync();

      final AtomicReference<Throwable> err = new AtomicReference<Throwable>();
      Thread appender = new Thread() {
          public void run() {
            try {
              stm = fs1.append(file1);
            } catch (Throwable t) {
              err.set(t);
            }
          }
        };
      LOG.info("Triggering append in other thread");
      appender.start();

      LOG.info("Waiting for logsync");
      delayer.waitForCall();

      LOG.info("Resetting spy");
      reset(editLogSpy);

      LOG.info("Deleting file");
      fs1.delete(file1, true);

      LOG.info("Allowing append to proceed");
      delayer.proceed();

      LOG.info("Waiting for append to finish");

      appender.join();

      if (err.get() != null) {
        if (err.get().getMessage().contains(
              "File does not exist.")) {
          LOG.info("Got expected exception", err.get());
        } else {
          throw err.get();
        }
      }
      LOG.info("Closing stream");
      stm.close();
    } finally {
      fs1.close();
      cluster.shutdown();
    }
  }

  /**
   * Test for what happens when the machine doing the write totally
   * loses power, and thus when it restarts, the local replica has been
   * truncated to 0 bytes (very common with journaling filesystems)
   */
  public void testTruncatedPrimaryDN() throws Exception {
    LOG.info("START");
    runDNRestartCorruptType(CorruptionType.TRUNCATE_BLOCK_TO_ZERO);
  }
  
  /**
   * Test for what happens when the machine doing the write loses power
   * but a previous length of the block being written had made it to the
   * journal
   */
  public void testHalfLengthPrimaryDN() throws Exception {
    LOG.info("START");
    runDNRestartCorruptType(CorruptionType.TRUNCATE_BLOCK_HALF);
  }
  
  private void runDNRestartCorruptType(CorruptionType corrupt) throws Exception {
    cluster = new MiniDFSCluster(conf, 3, true, null);
    FileSystem fs1 = cluster.getFileSystem();
    try {
      short rep = 3; // replication
      assertTrue(BLOCK_SIZE%4 == 0);

      file1 = new Path("/dnDeath.dat");

      // write 1/2 block & close
      stm = fs1.create(file1, true, 1024, rep, 4096);
      AppendTestUtil.write(stm, 0, 1024);
      stm.sync();
      loseLeases(fs1);
      
      DFSOutputStream dfso = (DFSOutputStream)stm.getWrappedStream();
      dfso.abortForTests();
      
      // close the primary DN
      DataNodeProperties badDN = cluster.stopDataNode(0);
      
      // Truncate the block on the primary DN
      corruptDataNode(0, corrupt);

      // Start the DN back up
      cluster.restartDataNode(badDN);

      // Recover the lease
      FileSystem fs2 = AppendTestUtil.createHdfsWithDifferentUsername(fs1.getConf());
      recoverFile(fs2);
      
      assertFileSize(fs2, 1024);
      checkFile(fs2, 1024);
    } finally {
      // explicitly do not shut down fs1, since it's been frozen up by
      // killing the DataStreamer and not allowing recovery
      cluster.shutdown();
    }
  }

  public void testFullClusterPowerLoss() throws Exception {
    cluster = new MiniDFSCluster(conf, 2, true, null);
    FileSystem fs1 = cluster.getFileSystem();
    try {
      short rep = 2; // replication
      assertTrue(BLOCK_SIZE%4 == 0);

      file1 = new Path("/dnDeath.dat");

      // write 1/2 block & close
      stm = fs1.create(file1, true, 1024, rep, 4096);
      AppendTestUtil.write(stm, 0, 1024);
      stm.sync();
      loseLeases(fs1);
      
      DFSOutputStream dfso = (DFSOutputStream)stm.getWrappedStream();
      dfso.abortForTests();
      
      // close the DNs
      DataNodeProperties badDN = cluster.stopDataNode(0);
      DataNodeProperties badDN2 = cluster.stopDataNode(0); // what was 1 is now 0
      assertNotNull(badDN);
      assertNotNull(badDN2);
      
      // Truncate one of them as if its journal got corrupted
      corruptDataNode(0, CorruptionType.TRUNCATE_BLOCK_HALF);
      
      // Start the DN back up
      cluster.restartDataNode(badDN);
      cluster.restartDataNode(badDN2);
      
      // Wait for a heartbeat to make sure we get the initial block
      // report of the replicasBeingWritten
      cluster.waitForDNHeartbeat(0, 10000);
      cluster.waitForDNHeartbeat(1, 10000);
      
      // Recover the lease
      FileSystem fs2 = AppendTestUtil.createHdfsWithDifferentUsername(fs1.getConf());
      recoverFile(fs2);
      
      assertFileSize(fs2, 512);
      checkFile(fs2, 512);
    } finally {
      // explicitly do not shut down fs1, since it's been frozen up by
      // killing the DataStreamer and not allowing recovery
      cluster.shutdown();
    }    
  }
  
  /**
   * Test that a file is not considered complete when it only has in-progress
   * blocks. This ensures that when a block is appended to, it is converted
   * back into the right kind of "in progress" state.
   */
  public void testNotPrematurelyComplete() throws Exception {
    LOG.info("START");
    cluster = new MiniDFSCluster(conf, 3, true, null);
    FileSystem fs1 = cluster.getFileSystem();
    try {
      int halfBlock = (int)BLOCK_SIZE/2;
      short rep = 3; // replication
      assertTrue(BLOCK_SIZE%4 == 0);

      file1 = new Path("/delayedReceiveBlock");

      // write 1/2 block & close
      stm = fs1.create(file1, true, (int)BLOCK_SIZE*2, rep, BLOCK_SIZE);
      AppendTestUtil.write(stm, 0, halfBlock);
      stm.close();

      NameNode nn = cluster.getNameNode();
      LOG.info("======== Appending");
      stm = fs1.append(file1);
      LOG.info("======== Writing");
      AppendTestUtil.write(stm, 0, halfBlock/2);
      LOG.info("======== Checking progress");
      assertFalse(NameNodeAdapter.checkFileProgress(nn.namesystem, "/delayedReceiveBlock", true));
      LOG.info("======== Closing");
      stm.close();

    } finally {
      LOG.info("======== Cleaning up");
      fs1.close();
      cluster.shutdown();
    }
  }

  /**
   * Test that the restart of a DN and the subsequent pipeline recovery do not cause
   * a file to become prematurely considered "complete". (ie that the block
   * synchronization as part of pipeline recovery doesn't add the block to the
   * nodes taking part in recovery)
   */
  public void testNotPrematurelyCompleteWithFailure() throws Exception {
    LOG.info("START");
    cluster = new MiniDFSCluster(conf, 3, true, null);
    FileSystem fs1 = cluster.getFileSystem();
    try {
      int halfBlock = (int)BLOCK_SIZE/2;
      short rep = 3; // replication
      assertTrue(BLOCK_SIZE%4 == 0);

      file1 = new Path("/delayedReceiveBlock");

      // write 1/2 block & close
      stm = fs1.create(file1, true, (int)BLOCK_SIZE*2, rep, BLOCK_SIZE);
      AppendTestUtil.write(stm, 0, halfBlock);
      stm.close();

      NameNode nn = cluster.getNameNode();
      LOG.info("======== Appending");
      stm = fs1.append(file1);
      LOG.info("======== Writing");
      AppendTestUtil.write(stm, 0, halfBlock/4);

      // restart one of the datanodes and wait for a few of its heartbeats
      // so that it will report the recovered replica
      MiniDFSCluster.DataNodeProperties dnprops = cluster.stopDataNode(0);
      stm.sync();
      assertTrue(cluster.restartDataNode(dnprops));
      for (int i = 0; i < 2; i++) {
        cluster.waitForDNHeartbeat(0, 3000);
      }

      AppendTestUtil.write(stm, 0, halfBlock/4);

      LOG.info("======== Checking progress");
      assertFalse(NameNodeAdapter.checkFileProgress(nn.namesystem, "/delayedReceiveBlock", true));
      LOG.info("======== Closing");
      stm.close();

    } finally {
      LOG.info("======== Cleaning up");
      fs1.close();
      cluster.shutdown();
    }
  }

  /**
   * Test that the restart of a DN and the subsequent pipeline recovery do not cause
   * a file to become prematurely considered "complete", when it's a fresh file
   * with no .append() called.
   */
  public void testNotPrematurelyCompleteWithFailureNotReopened() throws Exception {
    LOG.info("START");
    cluster = new MiniDFSCluster(conf, 3, true, null);
    NameNode nn = cluster.getNameNode();
    FileSystem fs1 = cluster.getFileSystem();
    try {
      short rep = 3; // replication

      file1 = new Path("/delayedReceiveBlock");

      stm = fs1.create(file1, true, (int)BLOCK_SIZE*2, rep, 64*1024*1024);
      LOG.info("======== Writing");
      AppendTestUtil.write(stm, 0, 1024*1024);

      LOG.info("======== Waiting for a block allocation");
      waitForBlockReplication(fs1, "/delayedReceiveBlock", 0, 3000);

      LOG.info("======== Checking not complete");
      assertFalse(NameNodeAdapter.checkFileProgress(nn.namesystem, "/delayedReceiveBlock", true));

      // Stop one of the DNs, don't restart
      MiniDFSCluster.DataNodeProperties dnprops = cluster.stopDataNode(0);

      // Write some more data
      AppendTestUtil.write(stm, 0, 1024*1024);

      // Make sure we don't see the file as complete
      LOG.info("======== Checking progress");
      assertFalse(NameNodeAdapter.checkFileProgress(nn.namesystem, "/delayedReceiveBlock", true));
      LOG.info("======== Closing");
      stm.close();

    } finally {
      LOG.info("======== Cleaning up");
      fs1.close();
      cluster.shutdown();
    }
  }

  /**
   * Test case where recovery starts on one node, but it's very slow
   * (delayed right after nextGenerationStamp). A second recovery attempt
   * completes while this one is being slow. Then we should reject the
   * recovery from the first one, since it has a lower gen stamp.
   */
  public void testSimultaneousRecoveries() throws Exception {
    LOG.info("START");
    cluster = new MiniDFSCluster(conf, 3, true, null);
    FileSystem fs1 = cluster.getFileSystem();;
    final FileSystem fs2 = AppendTestUtil.createHdfsWithDifferentUsername(fs1.getConf());
    try {
      createFile(fs1, "/testSimultaneousRecoveries", 3, BBW_SIZE);
      stm.sync();
      loseLeases(fs1);

      // Make the first nextGenerationStamp call get delayed
      DelayAnswer delayer = new DelayAnswer(false);

      NameNode nn = cluster.getNameNode();
      nn.namesystem = spy(nn.namesystem);
      NameNodeAdapter.callNextGenerationStampForBlock(
        doAnswer(delayer).when(nn.namesystem),
        (Block)anyObject(), anyBoolean());

      final AtomicReference<Throwable> err = new AtomicReference<Throwable>();
      Thread recoverThread = new Thread("Recovery thread") {
        public void run() {
          try {
            recoverFile(fs2);
          } catch (Throwable t) {
            err.set(t);
          }
        }
      };
      recoverThread.start();

      LOG.info("Waiting for first nextGenerationStamp to return");
      delayer.waitForCall();

      LOG.info("Allowing recovery time to try again");
      Thread.sleep(10000);

      LOG.info("Proceeding first recovery with old GS");
      delayer.proceed();

      LOG.info("Joining on recovery thread");
      recoverThread.join();

      LOG.info("Waiting a few seconds for blocks to get corrupted");
      Thread.sleep(5000);

      // close() should write recovered bbw to HDFS block
      assertFileSize(fs2, BBW_SIZE); 
      checkFile(fs2, BBW_SIZE);
    } finally {
      fs2.close();
      fs1.close();
      cluster.shutdown();
    }
    LOG.info("STOP");
  }

  
  /**
   * Mockito answer helper that triggers one latch as soon as the
   * method is called, then waits on another before continuing.
   */
  private static class DelayAnswer implements Answer {
    private final CountDownLatch fireLatch = new CountDownLatch(1);
    private final CountDownLatch waitLatch = new CountDownLatch(1);

    boolean delayBefore = true;

    int numTimes = 1;

    public DelayAnswer() {}

    /**
     * @param delayBefore if true, the delay is before the method is called.
     * if false, the delay is after the method returns.
     */
    public DelayAnswer(boolean delayBefore) {
      this.delayBefore = delayBefore;
    }

    /**
     * Wait until the method is called.
     */
    public void waitForCall() throws InterruptedException {
      fireLatch.await();
    }

    /**
     * Tell the method to proceed.
     * This should only be called after waitForCall()
     */
    public void proceed() {
      waitLatch.countDown();
    }

    private void doDelay() throws Throwable {
      synchronized (this) {
        if (--numTimes < 0) return;
      }

      LOG.info("DelayAnswer firing fireLatch");
      fireLatch.countDown();
      try {
        LOG.info("DelayAnswer waiting on waitLatch");
        waitLatch.await();
        LOG.info("DelayAnswer delay complete");
      } catch (InterruptedException ie) {
        throw new IOException("Interrupted waiting on latch", ie);
      }
    }

    public Object answer(InvocationOnMock invocation) throws Throwable {
      if (delayBefore) doDelay();
      Object ret = invocation.callRealMethod();
      if (!delayBefore) doDelay();
      return ret;
    }
  }

  /**
   * Mockito answer helper that will throw an exception a given number
   * of times before eventually succeding.
   */
  private static class ThrowNTimesAnswer implements Answer {
    private int numTimesToThrow;
    private Class<? extends Throwable> exceptionClass;

    public ThrowNTimesAnswer(Class<? extends Throwable> exceptionClass,
                             int numTimesToThrow) {
      this.exceptionClass = exceptionClass;
      this.numTimesToThrow = numTimesToThrow;
    }

    public Object answer(InvocationOnMock invocation) throws Throwable {
      if (numTimesToThrow-- > 0) {
        throw exceptionClass.newInstance();
      }

      return invocation.callRealMethod();
    }
  }

}
