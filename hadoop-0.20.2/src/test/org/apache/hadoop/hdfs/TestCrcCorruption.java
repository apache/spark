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

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.util.Random;
import junit.framework.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * A JUnit test for corrupted file handling.
 * This test creates a bunch of files/directories with replication 
 * factor of 2. Then verifies that a client can automatically 
 * access the remaining valid replica inspite of the following 
 * types of simulated errors:
 *
 *  1. Delete meta file on one replica
 *  2. Truncates meta file on one replica
 *  3. Corrupts the meta file header on one replica
 *  4. Corrupts any random offset and portion of the meta file
 *  5. Swaps two meta files, i.e the format of the meta files 
 *     are valid but their CRCs do not match with their corresponding 
 *     data blocks
 * The above tests are run for varied values of io.bytes.per.checksum 
 * and dfs.block.size. It tests for the case when the meta file is 
 * multiple blocks.
 *
 * Another portion of the test is commented out till HADOOP-1557 
 * is addressed:
 *  1. Create file with 2 replica, corrupt the meta file of replica, 
 *     decrease replication factor from 2 to 1. Validate that the 
 *     remaining replica is the good one.
 *  2. Create file with 2 replica, corrupt the meta file of one replica, 
 *     increase replication factor of file to 3. verify that the new 
 *     replica was created from the non-corrupted replica.
 */
public class TestCrcCorruption extends TestCase {

  /** 
   * check if DFS can handle corrupted CRC blocks
   */
  private void thistest(Configuration conf, DFSTestUtil util) throws Exception {
    MiniDFSCluster cluster = null;
    int numDataNodes = 2;
    short replFactor = 2;
    Random random = new Random();

    try {
      cluster = new MiniDFSCluster(conf, numDataNodes, true, null);
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      util.createFiles(fs, "/srcdat", replFactor);
      util.waitReplication(fs, "/srcdat", (short)2);

      // Now deliberately remove/truncate meta blocks from the first
      // directory of the first datanode. The complete absense of a meta
      // file disallows this Datanode to send data to another datanode.
      // However, a client is alowed access to this block.
      //
      File data_dir = new File(System.getProperty("test.build.data"),
                               "dfs/data/data1/current");
      assertTrue("data directory does not exist", data_dir.exists());
      File[] blocks = data_dir.listFiles();
      assertTrue("Blocks do not exist in data-dir", (blocks != null) && (blocks.length > 0));
      int num = 0;
      for (int idx = 0; idx < blocks.length; idx++) {
        if (blocks[idx].getName().startsWith("blk_") &&
            blocks[idx].getName().endsWith(".meta")) {
          num++;
          if (num % 3 == 0) {
            //
            // remove .meta file
            //
            System.out.println("Deliberately removing file " + blocks[idx].getName());
            assertTrue("Cannot remove file.", blocks[idx].delete());
          } else if (num % 3 == 1) {
            //
            // shorten .meta file
            //
            RandomAccessFile file = new RandomAccessFile(blocks[idx], "rw");
            FileChannel channel = file.getChannel();
            int newsize = random.nextInt((int)channel.size()/2);
            System.out.println("Deliberately truncating file " + 
                               blocks[idx].getName() + 
                               " to size " + newsize + " bytes.");
            channel.truncate(newsize);
            file.close();
          } else {
            //
            // corrupt a few bytes of the metafile
            //
            RandomAccessFile file = new RandomAccessFile(blocks[idx], "rw");
            FileChannel channel = file.getChannel();
            long position = 0;
            //
            // The very first time, corrupt the meta header at offset 0
            //
            if (num != 2) {
              position = (long)random.nextInt((int)channel.size());
            }
            int length = random.nextInt((int)(channel.size() - position + 1));
            byte[] buffer = new byte[length];
            random.nextBytes(buffer);
            channel.write(ByteBuffer.wrap(buffer), position);
            System.out.println("Deliberately corrupting file " + 
                               blocks[idx].getName() + 
                               " at offset " + position +
                               " length " + length);
            file.close();
          }
        }
      }
      //
      // Now deliberately corrupt all meta blocks from the second
      // directory of the first datanode
      //
      data_dir = new File(System.getProperty("test.build.data"),
                               "dfs/data/data2/current");
      assertTrue("data directory does not exist", data_dir.exists());
      blocks = data_dir.listFiles();
      assertTrue("Blocks do not exist in data-dir", (blocks != null) && (blocks.length > 0));

      int count = 0;
      File previous = null;
      for (int idx = 0; idx < blocks.length; idx++) {
        if (blocks[idx].getName().startsWith("blk_") &&
            blocks[idx].getName().endsWith(".meta")) {
          //
          // Move the previous metafile into the current one.
          //
          count++;
          if (count % 2 == 0) {
            System.out.println("Deliberately insertimg bad crc into files " +
                                blocks[idx].getName() + " " + previous.getName());
            assertTrue("Cannot remove file.", blocks[idx].delete());
            assertTrue("Cannot corrupt meta file.", previous.renameTo(blocks[idx]));
            assertTrue("Cannot recreate empty meta file.", previous.createNewFile());
            previous = null;
          } else {
            previous = blocks[idx];
          }
        }
      }

      //
      // Only one replica is possibly corrupted. The other replica should still
      // be good. Verify.
      //
      assertTrue("Corrupted replicas not handled properly.",
                 util.checkFiles(fs, "/srcdat"));
      System.out.println("All File still have a valid replica");

      //
      // set replication factor back to 1. This causes only one replica of
      // of each block to remain in HDFS. The check is to make sure that 
      // the corrupted replica generated above is the one that gets deleted.
      // This test is currently disabled until HADOOP-1557 is solved.
      //
      util.setReplication(fs, "/srcdat", (short)1);
      //util.waitReplication(fs, "/srcdat", (short)1);
      //System.out.println("All Files done with removing replicas");
      //assertTrue("Excess replicas deleted. Corrupted replicas found.",
      //           util.checkFiles(fs, "/srcdat"));
      System.out.println("The excess-corrupted-replica test is disabled " +
                         " pending HADOOP-1557");

      util.cleanup(fs, "/srcdat");
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  public void testCrcCorruption() throws Exception {
    //
    // default parameters
    //
    System.out.println("TestCrcCorruption with default parameters");
    Configuration conf1 = new Configuration();
    conf1.setInt("dfs.blockreport.intervalMsec", 3 * 1000);
    DFSTestUtil util1 = new DFSTestUtil("TestCrcCorruption", 40, 3, 8*1024);
    thistest(conf1, util1);

    //
    // specific parameters
    //
    System.out.println("TestCrcCorruption with specific parameters");
    Configuration conf2 = new Configuration();
    conf2.setInt("io.bytes.per.checksum", 17);
    conf2.setInt("dfs.block.size", 34);
    DFSTestUtil util2 = new DFSTestUtil("TestCrcCorruption", 40, 3, 400);
    thistest(conf2, util2);
  }

  /**
   * Make a single-DN cluster, corrupt a block, and make sure
   * there's no infinite loop, but rather it eventually
   * reports the exception to the client.
   */
  public void testEntirelyCorruptFileOneNode() throws Exception {
    doTestEntirelyCorruptFile(1);
  }

  /**
   * Same thing with multiple datanodes - in history, this has
   * behaved differently than the above.
   *
   * This test usually completes in around 15 seconds - if it
   * times out, this suggests that the client is retrying
   * indefinitely.
   */
  public void testEntirelyCorruptFileThreeNodes() throws Exception {
    doTestEntirelyCorruptFile(3);
  }

  private void doTestEntirelyCorruptFile(int numDataNodes) throws Exception {
    long fileSize = 4096;
    Path file = new Path("/testFile");

    Configuration conf = new Configuration();
    conf.setInt("dfs.replication", numDataNodes);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDataNodes, true, null);

    try {
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();

      DFSTestUtil.createFile(fs, file, fileSize, (short)numDataNodes, 12345L /*seed*/);
      DFSTestUtil.waitReplication(fs, file, (short)numDataNodes);

      String block = DFSTestUtil.getFirstBlock(fs, file).getBlockName();
      cluster.corruptBlockOnDataNodes(block);

      try {
        IOUtils.copyBytes(fs.open(file), new IOUtils.NullOutputStream(), conf,
                          true);
        fail("Didn't get exception");
      } catch (IOException ioe) {
        DFSClient.LOG.info("Got expected exception", ioe);
      }

    } finally {
      cluster.shutdown();
    }
    
  }
}
