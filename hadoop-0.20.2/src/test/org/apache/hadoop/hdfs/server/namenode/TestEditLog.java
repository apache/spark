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

import junit.framework.TestCase;
import java.io.*;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.*;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeFile;

/**
 * This class tests the creation and validation of a checkpoint.
 */
public class TestEditLog extends TestCase {
  static final int numDatanodes = 1;

  // This test creates numThreads threads and each thread does
  // 2 * numberTransactions Transactions concurrently.
  int numberTransactions = 100;
  int numThreads = 100;

  //
  // an object that does a bunch of transactions
  //
  static class Transactions implements Runnable {
    FSEditLog editLog;
    int numTransactions;
    short replication = 3;
    long blockSize = 64;

    Transactions(FSEditLog editlog, int num) {
      editLog = editlog;
      numTransactions = num;
    }

    // add a bunch of transactions.
    public void run() {
      PermissionStatus p = FSNamesystem.getFSNamesystem(
          ).createFsOwnerPermissions(new FsPermission((short)0777));

      for (int i = 0; i < numTransactions; i++) {
        try {
          INodeFileUnderConstruction inode = new INodeFileUnderConstruction(
                              p, replication, blockSize, 0, "", "", null);
          editLog.logOpenFile("/filename" + i, inode);
          editLog.logCloseFile("/filename" + i, inode);
          editLog.logSync();
        } catch (IOException e) {
          System.out.println("Transaction " + i + " encountered exception " +
                             e);
        }
      }
    }
  }

  /**
   * Tests transaction logging in dfs.
   */
  public void testEditLog() throws IOException {

    // start a cluster 

    Collection<File> namedirs = null;
    Collection<File> editsdirs = null;
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(0, conf, numDatanodes, 
                                                true, true, null, null);
    cluster.waitActive();
    FileSystem fileSys = cluster.getFileSystem();
    int numdirs = 0;

    try {
      namedirs = cluster.getNameDirs();
      editsdirs = cluster.getNameEditsDirs();
    } finally {
      fileSys.close();
      cluster.shutdown();
    }

    for (Iterator it = namedirs.iterator(); it.hasNext(); ) {
      File dir = (File)it.next();
      System.out.println(dir);
      numdirs++;
    }

    FSImage fsimage = new FSImage(namedirs, editsdirs);
    FSEditLog editLog = fsimage.getEditLog();

    // set small size of flush buffer
    editLog.setBufferCapacity(2048);
    editLog.close();
    editLog.open();
  
    // Create threads and make them run transactions concurrently.
    Thread threadId[] = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      Transactions trans = new Transactions(editLog, numberTransactions);
      threadId[i] = new Thread(trans, "TransactionThread-" + i);
      threadId[i].start();
    }

    // wait for all transactions to get over
    for (int i = 0; i < numThreads; i++) {
      try {
        threadId[i].join();
      } catch (InterruptedException e) {
        i--;      // retry 
      }
    } 
    
    editLog.close();

    // Verify that we can read in all the transactions that we have written.
    // If there were any corruptions, it is likely that the reading in
    // of these transactions will throw an exception.
    //
    for (Iterator<StorageDirectory> it = 
            fsimage.dirIterator(NameNodeDirType.EDITS); it.hasNext();) {
      File editFile = FSImage.getImageFile(it.next(), NameNodeFile.EDITS);
      System.out.println("Verifying file: " + editFile);
      int numEdits = FSEditLog.loadFSEdits(
          new EditLogFileInputStream(editFile), null);
      int numLeases = FSNamesystem.getFSNamesystem().leaseManager.countLease();
      System.out.println("Number of outstanding leases " + numLeases);
      assertEquals(0, numLeases);
      assertTrue("Verification for " + editFile + " failed. " +
                 "Expected " + (numThreads * 2 * numberTransactions) + " transactions. "+
                 "Found " + numEdits + " transactions.",
                 numEdits == numThreads * 2 * numberTransactions);

    }
  }
}
