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

import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.log4j.Level;

/**
 * This class tests the building blocks that are needed to
 * support HDFS appends.
 */
public class TestMultiThreadedSync {
  static final int blockSize = 1024*1024;
  static final int numBlocks = 10;
  static final int fileSize = numBlocks * blockSize + 1;

  private static final int NUM_THREADS = 10;
  private static final int WRITE_SIZE = 517;
  private static final int NUM_WRITES_PER_THREAD = 1000;
  
  private byte[] toWrite = null;

  {
    ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)InterDatanodeProtocol.LOG).getLogger().setLevel(Level.ALL);
  }

  /*
   * creates a file but does not close it
   */ 
  private FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)blockSize);
    return stm;
  }
  
  private void initBuffer(int size) {
    long seed = AppendTestUtil.nextLong();
    toWrite = AppendTestUtil.randomBytes(seed, size);
  }

  @Test
  public void testMultipleSyncers() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    Path p = new Path("/multiple-syncers.dat");
    try {
      doMultithreadedWrites(conf, p, NUM_THREADS, WRITE_SIZE, NUM_WRITES_PER_THREAD);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  public void doMultithreadedWrites(
    Configuration conf, Path p, int numThreads, int bufferSize, int numWrites)
    throws Exception {
    initBuffer(bufferSize);

    // create a new file.
    FileSystem fs = p.getFileSystem(conf);
    FSDataOutputStream stm = createFile(fs, p, 1);
    System.out.println("Created file simpleFlush.dat");

    // TODO move this bit to another test case
    // There have been a couple issues with flushing empty buffers, so do
    // some empty flushes first.
    stm.sync();
    stm.sync();
    stm.write(1);
    stm.sync();
    stm.sync();

    CountDownLatch countdown = new CountDownLatch(1);
    ArrayList<Thread> threads = new ArrayList<Thread>();
    AtomicReference<Throwable> thrown = new AtomicReference<Throwable>();
    for (int i = 0; i < numThreads; i++) {
      Thread t = new AppendTestUtil.WriterThread(stm, toWrite, thrown, countdown, numWrites);
      threads.add(t);
      t.start();
    }

    // Start all the threads at the same time for maximum raciness!
    countdown.countDown();
    
    for (Thread t : threads) {
      t.join();
    }
    if (thrown.get() != null) {
      
      throw new RuntimeException("Deferred", thrown.get());
    }
    stm.close();
    System.out.println("Closed file.");
  }

  public static void main(String args[]) throws Exception {
    TestMultiThreadedSync test = new TestMultiThreadedSync();
    Configuration conf = new Configuration();
    Path p = new Path("/user/todd/test.dat");
    long st = System.nanoTime();
    test.doMultithreadedWrites(conf, p, 10, 511, 50000);
    long et = System.nanoTime();

    System.out.println("Finished in " + ((et - st) / 1000000) + "ms");
  }

}
