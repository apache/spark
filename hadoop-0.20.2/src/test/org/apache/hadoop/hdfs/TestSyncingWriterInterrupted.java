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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.AppendTestUtil.WriterThread;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSyncingWriterInterrupted {
  static final Log LOG = LogFactory.getLog(TestFileAppend4.class);
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setInt("dfs.client.block.recovery.retries", 1);
    conf.setBoolean("dfs.support.broken.append", true);
  }
  
  @Test(timeout=90000)
  public void testWriterInterrupted() throws Exception {
    short repl = 3;
    int numWrites = 20000;
    
    MiniDFSCluster cluster = new MiniDFSCluster(conf, repl, true, null);
    FileSystem fs1 = cluster.getFileSystem();
    FileSystem fs2 = AppendTestUtil.createHdfsWithDifferentUsername(fs1.getConf());    
    
    Path path = new Path("/testWriterInterrupted");
    FSDataOutputStream stm = fs1.create(path);
    byte[] toWrite = AppendTestUtil.randomBytes(0, 5);
    
    CountDownLatch countdown = new CountDownLatch(1);
    AtomicReference<Throwable> thrown = new AtomicReference<Throwable>();
    WriterThread writerThread = new AppendTestUtil.WriterThread(
        stm, toWrite, thrown, countdown, numWrites);
    writerThread.start();
    countdown.countDown();
    while (writerThread.getNumWritten() == 0 &&
        thrown.get() == null &&
        writerThread.isAlive()) {
      System.err.println("Waiting for writer to start");
      Thread.sleep(10);
    }
    assertTrue(writerThread.isAlive());    
    if (thrown.get() != null) {
      throw new RuntimeException(thrown.get());
    }
    
    AppendTestUtil.loseLeases(fs1);    
    AppendTestUtil.recoverFile(cluster, fs2, path);

    while (thrown.get() == null) {
      LOG.info("Waiting for writer thread to get expected exception");
      Thread.sleep(1000);
    }
    assertNotNull(thrown.get());
    
    // Check that we can see all of the synced edits
    int expectedEdits = writerThread.getNumWritten();
    int gotEdits = (int)(fs2.getFileStatus(path).getLen() / toWrite.length);
    assertTrue("Expected at least " + expectedEdits +
        " edits, got " + gotEdits, gotEdits >= expectedEdits);
    
  }
}
