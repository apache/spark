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
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.DFSClient.DFSInputStream;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import org.junit.Test;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import static org.junit.Assert.*;

/**
 * Test the use of DFSInputStream by multiple concurrent readers.
 */
public class TestParallelRead {

  static final Log LOG = LogFactory.getLog(TestParallelRead.class);
  static BlockReaderTestUtil util = null;
  static DFSClient dfsClient = null;
  static final int FILE_SIZE_K = 256;
  static Random rand = null;
  
  static {
    // The client-trace log ends up causing a lot of blocking threads
    // in this when it's being used as a performance benchmark.
    LogManager.getLogger(DataNode.class.getName() + ".clienttrace")
      .setLevel(Level.WARN);
  }

  private class TestFileInfo {
    public DFSInputStream dis;
    public Path filepath;
    public byte[] authenticData;
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    final int REPLICATION_FACTOR = 2;
    util = new BlockReaderTestUtil(REPLICATION_FACTOR);
    dfsClient = util.getDFSClient();
    rand = new Random(System.currentTimeMillis());
  }

  /**
   * A worker to do one "unit" of read.
   */
  static class ReadWorker extends Thread {
    static public final int N_ITERATIONS = 1024;

    private static final double PROPORTION_NON_POSITIONAL_READ = 0.10;

    private TestFileInfo testInfo;
    private long fileSize;
    private long bytesRead;
    private boolean error;

    ReadWorker(TestFileInfo testInfo, int id) {
      super("ReadWorker-" + id + "-" + testInfo.filepath.toString());
      this.testInfo = testInfo;
      fileSize = testInfo.dis.getFileLength();
      assertEquals(fileSize, testInfo.authenticData.length);
      bytesRead = 0;
      error = false;
    }

    /**
     * Randomly do one of (1) Small read; and (2) Large Pread.
     */
    @Override
    public void run() {
      for (int i = 0; i < N_ITERATIONS; ++i) {
        int startOff = rand.nextInt((int) fileSize);
        int len = 0;
        try {
          double p = rand.nextDouble();
          if (p < PROPORTION_NON_POSITIONAL_READ) {
            // Do a small regular read. Very likely this will leave unread
            // data on the socket and make the socket uncacheable.
            len = Math.min(rand.nextInt(64), (int) fileSize - startOff);
            read(startOff, len);
            bytesRead += len;
          } else {
            // Do a positional read most of the time.
            len = rand.nextInt((int) (fileSize - startOff));
            pRead(startOff, len);
            bytesRead += len;
          }
        } catch (Throwable t) {
          LOG.error(getName() + ": Error while testing read at " + startOff +
                    " length " + len);
          error = true;
          fail(t.getMessage());
        }
      }
    }

    public long getBytesRead() {
      return bytesRead;
    }

    /**
     * Raising error in a thread doesn't seem to fail the test.
     * So check afterwards.
     */
    public boolean hasError() {
      return error;
    }

    /**
     * Seek to somewhere random and read.
     */
    private void read(int start, int len) throws Exception {
      assertTrue(
          "Bad args: " + start + " + " + len + " should be <= " + fileSize,
          start + len <= fileSize);
      DFSInputStream dis = testInfo.dis;

      synchronized (dis) {
        dis.seek(start);

        byte buf[] = new byte[len];
        int cnt = 0;
        while (cnt < len) {
          cnt += dis.read(buf, cnt, buf.length - cnt);
        }
        verifyData("Read data corrupted", buf, start, start + len);
      }
    }

    /**
     * Positional read.
     */
    private void pRead(int start, int len) throws Exception {
      assertTrue(
          "Bad args: " + start + " + " + len + " should be < " + fileSize,
          start + len < fileSize);
      DFSInputStream dis = testInfo.dis;

      byte buf[] = new byte[len];
      int cnt = 0;
      while (cnt < len) {
        cnt += dis.read(start, buf, cnt, buf.length - cnt);
      }
      verifyData("Pread data corrupted", buf, start, start + len);
    }

    /**
     * Verify read data vs authentic data
     */
    private void verifyData(String msg, byte actual[], int start, int end)
        throws Exception {
      byte auth[] = testInfo.authenticData;
      if (end > auth.length) {
        throw new Exception(msg + ": Actual array (" + end +
                            ") is past the end of authentic data (" +
                            auth.length + ")");
      }

      int j = start;
      for (int i = 0; i < actual.length; ++i, ++j) {
        if (auth[j] != actual[i]) {
          throw new Exception(msg + ": Arrays byte " + i + " (at offset " +
                              j + ") differs: expect " +
                              auth[j] + " got " + actual[i]);
        }
      }
    }
  }

  /**
   * Do parallel read several times with different number of files and threads.
   *
   * Note that while this is the only "test" in a junit sense, we're actually
   * dispatching a lot more. Failures in the other methods (and other threads)
   * need to be manually collected, which is inconvenient.
   */
  @Test
  public void testParallelRead() throws IOException {
    if (!runParallelRead(1, 4)) {
      fail("Check log for errors");
    }
    if (!runParallelRead(1, 16)) {
      fail("Check log for errors");
    }
    if (!runParallelRead(2, 4)) {
      fail("Check log for errors");
    }
  }

  /**
   * Start the parallel read with the given parameters.
   */
  boolean runParallelRead(int nFiles, int nWorkerEach) throws IOException {
    ReadWorker workers[] = new ReadWorker[nFiles * nWorkerEach];
    TestFileInfo testInfoArr[] = new TestFileInfo[nFiles];

    // Prepare the files and workers
    int nWorkers = 0;
    for (int i = 0; i < nFiles; ++i) {
      TestFileInfo testInfo = new TestFileInfo();
      testInfoArr[i] = testInfo;

      testInfo.filepath = new Path("/TestParallelRead.dat." + i);
      testInfo.authenticData = util.writeFile(testInfo.filepath, FILE_SIZE_K);
      testInfo.dis = dfsClient.open(testInfo.filepath.toString());

      for (int j = 0; j < nWorkerEach; ++j) {
        workers[nWorkers++] = new ReadWorker(testInfo, nWorkers);
      }
    }

    // Start the workers and wait
    long starttime = System.currentTimeMillis();
    for (ReadWorker worker : workers) {
      worker.start();
    }

    for (ReadWorker worker : workers) {
      try {
        worker.join();
      } catch (InterruptedException ignored) { }
    }
    long endtime = System.currentTimeMillis();

    // Cleanup
    for (TestFileInfo testInfo : testInfoArr) {
      testInfo.dis.close();
    }

    // Report
    boolean res = true;
    long totalRead = 0;
    for (ReadWorker worker : workers) {
      long nread = worker.getBytesRead();
      LOG.info("--- Report: " + worker.getName() + " read " + nread + " B; " +
               "average " + nread / ReadWorker.N_ITERATIONS + " B per read");
      totalRead += nread;
      if (worker.hasError()) {
        res = false;
      }
    }

    double timeTakenSec = (endtime - starttime) / 1000.0;
    long totalReadKB = totalRead / 1024;
    LOG.info("=== Report: " + nWorkers + " threads read " +
             totalReadKB + " KB (across " +
             nFiles + " file(s)) in " +
             timeTakenSec + "s; average " +
             totalReadKB / timeTakenSec + " KB/s");

    return res;
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    util.shutdown();
  }

}
