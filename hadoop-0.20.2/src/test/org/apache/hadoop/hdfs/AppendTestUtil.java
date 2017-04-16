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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.security.UserGroupInformation;

/** Utilities for append-related tests */ 
public class AppendTestUtil {
  /** For specifying the random number generator seed,
   *  change the following value:
   */
  static final Long RANDOM_NUMBER_GENERATOR_SEED = null;

  static final Log LOG = LogFactory.getLog(AppendTestUtil.class);

  private static final Random SEED = new Random();
  static {
    final long seed = RANDOM_NUMBER_GENERATOR_SEED == null?
        SEED.nextLong(): RANDOM_NUMBER_GENERATOR_SEED;
    LOG.info("seed=" + seed);
    SEED.setSeed(seed);
  }

  private static final ThreadLocal<Random> RANDOM = new ThreadLocal<Random>() {
    protected Random initialValue() {
      final Random r =  new Random();
      synchronized(SEED) { 
        final long seed = SEED.nextLong();
        r.setSeed(seed);
        LOG.info(Thread.currentThread().getName() + ": seed=" + seed);
      }
      return r;
    }
  };
  
  static int nextInt() {return RANDOM.get().nextInt();}
  static int nextInt(int n) {return RANDOM.get().nextInt(n);}
  static int nextLong() {return RANDOM.get().nextInt();}

  static byte[] randomBytes(long seed, int size) {
    LOG.info("seed=" + seed + ", size=" + size);
    final byte[] b = new byte[size];
    final Random rand = new Random(seed);
    rand.nextBytes(b);
    return b;
  }

  static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      LOG.info("ms=" + ms, e);
    }
  }

  public static FileSystem createHdfsWithDifferentUsername(final Configuration conf
      ) throws IOException, InterruptedException {
    String username = UserGroupInformation.getCurrentUser().getShortUserName()+"_XXX";
    UserGroupInformation ugi = 
      UserGroupInformation.createUserForTesting(username, new String[]{"supergroup"});
    
    return DFSTestUtil.getFileSystemAs(ugi, conf);
  }

  public static void write(OutputStream out, int offset, int length) throws IOException {
    final byte[] bytes = new byte[length];
    for(int i = 0; i < length; i++) {
      bytes[i] = (byte)(offset + i);
    }
    out.write(bytes);
  }
  
  public static void check(FileSystem fs, Path p, long length) throws IOException {
    int i = -1;
    try {
      final FileStatus status = fs.getFileStatus(p);
      TestCase.assertEquals(length, status.getLen());
      InputStream in = fs.open(p);
      for(i++; i < length; i++) {
        TestCase.assertEquals((byte)i, (byte)in.read());  
      }
      i = -(int)length;
      TestCase.assertEquals(-1, in.read()); //EOF  
      in.close();
    } catch(IOException ioe) {
      throw new IOException("p=" + p + ", length=" + length + ", i=" + i, ioe);
    }
  }
  
  static class WriterThread extends Thread {
    private final FSDataOutputStream stm;
    private final AtomicReference<Throwable> thrown;
    private final int numWrites;
    private final CountDownLatch countdown;
    private final byte[] toWrite;
    private AtomicInteger numWritten = new AtomicInteger();
    
    public WriterThread(FSDataOutputStream stm,
        byte[] toWrite,
        AtomicReference<Throwable> thrown,
        CountDownLatch countdown, int numWrites) {
      this.toWrite = toWrite;
      this.stm = stm;
      this.thrown = thrown;
      this.numWrites = numWrites;
      this.countdown = countdown;
    }
  
    public void run() {
      try {
        countdown.await();
        for (int i = 0; i < numWrites && thrown.get() == null; i++) {
          doAWrite();
          numWritten.getAndIncrement();
        }
      } catch (Throwable t) {
        thrown.compareAndSet(null, t);
      }
    }
  
    private void doAWrite() throws IOException {
      stm.write(toWrite);
      stm.sync();
    }
    
    public int getNumWritten() {
      return numWritten.get();
    }
  }

  public static void loseLeases(FileSystem whichfs) throws Exception {
    LOG.info("leasechecker.interruptAndJoin()");
    // lose the lease on the client
    DistributedFileSystem dfs = (DistributedFileSystem)whichfs;
    dfs.dfs.leasechecker.interruptAndJoin();
  }
  public static void recoverFile(MiniDFSCluster cluster, FileSystem fs,
      Path file1) throws IOException {
   
    int tries = 90;
    if (cluster != null) {
      // set the soft limit to be 1 second so that the
      // namenode triggers lease recovery upon append request
      cluster.setLeasePeriod(1000, FSConstants.LEASE_HARDLIMIT_PERIOD);
      tries = 40;
    }

    // Trying recovery
    boolean recovered = false;
    FSDataOutputStream out = null;
    while (!recovered && tries-- > 0) {
      try {
        out = fs.append(file1);
        LOG.info("Successfully opened for appends");
        recovered = true;
      } catch (IOException e) {
        if (!e.getMessage().contains("being recovered") &&
            !e.getMessage().contains("being created")) {
          throw e;
        }
        LOG.info("Failed open for append, waiting on lease recovery");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ex) {
          // ignore it and try again
        }
      }
    }
    if (out != null) {
      out.close();
    }
    if (!recovered) {
      throw new RuntimeException("Recovery failed");
    }    
  }  
}
