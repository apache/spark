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

import java.util.concurrent.atomic.AtomicReference;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.PrintStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Manual test that runs against a cluster, spawning a child which
 * writes and syncs as fast as it can, and then kill -9s the
 * child, verifying that all of the edits made it.
 */
public class ManualSyncTester extends Configured implements Tool {
  public static Log LOG = LogFactory.getLog(ManualSyncTester.class);

  public static final byte[] THING_TO_WRITE = "hello\n".getBytes();

  public static class ProgressRecorder {
    private RandomAccessFile raf;
    public ProgressRecorder(String path) throws IOException{
      raf = new RandomAccessFile(path, "rws");
    }

    public void recordIteration(long iteration) throws IOException {
      raf.seek(0);
      raf.writeLong(iteration);
    }

    public long readIteration() throws IOException {
      raf.seek(0);
      return raf.readLong();
    }

    public void close() throws IOException {
      if (raf != null) {
        raf.close();
      }
    }

    public void finalize() {
      try {
        close();
      } catch (Exception e) {}
    }
  }

  public static class Child extends Configured implements Tool {

    AtomicReference<Throwable> err = new AtomicReference<Throwable>();
    ProgressRecorder recorder;
    volatile long curIteration = 0;

    class Syncer extends Thread {
      private final FSDataOutputStream stm;
      private volatile boolean done;
  
      Syncer(FSDataOutputStream stm) {
        this.stm = stm;
      }
      
      public void run() {
        try {
          while (!done) {
            long iter = curIteration;
            stm.sync();
            recorder.recordIteration(iter);
          }
        } catch (Throwable e) {
          err.set(e);
        }
      }
      
      public void stopSyncing() {
        done = true;
      }
    }
    
    @Override
    public int run(String[] args) throws Exception {
      Path path = new Path(args[0]);
      String progressPath = args[1];
      recorder = new ProgressRecorder(progressPath);
      
      FileSystem fs = path.getFileSystem(getConf());
      LOG.info("Writing to fs: " + fs);
      
      while (true) {
        FSDataOutputStream stm = fs.create(path, true);
  
        Syncer syncer = new Syncer(stm);
        syncer.start();
  
        byte b[] = THING_TO_WRITE;
        long start = System.currentTimeMillis();
        long lastPrint = System.currentTimeMillis();
        while (syncer.isAlive()) {
          stm.write(b);
          curIteration++;
          if (System.currentTimeMillis() - lastPrint > 1000) {
            LOG.info("Written " + curIteration + " writes");
            lastPrint = System.currentTimeMillis();
          }
        }
        if (err.get() != null)
          throw new RuntimeException("Failed", err.get());
  
        syncer.stopSyncing();
        syncer.join();
        stm.close();
  
        if (err.get() != null)
          throw new RuntimeException("Failed", err.get());
      }
    }
  
    public static void main(String[] args) throws Exception {
      System.exit(ToolRunner.run(new Child(), args));
    }
  }

  private Process runChild(String dataPath, String progressPath)
      throws Exception {
    Runtime rt = Runtime.getRuntime();
    Process p = rt.exec(new String[] {
      "java",
      "-cp",
      System.getProperty("java.class.path"),
      "org.apache.hadoop.hdfs.ManualSyncTester$Child",
      dataPath, progressPath
    });
    new Pumper(p.getInputStream(), System.out).start();
    new Pumper(p.getErrorStream(), System.err).start();
    return p;
  }

  public static class Pumper extends Thread {
    InputStream is;
    PrintStream os;

    public Pumper(InputStream is, PrintStream os) {
      this.is = is;
      this.os = os;
    }

    @Override
    public void run() {
      try {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String s;
        while ((s = reader.readLine()) != null) {
          os.println(s);
        }
      } catch (IOException ioe) {
      }
    }
  }

  @Override
  public int run(String args[]) throws Exception {
    if (args.length != 2) {
      throw new RuntimeException(
        "usage: ManualSyncTester data-path /dev/shm/progress-path");
    }
    String dataPathStr = args[0];
    Path dataPath = new Path(dataPathStr);
    String progressPath = args[1];
    Process proc = runChild(dataPathStr, progressPath);
    LOG.info("Process started, letting it run for a bit...");
    Thread.sleep(15000);
    LOG.info("Destroying process...");
    killUnixProcess(proc, 9);
    int ret = proc.waitFor();
    if (ret != 137) {
      throw new RuntimeException("child not killed, rc=" + ret + "!");
    }
    FileSystem fs = FileSystem.get(getConf());
    LOG.info("Recovering file...");
    AppendTestUtil.recoverFile(null, fs, dataPath);
    LOG.info("Recovered file...");

    ProgressRecorder recorder = new ProgressRecorder(progressPath);
    long expected = recorder.readIteration();
    recorder.close();
    verifyData(dataPath, expected);

    return 0;
  }

  private void verifyData(Path f, long expected) throws Exception {
    InputStream is = f.getFileSystem(getConf()).open(f);
    byte[] buf = new byte[THING_TO_WRITE.length];
    for (int i = 0; i < expected; i++) {
      IOUtils.readFully(is, buf, 0, buf.length);
      if (WritableComparator.compareBytes(THING_TO_WRITE, 0, THING_TO_WRITE.length,
                                          buf, 0, buf.length) != 0) {
        throw new RuntimeException("Error at iteration " + i);
      }
    }
    LOG.info("Verified " + expected + " iterations");
  }

  public static int getUnixPID(Process process) throws Exception
  {
    if (process.getClass().getName().equals("java.lang.UNIXProcess"))
    {
      Class cl = process.getClass();
      Field field = cl.getDeclaredField("pid");
      field.setAccessible(true);
      Object pidObject = field.get(process);
      return (Integer) pidObject;
    } else
    {
      throw new IllegalArgumentException("Needs to be a UNIXProcess");
    }
  }
  
  public static int killUnixProcess(Process process, int signal) throws Exception
  {
      int pid = getUnixPID(process);
      return Runtime.getRuntime().exec("kill -" + signal + " " + pid).waitFor();
  }
  public static void main(String args[]) throws Exception {
    int rc = 0;
    while (rc == 0) {
      rc = ToolRunner.run(new ManualSyncTester(), args);
    }
    System.exit(rc);
  }
}
