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

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.atomic.*;

import org.apache.log4j.Level;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;

import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import static org.junit.Assert.*;

/**
 * Basic functional tests on a fuse-dfs mount.
 */
public class TestFuseDFS {

  private static MiniDFSCluster cluster;
  private static FileSystem fs;
  private static Runtime r;
  private static String mountPoint;

  private static final Log LOG = LogFactory.getLog(TestFuseDFS.class);
  {
    ((Log4JLogger)LOG).getLogger().setLevel(Level.ALL);
  }

  /** Dump the given intput stream to stderr */
  private static void dumpInputStream(InputStream is) throws IOException {
    int len;
    do {
      byte b[] = new byte[is.available()];
      len = is.read(b);
      System.out.println("Read "+len+" bytes");
      System.out.write(b, 0, b.length);
    } while (len > 0);
  }

  /** 
   * Wait for the given process to return and check that it exited
   * as required. Log if the process failed.
   */
  private static void checkProcessRet(Process p, boolean expectPass) 
      throws IOException {
    try {
      int ret = p.waitFor();
      if (ret != 0) {
	dumpInputStream(p.getErrorStream());
      }
      if (expectPass) {
	assertEquals(0, ret);
      } else {
	assertTrue(ret != 0);
      }
    } catch (InterruptedException ie) {
      fail("Process interrupted: "+ie.getMessage());
    }
  }

  /** Exec the given command and assert it executed successfully */
  private static void execWaitRet(String cmd) throws IOException {
    LOG.debug("EXEC "+cmd);
    Process p = r.exec(cmd);
    try {
      p.waitFor();
    } catch (InterruptedException ie) {
      fail("Process interrupted: "+ie.getMessage());
    }
  }

  /** Exec the given command and assert it executed successfully */
  private static void execIgnoreRet(String cmd) throws IOException {
    LOG.debug("EXEC "+cmd);
    r.exec(cmd);
  }

  /** Exec the given command and assert it executed successfully */
  private static void execAssertSucceeds(String cmd) throws IOException {
    LOG.debug("EXEC "+cmd);
    checkProcessRet(r.exec(cmd), true);
  }

  /** Exec the given command, assert it returned an error code */
  private static void execAssertFails(String cmd) throws IOException {
    LOG.debug("EXEC "+cmd);
    checkProcessRet(r.exec(cmd), false);
  }

  /** Create and write the given file */
  private static void createFile(File f, String s) throws IOException {
    InputStream is = new ByteArrayInputStream(s.getBytes());
    FileOutputStream fos = new FileOutputStream(f);
    IOUtils.copyBytes(is, fos, s.length(), true);
  }

  /** Check that the given file exists with the given contents */
  private static void checkFile(File f, String expectedContents) 
      throws IOException {
    FileInputStream fi = new FileInputStream(f);
    int len = expectedContents.length();
    byte[] b = new byte[len];
    try {
      IOUtils.readFully(fi, b, 0, len);
    } catch (IOException ie) {
      fail("Reading "+f.getName()+" failed with "+ie.getMessage());
    } finally {
      fi.close(); // NB: leaving f unclosed prevents unmount
    }
    String s = new String(b, 0, len);
    assertEquals("File content differs", expectedContents, s);
  }

  /** Run a fuse-dfs process to mount the given DFS */
  private static void establishMount(URI uri) throws IOException  {
    Runtime r = Runtime.getRuntime();
    String cp = System.getProperty("java.class.path");

    String buildTestDir = System.getProperty("build.test");
    String fuseCmd = buildTestDir + "/../fuse_dfs";
    String libHdfs = buildTestDir + "/../../../c++/lib";

    String arch = System.getProperty("os.arch");
    String jvm = System.getProperty("java.home") + "/lib/" + arch + "/server";
    String lp = System.getProperty("LD_LIBRARY_PATH")+":"+libHdfs+":"+jvm;
    LOG.debug("LD_LIBRARY_PATH=" + lp);

    String nameNode = 
      "dfs://" + uri.getHost() + ":" + String.valueOf(uri.getPort());

    // NB: We're mounting via an unprivileged user, therefore
    // user_allow_other needs to be set in /etc/fuse.conf, which also
    // needs to be world readable.
    String mountCmd[] = {
      fuseCmd, nameNode, mountPoint,
      // "-odebug",              // Don't daemonize
      "-obig_writes",            // Allow >4kb writes
      "-oentry_timeout=0.1",     // Don't cache dents long
      "-oattribute_timeout=0.1", // Don't cache attributes long
      "-ordbuffer=32768",        // Read buffer size in kb
      "rw"
    };

    String [] env = {
      "CLASSPATH="+cp,
      "LD_LIBRARY_PATH="+lp,
      "PATH=/usr/bin:/bin"
    };

    execWaitRet("fusermount -u " + mountPoint);
    execAssertSucceeds("rm -rf " + mountPoint);
    execAssertSucceeds("mkdir -p " + mountPoint);

    // Mount the mini cluster
    try {
      Process fuseProcess = r.exec(mountCmd, env);
      assertEquals(0, fuseProcess.waitFor());
    } catch (InterruptedException ie) {
      fail("Failed to mount");
    }
  }

  /** Tear down the fuse-dfs process and mount */
  private static void teardownMount() throws IOException {
    execWaitRet("fusermount -u " + mountPoint);
  }

  @BeforeClass
  public static void startUp() throws IOException {
    Configuration conf = new Configuration();
    r = Runtime.getRuntime();
    mountPoint = System.getProperty("build.test") + "/mnt";
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, false);
    cluster = new MiniDFSCluster(conf, 1, true, null);
    cluster.waitClusterUp();
    fs = cluster.getFileSystem();
    establishMount(fs.getUri());
  }

  @AfterClass
  public static void tearDown() throws IOException {
    // Unmount before taking down the mini cluster
    // so no outstanding operations hang.
    teardownMount();
    if (fs != null) {
      fs.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /** Test basic directory creation, access, removal */
  @Test
  public void testBasicDir() throws IOException {
    File d = new File(mountPoint, "dir1");

    // Mkdir, access and rm via the mount
    execAssertSucceeds("mkdir " + d.getAbsolutePath());
    execAssertSucceeds("ls " + d.getAbsolutePath());
    execAssertSucceeds("rmdir " + d.getAbsolutePath());

    // The dir should no longer exist
    execAssertFails("ls " + d.getAbsolutePath());
  }

  /** Test basic file creation and writing */
  @Test
  public void testCreate() throws IOException {
    final String contents = "hello world";
    File f = new File(mountPoint, "file1");

    // Create and access via the mount
    createFile(f, contents);

    // XX avoids premature EOF
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ie) { }

    checkFile(f, contents);

    // Cat, stat and delete via the mount
    execAssertSucceeds("cat " + f.getAbsolutePath());
    execAssertSucceeds("stat " + f.getAbsolutePath());
    execAssertSucceeds("rm " + f.getAbsolutePath());

    // The file should no longer exist
    execAssertFails("ls " + f.getAbsolutePath());
  }

  /** Test creating a file via touch */
  @Test
  public void testTouch() throws IOException {
    File f = new File(mountPoint, "file1");
    execAssertSucceeds("touch " + f.getAbsolutePath());
    execAssertSucceeds("rm " + f.getAbsolutePath());
  }

  /** Test random access to a file */
  @Test
  public void testRandomAccess() throws IOException {
    final String contents = "hello world";
    File f = new File(mountPoint, "file1");

    createFile(f, contents);

    RandomAccessFile raf = new RandomAccessFile(f, "rw");
    raf.seek(f.length());
    try {
      raf.write('b');
    } catch (IOException e) {
      // Expected: fuse-dfs not yet support append
      assertEquals("Operation not supported", e.getMessage());
    } finally {
      raf.close();
    }

    raf = new RandomAccessFile(f, "rw");
    raf.seek(0);
    try {
      raf.write('b');
      fail("Over-wrote existing bytes");
    } catch (IOException e) {
      // Expected: can-not overwrite a file
      assertEquals("Invalid argument", e.getMessage());
    } finally {
      raf.close();
    }
    execAssertSucceeds("rm " + f.getAbsolutePath());
  }

  /** Test copying a set of files from the mount to itself */
  @Test
  public void testCopyFiles() throws IOException {
    final String contents = "hello world";
    File d1 = new File(mountPoint, "dir1");
    File d2 = new File(mountPoint, "dir2");

    // Create and populate dir1 via the mount
    execAssertSucceeds("mkdir " + d1.getAbsolutePath());
    for (int i = 0; i < 5; i++) {
      createFile(new File(d1, "file"+i), contents);
    }
    assertEquals(5, d1.listFiles().length);

    // Copy dir from the mount to the mount
    execAssertSucceeds("cp -r " + d1.getAbsolutePath() +
                       " " + d2.getAbsolutePath());
    assertEquals(5, d2.listFiles().length);

    // Access all the files in the dirs and remove them
    execAssertSucceeds("find " + d1.getAbsolutePath());
    execAssertSucceeds("find " + d2.getAbsolutePath());
    execAssertSucceeds("rm -r " + d1.getAbsolutePath());
    execAssertSucceeds("rm -r " + d2.getAbsolutePath());
  }

  /** Test concurrent creation and access of the mount */
  @Test
  public void testMultipleThreads() throws IOException {
    ArrayList<Thread> threads = new ArrayList<Thread>();
    final AtomicReference<String> errorMessage = new AtomicReference<String>();

    for (int i = 0; i < 10; i++) {
      Thread t = new Thread() {
	  public void run() {
	    try {
	      File d = new File(mountPoint, "dir"+getId());
	      execWaitRet("mkdir " + d.getAbsolutePath());
	      for (int j = 0; j < 10; j++) {
		File f = new File(d, "file"+j);
		final String contents = "thread "+getId()+" "+j;
		createFile(f, contents);
	      }
	      for (int j = 0; j < 10; j++) {
		File f = new File(d, "file"+j);
		execWaitRet("cat " + f.getAbsolutePath());
		execWaitRet("rm " + f.getAbsolutePath());
	      }
	      execWaitRet("rmdir " + d.getAbsolutePath());
	    } catch (IOException ie) {
	      errorMessage.set(
		String.format("Exception %s", 
			      StringUtils.stringifyException(ie)));
	    }
          }
	};
      t.start();
      threads.add(t);
    }

    for (Thread t : threads) {
      try {
	t.join();
      } catch (InterruptedException ie) {
	fail("Thread interrupted: "+ie.getMessage());
      }
    }

    assertNull(errorMessage.get(), errorMessage.get());
  }
}
