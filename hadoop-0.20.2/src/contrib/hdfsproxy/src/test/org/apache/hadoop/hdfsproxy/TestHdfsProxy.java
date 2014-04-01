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

package org.apache.hadoop.hdfsproxy;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * A JUnit test for HdfsProxy
 */
public class TestHdfsProxy extends TestCase {
  {
    ((Log4JLogger) LogFactory.getLog("org.apache.hadoop.hdfs.StateChange"))
        .getLogger().setLevel(Level.OFF);
    ((Log4JLogger) DataNode.LOG).getLogger().setLevel(Level.OFF);
    ((Log4JLogger) FSNamesystem.LOG).getLogger().setLevel(Level.OFF);
  }

  static final URI LOCAL_FS = URI.create("file:///");

  private static final int NFILES = 10;
  private static String TEST_ROOT_DIR = new Path(System.getProperty(
      "test.build.data", "/tmp")).toString().replace(' ', '+');

  /**
   * class MyFile contains enough information to recreate the contents of a
   * single file.
   */
  private static class MyFile {
    private static Random gen = new Random();
    private static final int MAX_LEVELS = 3;
    private static final int MAX_SIZE = 8 * 1024;
    private static String[] dirNames = { "zero", "one", "two", "three", "four",
        "five", "six", "seven", "eight", "nine" };
    private final String name;
    private int size = 0;
    private long seed = 0L;

    MyFile() {
      this(gen.nextInt(MAX_LEVELS));
    }

    MyFile(int nLevels) {
      String xname = "";
      if (nLevels != 0) {
        int[] levels = new int[nLevels];
        for (int idx = 0; idx < nLevels; idx++) {
          levels[idx] = gen.nextInt(10);
        }
        StringBuffer sb = new StringBuffer();
        for (int idx = 0; idx < nLevels; idx++) {
          sb.append(dirNames[levels[idx]]);
          sb.append("/");
        }
        xname = sb.toString();
      }
      long fidx = gen.nextLong() & Long.MAX_VALUE;
      name = xname + Long.toString(fidx);
      reset();
    }

    void reset() {
      final int oldsize = size;
      do {
        size = gen.nextInt(MAX_SIZE);
      } while (oldsize == size);
      final long oldseed = seed;
      do {
        seed = gen.nextLong() & Long.MAX_VALUE;
      } while (oldseed == seed);
    }

    String getName() {
      return name;
    }

    int getSize() {
      return size;
    }

    long getSeed() {
      return seed;
    }
  }

  private static MyFile[] createFiles(URI fsname, String topdir)
      throws IOException {
    return createFiles(FileSystem.get(fsname, new Configuration()), topdir);
  }

  /**
   * create NFILES with random names and directory hierarchies with random (but
   * reproducible) data in them.
   */
  private static MyFile[] createFiles(FileSystem fs, String topdir)
      throws IOException {
    Path root = new Path(topdir);
    MyFile[] files = new MyFile[NFILES];
    for (int i = 0; i < NFILES; i++) {
      files[i] = createFile(root, fs);
    }
    return files;
  }

  private static MyFile createFile(Path root, FileSystem fs, int levels)
      throws IOException {
    MyFile f = levels < 0 ? new MyFile() : new MyFile(levels);
    Path p = new Path(root, f.getName());
    FSDataOutputStream out = fs.create(p);
    byte[] toWrite = new byte[f.getSize()];
    new Random(f.getSeed()).nextBytes(toWrite);
    out.write(toWrite);
    out.close();
    FileSystem.LOG.info("created: " + p + ", size=" + f.getSize());
    return f;
  }

  private static MyFile createFile(Path root, FileSystem fs) throws IOException {
    return createFile(root, fs, -1);
  }

  private static boolean checkFiles(FileSystem fs, String topdir, MyFile[] files)
      throws IOException {
    return checkFiles(fs, topdir, files, false);
  }

  private static boolean checkFiles(FileSystem fs, String topdir,
      MyFile[] files, boolean existingOnly) throws IOException {
    Path root = new Path(topdir);

    for (int idx = 0; idx < files.length; idx++) {
      Path fPath = new Path(root, files[idx].getName());
      try {
        fs.getFileStatus(fPath);
        FSDataInputStream in = fs.open(fPath);
        byte[] toRead = new byte[files[idx].getSize()];
        byte[] toCompare = new byte[files[idx].getSize()];
        Random rb = new Random(files[idx].getSeed());
        rb.nextBytes(toCompare);
        assertEquals("Cannnot read file.", toRead.length, in.read(toRead));
        in.close();
        for (int i = 0; i < toRead.length; i++) {
          if (toRead[i] != toCompare[i]) {
            return false;
          }
        }
        toRead = null;
        toCompare = null;
      } catch (FileNotFoundException fnfe) {
        if (!existingOnly) {
          throw fnfe;
        }
      }
    }

    return true;
  }

  /** delete directory and everything underneath it. */
  private static void deldir(FileSystem fs, String topdir) throws IOException {
    fs.delete(new Path(topdir), true);
  }

  /** verify hdfsproxy implements the hftp interface */
  public void testHdfsProxyInterface() throws Exception {
    MiniDFSCluster cluster = null;
    HdfsProxy proxy = null;
    try {
      final UserGroupInformation CLIENT_UGI = UserGroupInformation.getCurrentUser();
      final String testUser = CLIENT_UGI.getShortUserName();
      final String testGroup = CLIENT_UGI.getGroupNames()[0];

      final Configuration dfsConf = new Configuration();
      dfsConf.set("hadoop.proxyuser." + testUser + ".groups", testGroup);
      dfsConf.set("hadoop.proxyuser." + testGroup + ".hosts",
          "127.0.0.1,localhost");
      dfsConf.set("hadoop.proxyuser." + testUser + ".hosts",
          "127.0.0.1,localhost");
      dfsConf.set("hadoop.security.authentication", "simple");
      cluster = new MiniDFSCluster(dfsConf, 2, true, null);
      cluster.waitActive();

      final FileSystem localfs = FileSystem.get(LOCAL_FS, dfsConf);
      final FileSystem hdfs = cluster.getFileSystem();
      final Configuration proxyConf = new Configuration(false);
      proxyConf.set("hdfsproxy.dfs.namenode.address", hdfs.getUri().getHost() + ":"
          + hdfs.getUri().getPort());
      proxyConf.set("hdfsproxy.https.address", "localhost:0");
      final String namenode = hdfs.getUri().toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(LOCAL_FS, TEST_ROOT_DIR + "/srcdat");
        hdfs.copyFromLocalFile
	    (new Path("file:///" + TEST_ROOT_DIR + "/srcdat"),
             new Path(namenode + "/destdat" ));
        assertTrue("Source and destination directories do not match.",
            checkFiles(hdfs, "/destdat", files));

        proxyConf.set("proxy.http.test.listener.addr", "localhost:0");
        proxy = new HdfsProxy(proxyConf);
        proxy.start();
        InetSocketAddress proxyAddr = NetUtils.createSocketAddr("localhost:0");
        final String realProxyAddr = proxyAddr.getHostName() + ":"
            + proxy.getPort();
        final Path proxyUrl = new Path("hftp://" + realProxyAddr);
	final FileSystem hftp = proxyUrl.getFileSystem(dfsConf);
        FileUtil.copy(hftp, new Path(proxyUrl, "/destdat"),
                      hdfs, new Path(namenode + "/copied1"),
                      false, true, proxyConf);
        
        assertTrue("Source and copied directories do not match.", checkFiles(
            hdfs, "/copied1", files));

        FileUtil.copy(hftp, new Path(proxyUrl, "/destdat"),
                      localfs, new Path(TEST_ROOT_DIR + "/copied2"),
                      false, true, proxyConf);
        assertTrue("Source and copied directories do not match.", checkFiles(
            localfs, TEST_ROOT_DIR + "/copied2", files));

        deldir(hdfs, "/destdat");
        deldir(hdfs, "/logs");
        deldir(hdfs, "/copied1");
        deldir(localfs, TEST_ROOT_DIR + "/srcdat");
        deldir(localfs, TEST_ROOT_DIR + "/copied2");
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      if (proxy != null) {
        proxy.stop();
      }
    }
  }
}
