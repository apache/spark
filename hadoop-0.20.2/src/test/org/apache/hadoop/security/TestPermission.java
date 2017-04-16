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
package org.apache.hadoop.security;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.util.StringUtils;

import junit.framework.TestCase;

/** Unit tests for permission */
public class TestPermission extends TestCase {
  public static final Log LOG = LogFactory.getLog(TestPermission.class);

  final private static Path ROOT_PATH = new Path("/data");
  final private static Path CHILD_DIR1 = new Path(ROOT_PATH, "child1");
  final private static Path CHILD_DIR2 = new Path(ROOT_PATH, "child2");
  final private static Path CHILD_FILE1 = new Path(ROOT_PATH, "file1");
  final private static Path CHILD_FILE2 = new Path(ROOT_PATH, "file2");

  final private static int FILE_LEN = 100;
  final private static Random RAN = new Random();
  final private static String USER_NAME = "user" + RAN.nextInt();
  final private static String[] GROUP_NAMES = {"group1", "group2"};

  static FsPermission checkPermission(FileSystem fs,
      String path, FsPermission expected) throws IOException {
    FileStatus s = fs.getFileStatus(new Path(path));
    LOG.info(s.getPath() + ": " + s.isDir() + " " + s.getPermission()
        + ":" + s.getOwner() + ":" + s.getGroup());
    if (expected != null) {
      assertEquals(expected, s.getPermission());
      assertEquals(expected.toShort(), s.getPermission().toShort());
    }
    return s.getPermission();
  }

  /**
   * Tests backward compatibility. Configuration can be
   * either set with old param dfs.umask that takes decimal umasks
   * or dfs.umaskmode that takes symbolic or octal umask.
   */
  public void testBackwardCompatibility() {
    // Test 1 - old configuration key with decimal 
    // umask value should be handled when set using 
    // FSPermission.setUMask() API
    FsPermission perm = new FsPermission((short)18);
    Configuration conf = new Configuration();
    FsPermission.setUMask(conf, perm);
    assertEquals(18, FsPermission.getUMask(conf).toShort());
    
    // Test 2 - old configuration key set with decimal 
    // umask value should be handled
    perm = new FsPermission((short)18);
    conf = new Configuration();
    conf.set(FsPermission.DEPRECATED_UMASK_LABEL, "18");
    assertEquals(18, FsPermission.getUMask(conf).toShort());
    
    // Test 3 - old configuration key overrides the new one
    conf = new Configuration();
    conf.set(FsPermission.DEPRECATED_UMASK_LABEL, "18");
    conf.set(FsPermission.UMASK_LABEL, "000");
    assertEquals(18, FsPermission.getUMask(conf).toShort());
    
    // Test 4 - new configuration key is handled
    conf = new Configuration();
    conf.set(FsPermission.UMASK_LABEL, "022");
    assertEquals(18, FsPermission.getUMask(conf).toShort());
  }
  
  public void testCreate() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean("dfs.permissions", true);
    conf.set(FsPermission.UMASK_LABEL, "000");
    MiniDFSCluster cluster = null;
    FileSystem fs = null;

    try {
      cluster = new MiniDFSCluster(conf, 3, true, null);
      cluster.waitActive();
      fs = FileSystem.get(conf);
      FsPermission rootPerm = checkPermission(fs, "/", null);
      FsPermission inheritPerm = FsPermission.createImmutable(
          (short)(rootPerm.toShort() | 0300));

      FsPermission dirPerm = new FsPermission((short)0777);
      fs.mkdirs(new Path("/a1/a2/a3"), dirPerm);
      checkPermission(fs, "/a1", inheritPerm);
      checkPermission(fs, "/a1/a2", inheritPerm);
      checkPermission(fs, "/a1/a2/a3", dirPerm);

      FsPermission filePerm = new FsPermission((short)0444);
      FSDataOutputStream out = fs.create(new Path("/b1/b2/b3.txt"), filePerm,
          true, conf.getInt("io.file.buffer.size", 4096),
          fs.getDefaultReplication(), fs.getDefaultBlockSize(), null);
      out.write(123);
      out.close();
      checkPermission(fs, "/b1", inheritPerm);
      checkPermission(fs, "/b1/b2", inheritPerm);
      checkPermission(fs, "/b1/b2/b3.txt", filePerm);
      
      conf.set(FsPermission.UMASK_LABEL, "022");
      FsPermission permission = 
        FsPermission.createImmutable((short)0666);
      FileSystem.mkdirs(fs, new Path("/c1"), new FsPermission(permission));
      FileSystem.create(fs, new Path("/c1/c2.txt"),
          new FsPermission(permission));
      checkPermission(fs, "/c1", permission);
      checkPermission(fs, "/c1/c2.txt", permission);
    } finally {
      try {
        if(fs != null) fs.close();
      } catch(Exception e) {
        LOG.error(StringUtils.stringifyException(e));
      }
      try {
        if(cluster != null) cluster.shutdown();
      } catch(Exception e) {
        LOG.error(StringUtils.stringifyException(e));
      }
    }
  }

  public void testFilePermision() throws Exception {
    final Configuration conf = new Configuration();
    conf.setBoolean("dfs.permissions", true);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 3, true, null);
    cluster.waitActive();

    try {
      FileSystem nnfs = FileSystem.get(conf);
      // test permissions on files that do not exist
      assertFalse(nnfs.exists(CHILD_FILE1));
      try {
        nnfs.setOwner(CHILD_FILE1, "foo", "bar");
        assertTrue(false);
      }
      catch(java.io.FileNotFoundException e) {
        LOG.info("GOOD: got " + e);
      }
      try {
        nnfs.setPermission(CHILD_FILE1, new FsPermission((short)0777));
        assertTrue(false);
      }
      catch(java.io.FileNotFoundException e) {
        LOG.info("GOOD: got " + e);
      }
      // following dir/file creations are legal
      nnfs.mkdirs(CHILD_DIR1);
      FSDataOutputStream out = nnfs.create(CHILD_FILE1);
      byte data[] = new byte[FILE_LEN];
      RAN.nextBytes(data);
      out.write(data);
      out.close();
      nnfs.setPermission(CHILD_FILE1, new FsPermission("700"));

      // following read is legal
      byte dataIn[] = new byte[FILE_LEN];
      FSDataInputStream fin = nnfs.open(CHILD_FILE1);
      int bytesRead = fin.read(dataIn);
      assertTrue(bytesRead == FILE_LEN);
      for(int i=0; i<FILE_LEN; i++) {
        assertEquals(data[i], dataIn[i]);
      }

      ////////////////////////////////////////////////////////////////
      // test illegal file/dir creation
      UserGroupInformation userGroupInfo = 
        UserGroupInformation.createUserForTesting(USER_NAME, GROUP_NAMES );
      
      FileSystem userfs = DFSTestUtil.getFileSystemAs(userGroupInfo, conf);

      // make sure mkdir of a existing directory that is not owned by 
      // this user does not throw an exception.
      userfs.mkdirs(CHILD_DIR1);
      
      // illegal mkdir
      assertTrue(!canMkdirs(userfs, CHILD_DIR2));

      // illegal file creation
      assertTrue(!canCreate(userfs, CHILD_FILE2));

      // illegal file open
      assertTrue(!canOpen(userfs, CHILD_FILE1));

      nnfs.setPermission(ROOT_PATH, new FsPermission((short)0755));
      nnfs.setPermission(CHILD_DIR1, new FsPermission("777"));
      nnfs.setPermission(new Path("/"), new FsPermission((short)0777));
      final Path RENAME_PATH = new Path("/foo/bar");
      userfs.mkdirs(RENAME_PATH);
      assertTrue(canRename(userfs, RENAME_PATH, CHILD_DIR1));
    } finally {
      if(cluster != null) cluster.shutdown();
    }
  }

  static boolean canMkdirs(FileSystem fs, Path p) throws IOException {
    try {
      fs.mkdirs(p);
      return true;
    } catch(AccessControlException e) {
      return false;
    }
  }

  static boolean canCreate(FileSystem fs, Path p) throws IOException {
    try {
      fs.create(p);
      return true;
    } catch(AccessControlException e) {
      return false;
    }
  }

  static boolean canOpen(FileSystem fs, Path p) throws IOException {
    try {
      fs.open(p);
      return true;
    } catch(AccessControlException e) {
      return false;
    }
  }

  static boolean canRename(FileSystem fs, Path src, Path dst
      ) throws IOException {
    try {
      fs.rename(src, dst);
      return true;
    } catch(AccessControlException e) {
      return false;
    }
  }
}
