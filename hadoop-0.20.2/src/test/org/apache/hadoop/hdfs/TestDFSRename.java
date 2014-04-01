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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;

public class TestDFSRename extends junit.framework.TestCase {
  static Configuration CONF = new Configuration();
  static MiniDFSCluster cluster = null;
  static int countLease(MiniDFSCluster cluster) {
    return cluster.getNameNode().namesystem.leaseManager.countLease();
  }
  
  final Path dir = new Path("/test/rename/");

  @Override
  protected void setUp() throws Exception {
    cluster = new MiniDFSCluster(CONF, 2, true, null);
  }
  
  private void restartCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    cluster = new MiniDFSCluster(CONF, 1, false, null);
    cluster.waitClusterUp();
  }
  
  @Override
  protected void tearDown() throws Exception {
    if (cluster != null) {cluster.shutdown();}
  }
  
  void list(FileSystem fs, String name) throws IOException {
    FileSystem.LOG.info("\n\n" + name);
    for(FileStatus s : fs.listStatus(dir)) {
      FileSystem.LOG.info("" + s.getPath());
    }
  }

  static void createFile(FileSystem fs, Path f) throws IOException {
    DataOutputStream a_out = fs.create(f);
    a_out.writeBytes("something");
    a_out.close();
  }
  
  public void testRename() throws Exception {
    FileSystem fs = cluster.getFileSystem();
    assertTrue(fs.mkdirs(dir));
    
    { //test lease
      Path a = new Path(dir, "a");
      Path aa = new Path(dir, "aa");
      Path b = new Path(dir, "b");

      createFile(fs, a);
      
      //should not have any lease
      assertEquals(0, countLease(cluster)); 

      createFile(fs, aa);
      DataOutputStream aa_out = fs.create(aa);
      aa_out.writeBytes("something");

      //should have 1 lease
      assertEquals(1, countLease(cluster)); 
      list(fs, "rename0");
      fs.rename(a, b);
      list(fs, "rename1");
      aa_out.writeBytes(" more");
      aa_out.close();
      list(fs, "rename2");

      //should not have any lease
      assertEquals(0, countLease(cluster));
    }

    { // test non-existent destination
      Path dstPath = new Path("/c/d");
      assertFalse(fs.exists(dstPath));
      assertFalse(fs.rename(dir, dstPath));
    }

    { // dst cannot be a file or directory under src
      // test rename /a/b/foo to /a/b/c
      Path src = new Path("/a/b");
      Path dst = new Path("/a/b/c");

      createFile(fs, new Path(src, "foo"));
      
      // dst cannot be a file under src
      assertFalse(fs.rename(src, dst)); 
      
      // dst cannot be a directory under src
      assertFalse(fs.rename(src.getParent(), dst.getParent())); 
    }
    
    { // dst can start with src, if it is not a directory or file under src
      // test rename /test /testfile
      Path src = new Path("/testPrefix");
      Path dst = new Path("/testPrefixfile");

      createFile(fs, src);
      assertTrue(fs.rename(src, dst));
    }
    
    { // dst should not be same as src test rename /a/b/c to /a/b/c
      Path src = new Path("/a/b/c");
      createFile(fs, src);
      assertTrue(fs.rename(src, src));
      assertFalse(fs.rename(new Path("/a/b"), new Path("/a/b/")));
      assertTrue(fs.rename(src, new Path("/a/b/c/")));
    }
    
    fs.delete(dir, true);
  }
  
  public void testRenameWithQuota() throws Exception {
    DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();
    Path src1 = new Path(dir, "testRenameWithQuota/srcdir/src1");
    Path src2 = new Path(dir, "testRenameWithQuota/srcdir/src2");
    Path dst1 = new Path(dir, "testRenameWithQuota/dstdir/dst1");
    Path dst2 = new Path(dir, "testRenameWithQuota/dstdir/dst2");
    createFile(fs, src1);
    createFile(fs, src2);
    fs.setQuota(src1.getParent(), FSConstants.QUOTA_DONT_SET,
        FSConstants.QUOTA_DONT_SET);
    fs.mkdirs(dst1.getParent());
    fs.setQuota(dst1.getParent(), FSConstants.QUOTA_DONT_SET,
        FSConstants.QUOTA_DONT_SET);

    // Test1: src does not exceed quota and dst has quota to accommodate rename
    rename(src1, dst1, true, false);

    // Test2: src does not exceed quota and dst has *no* quota to accommodate
    // rename
    fs.setQuota(dst1.getParent(), 1, FSConstants.QUOTA_DONT_SET);
    rename(src2, dst2, false, true);

    // Test3: src exceeds quota and dst has *no* quota to accommodate rename
    fs.setQuota(src1.getParent(), 1, FSConstants.QUOTA_DONT_SET);
    rename(dst1, src1, false, true);
  }
  
  /**
   * Perform operations such as setting quota, deletion of files, rename and
   * ensure system can apply edits log during startup.
   */
  public void testEditsLog() throws Exception {
    DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();
    Path src1 = new Path(dir, "testEditsLog/srcdir/src1");
    Path dst1 = new Path(dir, "testEditsLog/dstdir/dst1");
    createFile(fs, src1);
    fs.mkdirs(dst1.getParent());
    createFile(fs, dst1);
    
    // Set quota so that dst1 parent cannot allow under it new files/directories 
    fs.setQuota(dst1.getParent(), 2, FSConstants.QUOTA_DONT_SET);
    // Free up quota for a subsequent rename
    fs.delete(dst1, true);
    rename(src1, dst1, true, false);
    
    // Restart the cluster and ensure the above operations can be
    // loaded from the edits log
    restartCluster();
    fs = (DistributedFileSystem)cluster.getFileSystem();
    assertFalse(fs.exists(src1));   // ensure src1 is already renamed
    assertTrue(fs.exists(dst1));    // ensure rename dst exists
  }
  
  private void rename(Path src, Path dst, boolean renameSucceeds,
      boolean quotaException) throws Exception {
    DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();
    try {
      assertEquals(renameSucceeds, fs.rename(src, dst));
    } catch (QuotaExceededException ex) {
      assertTrue(quotaException);
    }
    assertEquals(renameSucceeds, !fs.exists(src));
    assertEquals(renameSucceeds, fs.exists(dst));
  }
}
