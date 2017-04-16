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

import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;

import junit.framework.TestCase;

/** A class for testing quota-related commands */
public class TestQuota extends TestCase {
  
  private void runCommand(DFSAdmin admin, boolean expectError, String... args) 
                         throws Exception {
    runCommand(admin, args, expectError);
  }
  
  private void runCommand(DFSAdmin admin, String args[], boolean expectEror)
  throws Exception {
    int val = admin.run(args);
    if (expectEror) {
      assertEquals(val, -1);
    } else {
      assertTrue(val>=0);
    }
  }
  
  /** Test quota related commands: 
   *    setQuota, clrQuota, setSpaceQuota, clrSpaceQuota, and count 
   */
  public void testQuotaCommands() throws Exception {
    final Configuration conf = new Configuration();
    // set a smaller block size so that we can test with smaller 
    // Space quotas
    final int DEFAULT_BLOCK_SIZE = 512;
    conf.setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    conf.setBoolean("dfs.support.broken.append", true);
    final MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    final FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: "+fs.getUri(),
                fs instanceof DistributedFileSystem);
    final DistributedFileSystem dfs = (DistributedFileSystem)fs;
    DFSAdmin admin = new DFSAdmin(conf);
    
    try {
      final int fileLen = 1024;
      final short replication = 5;
      final long spaceQuota = fileLen * replication * 15 / 8;

      // 1: create a directory /test and set its quota to be 3
      final Path parent = new Path("/test");
      assertTrue(dfs.mkdirs(parent));
      String[] args = new String[]{"-setQuota", "3", parent.toString()};
      runCommand(admin, args, false);

      //try setting space quota with a 'binary prefix'
      runCommand(admin, false, "-setSpaceQuota", "2t", parent.toString());
      assertEquals(2L<<40, dfs.getContentSummary(parent).getSpaceQuota());
      
      // set diskspace quota to 10000 
      runCommand(admin, false, "-setSpaceQuota", 
                 Long.toString(spaceQuota), parent.toString());
      
      // 2: create directory /test/data0
      final Path childDir0 = new Path(parent, "data0");
      assertTrue(dfs.mkdirs(childDir0));

      // 3: create a file /test/datafile0
      final Path childFile0 = new Path(parent, "datafile0");
      DFSTestUtil.createFile(fs, childFile0, fileLen, replication, 0);
      
      // 4: count -q /test
      ContentSummary c = dfs.getContentSummary(parent);
      assertEquals(c.getFileCount()+c.getDirectoryCount(), 3);
      assertEquals(c.getQuota(), 3);
      assertEquals(c.getSpaceConsumed(), fileLen*replication);
      assertEquals(c.getSpaceQuota(), spaceQuota);
      
      // 5: count -q /test/data0
      c = dfs.getContentSummary(childDir0);
      assertEquals(c.getFileCount()+c.getDirectoryCount(), 1);
      assertEquals(c.getQuota(), -1);
      // check disk space consumed
      c = dfs.getContentSummary(parent);
      assertEquals(c.getSpaceConsumed(), fileLen*replication);

      // 6: create a directory /test/data1
      final Path childDir1 = new Path(parent, "data1");
      boolean hasException = false;
      try {
        assertFalse(dfs.mkdirs(childDir1));
      } catch (NSQuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);
      
      OutputStream fout;
      
      // 7: create a file /test/datafile1
      final Path childFile1 = new Path(parent, "datafile1");
      hasException = false;
      try {
        fout = dfs.create(childFile1);
      } catch (NSQuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);
      
      // 8: clear quota /test
      runCommand(admin, new String[]{"-clrQuota", parent.toString()}, false);
      c = dfs.getContentSummary(parent);
      assertEquals(c.getQuota(), -1);
      assertEquals(c.getSpaceQuota(), spaceQuota);
      
      // 9: clear quota /test/data0
      runCommand(admin, new String[]{"-clrQuota", childDir0.toString()}, false);
      c = dfs.getContentSummary(childDir0);
      assertEquals(c.getQuota(), -1);
      
      // 10: create a file /test/datafile1
      fout = dfs.create(childFile1, replication);
      
      // 10.s: but writing fileLen bytes should result in an quota exception
      hasException = false;
      try {
        fout.write(new byte[fileLen]);
        fout.close();
      } catch (DSQuotaExceededException e) {
        hasException = true;
        IOUtils.closeStream(fout);
      }
      assertTrue(hasException);
      
      //delete the file
      dfs.delete(childFile1, false);
      
      // 9.s: clear diskspace quota
      runCommand(admin, false, "-clrSpaceQuota", parent.toString());
      c = dfs.getContentSummary(parent);
      assertEquals(c.getQuota(), -1);
      assertEquals(c.getSpaceQuota(), -1);       
      
      // now creating childFile1 should succeed
      DFSTestUtil.createFile(dfs, childFile1, fileLen, replication, 0);
      
      // 11: set the quota of /test to be 1
      // HADOOP-5872 - we can set quota even if it is immediately violated
      args = new String[]{"-setQuota", "1", parent.toString()};
      runCommand(admin, args, false);
      runCommand(admin, false, "-setSpaceQuota",  // for space quota
                 Integer.toString(fileLen), args[2]);
      
      // 12: set the quota of /test/data0 to be 1
      args = new String[]{"-setQuota", "1", childDir0.toString()};
      runCommand(admin, args, false);
      
      // 13: not able create a directory under data0
      hasException = false;
      try {
        assertFalse(dfs.mkdirs(new Path(childDir0, "in")));
      } catch (NSQuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);
      c = dfs.getContentSummary(childDir0);
      assertEquals(c.getDirectoryCount()+c.getFileCount(), 1);
      assertEquals(c.getQuota(), 1);
      
      // 14a: set quota on a non-existent directory
      Path nonExistentPath = new Path("/test1");
      assertFalse(dfs.exists(nonExistentPath));
      args = new String[]{"-setQuota", "1", nonExistentPath.toString()};
      runCommand(admin, args, true);
      runCommand(admin, true, "-setSpaceQuota", "1g", // for space quota
                 nonExistentPath.toString());
      
      // 14b: set quota on a file
      assertTrue(dfs.isFile(childFile0));
      args[1] = childFile0.toString();
      runCommand(admin, args, true);
      // same for space quota
      runCommand(admin, true, "-setSpaceQuota", "1t", args[1]);
      
      // 15a: clear quota on a file
      args[0] = "-clrQuota";
      runCommand(admin, args, true);
      runCommand(admin, true, "-clrSpaceQuota", args[1]);
      
      // 15b: clear quota on a non-existent directory
      args[1] = nonExistentPath.toString();
      runCommand(admin, args, true);
      runCommand(admin, true, "-clrSpaceQuota", args[1]);
      
      // 16a: set the quota of /test to be 0
      args = new String[]{"-setQuota", "0", parent.toString()};
      runCommand(admin, args, true);
      runCommand(admin, true, "-setSpaceQuota", "0", args[2]);
      
      // 16b: set the quota of /test to be -1
      args[1] = "-1";
      runCommand(admin, args, true);
      runCommand(admin, true, "-setSpaceQuota", args[1], args[2]);
      
      // 16c: set the quota of /test to be Long.MAX_VALUE+1
      args[1] = String.valueOf(Long.MAX_VALUE+1L);
      runCommand(admin, args, true);
      runCommand(admin, true, "-setSpaceQuota", args[1], args[2]);
      
      // 16d: set the quota of /test to be a non integer
      args[1] = "33aa1.5";
      runCommand(admin, args, true);
      runCommand(admin, true, "-setSpaceQuota", args[1], args[2]);
      
      // 16e: set space quota with a value larger than Long.MAX_VALUE
      runCommand(admin, true, "-setSpaceQuota", 
                 (Long.MAX_VALUE/1024/1024 + 1024) + "m", args[2]);
      
      // 17:  setQuota by a non-administrator
      final String username = "userxx";
      UserGroupInformation ugi = 
        UserGroupInformation.createUserForTesting(username, 
                                                  new String[]{"groupyy"});
      
      final String[] args2 = args.clone(); // need final ref for doAs block
      ugi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          assertEquals("Not running as new user", username, 
              UserGroupInformation.getCurrentUser().getShortUserName());
          DFSAdmin userAdmin = new DFSAdmin(conf);
          
          args2[1] = "100";
          runCommand(userAdmin, args2, true);
          runCommand(userAdmin, true, "-setSpaceQuota", "1g", args2[2]);
          
          // 18: clrQuota by a non-administrator
          String[] args3 = new String[] {"-clrQuota", parent.toString()};
          runCommand(userAdmin, args3, true);
          runCommand(userAdmin, true, "-clrSpaceQuota",  args3[1]); 

          return null;
        }
      });

      // 19: clrQuota on the root directory ("/") should fail
      runCommand(admin, true, "-clrQuota", "/");

      // 20: setQuota on the root directory ("/") should succeed
      runCommand(admin, false, "-setQuota", "1000000", "/");

      runCommand(admin, true, "-clrQuota", "/");
      runCommand(admin, false, "-clrSpaceQuota", "/");
      runCommand(admin, new String[]{"-clrQuota", parent.toString()}, false);
      runCommand(admin, false, "-clrSpaceQuota", parent.toString());

      // 2: create directory /test/data2
      final Path childDir2 = new Path(parent, "data2");
      assertTrue(dfs.mkdirs(childDir2));

      final Path childFile2 = new Path(childDir2, "datafile2");
      final Path childFile3 = new Path(childDir2, "datafile3");
      final long spaceQuota2 = DEFAULT_BLOCK_SIZE * replication;
      final long fileLen2 = DEFAULT_BLOCK_SIZE;
      // set space quota to a real low value 
      runCommand(admin, false, "-setSpaceQuota", 
                 Long.toString(spaceQuota2), childDir2.toString());
      // clear space quota
      runCommand(admin, false, "-clrSpaceQuota", childDir2.toString());
      // create a file that is greater than the size of space quota
      DFSTestUtil.createFile(fs, childFile2, fileLen2, replication, 0);

      // now set space quota again. This should succeed
      runCommand(admin, false, "-setSpaceQuota", Long.toString(spaceQuota2), 
                 childDir2.toString());

      hasException = false;
      try {
        DFSTestUtil.createFile(fs, childFile3, fileLen2, replication, 0);
      } catch (DSQuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);

      // now test the same for root
      final Path childFile4 = new Path("/", "datafile2");
      final Path childFile5 = new Path("/", "datafile3");

      runCommand(admin, true, "-clrQuota", "/");
      runCommand(admin, false, "-clrSpaceQuota", "/");
      // set space quota to a real low value 
      runCommand(admin, false, "-setSpaceQuota", Long.toString(spaceQuota2), "/");
      runCommand(admin, false, "-clrSpaceQuota", "/");
      DFSTestUtil.createFile(fs, childFile4, fileLen2, replication, 0);
      runCommand(admin, false, "-setSpaceQuota", Long.toString(spaceQuota2), "/");

      hasException = false;
      try {
        DFSTestUtil.createFile(fs, childFile5, fileLen2, replication, 0);
      } catch (DSQuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);

    } finally {
      cluster.shutdown();
    }
  }
  
  /** Test commands that change the size of the name space:
   *  mkdirs, rename, and delete */
  public void testNamespaceCommands() throws Exception {
    final Configuration conf = new Configuration();
    final MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    final FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: "+fs.getUri(),
                fs instanceof DistributedFileSystem);
    final DistributedFileSystem dfs = (DistributedFileSystem)fs;
    
    try {
      // 1: create directory /nqdir0/qdir1/qdir20/nqdir30
      assertTrue(dfs.mkdirs(new Path("/nqdir0/qdir1/qdir20/nqdir30")));

      // 2: set the quota of /nqdir0/qdir1 to be 6
      final Path quotaDir1 = new Path("/nqdir0/qdir1");
      dfs.setQuota(quotaDir1, 6, FSConstants.QUOTA_DONT_SET);
      ContentSummary c = dfs.getContentSummary(quotaDir1);
      assertEquals(c.getDirectoryCount(), 3);
      assertEquals(c.getQuota(), 6);

      // 3: set the quota of /nqdir0/qdir1/qdir20 to be 7
      final Path quotaDir2 = new Path("/nqdir0/qdir1/qdir20");
      dfs.setQuota(quotaDir2, 7, FSConstants.QUOTA_DONT_SET);
      c = dfs.getContentSummary(quotaDir2);
      assertEquals(c.getDirectoryCount(), 2);
      assertEquals(c.getQuota(), 7);

      // 4: Create directory /nqdir0/qdir1/qdir21 and set its quota to 2
      final Path quotaDir3 = new Path("/nqdir0/qdir1/qdir21");
      assertTrue(dfs.mkdirs(quotaDir3));
      dfs.setQuota(quotaDir3, 2, FSConstants.QUOTA_DONT_SET);
      c = dfs.getContentSummary(quotaDir3);
      assertEquals(c.getDirectoryCount(), 1);
      assertEquals(c.getQuota(), 2);

      // 5: Create directory /nqdir0/qdir1/qdir21/nqdir32
      Path tempPath = new Path(quotaDir3, "nqdir32");
      assertTrue(dfs.mkdirs(tempPath));
      c = dfs.getContentSummary(quotaDir3);
      assertEquals(c.getDirectoryCount(), 2);
      assertEquals(c.getQuota(), 2);

      // 6: Create directory /nqdir0/qdir1/qdir21/nqdir33
      tempPath = new Path(quotaDir3, "nqdir33");
      boolean hasException = false;
      try {
        assertFalse(dfs.mkdirs(tempPath));
      } catch (NSQuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);
      c = dfs.getContentSummary(quotaDir3);
      assertEquals(c.getDirectoryCount(), 2);
      assertEquals(c.getQuota(), 2);

      // 7: Create directory /nqdir0/qdir1/qdir20/nqdir31
      tempPath = new Path(quotaDir2, "nqdir31");
      assertTrue(dfs.mkdirs(tempPath));
      c = dfs.getContentSummary(quotaDir2);
      assertEquals(c.getDirectoryCount(), 3);
      assertEquals(c.getQuota(), 7);
      c = dfs.getContentSummary(quotaDir1);
      assertEquals(c.getDirectoryCount(), 6);
      assertEquals(c.getQuota(), 6);

      // 8: Create directory /nqdir0/qdir1/qdir20/nqdir33
      tempPath = new Path(quotaDir2, "nqdir33");
      hasException = false;
      try {
        assertFalse(dfs.mkdirs(tempPath));
      } catch (NSQuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);

      // 9: Move /nqdir0/qdir1/qdir21/nqdir32 /nqdir0/qdir1/qdir20/nqdir30
      tempPath = new Path(quotaDir2, "nqdir30");
      dfs.rename(new Path(quotaDir3, "nqdir32"), tempPath);
      c = dfs.getContentSummary(quotaDir2);
      assertEquals(c.getDirectoryCount(), 4);
      assertEquals(c.getQuota(), 7);
      c = dfs.getContentSummary(quotaDir1);
      assertEquals(c.getDirectoryCount(), 6);
      assertEquals(c.getQuota(), 6);

      // 10: Move /nqdir0/qdir1/qdir20/nqdir30 to /nqdir0/qdir1/qdir21
      hasException = false;
      try {
        assertFalse(dfs.rename(tempPath, quotaDir3));
      } catch (NSQuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);
      assertTrue(dfs.exists(tempPath));
      assertFalse(dfs.exists(new Path(quotaDir3, "nqdir30")));
      
      // 10.a: Rename /nqdir0/qdir1/qdir20/nqdir30 to /nqdir0/qdir1/qdir21/nqdir32
      hasException = false;
      try {
        assertFalse(dfs.rename(tempPath, new Path(quotaDir3, "nqdir32")));
      } catch (NSQuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);
      assertTrue(dfs.exists(tempPath));
      assertFalse(dfs.exists(new Path(quotaDir3, "nqdir32")));

      // 11: Move /nqdir0/qdir1/qdir20/nqdir30 to /nqdir0
      assertTrue(dfs.rename(tempPath, new Path("/nqdir0")));
      c = dfs.getContentSummary(quotaDir2);
      assertEquals(c.getDirectoryCount(), 2);
      assertEquals(c.getQuota(), 7);
      c = dfs.getContentSummary(quotaDir1);
      assertEquals(c.getDirectoryCount(), 4);
      assertEquals(c.getQuota(), 6);

      // 12: Create directory /nqdir0/nqdir30/nqdir33
      assertTrue(dfs.mkdirs(new Path("/nqdir0/nqdir30/nqdir33")));

      // 13: Move /nqdir0/nqdir30 /nqdir0/qdir1/qdir20/qdir30
      hasException = false;
      try {
        assertFalse(dfs.rename(new Path("/nqdir0/nqdir30"), tempPath));
      } catch (NSQuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);

      // 14: Move /nqdir0/qdir1/qdir21 /nqdir0/qdir1/qdir20
      assertTrue(dfs.rename(quotaDir3, quotaDir2));
      c = dfs.getContentSummary(quotaDir1);
      assertEquals(c.getDirectoryCount(), 4);
      assertEquals(c.getQuota(), 6);
      c = dfs.getContentSummary(quotaDir2);
      assertEquals(c.getDirectoryCount(), 3);
      assertEquals(c.getQuota(), 7);
      tempPath = new Path(quotaDir2, "qdir21");
      c = dfs.getContentSummary(tempPath);
      assertEquals(c.getDirectoryCount(), 1);
      assertEquals(c.getQuota(), 2);

      // 15: Delete /nqdir0/qdir1/qdir20/qdir21
      dfs.delete(tempPath, true);
      c = dfs.getContentSummary(quotaDir2);
      assertEquals(c.getDirectoryCount(), 2);
      assertEquals(c.getQuota(), 7);
      c = dfs.getContentSummary(quotaDir1);
      assertEquals(c.getDirectoryCount(), 3);
      assertEquals(c.getQuota(), 6);

      // 16: Move /nqdir0/qdir30 /nqdir0/qdir1/qdir20
      assertTrue(dfs.rename(new Path("/nqdir0/nqdir30"), quotaDir2));
      c = dfs.getContentSummary(quotaDir2);
      assertEquals(c.getDirectoryCount(), 5);
      assertEquals(c.getQuota(), 7);
      c = dfs.getContentSummary(quotaDir1);
      assertEquals(c.getDirectoryCount(), 6);
      assertEquals(c.getQuota(), 6);
    } finally {
      cluster.shutdown();
    }
  }
  
  /**
   * Test HDFS operations that change disk space consumed by a directory tree.
   * namely create, rename, delete, append, and setReplication.
   * 
   * This is based on testNamespaceCommands() above.
   */
  public void testSpaceCommands() throws Exception {
    final Configuration conf = new Configuration();
    // set a smaller block size so that we can test with smaller 
    // diskspace quotas
    conf.set("dfs.block.size", "512");
    conf.setBoolean("dfs.support.broken.append", true);
    final MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    final FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: "+fs.getUri(),
                fs instanceof DistributedFileSystem);
    final DistributedFileSystem dfs = (DistributedFileSystem)fs;

    try {
      int fileLen = 1024;
      short replication = 3;
      int fileSpace = fileLen * replication;
      
      // create directory /nqdir0/qdir1/qdir20/nqdir30
      assertTrue(dfs.mkdirs(new Path("/nqdir0/qdir1/qdir20/nqdir30")));

      // set the quota of /nqdir0/qdir1 to 4 * fileSpace 
      final Path quotaDir1 = new Path("/nqdir0/qdir1");
      dfs.setQuota(quotaDir1, FSConstants.QUOTA_DONT_SET, 4 * fileSpace);
      ContentSummary c = dfs.getContentSummary(quotaDir1);
      assertEquals(c.getSpaceQuota(), 4 * fileSpace);
      
      // set the quota of /nqdir0/qdir1/qdir20 to 6 * fileSpace 
      final Path quotaDir20 = new Path("/nqdir0/qdir1/qdir20");
      dfs.setQuota(quotaDir20, FSConstants.QUOTA_DONT_SET, 6 * fileSpace);
      c = dfs.getContentSummary(quotaDir20);
      assertEquals(c.getSpaceQuota(), 6 * fileSpace);


      // Create /nqdir0/qdir1/qdir21 and set its space quota to 2 * fileSpace
      final Path quotaDir21 = new Path("/nqdir0/qdir1/qdir21");
      assertTrue(dfs.mkdirs(quotaDir21));
      dfs.setQuota(quotaDir21, FSConstants.QUOTA_DONT_SET, 2 * fileSpace);
      c = dfs.getContentSummary(quotaDir21);
      assertEquals(c.getSpaceQuota(), 2 * fileSpace);

      // 5: Create directory /nqdir0/qdir1/qdir21/nqdir32
      Path tempPath = new Path(quotaDir21, "nqdir32");
      assertTrue(dfs.mkdirs(tempPath));
      
      // create a file under nqdir32/fileDir
      DFSTestUtil.createFile(dfs, new Path(tempPath, "fileDir/file1"), fileLen, 
                             replication, 0);
      c = dfs.getContentSummary(quotaDir21);
      assertEquals(c.getSpaceConsumed(), fileSpace);
      
      // Create a larger file /nqdir0/qdir1/qdir21/nqdir33/
      boolean hasException = false;
      try {
        DFSTestUtil.createFile(dfs, new Path(quotaDir21, "nqdir33/file2"), 
                               2*fileLen, replication, 0);
      } catch (DSQuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);
      // delete nqdir33
      assertTrue(dfs.delete(new Path(quotaDir21, "nqdir33"), true));
      c = dfs.getContentSummary(quotaDir21);
      assertEquals(c.getSpaceConsumed(), fileSpace);
      assertEquals(c.getSpaceQuota(), 2*fileSpace);

      // Verify space before the move:
      c = dfs.getContentSummary(quotaDir20);
      assertEquals(c.getSpaceConsumed(), 0);
      
      // Move /nqdir0/qdir1/qdir21/nqdir32 /nqdir0/qdir1/qdir20/nqdir30
      Path dstPath = new Path(quotaDir20, "nqdir30");
      Path srcPath = new Path(quotaDir21, "nqdir32");
      assertTrue(dfs.rename(srcPath, dstPath));
      
      // verify space after the move
      c = dfs.getContentSummary(quotaDir20);
      assertEquals(c.getSpaceConsumed(), fileSpace);
      // verify space for its parent
      c = dfs.getContentSummary(quotaDir1);
      assertEquals(c.getSpaceConsumed(), fileSpace);
      // verify space for source for the move
      c = dfs.getContentSummary(quotaDir21);
      assertEquals(c.getSpaceConsumed(), 0);
      
      final Path file2 = new Path(dstPath, "fileDir/file2");
      int file2Len = 2 * fileLen;
      // create a larger file under /nqdir0/qdir1/qdir20/nqdir30
      DFSTestUtil.createFile(dfs, file2, file2Len, replication, 0);
      
      c = dfs.getContentSummary(quotaDir20);
      assertEquals(c.getSpaceConsumed(), 3 * fileSpace);
      c = dfs.getContentSummary(quotaDir21);
      assertEquals(c.getSpaceConsumed(), 0);
      
      // Reverse: Move /nqdir0/qdir1/qdir20/nqdir30 to /nqdir0/qdir1/qdir21/
      hasException = false;
      try {
        assertFalse(dfs.rename(dstPath, srcPath));
      } catch (DSQuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);
      
      // make sure no intermediate directories left by failed rename
      assertFalse(dfs.exists(srcPath));
      // directory should exist
      assertTrue(dfs.exists(dstPath));
            
      // verify space after the failed move
      c = dfs.getContentSummary(quotaDir20);
      assertEquals(c.getSpaceConsumed(), 3 * fileSpace);
      c = dfs.getContentSummary(quotaDir21);
      assertEquals(c.getSpaceConsumed(), 0);
      
      // Test Append :
      
      // verify space quota
      c = dfs.getContentSummary(quotaDir1);
      assertEquals(c.getSpaceQuota(), 4 * fileSpace);
      
      // verify space before append;
      c = dfs.getContentSummary(dstPath);
      assertEquals(c.getSpaceConsumed(), 3 * fileSpace);
      
      OutputStream out = dfs.append(file2);
      // appending 1 fileLen should succeed
      out.write(new byte[fileLen]);
      out.close();
      
      file2Len += fileLen; // after append
      
      // verify space after append;
      c = dfs.getContentSummary(dstPath);
      assertEquals(c.getSpaceConsumed(), 4 * fileSpace);
      
      // now increase the quota for quotaDir1
      dfs.setQuota(quotaDir1, FSConstants.QUOTA_DONT_SET, 5 * fileSpace);
      // Now, appending more than 1 fileLen should result in an error
      out = dfs.append(file2);
      hasException = false;
      try {
        out.write(new byte[fileLen + 1024]);
        out.flush();
        out.close();
      } catch (DSQuotaExceededException e) {
        hasException = true;
        IOUtils.closeStream(out);
      }
      assertTrue(hasException);
      
      file2Len += fileLen; // after partial append
      
      // verify space after partial append
      c = dfs.getContentSummary(dstPath);
      assertEquals(c.getSpaceConsumed(), 5 * fileSpace);
      
      // Test set replication :
      
      // first reduce the replication
      dfs.setReplication(file2, (short)(replication-1));
      
      // verify that space is reduced by file2Len
      c = dfs.getContentSummary(dstPath);
      assertEquals(c.getSpaceConsumed(), 5 * fileSpace - file2Len);
      
      // now try to increase the replication and and expect an error.
      hasException = false;
      try {
        dfs.setReplication(file2, (short)(replication+1));
      } catch (DSQuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);

      // verify space consumed remains unchanged.
      c = dfs.getContentSummary(dstPath);
      assertEquals(c.getSpaceConsumed(), 5 * fileSpace - file2Len);
      
      // now increase the quota for quotaDir1 and quotaDir20
      dfs.setQuota(quotaDir1, FSConstants.QUOTA_DONT_SET, 10 * fileSpace);
      dfs.setQuota(quotaDir20, FSConstants.QUOTA_DONT_SET, 10 * fileSpace);
      
      // then increasing replication should be ok.
      dfs.setReplication(file2, (short)(replication+1));
      // verify increase in space
      c = dfs.getContentSummary(dstPath);
      assertEquals(c.getSpaceConsumed(), 5 * fileSpace + file2Len);
      
    } finally {
      cluster.shutdown();
    }
  }
}
