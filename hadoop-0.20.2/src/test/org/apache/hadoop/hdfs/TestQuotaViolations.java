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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.tools.DFSAdmin;

import org.apache.log4j.Level;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/** Coverage for quota space violations */
public class TestQuotaViolations {

  private static final Log LOG = LogFactory.getLog(TestQuotaViolations.class);
  {
    ((Log4JLogger) TestQuotaViolations.LOG).getLogger().setLevel(Level.ALL);
  }

  private FileSystem fs;
  private MiniDFSCluster cluster;
  private Configuration conf;
  private DFSAdmin admin;
  private static int BLOCK_SIZE = 6 * 1024;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.set("dfs.block.size", Integer.toString(BLOCK_SIZE));
    cluster = new MiniDFSCluster(conf, 3, true, null);
    cluster.waitActive();
    fs = cluster.getFileSystem();
    admin = new DFSAdmin(conf);
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  private void runCommand(DFSAdmin admin, boolean expectError, String... args)
      throws Exception {
    runCommand(admin, args, expectError);
  }

  private void runCommand(DFSAdmin admin, String args[], boolean expectEror)
      throws Exception {
    int ret = admin.run(args);
    if (expectEror) {
      assertEquals(ret, -1);
    } else {
      assertTrue(ret >= 0);
    }
  }

  /**
   * Violate a space quota using files of size < 1 block. Test that block
   * allocation conservatively assumes that for quota checking the entire space
   * of the block is used.
   */
  @Test
  public void testBlockAllocationAdjustUsageConservatively() throws Exception {
    Path dir = new Path("/test");
    Path file1 = new Path("/test/test1");
    Path file2 = new Path("/test/test2");
    boolean exceededQuota = false;
    final int QUOTA_SIZE = 3 * BLOCK_SIZE; // total space usage including repl.
    final int FILE_SIZE = BLOCK_SIZE / 2; 
    ContentSummary c;

    // Create the directory and set the quota
    assertTrue(fs.mkdirs(dir));
    runCommand(admin, false, "-setSpaceQuota", Integer.toString(QUOTA_SIZE),
        dir.toString());
    
    // Creating one file should use half the quota
    DFSTestUtil.createFile(fs, file1, FILE_SIZE, (short)3, 1L);
    DFSTestUtil.waitReplication(fs, file1, (short)3);
    c = fs.getContentSummary(dir);
    assertEquals("Quota is half consumed", QUOTA_SIZE / 2,
        c.getSpaceConsumed());

    // We can not create the 2nd file because even though the total spaced used
    // by two files (2 * 3 * 512/2) would fit within the quota (3 * 512) when a
    // block for a file is created the space used is adjusted conservatively (3
    // * block size, ie assumes a full block is written) which will violate the
    // quota (3 * block size) since we've already used half the quota for the
    // first file.
    try {
      DFSTestUtil.createFile(fs, file2, FILE_SIZE, (short)3, 1L);
    } catch (QuotaExceededException e) {
      exceededQuota = true;
    }
    assertTrue("Quota not exceeded", exceededQuota);
  }

  /**
   * Like the previous test but create many files. This covers bugs
   * where the quota adjustment is incorrect but it takes many files
   * to accrue a big enough accounting error to violate the quota.
   */
  @Test
  public void testMultipleFilesSmallerThanOneBlock() throws Exception {
    Path dir = new Path("/test");
    boolean exceededQuota = false;
    ContentSummary c;
    //   1kb file
    //   6kb block
    // 192kb quota
    final int FILE_SIZE = 1024; 
    final int QUOTA_SIZE = 32 * (int)fs.getDefaultBlockSize();
    assertEquals(6 * 1024, fs.getDefaultBlockSize());
    assertEquals(192 * 1024, QUOTA_SIZE);

    // Create the dir and set the quota. We need to enable the quota before
    // writing the files as setting the quota afterwards will over-write 
    // the cached disk space used for quota verification with the actual
    // amount used as calculated by INode#spaceConsumedInTree.
    assertTrue(fs.mkdirs(dir));
    runCommand(admin, false, "-setSpaceQuota", Integer.toString(QUOTA_SIZE),
        dir.toString());

    // We can create at most 59 files because block allocation is
    // conservative and initially assumes a full block is used, so we
    // need to leave at least 3 * BLOCK_SIZE free space when allocating 
    // the last block: (58 * 3 * 1024) + (3 * 6 * 1024) = 192kb
    for (int i = 0; i < 59; i++) {
      Path file = new Path("/test/test" + i);
      DFSTestUtil.createFile(fs, file, FILE_SIZE, (short)3, 1L);
      DFSTestUtil.waitReplication(fs, file, (short)3);
    }

    // Should account for all 59 files (almost QUOTA_SIZE)
    c = fs.getContentSummary(dir);
    assertEquals("Invalid space consumed", 
        59 * FILE_SIZE * 3, 
        c.getSpaceConsumed());
    assertEquals("Invalid space consumed",
        QUOTA_SIZE - (59 * FILE_SIZE * 3),
        3 * (fs.getDefaultBlockSize() - FILE_SIZE));

    // Now check that trying to create another file violates the quota
    try {
      Path file = new Path("/test/test59");
      DFSTestUtil.createFile(fs, file, FILE_SIZE, (short) 3, 1L);
      DFSTestUtil.waitReplication(fs, file, (short) 3);
    } catch (QuotaExceededException e) {
      exceededQuota = true;
    }
    assertTrue("Quota not exceeded", exceededQuota);
  }
}
