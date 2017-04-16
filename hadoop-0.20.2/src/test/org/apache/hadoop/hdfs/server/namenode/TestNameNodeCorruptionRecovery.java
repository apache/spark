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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the name node's ability to recover from partially corrupted storage
 * directories.
 */
public class TestNameNodeCorruptionRecovery {
  
  private MiniDFSCluster cluster;
  
  @Before
  public void setUpCluster() throws IOException {
    cluster = new MiniDFSCluster(new Configuration(), 0, true, null);
    cluster.waitActive();
  }
  
  @After
  public void tearDownCluster() {
    cluster.shutdown();
  }

  /**
   * Test that a corrupted fstime file in a single storage directory does not
   * prevent the NN from starting up.
   */
  @Test
  public void testFsTimeFileCorrupt() throws IOException, InterruptedException {
    assertEquals(cluster.getNameDirs().size(), 2);
    // Get the first fstime file and truncate it.
    truncateStorageDirFile(cluster, NameNodeFile.TIME, 0);
    // Make sure we can start up despite the fact the fstime file is corrupted.
    cluster.restartNameNode();
  }
  
  private static void truncateStorageDirFile(MiniDFSCluster cluster,
      NameNodeFile f, int storageDirIndex) throws IOException {
    File currentDir = cluster.getNameNode().getFSImage()
        .getStorageDir(storageDirIndex).getCurrentDir();
    File nameNodeFile = new File(currentDir, f.getName());
    assertTrue(nameNodeFile.isFile());
    assertTrue(nameNodeFile.delete());
    assertTrue(nameNodeFile.createNewFile());
  }
}