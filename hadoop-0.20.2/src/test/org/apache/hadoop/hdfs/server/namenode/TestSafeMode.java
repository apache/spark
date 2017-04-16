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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.DFSTestUtil;
import junit.framework.TestCase;

/**
 * Tests to verify safe mode correctness.
 */
public class TestSafeMode extends TestCase {
  
  static Log LOG = LogFactory.getLog(TestSafeMode.class);

  /**
   * This test verifies that if SafeMode is manually entered, name-node does not
   * come out of safe mode even after the startup safe mode conditions are met.
   * <ol>
   * <li>Start cluster with 1 data-node.</li>
   * <li>Create 2 files with replication 1.</li>
   * <li>Re-start cluster with 0 data-nodes. 
   * Name-node should stay in automatic safe-mode.</li>
   * <li>Enter safe mode manually.</li>
   * <li>Start the data-node.</li>
   * <li>Wait longer than <tt>dfs.safemode.extension</tt> and 
   * verify that the name-node is still in safe mode.</li>
   * </ol>
   *  
   * @throws IOException
   */
  public void testManualSafeMode() throws IOException {
    MiniDFSCluster cluster = null;
    DistributedFileSystem fs = null;
    try {
      Configuration conf = new Configuration();
      // disable safemode extension to make the test run faster.
      conf.set("dfs.safemode.extension", "1");
      cluster = new MiniDFSCluster(conf, 1, true, null);
      cluster.waitActive();
      
      fs = (DistributedFileSystem)cluster.getFileSystem();
      Path file1 = new Path("/tmp/testManualSafeMode/file1");
      Path file2 = new Path("/tmp/testManualSafeMode/file2");
      
      LOG.info("Created file1 and file2.");
      
      // create two files with one block each.
      DFSTestUtil.createFile(fs, file1, 1000, (short)1, 0);
      DFSTestUtil.createFile(fs, file2, 2000, (short)1, 0);
      fs.close();
      cluster.shutdown();
      
      // now bring up just the NameNode.
      cluster = new MiniDFSCluster(conf, 0, false, null);
      cluster.waitActive();
      fs = (DistributedFileSystem)cluster.getFileSystem();
      
      LOG.info("Restarted cluster with just the NameNode");
      
      assertTrue("No datanode is started. Should be in SafeMode", 
                 fs.setSafeMode(SafeModeAction.SAFEMODE_GET));
      
      // manually set safemode.
      fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      
      // now bring up the datanode and wait for it to be active.
      cluster.startDataNodes(conf, 1, true, null, null);
      cluster.waitActive();
      
      LOG.info("Datanode is started.");

      // wait longer than dfs.safemode.extension
      try {
        Thread.sleep(2000);
      } catch (InterruptedException ignored) {}
      
      assertTrue("should still be in SafeMode",
          fs.setSafeMode(SafeModeAction.SAFEMODE_GET));
      
      fs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
      assertFalse("should not be in SafeMode",
          fs.setSafeMode(SafeModeAction.SAFEMODE_GET));
    } finally {
      if(fs != null) fs.close();
      if(cluster!= null) cluster.shutdown();
    }
  }


  /**
   * Verify that the NameNode stays in safemode when dfs.safemode.datanode.min
   * is set to a number greater than the number of live datanodes.
   */
  public void testDatanodeThreshold() throws IOException {
    MiniDFSCluster cluster = null;
    DistributedFileSystem fs = null;
    try {
      Configuration conf = new Configuration();
      conf.set("dfs.safemode.extension", "0");
      conf.set("dfs.safemode.min.datanodes", "1");

      // bring up a cluster with no datanodes
      cluster = new MiniDFSCluster(conf, 0, false, null);
      cluster.waitActive();
      fs = (DistributedFileSystem)cluster.getFileSystem();

      assertTrue("No datanode started, but we require one - safemode expected",
                 fs.setSafeMode(SafeModeAction.SAFEMODE_GET));

      String tipMsg = cluster.getNameNode().getNamesystem().getSafeModeTip();
      assertTrue("Safemode tip message looks right",
                 tipMsg.contains("The number of live datanodes 0 needs an " +
                                 "additional 1 live"));

      // Start a datanode
      cluster.startDataNodes(conf, 1, true, null, null);

      // Wait long enough for safemode check to refire
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignored) {}

      // We now should be out of safe mode.
      assertFalse(
        "Out of safe mode after starting datanode.",
        fs.setSafeMode(SafeModeAction.SAFEMODE_GET));
    } finally {
      if (fs != null) fs.close();
      if (cluster != null) cluster.shutdown();
    }
  }
}
