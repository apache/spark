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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

/**
 * Test the ability of a DN to tolerate volume failures.
 */
public class TestDataNodeVolumeFailureToleration {

  private static final Log LOG = LogFactory.getLog(TestDataNodeVolumeFailureToleration.class);
  {
    ((Log4JLogger)TestDataNodeVolumeFailureToleration.LOG).getLogger().setLevel(Level.ALL);
  }

  private FileSystem fs;
  private MiniDFSCluster cluster;
  private Configuration conf;
  private String dataDir;

  // Sleep at least 3 seconds (a 1s heartbeat plus padding) to allow
  // for heartbeats to propagate from the datanodes to the namenode.
  final int WAIT_FOR_HEARTBEATS = 3000;

  // Wait at least (2 * re-check + 10 * heartbeat) seconds for
  // a datanode to be considered dead by the namenode.  
  final int WAIT_FOR_DEATH = 15000;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong("dfs.block.size", 512L);
    /*
     * Lower the DN heartbeat, DF rate, and recheck interval to one second
     * so state about failures and datanode death propagates faster.
     */
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_DF_INTERVAL_KEY, 1000);
    conf.setInt("heartbeat.recheck.interval", 1000);
    // Allow a single volume failure (there are two volumes)
    conf.setInt("dfs.datanode.failed.volumes.tolerated", 1);
    cluster = new MiniDFSCluster(conf, 1, true, null);
    cluster.waitActive();
    fs = cluster.getFileSystem();
    dataDir = cluster.getDataDirectory();
  }

  @After
  public void tearDown() throws Exception {
    for (int i = 0; i < 3; i++) {
      new File(dataDir, "data"+(2*i+1)).setExecutable(true);
      new File(dataDir, "data"+(2*i+2)).setExecutable(true);
    }
    cluster.shutdown();
  }

  /**
   * Test the DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY configuration
   * option, ie the DN shuts itself down when the number of failures
   * experienced drops below the tolerated amount.
   */
  @Test
  public void testConfigureMinValidVolumes() throws Exception {
    assumeTrue(!System.getProperty("os.name").startsWith("Windows"));

    // Bring up two additional datanodes that need both of their volumes
    // functioning in order to stay up.
    conf.setInt("dfs.datanode.failed.volumes.tolerated", 0);
    cluster.startDataNodes(conf, 2, true, null, null);
    cluster.waitActive();
    FSNamesystem ns = cluster.getNameNode().getNamesystem();
    long origCapacity = DFSTestUtil.getLiveDatanodeCapacity(ns);
    long dnCapacity = DFSTestUtil.getDatanodeCapacity(ns, 0);

    // Fail a volume on the 2nd DN
    File dn2Vol1 = new File(dataDir, "data"+(2*1+1));
    assertTrue("Couldn't chmod local vol", dn2Vol1.setExecutable(false));

    // Should only get two replicas (the first DN and the 3rd)
    Path file1 = new Path("/test1");
    DFSTestUtil.createFile(fs, file1, 1024, (short)3, 1L);
    DFSTestUtil.waitReplication(fs, file1, (short)2);

    // Check that this single failure caused a DN to die.
    DFSTestUtil.waitForDatanodeStatus(ns, 2, 1, 0, 
        origCapacity - (1*dnCapacity), WAIT_FOR_HEARTBEATS);

    // If we restore the volume we should still only be able to get
    // two replicas since the DN is still considered dead.
    assertTrue("Couldn't chmod local vol", dn2Vol1.setExecutable(true));
    Path file2 = new Path("/test2");
    DFSTestUtil.createFile(fs, file2, 1024, (short)3, 1L);
    DFSTestUtil.waitReplication(fs, file2, (short)2);
  }

  /**
   * Restart the datanodes in the cluster with a new volume tolerated value.
   * @param volTolerated
   * @param manageDfsDirs
   * @throws IOException
   */
  private void restartDatanodes(int volTolerated, boolean manageDfsDirs)
      throws IOException {
    cluster.shutdownDataNodes();
    conf.setInt("dfs.datanode.failed.volumes.tolerated", volTolerated);
    cluster.startDataNodes(conf, 1, manageDfsDirs, null, null);
    cluster.waitActive();
  }

  /**
   * Test for different combination of volume configs and volumes
   * tolerated values.
   */
  @Test
  public void testInvalidFailedVolumesConfig() throws Exception {
    assumeTrue(!System.getProperty("os.name").startsWith("Windows"));

    // Check if DN exits for an invalid conf value.
    testVolumeConfig(-1, 0, false, true);

    testVolumeConfig(100, 0, false, true);

    // Test for one failed volume
    testVolumeConfig(0, 1, false, false);

    // Test for one failed volume with 1 tolerable volume
    testVolumeConfig(1, 1, true, false);

    // Test all good volumes
    testVolumeConfig(0, 0, true, false);

    // Test all failed volumes
    testVolumeConfig(0, 2, false, false);
  }

  /**
   * Tests for a given volumes to be tolerated and volumes failed.
   */
  private void testVolumeConfig(int volumesTolerated, int volumesFailed,
      boolean expectDnUp, boolean manageDfsDirs)
      throws IOException, InterruptedException {
    File dir0 = new File(dataDir, "data1");
    File dir1 = new File(dataDir, "data2");

    // Fail the current directory since invalid storage directory perms
    // get fixed up automatically on datanode startup.
    File[] currDirs = { new File(dir0, "/current"),
                        new File(dir1, "/current") };
    
    try {
      for (int i = 0; i < volumesFailed; i++) {
        prepareDirToFail(currDirs[i]);
      }
      try {
        restartDatanodes(volumesTolerated, manageDfsDirs);
        assertEquals(expectDnUp, cluster.getDataNodes().get(0).isDatanodeUp());
      } catch (IOException ioe) {
        assertFalse("Expected successful restart but got " + ioe, expectDnUp);
      }
    } finally {
      for (File dir : currDirs) {
        FileUtil.chmod(dir.toString(), "755");
      }
    }
  }

  /**
   * Prepare directories for a failure, set dir permission to 000
   * @param dir
   * @throws IOException
   * @throws InterruptedException
   */
  private void prepareDirToFail(File dir) throws IOException,
      InterruptedException {
    dir.mkdirs();
    assertEquals("Couldn't chmod local vol", 0,
        FileUtil.chmod(dir.toString(), "000"));
  }

  /**
   * Test that a volume that is considered failed on startup is seen as
   *  a failed volume by the NN.
   */
  @Test
  public void testFailedVolumeOnStartupIsCounted() throws Exception {
    assumeTrue(!System.getProperty("os.name").startsWith("Windows"));

    FSNamesystem ns = cluster.getNameNode().getNamesystem();
    long origCapacity = DFSTestUtil.getLiveDatanodeCapacity(ns);
    File dir = new File(dataDir, "data1/current");

    try {
      prepareDirToFail(dir);
      restartDatanodes(1, false);
      // The cluster is up..
      assertTrue(cluster.getDataNodes().get(0).isDatanodeUp());
      // but there has been a single volume failure
      DFSTestUtil.waitForDatanodeStatus(ns, 1, 0, 1,
          origCapacity / 2, WAIT_FOR_HEARTBEATS);
    } finally {
      FileUtil.chmod(dir.toString(), "755");
    }
  }

}
