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

import java.io.File;
import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;

import static org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType.NAME_NODE;
import static org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType.DATA_NODE;

import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.fs.Path;

/**
 * This test ensures the appropriate response (successful or failure) from 
 * a Datanode when the system is started with differing version combinations. 
 */
public class TestDFSStartupVersions extends TestCase {
  
  private static final Log LOG = LogFactory.getLog(
                                                   "org.apache.hadoop.hdfs.TestDFSStartupVersions");
  private static Path TEST_ROOT_DIR = new Path(
                                               System.getProperty("test.build.data","/tmp").toString().replace(' ', '+'));
  private MiniDFSCluster cluster = null;
  
  /**
   * Writes an INFO log message containing the parameters.
   */
  void log(String label, NodeType nodeType, Integer testCase, StorageInfo version) {
    String testCaseLine = "";
    if (testCase != null) {
      testCaseLine = " testCase="+testCase;
    }
    LOG.info("============================================================");
    LOG.info("***TEST*** " + label + ":"
             + testCaseLine
             + " nodeType="+nodeType
             + " layoutVersion="+version.getLayoutVersion()
             + " namespaceID="+version.getNamespaceID()
             + " fsscTime="+version.getCTime());
  }
  
  /**
   * Initialize the versions array.  This array stores all combinations 
   * of cross product:
   *  {oldLayoutVersion,currentLayoutVersion,futureLayoutVersion} X
   *    {currentNamespaceId,incorrectNamespaceId} X
   *      {pastFsscTime,currentFsscTime,futureFsscTime}
   */
  private StorageInfo[] initializeVersions() throws Exception {
    int layoutVersionOld = Storage.LAST_UPGRADABLE_LAYOUT_VERSION;
    int layoutVersionCur = UpgradeUtilities.getCurrentLayoutVersion();
    int layoutVersionNew = Integer.MIN_VALUE;
    int namespaceIdCur = UpgradeUtilities.getCurrentNamespaceID(null);
    int namespaceIdOld = Integer.MIN_VALUE;
    long fsscTimeOld = Long.MIN_VALUE;
    long fsscTimeCur = UpgradeUtilities.getCurrentFsscTime(null);
    long fsscTimeNew = Long.MAX_VALUE;
    
    return new StorageInfo[] {
      new StorageInfo(layoutVersionOld, namespaceIdCur, fsscTimeOld), // 0
      new StorageInfo(layoutVersionOld, namespaceIdCur, fsscTimeCur), // 1
      new StorageInfo(layoutVersionOld, namespaceIdCur, fsscTimeNew), // 2
      new StorageInfo(layoutVersionOld, namespaceIdOld, fsscTimeOld), // 3
      new StorageInfo(layoutVersionOld, namespaceIdOld, fsscTimeCur), // 4
      new StorageInfo(layoutVersionOld, namespaceIdOld, fsscTimeNew), // 5
      new StorageInfo(layoutVersionCur, namespaceIdCur, fsscTimeOld), // 6
      new StorageInfo(layoutVersionCur, namespaceIdCur, fsscTimeCur), // 7
      new StorageInfo(layoutVersionCur, namespaceIdCur, fsscTimeNew), // 8
      new StorageInfo(layoutVersionCur, namespaceIdOld, fsscTimeOld), // 9
      new StorageInfo(layoutVersionCur, namespaceIdOld, fsscTimeCur), // 10
      new StorageInfo(layoutVersionCur, namespaceIdOld, fsscTimeNew), // 11
      new StorageInfo(layoutVersionNew, namespaceIdCur, fsscTimeOld), // 12
      new StorageInfo(layoutVersionNew, namespaceIdCur, fsscTimeCur), // 13
      new StorageInfo(layoutVersionNew, namespaceIdCur, fsscTimeNew), // 14
      new StorageInfo(layoutVersionNew, namespaceIdOld, fsscTimeOld), // 15
      new StorageInfo(layoutVersionNew, namespaceIdOld, fsscTimeCur), // 16
      new StorageInfo(layoutVersionNew, namespaceIdOld, fsscTimeNew), // 17
    };
  }
  
  /**
   * Determines if the given Namenode version and Datanode version
   * are compatible with each other. Compatibility in this case mean
   * that the Namenode and Datanode will successfully start up and
   * will work together. The rules for compatibility,
   * taken from the DFS Upgrade Design, are as follows:
   * <pre>
   * 1. The data-node does regular startup (no matter which options 
   *    it is started with) if
   *       softwareLV == storedLV AND 
   *       DataNode.FSSCTime == NameNode.FSSCTime
   * 2. The data-node performs an upgrade if it is started without any 
   *    options and
   *       |softwareLV| > |storedLV| OR 
   *       (softwareLV == storedLV AND
   *        DataNode.FSSCTime < NameNode.FSSCTime)
   * 3. NOT TESTED: The data-node rolls back if it is started with
   *    the -rollback option and
   *       |softwareLV| >= |previous.storedLV| AND 
   *       DataNode.previous.FSSCTime <= NameNode.FSSCTime
   * 4. In all other cases the startup fails.
   * </pre>
   */
  boolean isVersionCompatible(StorageInfo namenodeVer, StorageInfo datanodeVer) {
    // check #0
    if (namenodeVer.getNamespaceID() != datanodeVer.getNamespaceID()) {
      LOG.info("namespaceIDs are not equal: isVersionCompatible=false");
      return false;
    }
    // check #1
    int softwareLV = FSConstants.LAYOUT_VERSION;  // will also be Namenode's LV
    int storedLV = datanodeVer.getLayoutVersion();
    if (softwareLV == storedLV &&  
        datanodeVer.getCTime() == namenodeVer.getCTime()) 
      {
        LOG.info("layoutVersions and cTimes are equal: isVersionCompatible=true");
        return true;
      }
    // check #2
    long absSoftwareLV = Math.abs((long)softwareLV);
    long absStoredLV = Math.abs((long)storedLV);
    if (absSoftwareLV > absStoredLV ||
        (softwareLV == storedLV &&
         datanodeVer.getCTime() < namenodeVer.getCTime())) 
      {
        LOG.info("softwareLayoutVersion is newer OR namenode cTime is newer: isVersionCompatible=true");
        return true;
      }
    // check #4
    LOG.info("default case: isVersionCompatible=false");
    return false;
  }
  
  /**
   * This test ensures the appropriate response (successful or failure) from 
   * a Datanode when the system is started with differing version combinations. 
   * <pre>
   * For each 3-tuple in the cross product
   *   ({oldLayoutVersion,currentLayoutVersion,futureLayoutVersion},
   *    {currentNamespaceId,incorrectNamespaceId},
   *    {pastFsscTime,currentFsscTime,futureFsscTime})
   *      1. Startup Namenode with version file containing 
   *         (currentLayoutVersion,currentNamespaceId,currentFsscTime)
   *      2. Attempt to startup Datanode with version file containing 
   *         this iterations version 3-tuple
   * </pre>
   */
  public void testVersions() throws Exception {
    UpgradeUtilities.initialize();
    Configuration conf = UpgradeUtilities.initializeStorageStateConf(1, 
                                                      new Configuration());
    StorageInfo[] versions = initializeVersions();
    UpgradeUtilities.createStorageDirs(
                                       NAME_NODE, conf.getStrings("dfs.name.dir"), "current");
    cluster = new MiniDFSCluster(conf, 0, StartupOption.REGULAR);
    StorageInfo nameNodeVersion = new StorageInfo(
                                                  UpgradeUtilities.getCurrentLayoutVersion(),
                                                  UpgradeUtilities.getCurrentNamespaceID(cluster),
                                                  UpgradeUtilities.getCurrentFsscTime(cluster));
    log("NameNode version info", NAME_NODE, null, nameNodeVersion);
    for (int i = 0; i < versions.length; i++) {
      File[] storage = UpgradeUtilities.createStorageDirs(
                                                          DATA_NODE, conf.getStrings("dfs.data.dir"), "current");
      log("DataNode version info", DATA_NODE, i, versions[i]);
      UpgradeUtilities.createVersionFile(DATA_NODE, storage, versions[i]);
      try {
        cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);
      } catch (Exception ignore) {
        // Ignore.  The asserts below will check for problems.
        // ignore.printStackTrace();
      }
      assertTrue(cluster.getNameNode() != null);
      assertEquals(isVersionCompatible(nameNodeVersion, versions[i]),
                   cluster.isDataNodeUp());
      cluster.shutdownDataNodes();
    }
  }
  
  protected void tearDown() throws Exception {
    LOG.info("Shutting down MiniDFSCluster");
    if (cluster != null) cluster.shutdown();
  }
  
  public static void main(String[] args) throws Exception {
    new TestDFSStartupVersions().testVersions();
  }
  
}

