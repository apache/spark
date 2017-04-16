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
package org.apache.hadoop.hdfs.server.common;

import java.io.IOException;
import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.hdfs.protocol.FSConstants.LAYOUT_VERSION;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestDFSUpgradeFromImage;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.UpgradeObjectDatanode;
import org.apache.hadoop.hdfs.server.namenode.UpgradeObjectNamenode;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.hdfs.tools.DFSAdmin;

/**
 */
public class TestDistributedUpgrade extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestDistributedUpgrade.class);
  private Configuration conf;
  private int testCounter = 0;
  private MiniDFSCluster cluster = null;
    
  /**
   * Writes an INFO log message containing the parameters.
   */
  void log(String label, int numDirs) {
    LOG.info("============================================================");
    LOG.info("***TEST " + (testCounter++) + "*** " 
             + label + ":"
             + " numDirs="+numDirs);
  }
  
  /**
   * Attempts to start a NameNode with the given operation.  Starting
   * the NameNode should throw an exception.
   */
  void startNameNodeShouldFail(StartupOption operation) {
    try {
      //cluster = new MiniDFSCluster(conf, 0, operation); // should fail
      // we set manage dirs to true as NN has to start from untar'ed image with 
      // nn dirs set to name1 and name2
      cluster = new MiniDFSCluster(0, conf, 0, false, true,
          operation, null); // Should fail
      throw new AssertionError("NameNode should have failed to start");
    } catch (Exception expected) {
      expected = null;
      // expected
    }
  }
  
  /**
   * Attempts to start a DataNode with the given operation.  Starting
   * the DataNode should throw an exception.
   */
  void startDataNodeShouldFail(StartupOption operation) {
    try {
      cluster.startDataNodes(conf, 1, false, operation, null); // should fail
      throw new AssertionError("DataNode should have failed to start");
    } catch (Exception expected) {
      // expected
      assertFalse(cluster.isDataNodeUp());
    }
  }
 
  /**
   */
  public void testDistributedUpgrade() throws Exception {
    int numDirs = 1;
    TestDFSUpgradeFromImage testImg = new TestDFSUpgradeFromImage();
    testImg.unpackStorage();
    int numDNs = testImg.numDataNodes;

    // register new upgrade objects (ignore all existing)
    UpgradeObjectCollection.initialize();
    UpgradeObjectCollection.registerUpgrade(new UO_Datanode1());
    UpgradeObjectCollection.registerUpgrade(new UO_Namenode1());
    UpgradeObjectCollection.registerUpgrade(new UO_Datanode2());
    UpgradeObjectCollection.registerUpgrade(new UO_Namenode2());
    UpgradeObjectCollection.registerUpgrade(new UO_Datanode3());
    UpgradeObjectCollection.registerUpgrade(new UO_Namenode3());

    conf = new Configuration();
    if (System.getProperty("test.build.data") == null) { // to test to be run outside of ant
      System.setProperty("test.build.data", "build/test/data");
    }
    conf.setInt("dfs.datanode.scan.period.hours", -1); // block scanning off

    log("NameNode start in regular mode when dustributed upgrade is required", numDirs);
    startNameNodeShouldFail(StartupOption.REGULAR);

    log("Start NameNode only distributed upgrade", numDirs);
    // cluster = new MiniDFSCluster(conf, 0, StartupOption.UPGRADE);
    cluster = new MiniDFSCluster(0, conf, 0, false, true,
                                  StartupOption.UPGRADE, null);
    cluster.shutdown();

    log("NameNode start in regular mode when dustributed upgrade has been started", numDirs);
    startNameNodeShouldFail(StartupOption.REGULAR);

    log("NameNode rollback to the old version that require a dustributed upgrade", numDirs);
    startNameNodeShouldFail(StartupOption.ROLLBACK);

    log("Normal distributed upgrade for the cluster", numDirs);
    cluster = new MiniDFSCluster(0, conf, numDNs, false, true,
                                  StartupOption.UPGRADE, null);
    DFSAdmin dfsAdmin = new DFSAdmin();
    dfsAdmin.setConf(conf);
    dfsAdmin.run(new String[] {"-safemode", "wait"});
    cluster.shutdown();

    // it should be ok to start in regular mode
    log("NameCluster regular startup after the upgrade", numDirs);
    cluster = new MiniDFSCluster(0, conf, numDNs, false, true,
                                  StartupOption.REGULAR, null);
    cluster.waitActive();
    cluster.shutdown();
  }

  public static void main(String[] args) throws Exception {
    new TestDistributedUpgrade().testDistributedUpgrade();
    LOG.info("=== DONE ===");
  }
}

/**
 * Upgrade object for data-node
 */
class UO_Datanode extends UpgradeObjectDatanode {
  int version;

  UO_Datanode(int v) {
    this.status = (short)0;
    version = v;
  }

  public int getVersion() {
    return version;
  }

  public void doUpgrade() throws IOException {
    this.status = (short)100;
    getDatanode().namenode.processUpgradeCommand(
        new UpgradeCommand(UpgradeCommand.UC_ACTION_REPORT_STATUS, 
            getVersion(), getUpgradeStatus()));
  }

  public UpgradeCommand startUpgrade() throws IOException {
    return null;
  }
}

/**
 * Upgrade object for name-node
 */
class UO_Namenode extends UpgradeObjectNamenode {
  int version;

  UO_Namenode(int v) {
    status = (short)0;
    version = v;
  }

  public int getVersion() {
    return version;
  }

  synchronized public UpgradeCommand processUpgradeCommand(
                                  UpgradeCommand command) throws IOException {
    switch(command.getAction()) {
      case UpgradeCommand.UC_ACTION_REPORT_STATUS:
        this.status += command.getCurrentStatus()/8;  // 4 reports needed
        break;
      default:
        this.status++;
    }
    return null;
  }

  public UpgradeCommand completeUpgrade() throws IOException {
    return null;
  }
}

class UO_Datanode1 extends UO_Datanode {
  UO_Datanode1() {
    super(LAYOUT_VERSION+1);
  }
}

class UO_Namenode1 extends UO_Namenode {
  UO_Namenode1() {
    super(LAYOUT_VERSION+1);
  }
}

class UO_Datanode2 extends UO_Datanode {
  UO_Datanode2() {
    super(LAYOUT_VERSION+2);
  }
}

class UO_Namenode2 extends UO_Namenode {
  UO_Namenode2() {
    super(LAYOUT_VERSION+2);
  }
}

class UO_Datanode3 extends UO_Datanode {
  UO_Datanode3() {
    super(LAYOUT_VERSION+3);
  }
}

class UO_Namenode3 extends UO_Namenode {
  UO_Namenode3() {
    super(LAYOUT_VERSION+3);
  }
}
