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
import java.io.IOException;
import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType.NAME_NODE;
import static org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType.DATA_NODE;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;

/**
 * This test ensures the appropriate response from the system when 
 * the system is finalized.
 */
public class TestDFSFinalize extends TestCase {
 
  private static final Log LOG = LogFactory.getLog(
                                                   "org.apache.hadoop.hdfs.TestDFSFinalize");
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
   * Verify that the current directory exists and that the previous directory
   * does not exist.  Verify that current hasn't been modified by comparing 
   * the checksum of all it's containing files with their original checksum.
   * Note that we do not check that previous is removed on the DataNode
   * because its removal is asynchronous therefore we have no reliable
   * way to know when it will happen.  
   */
  void checkResult(String[] nameNodeDirs, String[] dataNodeDirs) throws IOException {
    for (int i = 0; i < nameNodeDirs.length; i++) {
      assertTrue(new File(nameNodeDirs[i],"current").isDirectory());
      assertTrue(new File(nameNodeDirs[i],"current/VERSION").isFile());
      assertTrue(new File(nameNodeDirs[i],"current/edits").isFile());
      assertTrue(new File(nameNodeDirs[i],"current/fsimage").isFile());
      assertTrue(new File(nameNodeDirs[i],"current/fstime").isFile());
    }
    for (int i = 0; i < dataNodeDirs.length; i++) {
      assertEquals(
                   UpgradeUtilities.checksumContents(
                                                     DATA_NODE, new File(dataNodeDirs[i],"current")),
                   UpgradeUtilities.checksumMasterContents(DATA_NODE));
    }
    for (int i = 0; i < nameNodeDirs.length; i++) {
      assertFalse(new File(nameNodeDirs[i],"previous").isDirectory());
    }
  }
 
  /**
   * This test attempts to finalize the NameNode and DataNode.
   */
  public void testFinalize() throws Exception {
    UpgradeUtilities.initialize();
    
    for (int numDirs = 1; numDirs <= 2; numDirs++) {
      /* This test requires that "current" directory not change after
       * the upgrade. Actually it is ok for those contents to change.
       * For now disabling block verification so that the contents are 
       * not changed.
       */
      conf = new Configuration();
      conf.setInt("dfs.datanode.scan.period.hours", -1);
      conf = UpgradeUtilities.initializeStorageStateConf(numDirs, conf);
      String[] nameNodeDirs = conf.getStrings("dfs.name.dir");
      String[] dataNodeDirs = conf.getStrings("dfs.data.dir");
      
      log("Finalize with existing previous dir", numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "previous");
      UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
      UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "previous");
      cluster = new MiniDFSCluster(conf, 1, StartupOption.REGULAR);
      cluster.finalizeCluster(conf);
      checkResult(nameNodeDirs, dataNodeDirs);

      log("Finalize without existing previous dir", numDirs);
      cluster.finalizeCluster(conf);
      checkResult(nameNodeDirs, dataNodeDirs);

      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);
    } // end numDir loop
  }
 
  protected void tearDown() throws Exception {
    LOG.info("Shutting down MiniDFSCluster");
    if (cluster != null) cluster.shutdown();
  }
  
  public static void main(String[] args) throws Exception {
    new TestDFSFinalize().testFinalize();
  }
  
}


