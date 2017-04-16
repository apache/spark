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
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;

import static org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType.NAME_NODE;
import static org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType.DATA_NODE;

/**
* This test ensures the appropriate response (successful or failure) from
* the system when the system is started under various storage state and
* version conditions.
*/
public class TestDFSStorageStateRecovery extends TestCase {
 
  private static final Log LOG = LogFactory.getLog(
                                                   "org.apache.hadoop.hdfs.TestDFSStorageStateRecovery");
  private Configuration conf = null;
  private int testCounter = 0;
  private MiniDFSCluster cluster = null;
  
  /**
   * The test case table.  Each row represents a test case.  This table is
   * taken from the table in Apendix A of the HDFS Upgrade Test Plan
   * (TestPlan-HdfsUpgrade.html) attached to
   * http://issues.apache.org/jira/browse/HADOOP-702
   * The column meanings are:
   *  0) current directory exists
   *  1) previous directory exists
   *  2) previous.tmp directory exists
   *  3) removed.tmp directory exists
   *  4) lastcheckpoint.tmp directory exists
   *  5) node should recover and startup
   *  6) current directory should exist after recovery but before startup
   *  7) previous directory should exist after recovery but before startup
   */
  static boolean[][] testCases = new boolean[][] {
    new boolean[] {true,  false, false, false, false, true,  true,  false}, // 1
    new boolean[] {true,  true,  false, false, false, true,  true,  true }, // 2
    new boolean[] {true,  false, true,  false, false, true,  true,  true }, // 3
    new boolean[] {true,  true,  true,  true,  false, false, false, false}, // 4
    new boolean[] {true,  true,  true,  false, false, false, false, false}, // 4
    new boolean[] {false, true,  true,  true,  false, false, false, false}, // 4
    new boolean[] {false, true,  true,  false, false, false, false, false}, // 4
    new boolean[] {false, false, false, false, false, false, false, false}, // 5
    new boolean[] {false, true,  false, false, false, false, false, false}, // 6
    new boolean[] {false, false, true,  false, false, true,  true,  false}, // 7
    new boolean[] {true,  false, false, true,  false, true,  true,  false}, // 8
    new boolean[] {true,  true,  false, true,  false, false, false, false}, // 9
    new boolean[] {true,  true,  true,  true,  false, false, false, false}, // 10
    new boolean[] {true,  false, true,  true,  false, false, false, false}, // 10
    new boolean[] {false, true,  true,  true,  false, false, false, false}, // 10
    new boolean[] {false, false, true,  true,  false, false, false, false}, // 10
    new boolean[] {false, false, false, true,  false, false, false, false}, // 11
    new boolean[] {false, true,  false, true,  false, true,  true,  true }, // 12
    // name-node specific cases
    new boolean[] {true,  false, false, false, true,  true,  true,  false}, // 13
    new boolean[] {true,  true,  false, false, true,  true,  true,  false}, // 13
    new boolean[] {false, false, false, false, true,  true,  true,  false}, // 14
    new boolean[] {false, true,  false, false, true,  true,  true,  false}, // 14
    new boolean[] {true,  false, true,  false, true,  false, false, false}, // 15
    new boolean[] {true,  true,  false, true,  true,  false, false, false}  // 16
  };

  private static final int NUM_NN_TEST_CASES = testCases.length;
  private static final int NUM_DN_TEST_CASES = 18;

  /**
   * Writes an INFO log message containing the parameters. Only
   * the first 4 elements of the state array are included in the message.
   */
  void log(String label, int numDirs, int testCaseNum, boolean[] state) {
    LOG.info("============================================================");
    LOG.info("***TEST " + (testCounter++) + "*** " 
             + label + ":"
             + " numDirs="+numDirs
             + " testCase="+testCaseNum
             + " current="+state[0]
             + " previous="+state[1]
             + " previous.tmp="+state[2]
             + " removed.tmp="+state[3]
             + " lastcheckpoint.tmp="+state[4]);
  }
  
  /**
   * Sets up the storage directories for the given node type, either
   * dfs.name.dir or dfs.data.dir. For each element in dfs.name.dir or
   * dfs.data.dir, the subdirectories represented by the first four elements 
   * of the <code>state</code> array will be created and populated.
   * See UpgradeUtilities.createStorageDirs().
   * 
   * @param nodeType
   *   the type of node that storage should be created for. Based on this
   *   parameter either dfs.name.dir or dfs.data.dir is used from the global conf.
   * @param state
   *   a row from the testCases table which indicates which directories
   *   to setup for the node
   * @return file paths representing either dfs.name.dir or dfs.data.dir
   *   directories
   */
  String[] createStorageState(NodeType nodeType, boolean[] state) throws Exception {
    String[] baseDirs = (nodeType == NAME_NODE ?
                         conf.getStrings("dfs.name.dir") :
                         conf.getStrings("dfs.data.dir"));
    UpgradeUtilities.createEmptyDirs(baseDirs);
    if (state[0])  // current
      UpgradeUtilities.createStorageDirs(nodeType, baseDirs, "current");
    if (state[1])  // previous
      UpgradeUtilities.createStorageDirs(nodeType, baseDirs, "previous");
    if (state[2])  // previous.tmp
      UpgradeUtilities.createStorageDirs(nodeType, baseDirs, "previous.tmp");
    if (state[3])  // removed.tmp
      UpgradeUtilities.createStorageDirs(nodeType, baseDirs, "removed.tmp");
    if (state[4])  // lastcheckpoint.tmp
      UpgradeUtilities.createStorageDirs(nodeType, baseDirs, "lastcheckpoint.tmp");
    return baseDirs;
  }
 
  /**
   * Verify that the current and/or previous exist as indicated by 
   * the method parameters.  If previous exists, verify that
   * it hasn't been modified by comparing the checksum of all it's
   * containing files with their original checksum.  It is assumed that
   * the server has recovered.
   */
  void checkResult(NodeType nodeType, String[] baseDirs, 
                   boolean currentShouldExist, boolean previousShouldExist) 
    throws IOException
  {
    switch (nodeType) {
    case NAME_NODE:
      if (currentShouldExist) {
        for (int i = 0; i < baseDirs.length; i++) {
          assertTrue(new File(baseDirs[i],"current").isDirectory());
          assertTrue(new File(baseDirs[i],"current/VERSION").isFile());
          assertTrue(new File(baseDirs[i],"current/edits").isFile());
          assertTrue(new File(baseDirs[i],"current/fsimage").isFile());
          assertTrue(new File(baseDirs[i],"current/fstime").isFile());
        }
      }
      break;
    case DATA_NODE:
      if (currentShouldExist) {
        for (int i = 0; i < baseDirs.length; i++) {
          assertEquals(
                       UpgradeUtilities.checksumContents(
                                                         nodeType, new File(baseDirs[i],"current")),
                       UpgradeUtilities.checksumMasterContents(nodeType));
        }
      }
      break;
    }
    if (previousShouldExist) {
      for (int i = 0; i < baseDirs.length; i++) {
        assertTrue(new File(baseDirs[i],"previous").isDirectory());
        assertEquals(
                     UpgradeUtilities.checksumContents(
                                                       nodeType, new File(baseDirs[i],"previous")),
                     UpgradeUtilities.checksumMasterContents(nodeType));
      }
    }
  }
 
  /**
   * This test iterates over the testCases table and attempts
   * to startup the NameNode normally.
   */
  public void testNNStorageStates() throws Exception {
    String[] baseDirs;

    for (int numDirs = 1; numDirs <= 2; numDirs++) {
      conf = new Configuration();
      conf.setInt("dfs.datanode.scan.period.hours", -1);      
      conf = UpgradeUtilities.initializeStorageStateConf(numDirs, conf);
      for (int i = 0; i < NUM_NN_TEST_CASES; i++) {
        boolean[] testCase = testCases[i];
        boolean shouldRecover = testCase[5];
        boolean curAfterRecover = testCase[6];
        boolean prevAfterRecover = testCase[7];

        log("NAME_NODE recovery", numDirs, i, testCase);
        baseDirs = createStorageState(NAME_NODE, testCase);
        if (shouldRecover) {
          cluster = new MiniDFSCluster(conf, 0, StartupOption.REGULAR);
          checkResult(NAME_NODE, baseDirs, curAfterRecover, prevAfterRecover);
          cluster.shutdown();
        } else {
          try {
            cluster = new MiniDFSCluster(conf, 0, StartupOption.REGULAR);
            throw new AssertionError("NameNode should have failed to start");
          } catch (IOException expected) {
            // the exception is expected
            // check that the message says "not formatted" 
            // when storage directory is empty (case #5)
            if(!testCases[i][0] && !testCases[i][2] 
                  && !testCases[i][1] && !testCases[i][3] && !testCases[i][4]) {
              assertTrue(expected.getLocalizedMessage().contains(
                  "NameNode is not formatted"));
            }
          }
        }
        cluster.shutdown();
      } // end testCases loop
    } // end numDirs loop
  }

  /**
   * This test iterates over the testCases table and attempts
   * to startup the DataNode normally.
   */
  public void testDNStorageStates() throws Exception {
    String[] baseDirs;

    for (int numDirs = 1; numDirs <= 2; numDirs++) {
      conf = new Configuration();
      conf.setInt("dfs.datanode.scan.period.hours", -1);      
      conf = UpgradeUtilities.initializeStorageStateConf(numDirs, conf);
      for (int i = 0; i < NUM_DN_TEST_CASES; i++) {
        boolean[] testCase = testCases[i];
        boolean shouldRecover = testCase[5];
        boolean curAfterRecover = testCase[6];
        boolean prevAfterRecover = testCase[7];

        log("DATA_NODE recovery", numDirs, i, testCase);
        createStorageState(NAME_NODE,
                           new boolean[] {true, true, false, false, false});
        cluster = new MiniDFSCluster(conf, 0, StartupOption.REGULAR);
        baseDirs = createStorageState(DATA_NODE, testCase);
        if (!testCase[0] && !testCase[1] && !testCase[2] && !testCase[3]) {
          // DataNode will create and format current if no directories exist
          cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);
        } else {
          if (shouldRecover) {
            cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);
            checkResult(DATA_NODE, baseDirs, curAfterRecover, prevAfterRecover);
          } else {
            try {
              cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);
              throw new AssertionError("DataNode should have failed to start");
            } catch (Exception expected) {
              // expected
            }
          }
        }
        cluster.shutdown();
      } // end testCases loop
    } // end numDirs loop
  }

  protected void setUp() throws Exception {
    LOG.info("Setting up the directory structures.");
    UpgradeUtilities.initialize();
  }

  protected void tearDown() throws Exception {
    LOG.info("Shutting down MiniDFSCluster");
    if (cluster != null) cluster.shutdown();
  }
  
  public static void main(String[] args) throws Exception {
    TestDFSStorageStateRecovery test = new TestDFSStorageStateRecovery();
    test.testNNStorageStates();
    test.testDNStorageStates();
  }
  
}


