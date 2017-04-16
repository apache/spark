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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test the version check the DN performs when connecting to the NN
 */
public class TestDataNodeVersionCheck {

  /**
   * Test the strict DN version checking
   */
  @Test
  public void testStrictVersionCheck() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      conf.setBoolean(
          CommonConfigurationKeys.HADOOP_RELAXED_VERSION_CHECK_KEY, false);
      cluster = new MiniDFSCluster(conf, 1, true, null);

      DataNode dn = cluster.getDataNodes().get(0);
    
      final NamespaceInfo currInfo = new NamespaceInfo(0, 0, 0);
      assertTrue(dn.isPermittedVersion(currInfo));

      // Different revisions are not permitted
      NamespaceInfo infoDiffRev = new NamespaceInfo(0, 0, 0) {
                @Override public String getRevision() { return "bogus"; }
      };      
      assertFalse("Different revision is not permitted",
          dn.isPermittedVersion(infoDiffRev));

      // Different versions are not permitted
      NamespaceInfo infoDiffVersion = new NamespaceInfo(0, 0, 0) {
        @Override public String getVersion() { return "bogus"; }
        @Override public String getRevision() { return "bogus"; }
      };
      assertFalse("Different version is not permitted",
          dn.isPermittedVersion(infoDiffVersion));

      // A bogus version (matching revision but not version)
      NamespaceInfo bogusVersion = new NamespaceInfo(0, 0, 0) {
        @Override public String getVersion() { return "bogus"; }
      };
      try {
        dn.isPermittedVersion(bogusVersion);
        fail("Matched revision with mismatched version");
      } catch (AssertionError ae) {
        // Expected
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  /**
   * Test the "relaxed" DN version checking
   */
  @Test
  public void testRelaxedVersionCheck() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 1, true, null);
      
      DataNode dn = cluster.getDataNodes().get(0);
    
      final NamespaceInfo currInfo = new NamespaceInfo(0, 0, 0);
      assertTrue(dn.isPermittedVersion(currInfo));

      // Different revisions are permitted
      NamespaceInfo infoDiffRev = new NamespaceInfo(0, 0, 0) {
        @Override public String getRevision() { return "bogus"; }
      };      
      assertTrue("Different revisions should be permitted",
          dn.isPermittedVersion(infoDiffRev));

      // Different versions are not permitted
      NamespaceInfo infoDiffVersion = new NamespaceInfo(0, 0, 0) {
        @Override public String getVersion() { return "bogus"; }
        @Override public String getRevision() { return "bogus"; }
      };
      assertFalse("Different version is not permitted",
          dn.isPermittedVersion(infoDiffVersion));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}