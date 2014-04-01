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
package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.VersionInfo;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test the version check the TT performs when connecting to the JT
 */
public class TestTaskTrackerVersionCheck {

  /**
   * Test the strict TT version checking
   */
  @Test
  public void testDefaultVersionCheck() throws IOException {
    MiniMRCluster mr = null;
    try {
      JobConf jtConf = new JobConf();
      jtConf.setBoolean(
          CommonConfigurationKeys.HADOOP_RELAXED_VERSION_CHECK_KEY, false);
      mr = new MiniMRCluster(1, "file:///", 1, null, null, jtConf);
      TaskTracker tt = mr.getTaskTrackerRunner(0).getTaskTracker();
      String currBuildVersion = VersionInfo.getBuildVersion();
      String currVersion = VersionInfo.getVersion();

      assertTrue(tt.isPermittedVersion(currBuildVersion, currVersion));
      assertFalse("We disallow different versions",
          tt.isPermittedVersion(currBuildVersion+"x", currVersion+"x"));
      assertFalse("We disallow different full versions with same version",
          tt.isPermittedVersion(currBuildVersion+"x", currVersion));      
      try {
        tt.isPermittedVersion(currBuildVersion, currVersion+"x");
        fail("Matched full version with mismatched version");
      } catch (AssertionError ae) {
        // Expected. The versions should always match if the full
        // versions match as the full version contains the version.
      }
    } finally {
      if (mr != null) {
        mr.shutdown();
      }
    }
  }

  /**
   * Test the "relaxed" TT version checking
   */
  @Test
  public void testRelaxedVersionCheck() throws IOException {
    MiniMRCluster mr = null;
    try {
      JobConf jtConf = new JobConf();
      mr = new MiniMRCluster(1, "file:///", 1, null, null, jtConf);
      TaskTracker tt = mr.getTaskTrackerRunner(0).getTaskTracker();
      String currFullVersion = VersionInfo.getBuildVersion();
      String currVersion = VersionInfo.getVersion();

      assertTrue(tt.isPermittedVersion(currFullVersion, currVersion));
      assertFalse("We dissallow different versions",
          tt.isPermittedVersion(currFullVersion+"x", currVersion+"x"));
      assertTrue("We allow different full versions with same version",
          tt.isPermittedVersion(currFullVersion+"x", currVersion));
    } finally {
      if (mr != null) {
        mr.shutdown();
      }
    }
  }
}
