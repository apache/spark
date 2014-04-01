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

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;

public class TestSecondaryWebUi {

  @SuppressWarnings("deprecation")
  @Test
  public void testSecondaryWebUi() throws IOException {
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY,
        "0.0.0.0:0");
    MiniDFSCluster cluster = null;
    SecondaryNameNode snn = null;
    try {
      cluster = new MiniDFSCluster(conf, 0, true, null);
      cluster.waitActive();
      
      snn = new SecondaryNameNode(conf);
      String pageContents = DFSTestUtil.urlGet(new URL("http://localhost:" +
          SecondaryNameNode.getHttpAddress(conf).getPort() + "/status.jsp"));
      assertTrue(pageContents.contains("Last Checkpoint Time"));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      if (snn != null) {
        snn.shutdown();
      }
    }
  }
}
