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

import java.net.InetSocketAddress;
import java.util.ArrayList;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.datanode.DataNode;

/**
 * This test ensures the all types of data node report work correctly.
 */
public class TestDatanodeReport extends TestCase {
  final static private Configuration conf = new Configuration();
  final static private int NUM_OF_DATANODES = 4;
    
  /**
   * This test attempts to different types of datanode report.
   */
  public void testDatanodeReport() throws Exception {
    conf.setInt(
        "heartbeat.recheck.interval", 500); // 0.5s
    MiniDFSCluster cluster = 
      new MiniDFSCluster(conf, NUM_OF_DATANODES, true, null);
    try {
      //wait until the cluster is up
      cluster.waitActive();

      InetSocketAddress addr = new InetSocketAddress("localhost",
          cluster.getNameNodePort());
      DFSClient client = new DFSClient(addr, conf);

      assertEquals(client.datanodeReport(DatanodeReportType.ALL).length,
                   NUM_OF_DATANODES);
      assertEquals(client.datanodeReport(DatanodeReportType.LIVE).length,
                   NUM_OF_DATANODES);
      assertEquals(client.datanodeReport(DatanodeReportType.DEAD).length, 0);

      // bring down one datanode
      ArrayList<DataNode> datanodes = cluster.getDataNodes();
      datanodes.remove(datanodes.size()-1).shutdown();

      DatanodeInfo[] nodeInfo = client.datanodeReport(DatanodeReportType.DEAD);
      while (nodeInfo.length != 1) {
        try {
          Thread.sleep(500);
        } catch (Exception e) {
        }
        nodeInfo = client.datanodeReport(DatanodeReportType.DEAD);
      }

      assertEquals(client.datanodeReport(DatanodeReportType.LIVE).length,
                   NUM_OF_DATANODES-1);
      assertEquals(client.datanodeReport(DatanodeReportType.ALL).length,
                   NUM_OF_DATANODES);
    }finally {
      cluster.shutdown();
    }
  }
 
  public static void main(String[] args) throws Exception {
    new TestDatanodeReport().testDatanodeReport();
  }
  
}


