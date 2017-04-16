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

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.net.NetworkTopology;

import junit.framework.TestCase;

public class TestGetSplitHosts extends TestCase {

  public void testGetSplitHosts() throws Exception {

    int numBlocks = 3;
    int block1Size = 100, block2Size = 150, block3Size = 75;
    int fileSize = block1Size + block2Size + block3Size;
    int replicationFactor = 3;
    NetworkTopology clusterMap = new NetworkTopology();
    
    BlockLocation[] bs = new BlockLocation[numBlocks];
    
    String [] block1Hosts = {"host1","host2","host3"};
    String [] block1Names = {"host1:100","host2:100","host3:100"};
    String [] block1Racks = {"/rack1/","/rack1/","/rack2/"};
    String [] block1Paths = new String[replicationFactor];
    
    for (int i = 0; i < replicationFactor; i++) { 
      block1Paths[i] = block1Racks[i]+block1Names[i];
    }
    
    bs[0] = new BlockLocation(block1Names,block1Hosts,
                              block1Paths,0,block1Size);


    String [] block2Hosts = {"host4","host5","host6"};
    String [] block2Names = {"host4:100","host5:100","host6:100"};
    String [] block2Racks = {"/rack2/","/rack3/","/rack3/"};
    String [] block2Paths = new String[replicationFactor];
    
    for (int i = 0; i < replicationFactor; i++) { 
      block2Paths[i] = block2Racks[i]+block2Names[i];
    }

    bs[1] = new BlockLocation(block2Names,block2Hosts,
                              block2Paths,block1Size,block2Size);

    String [] block3Hosts = {"host1","host7","host8"};
    String [] block3Names = {"host1:100","host7:100","host8:100"};
    String [] block3Racks = {"/rack1/","/rack4/","/rack4/"};
    String [] block3Paths = new String[replicationFactor];
    
    for (int i = 0; i < replicationFactor; i++) { 
      block3Paths[i] = block3Racks[i]+block3Names[i];
    }

    bs[2] = new BlockLocation(block3Names,block3Hosts,
                              block3Paths,block1Size+block2Size,
                              block3Size);

    
    SequenceFileInputFormat< String, String> sif = 
      new SequenceFileInputFormat<String,String>();
    String [] hosts = sif.getSplitHosts(bs, 0, fileSize, clusterMap);

    // Contributions By Racks are
    // Rack1   175       
    // Rack2   275       
    // Rack3   150       
    // So, Rack2 hosts, host4 and host 3 should be returned
    // even if their individual contribution is not the highest

    assertTrue (hosts.length == replicationFactor);
    assertTrue(hosts[0].equalsIgnoreCase("host4"));
    assertTrue(hosts[1].equalsIgnoreCase("host3"));
    assertTrue(hosts[2].equalsIgnoreCase("host1"));
    
    
    // Now Create the blocks without topology information
    bs[0] = new BlockLocation(block1Names,block1Hosts,0,block1Size);
    bs[1] = new BlockLocation(block2Names,block2Hosts,block1Size,block2Size);
    bs[2] = new BlockLocation(block3Names,block3Hosts,block1Size+block2Size,
                               block3Size);

    hosts = sif.getSplitHosts(bs, 0, fileSize, clusterMap);
    
    // host1 makes the highest contribution among all hosts
    // So, that should be returned before others
    
    assertTrue (hosts.length == replicationFactor);
    assertTrue(hosts[0].equalsIgnoreCase("host1"));
  }
}
