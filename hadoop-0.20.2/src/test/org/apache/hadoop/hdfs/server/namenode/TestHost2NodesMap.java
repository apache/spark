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

import org.apache.hadoop.hdfs.protocol.DatanodeID;

import junit.framework.TestCase;

public class TestHost2NodesMap extends TestCase {
  static private Host2NodesMap map = new Host2NodesMap();
  private final static DatanodeDescriptor dataNodes[] = new DatanodeDescriptor[] {
    new DatanodeDescriptor(new DatanodeID("h1:5020"), "/d1/r1"),
    new DatanodeDescriptor(new DatanodeID("h2:5020"), "/d1/r1"),
    new DatanodeDescriptor(new DatanodeID("h3:5020"), "/d1/r2"),
    new DatanodeDescriptor(new DatanodeID("h3:5030"), "/d1/r2"),
  };
  private final static DatanodeDescriptor NULL_NODE = null; 
  private final static DatanodeDescriptor NODE = 
    new DatanodeDescriptor(new DatanodeID("h3:5040"), "/d1/r4");

  static {
    for(DatanodeDescriptor node:dataNodes) {
      map.add(node);
    }
    map.add(NULL_NODE);
  }
  
  public void testContains() throws Exception {
    for(int i=0; i<dataNodes.length; i++) {
      assertTrue(map.contains(dataNodes[i]));
    }
    assertFalse(map.contains(NULL_NODE));
    assertFalse(map.contains(NODE));
  }

  public void testGetDatanodeByHost() throws Exception {
    assertTrue(map.getDatanodeByHost("h1")==dataNodes[0]);
    assertTrue(map.getDatanodeByHost("h2")==dataNodes[1]);
    DatanodeDescriptor node = map.getDatanodeByHost("h3");
    assertTrue(node==dataNodes[2] || node==dataNodes[3]);
    assertTrue(null==map.getDatanodeByHost("h4"));
  }

  public void testGetDatanodeByName() throws Exception {
    assertTrue(map.getDatanodeByName("h1:5020")==dataNodes[0]);
    assertTrue(map.getDatanodeByName("h1:5030")==null);
    assertTrue(map.getDatanodeByName("h2:5020")==dataNodes[1]);
    assertTrue(map.getDatanodeByName("h2:5030")==null);
    assertTrue(map.getDatanodeByName("h3:5020")==dataNodes[2]);
    assertTrue(map.getDatanodeByName("h3:5030")==dataNodes[3]);
    assertTrue(map.getDatanodeByName("h3:5040")==null);
    assertTrue(map.getDatanodeByName("h4")==null);
    assertTrue(map.getDatanodeByName(null)==null);
  }

  public void testRemove() throws Exception {
    assertFalse(map.remove(NODE));
    
    assertTrue(map.remove(dataNodes[0]));
    assertTrue(map.getDatanodeByHost("h1")==null);
    assertTrue(map.getDatanodeByHost("h2")==dataNodes[1]);
    DatanodeDescriptor node = map.getDatanodeByHost("h3");
    assertTrue(node==dataNodes[2] || node==dataNodes[3]);
    assertTrue(null==map.getDatanodeByHost("h4"));
    
    assertTrue(map.remove(dataNodes[2]));
    assertTrue(map.getDatanodeByHost("h1")==null);
    assertTrue(map.getDatanodeByHost("h2")==dataNodes[1]);
    assertTrue(map.getDatanodeByHost("h3")==dataNodes[3]);
    
    assertTrue(map.remove(dataNodes[3]));
    assertTrue(map.getDatanodeByHost("h1")==null);
    assertTrue(map.getDatanodeByHost("h2")==dataNodes[1]);
    assertTrue(map.getDatanodeByHost("h3")==null);
    
    assertFalse(map.remove(NULL_NODE));
    assertTrue(map.remove(dataNodes[1]));
    assertFalse(map.remove(dataNodes[1]));
  }

}
