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
package org.apache.hadoop.tools.rumen;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * {@link AbstractClusterStory} provides a partial implementation of
 * {@link ClusterStory} by parsing the topology tree.
 */
public abstract class AbstractClusterStory implements ClusterStory {
  protected Set<MachineNode> machineNodes;
  protected Set<RackNode> rackNodes;
  protected MachineNode[] mNodesFlattened;
  protected Map<String, MachineNode> mNodeMap;
  protected Map<String, RackNode> rNodeMap;
  protected int maximumDistance = 0;
  protected Random random;
  
  @Override
  public Set<MachineNode> getMachines() {
    parseTopologyTree();
    return machineNodes;
  }
  
  @Override
  public synchronized Set<RackNode> getRacks() {
    parseTopologyTree();    
    return rackNodes;
  }
  
  @Override
  public synchronized MachineNode[] getRandomMachines(int expected) {
    if (expected == 0) {
      return new MachineNode[0];
    }

    parseTopologyTree();
    int total = machineNodes.size();
    int select = Math.min(expected, total);

    if (mNodesFlattened == null) {
      mNodesFlattened = machineNodes.toArray(new MachineNode[total]);
      random = new Random();
    }

    MachineNode[] retval = new MachineNode[select];
    int i = 0;
    while ((i != select) && (total != i + select)) {
      int index = random.nextInt(total - i);
      MachineNode tmp = mNodesFlattened[index];
      mNodesFlattened[index] = mNodesFlattened[total - i - 1];
      mNodesFlattened[total - i - 1] = tmp;
      ++i;
    }
    if (i == select) {
      System.arraycopy(mNodesFlattened, total - i, retval, 0, select);
    } else {
      System.arraycopy(mNodesFlattened, 0, retval, 0, select);
    }

    return retval;
  }
  
  protected synchronized void buildMachineNodeMap() {
    if (mNodeMap == null) {
      mNodeMap = new HashMap<String, MachineNode>(machineNodes.size());
      for (MachineNode mn : machineNodes) {
        mNodeMap.put(mn.getName(), mn);
      }
    }
  }
  
  @Override
  public MachineNode getMachineByName(String name) {
    buildMachineNodeMap();
    return mNodeMap.get(name);
  }
  
  @Override
  public int distance(Node a, Node b) {
    int lvl_a = a.getLevel();
    int lvl_b = b.getLevel();
    int retval = 0;
    if (lvl_a > lvl_b) {
      retval = lvl_a-lvl_b;
      for (int i=0; i<retval; ++i) {
        a = a.getParent();
      }
    } else if (lvl_a < lvl_b) {
      retval = lvl_b-lvl_a;
      for (int i=0; i<retval; ++i) {
        b = b.getParent();
      }      
    }
    
    while (a != b) {
      a = a.getParent();
      b = b.getParent();
      ++retval;
    }
    
    return retval;
  }
  
  protected synchronized void buildRackNodeMap() {
    if (rNodeMap == null) {
      rNodeMap = new HashMap<String, RackNode>(rackNodes.size());
      for (RackNode rn : rackNodes) {
        rNodeMap.put(rn.getName(), rn);
      }
    }
  }
  
  @Override
  public RackNode getRackByName(String name) {
    buildRackNodeMap();
    return rNodeMap.get(name);
  }
  
  @Override
  public int getMaximumDistance() {
    parseTopologyTree();
    return maximumDistance;
  }
  
  protected synchronized void parseTopologyTree() {
    if (machineNodes == null) {
      Node root = getClusterTopology();
      SortedSet<MachineNode> mNodes = new TreeSet<MachineNode>();
      SortedSet<RackNode> rNodes = new TreeSet<RackNode>();
      // dfs search of the tree.
      Deque<Node> unvisited = new ArrayDeque<Node>();
      Deque<Integer> distUnvisited = new ArrayDeque<Integer>();
      unvisited.add(root);
      distUnvisited.add(0);
      for (Node n = unvisited.poll(); n != null; n = unvisited.poll()) {
        int distance = distUnvisited.poll();
        if (n instanceof RackNode) {
          rNodes.add((RackNode) n);
          mNodes.addAll(((RackNode) n).getMachinesInRack());
          if (distance + 1 > maximumDistance) {
            maximumDistance = distance + 1;
          }
        } else if (n instanceof MachineNode) {
          mNodes.add((MachineNode) n);
          if (distance > maximumDistance) {
            maximumDistance = distance;
          }
        } else {
          for (Node child : n.getChildren()) {
            unvisited.addFirst(child);
            distUnvisited.addFirst(distance+1);
          }
        }
      }

      machineNodes = Collections.unmodifiableSortedSet(mNodes);
      rackNodes = Collections.unmodifiableSortedSet(rNodes);
    }
  }
}
