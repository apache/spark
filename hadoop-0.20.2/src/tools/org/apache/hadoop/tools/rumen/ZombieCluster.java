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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * {@link ZombieCluster} rebuilds the cluster topology using the information
 * obtained from job history logs.
 */
public class ZombieCluster extends AbstractClusterStory {
  private Node root;

  /**
   * Construct a homogeneous cluster. We assume that the leaves on the topology
   * are {@link MachineNode}s, and the parents of {@link MachineNode}s are
   * {@link RackNode}s. We also expect all leaf nodes are on the same level.
   * 
   * @param topology
   *          The network topology.
   * @param defaultNode
   *          The default node setting.
   */
  ZombieCluster(LoggedNetworkTopology topology, MachineNode defaultNode) {
    buildCluster(topology, defaultNode);
  }

  /**
   * Construct a homogeneous cluster. We assume that the leaves on the topology
   * are {@link MachineNode}s, and the parents of {@link MachineNode}s are
   * {@link RackNode}s. We also expect all leaf nodes are on the same level.
   * 
   * @param path Path to the JSON-encoded topology file.
   * @param conf
   * @param defaultNode
   *          The default node setting.
   * @throws IOException 
   */
  public ZombieCluster(Path path, MachineNode defaultNode, Configuration conf) throws IOException {
    this(new ClusterTopologyReader(path, conf).get(), defaultNode);
  }
  
  /**
   * Construct a homogeneous cluster. We assume that the leaves on the topology
   * are {@link MachineNode}s, and the parents of {@link MachineNode}s are
   * {@link RackNode}s. We also expect all leaf nodes are on the same level.
   * 
   * @param input The input stream for the JSON-encoded topology file.
   * @param defaultNode
   *          The default node setting.
   * @throws IOException 
   */
  public ZombieCluster(InputStream input, MachineNode defaultNode) throws IOException {
    this(new ClusterTopologyReader(input).get(), defaultNode);
  }

  @Override
  public Node getClusterTopology() {
    return root;
  }

  private final void buildCluster(LoggedNetworkTopology topology,
      MachineNode defaultNode) {
    Map<LoggedNetworkTopology, Integer> levelMapping = 
      new IdentityHashMap<LoggedNetworkTopology, Integer>();
    Deque<LoggedNetworkTopology> unvisited = 
      new ArrayDeque<LoggedNetworkTopology>();
    unvisited.add(topology);
    levelMapping.put(topology, 0);
    
    // building levelMapping and determine leafLevel
    int leafLevel = -1; // -1 means leafLevel unknown.
    for (LoggedNetworkTopology n = unvisited.poll(); n != null; 
      n = unvisited.poll()) {
      int level = levelMapping.get(n);
      List<LoggedNetworkTopology> children = n.getChildren();
      if (children == null || children.isEmpty()) {
        if (leafLevel == -1) {
          leafLevel = level;
        } else if (leafLevel != level) {
          throw new IllegalArgumentException(
              "Leaf nodes are not on the same level");
        }
      } else {
        for (LoggedNetworkTopology child : children) {
          levelMapping.put(child, level + 1);
          unvisited.addFirst(child);
        }
      }
    }

    /**
     * A second-pass dfs traverse of topology tree. path[i] contains the parent
     * of the node at level i+1.
     */
    Node[] path = new Node[leafLevel];
    unvisited.add(topology);
    for (LoggedNetworkTopology n = unvisited.poll(); n != null; 
      n = unvisited.poll()) {
      int level = levelMapping.get(n);
      Node current;
      if (level == leafLevel) { // a machine node
        MachineNode.Builder builder = new MachineNode.Builder(n.getName(), level);
        if (defaultNode != null) {
          builder.cloneFrom(defaultNode);
        }
        current = builder.build();
      } else {
        current = (level == leafLevel - 1) 
          ? new RackNode(n.getName(), level) : 
            new Node(n.getName(), level);
        path[level] = current;
        // Add all children to the front of the queue.
        for (LoggedNetworkTopology child : n.getChildren()) {
          unvisited.addFirst(child);
        }
      }
      if (level != 0) {
        path[level - 1].addChild(current);
      }
    }

    root = path[0];
  }
}
