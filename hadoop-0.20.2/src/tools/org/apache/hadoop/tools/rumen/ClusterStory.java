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

import java.util.Set;

/**
 * {@link ClusterStory} represents all configurations of a MapReduce cluster,
 * including nodes, network topology, and slot configurations.
 */
public interface ClusterStory {
  /**
   * Get all machines of the cluster.
   * @return A read-only set that contains all machines of the cluster.
   */
  public Set<MachineNode> getMachines();

  /**
   * Get all racks of the cluster.
   * @return A read-only set that contains all racks of the cluster.
   */
  public Set<RackNode> getRacks();
  
  /**
   * Get the cluster topology tree.
   * @return The root node of the cluster topology tree.
   */
  public Node getClusterTopology();
  
  /**
   * Select a random set of machines.
   * @param expected The expected sample size.
   * @return An array of up to expected number of {@link MachineNode}s.
   */
  public MachineNode[] getRandomMachines(int expected);

  /**
   * Get {@link MachineNode} by its host name.
   * 
   * @return The {@line MachineNode} with the same name. Or null if not found.
   */
  public MachineNode getMachineByName(String name);
  
  /**
   * Get {@link RackNode} by its name.
   * @return The {@line RackNode} with the same name. Or null if not found.
   */
  public RackNode getRackByName(String name);

  /**
   * Determine the distance between two {@link Node}s. Currently, the distance
   * is loosely defined as the length of the longer path for either a or b to
   * reach their common ancestor.
   * 
   * @param a
   * @param b
   * @return The distance between {@link Node} a and {@link Node} b.
   */
  int distance(Node a, Node b);
  
  /**
   * Get the maximum distance possible between any two nodes.
   * @return the maximum distance possible between any two nodes.
   */
  int getMaximumDistance();
}
