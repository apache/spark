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

import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * {@link Node} represents a node in the cluster topology. A node can be a
 * {@MachineNode}, or a {@link RackNode}, etc.
 */
public class Node implements Comparable<Node> {
  private static final SortedSet<Node> EMPTY_SET = 
    Collections.unmodifiableSortedSet(new TreeSet<Node>());
  private Node parent;
  private final String name;
  private final int level;
  private SortedSet<Node> children;

  /**
   * @param name
   *          A unique name to identify a node in the cluster.
   * @param level
   *          The level of the node in the cluster
   */
  public Node(String name, int level) {
    if (name == null) {
      throw new IllegalArgumentException("Node name cannot be null");
    }

    if (level < 0) {
      throw new IllegalArgumentException("Level cannot be negative");
    }

    this.name = name;
    this.level = level;
  }

  /**
   * Get the name of the node.
   * 
   * @return The name of the node.
   */
  public String getName() {
    return name;
  }

  /**
   * Get the level of the node.
   * @return The level of the node.
   */
  public int getLevel() {
    return level;
  }
  
  private void checkChildren() {
    if (children == null) {
      children = new TreeSet<Node>();
    }
  }

  /**
   * Add a child node to this node.
   * @param child The child node to be added. The child node should currently not be belong to another cluster topology.
   * @return Boolean indicating whether the node is successfully added.
   */
  public synchronized boolean addChild(Node child) {
    if (child.parent != null) {
      throw new IllegalArgumentException(
          "The child is already under another node:" + child.parent);
    }
    checkChildren();
    boolean retval = children.add(child);
    if (retval) child.parent = this;
    return retval;
  }

  /**
   * Does this node have any children?
   * @return Boolean indicate whether this node has any children.
   */
  public synchronized boolean hasChildren() {
    return children != null && !children.isEmpty();
  }

  /**
   * Get the children of this node.
   * 
   * @return The children of this node. If no child, an empty set will be
   *         returned. The returned set is read-only.
   */
  public synchronized Set<Node> getChildren() {
    return (children == null) ? EMPTY_SET : 
      Collections.unmodifiableSortedSet(children);
  }
  
  /**
   * Get the parent node.
   * @return the parent node. If root node, return null.
   */
  public Node getParent() {
    return parent;
  }
  
  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (obj.getClass() != this.getClass())
      return false;
    Node other = (Node) obj;
    return name.equals(other.name);
  }
  
  @Override
  public String toString() {
    return "(" + name +", " + level +")";
  }

  @Override
  public int compareTo(Node o) {
    return name.compareTo(o.name);
  }
}
