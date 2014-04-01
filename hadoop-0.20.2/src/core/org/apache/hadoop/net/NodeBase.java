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
package org.apache.hadoop.net;

/** A base class that implements interface Node
 * 
 */

public class NodeBase implements Node {
  public final static char PATH_SEPARATOR = '/';
  public final static String PATH_SEPARATOR_STR = "/";
  public final static String ROOT = ""; // string representation of root
  
  protected String name; //host:port#
  protected String location; //string representation of this node's location
  protected int level; //which level of the tree the node resides
  protected Node parent; //its parent
  
  /** Default constructor */
  public NodeBase() {
  }
  
  /** Construct a node from its path
   * @param path 
   *   a concatenation of this node's location, the path seperator, and its name 
   */
  public NodeBase(String path) {
    path = normalize(path);
    int index = path.lastIndexOf(PATH_SEPARATOR);
    if (index== -1) {
      set(ROOT, path);
    } else {
      set(path.substring(index+1), path.substring(0, index));
    }
  }
  
  /** Construct a node from its name and its location
   * @param name this node's name 
   * @param location this node's location 
   */
  public NodeBase(String name, String location) {
    set(name, normalize(location));
  }
  
  /** Construct a node from its name and its location
   * @param name this node's name 
   * @param location this node's location 
   * @param parent this node's parent node
   * @param level this node's level in the tree
   */
  public NodeBase(String name, String location, Node parent, int level) {
    set(name, normalize(location));
    this.parent = parent;
    this.level = level;
  }

  /* set this node's name and location */
  private void set(String name, String location) {
    if (name != null && name.contains(PATH_SEPARATOR_STR))
      throw new IllegalArgumentException(
                                         "Network location name contains /: "+name);
    this.name = (name==null)?"":name;
    this.location = location;      
  }
  
  /** Return this node's name */
  public String getName() { return name; }
  
  /** Return this node's network location */
  public String getNetworkLocation() { return location; }
  
  /** Set this node's network location */
  public void setNetworkLocation(String location) { this.location = location; }
  
  /** Return this node's path */
  public static String getPath(Node node) {
    return node.getNetworkLocation()+PATH_SEPARATOR_STR+node.getName();
  }
  
  /** Return this node's string representation */
  public String toString() {
    return getPath(this);
  }

  /** Normalize a path */
  static public String normalize(String path) {
    if (path == null || path.length() == 0) return ROOT;
    
    if (path.charAt(0) != PATH_SEPARATOR) {
      throw new IllegalArgumentException(
                                         "Network Location path does not start with "
                                         +PATH_SEPARATOR_STR+ ": "+path);
    }
    
    int len = path.length();
    if (path.charAt(len-1) == PATH_SEPARATOR) {
      return path.substring(0, len-1);
    }
    return path;
  }
  
  /** Return this node's parent */
  public Node getParent() { return parent; }
  
  /** Set this node's parent */
  public void setParent(Node parent) {
    this.parent = parent;
  }
  
  /** Return this node's level in the tree.
   * E.g. the root of a tree returns 0 and its children return 1
   */
  public int getLevel() { return level; }
  
  /** Set this node's level in the tree */
  public void setLevel(int level) {
    this.level = level;
  }
}
