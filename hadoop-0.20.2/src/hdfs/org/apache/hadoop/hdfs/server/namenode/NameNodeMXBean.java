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

/**
 * 
 * This is the JMX management interface for namenode information
 */
public interface NameNodeMXBean {

  /**
   * @return the host name
   */
  public String getHostName();
  
  /**
   * Gets the version of Hadoop.
   * 
   * @return the version
   */
  public String getVersion();
  
  /**
   * Gets the used space by data nodes.
   * 
   * @return the used space by data nodes
   */
  public long getUsed();
  
  /**
   * Gets total non-used raw bytes.
   * 
   * @return total non-used raw bytes
   */
  public long getFree();
  
  /**
   * Gets total raw bytes including non-dfs used space.
   * 
   * @return the total raw bytes including non-dfs used space
   */
  public long getTotal();
  
  /**
   * Gets the safemode status
   * 
   * @return the safemode status
   * 
   */
  public String getSafemode();
  
  /**
   * Checks if upgrade is finalized.
   * 
   * @return true, if upgrade is finalized
   */
  public boolean isUpgradeFinalized();
  
  /**
   * Gets total used space by data nodes for non DFS purposes such as storing
   * temporary files on the local file system
   * 
   * @return the non dfs space of the cluster
   */
  public long getNonDfsUsedSpace();
  
  /**
   * Gets the total used space by data nodes as percentage of total capacity
   * 
   * @return the percentage of used space on the cluster.
   */
  public float getPercentUsed();
  
  /**
   * Gets the total remaining space by data nodes as percentage of total 
   * capacity
   * 
   * @return the percentage of the remaining space on the cluster
   */
  public float getPercentRemaining();
  
  /**
   * Gets the total numbers of blocks on the cluster.
   * 
   * @return the total number of blocks of the cluster
   */
  public long getTotalBlocks();
  
  /**
   * Gets the total number of files on the cluster
   * 
   * @return the total number of files on the cluster
   */
  public long getTotalFiles();
  
  /**
   * Gets the number of threads.
   * 
   * @return the number of threads
   */
  public int getThreads();

  /**
   * Gets the live node information of the cluster.
   * 
   * @return the live node information
   */
  public String getLiveNodes();
  
  /**
   * Gets the dead node information of the cluster.
   * 
   * @return the dead node information
   */
  public String getDeadNodes();
  
  /**
   * Gets the decommissioning node information of the cluster.
   * 
   * @return the decommissioning node information
   */
  public String getDecomNodes();

  /**
   * Get status information about the directories storing image and edits logs
   * of the NN.
   * 
   * @return the name dir status information, as a JSON string.
   */
  public String getNameDirStatuses();
}
