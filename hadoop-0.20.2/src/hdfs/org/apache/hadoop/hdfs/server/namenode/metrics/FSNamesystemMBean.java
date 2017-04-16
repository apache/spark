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
package org.apache.hadoop.hdfs.server.namenode.metrics;

/**
 * 
 * This Interface defines the methods to get the status of a the FSNamesystem of
 * a name node.
 * It is also used for publishing via JMX (hence we follow the JMX naming
 * convention.)
 * 
 * Note we have not used the MetricsDynamicMBeanBase to implement this
 * because the interface for the NameNodeStateMBean is stable and should
 * be published as an interface.
 * 
 * <p>
 * Name Node runtime activity statistic  info is report in another MBean
 * @see org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeActivityMBean
 *
 */
public interface FSNamesystemMBean {

  /**
   * The state of the file system: Safemode or Operational
   * @return the state
   */
  public String getFSState();
  
  
  /**
   * Number of allocated blocks in the system
   * @return -  number of allocated blocks
   */
  public long getBlocksTotal();

  /**
   * Total storage capacity
   * @return -  total capacity in bytes
   */
  public long getCapacityTotal();


  /**
   * Free (unused) storage capacity
   * @return -  free capacity in bytes
   */
  public long getCapacityRemaining();
 
  /**
   * Used storage capacity
   * @return -  used capacity in bytes
   */
  public long getCapacityUsed();
 

  /**
   * Total number of files and directories
   * @return -  num of files and directories
   */
  public long getFilesTotal();
 
  /**
   * Blocks pending to be replicated
   * @return -  num of blocks to be replicated
   */
  public long getPendingReplicationBlocks();
 
  /**
   * Blocks under replicated 
   * @return -  num of blocks under replicated
   */
  public long getUnderReplicatedBlocks();
 
  /**
   * Blocks scheduled for replication
   * @return -  num of blocks scheduled for replication
   */
  public long getScheduledReplicationBlocks();

  /**
   * Total Load on the FSNamesystem
   * @return -  total load of FSNamesystem
   */
  public int getTotalLoad();

  /**
   * Number of Live data nodes
   * @return number of live data nodes
   */
  public int numLiveDataNodes();
  
  /**
   * Number of dead data nodes
   * @return number of dead data nodes
   */
  public int numDeadDataNodes();
}
