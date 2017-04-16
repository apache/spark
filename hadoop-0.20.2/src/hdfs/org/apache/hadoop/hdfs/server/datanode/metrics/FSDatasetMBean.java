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
package org.apache.hadoop.hdfs.server.datanode.metrics;

import java.io.IOException;

/**
 * 
 * This Interface defines the methods to get the status of a the FSDataset of
 * a data node.
 * It is also used for publishing via JMX (hence we follow the JMX naming
 * convention.) 
 *  * Note we have not used the MetricsDynamicMBeanBase to implement this
 * because the interface for the FSDatasetMBean is stable and should
 * be published as an interface.
 * 
 * <p>
 * Data Node runtime statistic  info is report in another MBean
 * @see org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeStatisticsMBean
 *
 */
public interface FSDatasetMBean {
  
  /**
   * Returns the total space (in bytes) used by dfs datanode
   * @return  the total space used by dfs datanode
   * @throws IOException
   */  
  public long getDfsUsed() throws IOException;
    
  /**
   * Returns total capacity (in bytes) of storage (used and unused)
   * @return  total capacity of storage (used and unused)
   * @throws IOException
   */
  public long getCapacity() throws IOException;

  /**
   * Returns the amount of free storage space (in bytes)
   * @return The amount of free storage space
   * @throws IOException
   */
  public long getRemaining() throws IOException;
  
  /**
   * Returns the storage id of the underlying storage
   */
  public String getStorageInfo();

  /**
   * Returns the number of failed volumes in the datanode.
   * @return The number of failed volumes in the datanode.
   */
  public int getNumFailedVolumes();
}
