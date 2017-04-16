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
package org.apache.hadoop.hdfs.server.datanode;

/**
 * 
 * This is the JMX management interface for data node information
 */
public interface DataNodeMXBean {
  
  /**
   * @return the host name
   */
  public String getHostName();
  
  /**
   * Gets the version of Hadoop.
   * 
   * @return the version of Hadoop
   */
  public String getVersion();
  
  /**
   * Gets the rpc port.
   * 
   * @return the rpc port
   */
  public String getRpcPort();
  
  /**
   * Gets the http port.
   * 
   * @return the http port
   */
  public String getHttpPort();
  
  /**
   * Gets the namenode IP address.
   * 
   * @return the namenode IP address
   */
  public String getNamenodeAddress();
  
  /**
   * Gets the information of each volume on the Datanode. Please
   * see the implementation for the format of returned information.
   * 
   * @return the volume info
   */
  public String getVolumeInfo();
}
