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

package org.apache.hadoop.test.system.process;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

/**
 * Interface to manage the remote processes in the cluster.
 */
public interface ClusterProcessManager {

  /**
   * Initialization method to pass the configuration object which is required 
   * by the ClusterProcessManager to manage the cluster.<br/>
   * Configuration object should typically contain all the parameters which are 
   * required by the implementations.<br/>
   *  
   * @param conf configuration containing values of the specific keys which 
   * are required by the implementation of the cluster process manger.
   * 
   * @throws IOException when initialization fails.
   */
  void init(Configuration conf) throws IOException;

  /**
   * Get the list of RemoteProcess handles of all the remote processes.
   */
  List<RemoteProcess> getAllProcesses();
  
  /**
   * Get a RemoteProcess given the hostname
   * @param The hostname for which the object instance needs to be obtained.
   * @param The role of process example are JT,TT,DN,NN
   */
  RemoteProcess getDaemonProcess(String hostname, Enum<?> role);

  /**
   * Get all the roles this cluster's daemon processes have.
   */
  Set<Enum<?>> getRoles();

  /**
   * Method to start all the remote daemons.<br/>
   * 
   * @throws IOException if startup procedure fails.
   */
  void start() throws IOException;

  /**
   * Starts the daemon from the user specified conf dir.
   * @param newConfLocation the dir where the new conf files reside.
   * @throws IOException if start from new conf fails. 
   */
  void start(String newConfLocation) throws IOException;

  /**
   * Stops the daemon running from user specified conf dir.
   * 
   * @param newConfLocation the dir where the new conf files reside.
   * @throws IOException if stop from new conf fails. 
   */
  void stop(String newConfLocation) throws IOException;

  /**
   * Method to shutdown all the remote daemons.<br/>
   * 
   * @throws IOException if shutdown procedure fails.
   */
  void stop() throws IOException;
  
  /**
   * Gets if multi-user support is enabled for this cluster. 
   * <br/>
   * @return true if multi-user support is enabled.
   * @throws IOException if RPC returns error. 
   */
  boolean isMultiUserSupported() throws IOException;

  /**
   * The pushConfig is used to push a new config to the daemons.
   * @param localDir
   * @return is the remoteDir location where config will be pushed
   * @throws IOException if pushConfig fails.
   */
  String pushConfig(String localDir) throws IOException;
}
