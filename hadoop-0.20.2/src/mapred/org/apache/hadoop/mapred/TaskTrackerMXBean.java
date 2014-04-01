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

package org.apache.hadoop.mapred;

/**
 * MXBean interface for TaskTracker
 */
public interface TaskTrackerMXBean {

  /**
   * @return the hostname of the tasktracker
   */
  String getHostname();

  /**
   * @return the version of the code base
   */
  String getVersion();

  /**
   * @return the config version (from a config properties)
   */
  String getConfigVersion();

  /**
   * @return the URL of the jobtracker
   */
  String getJobTrackerUrl();

  /**
   * @return the RPC port of the tasktracker
   */
  int getRpcPort();

  /**
   * @return the HTTP port of the tasktracker
   */
  int getHttpPort();

  /**
   * @return the health status of the tasktracker
   */
  boolean isHealthy();

  /**
   * @return a json formatted info about tasks of the tasktracker
   */
  String getTasksInfoJson();

}
