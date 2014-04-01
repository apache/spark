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
 * The MXBean interface for JobTrackerInfo
 */
public interface JobTrackerMXBean {

  /**
   * @return hostname of the jobtracker
   */
  String getHostname();

  /**
   * @return version of the code base
   */
  String getVersion();

  /**
   * @return the config version (from a config property)
   */
  String getConfigVersion();

  /**
   * @return number of threads of the jobtracker jvm
   */
  int getThreadCount();

  /**
   * @return the summary info in json
   */
  String getSummaryJson();

  /**
   * @return the alive nodes info in json
   */
  String getAliveNodesInfoJson();

  /**
   * @return the blacklisted nodes info in json
   */
  String getBlacklistedNodesInfoJson();

  /**
   * @return the queue info json
   */
  String getQueueInfoJson();

}
