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
package org.apache.hadoop.tools;

import java.io.IOException;

//import org.apache.hadoop.classification.InterfaceAudience;
//import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * Protocol implemented by the Name Node and Job Tracker which maps users to
 * groups.
 */
//@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
//@InterfaceStability.Evolving
public interface GetUserMappingsProtocol extends VersionedProtocol {
  
  /**
   * Version 1: Initial version.
   */
  public static final long versionID = 1L;
  
  /**
   * Get the groups which are mapped to the given user.
   * @param user The user to get the groups for.
   * @return The set of groups the user belongs to.
   * @throws IOException
   */
  public String[] getGroupsForUser(String user) throws IOException;
}