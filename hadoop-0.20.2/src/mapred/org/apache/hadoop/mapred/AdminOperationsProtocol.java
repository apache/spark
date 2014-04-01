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

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.KerberosInfo;

/**
 * Protocol for admin operations. This is a framework-public interface and is
 * NOT_TO_BE_USED_BY_USERS_DIRECTLY.
 */
@KerberosInfo(
    serverPrincipal = JobTracker.JT_USER_NAME)
public interface AdminOperationsProtocol extends VersionedProtocol {
  
  /**
   * Version 1: Initial version. Added refreshQueueAcls.
   * Version 2: Added node refresh facility
   * Version 3: Changed refreshQueueAcls to refreshQueues
   */
  public static final long versionID = 3L;

  /**
   * Refresh the queue acls in use currently.
   * Refresh the queues used by the jobtracker and scheduler.
   *
   * Access control lists and queue states are refreshed.
   */
  void refreshQueues() throws IOException;
  
  /**
   * Refresh the node list at the {@link JobTracker} 
   */
  void refreshNodes() throws IOException;
}
