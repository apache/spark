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

package org.apache.hive.service.cli.session;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.hive.service.cli.thrift.TProtocolVersion;

import java.io.File;

/**
 * Methods that don't need to be executed under a doAs
 * context are here. Rest of them in HiveSession interface
 */
public interface HiveSessionBase {

  TProtocolVersion getProtocolVersion();

  /**
   * Set the session manager for the session
   * @param sessionManager
   */
  void setSessionManager(SessionManager sessionManager);

  /**
   * Get the session manager for the session
   */
  SessionManager getSessionManager();

  /**
   * Set operation manager for the session
   * @param operationManager
   */
  void setOperationManager(OperationManager operationManager);

  /**
   * Check whether operation logging is enabled and session dir is created successfully
   */
  boolean isOperationLogEnabled();

  /**
   * Get the session dir, which is the parent dir of operation logs
   * @return a file representing the parent directory of operation logs
   */
  File getOperationLogSessionDir();

  /**
   * Set the session dir, which is the parent dir of operation logs
   * @param operationLogRootDir the parent dir of the session dir
   */
  void setOperationLogSessionDir(File operationLogRootDir);

  SessionHandle getSessionHandle();

  String getUsername();

  String getPassword();

  HiveConf getHiveConf();

  SessionState getSessionState();

  String getUserName();

  void setUserName(String userName);

  String getIpAddress();

  void setIpAddress(String ipAddress);

  long getLastAccessTime();
}
