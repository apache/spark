/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.cli.session

import java.io.File

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.service.cli.thrift.TProtocolVersion
import org.apache.spark.sql.hive.thriftserver.cli.SessionHandle
import org.apache.spark.sql.hive.thriftserver.cli.operation.OperationManager

trait ThriftSessionBase {

  def getProtocolVersion: TProtocolVersion

  /**
   * Set the session manager for the session
   *
   * @param sessionManager
   */
  def setSessionManager(sessionManager: SessionManager): Unit

  /**
   * Get the session manager for the session
   */
  def getSessionManager: SessionManager

  /**
   * Set operation manager for the session
   *
   * @param operationManager
   */
  def setOperationManager(operationManager: OperationManager): Unit

  /**
   * Check whether operation logging is enabled and session dir is created successfully
   */
  def isOperationLogEnabled: Boolean

  /**
   * Get the session dir, which is the parent dir of operation logs
   *
   * @return a file representing the parent directory of operation logs
   */
  def getOperationLogSessionDir: File

  /**
   * Set the session dir, which is the parent dir of operation logs
   *
   * @param operationLogRootDir the parent dir of the session dir
   */
  def setOperationLogSessionDir(operationLogRootDir: File): Unit

  def getSessionHandle: SessionHandle

  def getUsername: String

  def getPassword: String

  def getHiveConf: HiveConf

  def getSessionState: SessionState

  def getUserName: String

  def setUserName(userName: String): Unit

  def getIpAddress: String

  def setIpAddress(ipAddress: String): Unit

  def getLastAccessTime: Long
}
