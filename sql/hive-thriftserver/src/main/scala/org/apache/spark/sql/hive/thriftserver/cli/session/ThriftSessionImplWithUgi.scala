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

import java.io.IOException

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.{Hive, HiveException}
import org.apache.hadoop.hive.shims.Utils
import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.internal.Logging
import org.apache.spark.service.cli.thrift.TProtocolVersion
import org.apache.spark.sql.hive.thriftserver.auth.HiveAuthFactory
import org.apache.spark.sql.hive.thriftserver.cli.CLIService
import org.apache.spark.sql.hive.thriftserver.server.cli.SparkThriftServerSQLException

class ThriftSessionImplWithUgi(protocol: TProtocolVersion,
                               username: String,
                               password: String,
                               serverHiveConf: HiveConf,
                               ipAddress: String,
                               delegationTokenStr: String)
  extends ThriftSessionImpl(protocol, username, password, serverHiveConf, ipAddress)
    with Logging {

  val HS2TOKEN = "HiveServer2ImpersonationToken"

  private var sessionUgi: UserGroupInformation = null
  private var proxySession: ThriftSession = null

  try {
    setSessionUGI(username)
    setDelegationToken
  } catch {
    case e: Exception => e.printStackTrace()
  }

  // setup appropriate UGI for the session
  @throws[SparkThriftServerSQLException]
  def setSessionUGI(owner: String): Unit = {
    if (owner == null) {
      throw new SparkThriftServerSQLException("No username provided for impersonation")
    }
    if (UserGroupInformation.isSecurityEnabled) {
      try {
        sessionUgi = UserGroupInformation.createProxyUser(owner, UserGroupInformation.getLoginUser)
      } catch {
        case e: IOException =>
          throw new SparkThriftServerSQLException("Couldn't setup proxy user", e)
      }
    } else {
      sessionUgi = UserGroupInformation.createRemoteUser(owner)
    }
  }

  def getSessionUgi: UserGroupInformation = this.sessionUgi

  def getDelegationToken: String = this.delegationTokenStr

  override protected def acquire(userAccess: Boolean): Unit = {
    super.acquire(userAccess)
  }

  /**
   * Close the file systems for the session and remove it from the FileSystem cache.
   * Cancel the session's delegation token and close the metastore connection
   */
  @throws[SparkThriftServerSQLException]
  override def close(): Unit = {
    try {
      acquire(true)
      if (delegationTokenStr != null) {
        CLIService.delegationTokenFetcher.cancelDelegationToken(delegationTokenStr)
      }
    } finally try
      super.close()
    finally try
      FileSystem.closeAllForUGI(sessionUgi)
    catch {
      case ioe: IOException =>
        throw new SparkThriftServerSQLException("Could not clean up file-system " +
          "handles for UGI: " + sessionUgi, ioe)
    }
  }

  /**
   * Enable delegation token for the session
   * save the token string and set the token.signature in hive conf. The metastore client uses
   * this token.signature to determine where to use kerberos or delegation token
   *
   * @throws SparkThriftServerSQLException
   * @throws IOException
   */
  @throws[SparkThriftServerSQLException]
  private def setDelegationToken(): Unit = {
    if (delegationTokenStr != null) {
      getHiveConf.set("hive.metastore.token.signature", HS2TOKEN)
      try {
        Utils.setTokenStr(sessionUgi, delegationTokenStr, HS2TOKEN)
      } catch {
        case e: IOException =>
          throw new SparkThriftServerSQLException("Couldn't setup delegation token in the ugi", e)
      }
    }
  }

  override protected def getSession(): ThriftSession = {
    assert(proxySession != null)
    proxySession
  }

  def setProxySession(proxySession: ThriftSession): Unit = {
    this.proxySession = proxySession
  }

  @throws[SparkThriftServerSQLException]
  override def getDelegationToken(authFactory: HiveAuthFactory,
                                  owner: String,
                                  renewer: String,
                                  remoteAddr: String): String = {
    authFactory.getDelegationToken(owner, renewer, remoteAddr)
  }

  @throws[SparkThriftServerSQLException]
  override def cancelDelegationToken(authFactory: HiveAuthFactory, tokenStr: String): Unit = {
    authFactory.cancelDelegationToken(tokenStr)
  }

  @throws[SparkThriftServerSQLException]
  override def renewDelegationToken(authFactory: HiveAuthFactory, tokenStr: String): Unit = {
    authFactory.renewDelegationToken(tokenStr)
  }

}
