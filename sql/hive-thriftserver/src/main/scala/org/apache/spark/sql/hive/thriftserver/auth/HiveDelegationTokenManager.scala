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

package org.apache.spark.sql.hive.thriftserver.auth

import java.io.IOException
import java.security.PrivilegedExceptionAction

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.shims.Utils
import org.apache.hadoop.hive.thrift._
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge.Server
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.authorize.ProxyUsers
import org.apache.hadoop.util.ReflectionUtils

object HiveDelegationTokenManager {
  val DELEGATION_TOKEN_GC_INTERVAL = "hive.cluster.delegation.token.gc-interval"
  val DELEGATION_TOKEN_GC_INTERVAL_DEFAULT = 3600000 // 1 hour

  // Delegation token related keys
  val DELEGATION_KEY_UPDATE_INTERVAL_KEY = "hive.cluster.delegation.key.update-interval"
  val DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT: Long = 24 * 60 * 60 * 1000 // 1 day

  val DELEGATION_TOKEN_RENEW_INTERVAL_KEY = "hive.cluster.delegation.token.renew-interval"
  val DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT: Long = 24 * 60 * 60 * 1000
  val DELEGATION_TOKEN_MAX_LIFETIME_KEY = "hive.cluster.delegation.token.max-lifetime"
  val DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT: Long = 7 * 24 * 60 * 60 * 1000 // 7 days

  val DELEGATION_TOKEN_STORE_CLS = "hive.cluster.delegation.token.store.class"
  val DELEGATION_TOKEN_STORE_ZK_CONNECT_STR =
    "hive.cluster.delegation.token.store.zookeeper.connectString"
  // Alternate connect string specification configuration
  val DELEGATION_TOKEN_STORE_ZK_CONNECT_STR_ALTERNATE = "hive.zookeeper.quorum"

  val DELEGATION_TOKEN_STORE_ZK_CONNECT_TIMEOUTMILLIS =
    "hive.cluster.delegation.token.store.zookeeper.connectTimeoutMillis"
  val DELEGATION_TOKEN_STORE_ZK_ZNODE = "hive.cluster.delegation.token.store.zookeeper.znode"
  val DELEGATION_TOKEN_STORE_ZK_ACL = "hive.cluster.delegation.token.store.zookeeper.acl"
  val DELEGATION_TOKEN_STORE_ZK_ZNODE_DEFAULT = "/hivedelegation"
}

class HiveDelegationTokenManager {

  import HiveDelegationTokenManager._

  protected var secretManager: DelegationTokenSecretManager = null

  def getSecretManager: DelegationTokenSecretManager = secretManager

  @throws[IOException]
  def startDelegationTokenSecretManager(conf: HiveConf,
                                        hms: Any,
                                        smode: Server.ServerMode): Unit = {
    val secretKeyInterval =
      conf.getLong(DELEGATION_KEY_UPDATE_INTERVAL_KEY,
        DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT)
    val tokenMaxLifetime =
      conf.getLong(DELEGATION_TOKEN_MAX_LIFETIME_KEY,
        DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT)
    val tokenRenewInterval =
      conf.getLong(DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
        DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT)
    val tokenGcInterval =
      conf.getLong(DELEGATION_TOKEN_GC_INTERVAL,
        DELEGATION_TOKEN_GC_INTERVAL_DEFAULT)
    val dts: DelegationTokenStore = getTokenStore(conf)
    dts.setConf(conf)
    dts.init(hms, smode)
    secretManager =
      new TokenStoreDelegationTokenSecretManager(
        secretKeyInterval,
        tokenMaxLifetime,
        tokenRenewInterval,
        tokenGcInterval,
        dts)
    secretManager.startThreads
  }

  @throws[IOException]
  @throws[InterruptedException]
  def getDelegationToken(owner: String, renewer: String, remoteAddr: String): String = {
    /**
     * If the user asking the token is same as the 'owner' then don't do
     * any proxy authorization checks. For cases like oozie, where it gets
     * a delegation token for another user, we need to make sure oozie is
     * authorized to get a delegation token.
     */
    // Do all checks on short names
    val currUser = UserGroupInformation.getCurrentUser
    var ownerUgi = UserGroupInformation.createRemoteUser(owner)
    if (!(ownerUgi.getShortUserName == currUser.getShortUserName)) {
      // in the case of proxy users, the getCurrentUser will return the
      // real user (for e.g. oozie) due to the doAs that happened just before the
      // server started executing the method getDelegationToken in the MetaStore
      ownerUgi = UserGroupInformation.createProxyUser(owner, UserGroupInformation.getCurrentUser)
      ProxyUsers.authorize(ownerUgi, remoteAddr, null)
    }
    ownerUgi.doAs(new PrivilegedExceptionAction[String]() {
      @throws[IOException]
      override def run: String = return secretManager.getDelegationToken(renewer)
    })
  }

  @throws[IOException]
  @throws[InterruptedException]
  def getDelegationTokenWithService(owner: String,
                                    renewer: String,
                                    service: String,
                                    remoteAddr: String): String = {
    val token = getDelegationToken(owner, renewer, remoteAddr)
    Utils.addServiceToToken(token, service)
  }

  @throws[IOException]
  def renewDelegationToken(tokenStrForm: String): Long =
    secretManager.renewDelegationToken(tokenStrForm)

  @throws[IOException]
  def getUserFromToken(tokenStr: String): String = secretManager.getUserFromToken(tokenStr)

  @throws[IOException]
  def cancelDelegationToken(tokenStrForm: String): Unit = {
    secretManager.cancelDelegationToken(tokenStrForm)
  }

  @throws[IOException]
  private def getTokenStore(conf: Configuration): DelegationTokenStore = {
    val tokenStoreClassName = conf.get(DELEGATION_TOKEN_STORE_CLS, "")
    if (StringUtils.isBlank(tokenStoreClassName)) {
      return new MemoryTokenStore
    }
    try {
      val storeClass = org.apache.spark.util.Utils.classForName(tokenStoreClassName)
        .asSubclass(classOf[DelegationTokenStore])
      ReflectionUtils.newInstance(storeClass, conf)
    } catch {
      case e: ClassNotFoundException =>
        throw new IOException("Error initializing delegation token store: " +
          tokenStoreClassName, e)
    }
  }

}