/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.security

import java.lang.reflect.UndeclaredThrowableException
import java.security.PrivilegedExceptionAction

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.Token

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.CLASS_NAME
import org.apache.spark.internal.config.KEYTAB
import org.apache.spark.security.HadoopDelegationTokenProvider
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.util.Utils

private[spark] class HiveDelegationTokenProvider
    extends HadoopDelegationTokenProvider with Logging {

  override def serviceName: String = "hive"

  private val classNotFoundErrorStr =
    log"You are attempting to use the ${MDC(CLASS_NAME, getClass.getCanonicalName)}, " +
      log"but your Spark distribution is not built with Hive libraries."

  private def hiveConf(hadoopConf: Configuration): Configuration = {
    try {
      new HiveConf(hadoopConf, classOf[HiveConf])
    } catch {
      case NonFatal(e) =>
        logWarning("Fail to create Hive Configuration", e)
        hadoopConf
      case e: NoClassDefFoundError =>
        logWarning(classNotFoundErrorStr, e)
        hadoopConf
    }
  }

  override def delegationTokensRequired(
      sparkConf: SparkConf,
      hadoopConf: Configuration): Boolean = {
    // Delegation tokens are needed only when:
    // - trying to connect to a secure metastore
    // - either deploying in cluster mode without a keytab, or impersonating another user
    //
    // Other modes (such as client with or without keytab, or cluster mode with keytab) do not need
    // a delegation token, since there's a valid kerberos TGT for the right user available to the
    // driver, which is the only process that connects to the HMS.
    //
    // Note that this means Hive tokens are not re-created periodically by the token manager.
    // This is because HMS connections are only performed by the Spark driver, and the driver
    // either has a TGT, in which case it does not need tokens, or it has a token created
    // elsewhere, in which case it cannot create new ones. The check for an existing token avoids
    // printing an exception to the logs in the latter case.
    val currentToken = UserGroupInformation.getCurrentUser().getCredentials().getToken(tokenAlias)
    currentToken == null && UserGroupInformation.isSecurityEnabled &&
      hiveConf(hadoopConf).getTrimmed("hive.metastore.uris", "").nonEmpty &&
      (SparkHadoopUtil.get.isProxyUser(UserGroupInformation.getCurrentUser()) ||
        (!Utils.isClientMode(sparkConf) && !sparkConf.contains(KEYTAB)))
  }

  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    try {
      val conf = hiveConf(hadoopConf)

      val principalKey = "hive.metastore.kerberos.principal"
      val principal = conf.getTrimmed(principalKey, "")
      require(principal.nonEmpty, s"Hive principal $principalKey undefined")
      val metastoreUri = conf.getTrimmed("hive.metastore.uris", "")
      require(metastoreUri.nonEmpty, "Hive metastore uri undefined")

      val currentUser = UserGroupInformation.getCurrentUser()
      logDebug(s"Getting Hive delegation token for ${currentUser.getUserName()} against " +
        s"$principal at $metastoreUri")

      doAsRealUser {
        val hive = HiveClientImpl.getHive(conf)
        val tokenStr = hive.getDelegationToken(currentUser.getUserName(), principal)

        val hive2Token = new Token[DelegationTokenIdentifier]()
        hive2Token.decodeFromUrlString(tokenStr)
        logDebug(s"Get Token from hive metastore: ${hive2Token.toString}")
        creds.addToken(tokenAlias, hive2Token)
      }

      None
    } catch {
      case NonFatal(e) =>
        logWarning(Utils.createFailedToGetTokenMessage(serviceName, e))
        None
      case e: NoClassDefFoundError =>
        logWarning(classNotFoundErrorStr)
        None
    } finally {
      Utils.tryLogNonFatalError {
        Hive.closeCurrent()
      }
    }
  }

  /**
   * Run some code as the real logged in user (which may differ from the current user, for
   * example, when using proxying).
   */
  private def doAsRealUser[T](fn: => T): T = {
    val currentUser = UserGroupInformation.getCurrentUser()
    val realUser = Option(currentUser.getRealUser()).getOrElse(currentUser)

    // For some reason the Scala-generated anonymous class ends up causing an
    // UndeclaredThrowableException, even if you annotate the method with @throws.
    try {
      realUser.doAs(new PrivilegedExceptionAction[T]() {
        override def run(): T = fn
      })
    } catch {
      case e: UndeclaredThrowableException => throw Option(e.getCause()).getOrElse(e)
    }
  }

  private def tokenAlias: Text = new Text("hive.server2.delegation.token")
}
