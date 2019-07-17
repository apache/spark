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

package org.apache.spark.sql.hive.thriftserver

import java.io.{IOException, PrintStream}
import java.security.PrivilegedExceptionAction
import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.metadata.{Hive, HiveException}
import org.apache.hadoop.hive.shims.Utils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.auth.HiveAuthFactory
import org.apache.hive.service.cli.HiveSQLException
import org.apache.hive.service.cli.session.HiveSession
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.thriftserver.SparkSQLEnv.sparkContext
import org.apache.spark.sql.hive.{HiveExternalCatalog, HiveUtils}

class SparkSessionManager extends Logging {
  private val STS_TOKEN = "SparkThriftServer2ImpersonationToken"
  private val LOCK = new Object()
  private val cachedSession = new ConcurrentHashMap[String, SparkSession]()
  private val hiveConf = new HiveConf()

  def getDelegationToken(userName: String): String = {
    if (userName != null &&
      hiveConf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION).equalsIgnoreCase(HiveAuthFactory.AuthTypes.KERBEROS.toString) &&
      hiveConf.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL) &&
      hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS)) {
      try {
        Hive.closeCurrent()
        Hive.set(null)
        Hive.get(hiveConf).getDelegationToken(userName, userName)
      } catch {
        case e: HiveException =>
          if (e.getCause.isInstanceOf[UnsupportedOperationException]) throw e.getCause.asInstanceOf[UnsupportedOperationException]
          else throw new HiveSQLException("Error connect metastore to setup impersonation", e)
      }
    } else {
      null
    }
  }


  def getOrCreteSparkSession(session: HiveSession, withImpersonation: Boolean): SparkSession = LOCK.synchronized {
    if (cachedSession.containsKey(session.getUserName)) {
      cachedSession.get(session.getUserName).newSession()
    } else {
      val ugi = if (withImpersonation) {
        if (session.getUserName == null) throw new HiveSQLException("No username provided for impersonation")
        if (UserGroupInformation.isSecurityEnabled) try
          UserGroupInformation.createProxyUser(session.getUserName, UserGroupInformation.getLoginUser)
        catch {
          case e: IOException =>
            throw new HiveSQLException("Couldn't setup proxy user", e)
        }
        else {
          UserGroupInformation.createRemoteUser(session.getUserName)
        }
      } else {
        UserGroupInformation.getLoginUser
      }

      logInfo(s"Current Username => ${session.getUserName}")
      logInfo(s"Create SparkSession with UGI => ${ugi}")
      sparkContext.conf.set("hive.metastore.token.signature", STS_TOKEN)
      try {
        val delegationToken = getDelegationToken(session.getUserName)
        logInfo(s"Get DelegationToken => ${delegationToken}")
        Utils.setTokenStr(ugi, delegationToken, STS_TOKEN)
      } catch {
        case e: IOException =>
          throw new HiveSQLException("Couldn't setup delegation token in the ugi", e)
      }

      val sparkSession = ugi.doAs(new PrivilegedExceptionAction[SparkSession] {
        override def run(): SparkSession = {
          Hive.closeCurrent()
          val sessionForSpecUser = new SparkSession(sparkContext, SparkSQLEnv.extension)
          sessionForSpecUser.catalog
          sessionForSpecUser.sessionState.catalog
          val metadataHive = sessionForSpecUser
            .sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client
          metadataHive.setOut(new PrintStream(System.out, true, "UTF-8"))
          metadataHive.setInfo(new PrintStream(System.err, true, "UTF-8"))
          metadataHive.setError(new PrintStream(System.err, true, "UTF-8"))
          sessionForSpecUser.conf.set(HiveUtils.FAKE_HIVE_VERSION.key, HiveUtils.builtinHiveVersion)
          sessionForSpecUser
        }
      })

      cachedSession.put(session.getUserName, sparkSession)
      sparkSession
    }

  }
}