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

import java.util.concurrent.Executors

import org.apache.commons.logging.Log
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.security.Credentials
import org.apache.hive.service.cli.{HiveSQLException, SessionHandle}
import org.apache.hive.service.cli.session._
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.hive.service.server.HiveServer2
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._
import org.apache.spark.sql.hive.thriftserver.server.SparkSQLOperationManager


private[hive] class SparkSQLSessionManager(hiveServer: HiveServer2, sqlContext: SQLContext)
  extends SessionManager(hiveServer)
  with ReflectedCompositeService {

  private lazy val sparkSqlOperationManager = new SparkSQLOperationManager()
  private lazy val sparkSessionManager = new SparkSessionManager()
  private val hadoopTokenProvider = new HadoopFSDelegationTokenProvider(SparkSQLEnv.sparkContext.conf, SparkSQLEnv.sparkContext.hadoopConfiguration)

  override def init(hiveConf: HiveConf) {
    setSuperField(this, "operationManager", sparkSqlOperationManager)
    super.init(hiveConf)
  }

  override def openSession(
      protocol: TProtocolVersion,
      username: String,
      passwd: String,
      ipAddress: String,
      sessionConf: java.util.Map[String, String],
      withImpersonation: Boolean,
      delegationToken: String): SessionHandle = {
    var session: HiveSession = null
    var sparkSession: SparkSession = null
    if (withImpersonation) {
      val sessionWithUGI = new HiveSessionImplwithUGI(protocol, username, passwd, hiveConf, ipAddress, delegationToken)
      val ugi = sessionWithUGI.getSessionUgi
      val originalCreds = ugi.getCredentials
      val creds = new Credentials()
      ThriftServerHadoopUtil.doAs(ugi)(() => hadoopTokenProvider.obtainDelegationTokens(creds, username))
      ugi.addCredentials(creds)
      val existing = ugi.getCredentials()
      existing.mergeAll(originalCreds)
      ugi.addCredentials(existing)

      session = HiveSessionProxy.getProxy(sessionWithUGI, sessionWithUGI.getSessionUgi)
      sessionWithUGI.setProxySession(session)
      sparkSession = sparkSessionManager.getOrCreteSparkSession(sessionWithUGI, withImpersonation)
    }
    else {
      session = new HiveSessionImpl(protocol, username, passwd, hiveConf, ipAddress)
      sparkSession = sparkSessionManager.getOrCreteSparkSession(session, withImpersonation)
    }
    val ctx = sparkSession.sqlContext
    session.setSessionManager(this)
    session.setOperationManager(operationManager)
    try
      session.open(sessionConf)
    catch {
      case e: Exception =>
        try
          session.close()
        catch {
          case t: Throwable =>
            SessionManager.LOG.warn("Error closing session", t)
        }
        session = null
        throw new HiveSQLException("Failed to open new session: " + e, e)
    }
    if (isOperationLogEnabled)
      session.setOperationLogSessionDir(operationLogRootDir)
    handleToSession.put(session.getSessionHandle, session)

    val sessionHandle = session.getSessionHandle
    HiveThriftServer2.listener.onSessionCreated(
      session.getIpAddress, sessionHandle.getSessionId.toString, session.getUsername)
    sparkSession.sqlContext.setConf(HiveUtils.FAKE_HIVE_VERSION.key, HiveUtils.builtinHiveVersion)
    if (sessionConf != null && sessionConf.containsKey("use:database")) {
      sparkSession.sql(s"use ${sessionConf.get("use:database")}")
    }
    sparkSqlOperationManager.sessionToContexts.put(sessionHandle, sparkSession.sqlContext)
    sessionHandle
  }

  override def closeSession(sessionHandle: SessionHandle) {
    HiveThriftServer2.listener.onSessionClosed(sessionHandle.getSessionId.toString)
    super.closeSession(sessionHandle)
    sparkSqlOperationManager.sessionToActivePool.remove(sessionHandle)
    sparkSqlOperationManager.sessionToContexts.remove(sessionHandle)
  }
}
