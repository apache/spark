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

import scala.util.control.NonFatal

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.service.cli.{HiveSQLException, SessionHandle}
import org.apache.hive.service.cli.session.SessionManager
import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.apache.hive.service.server.HiveServer2

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._
import org.apache.spark.sql.hive.thriftserver.server.SparkSQLOperationManager
import org.apache.spark.sql.internal.SQLConf


private[hive] class SparkSQLSessionManager(hiveServer: HiveServer2, sqlContext: SQLContext)
  extends SessionManager(hiveServer)
  with ReflectedCompositeService with Logging {

  private lazy val sparkSqlOperationManager = new SparkSQLOperationManager()

  override def init(hiveConf: HiveConf): Unit = {
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
    val sessionHandle =
      super.openSession(protocol, username, passwd, ipAddress, sessionConf, withImpersonation,
          delegationToken)
    try {
      val session = super.getSession(sessionHandle)
      HiveThriftServer2.eventManager.onSessionCreated(
        session.getIpAddress, sessionHandle.getSessionId.toString, session.getUsername)
      val ctx = if (sqlContext.conf.hiveThriftServerSingleSession) {
        sqlContext
      } else {
        sqlContext.newSession()
      }
      ctx.setConf(HiveUtils.FAKE_HIVE_VERSION.key, HiveUtils.builtinHiveVersion)
      ctx.setConf(SQLConf.DATETIME_JAVA8API_ENABLED, true)
      val hiveSessionState = session.getSessionState
      setConfMap(ctx, hiveSessionState.getOverriddenConfigurations)
      setConfMap(ctx, hiveSessionState.getHiveVariables)
      if (sessionConf != null && sessionConf.containsKey("use:database")) {
        ctx.sql(s"use ${sessionConf.get("use:database")}")
      }
      sparkSqlOperationManager.sessionToContexts.put(sessionHandle, ctx)
      sessionHandle
    } catch {
      case NonFatal(e) =>
        try {
          closeSession(sessionHandle)
        } catch {
          case NonFatal(inner) =>
            logWarning("Error closing session", inner)
        }
        throw new HiveSQLException("Failed to open new session: " + e, e)
    }
  }

  override def closeSession(sessionHandle: SessionHandle): Unit = {
    HiveThriftServer2.eventManager.onSessionClosed(sessionHandle.getSessionId.toString)
    val ctx = sparkSqlOperationManager.sessionToContexts.getOrDefault(sessionHandle, sqlContext)
    ctx.sparkSession.sessionState.catalog.getTempViewNames().foreach(ctx.uncacheTable)
    super.closeSession(sessionHandle)
    sparkSqlOperationManager.sessionToContexts.remove(sessionHandle)
  }

  def setConfMap(conf: SQLContext, confMap: java.util.Map[String, String]): Unit = {
    val iterator = confMap.entrySet().iterator()
    while (iterator.hasNext) {
      val kv = iterator.next()
      conf.setConf(kv.getKey, kv.getValue)
    }
  }
}
