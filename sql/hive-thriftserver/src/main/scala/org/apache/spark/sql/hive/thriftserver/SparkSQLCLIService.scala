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

import java.io.IOException
import java.util.{List => JList}
import javax.security.auth.login.LoginException

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.shims.Utils
import org.apache.hadoop.security.{SecurityUtil, UserGroupInformation}
import org.apache.hive.service.{AbstractService, CompositeService, Service}
import org.apache.hive.service.Service.STATE
import org.apache.hive.service.auth.HiveAuthFactory
import org.apache.hive.service.cli._
import org.apache.hive.service.server.HiveServer2

import org.apache.spark.internal.SparkLogger
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.util.SQLKeywordUtils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._

private[hive] class SparkSQLCLIService(hiveServer: HiveServer2, sqlContext: SQLContext)
  extends CLIService(hiveServer)
  with ReflectedCompositeService {

  override def init(hiveConf: HiveConf): Unit = {
    setSuperField(this, "hiveConf", hiveConf)

    val sparkSqlSessionManager = new SparkSQLSessionManager(hiveServer, sqlContext)
    setSuperField(this, "sessionManager", sparkSqlSessionManager)
    addService(sparkSqlSessionManager)
    var sparkServiceUGI: UserGroupInformation = null
    var httpUGI: UserGroupInformation = null

    if (UserGroupInformation.isSecurityEnabled) {
      try {
        val principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL)
        val keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB)
        if (principal.isEmpty || keyTabFile.isEmpty) {
          throw QueryExecutionErrors.invalidKerberosConfigForHiveServer2Error()
        }

        val originalUgi = UserGroupInformation.getCurrentUser
        sparkServiceUGI = if (HiveAuthFactory.needUgiLogin(originalUgi,
          SecurityUtil.getServerPrincipal(principal, "0.0.0.0"), keyTabFile)) {
          HiveAuthFactory.loginFromKeytab(hiveConf)
          Utils.getUGI()
        } else {
          originalUgi
        }

        setSuperField(this, "serviceUGI", sparkServiceUGI)
      } catch {
        case e @ (_: IOException | _: LoginException) =>
          throw HiveThriftServerErrors.cannotLoginToKerberosError(e)
      }

      // Try creating spnego UGI if it is configured.
      val principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_PRINCIPAL).trim
      val keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_KEYTAB).trim
      if (principal.nonEmpty && keyTabFile.nonEmpty) {
        try {
          httpUGI = HiveAuthFactory.loginFromSpnegoKeytabAndReturnUGI(hiveConf)
          setSuperField(this, "httpUGI", httpUGI)
        } catch {
          case e: IOException =>
            throw HiveThriftServerErrors.cannotLoginToSpnegoError(principal, keyTabFile, e)
        }
      }
    }

    initCompositeService(hiveConf)
  }

  /**
   * the super class [[CLIService#start]] starts a useless dummy metastore client, skip it and call
   * the ancestor [[CompositeService#start]] directly.
   */
  override def start(): Unit = startCompositeService()

  override def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): GetInfoValue = {
    getInfoType match {
      case GetInfoType.CLI_SERVER_NAME => new GetInfoValue("Spark SQL")
      case GetInfoType.CLI_DBMS_NAME => new GetInfoValue("Spark SQL")
      case GetInfoType.CLI_DBMS_VER => new GetInfoValue(sqlContext.sparkContext.version)
      case GetInfoType.CLI_ODBC_KEYWORDS =>
        new GetInfoValue(SQLKeywordUtils.keywords.mkString(","))
      case _ => super.getInfo(sessionHandle, getInfoType)
    }
  }
}

private[thriftserver] trait ReflectedCompositeService { this: AbstractService =>

  private val logInfo = (msg: String) => getAncestorField[SparkLogger](this, 3, "LOG").info(msg)

  private val logError = (msg: String, e: Throwable) =>
    getAncestorField[SparkLogger](this, 3, "LOG").error(msg, e)

  def initCompositeService(hiveConf: HiveConf): Unit = {
    // Emulating `CompositeService.init(hiveConf)`
    val serviceList = getAncestorField[JList[Service]](this, 2, "serviceList")
    serviceList.asScala.foreach(_.init(hiveConf))

    // Emulating `AbstractService.init(hiveConf)`
    invoke(classOf[AbstractService], this, "ensureCurrentState", classOf[STATE] -> STATE.NOTINITED)
    setAncestorField(this, 3, "hiveConf", hiveConf)
    invoke(classOf[AbstractService], this, "changeState", classOf[STATE] -> STATE.INITED)
    logInfo(s"Service: $getName is inited.")
  }

  def startCompositeService(): Unit = {
    // Emulating `CompositeService.start`
    val serviceList = getAncestorField[JList[Service]](this, 2, "serviceList")
    var serviceStartCount = 0
    try {
      serviceList.asScala.foreach { service =>
        service.start()
        serviceStartCount += 1
      }
      // Emulating `AbstractService.start`
      val startTime = java.lang.Long.valueOf(System.currentTimeMillis())
      setAncestorField(this, 3, "startTime", startTime)
      invoke(classOf[AbstractService], this, "ensureCurrentState", classOf[STATE] -> STATE.INITED)
      invoke(classOf[AbstractService], this, "changeState", classOf[STATE] -> STATE.STARTED)
      logInfo(s"Service: $getName is started.")
    } catch {
      case NonFatal(e) =>
      logError(s"Error starting services $getName", e)
      invoke(classOf[CompositeService], this, "stop",
        classOf[Int] -> Integer.valueOf(serviceStartCount))
      throw HiveThriftServerErrors.failedToStartServiceError(getName, e)
    }
  }
}
