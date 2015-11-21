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

import scala.collection.JavaConverters._

import org.apache.commons.logging.Log
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.shims.Utils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.Service.STATE
import org.apache.hive.service.auth.HiveAuthFactory
import org.apache.hive.service.cli._
import org.apache.hive.service.server.HiveServer2
import org.apache.hive.service.{AbstractService, Service, ServiceException}

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._

private[hive] class SparkSQLCLIService(hiveServer: HiveServer2, hiveContext: HiveContext)
  extends CLIService(hiveServer)
  with ReflectedCompositeService {

  private val sparkSqlSessionManager = new SparkSQLSessionManager(hiveServer, hiveContext)

  override def init(hiveConf: HiveConf) {
    setSuperField(this, "hiveConf", hiveConf)
    setSuperField(this, "sessionManager", sparkSqlSessionManager)
    addService(sparkSqlSessionManager)
    var sparkServiceUGI: UserGroupInformation = null

    if (UserGroupInformation.isSecurityEnabled) {
      try {
        HiveAuthFactory.loginFromKeytab(hiveConf)
        sparkServiceUGI = Utils.getUGI
        setSuperField(this, "serviceUGI", sparkServiceUGI)
      } catch {
        case e @ (_: IOException | _: LoginException) =>
          throw new ServiceException("Unable to login to kerberos with given principal/keytab", e)
      }
    }

    initCompositeService(hiveConf)
  }

  override def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): GetInfoValue = {
    getInfoType match {
      case GetInfoType.CLI_SERVER_NAME => new GetInfoValue("Spark SQL")
      case GetInfoType.CLI_DBMS_NAME => new GetInfoValue("Spark SQL")
      case GetInfoType.CLI_DBMS_VER => new GetInfoValue(hiveContext.sparkContext.version)
      case _ => super.getInfo(sessionHandle, getInfoType)
    }
  }

  private def withMetadataHive[T](sessionHandle: SessionHandle)(f: => T): T = {
    val sessionConf = sparkSqlSessionManager.getSession(sessionHandle).getHiveConf
    val overridenVarnames = hiveContext.overridenExecutionHiveConfiguration.keys
    val originalConfs = for {
      varname <- overridenVarnames
    } yield varname -> Option(sessionConf.get(varname))

    overridenVarnames.foreach { varname =>
      val value = hiveContext.metadataHive.getConf(varname, null)
      if (value == null) sessionConf.unset(varname) else sessionConf.set(varname, value)
    }

    try hiveContext.metadataHive.withHiveState(f) finally {
      originalConfs.foreach {
        case (varname, Some(value)) => sessionConf.set(varname, value)
        case (varname, _) => sessionConf.unset(varname)
      }
    }
  }

  override def getCatalogs(sessionHandle: SessionHandle): OperationHandle = {
    withMetadataHive(sessionHandle) {
      super.getCatalogs(sessionHandle)
    }
  }

  override def getSchemas(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String): OperationHandle = {
    withMetadataHive(sessionHandle) {
      super.getSchemas(sessionHandle, catalogName, schemaName)
    }
  }

  override def getTables(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: JList[String]): OperationHandle = {
    withMetadataHive(sessionHandle) {
      super.getTables(sessionHandle, catalogName, schemaName, tableName, tableTypes)
    }
  }

  override def getTableTypes(sessionHandle: SessionHandle): OperationHandle = {
    withMetadataHive(sessionHandle) {
      super.getTableTypes(sessionHandle)
    }
  }

  override def getColumns(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): OperationHandle = {
    withMetadataHive(sessionHandle) {
      super.getColumns(sessionHandle, catalogName, schemaName, tableName, columnName)
    }
  }

  override def getFunctions(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      functionName: String): OperationHandle = {
    super.getFunctions(sessionHandle, catalogName, schemaName, functionName)
  }
}

private[thriftserver] trait ReflectedCompositeService { this: AbstractService =>
  def initCompositeService(hiveConf: HiveConf) {
    // Emulating `CompositeService.init(hiveConf)`
    val serviceList = getAncestorField[JList[Service]](this, 2, "serviceList")
    serviceList.asScala.foreach(_.init(hiveConf))

    // Emulating `AbstractService.init(hiveConf)`
    invoke(classOf[AbstractService], this, "ensureCurrentState", classOf[STATE] -> STATE.NOTINITED)
    setAncestorField(this, 3, "hiveConf", hiveConf)
    invoke(classOf[AbstractService], this, "changeState", classOf[STATE] -> STATE.INITED)
    getAncestorField[Log](this, 3, "LOG").info(s"Service: $getName is inited.")
  }
}
